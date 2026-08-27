import { createHash } from "node:crypto";
import { readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";

interface ProjectFile {
  key: string;
  hash: string;
  name: string;
  size: number;
  uploadedAt: string;
  url: string;
}

interface PaperAmigoState {
  schemaVersion: number;
  projectId: string;
  title: string;
  pdfPath: string;
  uploadedSha256: string;
  file: ProjectFile;
  syncPolicy: Record<string, unknown>;
}

interface PaperAmigoProject {
  id: string;
  files: ProjectFile[];
}

interface ProjectList {
  command: "project-list";
  projects: PaperAmigoProject[];
}

const root = resolve(import.meta.dir, "..");
const statePath = resolve(root, "paper/paper-amigo-project.json");
const state = JSON.parse(readFileSync(statePath, "utf8")) as PaperAmigoState;
const pdfPath = resolve(root, state.pdfPath);
const localHash = createHash("sha256")
  .update(readFileSync(pdfPath))
  .digest("hex");

function runList(): ProjectList {
  const child = Bun.spawnSync(["paper-amigo", "project", "list", "--json"], {
    cwd: root,
    stdout: "pipe",
    stderr: "pipe",
  });

  if (child.exitCode !== 0) {
    process.stderr.write(child.stderr);
    throw new Error("Could not read Paper Amigo projects");
  }

  return JSON.parse(child.stdout.toString()) as ProjectList;
}

function findProject(response: ProjectList): PaperAmigoProject {
  const project = response.projects.find((entry) => entry.id === state.projectId);
  if (!project) throw new Error(`Paper Amigo project ${state.projectId} was not found`);
  return project;
}

function findTarget(project: PaperAmigoProject): ProjectFile {
  const file = project.files.find((entry) => entry.key === state.file.key) ??
    project.files.find((entry) => entry.name === state.file.name);
  if (!file) throw new Error(`Paper Amigo project ${state.projectId} has no matching PDF`);
  return file;
}

const before = findTarget(findProject(runList()));

if (before.hash === localHash) {
  console.log(`Paper Amigo is current: ${state.projectId} (${localHash})`);
  process.exit(0);
}

console.log(`Replacing ${before.name} in Paper Amigo project ${state.projectId}`);
const replacement = Bun.spawnSync([
  "paper-amigo",
  "project",
  "replace",
  state.projectId,
  before.key,
  pdfPath,
  "--json",
], {
  cwd: root,
  stdout: "pipe",
  stderr: "pipe",
});

if (replacement.exitCode !== 0) {
  const output = `${replacement.stdout.toString()}\n${replacement.stderr.toString()}`;
  if (!output.includes("replacement-status-unknown")) {
    process.stderr.write(output);
    throw new Error("Paper Amigo replacement failed");
  }

  const uncertainProject = findProject(runList());
  const committed = uncertainProject.files.find((file) => file.hash === localHash);
  if (!committed) {
    throw new Error(
      "Paper Amigo could not confirm whether the replacement committed. The command was not retried.",
    );
  }
}

const afterProject = findProject(runList());
const after = afterProject.files.find((file) => file.hash === localHash);

if (!after) {
  throw new Error("Paper Amigo did not return the replacement PDF after a successful command");
}

const updated: PaperAmigoState = {
  ...state,
  uploadedSha256: localHash,
  file: after,
};
writeFileSync(statePath, `${JSON.stringify(updated, null, 2)}\n`, "utf8");
console.log(`Paper Amigo updated: ${state.projectId} (${localHash})`);
