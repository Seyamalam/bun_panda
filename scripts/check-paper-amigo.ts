import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";

interface PaperAmigoState {
  projectId: string;
  pdfPath: string;
}

interface PaperAmigoProject {
  id: string;
  files: Array<{ hash: string; name: string }>;
}

interface ProjectList {
  command: "project-list";
  projects: PaperAmigoProject[];
}

const root = resolve(import.meta.dir, "..");
const state = JSON.parse(
  readFileSync(resolve(root, "paper/paper-amigo-project.json"), "utf8"),
) as PaperAmigoState;
const pdfPath = resolve(root, state.pdfPath);
const localHash = createHash("sha256")
  .update(readFileSync(pdfPath))
  .digest("hex");

const child = Bun.spawnSync(["paper-amigo", "project", "list", "--json"], {
  cwd: root,
  stdout: "pipe",
  stderr: "pipe",
});

if (child.exitCode !== 0) {
  process.stderr.write(child.stderr);
  throw new Error("Could not read Paper Amigo projects");
}

const response = JSON.parse(child.stdout.toString()) as ProjectList;
const project = response.projects.find((entry) => entry.id === state.projectId);

if (!project) {
  throw new Error(`Paper Amigo project ${state.projectId} was not found`);
}

const remoteFile = project.files.find((file) => file.name === "main.pdf") ??
  project.files[0];

if (!remoteFile) {
  throw new Error(`Paper Amigo project ${state.projectId} has no PDF`);
}

if (remoteFile.hash !== localHash) {
  throw new Error(
    [
      "Paper Amigo is stale.",
      `Local PDF SHA-256:  ${localHash}`,
      `Remote PDF SHA-256: ${remoteFile.hash}`,
      "Replace the PDF in the existing project. Do not create a duplicate project.",
    ].join("\n"),
  );
}

console.log(`Paper Amigo is current: ${state.projectId} (${localHash})`);
