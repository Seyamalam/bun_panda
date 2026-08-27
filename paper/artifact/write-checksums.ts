import { createHash } from "node:crypto";
import { readdirSync, readFileSync, statSync, writeFileSync } from "node:fs";
import { join, relative } from "node:path";

const root = process.cwd();
const explicit = [
  "CITATION.cff",
  "LICENSE",
  "bun.lock",
  "package.json",
  "paper/CLAIM-EVIDENCE.md",
  "paper/manuscript/main.tex",
  "paper/manuscript/references.bib",
  "src/wasm/bun_panda_core.wasm",
];

function walk(directory: string): string[] {
  const files: string[] = [];
  for (const name of readdirSync(directory)) {
    const path = join(directory, name);
    if (statSync(path).isDirectory()) files.push(...walk(path));
    else files.push(relative(root, path));
  }
  return files;
}

const files = [...new Set([
  ...explicit,
  ...walk(join(root, "paper/data")),
])].sort();
const lines = files.map((path) => {
  const digest = createHash("sha256").update(readFileSync(join(root, path))).digest("hex");
  return `${digest}  ${path}`;
});
writeFileSync(join(root, "SHA256SUMS"), `${lines.join("\n")}\n`, "utf8");
console.log(`wrote ${lines.length} checksums to SHA256SUMS`);
