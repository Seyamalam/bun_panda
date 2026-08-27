import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { spawnSync } from "node:child_process";

const repoRoot = resolve(import.meta.dir, "..");
const scratch = mkdtempSync(join(tmpdir(), "bun-panda-pack-"));

function run(command: string, args: string[], cwd: string): string {
  const result = spawnSync(command, args, {
    cwd,
    encoding: "utf8",
    env: process.env,
  });

  if (result.status !== 0) {
    process.stderr.write(result.stdout);
    process.stderr.write(result.stderr);
    throw new Error(`${command} ${args.join(" ")} exited with status ${result.status}`);
  }

  return result.stdout;
}

try {
  const packed = JSON.parse(
    run("npm", ["pack", "--json", "--ignore-scripts", "--pack-destination", scratch], repoRoot),
  ) as Array<{ filename: string; files: Array<{ path: string }> }>;
  const artifact = packed[0];
  if (!artifact) throw new Error("npm pack did not produce an artifact");

  const paths = new Set(artifact.files.map((file) => file.path));
  const required = [
    "index.ts",
    "src/index.ts",
    "src/internal/dataframe/windowApi.ts",
    "src/internal/series/seriesApi.ts",
    "src/internal/shared/plotting.ts",
    "src/wasm/bun_panda_core.wasm",
  ];
  const missing = required.filter((path) => !paths.has(path));
  if (missing.length > 0) throw new Error(`packed artifact is missing: ${missing.join(", ")}`);

  const forbidden = [...paths].filter(
    (path) => path.startsWith("crates/") || path.startsWith("test/") || path.startsWith("scripts/"),
  );
  if (forbidden.length > 0) throw new Error(`packed artifact contains development files: ${forbidden.join(", ")}`);

  const tarball = join(scratch, artifact.filename);
  writeFileSync(
    join(scratch, "package.json"),
    `${JSON.stringify({ private: true, dependencies: { bun_panda: `file:${tarball}` } }, null, 2)}\n`,
  );
  run("bun", ["install", "--ignore-scripts", "--silent"], scratch);
  run(
    "bun",
    [
      "-e",
      'import { DataFrame, Series } from "bun_panda"; const frame = new DataFrame([{ x: 1 }, { x: 2 }]); const series = new Series([1, 2]); if (frame.shape.join(",") !== "2,1" || series.size !== 2) throw new Error("package import smoke test failed");',
    ],
    scratch,
  );

  console.log(`pack smoke passed: ${artifact.filename} (${paths.size} files)`);
} finally {
  rmSync(scratch, { recursive: true, force: true });
}
