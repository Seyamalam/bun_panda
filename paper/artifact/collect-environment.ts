import { mkdirSync, writeFileSync } from "node:fs";
import { arch, cpus, platform, release, totalmem } from "node:os";

function command(args: string[]): string {
  const result = Bun.spawnSync(args, { stdout: "pipe", stderr: "pipe" });
  return result.exitCode === 0 ? result.stdout.toString().trim() : "unavailable";
}

const cpu = cpus()[0];
const payload = {
  capturedAt: new Date().toISOString(),
  machine: {
    platform: platform(),
    release: release(),
    architecture: arch(),
    cpuModel: cpu?.model ?? "unknown",
    logicalCores: cpus().length,
    memoryBytes: totalmem(),
    macOS: command(["sw_vers"]),
  },
  runtimes: {
    bun: command(["bun", "--version"]),
    rustc: command(["rustc", "--version"]),
    cargo: command(["cargo", "--version"]),
    python: command(["bench/.venv/bin/python", "--version"]),
    pandas: command(["bench/.venv/bin/python", "-c", "import pandas; print(pandas.__version__)"]),
    typescript: command(["bunx", "tsc", "--version"]),
    arquero: JSON.parse(await Bun.file("node_modules/arquero/package.json").text()).version,
  },
  experiment: {
    powerMode: "not programmatically controlled",
    backgroundLoad: "best-effort quiescent interactive workstation",
    processIsolation: "single benchmark process per comparison suite",
    repositoryCommit: command(["git", "rev-parse", "HEAD"]),
    dirtyWorkingTree: command(["git", "status", "--porcelain"]).length > 0,
  },
};

mkdirSync("paper/data", { recursive: true });
writeFileSync("paper/data/environment.json", `${JSON.stringify(payload, null, 2)}\n`, "utf8");
console.log(JSON.stringify(payload, null, 2));
