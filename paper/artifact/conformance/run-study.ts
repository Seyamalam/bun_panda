const root = process.env.BUN_PANDA_CONFORMANCE_DIR ?? "paper/data/conformance";
const cases = `${root}/cases.json`;
const python = process.env.BUN_PANDA_PYTHON;

function run(command: string[], env: Record<string, string> = {}): void {
  const result = Bun.spawnSync(command, {
    cwd: process.cwd(),
    env: { ...process.env, ...env },
    stdout: "inherit",
    stderr: "inherit",
  });
  if (result.exitCode !== 0) {
    throw new Error(`command failed (${result.exitCode}): ${command.join(" ")}`);
  }
}

run(["bun", "run", "paper/artifact/conformance/generate-cases.ts"], {
  BUN_PANDA_CONFORMANCE_CASES: cases,
});
if (python) {
  run([
    python,
    "paper/artifact/conformance/run-pandas.py",
    "--cases", cases,
    "--out", `${root}/pandas.json`,
  ]);
} else {
  run([
    "uv", "run", "--with", "pandas==3.0.5", "python",
    "paper/artifact/conformance/run-pandas.py",
    "--cases", cases,
    "--out", `${root}/pandas.json`,
  ]);
}
for (const mode of ["typescript", "wasm", "adaptive"] as const) {
  run([
    "bun", "run", "paper/artifact/conformance/run-bun.ts",
    "--cases", cases,
    "--mode", mode,
    "--out", `${root}/${mode}.json`,
  ]);
}
run(["bun", "run", "paper/artifact/conformance/compare.ts"], {
  BUN_PANDA_CONFORMANCE_DIR: root,
});
