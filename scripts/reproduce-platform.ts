import { createHash } from "node:crypto";
import {
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  statSync,
  writeFileSync,
} from "node:fs";
import { arch, cpus, platform, release, tmpdir, totalmem } from "node:os";
import { dirname, join, resolve } from "node:path";

type Profile = "full" | "quick";

interface Options {
  profile: Profile;
  output?: string;
  tester?: string;
  skipBrowser: boolean;
}

interface CommandRecord {
  id: string;
  label: string;
  required: boolean;
  command: string[];
  startedAt: string;
  finishedAt: string;
  durationMs: number;
  exitCode: number | null;
  status: "passed" | "failed" | "skipped";
  stdout: string;
  stderr: string;
  outputTruncated: boolean;
  note?: string;
}

const repoRoot = resolve(import.meta.dir, "..");
const maxCapturedCharacters = 250_000;

function usage(): never {
  console.log(`Usage: bun run reproduce:platform [options]

Options:
  --profile full|quick  Full paper protocol or a short installation check (default: full)
  --output PATH        Report path (default: repository root with a dated name)
  --tester NAME        Name or identifier to place in the report
  --skip-browser       Skip Playwright installation and browser measurements
  --help               Show this message`);
  process.exit(0);
}

function parseOptions(args: string[]): Options {
  const options: Options = { profile: "full", skipBrowser: false };
  for (let index = 0; index < args.length; index += 1) {
    const argument = args[index]!;
    if (argument === "--help" || argument === "-h") usage();
    if (argument === "--skip-browser") {
      options.skipBrowser = true;
      continue;
    }
    if (argument === "--profile" || argument === "--output" || argument === "--tester") {
      const value = args[index + 1];
      if (!value) throw new Error(`${argument} requires a value`);
      index += 1;
      if (argument === "--profile") {
        if (value !== "full" && value !== "quick") {
          throw new Error(`unknown profile: ${value}`);
        }
        options.profile = value;
      } else if (argument === "--output") {
        options.output = value;
      } else {
        options.tester = value;
      }
      continue;
    }
    throw new Error(`unknown option: ${argument}`);
  }
  return options;
}

function quiet(command: string[]): { exitCode: number | null; stdout: string; stderr: string } {
  try {
    const child = Bun.spawnSync(command, {
      cwd: repoRoot,
      stdout: "pipe",
      stderr: "pipe",
    });
    return {
      exitCode: child.exitCode,
      stdout: child.stdout.toString().trim(),
      stderr: child.stderr.toString().trim(),
    };
  } catch (error) {
    return { exitCode: null, stdout: "", stderr: String(error) };
  }
}

function firstWorkingCommand(commands: string[][]): string[] | undefined {
  return commands.find((command) => quiet([...command, "--version"]).exitCode === 0);
}

function version(command: string[]): string {
  const result = quiet(command);
  return result.exitCode === 0 ? result.stdout || result.stderr : "unavailable";
}

function sha256(path: string): string {
  return createHash("sha256").update(readFileSync(path)).digest("hex");
}

function linuxDistribution(): string | null {
  if (platform() !== "linux" || !existsSync("/etc/os-release")) return null;
  const fields = Object.fromEntries(
    readFileSync("/etc/os-release", "utf8")
      .split("\n")
      .filter((line) => line.includes("="))
      .map((line) => {
        const split = line.indexOf("=");
        return [line.slice(0, split), line.slice(split + 1).replace(/^\"|\"$/g, "")];
      }),
  );
  return fields.PRETTY_NAME ?? fields.NAME ?? null;
}

const options = parseOptions(Bun.argv.slice(2));
const timestamp = new Date().toISOString().replaceAll(":", "-").replace(/\.\d{3}Z$/, "Z");
const platformName = platform() === "darwin" ? "macos" : platform() === "win32" ? "windows" : platform();
const defaultName = `bun-panda-reproduction-${platformName}-${arch()}-${timestamp}.json`;
const reportPath = resolve(repoRoot, options.output ?? defaultName);
const runDirectory = mkdtempSync(join(tmpdir(), "bun-panda-reproduction-"));
const homeDirectory = process.env.HOME ?? process.env.USERPROFILE;

function sanitize(value: string): string {
  let output = value.replaceAll(repoRoot, "<repo>").replaceAll(runDirectory, "<run-dir>");
  if (homeDirectory) output = output.replaceAll(homeDirectory, "<home>");
  return output;
}

function sanitizeData(value: unknown): unknown {
  if (typeof value === "string") return sanitize(value);
  if (Array.isArray(value)) return value.map(sanitizeData);
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>)
        .map(([key, child]) => [key, sanitizeData(child)]),
    );
  }
  return value;
}

const initialGitStatus = quiet(["git", "status", "--porcelain"]);
const runStarted = Date.now();
const report: Record<string, unknown> & {
  status: string;
  commands: CommandRecord[];
  artifacts: Record<string, unknown>;
} = {
  schemaVersion: "1.0.0",
  status: "running",
  startedAt: new Date(runStarted).toISOString(),
  finishedAt: null,
  profile: options.profile,
  tester: options.tester ?? null,
  claimScope: {
    currentManuscript: "macOS performance only",
    returnedReports: "independent validation inputs; do not pool or publish without review",
  },
  repository: {
    remote: "https://github.com/Seyamalam/bun_panda",
    commit: version(["git", "rev-parse", "HEAD"]),
    branch: version(["git", "branch", "--show-current"]),
    cleanAtStart: initialGitStatus.exitCode === 0 && initialGitStatus.stdout.length === 0,
    changesAtStart: initialGitStatus.stdout ? initialGitStatus.stdout.split("\n") : [],
  },
  machine: {
    operatingSystem: platformName,
    kernelRelease: release(),
    distribution: linuxDistribution(),
    architecture: arch(),
    cpuModel: cpus()[0]?.model ?? "unknown",
    logicalCores: cpus().length,
    totalMemoryBytes: totalmem(),
  },
  tools: {
    bun: Bun.version,
    git: version(["git", "--version"]),
    node: version(["node", "--version"]),
    npm: version(["npm", "--version"]),
    rustc: version(["rustc", "--version"]),
    cargo: version(["cargo", "--version"]),
  },
  protocol: {
    profile: options.profile,
    browserRequested: !options.skipBrowser,
    note: options.profile === "full"
      ? "Uses the manuscript's process, warm-up, iteration, scale, and browser replication settings."
      : "Uses reduced replication for setup diagnosis. Quick results are not manuscript evidence.",
  },
  commands: [],
  artifacts: {},
};

function persistReport(): void {
  mkdirSync(dirname(reportPath), { recursive: true });
  writeFileSync(reportPath, `${JSON.stringify(report, null, 2)}\n`, "utf8");
}

async function capture(
  stream: ReadableStream<Uint8Array>,
  destination: typeof process.stdout | typeof process.stderr,
): Promise<{ text: string; truncated: boolean }> {
  const reader = stream.getReader();
  const decoder = new TextDecoder();
  let output = "";
  let truncated = false;
  while (true) {
    const chunk = await reader.read();
    if (chunk.done) break;
    destination.write(chunk.value);
    const decoded = decoder.decode(chunk.value, { stream: true });
    if (output.length < maxCapturedCharacters) {
      const remaining = maxCapturedCharacters - output.length;
      output += decoded.slice(0, remaining);
      if (decoded.length > remaining) truncated = true;
    } else {
      truncated = true;
    }
  }
  const tail = decoder.decode();
  if (output.length < maxCapturedCharacters) output += tail.slice(0, maxCapturedCharacters - output.length);
  else if (tail.length > 0) truncated = true;
  return { text: sanitize(output), truncated };
}

async function runStep(
  id: string,
  label: string,
  command: string[],
  settings: { required?: boolean; env?: Record<string, string> } = {},
): Promise<boolean> {
  const required = settings.required ?? true;
  const started = Date.now();
  const startedAt = new Date(started).toISOString();
  console.log(`\n[${id}] ${label}`);
  console.log(`$ ${sanitize(command.join(" "))}`);
  try {
    const child = Bun.spawn(command, {
      cwd: repoRoot,
      env: { ...process.env, ...settings.env },
      stdout: "pipe",
      stderr: "pipe",
    });
    const [stdout, stderr, exitCode] = await Promise.all([
      capture(child.stdout, process.stdout),
      capture(child.stderr, process.stderr),
      child.exited,
    ]);
    const finished = Date.now();
    report.commands.push({
      id,
      label,
      required,
      command: command.map(sanitize),
      startedAt,
      finishedAt: new Date(finished).toISOString(),
      durationMs: finished - started,
      exitCode,
      status: exitCode === 0 ? "passed" : "failed",
      stdout: stdout.text,
      stderr: stderr.text,
      outputTruncated: stdout.truncated || stderr.truncated,
    });
    persistReport();
    return exitCode === 0;
  } catch (error) {
    const finished = Date.now();
    report.commands.push({
      id,
      label,
      required,
      command: command.map(sanitize),
      startedAt,
      finishedAt: new Date(finished).toISOString(),
      durationMs: finished - started,
      exitCode: null,
      status: "failed",
      stdout: "",
      stderr: sanitize(String(error)),
      outputTruncated: false,
    });
    console.error(String(error));
    persistReport();
    return false;
  }
}

function skipStep(id: string, label: string, note: string, required = false): void {
  const now = new Date().toISOString();
  report.commands.push({
    id,
    label,
    required,
    command: [],
    startedAt: now,
    finishedAt: now,
    durationMs: 0,
    exitCode: null,
    status: "skipped",
    stdout: "",
    stderr: "",
    outputTruncated: false,
    note,
  });
  persistReport();
}

function passRecordedCheck(id: string, label: string, note: string): void {
  const now = new Date().toISOString();
  report.commands.push({
    id,
    label,
    required: false,
    command: [],
    startedAt: now,
    finishedAt: now,
    durationMs: 0,
    exitCode: 0,
    status: "passed",
    stdout: "",
    stderr: "",
    outputTruncated: false,
    note,
  });
  persistReport();
}

function attachJson(name: string, path: string): void {
  if (!existsSync(path)) return;
  try {
    report.artifacts[name] = {
      sha256: sha256(path),
      bytes: statSync(path).size,
      data: sanitizeData(JSON.parse(readFileSync(path, "utf8"))),
    };
  } catch (error) {
    report.artifacts[name] = { error: sanitize(String(error)) };
  }
  persistReport();
}

persistReport();
console.log(`Platform reproduction profile: ${options.profile}`);
console.log(`A report is updated after every step: ${reportPath}`);

try {
  const installPassed = await runStep(
    "dependencies",
    "Install locked JavaScript dependencies",
    ["bun", "install", "--frozen-lockfile"],
  );

  if (!installPassed) {
    console.warn("Dependency installation failed. Later steps will still run so the report records the failures.");
  }

  await runStep("check", "Type-check, lint, and run the complete Bun test suite", ["bun", "run", "check"]);
  await runStep("package-smoke", "Pack and import the npm artifact", ["bun", "run", "pack:smoke"]);
  const parityOutput = join(runDirectory, "parity.md");
  await runStep(
    "api-parity",
    "Run the API name census",
    ["bun", "run", "parity"],
    { env: { BUN_PANDA_PARITY_OUTPUT: parityOutput } },
  );
  if (existsSync(parityOutput)) {
    report.artifacts.apiParity = {
      sha256: sha256(parityOutput),
      bytes: statSync(parityOutput).size,
      markdown: sanitize(readFileSync(parityOutput, "utf8")),
    };
    persistReport();
  }

  const packageOutput = join(runDirectory, "package-size.json");
  await runStep(
    "package-size",
    "Measure package, bundle, compressed bundle, and Wasm sizes",
    ["bun", "run", "paper/artifact/analyze-package-size.ts"],
    { env: { BUN_PANDA_PACKAGE_SIZE_OUTPUT: packageOutput } },
  );
  attachJson("packageSize", packageOutput);

  const pythonCommand = firstWorkingCommand(
    platform() === "win32" ? [["py", "-3"], ["python"], ["python3"]] : [["python3"], ["python"]],
  );
  let reproductionPython: string | undefined;
  if (!pythonCommand) {
    skipStep("python-venv", "Create the isolated pandas environment", "Python 3.11 or newer was not found.", true);
    skipStep("python-dependencies", "Install pandas 3.0.5", "The Python environment was not created.", true);
  } else {
    const venv = join(runDirectory, "python");
    const venvPassed = await runStep(
      "python-venv",
      "Create the isolated pandas environment",
      [...pythonCommand, "-m", "venv", venv],
    );
    reproductionPython = platform() === "win32"
      ? join(venv, "Scripts", "python.exe")
      : join(venv, "bin", "python");
    if (venvPassed) {
      const pandasPassed = await runStep(
        "python-dependencies",
        "Install pandas 3.0.5",
        [reproductionPython, "-m", "pip", "install", "--disable-pip-version-check", "-r", "paper/artifact/linux/requirements.lock"],
      );
      if (!pandasPassed) reproductionPython = undefined;
    } else {
      reproductionPython = undefined;
      skipStep("python-dependencies", "Install pandas 3.0.5", "The Python environment was not created.", true);
    }
  }

  const conformanceDirectory = join(runDirectory, "conformance");
  if (reproductionPython) {
    await runStep(
      "conformance",
      "Run the 2,500-case pandas differential oracle and backend checks",
      ["bun", "run", "conformance"],
      {
        env: {
          BUN_PANDA_PYTHON: reproductionPython,
          BUN_PANDA_CONFORMANCE_DIR: conformanceDirectory,
        },
      },
    );
    attachJson("conformanceSummary", join(conformanceDirectory, "summary.json"));
    attachJson("conformanceMismatchLedger", join(conformanceDirectory, "mismatch-ledger.json"));
  } else {
    skipStep("conformance", "Run the pandas differential oracle", "The isolated pandas environment is unavailable.", true);
  }

  const quickFresh: Record<string, string> = options.profile === "quick"
    ? {
        BUN_PANDA_FRESH_SIZES: "10000",
        BUN_PANDA_FRESH_PROCESSES: "2",
        BUN_PANDA_FRESH_WARMUPS: "1",
        BUN_PANDA_FRESH_ITERATIONS: "3",
        BUN_PANDA_BOOTSTRAPS: "200",
      }
    : {};
  const freshOutput = join(runDirectory, "fresh-process.json");
  await runStep(
    "fresh-process",
    "Run the TypeScript, Wasm, and adaptive fresh-process ablation",
    ["bun", "run", "bench:fresh"],
    { env: { ...quickFresh, BUN_PANDA_FRESH_OUTPUT: freshOutput } },
  );
  attachJson("freshProcessAblation", freshOutput);

  const quickCompetitors: Record<string, string> = options.profile === "quick"
    ? {
        BUN_PANDA_COMPETITOR_SIZES: "10000",
        BUN_PANDA_COMPETITOR_PROCESSES: "1",
        BUN_PANDA_COMPETITOR_WARMUPS: "1",
        BUN_PANDA_COMPETITOR_ITERATIONS: "3",
      }
    : {};
  const competitorOutput = join(runDirectory, "competitor-synthetic.json");
  await runStep(
    "competitors-synthetic",
    "Run the five-system synthetic comparison",
    ["bun", "run", "bench:competitors"],
    { env: { ...quickCompetitors, BUN_PANDA_COMPETITOR_OUTPUT: competitorOutput } },
  );
  attachJson("competitorSynthetic", competitorOutput);

  const uciPath = join(repoRoot, "paper", "data", "workloads", "uci-bank", "bank-full.csv");
  const expectedUciDigest = "d1513ec63b385506f7cfce9f2c5caa9fe99e7ba4e8c3fa264b3aaf0f849ed32d";
  const uciReady = existsSync(uciPath) && sha256(uciPath) === expectedUciDigest;
  if (!uciReady) {
    await runStep(
      "uci-prepare",
      "Download and verify the UCI Bank Marketing workload",
      ["bun", "run", "workload:public"],
    );
  } else {
    passRecordedCheck(
      "uci-prepare",
      "Verify the UCI Bank Marketing workload",
      "The checked-in CSV has the expected SHA-256 digest.",
    );
  }
  const verifiedUci = existsSync(uciPath) && sha256(uciPath) === expectedUciDigest;
  if (verifiedUci) {
    const uciOutput = join(runDirectory, "competitor-uci.json");
    await runStep(
      "competitors-uci",
      "Run the five-system public-data comparison",
      ["bun", "run", "paper/artifact/competitors/run-study.ts"],
      {
        env: {
          ...quickCompetitors,
          BUN_PANDA_COMPETITOR_DATASET: "uci_bank",
          BUN_PANDA_COMPETITOR_SIZES: "45211",
          BUN_PANDA_COMPETITOR_DATASET_PATH: uciPath,
          BUN_PANDA_COMPETITOR_OUTPUT: uciOutput,
        },
      },
    );
    attachJson("competitorUciBank", uciOutput);
  } else {
    skipStep("competitors-uci", "Run the five-system public-data comparison", "The checksum-pinned UCI CSV is unavailable.", true);
  }

  if (options.skipBrowser) {
    skipStep("playwright-install", "Install Playwright browser binaries", "Skipped by --skip-browser.");
    skipStep("browser", "Run isolated browser-worker measurements", "Skipped by --skip-browser.");
  } else {
    const browserInstallPassed = await runStep(
      "playwright-install",
      "Install Chromium, Firefox, and WebKit binaries",
      ["bunx", "playwright", "install", "chromium", "firefox", "webkit"],
    );
    if (browserInstallPassed) {
      const browserOutput = join(runDirectory, "browser.json");
      const quickBrowser: Record<string, string> = options.profile === "quick"
        ? {
            BUN_PANDA_BROWSER_SIZES: "10000",
            BUN_PANDA_BROWSER_REPLICATES: "1",
            BUN_PANDA_BROWSER_WARMUPS: "1",
            BUN_PANDA_BROWSER_ITERATIONS: "3",
          }
        : {};
      await runStep(
        "browser",
        "Run isolated Chromium, Firefox, and WebKit worker measurements",
        ["bun", "run", "bench:browser"],
        { env: { ...quickBrowser, BUN_PANDA_BROWSER_OUTPUT: browserOutput } },
      );
      attachJson("browserStudy", browserOutput);
    } else {
      skipStep("browser", "Run isolated browser-worker measurements", "Playwright browser installation failed.", true);
    }
  }

  const failedRequired = report.commands.filter((entry) => entry.required && entry.status !== "passed");
  const skipped = report.commands.filter((entry) => entry.status === "skipped");
  report.status = failedRequired.length > 0
    ? "completed_with_failures"
    : skipped.length > 0
      ? "completed_with_skips"
      : "completed";
  report.finishedAt = new Date().toISOString();
  report.totalDurationMs = Date.now() - runStarted;
  report.summary = {
    passed: report.commands.filter((entry) => entry.status === "passed").length,
    failed: report.commands.filter((entry) => entry.status === "failed").length,
    skipped: skipped.length,
    failedRequiredSteps: failedRequired.map((entry) => entry.id),
  };
  persistReport();

  console.log(`\nReproduction status: ${report.status}`);
  console.log(`Send this one file to the author:\n${reportPath}`);
  if (failedRequired.length > 0) process.exitCode = 1;
} finally {
  rmSync(runDirectory, { recursive: true, force: true });
}
