import { mkdirSync, writeFileSync } from "node:fs";
import { cpus, platform, arch } from "node:os";

type Mode = "typescript" | "wasm" | "adaptive";
type Workload = "groupby_fused_4" | "sort_numeric_full" | "sort_numeric_top1000" | "filter_boolean_mask";

interface ProcessResult {
  workload: Workload;
  mode: Mode;
  rows: number;
  seed: number;
  samplesMs: number[];
  digest: string;
  outputShape: [number, number];
  processWallMs: number;
  memory: Record<string, number | string | null>;
}

const sizes = (process.env.BUN_PANDA_FRESH_SIZES ?? "10000,100000,250000")
  .split(",").map(Number);
const workloads = (process.env.BUN_PANDA_FRESH_WORKLOADS ??
  "groupby_fused_4,sort_numeric_full,sort_numeric_top1000,filter_boolean_mask")
  .split(",") as Workload[];
const modes: Mode[] = ["typescript", "wasm", "adaptive"];
const processReplicates = Number(process.env.BUN_PANDA_FRESH_PROCESSES ?? "20");
const iterations = Number(process.env.BUN_PANDA_FRESH_ITERATIONS ?? "20");
const warmups = Number(process.env.BUN_PANDA_FRESH_WARMUPS ?? "5");
const bootstrapReplicates = Number(process.env.BUN_PANDA_BOOTSTRAPS ?? "2000");
const outputPath = process.env.BUN_PANDA_FRESH_OUTPUT ?? "paper/data/fresh-process-ablation.json";
const baseSeed = 20260826;

interface Task { workload: Workload; mode: Mode; rows: number; replicate: number; seed: number }

function lcg(initialSeed: number): () => number {
  let state = initialSeed >>> 0;
  return () => {
    state = (Math.imul(1664525, state) + 1013904223) >>> 0;
    return state / 2 ** 32;
  };
}

function shuffle<T>(values: T[], seed: number): T[] {
  const random = lcg(seed);
  const out = [...values];
  for (let i = out.length - 1; i > 0; i -= 1) {
    const j = Math.floor(random() * (i + 1));
    [out[i], out[j]] = [out[j]!, out[i]!];
  }
  return out;
}

function mean(values: number[]): number {
  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function median(values: number[]): number {
  const sorted = [...values].sort((a, b) => a - b);
  const middle = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 0
    ? (sorted[middle - 1]! + sorted[middle]!) / 2
    : sorted[middle]!;
}

function quantile(values: number[], probability: number): number {
  const sorted = [...values].sort((a, b) => a - b);
  const index = (sorted.length - 1) * probability;
  const lower = Math.floor(index);
  const upper = Math.ceil(index);
  if (lower === upper) return sorted[lower]!;
  return sorted[lower]! * (upper - index) + sorted[upper]! * (index - lower);
}

function resampledMean(processes: ProcessResult[], random: () => number): number {
  let sum = 0;
  let count = 0;
  for (let i = 0; i < processes.length; i += 1) {
    const selected = processes[Math.floor(random() * processes.length)]!;
    for (let j = 0; j < selected.samplesMs.length; j += 1) {
      sum += selected.samplesMs[Math.floor(random() * selected.samplesMs.length)]!;
      count += 1;
    }
  }
  return sum / count;
}

function ratioInterval(
  baseline: ProcessResult[],
  candidate: ProcessResult[],
  seed: number
): { estimate: number; lower95: number; upper95: number; bootstrapReplicates: number } {
  const estimate = mean(baseline.flatMap((entry) => entry.samplesMs)) /
    mean(candidate.flatMap((entry) => entry.samplesMs));
  const random = lcg(seed);
  const ratios = new Array<number>(bootstrapReplicates);
  for (let i = 0; i < bootstrapReplicates; i += 1) {
    ratios[i] = resampledMean(baseline, random) / resampledMean(candidate, random);
  }
  return {
    estimate,
    lower95: quantile(ratios, 0.025),
    upper95: quantile(ratios, 0.975),
    bootstrapReplicates,
  };
}

const tasks: Task[] = [];
for (const workload of workloads) {
  for (const rows of sizes) {
    for (const mode of modes) {
      for (let replicate = 0; replicate < processReplicates; replicate += 1) {
        tasks.push({ workload, rows, mode, replicate, seed: baseSeed + replicate });
      }
    }
  }
}

const orderedTasks = shuffle(tasks, baseSeed);
const raw: ProcessResult[] = [];
for (let position = 0; position < orderedTasks.length; position += 1) {
  const task = orderedTasks[position]!;
  const started = Bun.nanoseconds();
  const child = Bun.spawnSync([
    "bun", "run", "paper/artifact/benchmark-worker.ts",
    "--workload", task.workload,
    "--mode", task.mode,
    "--rows", String(task.rows),
    "--iterations", String(iterations),
    "--warmups", String(warmups),
    "--seed", String(task.seed),
  ], { stdout: "pipe", stderr: "pipe" });
  if (child.exitCode !== 0) {
    throw new Error(`worker failed: ${child.stderr.toString()}`);
  }
  const parsed = JSON.parse(child.stdout.toString()) as ProcessResult;
  parsed.processWallMs = (Bun.nanoseconds() - started) / 1_000_000;
  raw.push(parsed);
  if ((position + 1) % Math.max(1, Math.floor(orderedTasks.length / 20)) === 0) {
    console.log(`fresh-process progress ${position + 1}/${orderedTasks.length}`);
  }
}

const cells: Record<string, unknown>[] = [];
for (const workload of workloads) {
  for (const rows of sizes) {
    const byMode = Object.fromEntries(modes.map((mode) => [
      mode,
      raw.filter((entry) => entry.workload === workload && entry.rows === rows && entry.mode === mode),
    ])) as Record<Mode, ProcessResult[]>;
    const digestBySeed: Record<string, string> = {};
    for (let replicate = 0; replicate < processReplicates; replicate += 1) {
      const seed = baseSeed + replicate;
      const digests = new Set(modes.map((mode) =>
        byMode[mode].find((entry) => entry.seed === seed)?.digest
      ));
      if (digests.size !== 1 || digests.has(undefined)) {
        throw new Error(`correctness digest mismatch for ${workload} at n=${rows}, seed=${seed}`);
      }
      digestBySeed[String(seed)] = [...digests][0]!;
    }
    const processMeans = Object.fromEntries(modes.map((mode) => [
      mode,
      byMode[mode].map((entry) => mean(entry.samplesMs)),
    ])) as Record<Mode, number[]>;
    cells.push({
      workload,
      rows,
      equivalent: true,
      digestBySeed,
      processReplicates,
      iterationsPerProcess: iterations,
      modes: Object.fromEntries(modes.map((mode) => [mode, {
        meanOfProcessMeansMs: mean(processMeans[mode]),
        medianOfProcessMeansMs: median(processMeans[mode]),
        processMeansMs: processMeans[mode],
      }])),
      speedupVsTypescript: {
        wasm: ratioInterval(byMode.typescript, byMode.wasm, baseSeed ^ rows),
        adaptive: ratioInterval(byMode.typescript, byMode.adaptive, (baseSeed ^ rows) + 1),
      },
    });
  }
}

const payload = {
  schemaVersion: "2.0.0",
  generatedAt: new Date().toISOString(),
  runtime: { bun: Bun.version, platform: platform(), arch: arch(), cpu: cpus()[0]?.model ?? "unknown" },
  design: {
    baseSeed,
    sizes,
    workloads,
    modes,
    processReplicates,
    warmupsPerProcess: warmups,
    iterationsPerProcess: iterations,
    taskOrder: "deterministically randomized across workload, scale, mode, and replicate",
    experimentalUnit: "fresh Bun process",
    primaryEffect: "ratio of iteration means, TypeScript divided by candidate",
    uncertainty: "95% hierarchical percentile bootstrap, resampling processes then iterations",
    correctness: "SHA-256 digest of fully materialized final output; all modes must match",
    construction: "excluded from operation samples; process wall and before/after construction memory retained in raw records",
    memoryCaveat: process.platform === "linux"
      ? "enclosing cgroup and runtime diagnostics only; fixed-limit isolated cgroups remain an external run"
      : "macOS runtime diagnostics are not used for cross-runtime memory claims; Linux cgroup v2 run required",
  },
  cells,
  raw,
};

mkdirSync("paper/data", { recursive: true });
writeFileSync(outputPath, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
console.log(`wrote ${raw.length} fresh-process observations to ${outputPath}`);
