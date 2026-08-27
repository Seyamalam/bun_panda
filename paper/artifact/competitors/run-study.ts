import { mkdirSync, writeFileSync } from "node:fs";
import { arch, cpus, platform } from "node:os";
import type { DatasetSource, Scope, SystemName, Workload } from "./dataset";

interface Task {
  system: SystemName;
  workload: Workload;
  scope: Scope;
  rows: number;
  replicate: number;
  seed: number;
}

interface Observation extends Omit<Task, "replicate"> {
  systemVersion: string;
  warmups: number;
  iterations: number;
  samplesMs: number[];
  digest: string;
  outputRows: number;
  processWallMs: number;
  memory: Record<string, number>;
}

const systems = (process.env.BUN_PANDA_COMPETITOR_SYSTEMS ??
  "bun_panda,arquero,danfojs,nodejs_polars,duckdb_wasm").split(",") as SystemName[];
const workloads = (process.env.BUN_PANDA_COMPETITOR_WORKLOADS ??
  "groupby_sum,filter_sort_top100,value_counts,inner_join").split(",") as Workload[];
const scopes = (process.env.BUN_PANDA_COMPETITOR_SCOPES ??
  "operation,load_and_operation").split(",") as Scope[];
const sizes = (process.env.BUN_PANDA_COMPETITOR_SIZES ?? "10000,50000").split(",").map(Number);
const processReplicates = Number(process.env.BUN_PANDA_COMPETITOR_PROCESSES ?? "5");
const warmups = Number(process.env.BUN_PANDA_COMPETITOR_WARMUPS ?? "3");
const iterations = Number(process.env.BUN_PANDA_COMPETITOR_ITERATIONS ?? "10");
const outputPath = process.env.BUN_PANDA_COMPETITOR_OUTPUT ?? "paper/data/competitor-study.json";
const datasetSource = (process.env.BUN_PANDA_COMPETITOR_DATASET ?? "synthetic") as DatasetSource;
const datasetPath = process.env.BUN_PANDA_COMPETITOR_DATASET_PATH ?? "paper/data/workloads/uci-bank/bank-full.csv";
const baseSeed = 20260826;

function lcg(initialSeed: number): () => number {
  let state = initialSeed >>> 0;
  return () => {
    state = (Math.imul(1664525, state) + 1013904223) >>> 0;
    return state / 2 ** 32;
  };
}

function shuffle<T>(values: T[], seed: number): T[] {
  const output = [...values];
  const random = lcg(seed);
  for (let index = output.length - 1; index > 0; index -= 1) {
    const target = Math.floor(random() * (index + 1));
    [output[index], output[target]] = [output[target]!, output[index]!];
  }
  return output;
}

function mean(values: number[]): number {
  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function median(values: number[]): number {
  const sorted = [...values].sort((left, right) => left - right);
  const middle = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 0 ? (sorted[middle - 1]! + sorted[middle]!) / 2 : sorted[middle]!;
}

const tasks: Task[] = [];
for (const rows of sizes) {
  for (const workload of workloads) {
    for (const scope of scopes) {
      for (const system of systems) {
        for (let replicate = 0; replicate < processReplicates; replicate += 1) {
          tasks.push({ system, workload, scope, rows, replicate, seed: baseSeed + replicate });
        }
      }
    }
  }
}

const ordered = shuffle(tasks, baseSeed);
const raw: Observation[] = [];
for (let position = 0; position < ordered.length; position += 1) {
  const task = ordered[position]!;
  const started = Bun.nanoseconds();
  const child = Bun.spawnSync([
    "bun", "run", "paper/artifact/competitors/worker.ts",
    "--system", task.system,
    "--workload", task.workload,
    "--scope", task.scope,
    "--rows", String(task.rows),
    "--seed", String(task.seed),
    "--warmups", String(warmups),
    "--iterations", String(iterations),
    "--dataset", datasetSource,
    "--dataset-path", datasetPath,
  ], { stdout: "pipe", stderr: "pipe" });
  if (child.exitCode !== 0) {
    throw new Error(`${task.system}/${task.workload}/${task.scope} failed:\n${child.stderr.toString()}`);
  }
  const observation = JSON.parse(child.stdout.toString()) as Observation;
  observation.processWallMs = (Bun.nanoseconds() - started) / 1_000_000;
  raw.push(observation);
  if ((position + 1) % Math.max(1, Math.floor(ordered.length / 20)) === 0) {
    console.log(`competitor progress ${position + 1}/${ordered.length}`);
  }
}

const cells: Record<string, unknown>[] = [];
for (const rows of sizes) {
  for (const workload of workloads) {
    for (const scope of scopes) {
      const observations = raw.filter((entry) =>
        entry.rows === rows && entry.workload === workload && entry.scope === scope
      );
      const digestsBySeed: Record<string, string> = {};
      for (let replicate = 0; replicate < processReplicates; replicate += 1) {
        const seed = baseSeed + replicate;
        const digests = new Set(observations.filter((entry) => entry.seed === seed).map((entry) => entry.digest));
        if (digests.size !== 1) throw new Error(`correctness mismatch for ${workload}, ${scope}, n=${rows}, seed=${seed}`);
        digestsBySeed[String(seed)] = [...digests][0]!;
      }
      cells.push({
        rows,
        workload,
        scope,
        equivalent: true,
        processReplicates,
        iterationsPerProcess: iterations,
        digestsBySeed,
        systems: Object.fromEntries(systems.map((system) => {
          const selected = observations.filter((entry) => entry.system === system);
          const processMeans = selected.map((entry) => mean(entry.samplesMs));
          return [system, {
            version: selected[0]?.systemVersion,
            meanOfProcessMeansMs: mean(processMeans),
            medianOfProcessMeansMs: median(processMeans),
            processMeansMs: processMeans,
            medianPeakRssBytes: median(selected.map((entry) => entry.memory.observedRssPeak)),
          }];
        })),
      });
    }
  }
}

const payload = {
  schemaVersion: "1.0.0",
  generatedAt: new Date().toISOString(),
  runtime: { bun: Bun.version, platform: platform(), arch: arch(), cpu: cpus()[0]?.model ?? "unknown" },
  design: {
    systems,
    datasetSource,
    datasetPath: datasetSource === "uci_bank" ? datasetPath : null,
    workloads,
    scopes,
    sizes,
    processReplicates,
    warmupsPerProcess: warmups,
    iterationsPerProcess: iterations,
    baseSeed,
    taskOrder: "deterministically randomized",
    experimentalUnit: "fresh Bun process",
    correctness: "all systems must match a pure TypeScript reference SHA-256 digest for each seed",
    operationScope: "data structures are built before timing; output materialization is timed",
    loadAndOperationScope: "dataset loading and operation are timed; module import and engine initialization are excluded",
    memory: "peak process RSS is descriptive because in-process allocators and garbage collectors differ",
  },
  cells,
  raw,
};

mkdirSync("paper/data", { recursive: true });
writeFileSync(outputPath, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
console.log(`wrote ${raw.length} observations to ${outputPath}`);
