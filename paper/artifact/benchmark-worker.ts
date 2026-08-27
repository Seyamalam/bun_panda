import { readFileSync } from "node:fs";
import { DataFrame } from "../../index";
import type { CellValue } from "../../src/types";

type Workload = "groupby_fused_4" | "sort_numeric_full" | "sort_numeric_top1000" | "filter_boolean_mask";
type Mode = "typescript" | "wasm" | "adaptive";

function argument(name: string, fallback: string): string {
  const position = process.argv.indexOf(name);
  return position >= 0 ? process.argv[position + 1] ?? fallback : fallback;
}

const workload = argument("--workload", "groupby_fused_4") as Workload;
const mode = argument("--mode", "adaptive") as Mode;
const rows = Number(argument("--rows", "10000"));
const iterations = Number(argument("--iterations", "20"));
const warmups = Number(argument("--warmups", "5"));
const seed = Number(argument("--seed", "42"));

if (mode === "typescript") process.env.BUN_PANDA_WASM = "0";
else if (mode === "wasm") process.env.BUN_PANDA_WASM = "1";
else delete process.env.BUN_PANDA_WASM;

function lcg(initialSeed: number): () => number {
  let state = initialSeed >>> 0;
  return () => {
    state = (Math.imul(1664525, state) + 1013904223) >>> 0;
    return state / 2 ** 32;
  };
}

function buildFrame(rowCount: number): { frame: DataFrame; mask: boolean[] } {
  const random = lcg(seed);
  const id = new Float64Array(rowCount);
  const value = new Float64Array(rowCount);
  const weight = new Float64Array(rowCount);
  const revenue = new Float64Array(rowCount);
  const group = new Array<CellValue>(rowCount);
  const region = new Array<CellValue>(rowCount);
  const active = new Array<CellValue>(rowCount);
  const mask = new Array<boolean>(rowCount);
  for (let i = 0; i < rowCount; i += 1) {
    const nextValue = Math.floor(random() * 1000);
    const nextWeight = Math.round((random() * 5 + 0.5) * 100) / 100;
    id[i] = i;
    value[i] = nextValue;
    weight[i] = nextWeight;
    revenue[i] = Math.round(nextValue * nextWeight * 100) / 100;
    group[i] = `g${i % 64}`;
    region[i] = `r${i % 11}`;
    active[i] = i % 3 === 0;
    mask[i] = i % 3 === 0 && nextValue >= 500;
  }
  return {
    frame: DataFrame.from_typed({ id, group, region, value, weight, revenue, active }),
    mask,
  };
}

function cgroupValue(name: string): number | null {
  if (process.platform !== "linux") return null;
  try {
    const value = readFileSync(`/sys/fs/cgroup/${name}`, "utf8").trim();
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  } catch {
    return null;
  }
}

function outputDigest(value: unknown): string {
  const canonical = value instanceof DataFrame
    ? JSON.stringify({ columns: value.columns, index: value.index, rows: value.to_records() })
    : JSON.stringify(value);
  return new Bun.CryptoHasher("sha256").update(canonical).digest("hex");
}

const memoryBeforeConstruction = process.memoryUsage();
const cgroupBeforeConstruction = cgroupValue("memory.current");
const built = buildFrame(rows);
const memoryAfterConstruction = process.memoryUsage();
const cgroupAfterConstruction = cgroupValue("memory.current");

function operation(): DataFrame {
  if (workload === "groupby_fused_4") {
    return built.frame.groupby("group").agg({
      value: "mean",
      revenue: "sum",
      weight: "max",
      id: "count",
    });
  }
  if (workload === "sort_numeric_full") return built.frame.sort_values("revenue", false);
  if (workload === "sort_numeric_top1000") return built.frame.sort_values("revenue", false, 1_000);
  return built.frame.filter(built.mask);
}

let output: DataFrame | null = null;
for (let i = 0; i < warmups; i += 1) output = operation();

const samplesMs: number[] = [];
let observedRssPeak = process.memoryUsage().rss;
for (let i = 0; i < iterations; i += 1) {
  const start = Bun.nanoseconds();
  output = operation();
  samplesMs.push((Bun.nanoseconds() - start) / 1_000_000);
  observedRssPeak = Math.max(observedRssPeak, process.memoryUsage().rss);
}
if (!output) throw new Error("benchmark produced no output");

const usage = process.resourceUsage();
const payload = {
  schemaVersion: "1.0.0",
  workload,
  mode,
  rows,
  seed,
  warmups,
  iterations,
  samplesMs,
  digest: outputDigest(output),
  outputShape: output.shape,
  memory: {
    rssBeforeConstruction: memoryBeforeConstruction.rss,
    rssAfterConstruction: memoryAfterConstruction.rss,
    observedRssPeak,
    maxRssKilobytes: usage.maxRSS,
    heapUsedAfter: process.memoryUsage().heapUsed,
    cgroupCurrentBeforeConstruction: cgroupBeforeConstruction,
    cgroupCurrentAfterConstruction: cgroupAfterConstruction,
    cgroupPeak: cgroupValue("memory.peak"),
    scope: process.platform === "linux"
      ? "enclosing cgroup diagnostics; isolated fixed-limit study still required"
      : "runtime diagnostics only; comparable cgroup study requires Linux",
  },
};

process.stdout.write(`${JSON.stringify(payload)}\n`);
