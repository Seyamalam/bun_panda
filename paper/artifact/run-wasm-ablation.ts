import { mkdirSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";
import { DataFrame } from "../../index";
import type { Row } from "../../src/types";

const SIZES = [10_000, 25_000, 100_000, 250_000];
const WARMUPS = 5;
const ROUNDS = 5;
const ITERATIONS = 8;
const OUTPUT = "paper/data/wasm-ablation.json";

interface Summary {
  medianMs: number;
  meanMs: number;
  stddevMs: number;
  p05Ms: number;
  p95Ms: number;
  samplesMs: number[];
}

function lcg(seed: number): () => number {
  let state = seed >>> 0;
  return () => {
    state = (1664525 * state + 1013904223) >>> 0;
    return state / 2 ** 32;
  };
}

function buildRows(size: number): Row[] {
  const random = lcg(42);
  return Array.from({ length: size }, (_, id) => {
    const value = Math.floor(random() * 1000);
    const weight = Number((random() * 5 + 0.5).toFixed(2));
    return {
      id,
      group: `g${id % 64}`,
      region: `r${id % 11}`,
      value,
      weight,
      revenue: Number((value * weight).toFixed(2)),
      active: id % 3 === 0,
    };
  });
}

function quantile(sorted: number[], q: number): number {
  if (sorted.length === 0) return 0;
  const index = (sorted.length - 1) * q;
  const lower = Math.floor(index);
  const upper = Math.ceil(index);
  if (lower === upper) return sorted[lower]!;
  const fraction = index - lower;
  return sorted[lower]! * (1 - fraction) + sorted[upper]! * fraction;
}

function summarize(samples: number[]): Summary {
  const sorted = [...samples].sort((a, b) => a - b);
  const mean = samples.reduce((sum, value) => sum + value, 0) / samples.length;
  const variance =
    samples.reduce((sum, value) => sum + (value - mean) ** 2, 0) /
    Math.max(1, samples.length - 1);
  return {
    medianMs: quantile(sorted, 0.5),
    meanMs: mean,
    stddevMs: Math.sqrt(variance),
    p05Ms: quantile(sorted, 0.05),
    p95Ms: quantile(sorted, 0.95),
    samplesMs: samples,
  };
}

function canonical(value: unknown): string {
  if (value instanceof DataFrame) return JSON.stringify(value.to_records());
  return JSON.stringify(value);
}

function runWithMode<T>(wasm: boolean, fn: () => T): T {
  process.env.BUN_PANDA_WASM = wasm ? "1" : "0";
  return fn();
}

function measure(fn: () => unknown): Summary {
  for (let i = 0; i < WARMUPS; i += 1) fn();
  const samples: number[] = [];
  for (let round = 0; round < ROUNDS; round += 1) {
    for (let iteration = 0; iteration < ITERATIONS; iteration += 1) {
      const start = Bun.nanoseconds();
      const output = fn();
      if (output === undefined || output === null) {
        throw new Error("Benchmark returned no output.");
      }
      samples.push((Bun.nanoseconds() - start) / 1_000_000);
    }
  }
  return summarize(samples);
}

const results: Record<string, unknown>[] = [];

for (const size of SIZES) {
  const rows = buildRows(size);
  const frame = new DataFrame(rows);
  const mask = rows.map((row) => row.active === true && Number(row.value) >= 500);
  const cases = [
    {
      name: "groupby_fused_4",
      run: () =>
        frame.groupby("group").agg({
          value: "mean",
          revenue: "sum",
          weight: "max",
          id: "count",
        }),
    },
    {
      name: "sort_numeric_full",
      run: () => frame.sort_values("revenue", false),
    },
    {
      name: "sort_numeric_top1000",
      run: () => frame.sort_values("revenue", false, 1_000),
    },
    {
      name: "filter_boolean_mask",
      run: () => frame.filter(mask),
    },
  ];

  for (const benchmark of cases) {
    const tsOutput = runWithMode(false, benchmark.run);
    const wasmOutput = runWithMode(true, benchmark.run);
    if (canonical(tsOutput) !== canonical(wasmOutput)) {
      throw new Error(`WASM/TypeScript mismatch for ${benchmark.name} at n=${size}.`);
    }

    // Alternate the order by row count to reduce systematic thermal/order bias.
    const wasmFirst = SIZES.indexOf(size) % 2 === 0;
    const firstMode = wasmFirst ? "wasm" : "typescript";
    const first = measure(() => runWithMode(wasmFirst, benchmark.run));
    const second = measure(() => runWithMode(!wasmFirst, benchmark.run));
    const wasm = wasmFirst ? first : second;
    const typescript = wasmFirst ? second : first;

    results.push({
      case: benchmark.name,
      rows: size,
      firstMode,
      samplesPerMode: ROUNDS * ITERATIONS,
      wasm,
      typescript,
      speedup: typescript.medianMs / wasm.medianMs,
      equivalent: true,
    });
    console.log(
      `${benchmark.name.padEnd(24)} n=${String(size).padStart(6)} ` +
        `wasm=${wasm.medianMs.toFixed(3)}ms ts=${typescript.medianMs.toFixed(3)}ms ` +
        `speedup=${(typescript.medianMs / wasm.medianMs).toFixed(2)}x`
    );
  }
}

const payload = {
  schemaVersion: 1,
  generatedAt: new Date().toISOString(),
  runtime: {
    bun: Bun.version,
    platform: process.platform,
    arch: process.arch,
  },
  design: {
    seed: 42,
    sizes: SIZES,
    warmups: WARMUPS,
    rounds: ROUNDS,
    iterationsPerRound: ITERATIONS,
    statistic: "median of 40 individual wall-clock samples",
    clock: "Bun.nanoseconds",
    constructionExcluded: true,
    correctnessCheck: "canonical row equality before timing",
  },
  results,
};

mkdirSync(dirname(OUTPUT), { recursive: true });
writeFileSync(OUTPUT, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
