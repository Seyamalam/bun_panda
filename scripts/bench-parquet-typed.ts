/**
 * Benchmark: typed columnar parquet ingest vs row-major materialization.
 *
 * The typed path skips per-cell fromPrimitive dispatch inside
 * buildTypedColumns by reading raw decoded values directly; the
 * end-to-end win depends on how much of read time is decode vs
 * row-materialization, so we measure both stages:
 *   - stage 1: raw row-group decode (shared by both paths)
 *   - stage 2: record assembly (typedColumnsToRecords vs cursor.next())
 */
import { DataFrame } from "../src/index";
import { read_parquet, to_parquet } from "../src/io";

const ROWS = Number(process.env.BUN_PANDA_BENCH_ROWS ?? 50000);
const ITERS = Number(process.env.BUN_PANDA_ITERS ?? 5);
const PATH = "/tmp/bun_panda_typed_bench.parquet";

function bench(label: string, fn: () => Promise<unknown>, iters = ITERS) {
  return (async () => {
    await fn();
    const times: number[] = [];
    for (let i = 0; i < iters; i += 1) {
      const start = performance.now();
      await fn();
      times.push(performance.now() - start);
    }
    const avg = times.reduce((a, b) => a + b, 0) / times.length;
    console.log(`${label}: ${avg.toFixed(2)}ms avg (${ROWS} rows)`);
    return avg;
  })();
}

const rows = Array.from({ length: ROWS }, (_, i) => ({
  id: i,
  group: ["A", "B", "C", "D", "E"][i % 5],
  value: (i * 7919) % 100000,
  revenue: ((i * 7919) % 100000) * 1.37,
  active: i % 3 === 0,
}));

await to_parquet(new DataFrame(rows), { path: PATH });

// Stage A: full typed pipeline (read_parquet).
const typedRead = () => read_parquet(PATH);

// Stage B: old per-row cursor loop.
async function rowMajorRead() {
  const parquet = await import("parquetjs-lite");
  const reader = await parquet.ParquetReader.openFile(PATH);
  try {
    const records: unknown[] = [];
    const cursor = reader.getCursor();
    for (;;) {
      const next = await cursor.next();
      if (!next) break;
      records.push(next);
    }
    return records;
  } finally {
    await reader.close();
  }
}

console.log(`--- parquet ingest (${ITERS} iters each) ---`);
const rowMs = await bench("row-major (old)", rowMajorRead);
const typedMs = await bench("read_parquet (typed)", typedRead);

// Downstream: agg on the ingested frame.
const aggTyped = async () => {
  const df = await read_parquet(PATH);
  return df.groupby("group").agg({ value: "sum" }).shape[0];
};
await bench("read_parquet + groupby agg", aggTyped);

console.log(`ratio typed/row-major: ${(typedMs / rowMs).toFixed(2)}x`);

// Correctness: typed read must match source rows exactly.
const typed = await read_parquet(PATH);
const t = typed.sort_values("id").to_records();
if (JSON.stringify(t[0]) !== JSON.stringify(rows[0]) || typed.shape[0] !== ROWS) {
  console.log("MISMATCH:", JSON.stringify(t[0]), "vs", JSON.stringify(rows[0]));
  process.exit(1);
}
console.log("round-trip parity OK");
