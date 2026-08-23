import { afterAll, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { DataFrame } from "../src/index";
import { read_parquet, to_parquet } from "../src/io";

const dir = mkdtempSync(join(tmpdir(), "bun-panda-typed-"));
const path = join(dir, "typed.parquet");

afterAll(() => {
  rmSync(dir, { recursive: true, force: true });
});

describe("typed parquet ingest", () => {
  test("round-trips numeric/string/boolean columns with nulls", async () => {
    const df = new DataFrame([
      { id: 1, city: "Austin", score: 10.5, active: true },
      { id: 2, city: null, score: 8.25, active: false },
      { id: 3, city: "Denver", score: null, active: true },
    ]);

    await to_parquet(df, { path });
    const back = await read_parquet(path);

    expect(back.columns).toEqual(["id", "city", "score", "active"]);
    expect(back.shape).toEqual([3, 4]);
    expect(back.sort_values("id").to_records()).toEqual([
      { id: 1, city: "Austin", score: 10.5, active: true },
      { id: 2, city: null, score: 8.25, active: false },
      { id: 3, city: "Denver", score: null, active: true },
    ]);
  });

  test("typed ingest feeds wasm groupby with identical results to TS path", async () => {
    const rows = Array.from({ length: 2000 }, (_, i) => ({
      g: ["a", "b", "c"][i % 3],
      x: i % 17 === 0 ? null : i,
    }));
    await to_parquet(new DataFrame(rows), { path });

    const ingested = await read_parquet(path);
    const direct = new DataFrame(rows);

    const fromParquet = ingested.groupby("g").agg({ x: "sum", m: "mean" }).sort_values("g").to_records();
    const fromRows = direct.groupby("g").agg({ x: "sum", m: "mean" }).sort_values("g").to_records();

    expect(fromParquet).toEqual(fromRows);
  });

  test("index_col selection works through the typed path", async () => {
    const df = new DataFrame([
      { key: "x", v: 1 },
      { key: "y", v: 2 },
    ]);
    await to_parquet(df, { path });

    const indexed = await read_parquet(path, { index_col: "key" });
    expect(indexed.index).toEqual(["x", "y"]);
  });
});
