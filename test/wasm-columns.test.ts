import { afterEach, describe, expect, test } from "bun:test";
import { DataFrame } from "../src/index";
import {
  buildColumnStore,
  hasCachedColumns,
  invalidateColumnStore,
  primeNumericColumns,
} from "../src/wasm/columns";

const priorMode = process.env.BUN_PANDA_WASM;

afterEach(() => {
  if (priorMode === undefined) delete process.env.BUN_PANDA_WASM;
  else process.env.BUN_PANDA_WASM = priorMode;
});

describe("typed-column cache", () => {
  test("reuses materialized columns for one row snapshot", () => {
    const rows = [
      { a: 1, b: "x" },
      { a: 2, b: "y" },
    ];
    expect(hasCachedColumns(rows, ["a"])).toBe(false);
    const first = buildColumnStore(rows, ["a"]);
    const firstColumn = first.columns.get("a");
    expect(hasCachedColumns(rows, ["a"])).toBe(true);
    const second = buildColumnStore(rows, ["a", "b"]);
    expect(second).toBe(first);
    expect(second.columns.get("a")).toBe(firstColumn);
  });

  test("explicit invalidation rebuilds a mutated row snapshot", () => {
    const rows = [{ a: 1 }, { a: 2 }];
    const first = buildColumnStore(rows, ["a"]).columns.get("a");
    rows[0]!.a = 10;
    invalidateColumnStore(rows);
    const second = buildColumnStore(rows, ["a"]).columns.get("a");
    expect(second).not.toBe(first);
    expect(second?.kind).toBe("f64");
    if (second?.kind === "f64") expect(second.values[0]).toBe(10);
  });

  test("typed ingest primes an isolated numeric copy", () => {
    const rows = [{ a: 1 }, { a: 2 }];
    const source = new Float64Array([1, 2]);
    primeNumericColumns(rows, { a: source });
    source[0] = 99;
    const column = buildColumnStore(rows, ["a"]).columns.get("a");
    expect(column?.kind).toBe("f64");
    if (column?.kind === "f64") expect(column.values[0]).toBe(1);
  });

  test("DataFrame.update invalidates columns used by forced Wasm sort", () => {
    process.env.BUN_PANDA_WASM = "1";
    const frame = new DataFrame([
      { id: 0, value: 2 },
      { id: 1, value: 1 },
    ], { index: [0, 1] });
    expect(frame.sort_values("value").to_records().map((row) => row.id)).toEqual([1, 0]);

    frame.update(new DataFrame([{ value: 0 }], { index: [0] }));
    expect(frame.sort_values("value").to_records().map((row) => row.id)).toEqual([0, 1]);
  });
});
