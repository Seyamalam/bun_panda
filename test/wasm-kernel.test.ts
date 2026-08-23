import { describe, expect, test } from "bun:test";
import { DataFrame } from "../src/index";
import {
  wasmAggregateColumn,
  wasmGroupIds,
  wasmKernel,
} from "../src/wasm/kernel";

describe("wasm kernel", () => {
  test("kernel loads or degrades gracefully", () => {
    const kernel = wasmKernel();
    if (kernel) {
      expect(kernel.memory).toBeInstanceOf(WebAssembly.Memory);
      expect(typeof kernel.bp_agg_f64).toBe("function");
    }
  });

  test("group ids match TS grouping on multi-key frames", () => {
    if (!wasmKernel()) {
      return;
    }
    const rows = [
      { a: "x", b: 1, v: 10 },
      { a: "x", b: 1, v: 20 },
      { a: "x", b: 2, v: 30 },
      { a: "y", b: 1, v: 40 },
      { a: "y", b: 1, v: 50 },
    ];
    const result = wasmGroupIds(rows, ["a", "b"]);
    expect(result).not.toBeNull();
    expect(result!.groupCount).toBe(3);
    expect(Array.from(result!.ids)).toEqual([0, 0, 1, 2, 2]);
  });

  test("missing keys are excluded like pandas dropna", () => {
    if (!wasmKernel()) {
      return;
    }
    const rows = [
      { k: "a", v: 1 },
      { k: null, v: 2 },
      { k: "a", v: 3 },
    ];
    const result = wasmGroupIds(rows, ["k"]);
    expect(result!.groupCount).toBe(1);
    expect(Array.from(result!.ids)).toEqual([0, -1, 0]);
  });

  test("aggregate codes match TS finalize semantics with nulls", () => {
    if (!wasmKernel()) {
      return;
    }
    const rows = [
      { g: "a", v: 1 },
      { g: "a", v: null },
      { g: "a", v: 3 },
      { g: "b", v: null },
    ];
    const ids = wasmGroupIds(rows, ["g"]);
    expect(ids!.groupCount).toBe(2);
    const idArray = ids!.ids;

    const sum = wasmAggregateColumn(rows, "v", idArray, 2, 0)!;
    expect(sum[0]).toBe(4);
    expect(Number.isNaN(sum[1])).toBe(true); // empty group -> NaN -> null in caller

    const mean = wasmAggregateColumn(rows, "v", idArray, 2, 1)!;
    expect(mean[0]).toBe(2);

    const count = wasmAggregateColumn(rows, "v", idArray, 2, 4)!;
    expect(count[0]).toBe(2);
    expect(count[1]).toBe(0);

    const min = wasmAggregateColumn(rows, "v", idArray, 2, 2)!;
    expect(min[0]).toBe(1);
    const max = wasmAggregateColumn(rows, "v", idArray, 2, 3)!;
    expect(max[0]).toBe(3);
  });

  test("groupby agg results identical with and without wasm", () => {
    const rows = Array.from({ length: 300 }, (_, i) => ({
      g: ["p", "q", "r"][i % 3],
      v: i % 11 === 0 ? null : i * 1.5,
    }));
    const df = new DataFrame(rows);
    const specs = [
      { v: "sum" as const },
      { v: "mean" as const },
      { v: "min" as const },
      { v: "max" as const },
      { v: "count" as const },
    ];
    for (const spec of specs) {
      // dropna=false forces the pure-TS path; dropna=true may take wasm.
      const tsRows = df
        .groupby("g", { dropna: false })
        .agg(spec)
        .to_records()
        .filter((row) => row.g !== null);
      const fastRows = df.groupby("g", { dropna: true }).agg(spec).to_records();
      expect(fastRows.length).toBe(tsRows.length);
      for (let i = 0; i < tsRows.length; i += 1) {
        expect(fastRows[i]!.g).toBe(tsRows[i]!.g);
        const fast = fastRows[i]!.v;
        const slow = tsRows[i]!.v;
        if (typeof fast === "number" && typeof slow === "number") {
          expect(Math.abs(fast - slow)).toBeLessThan(1e-9);
        } else {
          expect(fast ?? null).toBe(slow ?? null);
        }
      }
    }
  });
});
