import { describe, expect, test } from "bun:test";
import { DataFrame } from "../src/index";
import { shouldTryWasm } from "../src/internal/groupby/fastAgg";

describe("GroupBy fastAgg (split module)", () => {
  test("shouldTryWasm delegates to fastAgg (pure-TS fallback when BUN_PANDA_WASM=0)", () => {
    const prev = process.env.BUN_PANDA_WASM;
    try {
      process.env.BUN_PANDA_WASM = "0";
      expect(shouldTryWasm(true, 100_000, 1)).toBe(false);
      process.env.BUN_PANDA_WASM = "1";
      expect(shouldTryWasm(true, 100_000, 1)).toBe(true);
      delete process.env.BUN_PANDA_WASM;
      expect(shouldTryWasm(true, 100_000, 1)).toBe(false);
      expect(shouldTryWasm(false, 100_000, 1)).toBe(false);
    } finally {
      if (prev === undefined) delete process.env.BUN_PANDA_WASM;
      else process.env.BUN_PANDA_WASM = prev;
    }
  });

  test("fastAgg helpers agree with GroupBy results (numeric mean)", () => {
    const frame = new DataFrame([
      { g: "a", x: 1 },
      { g: "a", x: 3 },
      { g: "b", x: 10 },
    ]);
    const out = frame.groupby("g").agg({ x: "mean" }).sort_values("g");
    expect(out.to_records()).toEqual([
      { g: "a", x: 2 },
      { g: "b", x: 10 },
    ]);
  });
});
