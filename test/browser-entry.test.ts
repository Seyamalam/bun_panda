import { describe, expect, test } from "bun:test";
import { BROWSER_AGG_MEAN, createBrowserKernel } from "../src/browser";

describe("browser-safe Wasm entry", () => {
  test("loads asynchronously from bytes and exposes typed kernels", async () => {
    const file = Bun.file(new URL("../src/wasm/bun_panda_core.wasm", import.meta.url));
    const kernel = await createBrowserKernel({ bytes: await file.arrayBuffer() });

    expect([...kernel.stableArgsort(Float64Array.of(3, 1, Number.NaN, 2))]).toEqual([1, 3, 0, 2]);
    expect([...kernel.filterIndices([true, false, true, false])]).toEqual([0, 2]);
    expect([...kernel.aggregate(
      Float64Array.of(2, 4, 10, Number.NaN),
      Int32Array.of(0, 0, 1, 1),
      2,
      BROWSER_AGG_MEAN,
    )]).toEqual([3, 10]);
  });
});
