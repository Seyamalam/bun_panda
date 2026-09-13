import { describe, expect, test } from "bun:test";
import {
  byteLength,
  createDataset,
  filterIndices,
  gather,
  groupedSum,
  quantile,
  stableArgsort,
  summarize,
} from "../src/benchmark-core";
import type { ExperimentConfig } from "../src/types";

const config: ExperimentConfig = {
  rows: 128,
  groups: 8,
  selectivity: 0.4,
  iterations: 5,
  warmups: 1,
  seed: 42,
  shape: "uniform",
};

describe("deterministic benchmark core", () => {
  test("repeats every generated typed column for a seed", () => {
    const left = createDataset(config);
    const right = createDataset(config);
    expect([...left.values]).toEqual([...right.values]);
    expect([...left.mask]).toEqual([...right.mask]);
    expect([...left.groupIds]).toEqual([...right.groupIds]);
    expect(byteLength(left)).toBe(config.rows * (8 + 1 + 4));
  });

  test("changes distribution without violating group bounds", () => {
    const skewed = createDataset({ ...config, shape: "skewed" });
    expect([...skewed.groupIds].every((id) => id >= 0 && id < config.groups)).toBe(true);
    expect([...skewed.values].every((value) => value >= 0 && value < 10_000)).toBe(true);
  });

  test("keeps sort ties stable", () => {
    expect([...stableArgsort(Float64Array.of(4, 1, 4, 2, 1))]).toEqual([1, 4, 3, 0, 2]);
  });

  test("compacts and gathers selected positions", () => {
    const indices = filterIndices(Uint8Array.of(0, 1, 0, 1));
    expect([...indices]).toEqual([1, 3]);
    expect([...gather(Float64Array.of(10, 20, 30, 40), indices)]).toEqual([20, 40]);
  });

  test("reduces grouped values", () => {
    expect([...groupedSum(Float64Array.of(3, 5, 7, 11), Int32Array.of(0, 1, 0, 1), 2)]).toEqual([10, 16]);
  });

  test("reports interpolated quantiles", () => {
    expect(quantile([4, 1, 3, 2], 0.5)).toBe(2.5);
    expect(summarize([1, 2, 3]).medianMs).toBe(2);
    expect(summarize([]).p95Ms).toBe(0);
  });
});
