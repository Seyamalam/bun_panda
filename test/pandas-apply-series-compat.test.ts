import { describe, expect, test } from "bun:test";
import { DataFrame, Series } from "../index";

describe("dataframe apply/map compatibility", () => {
  test("apply defaults to axis=0 and passes each column as Series", () => {
    const df = new DataFrame([
      { a: 1, b: 10 },
      { a: 2, b: 20 },
      { a: 3, b: 30 },
    ]);

    const out = df.apply((column) => column.sum() ?? 0);
    expect(out.index).toEqual(["a", "b"]);
    expect(out.to_list()).toEqual([6, 60]);
  });

  test("apply axis=1 passes each row as Series", () => {
    const df = new DataFrame([
      { a: 1, b: 10 },
      { a: 2, b: 20 },
      { a: 3, b: 30 },
    ]);

    const out = df.apply((row) => row.sum() ?? 0, 1);
    expect(out.index).toEqual([0, 1, 2]);
    expect(out.to_list()).toEqual([11, 22, 33]);
  });

  test("apply supports string axis aliases", () => {
    const df = new DataFrame([
      { a: 1, b: 10 },
      { a: 2, b: 20 },
    ]);

    const out = df.apply((row) => row.max() ?? null, "columns");
    expect(out.to_list()).toEqual([10, 20]);
  });

  test("applymap and map transform every cell", () => {
    const df = new DataFrame([
      { city: "Austin", score: 10 },
      { city: "Seattle", score: 20 },
    ]);

    const viaApplyMap = df.applymap((value) =>
      typeof value === "number" ? value * 2 : value
    );
    const viaMap = df.map((value) =>
      typeof value === "number" ? value + 1 : value
    );

    expect(viaApplyMap.to_records()).toEqual([
      { city: "Austin", score: 20 },
      { city: "Seattle", score: 40 },
    ]);
    expect(viaMap.to_records()).toEqual([
      { city: "Austin", score: 11 },
      { city: "Seattle", score: 21 },
    ]);
  });
});

describe("series compatibility helpers", () => {
  test("isin matches pandas-style membership checks", () => {
    const series = new Series([1, 2, null, 4], { name: "value" });
    expect(series.isin([2, null]).to_list()).toEqual([
      false,
      true,
      true,
      false,
    ]);
  });

  test("clip applies numeric bounds and keeps non-numeric values", () => {
    const series = new Series([1, 5, null, "x"]);
    expect(series.clip(2, 4).to_list()).toEqual([2, 4, null, "x"]);
  });

  test("replace supports scalar, array, and mapping inputs", () => {
    const cities = new Series(["Austin", "Seattle", "Austin"]);
    expect(cities.replace("Austin", "ATX").to_list()).toEqual([
      "ATX",
      "Seattle",
      "ATX",
    ]);
    expect(cities.replace(["Austin", "Seattle"], "X").to_list()).toEqual([
      "X",
      "X",
      "X",
    ]);
    expect(cities.replace({ Austin: "ATX", Seattle: "SEA" }).to_list()).toEqual([
      "ATX",
      "SEA",
      "ATX",
    ]);
  });

  test("replace array input without replacement value throws", () => {
    const series = new Series([1, 2, 3]);
    expect(() => series.replace([1, 2])).toThrow(
      "replace with an array requires a replacement value."
    );
  });
});
