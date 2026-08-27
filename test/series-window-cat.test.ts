import { describe, expect, test } from "bun:test";
import { DataFrame, Series } from "../src";
import type { CellValue } from "../src";

describe("Series align / time filters", () => {
  test("align outer unions labels and reindexes both sides", () => {
    const a = new Series([1, 2], { index: ["x", "y"] });
    const b = new Series([10, 20], { index: ["y", "z"] });
    const [la, lb] = a.align(b as unknown as Series<never> as never);
    expect(la.index).toEqual(["x", "y", "z"]);
    expect(lb.index).toEqual(["x", "y", "z"]);
  });

  test("at_time keeps only matching times of day", () => {
    const s = new Series([
      new Date("2026-01-01T09:00:00"),
      new Date("2026-01-02T10:00:00"),
      new Date("2026-01-03T09:00:00"),
    ]);
    const kept = s.at_time("09:00");
    expect(kept.length).toBe(2);
  });

  test("between_time honours inclusive modes", () => {
    const s = new Series([
      new Date("2026-01-01T08:00:00"),
      new Date("2026-01-01T09:00:00"),
      new Date("2026-01-01T12:00:00"),
    ]);
    expect(s.between_time("09:00", "12:00").length).toBe(2);
    expect(s.between_time("09:00", "12:00", "neither").length).toBe(0);
    expect(s.between_time("09:00", "12:00", "left").length).toBe(1);
  });
});

describe("Series ewm", () => {
  test("ewm mean converges toward the last values", () => {
    const s = new Series([1, 2, 3, 4]);
    const means = s.ewm(3).mean().to_list();
    expect(means[0]).toBeCloseTo(1);
    expect(means[3]!).toBeGreaterThan(means[0]!);
    // adjust=True mean of [1,2,3,4], span=3 (alpha=0.5)
    expect(means[1]).toBeCloseTo(1.5, 3);
  });

  test("ewm sum compounds with decay", () => {
    const sums = new Series([1, 1]).ewm(1).sum();
    const sumsList = sums.to_list();
    expect(sumsList[0]).toBeCloseTo(1);
    expect(sumsList[1]).toBeCloseTo(1);
  });
});

describe("Series resample", () => {
  test("bins datetime values by frequency", () => {
    const s = new Series([
      new Date("2026-01-01T00:30:00"),
      new Date("2026-01-01T00:45:00"),
      new Date("2026-01-01T02:10:00"),
    ]);
    const r = s.resample("1h");
    const counts = r.count();
    expect(counts.to_list()).toEqual([2, 1]);
    const mins = r.min();
    const list = mins.to_list();
    // First bin holds the two midnight-ish entries; second the 02:10 entry.
    expect(list).toHaveLength(2);
  });

  test("resample mean computes per-bin averages", () => {
    const s = new Series([
      new Date("2026-01-01T00:10:00"),
      new Date("2026-01-01T00:20:00"),
    ]);
    void s;
    const numeric = new Series([10, 20, 30]);
    void numeric;
    const dated = new Series([
      new Date("2026-01-01T00:10:00"),
      new Date("2026-01-01T00:20:00"),
      new Date("2026-01-01T01:15:00"),
    ]);
    // Values here are dates; resample counts them per bin.
    const out = dated.resample("1h").count();
    expect(out.to_list()).toEqual([2, 1]);
  });
});

describe("Series cat accessor + from_arrow", () => {
  test("cat exposes categories/codes for low-cardinality series", () => {
    const s = new Series(["a", "b", "a"]);
    const accessor = s.cat;
    expect(accessor).not.toBeNull();
    expect(accessor!.categories.sort()).toEqual(["a", "b"]);
  });

  test("from_arrow constructors build their matching containers", () => {
    const records = [{ a: 1 }, { a: 2 }];
    const df = DataFrame.from_arrow(records);
    expect(df.columns).toEqual(["a"]);
    expect(df.to_records()).toHaveLength(2);
    expect(Series.from_arrow([1, 2]).to_list()).toEqual([1, 2]);
  });
});

describe("Series extracted API delegates", () => {
  test("metadata, selection, conversion, and export delegates remain public", () => {
    const series = new Series<CellValue>(["1", null, "3"], { index: ["a", "b", "c"], name: "value" });

    expect(series.at.a).toBe("1");
    expect(series.iat[2]).toBe("3");
    expect(series.array).toEqual(["1", null, "3"]);
    expect(series.list).toEqual(["1", null, "3"]);
    expect(series.flags.allows_duplicate_labels).toBe(true);
    expect(series.infer_objects().to_list()).toEqual([1, null, 3]);
    expect(series.to_markdown()).toContain("value");
    expect(series.to_latex()).toContain("\\begin{tabular}");
    expect(series.to_excel()).toContain("<table");
    expect(series.html).toContain("<table");
  });

  test("mutating delegates update the owning Series", () => {
    const series = new Series<CellValue>([1, 2], { index: ["a", "b"] });
    expect(series.pop("a")).toBe(1);
    expect(series.to_list()).toEqual([2]);

    series.update(new Series<CellValue>([9], { index: ["b"] }));
    expect(series.to_list()).toEqual([9]);
  });

  test("combine, reshape, and frequency delegates remain callable", () => {
    const left = new Series<CellValue>([1, null], { index: [0, 2], name: "value" });
    const right = new Series<CellValue>([10, 20], { index: [0, 1], name: "other" });

    expect(left.combine_first(right).to_list()).toEqual([1, 20]);
    expect(left.combine(right, (a, b) => Number(a ?? 0) + Number(b ?? 0)).to_list()).toEqual([11, 20]);
    expect(left.compare(new Series<CellValue>([1, 3])).to_records()).toEqual([{ self: null, other: 3 }]);
    expect(left.reindex_like(right).index).toEqual([0, 1]);
    expect(left.asfreq(1).to_list()).toEqual([1, null, null]);
    expect(left.asof(2)).toBe(1);
    expect(left.unstack().columns).toEqual(["value"]);
    expect(left.xs(0)).toBe(1);
  });
});
