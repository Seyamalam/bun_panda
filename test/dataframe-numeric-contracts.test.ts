import { describe, expect, test } from "bun:test";
import { DataFrame, Series } from "../src/index";

describe("DataFrame numeric contracts", () => {
  test("fills preserve index, existing values and source data", () => {
    const frame = new DataFrame({ x: [null, 2, null, null, 8, null], label: [null, "a", null, "b", null, null] }, { index: ["a", "b", "c", "d", "e", "f"] });
    expect(frame.ffill().get("x").to_list()).toEqual([NaN, 2, 2, 2, 8, 8]);
    expect(frame.bfill().get("x").to_list()).toEqual([2, 2, 8, 8, 8, NaN]);
    expect(frame.interpolate().get("x").to_list()).toEqual([NaN, 2, 4, 6, 8, NaN]);
    expect(frame.interpolate().get("label").to_list()).toEqual([NaN, "a", NaN, "b", NaN, NaN]);
    expect(frame.ffill().index).toEqual(frame.index);
    expect(frame.get("x").to_list()).toEqual([NaN, 2, NaN, NaN, 8, NaN]);
    expect(new DataFrame({ x: [null, 1, null], y: [null, null, null] }).interpolate().values()).toEqual([[NaN, null], [1, null], [NaN, null]]);
  });

  const arithmetic = [
    ["add", [5, 7]], ["sub", [1, 3]], ["rsub", [-1, -3]],
    ["mul", [6, 10]], ["div", [1.5, 2.5]], ["floordiv", [1, 2]],
    ["mod", [1, 1]], ["pow", [9, 25]], ["radd", [5, 7]],
    ["rmul", [6, 10]], ["rdiv", [2 / 3, 0.4]], ["rpow", [8, 32]],
    ["rmod", [2, 2]], ["rfloordiv", [0, 0]], ["rtruediv", [2 / 3, 0.4]],
  ] as const;
  for (const [method, expected] of arithmetic) {
    test(`${method} handles scalars and equal-shape frames`, () => {
      const frame = new DataFrame({ x: [3, 5, null, "text", Infinity] });
      expect(frame[method](2).get("x").to_list()).toEqual([...expected, NaN, NaN, NaN]);
      const right = new DataFrame({ x: [2, 2, 2, 2, 2] });
      expect(frame[method](right).get("x").to_list()).toEqual([...expected, NaN, NaN, NaN]);
      // pandas aligns on the union of index labels rather than requiring identical shape.
      expect(frame[method](new DataFrame({ x: [1] })).index).toEqual([0, 1, 2, 3, 4]);
    });
  }

  const comparisons = [["eq", [false, true]], ["ne", [true, false]], ["lt", [true, false]], ["le", [true, true]], ["gt", [false, false]], ["ge", [false, true]]] as const;
  for (const [method, expected] of comparisons) {
    test(`${method} compares numeric cells and propagates missing data`, () => {
      const frame = new DataFrame({ x: [1, 2, null, "text"] });
      expect(frame[method](2).get("x").to_list()).toEqual([...expected, null, null]);
      expect(frame[method](new DataFrame({ x: [2, 2, 2, 2] })).values()).toEqual([...expected, null, null].map((v) => [v]));
    });
  }

  test("reductions exclude missing and nonnumeric cells", () => {
    const frame = new DataFrame({ x: [1, 2, 3, null], text: ["a", "b", "c", null] }, { index: ["a", "b", "c", "d"] });
    expect(frame.sum()).toEqual({ x: 6, text: null });
    expect(frame.mean()).toEqual({ x: 2, text: null });
    expect(frame.median()).toEqual({ x: 2, text: null });
    expect(frame.min()).toEqual({ x: 1, text: null });
    expect(frame.max()).toEqual({ x: 3, text: null });
    expect(frame.prod()).toEqual({ x: 6, text: null });
    expect(frame.var()).toEqual({ x: 1, text: null });
    expect(frame.std()).toEqual({ x: 1, text: null });
    expect(frame.count()).toEqual({ x: 3, text: 3 });
    expect(frame.quantile(0.25)).toEqual({ x: 1.5, text: null });
    expect(frame.idxmax()).toEqual({ x: "c", text: null });
    expect(frame.idxmin()).toEqual({ x: "a", text: null });
    expect(frame.sem().x).toBeCloseTo(1 / Math.sqrt(3));
    expect(frame.skew()).toEqual({ x: 0, text: null });
    expect(frame.kurt()).toEqual({ x: null, text: null });
    expect(frame.kurtosis()).toEqual(frame.kurt());
    expect(frame.describe().get("x").to_list()).toEqual([3, 2, 1, 1, 3]);
  });

  test("cumulative values skip gaps and retain their positions", () => {
    const frame = new DataFrame({ x: [3, null, 2, 4], y: [1, 2, 3, 4] });
    expect(frame.cummax().get("x").to_list()).toEqual([3, NaN, 3, 4]);
    expect(frame.cummin().get("x").to_list()).toEqual([3, NaN, 2, 2]);
    expect(frame.cumprod().get("x").to_list()).toEqual([3, NaN, 6, 24]);
    expect(frame.cumsum("y").get("y").to_list()).toEqual([1, 3, 6, 10]);
  });

  test("modes include ties in first-seen order and pad shorter columns", () => {
    const date = new Date("2026-01-01T00:00:00Z");
    expect(new DataFrame({ x: [2, 1, 2, 1], y: ["a", "a", "a", "b"], z: [null, null, null, null], d: [date, date, null, null] }).mode().to_records()).toEqual([
      { x: 2, y: "a", z: null, d: date }, { x: 1, y: NaN, z: null, d: null },
    ]);
    expect(new DataFrame({ x: [null] }).mode().shape).toEqual([0, 1]);
  });

  test("take and truncate select by position or label without mutating input", () => {
    const frame = new DataFrame({ x: [1, 2, 3, 4] }, { index: ["a", "b", "c", "d"] });
    expect(frame.take([-1, 0, 0]).index).toEqual(["d", "a", "a"]);
    expect(frame.take([-1, 0, 0]).get("x").to_list()).toEqual([4, 1, 1]);
    expect(() => frame.take([4])).toThrow("out of bounds");
    expect(() => frame.take([-5])).toThrow("out of bounds");
    expect(frame.truncate("b", "c").index).toEqual(["b", "c"]);
    expect(frame.truncate({ before: "b", after: "c" }).index).toEqual(["b", "c"]);
    expect(frame.truncate().shape).toEqual(frame.shape);
    expect(frame.truncate("d", "a").empty).toBe(true);
    expect(() => frame.truncate("missing")).toThrow("not found");
    expect(() => frame.truncate(undefined, "missing")).toThrow("not found");
  });

  test("reindex fills new labels and columns while preserving existing nulls", () => {
    const frame = new DataFrame({ x: [1, null], y: [4, 5] }, { index: ["a", "b"] });
    expect(frame.reindex({ index: ["b", "c", "a"], columns: ["x", "z"], fill_value: -1 }).to_records()).toEqual([{ x: NaN, z: -1 }, { x: -1, z: -1 }, { x: 1, z: -1 }]);
    expect(frame.reindex().equals(frame)).toBe(true);
    expect(frame.shape).toEqual([2, 2]);
  });

  test("squeeze returns a scalar, labeled series or frame according to shape", () => {
    expect(new DataFrame({ x: [7] }).squeeze()).toBe(7);
    const column = new DataFrame({ x: [1, 2] }, { index: ["a", "b"] }).squeeze();
    expect(column).toBeInstanceOf(Series);
    if (column instanceof Series) expect(column.index).toEqual(["a", "b"]);
    const row = new DataFrame({ x: [1], y: [2] }).squeeze();
    if (!(row instanceof Series)) throw new Error("Expected a row Series");
    expect(row.to_dict()).toEqual({ x: 1, y: 2 });
    expect(new DataFrame({ x: [1, 2], y: [3, 4] }).squeeze()).toBeInstanceOf(DataFrame);
  });

  test("aggregate supports named and custom column reducers", () => {
    const frame = new DataFrame({ x: [1, 2, 3], y: [4, 5, 6] });
    expect(frame.agg("sum")).toEqual({ x: 6, y: 15 });
    expect(frame.aggregate({ x: "mean", y: (values) => values.length })).toEqual({ x: 2, y: 3 });
    expect(() => frame.agg({ missing: "sum" })).toThrow("does not exist");
  });

  test("exports escape HTML and report labeled data and memory", () => {
    const date = new Date("2026-01-01T00:00:00Z");
    const frame = new DataFrame({ '<x>': ['<&"', null], d: [date, null] }, { index: ["<a>", "b"] });
    const html = frame.to_html();
    expect(html).toContain("&lt;x&gt;");
    expect(html).toContain("&lt;&amp;&quot;");
    expect(html).toContain("&lt;a&gt;");
    expect(html).toContain(date.toISOString());
    expect(frame.to_markdown()).toContain(date.toISOString());
    expect(frame.info()).toContain("2 entries");
    expect(frame.info()).toContain("1 non-null");
    expect(frame.memory_usage()).toEqual({ '<x>': 16, d: 16, Index: 16 });
    expect(new DataFrame({ empty: [null] }).info()).toContain("unknown");
    expect(new DataFrame([]).info()).toContain("0 entries");
    expect(frame.to_numpy()).toEqual([['<&"', date], [NaN, null]]);
    expect(frame.to_string(1)).toContain("1 more rows");
  });
});
