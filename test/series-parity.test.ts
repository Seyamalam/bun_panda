import { describe, expect, test } from "bun:test";
import { DataFrame } from "../src/index";
import { Series } from "../src/series";

describe("Series arithmetic", () => {
  const s = new Series<number>([1, 2, 3, 4], { name: "x" });

  test("add/sub/mul/div with scalar", () => {
    expect(s.add(10).to_list()).toEqual([11, 12, 13, 14]);
    expect(s.sub(1).to_list()).toEqual([0, 1, 2, 3]);
    expect(s.mul(2).to_list()).toEqual([2, 4, 6, 8]);
    expect(s.div(2).to_list()).toEqual([0.5, 1, 1.5, 2]);
  });

  test("rsub reverses operand order", () => {
    expect(s.rsub(10).to_list()).toEqual([9, 8, 7, 6]);
  });

  test("mod/pow", () => {
    expect(s.mod(2).to_list()).toEqual([1, 0, 1, 0]);
    expect(new Series<number>([2, 3]).pow(2).to_list()).toEqual([4, 9]);
  });

  test("series-to-series aligns by position", () => {
    const a = new Series<number>([10, 20, 30]);
    const b = new Series<number>([1, 2, 3]);
    expect(a.sub(b).to_list()).toEqual([9, 18, 27]);
  });

  test("nulls propagate", () => {
    const withNull = new Series<number | null>([1, null, 3]);
    expect(withNull.add(1).to_list() as (number | null)[]).toEqual([2, null, 4]);
  });

  test("neg/abs/round", () => {
    expect(s.neg().to_list()).toEqual([-1, -2, -3, -4]);
    expect(new Series<number>([-1.5, 2.5]).abs().to_list()).toEqual([1.5, 2.5]);
    expect(new Series<number>([1.234, 5.678]).round(1).to_list()).toEqual([1.2, 5.7]);
  });
});

describe("Series comparisons", () => {
  const s = new Series<number>([1, 2, 3, 4]);

  test("scalar comparisons", () => {
    expect([...s.gt(2).to_list()]).toEqual([false, false, true, true]);
    expect([...s.le(2).to_list()]).toEqual([true, true, false, false]);
    expect([...s.eq(3).to_list()]).toEqual([false, false, true, false]);
    expect([...s.ne(3).to_list()]).toEqual([true, true, false, true]);
  });

  test("series comparisons", () => {
    const a = new Series<number>([1, 5, 3]);
    const b = new Series<number>([2, 2, 2]);
    expect([...a.lt(b).to_list()]).toEqual([true, false, false]);
  });

  test("nulls yield null in output", () => {
    const withNull = new Series<number | null>([1, null]);
    expect(withNull.gt(0).to_list() as (boolean | null)[]).toEqual([true, null]);
  });
});

describe("Series cumulative + selection", () => {
  test("cumsum skips nulls but keeps positions", () => {
    const s = new Series<number | null>([1, null, 3, 4]);
    expect(s.cumsum().to_list() as (number | null)[]).toEqual([1, null, 4, 8]);
  });

  test("cummax/cummin", () => {
    expect(new Series<number>([3, 1, 4, 2]).cummax().to_list()).toEqual([3, 3, 4, 4]);
    expect(new Series<number>([3, 1, 4, 2]).cummin().to_list()).toEqual([3, 1, 1, 1]);
  });

  test("nlargest/nsmallest keep original index labels", () => {
    const s = new Series<number>([5, 1, 9, 3], { index: ["a", "b", "c", "d"] });
    expect(s.nlargest(2).to_list()).toEqual([9, 5]);
    expect(s.nlargest(2).index).toEqual(["c", "a"]);
    expect(s.nsmallest(2).to_list()).toEqual([1, 3]);
    expect(s.nsmallest(2).index).toEqual(["b", "d"]);
  });
});

describe("Series .str accessor", () => {
  test("case conversions", () => {
    const s = new Series<string>(["hello", "world"]);
    expect(s.str.upper().map((v) => v as string)).toEqual(["HELLO", "WORLD"]);
    expect(s.str.capitalize().map((v) => v as string)).toEqual(["Hello", "World"]);
    expect(new Series<string>(["foo bar"]).str.title().map((v) => v as string)).toEqual(["Foo Bar"]);
  });

  test("trimming and padding", () => {
    const s = new Series<string>(["  x  ", " y"]);
    expect(s.str.strip().map((v) => v as string)).toEqual(["x", "y"]);
    expect(new Series<string>(["7"]).str.zfill(3).map((v) => v as string)).toEqual(["007"]);
    expect(new Series<string>(["ab"]).str.pad(5, "both", "*").map((v) => v as string)).toEqual(["*ab**"]);
  });

  test("predicates return boolean arrays with null propagation", () => {
    const s = new Series<string | null>(["apple", "banana", null]);
    expect(s.str.startswith("ap")).toEqual([true, false, null]);
    expect(s.str.endswith("na")).toEqual([false, true, null]);
    expect(s.str.contains("an")).toEqual([false, true, null]);
    expect(s.str.len()).toEqual([5, 6, null]);
  });

  test("replace literal and regex", () => {
    const s = new Series<string>(["a-b-c"]);
    expect(s.str.replace("-", "+").map((v) => v as string)).toEqual(["a+b+c"]);
    expect(s.str.replace(/-/g, "_").map((v) => v as string)).toEqual(["a_b_c"]);
  });

  test("slice/get/find/count", () => {
    const s = new Series<string>(["hello"]);
    expect(s.str.slice(1, 3).map((v) => v as string)).toEqual(["el"]);
    expect(s.str.get(-1)).toEqual(["o"]);
    expect(s.str.find("l")).toEqual([2]);
    expect(new Series<string>(["aaa"]).str.count("a")).toEqual([3]);
  });

  test("split returns component arrays", () => {
    const s = new Series<string>(["a,b", "c,d,e"]);
    expect(s.str.split(",")).toEqual([["a", "b"], ["c", "d", "e"]]);
  });

  test("cat joins with separator", () => {
    const s = new Series<string>(["x", "y"]);
    expect(s.str.cat(["1", "2"], "-").map((v) => v as string)).toEqual(["x-1", "y-2"]);
  });
});

describe("DataFrame iteration", () => {
  const df = new DataFrame([
    { a: 1, b: "x" },
    { a: 2, b: "y" },
  ]);

  test("iterrows yields [index, row]", () => {
    const out = [...df.iterrows()];
    expect(out.length).toBe(2);
    expect(out[0]![0]).toBe(0);
    expect(out[0]![1]).toEqual({ a: 1, b: "x" });
    // rows are copies — mutating them must not affect the frame
    out[0]![1].a = 99;
    expect(df.iloc(0)).toEqual({ a: 1, b: "x" });
  });

  test("itertuples includes Index field", () => {
    const out = [...df.itertuples()];
    expect(out[0]).toEqual({ Index: 0, a: 1, b: "x" });
    expect(out[1]).toEqual({ Index: 1, a: 2, b: "y" });
  });

  test("items yields column name + Series", () => {
    const out = [...df.items()];
    expect(out.map(([name]) => name)).toEqual(["a", "b"]);
    expect(out[0]![1].to_list()).toEqual([1, 2]);
    expect(out[1]![1].to_list()).toEqual(["x", "y"]);
  });
});

describe("DataFrame shift/diff/pct_change", () => {
  const df = new DataFrame([
    { v: 10, tag: "a" },
    { v: 20, tag: "b" },
    { v: 40, tag: "c" },
  ]);

  test("shift moves values down, filling nulls", () => {
    const shifted = df.shift(1);
    expect(shifted.to_records()).toEqual([
      { v: null, tag: null },
      { v: 10, tag: "a" },
      { v: 20, tag: "b" },
    ]);
    expect(shifted.index).toEqual(df.index);
  });

  test("negative shift moves values up", () => {
    expect(df.shift(-1).to_records()).toEqual([
      { v: 20, tag: "b" },
      { v: 40, tag: "c" },
      { v: null, tag: null },
    ]);
  });

  test("diff computes row deltas on numeric columns only", () => {
    expect(df.diff().to_records()).toEqual([
      { v: null, tag: null },
      { v: 10, tag: null },
      { v: 20, tag: null },
    ]);
  });

  test("pct_change computes relative deltas", () => {
    expect(df.pct_change().to_records()).toEqual([
      { v: null, tag: null },
      { v: 1, tag: null },
      { v: 1, tag: null },
    ]);
  });

  test("diff respects periods", () => {
    expect(
      new DataFrame([{ v: 1 }, { v: 4 }, { v: 9 }]).diff(2).to_records()
    ).toEqual([{ v: null }, { v: null }, { v: 8 }]);
  });
});
