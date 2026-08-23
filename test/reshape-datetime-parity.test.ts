import { describe, expect, test } from "bun:test";
import { DataFrame, date_range, isna, notna, to_datetime } from "../src/index";
import { Series } from "../src/series";

describe("DataFrame melt", () => {
  const wide = new DataFrame([
    { id: 1, q1: 10, q2: 20 },
    { id: 2, q1: 30, q2: 40 },
  ]);

  test("unpivots all value columns by default", () => {
    const long = wide.melt({ id_vars: "id" });
    expect(long.columns).toEqual(["id", "variable", "value"]);
    expect(long.shape).toEqual([4, 3]);
    expect(long.to_records()).toEqual([
      { id: 1, variable: "q1", value: 10 },
      { id: 1, variable: "q2", value: 20 },
      { id: 2, variable: "q1", value: 30 },
      { id: 2, variable: "q2", value: 40 },
    ]);
  });

  test("respects value_vars subset", () => {
    const long = wide.melt({ id_vars: ["id"], value_vars: "q1" });
    expect(long.shape).toEqual([2, 3]);
    expect(long.to_records()).toEqual([
      { id: 1, variable: "q1", value: 10 },
      { id: 2, variable: "q1", value: 30 },
    ]);
  });

  test("round-trips with pivot", () => {
    const long = wide.melt({ id_vars: "id" });
    const back = long.pivot("id", "variable", "value");
    expect(back.sort_values("id").to_records()).toEqual([
      { id: 1, q1: 10, q2: 20 },
      { id: 2, q1: 30, q2: 40 },
    ]);
  });
});

describe("DataFrame pivot", () => {
  test("spreads rows into columns", () => {
    const long = new DataFrame([
      { day: "Mon", fruit: "apple", sold: 3 },
      { day: "Mon", fruit: "kiwi", sold: 5 },
      { day: "Tue", fruit: "apple", sold: 7 },
    ]);
    const wide = long.pivot("day", "fruit", "sold");
    expect(wide.to_records()).toEqual([
      { day: "Mon", apple: 3, kiwi: 5 },
      { day: "Tue", apple: 7, kiwi: null },
    ]);
  });

  test("throws on duplicates without aggregate", () => {
    const dupes = new DataFrame([
      { d: "Mon", f: "a", v: 1 },
      { d: "Mon", f: "a", v: 2 },
    ]);
    expect(() => dupes.pivot("d", "f", "v")).toThrow(/duplicate/i);
  });

  test("aggregate resolves duplicates", () => {
    const dupes = new DataFrame([
      { d: "Mon", f: "a", v: 1 },
      { d: "Mon", f: "a", v: 3 },
    ]);
    const out = dupes.pivot("d", "f", "v", {
      aggregate: (vals) => vals.reduce<number>((sum, v) => sum + (v as number), 0),
    });
    expect(out.to_records()).toEqual([{ d: "Mon", a: 4 }]);
  });
});

describe("DataFrame transpose + select_dtypes", () => {
  test("transpose swaps rows and columns", () => {
    const df = new DataFrame([
      { a: 1, b: 2 },
      { a: 3, b: 4 },
    ], { index: ["r1", "r2"] });
    const t = df.transpose();
    expect(t.columns).toEqual(["index", "r1", "r2"]);
    expect(t.to_records()).toEqual([
      { index: "a", r1: 1, r2: 3 },
      { index: "b", r1: 2, r2: 4 },
    ]);
  });

  test("select_dtypes keeps only matching columns", () => {
    const df = new DataFrame([
      { n: 1, s: "x", b: true },
      { n: 2, s: "y", b: false },
    ]);
    const nums = df.select_dtypes("number");
    expect(nums.columns).toEqual(["n"]);
    const multi = df.select_dtypes(["string", "boolean"]);
    expect(multi.columns).toEqual(["s", "b"]);
  });
});

describe("Series .dt accessor and to_datetime", () => {
  test("extracts date parts", () => {
    const s = new Series<string>(["2026-01-15", "2026-07-04"]);
    const dt = s.to_datetime().dt;
    expect(dt.year()).toEqual([2026, 2026]);
    expect(dt.month()).toEqual([1, 7]);
    expect(dt.day()).toEqual([15, 4]);
  });

  test("dayofweek is Monday=0", () => {
    // 2026-08-23 is a Sunday.
    const s = new Series<string>(["2026-08-23", "2026-08-24"]);
    expect(s.to_datetime().dt.dayofweek()).toEqual([6, 0]);
    expect(s.to_datetime().dt.day_name()).toEqual(["Sunday", "Monday"]);
  });

  test("nulls propagate through .dt", () => {
    const s = new Series<string | null>(["2026-01-15", null]);
    expect(s.to_datetime().dt.year()).toEqual([2026, null]);
  });

  test("invalid strings become null", () => {
    const s = new Series<string>(["2026-01-15", "not-a-date"]);
    expect(s.to_datetime().to_list()[1]).toBe(null);
  });
});

describe("top-level datetime helpers", () => {
  test("to_datetime converts arrays", () => {
    const dates = to_datetime(["2026-01-01", "2026-06-15T10:00:00Z"]);
    expect(dates[0]!.getFullYear()).toBe(2026);
    expect(dates[1]!).toBeInstanceOf(Date);
  });

  test("date_range with periods", () => {
    const days = date_range({ start: "2026-01-01", periods: 5 });
    expect(days.length).toBe(5);
    expect(days[4]!.toISOString().slice(0, 10)).toBe("2026-01-05");
  });

  test("date_range weekly freq", () => {
    const weeks = date_range({ start: "2026-01-01", end: "2026-01-29", freq: "W" });
    expect(weeks.length).toBe(5);
  });

  test("date_range month steps preserve day-of-month", () => {
    const months = date_range({ start: "2026-01-31", periods: 3, freq: "M" });
    expect(months.map((d) => `${d.getFullYear()}-${d.getMonth() + 1}`)).toEqual([
      "2026-1", "2026-2", "2026-3",
    ]);
  });

  test("isna/notna", () => {
    const values = [1, null, NaN, undefined, "x"];
    expect(isna(values)).toEqual([false, true, true, true, false]);
    expect(notna(values)).toEqual([true, false, false, false, true]);
  });
});

describe("Series.rank", () => {
  test("average method by default", () => {
    const s = new Series<number>([10, 20, 10, 30]);
    // sorted values: 10,10,20,30 -> ranks 1.5,1.5,3,4
    expect(s.rank().to_list() as (number | null)[]).toEqual([1.5, 3, 1.5, 4]);
  });

  test("rank min/max/first/dense methods", () => {
    const s = new Series<number>([10, 20, 10, 30]);
    expect(s.rank({ method: "min" }).to_list() as (number | null)[]).toEqual([1, 3, 1, 4]);
    expect(s.rank({ method: "max" }).to_list() as (number | null)[]).toEqual([2, 3, 2, 4]);
    expect(s.rank({ method: "first" }).to_list() as (number | null)[]).toEqual([1, 3, 2, 4]);
    expect(s.rank({ method: "dense" }).to_list() as (number | null)[]).toEqual([1, 2, 1, 3]);
  });

  test("descending rank", () => {
    expect(new Series<number>([10, 30, 20]).rank({ ascending: false }).to_list()).toEqual([3, 1, 2]);
  });

  test("nulls stay null with na_option keep", () => {
    const s = new Series<number | null>([5, null, 1]);
    expect(s.rank().to_list()).toEqual([2, null, 1]);
  });
});

describe("DataFrame.join", () => {
  test("joins on index by default", () => {
    const left = new DataFrame([{ v: 1 }, { v: 2 }], { index: ["a", "b"] });
    const right = new DataFrame([{ w: 10 }, { w: 20 }], { index: ["a", "c"] });
    const joined = left.join(right, { how: "inner" });
    expect(joined.to_records()).toEqual([{ v: 1, w: 10 }]);
    expect(joined.index).toEqual(["a"]);
  });

  test("left join keeps unmatched rows with undefined cells", () => {
    const left = new DataFrame([{ k: "x", v: 1 }, { k: "y", v: 2 }]);
    const right = new DataFrame([{ k: "x", w: 99 }]);
    const joined = left.join(right, { on: "k", how: "left" });
    expect(joined.to_records()).toEqual([
      { k: "x", v: 1, w: 99 },
      { k: "y", v: 2, w: undefined },
    ]);
  });

  test("suffixes resolve column collisions", () => {
    const left = new DataFrame([{ k: "x", v: 1 }]);
    const right = new DataFrame([{ k: "x", v: 5 }]);
    const joined = left.join(right, { on: "k", how: "inner", suffixes: ["_l", "_r"] });
    expect(joined.columns.includes("v_l")).toBe(true);
    expect(joined.columns.includes("v_r")).toBe(true);
  });
});
