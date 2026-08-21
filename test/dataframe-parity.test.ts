import { describe, expect, test } from "bun:test";
import { DataFrame, Series } from "../index";

const base = () =>
  new DataFrame([
    { a: 1, b: 10 },
    { a: 2, b: 20 },
    { a: 3, b: 30 },
  ]);

describe("DataFrame where/mask", () => {
  test("where with predicate fn keeps values where cond holds", () => {
    const df = base().where((row) => (row.a as number) > 1);
    expect(df.to_records()).toEqual([
      { a: null, b: null },
      { a: 2, b: 20 },
      { a: 3, b: 30 },
    ]);
  });

  test("where with scalar other replaces failing cells", () => {
    const df = base().where((row) => (row.a as number) > 1, -1);
    expect(df.to_records()).toEqual([
      { a: -1, b: -1 },
      { a: 2, b: 20 },
      { a: 3, b: 30 },
    ]);
  });

  test("where with per-column record other leaves unspecified columns intact", () => {
    const df = base().where((row) => (row.a as number) > 1, { a: 0 });
    expect(df.to_records()).toEqual([
      { a: 0, b: 10 },
      { a: 2, b: 20 },
      { a: 3, b: 30 },
    ]);
  });

  test("where with fn other receives value and coordinates", () => {
    const df = base().where(
      (row) => (row.a as number) > 1,
      (value, column) => (column === "a" ? `missing:${value}` : value)
    );
    expect(df.to_records()).toEqual([
      { a: "missing:1", b: 10 },
      { a: 2, b: 20 },
      { a: 3, b: 30 },
    ]);
  });

  test("where with boolean array cond", () => {
    const df = base().where([true, false, true], 0);
    expect(df.to_records()).toEqual([
      { a: 1, b: 10 },
      { a: 0, b: 0 },
      { a: 3, b: 30 },
    ]);
  });

  test("where with boolean array of wrong length throws", () => {
    expect(() => base().where([true, false], 0)).toThrow(
      "Mask length must match row count."
    );
  });

  test("where with per-column boolean record keeps unmasked columns", () => {
    const df = base().where({ a: [true, false, true] }, 0);
    expect(df.to_records()).toEqual([
      { a: 1, b: 10 },
      { a: 0, b: 20 },
      { a: 3, b: 30 },
    ]);
  });

  test("where with per-column mask of wrong length throws", () => {
    expect(() => base().where({ a: [true, false] }, 0)).toThrow(
      "Mask length must match row count."
    );
  });

  test("where with boolean DataFrame cond", () => {
    const cond = new DataFrame([
      { a: true, b: false },
      { a: false, b: true },
      { a: true, b: true },
    ]);
    const df = base().where(cond, 0);
    expect(df.to_records()).toEqual([
      { a: 1, b: 0 },
      { a: 0, b: 20 },
      { a: 3, b: 30 },
    ]);
  });

  test("where with mismatched DataFrame cond throws", () => {
    const cond = new DataFrame([{ a: true }, { a: false }]);
    expect(() => base().where(cond, 0)).toThrow(
      "where/mask condition shape must match frame."
    );
  });

  test("mask inverts where semantics", () => {
    const df = base().mask((row) => (row.a as number) > 1, "x");
    expect(df.to_records()).toEqual([
      { a: 1, b: 10 },
      { a: "x", b: "x" },
      { a: "x", b: "x" },
    ]);
  });

  test("mask with scalar other mirrors pandas", () => {
    const df = base().mask([true, false, true], 0);
    expect(df.to_records()).toEqual([
      { a: 0, b: 0 },
      { a: 2, b: 20 },
      { a: 0, b: 0 },
    ]);
  });
});

describe("DataFrame transform", () => {
  test("fn mode broadcasts scalar result per column", () => {
    const df = base().transform((series) => series.sum());
    expect(df.to_records()).toEqual([
      { a: 6, b: 60 },
      { a: 6, b: 60 },
      { a: 6, b: 60 },
    ]);
  });

  test("fn mode accepts one value per row", () => {
    const df = base().transform((series, name) =>
      name === "a" ? series.values.map((v) => (v as number) * 10) : series.values
    );
    expect(df.to_records()).toEqual([
      { a: 10, b: 10 },
      { a: 20, b: 20 },
      { a: 30, b: 30 },
    ]);
  });

  test("fn mode throws on wrong-length array", () => {
    expect(() =>
      base().transform((series) => series.values.slice(0, 2))
    ).toThrow("transform function must return a value per row.");
  });

  test("spec mode broadcasts named aggs per column", () => {
    const df = new DataFrame([
      { a: 1, b: "x" },
      { a: 2, b: "y" },
      { a: null, b: "x" },
    ]).transform({
      a: "count",
      b: "nunique",
    });
    expect(df.to_records()).toEqual([
      { a: 2, b: 2 },
      { a: 2, b: 2 },
      { a: 2, b: 2 },
    ]);
  });

  test("spec mode numeric aggs skip missing values", () => {
    const df = new DataFrame([
      { v: 1, w: 4 },
      { v: 3, w: null },
      { v: null, w: 6 },
    ]).transform({
      v: "sum",
      w: "mean",
    });
    expect(df.get("v").values).toEqual([4, 4, 4]);
    expect(df.get("w").values).toEqual([5, 5, 5]);
  });

  test("spec mode sum/mean/min/max are null on empty numeric input", () => {
    const df = new DataFrame([{ v: null }, { v: null }]).transform({
      v: "sum",
    });
    expect(df.get("v").values).toEqual([null, null]);
  });

  test("spec mode median/std/var use ddof=1 semantics over non-missing", () => {
    const df = new DataFrame([
      { v: 1 },
      { v: 2 },
      { v: 3 },
      { v: null },
    ]).transform({ v: "median" });
    expect(df.get("v").values).toEqual([2, 2, 2, 2]);

    const spread = new DataFrame([{ v: 1 }, { v: 2 }, { v: 3 }]).transform({
      v: "std",
    });
    expect(spread.get("v").values[0]).toBeCloseTo(1);
  });

  test("spec mode first/last skip missing values", () => {
    const df = new DataFrame([
      { v: null, w: 1 },
      { v: 7, w: null },
      { v: 9, w: 3 },
    ]).transform({ v: "first", w: "last" });
    expect(df.to_records()).toEqual([
      { v: 7, w: 3 },
      { v: 7, w: 3 },
      { v: 7, w: 3 },
    ]);
  });

  test("spec mode first/last return null when all missing", () => {
    const df = new DataFrame([{ v: null }, { v: null }]).transform({
      v: "first",
    });
    expect(df.get("v").values).toEqual([null, null]);
  });

  test("spec mode nunique ignores nulls", () => {
    const df = new DataFrame([
      { v: "a" },
      { v: "a" },
      { v: null },
    ]).transform({ v: "nunique" });
    expect(df.get("v").values).toEqual([1, 1, 1]);
  });

  test("spec mode throws for unknown column", () => {
    expect(() => base().transform({ nope: "count" })).toThrow();
  });
});

describe("DataFrame insert/pop", () => {
  test("insert at loc 0 puts column first", () => {
    const df = base().insert(0, "z", 1);
    expect(df.columns).toEqual(["z", "a", "b"]);
    expect(df.get("z").values).toEqual([1, 1, 1]);
  });

  test("insert in the middle", () => {
    const df = base().insert(1, "m", [9, 8, 7]);
    expect(df.columns).toEqual(["a", "m", "b"]);
    expect(df.get("m").values).toEqual([9, 8, 7]);
  });

  test("insert clamps out-of-range loc to end", () => {
    const df = base().insert(99, "end", 0);
    expect(df.columns).toEqual(["a", "b", "end"]);
  });

  test("insert rejects duplicate column name", () => {
    expect(() => base().insert(0, "a", 1)).toThrow(
      "Column 'a' already exists."
    );
  });

  test("insert aligns Series by label not position", () => {
    const s = new Series([100, 200, 300], { index: ["c", "a", "b"] });
    const df = new DataFrame(
      [{ v: 1 }, { v: 2 }, { v: 3 }],
      { index: ["a", "b", "c"] }
    ).insert(1, "s", s);
    expect(df.get("s").values).toEqual([200, 300, 100]);
    expect(df.index).toEqual(["a", "b", "c"]);
  });

  test("pop removes column in place and returns aligned Series", () => {
    const df = base();
    const popped = df.pop("b");
    expect(popped.values).toEqual([10, 20, 30]);
    expect(df.columns).toEqual(["a"]);
    expect(df.shape).toEqual([3, 1]);
  });

  test("pop throws for unknown column", () => {
    expect(() => base().pop("nope")).toThrow(
      "Column 'nope' does not exist."
    );
  });
});

describe("DataFrame numeric parity methods", () => {
  test("round rounds numeric cells only", () => {
    const df = new DataFrame([
      { v: 1.234, s: "keep" },
      { v: 2.567, s: null },
      { v: -1.891, s: "x" },
    ]).round(2);
    expect(df.get("v").values).toEqual([1.23, 2.57, -1.89]);
    expect(df.get("s").values).toEqual(["keep", null, "x"]);
  });

  test("round defaults to integers", () => {
    const df = new DataFrame([{ v: 1.6 }, { v: 2.4 }]).round();
    expect(df.get("v").values).toEqual([2, 2]);
  });

  test("abs applies to all numeric columns by default", () => {
    const df = new DataFrame([
      { a: -1, b: -2.5, s: "x" },
      { a: 3, b: null, s: "y" },
    ]).abs();
    expect(df.get("a").values).toEqual([1, 3]);
    expect(df.get("b").values).toEqual([2.5, null]);
  });

  test("abs restricts to given columns", () => {
    const df = new DataFrame([
      { a: -1, b: -2 },
      { a: 3, b: -4 },
    ]).abs("a");
    expect(df.get("a").values).toEqual([1, 3]);
    expect(df.get("b").values).toEqual([-2, -4]);
  });

  test("count counts non-missing cells per column", () => {
    const df = new DataFrame([
      { a: 1, b: "x" },
      { a: null, b: null },
      { a: 3, b: "z" },
    ]);
    expect(df.count()).toEqual({ a: 2, b: 2 });
  });

  test("median/std/var/min/max skip missing values", () => {
    const df = new DataFrame([
      { a: 1, b: 4 },
      { a: 2, b: null },
      { a: 3, b: 8 },
      { a: null, b: 6 },
    ]);
    expect(df.median()).toEqual({ a: 2, b: 6 });
    expect(df.min()).toEqual({ a: 1, b: 4 });
    expect(df.max()).toEqual({ a: 3, b: 8 });
    expect(df.var()["a"]).toBeCloseTo(1);
    expect(df.std()["a"]).toBeCloseTo(1);
    expect(df.std()["b"]).toBeCloseTo(2);
  });

  test("median/std/var return null for all-missing columns", () => {
    const df = new DataFrame([{ a: null }, { a: null }]);
    expect(df.median()).toEqual({ a: null });
    expect(df.std()).toEqual({ a: null });
    expect(df.var()).toEqual({ a: null });
    expect(df.min()).toEqual({ a: null });
    expect(df.max()).toEqual({ a: null });
  });

  test("cumsum accumulates skipping nulls which stay null", () => {
    const df = new DataFrame([
      { a: 1, b: 10 },
      { a: null, b: 20 },
      { a: 2, b: null },
      { a: 3, b: 5 },
    ]).cumsum();
    expect(df.get("a").values).toEqual([1, null, 3, 6]);
    expect(df.get("b").values).toEqual([10, 30, null, 35]);
  });

  test("cumsum restricts accumulation to target columns", () => {
    const df = new DataFrame([
      { a: 1, b: 10 },
      { a: 2, b: 20 },
    ]).cumsum("b");
    expect(df.get("a").values).toEqual([1, 2]);
    expect(df.get("b").values).toEqual([10, 30]);
  });
});

describe("DataFrame duplicated/equals", () => {
  const dupes = () =>
    new DataFrame(
      [
        { a: 1, b: "x" },
        { a: 1, b: "x" },
        { a: 2, b: "z" },
        { a: 1, b: "x" },
      ],
      { index: ["r1", "r2", "r3", "r4"] }
    );

  test("duplicated keep first marks later occurrences", () => {
    expect(dupes().duplicated().values).toEqual([false, true, false, true]);
  });

  test("duplicated keep last marks earlier occurrences", () => {
    expect(dupes().duplicated(undefined, "last").values).toEqual([
      true,
      true,
      false,
      false,
    ]);
  });

  test("duplicated keep false marks every duplicate row", () => {
    expect(dupes().duplicated(undefined, false).values).toEqual([
      true,
      true,
      false,
      true,
    ]);
  });

  test("duplicated respects subset columns", () => {
    const df = new DataFrame([
      { a: 1, b: "x" },
      { a: 1, b: "y" },
      { a: 2, b: "x" },
    ]);
    expect(df.duplicated("a").values).toEqual([false, true, false]);
    expect(df.duplicated(["a", "b"]).values).toEqual([false, false, false]);
  });

  test("equals is true for identical frames", () => {
    expect(base().equals(base())).toBe(true);
  });

  test("equals requires same column order", () => {
    const flipped = base().select(["b", "a"]);
    expect(base().equals(flipped)).toBe(false);
  });

  test("equals requires same index labels", () => {
    const relabeled = new DataFrame(base().to_records(), {
      index: ["x", "y", "z"],
    });
    expect(base().equals(relabeled)).toBe(false);
  });

  test("equals compares cell values including null vs value", () => {
    const left = new DataFrame([{ a: 1 }, { a: null }]);
    const right = new DataFrame([{ a: 1 }, { a: null }]);
    const other = new DataFrame([{ a: 1 }, { a: 0 }]);
    expect(left.equals(right)).toBe(true);
    expect(left.equals(other)).toBe(false);
  });

  test("equals requires same row count", () => {
    expect(base().equals(base().head(2))).toBe(false);
  });
});
