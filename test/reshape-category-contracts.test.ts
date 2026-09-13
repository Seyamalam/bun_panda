import { describe, expect, test } from "bun:test";
import { Categorical, CategoricalDtype, DataFrame, crosstab, cut, factorize, get_dummies, qcut } from "../src/index";
import { CategoricalAccessor } from "../src/categorical";

describe("reshape helpers", () => {
  test("crosstab counts pairs in first-seen order and excludes missing keys", () => {
    const frame = new DataFrame({ city: ["b", "a", "b", "a", null, "b"], kind: ["x", "y", "x", "x", "x", null] });
    expect(crosstab(frame, "city", "kind").to_records()).toEqual([{ city: "b", x: 2, y: 0 }, { city: "a", x: 1, y: 1 }]);
    expect(crosstab(new DataFrame([]), "city", "kind").shape).toEqual([0, 1]);
  });

  test("cut bins finite values and marks out-of-range entries", () => {
    expect(cut([0, 1, 2, 3, 4, NaN, Infinity, -1, 5], 2, { lower: 0, upper: 4 })).toEqual({ labels: [0, 0, 1, 1, 1, -1, -1, -1, -1], edges: [0, 2, 4] });
    expect(cut([0, 2, 4], 2)).toEqual({ labels: [0, 1, 1], edges: [0, 2, 4] });
    expect(cut([], 2, { lower: 0, upper: 4 })).toEqual({ labels: [], edges: [0, 2, 4] });
    expect(() => cut([NaN], 2)).toThrow("numeric values");
  });

  test("qcut keeps equal values in the same group and restores input order", () => {
    expect(qcut([4, 1, 3, 2, NaN], 2)).toEqual([1, 0, 1, 0, -1]);
    expect(qcut([1, 1, 1, 2], 2)).toEqual([0, 0, 0, 1]);
    expect(qcut([], 2)).toEqual([]);
  });

  test("dummy encoding retains nonencoded data and can drop the first category", () => {
    const frame = new DataFrame({ id: [1, 2, 3], color: ["red", "blue", null], size: ["s", "m", "s"] });
    expect(get_dummies(frame, "color").to_records()).toEqual([
      { id: 1, size: "s", color_red: 1, color_blue: 0 },
      { id: 2, size: "m", color_red: 0, color_blue: 1 },
      { id: 3, size: "s", color_red: 0, color_blue: 0 },
    ]);
    expect(get_dummies(frame, ["color", "size"], { drop_first: true, prefix_sep: ":" }).to_records()).toEqual([
      { id: 1, "color:blue": 0, "size:m": 0 },
      { id: 2, "color:blue": 1, "size:m": 1 },
      { id: 3, "color:blue": 0, "size:m": 0 },
    ]);
    expect(frame.columns).toEqual(["id", "color", "size"]);
  });

  test("factorization distinguishes types and normalizes dates and missing values", () => {
    const date = new Date("2026-01-01T00:00:00Z");
    expect(factorize([1, "1", date, new Date(date), null, undefined, NaN, 1])).toEqual({ codes: [0, 1, 2, 2, -1, -1, -1, 0], uniques: [1, "1", date] });
  });
});

describe("categorical contracts", () => {
  test("explicit dtypes encode unknowns, compare order and return defensive snapshots", () => {
    const dtype = new CategoricalDtype(["b", "a", "b"], true);
    expect(dtype.categories).toEqual(["b", "a"]);
    expect(dtype.equals(new CategoricalDtype(["b", "a"], true))).toBe(true);
    expect(dtype.equals(new CategoricalDtype(["b", "a"]))).toBe(false);
    expect(dtype.equals(new CategoricalDtype(["a", "b"], true))).toBe(false);
    expect(dtype.equals({})).toBe(false);
    expect(dtype.toString()).toContain("'b', 'a'");
    expect(new CategoricalDtype().toString()).toContain("non-ordered");
    const category = new Categorical(["a", "missing", "b"], { dtype });
    expect(category.codes).toEqual([1, -1, 0]);
    expect(category.to_list()).toEqual(["a", null, "b"]);
    category.codes[0] = 99;
    category.categories.push("c");
    expect(category.codes).toEqual([1, -1, 0]);
    expect(category.categories).toEqual(["b", "a"]);
    expect(category.equals(new Categorical(["a", "missing", "b"], { dtype }))).toBe(true);
    expect(category.equals(new Categorical(["b", "missing", "a"], { dtype }))).toBe(false);
    expect(category.equals(new Categorical(["a"]))).toBe(false);
    expect(category.equals({})).toBe(false);
    expect(category.toString()).toContain("NaN");
    expect(() => new Categorical([], { dtype, ordered: false })).toThrow("both");
    expect(category.describe().get("freqs").to_list()).toEqual([0.5, 0.5]);
    expect(new Categorical([], { categories: ["a"] }).describe().get("freqs").to_list()).toEqual([0]);
  });

  test("maps categories and supports accessor recoding", () => {
    const category = new Categorical(["a", "b", "a"]);
    expect(category.map((v) => String(v).toUpperCase()).to_list()).toEqual(["A", "B", "A"]);
    expect(category.map({ a: "first", b: "second" }).to_list()).toEqual(["first", "second", "first"]);
    const accessor = new CategoricalAccessor(["b", "a", "b"]);
    expect(accessor.length).toBe(3);
    expect(accessor.dtype.categories).toEqual(["a", "b"]);
    expect(accessor.to_list()).toEqual(["b", "a", "b"]);
    expect(accessor.describe().get("counts").to_list()).toEqual([1, 2]);
    expect(accessor.as_ordered().as_ordered().ordered).toBe(true);
    expect(accessor.as_ordered().as_unordered().as_unordered().ordered).toBe(false);
    expect(accessor.rename_categories(["A", "B"]).to_list()).toEqual(["B", "A", "B"]);
    expect(accessor.reorder_categories(["b", "a"]).codes).toEqual([0, 1, 0]);
    expect(accessor.add_categories(["c"]).remove_unused_categories().categories).toEqual(["a", "b"]);
    expect(() => accessor.rename_categories(["a"])).toThrow("expected");
    expect(() => accessor.reorder_categories(["a", "c"])).toThrow("existing set");
    expect(() => accessor.add_categories(["a"])).toThrow("already exists");
  });
});
