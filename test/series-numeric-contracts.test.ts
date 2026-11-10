import { describe, expect, test } from "bun:test";
import { Series, type CellValue } from "../src/index";

describe("Series numeric contracts", () => {
  test("sample moments use unbiased estimators", () => {
    const series = new Series<CellValue>([1, 1, 1, 5, null]);
    expect(series.skew()).toBeCloseTo(2);
    expect(series.kurt()).toBeCloseTo(4);
    expect(series.kurtosis()).toBeCloseTo(4);
    expect(series.sem()).toBeCloseTo(1);
    expect(series.var()).toBeCloseTo(4);
    expect(series.std()).toBeCloseTo(2);
    expect(series.std(0)).toBeCloseTo(Math.sqrt(3));
    expect(new Series<CellValue>([1, 2, 3, 4]).kurt()).toBeCloseTo(-1.2);
    expect(new Series<CellValue>([5, 5, 5, 5]).skew()).toBe(0);
    expect(new Series<CellValue>([5, 5, 5, 5]).kurt()).toBe(0);
    const empty = new Series<CellValue>([]);
    // pandas reductions on an empty series return NaN.
    for (const reduce of ["mean", "min", "max", "median", "var", "std", "skew", "kurt", "sem"] as const) {
      expect(empty[reduce]()).toBeNaN();
    }
    expect(empty.autocorr()).toBeNull();
    expect(empty.sum()).toBe(0);
    expect(empty.prod()).toBe(1);
    expect(empty.product()).toBe(1);
    expect(new Series<CellValue>([1]).var()).toBeNaN();
    expect(new Series<CellValue>([1]).sem()).toBeNaN();
  });

  test("autocorrelation handles aligned pairs and degenerate inputs", () => {
    expect(new Series<CellValue>([1, 2, 3, 4]).autocorr()).toBeCloseTo(1);
    expect(new Series<CellValue>([1, -1, 1, -1]).autocorr()).toBeCloseTo(-1);
    expect(new Series<CellValue>([1, 1, 1]).autocorr()).toBeNull();
    expect(new Series<CellValue>([1, null, 2]).autocorr()).toBeNull();
    expect(new Series<CellValue>([1, 2, null, 4, 5]).autocorr()).toBeCloseTo(1);
  });

  test("describe reports numeric quartiles and categorical counts", () => {
    const description = new Series<CellValue>([1, 2, 3, 4, null], { name: "x" }).describe();
    expect(description.get("count")).toBe(4);
    expect(description.get("mean")).toBe(2.5);
    expect(description.get("std")).toBeCloseTo(Math.sqrt(5 / 3));
    expect(description.get("25%")).toBe(1.75);
    expect(description.get("50%")).toBe(2.5);
    expect(description.get("75%")).toBe(3.25);
    expect(description.get("min")).toBe(1);
    expect(description.get("max")).toBe(4);
    expect(description.name).toBe("x");
    expect(new Series<CellValue>(["b", "a", "b", null]).describe().to_dict()).toEqual({ count: 3, unique: 2, top: "b", freq: 2 });
    expect(new Series<CellValue>([]).describe().to_dict()).toEqual({ count: 0, unique: 0, top: NaN, freq: NaN });
    const date = new Date("2026-01-01T00:00:00Z");
    expect(new Series<CellValue>([date, new Date(date)]).describe().get("freq")).toBe(2);
  });

  test("equality compares labels, missing values and date timestamps", () => {
    const date = new Date("2026-01-01T00:00:00Z");
    const series = new Series<CellValue>([date, null, 3], { index: ["a", "b", "c"] });
    expect(series.equals(new Series<CellValue>([new Date(date), NaN, 3], { index: ["a", "b", "c"] }))).toBe(true);
    expect(series.equals(new Series<CellValue>([new Date(0), null, 3], { index: ["a", "b", "c"] }))).toBe(false);
    expect(series.equals(new Series<CellValue>([date, 2, 3], { index: ["a", "b", "c"] }))).toBe(false);
    expect(series.equals(new Series<CellValue>([date, null, 4], { index: ["a", "b", "c"] }))).toBe(false);
    expect(series.equals(new Series<CellValue>([date, null, 3]))).toBe(false);
    expect(series.equals(new Series<CellValue>([date]))).toBe(false);
    expect(series.copy().equals(series)).toBe(true);
  });

  test("boolean reductions, extrema and membership preserve missing semantics", () => {
    expect(new Series<CellValue>([true, null]).all()).toBe(true);
    expect(new Series<CellValue>([true, null]).all(false)).toBe(false);
    expect(new Series<CellValue>([false, null]).any()).toBe(false);
    expect(new Series<CellValue>([null, true]).any(false)).toBe(true);
    expect(new Series<CellValue>([0, 1]).all()).toBe(false);
    // pandas treats NaN as truthy under skipna=False, so any() is True.
    expect(new Series<CellValue>([NaN, false]).any(false)).toBe(true);
    expect(new Series<CellValue>([]).all()).toBe(true);
    const series = new Series<CellValue>([null, "text", 4, 1, 4], { index: ["a", "b", "c", "d", "e"] });
    // pandas raises TypeError comparing str with float on mixed object data.
    expect(() => series.argmax()).toThrow(TypeError);
    expect(() => series.argmin()).toThrow(TypeError);
    expect(() => series.idxmax()).toThrow(TypeError);
    expect(() => series.idxmin()).toThrow(TypeError);
    const numeric = new Series<CellValue>([null, 4, 1, 4], { index: ["a", "b", "c", "d"] });
    expect(numeric.argmax()).toBe(1);
    expect(numeric.argmin()).toBe(2);
    expect(numeric.idxmax()).toBe("b");
    expect(numeric.idxmin()).toBe("c");
    // pandas raises ValueError for arg extremes on all-missing series.
    expect(() => new Series<CellValue>([null]).argmax()).toThrow("empty sequence");
    expect(() => new Series<CellValue>([null]).argmin()).toThrow("empty sequence");
    expect(() => new Series<CellValue>([null]).idxmax()).toThrow("empty sequence");
    expect(() => new Series<CellValue>([null]).idxmin()).toThrow("empty sequence");
    expect(series.mode()).toEqual([4]);
    // pandas mode() sorts tied modes ascending.
    expect(new Series<CellValue>([2, 1, 2, 1]).mode()).toEqual([1, 2]);
    expect(new Series<CellValue>([null]).mode()).toEqual([]);
    expect(series.is_unique()).toBe(false);
    expect(new Series<CellValue>([1, 2, null]).is_unique()).toBe(true);
  });

  test("between includes exactly the requested boundaries", () => {
    const series = new Series<CellValue>([1, 2, 3, null, "text"]);
    for (const [inclusive, expected] of [
      ["both", [true, true, true]], ["neither", [false, true, false]],
      ["left", [true, true, false]], ["right", [false, true, true]],
    ] as const) expect<CellValue[]>(series.between(1, 3, inclusive).to_list()).toEqual([...expected, false, false]);
  });

  test("sorting preserves tie order, labels and placement of nulls", () => {
    const series = new Series<CellValue>([3, null, 1, 3, null], { index: ["e", "d", "c", "b", "a"] });
    expect(series.sort_values().index).toEqual(["c", "e", "b", "d", "a"]);
    expect(series.sort_values({ ascending: false, na_position: "first" }).index).toEqual(["d", "a", "e", "b", "c"]);
    expect(series.sort_values(false).to_list()).toEqual([3, 3, 1, NaN, NaN]);
    expect(series.sort_index().index).toEqual(["a", "b", "c", "d", "e"]);
    expect(series.sort_index(false).index).toEqual(series.index);
    // pandas argsort keeps missing positions at the end.
    expect(series.argsort()).toEqual([2, 0, 3, 1, 4]);
    expect(series.searchsorted(3)).toBe(1);
    expect(series.searchsorted(3, "right")).toBe(3);
    expect(series.searchsorted(0)).toBe(0);
    expect(series.is_monotonic_increasing()).toBe(false);
    expect(series.is_monotonic_decreasing()).toBe(false);
    expect(new Series<CellValue>([1, 1, 2]).is_monotonic_increasing()).toBe(true);
    expect(new Series<CellValue>([2, 1, 1]).is_monotonic_decreasing()).toBe(true);
  });

  test("where and mask apply label-aware callbacks or positional masks", () => {
    const series = new Series<CellValue>([1, 2, 3], { index: ["a", "b", "c"], name: "x" });
    expect(series.where([true, false, true], 0).to_list()).toEqual([1, 0, 3]);
    expect(series.mask([true, false, true], 0).to_list()).toEqual([0, 2, 0]);
    expect(series.where((_value, label) => label === "b").to_list()).toEqual([NaN, 2, NaN]);
    expect(series.mask((_value, _label, position) => position === 1).to_list()).toEqual([1, NaN, 3]);
    expect(series.where(() => true).name).toBe("x");
    expect(() => series.where([true])).toThrow("length");
    expect(() => series.mask([])).toThrow("length");
    expect(series.drop("b").index).toEqual(["a", "c"]);
    expect(series.drop(["a", "c"]).to_list()).toEqual([2]);
    // pandas raises KeyError when a dropped label is absent.
    expect(() => series.drop("absent")).toThrow("not found in axis");
  });

  test("sampling is deterministic, respects replacement and validates size", () => {
    const series = new Series<CellValue>([1, 2, 3, 4, 5], { index: ["a", "b", "c", "d", "e"] });
    expect(series.sample(3, { random_state: 7 }).equals(series.sample(3, { random_state: 7 }))).toBe(true);
    expect(new Set(series.sample(5, { random_state: 9 }).to_list()).size).toBe(5);
    expect(series.sample(8, { replace: true, random_state: 7, ignore_index: true }).index).toEqual([0, 1, 2, 3, 4, 5, 6, 7]);
    expect(series.sample(0, { frac: 0.4 }).length).toBe(2);
    expect(series.sample(0).length).toBe(0);
    expect(() => series.sample(6)).toThrow("exceed");
    expect(() => series.sample(-1)).toThrow("non-negative");
    expect(() => series.sample(1.5)).toThrow("integer");
    expect(() => series.sample(1, { frac: -0.1 })).toThrow("non-negative");
  });

  test("shifts, differences and percentage changes handle direction and gaps", () => {
    const series = new Series<CellValue>([1, 2, 4, null, 0, 5]);
    expect(series.shift().to_list()).toEqual([NaN, 1, 2, 4, NaN, 0]);
    expect(series.shift(-1).to_list()).toEqual([2, 4, NaN, 0, 5, NaN]);
    expect(series.diff().to_list()).toEqual([NaN, 1, 2, NaN, NaN, 5]);
    expect(series.diff(-1).to_list()).toEqual([-1, -2, NaN, NaN, -5, NaN]);
    // pandas divides curr/prev: 5/0 yields inf, not NaN.
    expect(series.pct_change().to_list()).toEqual([NaN, 1, 1, NaN, NaN, Infinity]);
    expect(series.pct_change(-1).to_list()).toEqual([-0.5, -0.5, NaN, NaN, -1, NaN]);
    expect(series.shift(0).equals(series)).toBe(true);
  });

  test("duplicate policies retain the expected occurrences including nulls", () => {
    const series = new Series<CellValue>([2, 1, 2, null, null, 3]);
    expect(series.drop_duplicates().index).toEqual([0, 1, 3, 5]);
    expect(series.drop_duplicates("last").index).toEqual([1, 2, 4, 5]);
    expect(series.drop_duplicates(false).index).toEqual([1, 5]);
    expect(series.duplicated(false).to_list()).toEqual([true, false, true, true, true, false]);
    expect(series.duplicated("last").to_list()).toEqual([true, false, false, true, false, false]);
    expect(series.nunique()).toBe(3);
    expect(series.nunique(false)).toBe(4);
  });

  test("fills and reindex retain source labels and source data", () => {
    const series = new Series<CellValue>([null, 2, null, 6, null], { index: ["a", "b", "c", "d", "e"] });
    expect(series.ffill().to_list()).toEqual([NaN, 2, 2, 6, 6]);
    expect(series.bfill().to_list()).toEqual([2, 2, 6, 6, NaN]);
    expect(series.interpolate().to_list()).toEqual([2, 2, 4, 6, 6]);
    expect(new Series<CellValue>([]).interpolate().empty).toBe(true);
    expect(new Series<CellValue>([null, null]).interpolate().to_list()).toEqual([null, null]);
    expect(series.reindex(["d", "missing", "a"], -1).to_list()).toEqual([6, -1, NaN]);
    expect(series.to_list()).toEqual([NaN, 2, NaN, 6, NaN]);
  });

  test("aggregate routes named reducers and rejects unknown reducers", () => {
    const series = new Series<CellValue>([1, 2, 3, 4]);
    const expected = { sum: 10, mean: 2.5, min: 1, max: 4, count: 4, std: Math.sqrt(5 / 3), var: 5 / 3, median: 2.5, skew: 0, kurt: -1.2, sem: Math.sqrt(5 / 3) / 2, prod: 24 };
    for (const [method, value] of Object.entries(expected)) expect(series.agg(method)).toBeCloseTo(value);
    expect(series.aggregate((values) => values.length)).toBe(4);
    expect(() => series.agg("missing")).toThrow("unknown aggregator");
  });

  test("reverse arithmetic and aliases preserve operand order", () => {
    const series = new Series<CellValue>([2, 4, null]);
    const cases = [
      ["subtract", [-1, 1, NaN]], ["multiply", [6, 12, NaN]],
      ["truediv", [2 / 3, 4 / 3, NaN]], ["floordiv", [0, 1, NaN]],
      ["radd", [5, 7, NaN]], ["rmul", [6, 12, NaN]],
      ["rdiv", [1.5, 0.75, NaN]], ["rtruediv", [1.5, 0.75, NaN]],
      ["rmod", [1, 3, NaN]], ["rpow", [9, 81, NaN]], ["rfloordiv", [1, 0, NaN]],
    ] as const;
    for (const [method, expected] of cases) expect<CellValue[]>(series[method](3).to_list()).toEqual([...expected]);
    expect<CellValue[]>(new Series<CellValue>(["2", null]).neg().to_list()).toEqual([-2, NaN]);
    // pandas raises TypeError for abs() on string values.
    expect(() => new Series<CellValue>(["-2.25", null]).abs()).toThrow("bad operand type for abs()");
    expect(new Series<CellValue>(["2.25", null]).round(1).to_list()).toEqual(["2.25", NaN]);
    // pandas IEEE semantics: NaN >= 3 is False for plain (non-nullable) dtypes.
    expect<CellValue[]>(series.ge(3).to_list()).toEqual([false, true, false]);
  });

  test("pairwise statistics use complete numeric pairs", () => {
    const a = new Series<CellValue>([1, 2, null, 4]);
    const b = new Series<CellValue>([2, 4, 7, 8]);
    expect(a.corr(b)).toBeCloseTo(1);
    expect(a.cov(b)).toBeCloseTo(14 / 3);
    expect(a.dot(b)).toBe(42);
    expect(new Series<CellValue>([null]).corr(new Series<CellValue>([1]))).toBeNull();
    expect(new Series<CellValue>([null]).cov(new Series<CellValue>([1]))).toBeNull();
    expect(new Series<CellValue>([null]).dot(new Series<CellValue>([1]))).toBeNull();
    expect(new Series<CellValue>([1, 1]).corr(new Series<CellValue>([2, 3]))).toBeNull();
    expect(() => a.dot(new Series<CellValue>([1]))).toThrow("length");
    expect(a.first_valid_index()).toBe(0);
    expect(a.last_valid_index()).toBe(3);
    expect(new Series<CellValue>([null]).first_valid_index()).toBeNull();
    expect(new Series<CellValue>([null]).last_valid_index()).toBeNull();
  });
});
