import { describe, expect, test } from "bun:test";
import { DataFrame } from "../src/index";
import { Series } from "../src/series";

/**
 * pandas 3.0.5-verified MultiIndex (Phase 2) coverage: every expectation
 * below was captured from pandas running on the same inputs.
 */

const FRAME = () =>
  new DataFrame(
    { v: [1, 2, 3, 4] },
    {
      index: [["a", 1], ["a", 2], ["b", 1], ["b", 2]],
      indexNames: ["l1", "l2"],
    }
  );

const SERIES = () =>
  new Series([10, 20, 30, 40], {
    index: [["a", 1], ["a", 2], ["b", 1], ["b", 2]],
    indexNames: ["l1", "l2"],
    name: "s",
  });

describe("MultiIndex construction and metadata", () => {
  test("constructor tuple index carries nlevels and names", () => {
    const df = FRAME();
    expect(df.nlevels).toBe(2);
    expect(df.index_names).toEqual(["l1", "l2"]);
    expect(df.index).toEqual([["a", 1], ["a", 2], ["b", 1], ["b", 2]]);
  });

  test("set_index with multiple columns builds a named MultiIndex", () => {
    const df = new DataFrame({
      k1: ["a", "a", "b", "b"],
      k2: [1, 2, 1, 2],
      v: [1, 2, 3, 4],
    }).set_index(["k1", "k2"]);
    expect(df.nlevels).toBe(2);
    expect(df.index_names).toEqual(["k1", "k2"]);
    expect(df.index).toEqual([["a", 1], ["a", 2], ["b", 1], ["b", 2]]);
    expect(df.columns).toEqual(["v"]);
  });

  test("flat indexes report nlevels 1 and a single-element name list", () => {
    const df = new DataFrame({ v: [1] }, { index: ["r"], indexName: "id" });
    expect(df.nlevels).toBe(1);
    expect(df.index_names).toEqual(["id"]);
  });
});

describe("DataFrame.loc on MultiIndex", () => {
  test("outer-level scalar selects a sub-frame with the level dropped", () => {
    // pandas df.loc["a"] -> rows (a,*), index drops to level "l2".
    const out = FRAME().loc("a") as DataFrame;
    expect(out.index).toEqual([1, 2]);
    expect(out.index_names).toEqual(["l2"]);
    expect(out.to_records()).toEqual([{ v: 1 }, { v: 2 }]);
  });

  test("variadic tuple arguments select a full row", () => {
    // pandas df.loc[("a", 1)] -> the row as a Series.
    expect(FRAME().loc("a", 1)).toEqual({ v: 1 });
    expect(FRAME().loc("b", 2)).toEqual({ v: 4 });
  });

  test("array-of-tuples selects exact rows retaining the MultiIndex", () => {
    const out = FRAME().loc([["a", 1], ["b", 2]]) as DataFrame;
    expect(out.index).toEqual([["a", 1], ["b", 2]]);
    expect(out.to_records()).toEqual([{ v: 1 }, { v: 4 }]);
  });

  test("scalar list selects outer-level groups retaining tuple labels", () => {
    // pandas df.loc[["a", "b"]] -> all rows, full MultiIndex kept.
    const out = FRAME().loc(["a", "b"]) as DataFrame;
    expect(out.index).toEqual([["a", 1], ["a", 2], ["b", 1], ["b", 2]]);
  });

  test("missing keys raise KeyError", () => {
    expect(() => FRAME().loc("z")).toThrow();
  });
});

describe("Series.loc on MultiIndex", () => {
  test("outer-level scalar returns a Series with the level dropped", () => {
    const out = SERIES().loc("a") as Series<number>;
    expect(out.to_list()).toEqual([10, 20]);
    expect(out.index).toEqual([1, 2]);
  });

  test("full tuple key returns the scalar", () => {
    expect(SERIES().loc("a", 1)).toBe(10);
    expect(SERIES().loc("b", 2)).toBe(40);
  });

  test("tuple list retains full MultiIndex labels", () => {
    const out = SERIES().loc([["a", 1]]) as Series<number>;
    expect(out.index).toEqual([["a", 1]]);
    expect(out.to_list()).toEqual([10]);
  });
});

describe("xs cross-sections", () => {
  test("DataFrame xs on a named level drops it by default", () => {
    const out = FRAME().xs(1, { level: "l2" }) as DataFrame;
    expect(out.index).toEqual(["a", "b"]);
    expect(out.index_names).toEqual(["l1"]);
    expect(out.to_records()).toEqual([{ v: 1 }, { v: 3 }]);
  });

  test("DataFrame xs keeps the MultiIndex with drop_level=false", () => {
    const out = FRAME().xs(2, { level: 1, drop_level: false }) as DataFrame;
    expect(out.index).toEqual([["a", 2], ["b", 2]]);
  });

  test("Series xs cross-section drops the selected level", () => {
    const out = SERIES().xs(1, { level: "l2" }) as Series<number>;
    expect(out.index).toEqual(["a", "b"]);
    expect(out.to_list()).toEqual([10, 30]);
  });

  test("invalid levels raise a level error", () => {
    expect(() => FRAME().xs(1, { level: 5 })).toThrow(/level/i);
  });
});

describe("unstack / stack", () => {
  test("Series.unstack moves the sorted last level into columns", () => {
    const out = SERIES().unstack();
    expect(out.columns).toEqual([1, 2]);
    expect(out.index).toEqual(["a", "b"]);
    expect(out.values()).toEqual([
      [10, 20],
      [30, 40],
    ]);
  });

  test("Series.unstack(level=0) moves the first level", () => {
    const out = SERIES().unstack(0);
    expect(out.columns).toEqual(["a", "b"]);
    expect(out.index).toEqual([1, 2]);
    expect(out.values()).toEqual([
      [10, 30],
      [20, 40],
    ]);
  });

  test("sparse combinations fill with missing values", () => {
    const s = new Series([1, 2, 3], {
      index: [["a", 1], ["b", 2], ["a", 2]],
      indexNames: ["x", "y"],
    });
    const out = s.unstack();
    expect(out.columns).toEqual([1, 2]);
    expect(out.index).toEqual(["a", "b"]);
    expect(out.values()).toEqual([
      [1, 3],
      [NaN, 2],
    ]);
  });

  test("DataFrame.unstack produces tuple MultiColumn labels", () => {
    const out = FRAME().unstack();
    expect(out.columns).toEqual([["v", 1], ["v", 2]]);
    expect(out.index).toEqual(["a", "b"]);
    expect(out.values()).toEqual([
      [1, 2],
      [3, 4],
    ]);
  });

  test("stack inverts an unstacked frame, restoring index and names", () => {
    const back = FRAME().unstack().stack() as DataFrame;
    expect(back.index).toEqual([["a", 1], ["a", 2], ["b", 1], ["b", 2]]);
    expect(back.index_names).toEqual(["l1", "l2"]);
    expect(back.to_records()).toEqual([{ v: 1 }, { v: 2 }, { v: 3 }, { v: 4 }]);
  });
});

describe("MultiIndex sort_index", () => {
  const unsorted = () =>
    new DataFrame(
      { v: [1, 2, 3, 4] },
      {
        index: [["b", 2], ["a", 2], ["b", 1], ["a", 1]],
        indexNames: ["l1", "l2"],
      }
    );

  test("default sorts tuples lexicographically", () => {
    expect(unsorted().sort_index().index).toEqual([
      ["a", 1],
      ["a", 2],
      ["b", 1],
      ["b", 2],
    ]);
  });

  test("level= sorts by that level first", () => {
    expect(unsorted().sort_index({ level: 1 }).index).toEqual([
      ["a", 1],
      ["b", 1],
      ["a", 2],
      ["b", 2],
    ]);
  });

  test("named levels resolve like positions", () => {
    expect(unsorted().sort_index({ level: "l2" }).index).toEqual(
      unsorted().sort_index({ level: 1 }).index
    );
  });

  test("descending order", () => {
    expect(unsorted().sort_index({ ascending: false }).index).toEqual([
      ["b", 2],
      ["b", 1],
      ["a", 2],
      ["a", 1],
    ]);
  });
});

describe("groupby(level=...)", () => {
  test("as_index=true moves the level keys into the index", () => {
    const out = FRAME().groupby(null, { level: "l1", as_index: true }).sum();
    expect(out.index).toEqual(["a", "b"]);
    expect(out.index_names).toEqual(["l1"]);
    expect(out.columns).toEqual(["v"]);
    expect(out.to_records()).toEqual([{ v: 3 }, { v: 7 }]);
  });

  test("as_index=false keeps level keys as columns", () => {
    const out = FRAME().groupby(null, { level: "l1", as_index: false }).sum();
    expect(out.columns).toEqual(["l1", "v"]);
    expect(out.to_records()).toEqual([
      { l1: "a", v: 3 },
      { l1: "b", v: 7 },
    ]);
  });

  test("multiple levels build a tuple index", () => {
    const out = FRAME()
      .groupby(null, { level: ["l1", "l2"], as_index: true })
      .sum();
    expect(out.index).toEqual([["a", 1], ["a", 2], ["b", 1], ["b", 2]]);
    expect(out.index_names).toEqual(["l1", "l2"]);
  });

  test("numeric level positions resolve like names", () => {
    expect(
      FRAME().groupby(null, { level: 0, as_index: true }).sum().index
    ).toEqual(["a", "b"]);
  });
});

describe("level operations", () => {
  test("droplevel removes a level by position or name", () => {
    expect(FRAME().droplevel(0).index).toEqual([1, 2, 1, 2]);
    expect(SERIES().droplevel("l1").index).toEqual([1, 2, 1, 2]);
  });

  test("swaplevel swaps the last two levels and their names", () => {
    expect(SERIES().swaplevel().index).toEqual([
      [1, "a"],
      [2, "a"],
      [1, "b"],
      [2, "b"],
    ]);
    expect(SERIES().swaplevel().index_names).toEqual(["l2", "l1"]);
  });

  test("reorder_levels permutes levels by list or variadic order", () => {
    const out = SERIES().reorder_levels([1, 0]);
    expect(out.index).toEqual([
      [1, "a"],
      [2, "a"],
      [1, "b"],
      [2, "b"],
    ]);
    expect(out.index_names).toEqual(["l2", "l1"]);
    expect(FRAME().reorder_levels([1, 0]).index).toEqual(out.index);
  });
});
