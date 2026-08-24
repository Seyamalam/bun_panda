import { describe, expect, test } from "bun:test";
import { DataFrame } from "../src";

describe("DataFrame combine/compare", () => {
  test("combine aligns on index union and calls fn per column", () => {
    const left = new DataFrame({ a: [1, 2], b: [3, 4] }, { index: ["x", "y"] });
    const right = new DataFrame({ a: [10, 20] }, { index: ["y", "z"] });
    const combined = left.combine(right, (a, b) =>
      a.map((v, i) => Number(v ?? 0) + Number(b[i] ?? 0))
    );
    expect(combined.keys()).toEqual(["a", "b"]);
    expect(combined.to_dict("list")).toEqual({
      a: [1, 12, 20],
      b: [3, 4, 0],
    });
    expect(combined.at("z" as never, "a")).toBe(20);
  });

  test("combine broadcasts scalar returns", () => {
    const df = new DataFrame({ a: [1, 2] });
    const out = df.combine(new DataFrame({ a: [9, 9] }), () => -1);
    expect(out.to_dict("list")).toEqual({ a: [-1, -1] });
  });

  test("combine_first fills missing from the other frame", () => {
    const left = new DataFrame(
      [{ a: 1, b: null }, { a: null, b: 4 }],
      { index: ["x", "y"] }
    );
    const right = new DataFrame([{ a: 7, b: 8 }], { index: ["x"] });
    const merged = left.combine_first(right);
    expect(merged.to_records()).toEqual([
      { a: 1, b: 8 },
      { a: null, b: 4 },
    ]);
    // right-only index labels are appended
    const rightOnly = new DataFrame([{ a: 5 }], { index: ["z"] });
    expect(left.combine_first(rightOnly).to_records().at(-1)).toEqual({ a: 5, b: null });
  });

  test("compare returns only differing rows with _other pairs", () => {
    const base = new DataFrame([{ a: 1, b: "x" }, { a: 2, b: "y" }], { index: [0, 1] });
    const other = new DataFrame([{ a: 1, b: "x" }, { a: 9, b: "z" }], { index: [0, 1] });
    const diff = base.compare(other);
    expect(diff.columns).toEqual(["a", "a_other", "b", "b_other"]);
    expect(diff.to_records()).toEqual([{ a: 2, a_other: 9, b: "y", b_other: "z" }]);
    const everything = base.compare(other, { equal_values: true });
    expect(everything.shape).toEqual([2, 4]);
  });

  test("compare rejects non-identical labels like pandas", () => {
    const a = new DataFrame({ x: [1] });
    const b = new DataFrame({ x: [1], y: [2] });
    expect(() => a.compare(b)).toThrow(/identically-labeled/);
  });
});

describe("DataFrame dtype conversion", () => {
  test("convert_dtypes coerces numeric and boolean strings everywhere", () => {
    const df = new DataFrame({ a: ["1", "2.5"], b: ["true", "false"], c: [1, 2] });
    const converted = df.convert_dtypes();
    expect(converted.to_dict("list")).toEqual({ a: [1, 2.5], b: [true, false], c: [1, 2] });
  });

  test("infer_objects only touches mixed columns", () => {
    const df = new DataFrame({
      mixed: ["1", "2"],
      text: ["a", "b"],
      nums: [3, 4],
    });
    const inferred = df.infer_objects();
    expect(inferred.to_dict("list")).toEqual({
      mixed: [1, 2],
      text: ["a", "b"],
      nums: [3, 4],
    });
  });
});

describe("DataFrame corrwith / reindex_like", () => {
  test("corrwith correlates shared numeric columns by index label", () => {
    const left = new DataFrame({ a: [1, 2, 3], b: [1, 2, 3], c: ["x", "y", "z"] }, { index: [0, 1, 2] });
    const right = new DataFrame({ a: [1, 2, 3], b: [3, 2, 1] }, { index: [0, 1, 2] });
    const result = left.corrwith(right);
    expect(result.a).toBeCloseTo(1);
    expect(result.b).toBeCloseTo(-1);
    expect(result.c).toBeUndefined();
  });

  test("reindex_like reshapes to the other frame's labels with nulls", () => {
    const df = new DataFrame({ a: [1, 2], b: [3, 4] }, { index: ["x", "y"] });
    const target = new DataFrame({ a: [9, 8], z: [0, 1] }, { index: ["y", "q"] });
    const reindexed = df.reindex_like(target);
    expect(reindexed.keys()).toEqual(["a", "z"]);
    expect(reindexed.to_records()).toEqual([
      { a: 2, z: null },
      { a: null, z: null },
    ]);
  });
});

describe("DataFrame stack/unstack", () => {
  test("stack pivots columns into long rows, dropping NaN by default", () => {
    const df = new DataFrame({ x: [1, null], y: [3, 4] }, { index: ["r1", "r2"] });
    const long = df.stack();
    expect(long.columns).toEqual(["index", "column", "value"]);
    expect(long.to_records()).toEqual([
      { index: "r1", column: "x", value: 1 },
      { index: "r1", column: "y", value: 3 },
      { index: "r2", column: "y", value: 4 },
    ]);
    const keepNa = df.stack(false);
    expect(keepNa.shape).toEqual([4, 3]);
  });

  test("unstack inverts stack for single-level frames", () => {
    const df = new DataFrame({ x: [1, 2], y: [3, 4] }, { index: ["r1", "r2"] });
    const restored = df.stack().unstack();
    expect(restored.keys()).toEqual(["index", "x", "y"]);
    expect(restored.to_records()).toEqual([
      { index: "r1", x: 1, y: 3 },
      { index: "r2", x: 2, y: 4 },
    ]);
    expect(restored.index).toEqual(["r1", "r2"]);
  });

  test("unstack rejects frames that are not 3-column long format", () => {
    const wide = new DataFrame({ a: [1], b: [2] });
    expect(() => wide.unstack()).toThrow(/exactly 3 columns/);
  });
});

describe("DataFrame factories and shims", () => {
  test("from_dict accepts both dict-of-lists and records list", () => {
    const fromLists = DataFrame.from_dict({ a: [1, 2], b: [3, 4] });
    const fromRecords = DataFrame.from_dict([{ a: 1, b: 3 }, { a: 2, b: 4 }]);
    expect(fromLists.equals(fromRecords)).toBe(true);
  });

  test("from_records builds a frame from row objects", () => {
    const df = DataFrame.from_records([{ p: 1, q: 2 }], { index: ["row0"] });
    expect(df.index).toEqual(["row0"]);
    expect(df.keys()).toEqual(["p", "q"]);
  });

  test("to_pickle serializes to a JSON Buffer", () => {
    const df = new DataFrame({ a: [1, 2] }, { index: ["x", "y"] });
    const buf = df.to_pickle();
    expect(buf).toBeInstanceOf(Buffer);
    const parsed = JSON.parse(buf.toString("utf8"));
    expect(parsed.columns).toEqual(["a"]);
    expect(parsed.index).toEqual(["x", "y"]);
    expect(parsed.rows).toEqual([{ a: 1 }, { a: 2 }]);
  });

  test("to_period/to_timestamp are identity shims", () => {
    const df = new DataFrame({ a: [1] });
    expect(df.to_period().equals(df)).toBe(true);
    expect(df.to_timestamp().equals(df)).toBe(true);
    expect(df.to_period()).not.toBe(df);
  });

  test("set_flags validates duplicate labels and returns a copy", () => {
    const unique = new DataFrame({ a: [1, 2] }, { index: ["x", "y"] });
    expect(unique.set_flags().equals(unique)).toBe(true);
    expect(unique.set_flags({ allows_duplicate_labels: false })).not.toBe(unique);
    const duped = new DataFrame({ a: [1, 2] }, { index: ["x", "x"] });
    expect(() => duped.set_flags({ allows_duplicate_labels: false })).toThrow(/duplicates/);
    expect(duped.set_flags().equals(duped)).toBe(true);
  });
});
