import { describe, expect, test } from "bun:test";
import {
  DataFrame,
  Interval,
  Series,
  align,
  array,
  melt,
  melt_frame,
  pivot,
  pivot_frame,
} from "../src/index";

describe("Interval", () => {
  test("contains respects closedness", () => {
    const right = new Interval(0, 10);
    expect(right.closed).toBe("right");
    expect(right.contains(0)).toBe(false);
    expect(right.contains(5)).toBe(true);
    expect(right.contains(10)).toBe(true);

    const left = new Interval(0, 10, "left");
    expect(left.contains(0)).toBe(true);
    expect(left.contains(10)).toBe(false);

    const both = new Interval(0, 10, "both");
    expect(both.contains(0)).toBe(true);
    expect(both.contains(10)).toBe(true);

    const neither = new Interval(0, 10, "neither");
    expect(neither.contains(0)).toBe(false);
    expect(neither.contains(10)).toBe(false);

    expect(right.contains(-1)).toBe(false);
    expect(right.contains(11)).toBe(false);
  });

  test("contains accepts sub-intervals", () => {
    const outer = new Interval(0, 10, "both");
    expect(outer.contains(new Interval(2, 4))).toBe(true);
    expect(outer.contains(new Interval(0, 10, "neither"))).toBe(true);
    // (0, 10] is not contained in [1, 9].
    expect(new Interval(1, 9).contains(new Interval(0, 10, "right"))).toBe(false);
  });

  test("overlaps honours shared points", () => {
    expect(new Interval(0, 5).overlaps(new Interval(3, 8))).toBe(true);
    expect(new Interval(0, 5).overlaps(new Interval(6, 8))).toBe(false);
    // Shared endpoint only: counts when each side is closed there
    // (first interval's upper, second interval's lower).
    expect(new Interval(0, 5, "right").overlaps(new Interval(5, 9, "left"))).toBe(true);
    expect(new Interval(0, 5, "both").overlaps(new Interval(5, 9, "left"))).toBe(true);
    // (5, 9] excludes 5, so it cannot touch [0, 5].
    expect(new Interval(0, 5).overlaps(new Interval(5, 9))).toBe(false);
    expect(new Interval(0, 5, "neither").overlaps(new Interval(5, 9, "left"))).toBe(false);
  });

  test("equals and toString", () => {
    expect(new Interval(0, 1).equals(new Interval(0, 1))).toBe(true);
    expect(new Interval(0, 1).equals(new Interval(0, 1, "left"))).toBe(false);
    expect(new Interval(0, 1).equals(new Interval(0, 2))).toBe(false);
    expect(String(new Interval(0, 1))).toBe("Interval(0, 1, closed='right')");
  });
});

describe("bare-name reshape wrappers", () => {
  const frame = new DataFrame([
    { id: "a", x: 1, y: 2 },
    { id: "b", x: 3, y: 4 },
  ]);

  test("melt delegates to DataFrame.melt", () => {
    const long = melt(frame, { id_vars: ["id"] });
    expect([...long.columns]).toEqual(["id", "variable", "value"]);
    expect(long.shape[0]).toBe(4);
    // Alias kept for the previous spelling.
    expect(melt_frame(frame, { id_vars: ["id"] }).shape).toEqual(long.shape);
  });

  test("pivot delegates to DataFrame.pivot", () => {
    const long = melt(frame, { id_vars: ["id"] });
    const wide = pivot(long, { index: "id", columns: "variable", values: "value" });
    expect([...wide.columns].sort()).toEqual(["id", "x", "y"]);
    expect(wide.shape[0]).toBe(2);
    expect(pivot_frame(long, { index: "id", columns: "variable", values: "value" }).shape).toEqual(
      wide.shape
    );
  });
});

describe("align helper", () => {
  test("Series inputs on union index", () => {
    const a = new Series([1, 2, 3], { index: ["a", "b", "c"], name: "s" }) as Series<number>;
    const b = new Series([4, 5], { index: ["c", "d"], name: "t" }) as Series<number>;
    const [la, lb] = align(a, b);
    expect(la.index).toEqual(["a", "b", "c", "d"]);
    expect(lb.index).toEqual(["a", "b", "c", "d"]);
    expect(lb.values[0]).toBeNull();
    expect(la.values[3]).toBeNull();
  });

  test("Series inputs on inner join", () => {
    const a = new Series([1, 2, 3], { index: ["a", "b", "c"] }) as Series<number>;
    const b = new Series([4, 5], { index: ["b", "z"] }) as Series<number>;
    const [la, lb] = align(a, b, "inner");
    expect(la.index).toEqual(["b"]);
    expect(lb.index).toEqual(["b"]);
    expect(la.values[0]).toBe(2);
    expect(lb.values[0]).toBe(4);
  });

  test("DataFrame inputs align index and columns", () => {
    const left = new DataFrame([{ a: 1, b: 2 }], { index: [0] });
    const right = new DataFrame([{ a: 9, c: 7 }], { index: [0] });
    const [rl, rr] = align(left, right, "outer");
    expect([...rl.columns].sort()).toEqual(["a", "b", "c"]);
    expect([...rr.columns].sort()).toEqual(["a", "b", "c"]);
    expect(rr.at(0, "b")).toBeNull();
    expect(rl.at(0, "c")).toBeNull();
  });

  test("mismatched operand kinds throw", () => {
    const mixed = align as unknown as (a: unknown, b: unknown) => unknown;
    expect(() => mixed(new Series([1]), new DataFrame([{ a: 1 }]))).toThrow();
  });
});

describe("array namespace", () => {
  test("to_json/from_json round-trip", () => {
    const values = [1, "two", true, null];
    expect(array.from_json(array.to_json(values))).toEqual(values);
  });

  test("from_json rejects non-array JSON", () => {
    expect(() => array.from_json('{"a":1}')).toThrow();
  });
});
