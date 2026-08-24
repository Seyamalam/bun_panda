import { describe, expect, test } from "bun:test";
import { DataFrame } from "../src/dataframe";
import { Series } from "../src/series";
import type { Row } from "../src/types";

describe("DataFrame iteration APIs", () => {
  const df = new DataFrame(
    [{ a: 1, b: "x" }, { a: 2, b: "y" }],
    { index: ["r1", "r2"] }
  );

  test("iterrows returns [index, row] pairs with copied rows", () => {
    const out = [...df.iterrows()];
    expect(out.length).toBe(2);
    expect(out[0]).toEqual(["r1", { a: 1, b: "x" }]);
    (out[0]![1] as Record<string, unknown>).a = 99;
    expect(df.iloc(0)).toEqual({ a: 1, b: "x" });
  });

  test("itertuples includes Index field", () => {
    const out = [...df.itertuples()];
    expect(out[0]).toEqual({ Index: "r1", a: 1, b: "x" });
    expect(out[1]).toEqual({ Index: "r2", a: 2, b: "y" });
  });

  test("items yields column name + Series", () => {
    const out = [...df.items()];
    expect(out.map(([name]) => name)).toEqual(["a", "b"]);
    expect(out[0]![1].to_list()).toEqual([1, 2]);
    expect(out[1]![1].to_list()).toEqual(["x", "y"]);
  });
});

describe("DataFrame.iat / axes / attrs", () => {
  const df = new DataFrame(
    [{ a: 10, b: 20 }, { a: 30, b: 40 }],
    { index: ["p", "q"] }
  );

  test("iat reads cells positionally", () => {
    expect(df.iat[0][0]).toBe(10);
    expect(df.iat[0][1]).toBe(20);
    expect(df.iat[1][0]).toBe(30);
    expect(df.iat[-1][1]).toBe(40); // negative positions allowed like iloc
    expect(df.iat[5][0]).toBeUndefined();
    expect(df.iat[0][9]).toBeUndefined();
  });

  test("axes returns [index labels, column names]", () => {
    expect(df.axes).toEqual([["p", "q"], ["a", "b"]]);
    // copies, not live references
    df.axes[0]!.push("zz");
    expect(df.index).toEqual(["p", "q"]);
  });

  test("attrs getter/setter round-trip", () => {
    expect(df.attrs).toEqual({});
    df.attrs["source"] = "unit-test";
    expect(df.attrs["source"]).toBe("unit-test");
    df.attrs = { note: 42 };
    expect(df.attrs).toEqual({ note: 42 });
  });

  test("attrs is per-frame state", () => {
    const other = new DataFrame([{ a: 1 }]);
    other.attrs["tag"] = "other";
    expect(df.attrs).toEqual({ note: 42 });
  });
});

describe("DataFrame.xs", () => {
  const df = new DataFrame(
    [{ a: 1, b: 2 }, { a: 3, b: 4 }],
    { index: ["r1", "r2"] }
  );

  test("column key returns Series", () => {
    const result = df.xs("a") as Series<number>;
    expect(result.to_list()).toEqual([1, 3]);
    expect(result.name).toBe("a");
  });

  test("index key returns row record", () => {
    expect(df.xs("r2")).toEqual({ a: 3, b: 4 });
  });

  test("unknown key raises", () => {
    expect(() => df.xs("nope")).toThrow();
  });
});

describe("DataFrame.update", () => {
  test("overwrites aligned non-missing cells in place", () => {
    const df = new DataFrame(
      [{ a: 1, b: 2 }, { a: 3, b: 4 }],
      { index: ["r1", "r2"] }
    );
    const other = new DataFrame(
      [{ a: 100, b: null as unknown as number }, { a: 300, c: 7 }],
      { index: ["r1", "r2"] }
    );
    df.update(other);
    expect(df.to_records()).toEqual([
      { a: 100, b: 2 },
      { a: 300, b: 4 },
    ]);
    expect(df.columns).toEqual(["a", "b"]); // no new columns added
  });

  test("misaligned rows are untouched", () => {
    const df = new DataFrame([{ a: 1 }, { a: 2 }], { index: [0, 1] });
    const other = new DataFrame([{ a: 999 }], { index: [7] });
    df.update(other);
    expect(df.get("a").to_list()).toEqual([1, 2]);
  });
});

describe("DataFrame.explode", () => {
  test("blows list cells into one row per element", () => {
    const df = new DataFrame([
      { id: 1, tags: ["a", "b"] },
      { id: 2, tags: [] },
      { id: 3, tags: ["c"] },
      { id: 4, tags: "scalar" },
    ] as unknown as Row[]);
    const out = df.explode("tags");
    expect(out.to_records()).toEqual([
      { id: 1, tags: "a" },
      { id: 1, tags: "b" },
      { id: 3, tags: "c" },
      { id: 4, tags: "scalar" },
    ]);
    expect(out.index).toEqual([0, 0, 2, 3]); // original labels repeated
    expect(out.shape).toEqual([4, 2]);
  });

  test("original frame unchanged and unknown column raises", () => {
    const df = new DataFrame([{ id: 1, tags: ["a"] }] as unknown as Row[]);
    df.explode("tags");
    expect(df.shape).toEqual([1, 2]);
    expect(() => df.explode("missing")).toThrow();
  });
});

describe("DataFrame.dot", () => {
  test("matrix product keeps left index, adopts right columns", () => {
    const left = new DataFrame([{ a: 1, b: 2 }, { a: 3, b: 4 }]);
    const right = new DataFrame([
      { x: 5, y: 6 },
      { x: 7, y: 8 },
    ]);
    const out = left.dot(right);
    // row0: [1*5+2*7, 1*6+2*8] = [19, 22]; row1: [15*? ...] = [43, 50]
    expect(out.to_records()).toEqual([
      { x: 19, y: 22 },
      { x: 43, y: 50 },
    ]);
    expect(out.index).toEqual([0, 1]);
    expect(out.columns).toEqual(["x", "y"]);
  });

  test("shape mismatch raises; missing operands become null", () => {
    const left = new DataFrame([{ a: 1, b: 2 }]);
    expect(() => left.dot(new DataFrame([{ x: 1 }]))).toThrow();
    const right = new DataFrame([
      { x: 10 as number | null },
      { x: null as unknown as number },
    ]);
    expect(left.dot(right).to_records()).toEqual([{ x: null }]);
  });
});

describe("DataFrame.eval", () => {
  const df = new DataFrame(
    [{ a: 1, b: 10 }, { a: 2, b: 20 }, { a: 3, b: null as unknown as number }],
    { index: ["i", "j", "k"] }
  );

  test("arithmetic expression 'a + b'", () => {
    const out = df.eval("a + b");
    expect(out).toBeInstanceOf(Series);
    expect(out.name).toBe("a + b");
    expect(out.to_list()).toEqual([11, 22, null]);
    expect(out.index).toEqual(["i", "j", "k"]);
  });

  test("comparison expression 'a > 2' returns booleans", () => {
    expect(df.eval("a > 2").to_list()).toEqual([false, false, true]);
    expect(df.eval("b >= 20").to_list()).toEqual([false, true, null]);
  });

  test("precedence, unary minus, parens, floor-div, power", () => {
    expect(df.eval("a * 2 + 1").to_list()).toEqual([3, 5, 7]);
    expect(df.eval("(a + 1) * b").to_list()).toEqual([20, 60, null]);
    expect(df.eval("-a + 1").to_list()).toEqual([0, -1, -2]);
    expect(new DataFrame([{ a: 7 }, { a: 8 }]).eval("a // 3").to_list())
      .toEqual([2, 2]);
    expect(new DataFrame([{ a: 2 }, { a: 3 }]).eval("a ** 3").to_list())
      .toEqual([8, 27]);
  });

  test("unknown column raises and no eval() escape possible", () => {
    expect(() => df.eval("a + zz")).toThrow();
    expect(() => df.eval("process.exit(1)")).toThrow();
    expect(() => df.eval(";")).toThrow();
  });
});
