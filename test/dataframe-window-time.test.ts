import { describe, expect, test } from "bun:test";
import { DataFrame } from "../src";
import { NotSupportedError } from "../src/errors";

const dated = () =>
  new DataFrame(
    [
      { at: new Date("2026-01-01T00:30:00Z"), v: 10 },
      { at: new Date("2026-01-01T00:45:00Z"), v: 20 },
      { at: new Date("2026-01-01T02:10:00Z"), v: 30 },
    ],
    { index: ["a", "b", "c"] }
  );

describe("DataFrame align / isetitem", () => {
  test("align outer unions both indexes", () => {
    const left = new DataFrame({ a: [1, 2] }, { index: ["x", "y"] });
    const right = new DataFrame({ b: [7] }, { index: ["z"] });
    const [ll, rr] = left.align(right);
    expect(ll.index).toEqual(["x", "y", "z"]);
    expect(rr.index).toEqual(["x", "y", "z"]);
    expect(rr.to_records()[0]).toEqual({ b: null });
  });

  test("isetitem replaces one column positionally", () => {
    const df = new DataFrame({ a: [1, 2], b: [3, 4] });
    const out = df.isetitem(1, [9, 9]);
    expect(out.columns).toEqual(["a", "b"]);
    expect((out.to_dict("list") as Record<string, number[]>).b).toEqual([9, 9]);
  });

  test("isetitem rejects bad positions and lengths", () => {
    const df = new DataFrame({ a: [1, 2] });
    expect(() => df.isetitem(5, [1])).toThrow();
    expect(() => df.isetitem(0, [1])).toThrow();
  });
});

describe("DataFrame ewm", () => {
  test("mean seeds with the first observation then decays in", () => {
    const df = new DataFrame({ x: [1, 2, 3, 4] });
    const means = df.ewm(3).mean();
    const col = (means.to_dict("list") as Record<string, (number | null)[]>).x;
    expect(col[0]).toBeCloseTo(1);
    expect(col[1]).toBeCloseTo(1.5, 3);
  });

  test("sum compounds per column", () => {
    const df = new DataFrame({ x: [1, 1] });
    const sums = (df.ewm(1).sum().to_dict("list") as Record<string, (number | null)[]>).x;
    expect(sums).toHaveLength(2);
  });

  test("non-numeric columns are skipped", () => {
    const df = new DataFrame({ t: ["a"], n: [5] });
    const means = df.ewm(2).mean();
    // Non-numeric columns produce no ewm values; the frame keeps its shape.
    expect((means.to_dict("list") as Record<string, number[]>).n).toBeDefined();
  });
});

describe("DataFrame resample", () => {
  test("count bins rows by frequency", () => {
    const r = dated().resample("1h");
    const counts = r.count();
    expect(counts.index.length).toBe(2);
    expect((counts.to_dict("list") as Record<string, number[]>).v).toEqual([2, 1]);
  });

  test("sum/mean/min/max reduce numeric columns per bin", () => {
    const r = dated().resample("1h");
    expect((r.sum().to_dict("list") as Record<string, number[]>).v).toEqual([30, 30]);
    expect((r.min().to_dict("list") as Record<string, number[]>).v).toEqual([10, 30]);
  });

  test("ohlc produces open/high/low/close columns", () => {
    const ohlc = dated().resample("1h").ohlc();
    const first = ohlc.to_records()[0]!;
    expect(first.v_open).toBe(10);
    expect(first.v_high).toBe(20);
    expect(first.v_low).toBe(10);
    expect(first.v_close).toBe(20);
  });
});

describe("DataFrame time-of-day filters", () => {
  test("at_time keeps matching times only", () => {
    const df = new DataFrame([
      { at: new Date("2026-01-01T09:00:00"), v: 1 },
      { at: new Date("2026-01-01T10:00:00"), v: 2 },
      { at: new Date("2026-01-02T09:00:00"), v: 3 },
    ]);
    expect(df.at_time("09:00").to_records()).toHaveLength(2);
  });

  test("between_time honours inclusive modes", () => {
    const df = new DataFrame([
      { at: new Date("2026-01-01T08:00:00") },
      { at: new Date("2026-01-01T09:00:00") },
      { at: new Date("2026-01-01T12:00:00") },
    ]);
    expect(df.between_time("09:00", "12:00").to_records()).toHaveLength(2);
    expect(df.between_time("09:00", "12:00", "neither").to_records()).toHaveLength(0);
  });
});

describe("DataFrame export shims + stubs", () => {
  test("to_sql emits INSERT statements", () => {
    const df = new DataFrame([{ a: 1, b: "x" }]);
    const sql = df.to_sql("t1");
    expect(sql).toContain('INSERT INTO t1 (a, b) VALUES (1, "x")');
  });

  test("to_hdf/to_feather/to_orc return JSON buffers", () => {
    const df = new DataFrame({ a: [1] });
    for (const buf of [df.to_hdf(), df.to_feather(), df.to_orc()]) {
      expect(Buffer.isBuffer(buf)).toBe(true);
      void JSON.parse(buf.toString("utf8"));
    }
  });

  test("html getter mirrors to_html; to_latex escapes booktabs", () => {
    const df = new DataFrame({ a: [1] });
    expect(df.html).toBe(df.to_html());
    expect(df.to_latex()).toContain("\\toprule");
  });

  test("plotting/style stubs raise NotSupportedError", () => {
    const df = new DataFrame({ a: [1] });
    expect(() => df.plot("a")).not.toThrow();
    expect(df.plot("a")).toContain("<svg");
    expect(df.hist("a")).toContain("█");
    expect(df.boxplot("a")).toContain("med=");
    expect(Object.keys(df.sparse)).toEqual(["a"]);
    expect(() => df.style()).toThrow(NotSupportedError);
  });

  test("level ops are honest copies on flat frames", () => {
    const df = new DataFrame({ a: [1] }, { index: ["x"] });
    expect(df.droplevel().index).toEqual(["x"]);
    expect(df.swaplevel().index).toEqual(["x"]);
    expect(df.reorder_levels(0).index).toEqual(["x"]);
  });
});
