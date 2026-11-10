import { describe, expect, test } from "bun:test";
import { DataFrame, NA, Series } from "../src/index";
import type { CellValue, Row } from "../src/types";

// Phase 1 bigint storage: 64-bit integer columns hold exact bigint cells
// whenever any input needs them (bigint input or an out-of-safe-range value).
// Selection, records, equality, and integer-preserving arithmetic stay exact.

const WIDE = 2n ** 60n;          // 1152921504606846976n — above Number.MAX_SAFE_INTEGER
const WIDE2 = 2n ** 60n + 7n;    // not representable as a double
const INT64_MAX = 2n ** 63n - 1n;
const UINT64_MAX = 2n ** 64n - 1n;

describe("bigint construction and inference", () => {
  test("bigint literals infer int64 and store exactly", () => {
    const s = new Series([1n, WIDE, -3n]);
    expect(s.dtype).toBe("int64");
    expect(s.to_list()).toEqual([1n, WIDE, -3n]);
    expect(typeof s.iloc(0)).toBe("bigint");
  });

  test("explicit int64 accepts bigint and number inputs, storing bigint uniformly", () => {
    const s = new Series([1, 2n, WIDE], { dtype: "int64" });
    expect(s.dtype).toBe("int64");
    expect(s.to_list()).toEqual([1n, 2n, WIDE]);
  });

  test("all-safe int64 columns keep plain number cells", () => {
    const s = new Series([1, 2, 3], { dtype: "int64" });
    expect(s.to_list()).toEqual([1, 2, 3]);
    expect(typeof s.iloc(0)).toBe("number");
  });

  test("nullable Int64 keeps NA sentinel and bigint values", () => {
    const s = new Series<CellValue>([WIDE, null, 5n], { dtype: "Int64" });
    expect(s.dtype).toBe("Int64");
    expect(s.to_list()).toEqual([WIDE, NA, 5n]);
  });

  test("uint64 enforces range with bigint bounds", () => {
    expect(new Series([UINT64_MAX], { dtype: "uint64" }).to_list()).toEqual([UINT64_MAX]);
    expect(() => new Series([-1n], { dtype: "uint64" })).toThrow(RangeError);
    expect(() => new Series([2n ** 64n], { dtype: "uint64" })).toThrow(RangeError);
  });

  test("int64 bounds are enforced for bigint inputs", () => {
    expect(new Series([INT64_MAX, -(2n ** 63n)], { dtype: "int64" }).to_list()).toEqual([INT64_MAX, -(2n ** 63n)]);
    expect(() => new Series([2n ** 63n], { dtype: "int64" })).toThrow(RangeError);
    expect(() => new Series([-(2n ** 63n) - 1n], { dtype: "int64" })).toThrow(RangeError);
  });

  test("numeric strings cast exactly into 64-bit storage", () => {
    const s = new Series<CellValue>(["1152921504606846977"], { dtype: "int64" });
    expect(s.to_list()).toEqual([1152921504606846977n]);
  });

  test("unsafe number inputs store the double's exact integer value", () => {
    // 2**53 + 1 is not representable; the literal already rounded to 2**53+2,
    // so the stored bigint is that exact double value — no silent throw.
    const s = new Series<CellValue>([Number.MAX_SAFE_INTEGER + 1], { dtype: "int64" });
    expect(s.to_list()).toEqual([9007199254740992n]);
  });

  test("non-integer and out-of-bounds inputs still raise", () => {
    expect(() => new Series([1.5], { dtype: "int64" })).toThrow();
    expect(() => new Series([NaN], { dtype: "int64" })).toThrow();
    expect(() => new Series([Infinity], { dtype: "int64" })).toThrow();
  });
});

describe("selection and output APIs preserve bigint", () => {
  const frame = new DataFrame({ wide: [WIDE, WIDE2, 4n], small: [1, 2, 3] }, { index: ["a", "b", "c"] });

  test("column dtype inference and to_records", () => {
    expect(frame.dtypes().wide).toBe("int64");
    expect(frame.to_records()).toEqual([
      { wide: WIDE, small: 1 },
      { wide: WIDE2, small: 2 },
      { wide: 4n, small: 3 },
    ]);
  });

  test("loc/iloc return the exact bigint", () => {
    expect((frame.iloc(1) as Row).wide).toBe(WIDE2);
    expect((frame.loc("c") as Row).wide).toBe(4n);
    expect(frame.get("wide").iloc(0)).toBe(WIDE);
    expect(frame.get("wide").loc("b")).toBe(WIDE2);
  });

  test("Series to_list/values carry bigint unchanged", () => {
    const wide = frame.get("wide");
    expect(wide.to_list()).toEqual([WIDE, WIDE2, 4n]);
    expect(wide.values).toEqual([WIDE, WIDE2, 4n]);
  });

  test("head/tail/filter keep bigint cells", () => {
    expect(frame.head(1).to_records()).toEqual([{ wide: WIDE, small: 1 }]);
    expect(frame.tail(1).to_records()).toEqual([{ wide: 4n, small: 3 }]);
    const filtered = frame.filter((row) => (row.small as number) > 1);
    expect(filtered.to_records().map((r) => r.wide)).toEqual([WIDE2, 4n]);
  });

  test("equals treats number and bigint cells as numerically equal", () => {
    const left = new Series([5n, WIDE]);
    const right = new Series([5n, WIDE]);
    expect(left.equals(right)).toBe(true);
    const numbers = new Series([5, 6], { dtype: "object" });
    expect(numbers.equals(new Series([5n, 6n], { dtype: "object" }))).toBe(true);
  });
});

describe("comparisons on bigint columns", () => {
  const s = new Series([WIDE, 5n, -2n]);

  test("ordering comparisons are exact above 2^53", () => {
    expect(s.gt(5).to_list()).toEqual([true, false, false]);
    expect(s.ge(WIDE).to_list()).toEqual([true, false, false]);
    expect(s.lt(0).to_list()).toEqual([false, false, true]);
    // between a number and bigint bound stays exact
    expect(s.between(4n, WIDE2).to_list()).toEqual([true, true, false]);
  });

  test("equality joins numeric-equal number and bigint cells", () => {
    expect(s.eq(WIDE).to_list()).toEqual([true, false, false]);
    expect(s.eq(5).to_list()).toEqual([false, true, false]);
    expect(s.ne(5).to_list()).toEqual([true, false, true]);
    expect(new Series([WIDE2]).eq(WIDE).to_list()).toEqual([false]);
  });

  test("frame comparisons work element-wise", () => {
    const f = new DataFrame({ v: [WIDE, 2n] });
    expect(f.gt(1).to_records()).toEqual([{ v: true }, { v: true }]);
    expect(f.eq(WIDE).to_records()).toEqual([{ v: true }, { v: false }]);
  });
});

describe("exact integer arithmetic", () => {
  const s = new Series([WIDE, WIDE2, -4n]);

  test("add/sub/mul stay exact in the 2^60 range", () => {
    expect(s.add(1n).to_list()).toEqual([WIDE + 1n, WIDE2 + 1n, -3n]);
    expect(s.sub(WIDE).to_list()).toEqual([0n, 7n, -4n - WIDE]);
    expect(s.mul(3n).to_list()).toEqual([WIDE * 3n, WIDE2 * 3n, -12n]);
    expect(s.add(s).to_list()).toEqual([WIDE * 2n, WIDE2 * 2n, -8n]);
  });

  test("scalar number operands join the bigint path when integer", () => {
    expect(s.add(2).to_list()).toEqual([WIDE + 2n, WIDE2 + 2n, -2n]);
    expect(s.mul(0).to_list()).toEqual([0n, 0n, 0n]);
  });

  test("result dtype stays int64 for integer-preserving ops", () => {
    expect(s.add(1n).dtype).toBe("int64");
    expect(s.sub(s).dtype).toBe("int64");
    expect(s.mul(2).dtype).toBe("int64");
  });

  test("floordiv and mod use Python semantics (floor / divisor sign)", () => {
    const v = new Series([-7n, 7n]);
    expect(v.floordiv(2n).to_list()).toEqual([-4n, 3n]); // -7 // 2 = -4, not -3
    expect(v.mod(2n).to_list()).toEqual([1n, 1n]);       // -7 % 2 = 1 (divisor sign)
    expect(s.floordiv(0n).to_list()).toEqual([0n, 0n, 0n]); // int floordiv-by-zero -> 0
  });

  test("pow stays exact for non-negative exponents and widens on negative", () => {
    expect(new Series([2n]).pow(60n).to_list()).toEqual([WIDE]);
    const widened = new Series([2n]).pow(-1n);
    expect(widened.to_list()).toEqual([0.5]);
    expect(widened.dtype).toBe("float64");
  });

  test("division widens to float64 (approximate above 2^53, matching pandas)", () => {
    const d = new Series([WIDE]).div(2n);
    expect(d.dtype).toBe("float64");
    expect(d.to_list()).toEqual([Number(WIDE) / 2]);
  });

  test("DataFrame arithmetic propagates bigint through the frame", () => {
    const f = new DataFrame({ v: [WIDE, 2n] });
    expect(f.add(1n).to_records()).toEqual([{ v: WIDE + 1n }, { v: 3n }]);
    expect(f.mul(2).to_records()).toEqual([{ v: WIDE * 2n }, { v: 4n }]);
    // Results past int64 range raise RangeError rather than silently wrapping
    // (numpy wraps; we chose the explicit failure for Phase 1).
    expect(() => f.mul(f)).toThrow(RangeError);
  });

  test("sum and prod reduce exactly on all-bigint columns", () => {
    expect(new Series([WIDE, WIDE2]).sum()).toBe(WIDE + WIDE2);
    expect(new Series([WIDE, 3n]).prod()).toBe(WIDE * 3n);
    expect(new DataFrame({ v: [WIDE, WIDE2] }).sum().v).toBe(WIDE + WIDE2);
  });

  test("cumsum stays in bigint while inputs are integers", () => {
    const c = new Series([WIDE, 1n, 1n]).cumsum();
    expect(c.to_list()).toEqual([WIDE, WIDE + 1n, WIDE + 2n]);
    expect(c.dtype).toBe("int64");
  });

  test("diff widens to float64 like pandas (leading NaN)", () => {
    const d = new Series([WIDE, WIDE2, WIDE2 + 10n]).diff();
    expect(d.dtype).toBe("float64");
    const frameDiff = new DataFrame({ v: [WIDE, WIDE2] }).diff();
    expect(frameDiff.to_records()[1]!.v).toBe(7);
  });
});
