import { describe, expect, test } from "bun:test";
import {
  cloneRow,
  coerceValueToDType,
  compareCellValues,
  inferColumnDType,
  isMissing,
  isNumber,
  numericValues,
  range,
  std,
} from "../src/utils";

describe("utils.range", () => {
  test("creates zero-based sequence", () => {
    expect(range(5)).toEqual([0, 1, 2, 3, 4]);
  });

  test("handles zero length", () => {
    expect(range(0)).toEqual([]);
  });
});

describe("utils.cloneRow", () => {
  test("clones all keys when columns omitted", () => {
    const input = { a: 1, b: "x", c: true };
    const out = cloneRow(input);
    expect(out).toEqual(input);
    expect(out).not.toBe(input);
  });

  test("clones only requested columns", () => {
    const input = { a: 1, b: "x", c: true };
    const out = cloneRow(input, ["b", "c"]);
    expect(out).toEqual({ b: "x", c: true });
  });
});

describe("utils missing and numeric checks", () => {
  test("isMissing recognizes nullish only", () => {
    expect(isMissing(null)).toBe(true);
    expect(isMissing(undefined)).toBe(true);
    expect(isMissing(0)).toBe(false);
    expect(isMissing("")).toBe(false);
  });

  test("isNumber recognizes finite numbers only", () => {
    expect(isNumber(10)).toBe(true);
    expect(isNumber(Number.NaN)).toBe(false);
    expect(isNumber(Infinity)).toBe(false);
    expect(isNumber("10")).toBe(false);
  });

  test("numericValues filters mixed arrays", () => {
    expect(numericValues([1, "2", 3, null, true, 4])).toEqual([1, 3, 4]);
  });
});

describe("utils.compareCellValues", () => {
  test("orders numbers ascending", () => {
    expect(compareCellValues(1, 2)).toBeLessThan(0);
    expect(compareCellValues(2, 1)).toBeGreaterThan(0);
  });

  test("puts missing values last", () => {
    expect(compareCellValues(null, 1)).toBeGreaterThan(0);
    expect(compareCellValues(1, undefined)).toBeLessThan(0);
  });

  test("compares dates by time", () => {
    const a = new Date("2026-01-01T00:00:00.000Z");
    const b = new Date("2026-01-02T00:00:00.000Z");
    expect(compareCellValues(a, b)).toBeLessThan(0);
  });

  test("falls back to lexical string compare for mixed values", () => {
    expect(compareCellValues("10", 2)).toBeLessThan(0);
  });
});

describe("utils.std", () => {
  test("returns sample stddev", () => {
    const out = std([2, 4, 4, 4, 5, 5, 7, 9]);
    expect(out).not.toBeNull();
    expect(Number(out!.toFixed(6))).toBe(2.13809);
  });

  test("returns null when n <= 1", () => {
    expect(std([])).toBeNull();
    expect(std([1])).toBeNull();
  });
});

describe("utils.coerceValueToDType", () => {
  test("coerces to number", () => {
    expect(coerceValueToDType("12.5", "number")).toBe(12.5);
    expect(coerceValueToDType(true, "number")).toBe(1);
    expect(coerceValueToDType("bad", "number")).toBeNull();
  });

  test("coerces to boolean", () => {
    expect(coerceValueToDType("true", "boolean")).toBe(true);
    expect(coerceValueToDType("0", "boolean")).toBe(false);
    expect(coerceValueToDType(3, "boolean")).toBe(true);
  });

  test("coerces to date", () => {
    expect(coerceValueToDType("2026-01-01", "date")).toBeInstanceOf(Date);
    expect(coerceValueToDType("not-a-date", "date")).toBeNull();
  });

  test("coerces to string", () => {
    expect(coerceValueToDType(99, "string")).toBe("99");
  });
});

describe("utils.inferColumnDType", () => {
  test("infers number/string/boolean/date", () => {
    expect(inferColumnDType([1, 2, null])).toBe("number");
    expect(inferColumnDType(["a", "b", null])).toBe("string");
    expect(inferColumnDType([true, false, null])).toBe("boolean");
    expect(inferColumnDType([new Date(), null])).toBe("date");
  });

  test("infers unknown and mixed", () => {
    expect(inferColumnDType([null, undefined])).toBe("unknown");
    expect(inferColumnDType([1, "x"])).toBe("mixed");
  });
});
