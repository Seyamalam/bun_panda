import { describe, expect, test } from "bun:test";
import { bdate_range, date_range, isin, isnull, map, notnull, to_datetime, to_numeric, to_timedelta, unique, value_counts } from "../src/index";

describe("datetime and conversion contracts", () => {
  test("fixed frequencies advance by the specified milliseconds", () => {
    const start = new Date("2026-01-01T00:00:00Z");
    for (const [freq, milliseconds] of [[2, 172800000], ["2D", 172800000], ["W", 604800000], ["h", 3600000], ["2hr", 7200000], ["hour", 3600000], ["min", 60000], ["T", 60000], ["s", 1000]] as const) {
      const dates = date_range({ start, periods: 3, freq });
      expect(dates.map((d) => d.getTime() - start.getTime())).toEqual([0, milliseconds, 2 * milliseconds]);
    }
    expect(date_range(start, new Date(start.getTime() + 172800000)).length).toBe(3);
    expect(start.toISOString()).toBe("2026-01-01T00:00:00.000Z");
  });

  test("calendar frequencies retain the requested day where it exists", () => {
    const start = new Date(2026, 0, 15);
    expect(date_range({ start, periods: 3, freq: "M" }).map((d) => [d.getMonth(), d.getDate()])).toEqual([[0, 15], [1, 15], [2, 15]]);
    expect(date_range({ start: new Date(2026, 0, 31), periods: 2, freq: "M" }).map((d) => d.getDate())).toEqual([31, 28]);
    for (const freq of ["Y", "A"]) expect(date_range({ start, periods: 3, freq }).map((d) => d.getFullYear())).toEqual([2026, 2027, 2028]);
    expect(() => date_range({ start: "invalid", periods: 1 })).toThrow("valid start");
    expect(() => date_range({ start })).toThrow("end or periods");
    expect(() => date_range({ start, end: "invalid" })).toThrow("valid end");
    expect(() => date_range({ start, periods: 3, freq: "fortnight" })).toThrow("Unsupported freq");
  });

  test("business-day ranges skip weekends and honor an inclusive end", () => {
    const friday = new Date(2026, 8, 11);
    expect(bdate_range({ start: friday, periods: 3 }).map((d) => d.getDate())).toEqual([11, 14, 15]);
    expect(bdate_range({ start: new Date(2026, 8, 12), periods: 2 }).map((d) => d.getDate())).toEqual([14, 15]);
    expect(bdate_range({ start: friday, end: new Date(2026, 8, 15) }).map((d) => d.getDate())).toEqual([11, 14, 15]);
    expect(bdate_range({ start: friday, end: new Date(2026, 8, 13) }).map((d) => d.getDate())).toEqual([11]);
    expect(bdate_range({ start: friday, periods: 0 })).toEqual([]);
    expect(() => bdate_range({ start: "invalid", periods: 1 })).toThrow("valid start");
  });

  test("duration units include milliseconds without interpreting them as minutes", () => {
    expect(to_timedelta([2, "1d 2h 3m 4s 5ms", "1.5h", "250ms"])).toEqual([2000, 93784005, 5400000, 250]);
  });

  test("numeric conversion honors error policy and converts booleans", () => {
    expect(to_numeric(["12", "1.5", true, false, 9])).toEqual([12, 1.5, 1, 0, 9]);
    expect(to_numeric(["no", null, undefined]).every(Number.isNaN)).toBe(true);
    expect(() => to_numeric(["no"], { errors: "raise" })).toThrow("Unable to parse");
    expect(() => to_numeric([null], { errors: "raise" })).toThrow("Unable to parse");
    expect(to_datetime([0, "2026-01-01", null, "no"]).map((d) => d?.toISOString() ?? null)).toEqual(["1970-01-01T00:00:00.000Z", "2026-01-01T00:00:00.000Z", null, null]);
  });

  test("array helpers preserve ordering, missingness and count normalization", () => {
    expect(unique([3, 1, 3, null, 2])).toEqual([3, 1, 2]);
    expect(isin([1, 2, null], [2, null])).toEqual([false, true, true]);
    expect(isnull([1, null, undefined, NaN])).toEqual([false, true, true, true]);
    expect(notnull([1, null, undefined, NaN])).toEqual([true, false, false, false]);
    expect(map([1, 2, 3], (value) => value * 2)).toEqual([2, 4, 6]);
    expect(value_counts(["b", "a", "b", null]).to_dict()).toEqual({ b: 2, a: 1 });
    expect(value_counts(["b", "a", "b", null], { dropna: false, normalize: true, ascending: true }).to_dict()).toEqual({ null: 0.25, a: 0.25, b: 0.5 });
  });
});
