import { describe, expect, test } from "bun:test";
import { DataFrame, DatetimeIndex, NaT, Series, Timestamp } from "../src/index";
import type { CellValue } from "../src/types";

// Naive wall clock 2021-01-01 00:00 (stored as the epoch-ms of the naive
// reading, like pandas datetime64[ns] without a zone).
const NAIVE = new Date("2021-01-01T00:00:00.000Z");

describe("datetime64 dtype declaration", () => {
  test("Series declares datetime64[ns] and stores Date instants", () => {
    const s = new Series<CellValue>([NAIVE, "2021-01-02T00:00:00Z"], { dtype: "datetime64[ns]" });
    expect(s.dtype).toBe("datetime64[ns]");
    const values = s.to_list();
    expect(values[0]).toBeInstanceOf(Date);
    expect((values[0] as Date).getTime()).toBe(NAIVE.getTime());
    expect((values[1] as Date).toISOString()).toBe("2021-01-02T00:00:00.000Z");
  });

  test("datetime64 and Datetime64 spellings are accepted", () => {
    expect(new Series<CellValue>([NAIVE], { dtype: "datetime64" }).dtype).toBe("datetime64");
    expect(new Series<CellValue>([NAIVE], { dtype: "Datetime64" }).dtype).toBe("Datetime64");
  });

  test("DataFrame astype declares datetime64[ns] per column", () => {
    const df = new DataFrame([
      { t: "2021-01-01T00:00:00.000Z", n: 1 },
    ]).astype({ t: "datetime64[ns]" });
    expect(df.dtypes().t).toBe("datetime64[ns]");
    const col = df.get("t");
    expect(col.dtype).toBe("datetime64[ns]");
    expect(col.iloc(0)).toBeInstanceOf(Date);
  });

  test("other units keep their declared spelling", () => {
    const s = new Series<CellValue>([NAIVE], { dtype: "datetime64[s]" });
    expect(s.dtype).toBe("datetime64[s]");
  });
});

describe("NaT as the datetime missing value", () => {
  test("datetime64 stores NaT while inferred date keeps null", () => {
    const declared = new Series<CellValue>([NAIVE, null], { dtype: "datetime64[ns]" });
    const inferred = new Series<CellValue>([NAIVE, null]); // legacy "date" inference
    expect(declared.dtype).toBe("datetime64[ns]");
    expect(inferred.dtype).toBe("date");
    expect(declared.to_list()[1]).toBe(NaT);
    expect(inferred.to_list()[1]).toBeNull();
    // isna treats both as missing, but the stored sentinels stay distinct.
    expect(declared.isna().to_list()).toEqual([false, true]);
    expect(inferred.isna().to_list()).toEqual([false, true]);
    expect(NaT === null).toBe(false);
  });

  test("NaT stays distinct from null/None in equality paths", () => {
    const nat = new Series<CellValue>([NaT], { dtype: "object" });
    const none = new Series<CellValue>([null], { dtype: "object" });
    expect(nat.equals(none)).toBe(false);
    expect(nat.equals(nat.copy())).toBe(true);
  });

  test("missing values in datetime64 columns are counted and dropped like pandas NaT", () => {
    const s = new Series<CellValue>([NAIVE, NaT, undefined], { dtype: "datetime64[ns]" });
    expect(s.to_list().slice(1)).toEqual([NaT, NaT]);
    expect(s.count()).toBe(1);
    expect(s.hasnans()).toBe(true);
    expect(s.dropna().to_list()).toEqual([NAIVE]);
  });
});

describe("Series.tz_localize", () => {
  test("localizing naive values produces the correct UTC instants", () => {
    const s = new Series<CellValue>([NAIVE], { dtype: "datetime64[ns]" });
    const kolkata = s.tz_localize("UTC+05:30");
    // 00:00 at UTC+05:30 == 18:30 UTC the previous day.
    expect((kolkata.to_list()[0] as Date).toISOString()).toBe("2020-12-31T18:30:00.000Z");
    expect(kolkata.dtype).toBe("datetime64[ns, UTC+05:30]");

    const eastern = s.tz_localize("US/Eastern");
    // 00:00 EST (UTC-05:00) == 05:00 UTC.
    expect((eastern.to_list()[0] as Date).toISOString()).toBe("2021-01-01T05:00:00.000Z");
    expect(eastern.dtype).toBe("datetime64[ns, US/Eastern]");

    const tokyo = s.tz_localize("Asia/Tokyo");
    expect((tokyo.to_list()[0] as Date).toISOString()).toBe("2020-12-31T15:00:00.000Z");

    expect((s.tz_localize("UTC").to_list()[0] as Date).getTime()).toBe(NAIVE.getTime());
  });

  test("NaT propagates through tz_localize", () => {
    const s = new Series<CellValue>([NAIVE, null], { dtype: "datetime64[ns]" });
    const localized = s.tz_localize("US/Pacific");
    expect(localized.to_list()[1]).toBe(NaT);
    expect(localized.isna().to_list()).toEqual([false, true]);
    expect(localized.dtype).toBe("datetime64[ns, US/Pacific]");
  });

  test("tz_localize(null) strips the zone keeping the source wall clock", () => {
    const s = new Series<CellValue>([NAIVE], { dtype: "datetime64[ns]" });
    const stripped = s.tz_localize("US/Eastern").tz_localize(null);
    expect(stripped.dtype).toBe("datetime64[ns]");
    // 00:00 EST stripped back to naive stays 00:00.
    expect((stripped.to_list()[0] as Date).getTime()).toBe(NAIVE.getTime());
    // tz_localize(null) on naive is a no-op.
    expect((s.tz_localize(null).to_list()[0] as Date).getTime()).toBe(NAIVE.getTime());
  });

  test("localize errors: aware input, unknown zone, non-datetime dtype", () => {
    const s = new Series<CellValue>([NAIVE], { dtype: "datetime64[ns]" });
    const aware = s.tz_localize("UTC");
    expect(() => aware.tz_localize("UTC")).toThrow(/Already tz-aware/);
    expect(() => s.tz_localize("Mars/Olympus")).toThrow(/Unknown time zone/);
    expect(() => new Series<number>([1, 2]).tz_localize("UTC")).toThrow(/datetime dtype/);
  });
});

describe("Series.tz_convert", () => {
  test("converting keeps the UTC instant and only relabels the zone", () => {
    const s = new Series<CellValue>([NAIVE], { dtype: "datetime64[ns]" });
    const eastern = s.tz_localize("US/Eastern"); // 2021-01-01 05:00 UTC
    const tokyo = eastern.tz_convert("Asia/Tokyo");
    // pandas: the stored instant is fixed; only the zone label changes.
    expect((tokyo.to_list()[0] as Date).getTime()).toBe((eastern.to_list()[0] as Date).getTime());
    expect(tokyo.dtype).toBe("datetime64[ns, Asia/Tokyo]");
    // The wall-clock reading shifts: 00:00 EST == 14:00 JST.
    expect(new DatetimeIndex(tokyo.to_list() as Date[], "Asia/Tokyo").wallClock()[0]!.toISOString())
      .toBe("2021-01-01T14:00:00.000Z");
    expect(new DatetimeIndex(eastern.to_list() as Date[], "US/Eastern").wallClock()[0]!.toISOString())
      .toBe("2021-01-01T00:00:00.000Z");
  });

  test("tz_convert(null) yields naive UTC instants", () => {
    const s = new Series<CellValue>([NAIVE], { dtype: "datetime64[ns]" });
    const naive = s.tz_localize("US/Eastern").tz_convert(null);
    expect(naive.dtype).toBe("datetime64[ns]");
    expect((naive.to_list()[0] as Date).toISOString()).toBe("2021-01-01T05:00:00.000Z");
  });

  test("convert errors: naive input and unknown zone", () => {
    const s = new Series<CellValue>([NAIVE], { dtype: "datetime64[ns]" });
    expect(() => s.tz_convert("UTC")).toThrow(/tz-naive/);
    expect(() => s.tz_localize("UTC").tz_convert("Mars/Olympus")).toThrow(/Unknown time zone/);
  });
});

describe("DataFrame tz ops", () => {
  test("tz_localize shifts datetime columns and leaves others untouched", () => {
    const df = new DataFrame([
      { t: NAIVE, n: 5, s: "x" },
      { t: null, n: 7, s: "y" },
    ]).astype({ t: "datetime64[ns]" });
    const localized = df.tz_localize("UTC-08:00");
    expect(localized.dtypes().t).toBe("datetime64[ns, UTC-08:00]");
    expect(localized.dtypes().n).toBe(df.dtypes().n);
    const t = localized.get("t").to_list();
    // 00:00 PST (UTC-08:00) == 08:00 UTC.
    expect((t[0] as Date).toISOString()).toBe("2021-01-01T08:00:00.000Z");
    expect(t[1]).toBe(NaT);
    expect(localized.get("n").to_list()).toEqual([5, 7]);
  });

  test("tz_localize works on the legacy date dtype", () => {
    const df = new DataFrame([{ t: NAIVE }]); // inferred "date"
    expect(df.dtypes().t).toBe("date");
    const localized = df.tz_localize("Asia/Tokyo");
    expect(localized.dtypes().t).toBe("datetime64[ns, Asia/Tokyo]");
    expect((localized.get("t").iloc(0) as Date).toISOString()).toBe("2020-12-31T15:00:00.000Z");
  });

  test("tz_convert relabels aware columns and preserves instants", () => {
    const df = new DataFrame([{ t: NAIVE }]).astype({ t: "datetime64[ns]" }).tz_localize("US/Central");
    const converted = df.tz_convert("Asia/Kolkata");
    expect(converted.dtypes().t).toBe("datetime64[ns, Asia/Kolkata]");
    expect((converted.get("t").iloc(0) as Date).getTime()).toBe((df.get("t").iloc(0) as Date).getTime());
  });

  test("frame-level errors mirror pandas", () => {
    const naive = new DataFrame([{ t: NAIVE }]).astype({ t: "datetime64[ns]" });
    expect(() => naive.tz_convert("UTC")).toThrow(/tz-naive/);
    const aware = naive.tz_localize("UTC");
    expect(() => aware.tz_localize("UTC")).toThrow(/Already tz-aware/);
  });
});

describe("DatetimeIndex and Timestamp timezones", () => {
  test("DatetimeIndex carries tz, localizes, and converts", () => {
    const naive = new DatetimeIndex(["2021-01-01T00:00:00Z"]);
    expect(naive.tz).toBeNull();
    const eastern = naive.tz_localize("US/Eastern");
    expect(eastern.tz).toBe("US/Eastern");
    expect(eastern.as_i8[0]).toBe(Date.parse("2021-01-01T05:00:00.000Z"));
    const tokyo = eastern.tz_convert("Asia/Tokyo");
    expect(tokyo.tz).toBe("Asia/Tokyo");
    expect(tokyo.as_i8[0]).toBe(eastern.as_i8[0]); // instant preserved
    // Wall-clock shift is observable through the display helper.
    expect(tokyo.wallClock()[0]!.toISOString()).toBe("2021-01-01T14:00:00.000Z");
    expect(eastern.wallClock()[0]!.toISOString()).toBe("2021-01-01T00:00:00.000Z");
    expect(() => naive.tz_convert("UTC")).toThrow(/tz-naive/);
  });

  test("Timestamp localizes, converts, and renders zone-aware text", () => {
    const ts = new Timestamp("2021-01-01T00:00:00Z");
    expect(ts.tz).toBeNull();
    const kolkata = ts.tz_localize("UTC+05:30");
    expect(kolkata.tz).toBe("UTC+05:30");
    expect(kolkata.date.toISOString()).toBe("2020-12-31T18:30:00.000Z");
    expect(kolkata.toString()).toBe("2021-01-01 00:00:00+05:30");
    const utc = kolkata.tz_convert("UTC");
    expect(utc.date.getTime()).toBe(kolkata.date.getTime());
    expect(utc.toString()).toBe("2020-12-31 18:30:00+00:00");
    expect(() => ts.tz_convert("UTC")).toThrow(/tz-naive/);
  });
});

describe("select_dtypes datetime families", () => {
  test("naive and aware columns select separately", () => {
    const df = new DataFrame([{ a: NAIVE }]).astype({ a: "datetime64[ns]" });
    const mixed = df.assign({ b: df.get("a").tz_localize("US/Eastern") });
    expect(mixed.dtypes().a).toBe("datetime64[ns]");
    expect(mixed.dtypes().b).toBe("datetime64[ns, US/Eastern]");
    // pandas np.datetime64 selects naive columns only; datetimetz selects aware.
    expect(mixed.select_dtypes("datetime").columns).toEqual(["a"]);
    expect(mixed.select_dtypes("datetimetz").columns).toEqual(["b"]);
  });
});
