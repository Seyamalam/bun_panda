import { describe, expect, test } from "bun:test";
import {
  DataFrame, isPeriodDType, NaT, normalizePeriodFreq, Period, PeriodIndex,
  period_range, Series, Timestamp, ValueError,
} from "../src/index";
import type { CellValue } from "../src/index";

// All oracle values below were verified against pandas 3.0.5
// (`pd.Period(...)`, `pd.period_range(...)`, Series period ops).
// Naive datetime tests use instants far from period boundaries; exact
// boundary checks go through `datetime64[ns, UTC]` dtypes or UTC epoch
// millis so they hold in any machine timezone.

describe("Period scalar", () => {
  test("parses literals and exposes pandas ordinals", () => {
    const m = new Period("2024-01", "M");
    expect(m.toString()).toBe("2024-01");
    expect(m.ordinal).toBe(648); // (2024-1970)*12 + 0
    expect(m.freq).toBe("M");
    expect(m.freqstr).toBe("M");

    const q = new Period("2024Q1", "Q");
    expect(q.toString()).toBe("2024Q1");
    expect(q.ordinal).toBe(216);
    expect(q.freqstr).toBe("Q-DEC");

    const y = new Period("2024", "Y");
    expect(y.toString()).toBe("2024");
    expect(y.ordinal).toBe(54);
    expect(y.freqstr).toBe("Y-DEC");

    const w = new Period("2024-01-15", "W");
    expect(w.toString()).toBe("2024-01-15/2024-01-21"); // W-SUN weeks end Sunday
    expect(w.ordinal).toBe(2821);
    expect(w.freqstr).toBe("W-SUN");

    const d = new Period("2024-01-15", "D");
    expect(d.toString()).toBe("2024-01-15");
    expect(d.ordinal).toBe(19737);

    const h = new Period("2024-01-15 05:00", "h");
    expect(h.toString()).toBe("2024-01-15 05:00");
    expect(h.ordinal).toBe(473693);
    expect(h.freqstr).toBe("h");
  });

  test("infers freq from the literal granularity like pandas", () => {
    expect(new Period("2024").freq).toBe("Y");
    expect(new Period("2024Q3").freq).toBe("Q");
    expect(new Period("2024-07").freq).toBe("M");
    expect(new Period("2024-01-15").freq).toBe("D");
    expect(new Period("2024-01-15 05:00").freq).toBe("H");
  });

  test("respects calendar quarter boundaries (oracle-checked)", () => {
    expect(new Period("2024-03-31", "Q").toString()).toBe("2024Q1");
    expect(new Period("2024-03-31", "Q").ordinal).toBe(216);
    expect(new Period("2024-04-01", "Q").toString()).toBe("2024Q2");
    expect(new Period("2024-06-30", "Q").toString()).toBe("2024Q2");
    expect(new Period("2024-07-01", "Q").toString()).toBe("2024Q3");
    expect(new Period("2024-12-31", "Q").ordinal).toBe(219);
    expect(new Period("2025-01-01", "Q").toString()).toBe("2025Q1");
  });

  test("weekly periods end on Sunday and count from the epoch", () => {
    // pandas: Period('1970-01-01', 'W').ordinal == 1 covering Dec 29..Jan 4.
    expect(new Period("1970-01-01", "W").ordinal).toBe(1);
    expect(new Period("1970-01-01", "W").toString()).toBe("1969-12-29/1970-01-04");
    expect(new Period("1970-01-05", "W").ordinal).toBe(2);
    expect(new Period("1970-01-05", "W").toString()).toBe("1970-01-05/1970-01-11");
    expect(new Period("2024-01-21", "W").toString()).toBe("2024-01-15/2024-01-21");
  });

  test("pre-epoch periods carry negative ordinals", () => {
    expect(new Period("1969-12-31", "D").ordinal).toBe(-1);
    expect(new Period("1969-12", "M").ordinal).toBe(-1);
    expect(new Period("1969", "Y").ordinal).toBe(-1);
    expect(new Period("1969-12-31", "D").toString()).toBe("1969-12-31");
    expect(new Period("1969-12", "M").toString()).toBe("1969-12");
  });

  test("aliases normalize to canonical freqs", () => {
    expect(normalizePeriodFreq("A")).toBe("Y");
    expect(normalizePeriodFreq("Y-DEC")).toBe("Y");
    expect(normalizePeriodFreq("Q-DEC")).toBe("Q");
    expect(normalizePeriodFreq("W-SUN")).toBe("W");
    expect(normalizePeriodFreq("h")).toBe("H");
    expect(new Period("2024", "A").equals(new Period("2024", "Y"))).toBe(true);
    expect(() => normalizePeriodFreq("2M")).toThrow(ValueError);
    expect(() => normalizePeriodFreq("min")).toThrow(ValueError);
    expect(() => new Period("2024-01-15 05:30")).toThrow(ValueError); // minute freq unsupported
    expect(() => new Period("nonsense", "D")).toThrow(ValueError);
  });

  test("constructs from ordinals, Dates, Timestamps, and Periods", () => {
    expect(new Period(648, "M").toString()).toBe("2024-01");
    expect(new Period(new Date("2024-06-15T00:00:00Z"), "Q").toString()).toBe("2024Q2");
    expect(new Period(216, "Q").equals(new Period("2024Q1", "Q"))).toBe(true);
    expect(new Period(new Period("2024-01", "M")).ordinal).toBe(648);
    // pandas `Period(otherPeriod, freq)` raises IncompatibleFrequency.
    expect(() => new Period(new Period("2024-01-15", "D"), "M")).toThrow(/freq/);
  });

  test("exposes pandas boundary instants and end-based calendar fields", () => {
    const m = new Period("2024-01", "M");
    expect(m.start_time.toISOString()).toBe("2024-01-01T00:00:00.000Z");
    expect(m.end_time.toISOString()).toBe("2024-01-31T23:59:59.999Z");
    expect(m.year).toBe(2024);
    expect(m.month).toBe(1);
    expect(m.day).toBe(31); // pandas derives fields from the period end
    expect(m.quarter).toBe(1);

    const q = new Period("2024Q1", "Q");
    expect(q.start_time.toISOString()).toBe("2024-01-01T00:00:00.000Z");
    expect(q.end_time.toISOString()).toBe("2024-03-31T23:59:59.999Z");
    expect(q.month).toBe(3);
    expect(q.day).toBe(31);
    expect(q.quarter).toBe(1);

    const w = new Period("2024-01-15", "W");
    expect(w.start_time.toISOString()).toBe("2024-01-15T00:00:00.000Z");
    expect(w.end_time.toISOString()).toBe("2024-01-21T23:59:59.999Z");
    expect(w.year).toBe(2024);
    expect(w.month).toBe(1);
    expect(w.day).toBe(21);
    expect(w.quarter).toBe(1);

    const y = new Period("2024", "Y");
    expect(y.start_time.toISOString()).toBe("2024-01-01T00:00:00.000Z");
    expect(y.end_time.toISOString()).toBe("2024-12-31T23:59:59.999Z");
    expect(y.month).toBe(12);
    expect(y.quarter).toBe(4);

    const h = new Period("2024-01-15 05:00", "h");
    expect(h.start_time.toISOString()).toBe("2024-01-15T05:00:00.000Z");
    expect(h.end_time.toISOString()).toBe("2024-01-15T05:59:59.999Z");
    expect(h.hour).toBe(5);
  });

  test("to_timestamp emits the start or end boundary", () => {
    const m = new Period("2024-01", "M");
    expect(m.to_timestamp().toISOString()).toBe("2024-01-01T00:00:00.000Z");
    expect(m.to_timestamp("S").toISOString()).toBe("2024-01-01T00:00:00.000Z");
    expect(m.to_timestamp("E").toISOString()).toBe("2024-01-31T23:59:59.999Z");
    expect(m.to_timestamp("end").toISOString()).toBe("2024-01-31T23:59:59.999Z");
  });

  test("asfreq re-bases on the start or end instant (pandas default 'E')", () => {
    const m = new Period("2024-01", "M");
    expect(m.asfreq("D").toString()).toBe("2024-01-31");
    expect(m.asfreq("D", "S").toString()).toBe("2024-01-01");
    expect(m.asfreq("h", "E").toString()).toBe("2024-01-31 23:00");
    expect(m.asfreq("W", "S").toString()).toBe("2024-01-01/2024-01-07");
    expect(m.asfreq("W", "E").toString()).toBe("2024-01-29/2024-02-04");
    expect(new Period("2024-06", "M").asfreq("Q", "S").toString()).toBe("2024Q2");
    expect(new Period("2024-06", "M").asfreq("Y").toString()).toBe("2024");
    expect(new Period("2024Q1", "Q").asfreq("M").toString()).toBe("2024-03");
    expect(new Period("2024", "Y").asfreq("W", "E").toString()).toBe("2024-12-30/2025-01-05");
    expect(new Period("2024-01-15 05:00", "h").asfreq("D").toString()).toBe("2024-01-15");
  });

  test("integer arithmetic shifts ordinals; Period difference counts steps", () => {
    const p = new Period("2024-01", "M");
    expect(p.add(1).toString()).toBe("2024-02");
    expect(p.add(-13).toString()).toBe("2022-12");
    expect((p.sub(2) as Period).toString()).toBe("2023-11");
    expect(p.sub(new Period("2023-10", "M"))).toBe(3);
    expect(new Period("2024Q2", "Q").sub(new Period("2024Q1", "Q"))).toBe(1);
    expect(() => p.add(1.5)).toThrow(TypeError);
    expect(() => p.sub(0.5)).toThrow(TypeError);
    expect(() => p.sub(new Period("2024-01-01", "D"))).toThrow(/freq/);
  });

  test("comparisons follow pandas freq-compatibility rules", () => {
    const jan = new Period("2024-01", "M");
    const feb = new Period("2024-02", "M");
    expect(jan.equals(new Period("2024-01", "M"))).toBe(true);
    expect(jan.equals(feb)).toBe(false);
    expect(jan.equals(new Period("2024-01-31", "D"))).toBe(false); // cross-freq eq → False
    expect(jan.lt(feb)).toBe(true);
    expect(feb.gt(jan)).toBe(true);
    expect(jan.le(jan)).toBe(true);
    expect(jan.compareTo(feb)).toBeLessThan(0);
    expect(() => jan.lt(new Period("2024Q1", "Q"))).toThrow(/freq/);
    expect(() => jan.compareTo(new Period("2024-01-01", "D"))).toThrow(/freq/);
  });

  test("string and repr forms match pandas", () => {
    expect(`${new Period("2024-01", "M")}`).toBe("2024-01");
    expect(new Period("2024Q1", "Q").repr()).toBe("Period('2024Q1', 'Q-DEC')");
    expect(new Period("2024", "Y").repr()).toBe("Period('2024', 'Y-DEC')");
    expect(JSON.stringify(new Period("2024-01", "M"))).toBe('"2024-01"');
  });
});

describe("period dtype plumbing", () => {
  test("dtype spellings validate and infer", () => {
    expect(isPeriodDType("period[M]")).toBe(true);
    expect(isPeriodDType("period[Q-DEC]")).toBe(true);
    expect(isPeriodDType("period[h]")).toBe(true);
    expect(isPeriodDType("period[2M]")).toBe(false);
    expect(isPeriodDType("period")).toBe(false);
    expect(isPeriodDType("datetime64[ns]")).toBe(false);
    expect(() => new Series([1, 2], { dtype: "period[min]" as never })).toThrow(/not understood/);
  });

  test("Series construction infers period dtype from homogeneous Periods", () => {
    const s = new Series([new Period("2024-01", "M"), new Period("2024-02", "M")]);
    expect(s.dtype).toBe("period[M]");
    const mixed = new Series([new Period("2024-01", "M"), new Period("2024-01-15", "D")]);
    expect(mixed.dtype).toBe("object");
  });

  test("explicit dtype parses strings/Dates and rejects mismatched Periods", () => {
    const s = new Series<CellValue>(["2024-01", "2024-03"], { dtype: "period[M]" });
    expect(s.dtype).toBe("period[M]");
    expect((s.get(0) as Period).toString()).toBe("2024-01");
    const fromDates = new Series<CellValue>(
      [new Date("2024-01-15T00:00:00Z"), new Date("2024-03-20T00:00:00Z")],
      { dtype: "period[M]" }
    );
    expect((fromDates.get(1) as Period).toString()).toBe("2024-03");
    // pandas Series(..., dtype='period[M]') on a period[D] cell raises.
    expect(() => new Series([new Period("2024-01-15", "D")], { dtype: "period[M]" })).toThrow(/freq/);
  });

  test("missing cells become NaT and propagate through arithmetic", () => {
    const s = new Series<CellValue>([new Period("2024-01", "M"), null], { dtype: "period[M]" });
    expect(s.get(1)).toBe(NaT);
    const shifted = s.add(1);
    expect((shifted.get(0) as Period).toString()).toBe("2024-02");
    expect(shifted.get(1) as unknown).toBe(NaT);
    expect(shifted.dtype).toBe("period[M]");
  });

  test("astype converts datetime and period columns", () => {
    const dates = new Series(
      [new Date("2024-01-15T00:00:00Z"), new Date("2024-03-31T00:00:00Z")],
      { dtype: "datetime64[ns]" }
    );
    const months = dates.astype("period[M]");
    expect(months.dtype).toBe("period[M]");
    expect((months.get(0) as Period).toString()).toBe("2024-01");
    expect((months.get(1) as Period).toString()).toBe("2024-03");
    const quarters = months.astype("period[Q]");
    expect(quarters.dtype).toBe("period[Q]");
    expect((quarters.get(0) as Period).toString()).toBe("2024Q1");
    // astype re-bases on the period END like pandas asfreq(how='E').
    const days = months.astype("period[D]");
    expect((days.get(0) as Period).toString()).toBe("2024-01-31");
  });
});

describe("Series period ops", () => {
  test("integer add/sub shift the whole column, keeping period dtype", () => {
    const s = new Series<CellValue>(["2024-01", "2024-02", "2024-03"], { dtype: "period[M]" });
    const plus = s.add(1);
    expect((plus.get(0) as Period).toString()).toBe("2024-02");
    expect(plus.dtype).toBe("period[M]");
    const minus = s.sub(13);
    expect((minus.get(0) as Period).toString()).toBe("2022-12");
    expect((s.subtract(1).get(2) as Period).toString()).toBe("2024-02");
    expect((s.radd(1).get(0) as Period).toString()).toBe("2024-02");
    expect(() => s.add(1.5)).toThrow(TypeError);
    expect(() => s.add(new Period("2024-01", "M"))).toThrow(TypeError);
  });

  test("Period minus Period yields ordinal step counts", () => {
    const s = new Series<CellValue>(["2024-01", "2024-03"], { dtype: "period[M]" });
    const diff = s.sub(new Period("2024-01", "M"));
    expect(diff.to_list()).toEqual([0, 2]);
    const other = new Series<CellValue>(["2024-01", "2023-12"], { dtype: "period[M]" });
    expect(s.sub(other).to_list()).toEqual([0, 3]);
    expect(() => s.sub(new Period("2024Q1", "Q"))).toThrow(/freq/);
    const badFreq = new Series<CellValue>(["2024Q1", "2024Q2"], { dtype: "period[Q]" });
    expect(() => s.sub(badFreq)).toThrow(/freq/);
  });

  test("comparisons honor identity and freq compatibility", () => {
    const s = new Series<CellValue>(["2024-01", "2024-02"], { dtype: "period[M]" });
    expect(s.eq(new Period("2024-01", "M")).to_list()).toEqual([true, false]);
    expect(s.ne(new Period("2024-01", "M")).to_list()).toEqual([false, true]);
    expect(s.lt(new Period("2024-02", "M")).to_list()).toEqual([true, false]);
    expect(s.gt(new Period("2024-01", "M")).to_list()).toEqual([false, true]);
    // cross-freq equality is False; ordering raises like pandas.
    expect(s.eq(new Period("2024-01-31", "D")).to_list()).toEqual([false, false]);
    expect(s.ne(new Period("2024-01-31", "D")).to_list()).toEqual([true, true]);
    expect(() => s.lt(new Period("2024Q1", "Q"))).toThrow(/freq/);
    const other = new Series<CellValue>(["2024-02", "2024-01"], { dtype: "period[M]" });
    expect(s.lt(other).to_list()).toEqual([true, false]);
  });

  test("missing cells follow NaT comparison semantics", () => {
    const s = new Series<CellValue>([new Period("2024-01", "M"), null], { dtype: "period[M]" });
    expect(s.eq(new Period("2024-01", "M")).to_list()).toEqual([true, false]);
    expect(s.ne(new Period("2024-01", "M")).to_list()).toEqual([false, true]);
    expect(s.lt(new Period("2024-02", "M")).to_list()).toEqual([true, false]);
  });

  test("sort_values orders ordinals and keeps NaT last", () => {
    const s = new Series<CellValue>(
      [new Period("2024-03", "M"), null, new Period("2024-01", "M"), new Period("2024-02", "M")],
      { dtype: "period[M]" }
    );
    const sorted = s.sort_values().to_list();
    expect((sorted[0] as Period).toString()).toBe("2024-01");
    expect((sorted[1] as Period).toString()).toBe("2024-02");
    expect((sorted[2] as Period).toString()).toBe("2024-03");
    expect(sorted[3]).toBe(NaT);
  });

  test("min/max use ordinal order; numeric reductions reject periods", () => {
    const s = new Series<CellValue>(["2024-03", "2024-01", "2024-02"], { dtype: "period[M]" });
    expect((s.min() as Period).toString()).toBe("2024-01");
    expect((s.max() as Period).toString()).toBe("2024-03");
    expect(() => s.sum()).toThrow(TypeError);
    expect(() => s.mean()).toThrow(TypeError);
  });

  test("monotonicity recognizes ordinal runs", () => {
    const up = new Series<CellValue>(["2024-01", "2024-02", "2024-03"], { dtype: "period[M]" });
    expect(up.is_monotonic_increasing()).toBe(true);
    const down = new Series<CellValue>(["2024-03", "2024-02", "2024-01"], { dtype: "period[M]" });
    expect(down.is_monotonic_increasing()).toBe(false);
    expect(down.is_monotonic_decreasing()).toBe(true);
  });
});

describe("conversions", () => {
  test("Timestamp.to_period uses the wall-clock reading", () => {
    const ts = new Timestamp(new Date("2024-06-15T10:00:00Z"));
    expect(ts.to_period("Q").toString()).toBe("2024Q2");
    expect(ts.to_period("M").toString()).toBe("2024-06");
    expect(ts.to_period("D").toString()).toBe("2024-06-15");
    expect(ts.to_period("Y").toString()).toBe("2024");
  });

  test("Series.dt.to_period maps each cell to its containing period", () => {
    const s = new Series(
      [new Date("2024-01-15T00:00:00Z"), new Date("2024-03-31T00:00:00Z")],
      { dtype: "datetime64[ns, UTC]" }
    );
    const months = s.dt.to_period("M");
    expect((months[0] as Period).toString()).toBe("2024-01");
    expect((months[1] as Period).toString()).toBe("2024-03");
    const quarters = s.dt.to_period("Q");
    expect((quarters[0] as Period).toString()).toBe("2024Q1");
    expect((quarters[1] as Period).toString()).toBe("2024Q1");
    const days = s.dt.to_period("D");
    expect((days[1] as Period).toString()).toBe("2024-03-31");
  });

  test("Series.to_period converts Date labels and Period labels", () => {
    const idx = [new Date("2024-01-15T00:00:00Z"), new Date("2024-03-20T00:00:00Z")];
    const s = new Series([10, 20], { index: idx as never[] });
    const monthly = s.to_period("M");
    const labels = monthly.index;
    expect(labels[0] instanceof Period).toBe(true);
    expect((labels[0] as unknown as Period).toString()).toBe("2024-01");
    expect((labels[1] as unknown as Period).toString()).toBe("2024-03");
    // A Period index re-bases via asfreq (pandas end-anchored default).
    const quarterly = monthly.to_period("Q");
    expect((quarterly.index[1] as unknown as Period).toString()).toBe("2024Q1");
    // Period values re-base alongside the index.
    const vs = new Series<CellValue>(["2024-01", "2024-03"], { dtype: "period[M]", index: idx as never[] });
    const vq = vs.to_period("Q");
    expect(vq.dtype).toBe("period[Q]");
    expect((vq.to_list()[0] as Period).toString()).toBe("2024Q1");
    // Non-convertible labels raise like pandas (`unsupported Type`).
    expect(() => new Series([1, 2]).to_period("M")).toThrow(TypeError);
  });

  test("Series.to_timestamp round-trips period labels and values", () => {
    const s = new Series(
      [1, 2],
      { index: [new Period("2024-01", "M"), new Period("2024-03", "M")] as never[] }
    );
    const ts = s.to_timestamp();
    const labels = ts.index;
    expect((labels[0] as unknown as Date).toISOString()).toBe("2024-01-01T00:00:00.000Z");
    expect((labels[1] as unknown as Date).toISOString()).toBe("2024-03-01T00:00:00.000Z");
    const tsEnd = s.to_timestamp("E");
    expect((tsEnd.index[0] as unknown as Date).toISOString()).toBe("2024-01-31T23:59:59.999Z");
    const vs = new Series<CellValue>(["2024-01", "2024-03"], { dtype: "period[M]" });
    const out = vs.to_timestamp();
    expect(out.dtype).toBe("datetime64[ns]");
    expect((out.to_list()[0] as Date).toISOString()).toBe("2024-01-01T00:00:00.000Z");
  });
});

describe("PeriodIndex and period_range", () => {
  test("PeriodIndex stores Periods at one shared freq", () => {
    const idx = new PeriodIndex(["2024-01", "2024-02", "2024-03"], "M");
    expect(idx.freq).toBe("M");
    expect(idx.freqstr).toBe("M");
    expect(idx.dtype).toBe("period[M]");
    expect(idx.periods.map((p) => p.toString())).toEqual(["2024-01", "2024-02", "2024-03"]);
    expect(String(idx)).toBe("PeriodIndex(['2024-01', '2024-02', '2024-03'], dtype='period[M]')");
    const q = new PeriodIndex(["2024Q1"], "Q");
    expect(String(q)).toBe("PeriodIndex(['2024Q1'], dtype='period[Q-DEC]')");
    expect(() => new PeriodIndex([new Period("2024-01", "M"), new Period("2024-01-15", "D")])).toThrow(/single freq/);
    expect(() => new PeriodIndex([new Period("2024-01", "M")], "Q")).toThrow(/freq mismatch/);
  });

  test("PeriodIndex.to_timestamp returns a DatetimeIndex of boundaries", () => {
    const idx = new PeriodIndex(["2024-01", "2024-02"], "M");
    const starts = idx.to_timestamp();
    expect(starts.labels.map((d) => (d as unknown as Date).toISOString()))
      .toEqual(["2024-01-01T00:00:00.000Z", "2024-02-01T00:00:00.000Z"]);
    const ends = idx.to_timestamp("E");
    expect((ends.labels[0] as unknown as Date).toISOString()).toBe("2024-01-31T23:59:59.999Z");
  });

  test("period_range counts forward or runs to an inclusive end", () => {
    const byCount = period_range("2024-01", 3, "M");
    expect(byCount.periods.map((p) => p.toString())).toEqual(["2024-01", "2024-02", "2024-03"]);
    expect(byCount.freq).toBe("M");
    const byEnd = period_range("2024-01", "2024-04", "M");
    expect(byEnd.periods.map((p) => p.toString()))
      .toEqual(["2024-01", "2024-02", "2024-03", "2024-04"]);
    const quarters = period_range("2024Q1", 4, "Q");
    expect(quarters.periods.map((p) => p.toString())).toEqual(["2024Q1", "2024Q2", "2024Q3", "2024Q4"]);
    const preEpoch = period_range("1969-11", 3, "M");
    expect(preEpoch.periods.map((p) => p.ordinal)).toEqual([-2, -1, 0]);
    expect(period_range(new Period("2024-01", "M"), 2).freq).toBe("M");
    expect(period_range("2024-01-15", 2, "W").periods.map((p) => p.toString()))
      .toEqual(["2024-01-15/2024-01-21", "2024-01-22/2024-01-28"]);
  });

  test("period labels work as a Series index (value-based label equality)", () => {
    const s = new Series(
      [10, 20, 30],
      { index: [new Period("2024-01", "M"), new Period("2024-02", "M"), new Period("2024-03", "M")] as never[] }
    );
    // A different Period instance with the same identity must hit the row.
    expect(s.loc(new Period("2024-02", "M") as never)).toBe(20);
  });
});

describe("groupby and DataFrame integration", () => {
  test("period cells group by (freq, ordinal) identity", () => {
    const df = new DataFrame({
      month: [new Period("2024-01", "M"), new Period("2024-02", "M"), new Period("2024-01", "M")],
      v: [1, 2, 3],
    });
    const grouped = df.groupby("month").sum();
    // bun_pandas keeps group keys as a column (like pandas as_index=False);
    // the key cells stay Period objects grouped on value identity.
    const keys = grouped.get("month")!.to_list();
    expect(keys[0] instanceof Period).toBe(true);
    expect((keys[0] as Period).toString()).toBe("2024-01");
    expect((keys[1] as Period).toString()).toBe("2024-02");
    expect(grouped.get("v")!.to_list()).toEqual([4, 2]);
  });

  test("DataFrame column dtypes infer period dtype", () => {
    const df = new DataFrame({
      p: [new Period("2024-01", "M"), new Period("2024-02", "M")],
    });
    expect(df.get("p")!.dtype).toBe("period[M]");
  });
});
