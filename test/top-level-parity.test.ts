import { describe, expect, test } from "bun:test";
import {
  DataFrame,
  DatetimeIndex,
  NA,
  NaT,
  Timestamp,
  Timedelta,
  date_range,
  describe_option,
  get_option,
  interval_range,
  lreshape,
  merge_asof,
  merge_ordered,
  option_context,
  period_range,
  set_option,
  timedelta_range,
  wide_to_long,
} from "../src/index";

describe("options registry", () => {
  test("get/set/describe round-trip", () => {
    expect(get_option("display.max_rows")).toBe(60);
    set_option("display.max_rows", 120);
    expect(get_option("display.max_rows")).toBe(120);
    expect(describe_option("display.max_rows")).toContain("120");
    set_option("display.max_rows", 60);
  });

  test("unknown option throws", () => {
    expect(() => get_option("nope.nothing")).toThrow();
  });

  test("option_context restores prior values", () => {
    option_context({ "display.max_rows": 5 }, () => {
      expect(get_option("display.max_rows")).toBe(5);
    });
    expect(get_option("display.max_rows")).toBe(60);
  });
});

describe("scalars and index types", () => {
  test("Timestamp + Timedelta arithmetic", () => {
    const ts = new Timestamp("2026-01-01").add(new Timedelta(86_400_000));
    expect(ts.toString()).toBe("2026-01-02T00:00:00.000Z");
    const delta = ts.sub(new Timestamp("2026-01-01")) as Timedelta;
    expect(delta.ms).toBe(86_400_000);
  });

  test("NA / NaT sentinels are stable singletons", () => {
    expect(NA).toBe(NA);
    expect(NaT).toBe(NaT);
  });

  test("DatetimeIndex wraps dates with i8 view", () => {
    const idx = new DatetimeIndex(["2026-01-01", "2026-01-02"]);
    expect(idx.length).toBe(2);
    expect(idx.as_i8[1]! - idx.as_i8[0]!).toBe(86_400_000);
  });
});

describe("range builders", () => {
  test("date_range supports both call shapes", () => {
    expect(date_range("2026-01-01", "2026-01-05").length).toBe(5);
    expect(date_range({ start: "2026-01-01", periods: 3 }).length).toBe(3);
  });

  test("timedelta_range steps by freq", () => {
    const td = timedelta_range(0, 3 * 86_400_000);
    expect(td.length).toBe(4);
  });

  test("interval_range builds half-open bins", () => {
    const bins = interval_range(0, 10, 6);
    expect(bins).toHaveLength(5);
    expect(bins[0]).toEqual({ left: 0, right: 2 });
    expect(bins.at(-1)).toEqual({ left: 8, right: 10 });
  });

  test("period_range produces Period objects", () => {
    const pr = period_range("2026-01-01", 3);
    expect(pr.length).toBe(3);
    expect(pr.periods[0]!.contains(new Date("2026-01-01T12:00:00Z"))).toBe(true);
    expect(pr.periods[0]!.contains(new Date("2026-01-02T12:00:00Z"))).toBe(false);
  });
});

describe("reshape wrappers", () => {
  test("lreshape interleaves grouped columns", () => {
    const wide = new DataFrame([
      { id: 1, x1: "a", x2: "b" },
      { id: 2, x1: "c", x2: null },
    ]);
    const long = lreshape(wide, { x: ["x1", "x2"] });
    const values = long.to_records().map((r) => r.x).filter((v) => v != null);
    expect(values.sort()).toEqual(["a", "b", "c"]);
  });

  test("wide_to_long splits stubnames", () => {
    const wide = new DataFrame([
      { id: 1, q1: 10, q2: 20 },
      { id: 2, q1: 30, q2: 40 },
    ]);
    const long = wide_to_long(wide, ["q"], "id", "quarter");
    expect(long.columns).toEqual(["id", "quarter", "q"]);
    expect(long.to_records().at(-1)).toEqual({ id: 2, quarter: "2", q: 40 });
  });
});

describe("ordered merges", () => {
  test("merge_asof takes last right row <= key", () => {
    const left = new DataFrame([{ t: 1, v: 1 }, { t: 2, v: 2 }]);
    const right = new DataFrame([{ t: 2, w: 99 }, { t: 3, w: 100 }]);
    const merged = merge_asof(left, right, "t");
    expect(merged.to_records()[0]).toEqual({ t: 1, v: 1 });
    expect(merged.to_records()[1]).toEqual({ t: 2, v: 2, w: 99 });
  });

  test("merge_ordered unions keys in order", () => {
    const left = new DataFrame([{ t: 1, v: 1 }, { t: 3, v: 3 }]);
    const right = new DataFrame([{ t: 2, w: 2 }]);
    const merged = merge_ordered(left, right, "t");
    expect(merged.index).toEqual([0, 1, 2]);
    expect(merged.to_records().map((r) => r.t)).toEqual([1, 2, 3]);
  });
});
