import type { CellValue } from "../../types";
import { isMissing } from "../../utils";
import { dtypeZone } from "../shared/timezone";
import { NaT } from "../../missing";
import { Period, normalizePeriodFreq, periodOrdinalFromFields } from "../../period";

const DAY_MS = 86_400_000;

const DAY_NAMES = [
  "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday",
];

function pad2(n: number): string {
  return n < 10 ? `0${n}` : String(n);
}

/**
 * Zone wall clock for an instant, expressed as a Date whose UTC fields read
 * like a `datetime64[ns, zone]` cell in pandas. Resolved through
 * Intl.DateTimeFormat so real DST rules apply (US/Eastern is -5 in January,
 * -4 in July).
 */
function zonedWallClock(d: Date, zone: string): Date {
  const parts: Record<string, string> = {};
  for (const part of new Intl.DateTimeFormat("en-US", {
    timeZone: zone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hourCycle: "h23",
  }).formatToParts(d)) {
    parts[part.type] = part.value;
  }
  const hour = Number(parts.hour) % 24;
  return new Date(
    Date.UTC(
      Number(parts.year),
      Number(parts.month) - 1,
      Number(parts.day),
      hour,
      Number(parts.minute),
      Number(parts.second),
      d.getUTCMilliseconds()
    )
  );
}

/**
 * pandas-style `.dt` accessor for Series of dates (or date-strings).
 * Missing values propagate as null. Values that are neither Date nor
 * parseable strings produce null per cell.
 *
 * A naive `datetime64[ns]` series reads local fields of each stored instant
 * (naive strings parse as local wall clocks). An aware
 * `datetime64[ns, <zone>]` series shifts every instant into the declared
 * zone first — matching pandas, which reports calendar fields in the zone.
 */
export class DatetimeMethods {
  private readonly values: (Date | null)[];
  /** Original instants alongside the zone-shifted walls (aware dtypes). */
  private readonly instants: (Date | null)[];
  private readonly zoneShifted: boolean;

  constructor(values: CellValue[], dtype?: string) {
    const zone = dtypeZone(dtype);
    this.zoneShifted = zone !== null;
    this.instants = values.map((value) => normalizeDate(value));
    this.values = zone === null
      ? this.instants
      : this.instants.map((d) => (d === null ? null : zonedWallClock(d, zone)));
  }

  private mapParts(fn: (d: Date) => CellValue): CellValue[] {
    return this.values.map((value) => (value === null ? null : fn(value as Date)));
  }

  private yearOf(d: Date): number {
    return this.zoneShifted ? d.getUTCFullYear() : d.getFullYear();
  }

  private monthOf(d: Date): number {
    return this.zoneShifted ? d.getUTCMonth() : d.getMonth();
  }

  private dayOf(d: Date): number {
    return this.zoneShifted ? d.getUTCDate() : d.getDate();
  }

  private hourOf(d: Date): number {
    return this.zoneShifted ? d.getUTCHours() : d.getHours();
  }

  private minuteOf(d: Date): number {
    return this.zoneShifted ? d.getUTCMinutes() : d.getMinutes();
  }

  private secondOf(d: Date): number {
    return this.zoneShifted ? d.getUTCSeconds() : d.getSeconds();
  }

  /** 0 = Monday ... 6 = Sunday (pandas `dayofweek`). */
  private weekdayOf(d: Date): number {
    const w = this.zoneShifted ? d.getUTCDay() : d.getDay();
    return (w + 6) % 7;
  }

  year(): CellValue[] {
    return this.mapParts((d) => this.yearOf(d));
  }
  month(): CellValue[] {
    return this.mapParts((d) => this.monthOf(d) + 1);
  }
  day(): CellValue[] {
    return this.mapParts((d) => this.dayOf(d));
  }
  hour(): CellValue[] {
    return this.mapParts((d) => this.hourOf(d));
  }
  minute(): CellValue[] {
    return this.mapParts((d) => this.minuteOf(d));
  }
  second(): CellValue[] {
    return this.mapParts((d) => this.secondOf(d));
  }

  dayofweek(): CellValue[] {
    return this.mapParts((d) => this.weekdayOf(d));
  }

  /** Day name, e.g. "Monday". */
  day_name(): CellValue[] {
    return this.mapParts((d) => DAY_NAMES[this.weekdayOf(d)]!);
  }

  /** 1-based day of year. */
  dayofyear(): CellValue[] {
    return this.mapParts((d) => {
      const start = Date.UTC(this.yearOf(d), 0, 1);
      const current = Date.UTC(this.yearOf(d), this.monthOf(d), this.dayOf(d));
      return Math.floor((current - start) / DAY_MS) + 1;
    });
  }

  /** 1-53 ISO week number. */
  isocalendar_week(): CellValue[] {
    return this.mapParts((d) => {
      const target = new Date(Date.UTC(this.yearOf(d), this.monthOf(d), this.dayOf(d)));
      const dayNumber = (target.getUTCDay() + 6) % 7;
      target.setUTCDate(target.getUTCDate() - dayNumber + 3);
      const firstThursday = new Date(Date.UTC(target.getUTCFullYear(), 0, 4));
      const firstDayNumber = (firstThursday.getUTCDay() + 6) % 7;
      firstThursday.setUTCDate(firstThursday.getUTCDate() - firstDayNumber + 3);
      return 1 + Math.round((target.getTime() - firstThursday.getTime()) / (7 * DAY_MS));
    });
  }

  /** Days since the Unix epoch as integers. */
  to_julian(): CellValue[] {
    return this.mapParts((d) => Math.floor(d.getTime() / DAY_MS));
  }

  /** Milliseconds since epoch. */
  view_ms(): CellValue[] {
    return this.instants.map((d) => (d === null ? null : d.getTime()));
  }

  /** Date portion formatted as YYYY-MM-DD (zone-local for aware dtypes). */
  date(): CellValue[] {
    return this.mapParts(
      (d) => `${this.yearOf(d)}-${pad2(this.monthOf(d) + 1)}-${pad2(this.dayOf(d))}`
    );
  }

  /** Floor to day boundaries (zeroes the time component, zone-local). */
  floor_day(): CellValue[] {
    if (!this.zoneShifted) {
      return this.values.map((d) =>
        d === null
          ? null
          : new Date(d.getFullYear(), d.getMonth(), d.getDate())
      );
    }
    // Aware dtype: floor the zone wall clock, then unshift to the instant
    // using the same offset that produced the wall clock.
    return this.values.map((wall, i) => {
      if (wall === null) return null;
      const flooredWall = Date.UTC(
        wall.getUTCFullYear(),
        wall.getUTCMonth(),
        wall.getUTCDate()
      );
      const offset = wall.getTime() - this.instants[i]!.getTime();
      return new Date(flooredWall - offset);
    });
  }

  /**
   * pandas `Series.dt.to_period`: each cell becomes the Period containing
   * its calendar reading (zone-local fields for aware dtypes, the naive
   * wall-clock fields — consistent with `year()`/`day()` — otherwise).
   * Missing cells propagate as NaT.
   */
  to_period(freq: string): CellValue[] {
    const resolved = normalizePeriodFreq(freq);
    return this.values.map((d) => {
      if (d === null) return NaT;
      const wall = d as Date;
      return new Period(
        periodOrdinalFromFields(resolved, {
          year: this.yearOf(wall),
          month: this.monthOf(wall) + 1,
          day: this.dayOf(wall),
          hour: this.hourOf(wall),
          minute: this.minuteOf(wall),
          second: this.secondOf(wall),
        }),
        resolved
      );
    });
  }
}

/** Coerces a value to a Date, or null when not representable. */
export function normalizeDate(value: CellValue): Date | null {
  if (isMissing(value)) {
    return null;
  }
  if (value instanceof Date) {
    return Number.isNaN(value.getTime()) ? null : value;
  }
  if (typeof value === "string") {
    // pandas-style fast paths first: YYYY-MM-DD and ISO.
    const parsed = new Date(value);
    return Number.isNaN(parsed.getTime()) ? null : parsed;
  }
  if (typeof value === "number" || typeof value === "bigint") {
    return new Date(Number(value));
  }
  return null;
}
