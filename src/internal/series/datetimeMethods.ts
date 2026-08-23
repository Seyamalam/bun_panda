import type { CellValue } from "../../types";
import { isMissing } from "../../utils";

/**
 * pandas-style `.dt` accessor for Series of dates (or date-strings).
 * Missing values propagate as null. Values that are neither Date nor
 * parseable strings produce null per cell.
 */
export class DatetimeMethods {
  private readonly values: CellValue[];

  constructor(values: CellValue[]) {
    this.values = values.map((value) => normalizeDate(value));
  }

  private mapParts(fn: (d: Date) => CellValue): CellValue[] {
    return this.values.map((value) => (value === null ? null : fn(value as Date)));
  }

  year(): CellValue[] {
    return this.mapParts((d) => d.getFullYear());
  }
  month(): CellValue[] {
    return this.mapParts((d) => d.getMonth() + 1);
  }
  day(): CellValue[] {
    return this.mapParts((d) => d.getDate());
  }
  hour(): CellValue[] {
    return this.mapParts((d) => d.getHours());
  }
  minute(): CellValue[] {
    return this.mapParts((d) => d.getMinutes());
  }
  second(): CellValue[] {
    return this.mapParts((d) => d.getSeconds());
  }

  /** 0 = Monday ... 6 = Sunday, matching pandas `dayofweek`. */
  dayofweek(): CellValue[] {
    return this.mapParts((d) => (d.getDay() + 6) % 7);
  }

  /** Day name, e.g. "Monday". */
  day_name(): CellValue[] {
    const names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"];
    return this.mapParts((d) => names[(d.getDay() + 6) % 7]!);
  }

  /** 1-based day of year. */
  dayofyear(): CellValue[] {
    return this.mapParts((d) => {
      const start = Date.UTC(d.getFullYear(), 0, 1);
      const current = Date.UTC(d.getFullYear(), d.getMonth(), d.getDate());
      return Math.floor((current - start) / 86_400_000) + 1;
    });
  }

  /** 1-53 ISO-ish week number. */
  isocalendar_week(): CellValue[] {
    return this.mapParts((d) => {
      const target = new Date(Date.UTC(d.getFullYear(), d.getMonth(), d.getDate()));
      const dayNumber = (target.getUTCDay() + 6) % 7;
      target.setUTCDate(target.getUTCDate() - dayNumber + 3);
      const firstThursday = new Date(Date.UTC(target.getUTCFullYear(), 0, 4));
      const firstDayNumber = (firstThursday.getUTCDay() + 6) % 7;
      firstThursday.setUTCDate(firstThursday.getUTCDate() - firstDayNumber + 3);
      return 1 + Math.round((target.getTime() - firstThursday.getTime()) / (7 * 86_400_000));
    });
  }

  /** Days since the Unix epoch as integers. */
  to_julian(): CellValue[] {
    return this.mapParts((d) => Math.floor(d.getTime() / 86_400_000));
  }

  /** Milliseconds since epoch. */
  view_ms(): CellValue[] {
    return this.mapParts((d) => d.getTime());
  }

  /** Date portion formatted as YYYY-MM-DD. */
  date(): CellValue[] {
    return this.mapParts((d) => d.toISOString().slice(0, 10));
  }

  /** Floor to day boundaries (zeroes the time component). */
  floor_day(): CellValue[] {
    return this.mapParts(
      (d) => new Date(d.getFullYear(), d.getMonth(), d.getDate())
    );
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
