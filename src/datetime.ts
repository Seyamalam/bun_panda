import type { CellValue } from "./types";
import { Series } from "./series";

/**
 * pandas-style `to_datetime` for arrays: converts strings / epoch
 * millis / Dates into Date objects, null where unparseable.
 */
export function to_datetime(values: CellValue[]): (Date | null)[] {
  return new Series(values).to_datetime().to_list() as (Date | null)[];
}

/**
 * pandas-style `date_range`: fixed-frequency DatetimeIndex-like list
 * of Dates from `start` to `end` inclusive. Frequency is a number of
 * days (`freq: 7` = weekly) or a string: "D", "W", "M" (month start),
 * "Y" (year start), "h"/"H" (hours), "min"/"T" (minutes).
 */
export function date_range(
  options: {
    start: Date | string;
    end?: Date | string;
    periods?: number;
    freq?: number | string;
  }
): Date[] {
  const start = coerceDate(options.start);
  if (!start) {
    throw new Error("date_range requires a valid start date.");
  }

  const freq = parseFreq(options.freq ?? "D");
  const out: Date[] = [start];

  if (options.periods !== undefined) {
    let current = new Date(start.getTime());
    for (let i = 1; i < Math.max(0, options.periods); i += 1) {
      current = freq.add(current);
      out.push(new Date(current.getTime()));
    }
    return out;
  }

  if (options.end === undefined) {
    throw new Error("date_range requires either end or periods.");
  }
  const end = coerceDate(options.end);
  if (!end) {
    throw new Error("date_range requires a valid end date.");
  }
  let current = new Date(start.getTime());
  while (true) {
    current = freq.add(current);
    if (current.getTime() > end.getTime()) {
      break;
    }
    out.push(new Date(current.getTime()));
    if (out.length > 1_000_000) {
      throw new Error("date_range produced over 1,000,000 periods; check freq.");
    }
  }
  return out;
}

interface Freq {
  add(date: Date): Date;
}

function parseFreq(freq: number | string): Freq {
  if (typeof freq === "number") {
    return step(freq * 86_400_000);
  }
  const match = /^(\d+)?\s*(.+)$/.exec(freq.trim());
  const count = match?.[1] ? Number(match[1]) : 1;
  const unit = (match?.[2] ?? freq).toLowerCase();
  switch (unit) {
    case "d":
      return step(count * 86_400_000);
    case "w":
      return step(count * 7 * 86_400_000);
    case "h":
    case "hr":
    case "hour":
      return step(count * 3_600_000);
    case "min":
    case "t":
      return step(count * 60_000);
    case "s":
      return step(count * 1000);
    case "m": {
      return {
        add: (date) => {
          const next = new Date(date.getFullYear(), date.getMonth() + count, 1);
          carryEndOfMonth(date, next);
          return next;
        },
      };
    }
    case "y":
    case "a": {
      return {
        add: (date) => new Date(date.getFullYear() + count, date.getMonth(), date.getDate()),
      };
    }
    default:
      throw new Error(`Unsupported freq '${freq}'. Use days count or 'D'/'W'/'M'/'Y'/'h'/'min'.`);
  }
}

function step(ms: number): Freq {
  return { add: (date) => new Date(date.getTime() + ms) };
}

/** Keeps end-of-month anchors on end-of-month after adding months. */
function carryEndOfMonth(original: Date, next: Date): void {
  if (original.getDate() !== original.getDate()) {
    return;
  }
  const daysInMonth = new Date(next.getFullYear(), next.getMonth() + 1, 0).getDate();
  if (original.getDate() >= daysInMonth && next.getDate() < original.getDate()) {
    next.setDate(daysInMonth);
  } else if (original.getDate() <= daysInMonth && original.getDate() > 1) {
    next.setDate(Math.min(original.getDate(), daysInMonth));
  }
}

function coerceDate(value: Date | string): Date | null {
  if (value instanceof Date) {
    return value;
  }
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
}

/** True for each null/undefined/NaN entry (pandas isna). */
export function isna(values: CellValue[]): boolean[] {
  return values.map((v) => v === null || v === undefined || (typeof v === "number" && Number.isNaN(v)));
}

/** Inverse of `isna`. */
export function notna(values: CellValue[]): boolean[] {
  return isna(values).map((v) => !v);
}

export const isnull = isna;
export const notnull = notna;

/**
 * pandas-style `unique`: distinct values in first-seen order, missing
 * values excluded.
 */
export function unique<T extends CellValue>(values: T[]): T[] {
  return new Series(values).dropna().unique();
}

/**
 * pandas-style top-level `value_counts`: counts as a Series indexed by
 * the string form of each value.
 */
export function value_counts<T extends CellValue>(
  values: T[],
  options: { normalize?: boolean; ascending?: boolean; dropna?: boolean } = {}
): Series<number> {
  const counts = new Map<string, { value: T; count: number }>();
  for (const value of values) {
    if ((options.dropna ?? true) && (value === null || value === undefined)) {
      continue;
    }
    const key = String(value);
    const entry = counts.get(key);
    if (entry) {
      entry.count += 1;
    } else {
      counts.set(key, { value, count: 1 });
    }
  }
  let entries = [...counts.values()].sort(
    (left, right) => right.count - left.count
  );
  if (options.ascending) {
    entries = entries.reverse();
  }
  const total = entries.reduce((sum, entry) => sum + entry.count, 0);
  return new Series(
    entries.map((entry) => (options.normalize ? entry.count / total : entry.count)),
    { index: entries.map((entry) => String(entry.value)), name: "count" }
  );
}

/**
 * pandas-style `isin` over an array: boolean mask of membership.
 */
export function isin<T extends CellValue>(values: T[], against: CellValue[]): boolean[] {
  return new Series(values).isin(against).to_list() as boolean[];
}

/**
 * pandas-style `to_numeric`: coerces values to numbers; unparseable
 * values become NaN when errors="coerce" (default) or throw when
 * errors="raise".
 */
export function to_numeric(
  values: CellValue[],
  options: { errors?: "coerce" | "raise" } = {}
): number[] {
  const errors = options.errors ?? "coerce";
  return values.map((value) => {
    if (typeof value === "number") {
      return value;
    }
    if (typeof value === "boolean") {
      return value ? 1 : 0;
    }
    if (typeof value === "string") {
      const parsed = Number(value);
      if (!Number.isNaN(parsed)) {
        return parsed;
      }
      if (errors === "raise") {
        throw new Error(`Unable to parse value '${value}'`);
      }
      return NaN;
    }
    if (errors === "raise") {
      throw new Error(`Unable to parse value '${String(value)}'`);
    }
    return NaN;
  });
}

/**
 * pandas-style `to_timedelta`: converts durations (seconds as numbers
 * or strings like "1h 30m", "45m", "2d") to milliseconds.
 */
export function to_timedelta(values: (number | string)[]): number[] {
  return values.map((value) => {
    if (typeof value === "number") {
      return value * 1000;
    }
    let totalMs = 0;
    const regex = /(\d+(?:\.\d+)?)\s*(d|h|m|s|ms)/g;
    let match: RegExpExecArray | null;
    while ((match = regex.exec(value)) !== null) {
      const amount = Number(match[1]);
      switch (match[2]) {
        case "d": totalMs += amount * 86_400_000; break;
        case "h": totalMs += amount * 3_600_000; break;
        case "m": totalMs += amount * 60_000; break;
        case "s": totalMs += amount * 1000; break;
        case "ms": totalMs += amount; break;
      }
    }
    return totalMs;
  });
}

/**
 * pandas-style `bdate_range`: business-day (Mon-Fri) date range.
 */
export function bdate_range(
  options: { start: Date | string; end?: Date | string; periods?: number }
): Date[] {
  const start = new Date(options.start);
  if (Number.isNaN(start.getTime())) {
    throw new Error("bdate_range requires a valid start date.");
  }
  const out: Date[] = [];
  let current = new Date(start.getFullYear(), start.getMonth(), start.getDate());
  const limit = options.periods ?? 1_000_000;
  while (out.length < limit) {
    const day = current.getDay();
    if (day !== 0 && day !== 6) {
      out.push(new Date(current.getTime()));
      if (options.periods === undefined && options.end !== undefined) {
        const end = new Date(options.end);
        if (current.getTime() >= end.getTime()) {
          break;
        }
      }
    }
    if (options.periods === undefined && options.end !== undefined) {
      const end = new Date(options.end);
      if (current.getTime() > end.getTime() && out.length === 0) {
        break;
      }
      const endDay = new Date(end.getFullYear(), end.getMonth(), end.getDate());
      if (current.getTime() >= endDay.getTime()) {
        break;
      }
    }
    current.setDate(current.getDate() + 1);
  }
  return out;
}

/**
 * pandas-style top-level `map`: applies fn element-wise over an array
 * (missing values are passed through to fn like pandas object dtype).
 */
export function map<T extends CellValue, U extends CellValue>(
  values: T[],
  fn: (value: T) => U
): U[] {
  return values.map(fn);
}
