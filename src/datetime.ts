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

/** True for each null/undefined/NaN entry (pandas `isna`). */
export function isna(values: CellValue[]): boolean[] {
  return values.map((v) => v === null || v === undefined || (typeof v === "number" && Number.isNaN(v)));
}

/** Inverse of `isna`. */
export function notna(values: CellValue[]): boolean[] {
  return isna(values).map((v) => !v);
}
