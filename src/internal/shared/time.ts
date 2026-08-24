// Shared time-of-day parsing for Series/DataFrame at_time/between_time.
import type { CellValue } from "../../types";

/** Parses "HH:MM[:SS]" into seconds-of-day; null when invalid. */
export function parseTimeOfDay(text: string): number | null {
  const match = /^(\d{1,2}):(\d{2})(?::(\d{2}))?$/.exec(text.trim());
  if (!match) return null;
  const h = Number(match[1]);
  const m = Number(match[2]);
  const s = match[3] !== undefined ? Number(match[3]) : 0;
  if (h > 23 || m > 59 || s > 59) return null;
  return h * 3600 + m * 60 + s;
}

/** Seconds-of-day for a Date-like cell; null when the value is not datetime-like. */
export function secondsOfDay(value: CellValue): number | null {
  if (value instanceof Date && !Number.isNaN(value.getTime())) {
    return value.getHours() * 3600 + value.getMinutes() * 60 + value.getSeconds();
  }
  if (typeof value === "string") {
    const parsed = new Date(value);
    if (Number.isNaN(parsed.getTime())) return null;
    return parsed.getHours() * 3600 + parsed.getMinutes() * 60 + parsed.getSeconds();
  }
  return null;
}

/**
 * Exponentially weighted scan (pandas ewm, adjust=True).
 * Returns per-position values; positions before min_periods observations
 * are null.
 */
export function ewmValues(
  values: (number | null)[],
  span: number,
  minPeriods: number,
  kind: "mean" | "sum" | "std"
): (number | null)[] {
  const alpha = 2 / (span + 1);
  const out: (number | null)[] = new Array(values.length).fill(null);
  let weightedSum = 0;
  let weightTotal = 0;
  let count = 0;
  // Welford-style running variance over the weighted sample.
  let weightedMean = 0;
  let weightedM2 = 0;

  for (let i = 0; i < values.length; i += 1) {
    const v = values[i];
    if (v === null || Number.isNaN(v)) {
      // pandas ewm ignores missing values but keeps positions aligned.
      continue;
    }
    count += 1;
    weightTotal = 1 + (1 - alpha) * weightTotal;
    if (kind === "sum") {
      weightedSum = v + (1 - alpha) * weightedSum;
      if (count >= minPeriods) out[i] = weightedSum;
      continue;
    }
    const prevMean = weightedMean;
    // adjust=True: weights renormalize over all observations so far, so the
    // running mean is a plain weighted average with decay weight 1-alpha.
    if (count === 1) {
      weightedMean = v;
      weightedM2 = 0;
    } else {
      weightedMean = alpha * v + (1 - alpha) * prevMean;
      weightedM2 = (1 - alpha) * (weightedM2 + alpha * (v - prevMean) ** 2);
    }
    if (count >= minPeriods) {
      out[i] = kind === "mean" ? weightedMean : Math.sqrt(weightedM2 / weightTotal);
    }
  }
  return out;
}

/** Parses a pandas-style frequency string into milliseconds. */
export function parseFreqMs(freq: string): number {
  const match = /^(\d+)?\s*(ms|s|sec|min|T|h|H|D|day|W)$/.exec(freq.trim());
  if (!match) throw new Error(`Unsupported frequency '${freq}'.`);
  const n = match[1] ? Number(match[1]) : 1;
  switch (match[2]) {
    case "ms": return n;
    case "s":
    case "sec": return n * 1000;
    case "min":
    case "T": return n * 60_000;
    case "h":
    case "H": return n * 3_600_000;
    case "D":
    case "day": return n * 86_400_000;
    case "W": return n * 7 * 86_400_000;
  }
  throw new Error(`Unsupported frequency '${freq}'.`);
}
