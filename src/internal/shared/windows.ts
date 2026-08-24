// Window/time/level helpers shared by DataFrame and Series parity APIs.
import type { CellValue, IndexLabel, Row } from "../../types";
import { isMissing } from "../../utils";
import { parseTimeOfDay, secondsOfDay } from "./time";

export { parseTimeOfDay, secondsOfDay };

/** Positions kept after a time-of-day filter over the given row values. */
export function timeFilterPositions(
  values: CellValue[],
  startSeconds: number | null,
  endSeconds: number | null,
  inclusive: "both" | "neither" | "left" | "right"
): number[] {
  const positions: number[] = [];
  for (let i = 0; i < values.length; i += 1) {
    const seconds = secondsOfDay(values[i]!);
    if (seconds === null) continue;
    if (startSeconds !== null && endSeconds !== null && startSeconds > endSeconds) {
      // Wrap-around window (e.g. 22:00-06:00).
      const inLate = seconds >= startSeconds;
      const inEarly = seconds <= endSeconds;
      if (inclusive === "left" ? inLate || seconds < endSeconds : inLate || inEarly) {
        if (inclusive === "neither" && (seconds === startSeconds || seconds === endSeconds)) continue;
        positions.push(i);
      }
      continue;
    }
    let keep = true;
    if (startSeconds !== null) {
      keep = inclusive === "neither" || inclusive === "right"
        ? seconds > startSeconds
        : seconds >= startSeconds;
    }
    if (keep && endSeconds !== null) {
      keep = inclusive === "neither" || inclusive === "left"
        ? seconds < endSeconds
        : seconds <= endSeconds;
    }
    if (keep) positions.push(i);
  }
  return positions;
}

/** Union or intersection of two label arrays, sorted for determinism. */
export function joinedLabels(
  left: IndexLabel[],
  right: IndexLabel[],
  join: "outer" | "inner"
): IndexLabel[] {
  if (join === "outer") {
    return [...new Set([...left, ...right])].sort((a, b) =>
      String(a).localeCompare(String(b))
    );
  }
  const rightSet = new Set(right.map(String));
  const seen = new Set<string>();
  const out: IndexLabel[] = [];
  for (const label of left) {
    if (rightSet.has(String(label)) && !seen.has(String(label))) {
      seen.add(String(label));
      out.push(label);
    }
  }
  return out;
}

/**
 * Bins rows by a datetime value column on a fixed frequency and returns
 * { binStartMs, positions } per bin that has at least one row.
 */
export function resampleBins(
  values: CellValue[],
  freqMs: number
): { binStartMs: number; positions: number[] }[] {
  const times: number[] = [];
  for (const v of values) {
    if (v instanceof Date && !Number.isNaN(v.getTime())) times.push(v.getTime());
    else if (typeof v === "string") {
      const parsed = new Date(v);
      if (!Number.isNaN(parsed.getTime())) times.push(parsed.getTime());
      else times.push(Number.NaN);
    } else times.push(Number.NaN);
  }
  const valid = times.filter((t) => !Number.isNaN(t));
  if (valid.length === 0) return [];
  const minT = Math.min(...valid);
  const maxT = Math.max(...valid);
  const bins: { binStartMs: number; positions: number[] }[] = [];
  for (let start = Math.floor(minT / freqMs) * freqMs; start <= maxT; start += freqMs) {
    const positions: number[] = [];
    for (let i = 0; i < times.length; i += 1) {
      if (!Number.isNaN(times[i]) && times[i]! >= start && times[i]! < start + freqMs) {
        positions.push(i);
      }
    }
    if (positions.length > 0) bins.push({ binStartMs: start, positions });
  }
  return bins;
}

/** Numeric values at the given positions with missing entries dropped. */
export function numericAt(rows: Row[], column: string, positions: number[]): number[] {
  const out: number[] = [];
  for (const p of positions) {
    const v = rows[p]?.[column];
    if (!isMissing(v) && typeof v === "number" && Number.isFinite(v)) out.push(v);
  }
  return out;
}
