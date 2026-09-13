import { indexLabelEquals } from "../shared/indexing";
import { scalarEquals } from "../shared/valueSemantics";
// Numeric statistics for Series — pure functions over raw value arrays.
import type { CellValue } from "../../types";
import { isMissing } from "../../utils";

export function seriesCount(values: CellValue[]): number {
  return values.filter((value) => !isMissing(value)).length;
}

export function seriesVariance(values: number[]): number | null {
  if (values.length < 2) return null;
  const mean = values.reduce((s, v) => s + v, 0) / values.length;
  return values.reduce((s, v) => s + (v - mean) ** 2, 0) / (values.length - 1);
}

export function seriesProd(values: number[]): number | null {
  return values.reduce((acc, value) => acc * value, 1);
}

/** Unbiased skewness (Fisher-Pearson with sample correction G1). */
export function adjustedSkew(numbers: number[]): number | null {
  const n = numbers.length;
  if (n < 3) return null;
  const mean = numbers.reduce((s, v) => s + v, 0) / n;
  const m2 = numbers.reduce((s, v) => s + (v - mean) ** 2, 0);
  const m3 = numbers.reduce((s, v) => s + (v - mean) ** 3, 0);
  if (m2 === 0) return 0;
  return (n * Math.sqrt(n - 1) / (n - 2)) * (m3 / m2 ** 1.5);
}

/** Unbiased excess kurtosis (Fisher definition; normal distribution = 0). */
export function excessKurtosis(numbers: number[]): number | null {
  const n = numbers.length;
  if (n < 4) return null;
  const mean = numbers.reduce((s, v) => s + v, 0) / n;
  const m2 = numbers.reduce((s, v) => s + (v - mean) ** 2, 0);
  const m4 = numbers.reduce((s, v) => s + (v - mean) ** 4, 0);
  if (m2 === 0) return 0;
  const correction = (n - 2) * (n - 3);
  return (n * (n + 1) * (n - 1) * m4) / (correction * m2 ** 2) - (3 * (n - 1) ** 2) / correction;
}

/** Standard error of the mean (sample std / sqrt(n)). */
export function semOfMean(numbers: number[]): number | null {
  if (numbers.length < 2) return null;
  const mean = numbers.reduce((s, v) => s + v, 0) / numbers.length;
  const sampleStd = Math.sqrt(
    numbers.reduce((s, v) => s + (v - mean) ** 2, 0) / (numbers.length - 1)
  );
  return sampleStd / Math.sqrt(numbers.length);
}

export function seriesStd(values: number[], ddof = 1): number | null {
  if (values.length <= ddof) return null;
  const mean = values.reduce((s, v) => s + v, 0) / values.length;
  const variance = values.reduce((s, v) => s + (v - mean) ** 2, 0) / (values.length - ddof);
  return Math.sqrt(variance);
}

export function seriesAll(values: CellValue[], skipna = true): boolean {
  for (const value of values) {
    if (isMissing(value)) {
      if (skipna) continue;
      return false;
    }
    if (typeof value === "number" && Number.isNaN(value)) {
      if (skipna) continue;
      return false;
    }
    if (!value) return false;
  }
  return true;
}

export function seriesAny(values: CellValue[], skipna = true): boolean {
  for (const value of values) {
    if (isMissing(value)) {
      if (skipna) continue;
      continue;
    }
    if (typeof value === "number" && Number.isNaN(value)) {
      if (skipna) continue;
      continue;
    }
    if (value) return true;
  }
  return false;
}

export function seriesArgmax(values: CellValue[]): number | null {
  let best: number | null = null;
  let bestPos: number | null = null;
  for (let i = 0; i < values.length; i += 1) {
    const value = values[i]!;
    if (isMissing(value) || typeof value !== "number" || Number.isNaN(value)) continue;
    if (best === null || value > best) {
      best = value;
      bestPos = i;
    }
  }
  return bestPos;
}

export function seriesArgmin(values: CellValue[]): number | null {
  let best: number | null = null;
  let bestPos: number | null = null;
  for (let i = 0; i < values.length; i += 1) {
    const value = values[i]!;
    if (isMissing(value) || typeof value !== "number" || Number.isNaN(value)) continue;
    if (best === null || value < best) {
      best = value;
      bestPos = i;
    }
  }
  return bestPos;
}

export function seriesEquals(
  leftValues: CellValue[],
  leftIndex: import("../../types").IndexLabel[],
  rightValues: CellValue[],
  rightIndex: import("../../types").IndexLabel[]
): boolean {
  if (leftValues.length !== rightValues.length) return false;
  if (leftIndex.length !== rightIndex.length) return false;
  for (let i = 0; i < leftIndex.length; i += 1) {
    if (!indexLabelEquals(leftIndex[i]!, rightIndex[i]!)) return false;
  }
  for (let i = 0; i < leftValues.length; i += 1) {
    if (!scalarEquals(leftValues[i], rightValues[i])) return false;
  }
  return true;
}

export function seriesDescribe(values: CellValue[]): { keys: string[]; values: CellValue[] } {
  const numeric = values.filter((value) => typeof value === "number" && Number.isFinite(value)) as number[];
  const count = values.filter((value) => !isMissing(value) && !(typeof value === "number" && Number.isNaN(value as number))).length;
  if (numeric.length > 0 && numeric.length === count) {
    const sorted = [...numeric].sort((a, b) => a - b);
    const meanVal = numeric.reduce((s, v) => s + v, 0) / numeric.length;
    const stdVal = seriesStd(numeric, 1);
    const q = (p: number): number => {
      const pos = (sorted.length - 1) * p;
      const lower = Math.floor(pos);
      const upper = Math.ceil(pos);
      return sorted[lower]! + (sorted[upper]! - sorted[lower]!) * (pos - lower);
    };
    return {
      keys: ["count", "mean", "std", "min", "25%", "50%", "75%", "max"],
      values: [count, meanVal, stdVal, sorted[0]!, q(0.25), q(0.5), q(0.75), sorted[sorted.length - 1]!],
    };
  }
  // object / mixed fallback similar to pandas describe for non-numeric
  const nonMissing = values.filter((value) => !isMissing(value) && !(typeof value === "number" && Number.isNaN(value as number)));
  const unique = new Set(nonMissing.map((value) => (value instanceof Date ? value.toISOString() : String(value)))).size;
  let top: CellValue = null as unknown as CellValue;
  let freq = 0;
  if (nonMissing.length > 0) {
    const counts = new Map<string, { value: CellValue; count: number }>();
    for (const value of nonMissing) {
      const key = value instanceof Date ? `date:${value.toISOString()}` : `${typeof value}:${String(value)}`;
      const entry = counts.get(key);
      if (entry) entry.count += 1;
      else counts.set(key, { value, count: 1 });
    }
    let best: { value: CellValue; count: number } | null = null;
    for (const entry of counts.values()) {
      if (!best || entry.count > best.count) best = entry;
    }
    if (best) {
      top = best.value;
      freq = best.count;
    }
  }
  return {
    keys: ["count", "unique", "top", "freq"],
    values: [count, unique, top, freq],
  };
}

/** Lag-N autocorrelation over aligned non-missing numeric pairs. */
export function autocorrelation(rawValues: CellValue[], lag: number): number | null {
  const a: number[] = [];
  const b: number[] = [];
  for (let i = lag; i < rawValues.length; i += 1) {
    const x = rawValues[i];
    const y = rawValues[i - lag];
    if (
      !isMissing(x) && typeof x === "number" &&
      !isMissing(y) && typeof y === "number"
    ) {
      a.push(x);
      b.push(y);
    }
  }
  const n = a.length;
  if (n < 2) return null;
  const ma = a.reduce((s, v) => s + v, 0) / n;
  const mb = b.reduce((s, v) => s + v, 0) / n;
  let cov = 0;
  let va = 0;
  let vb = 0;
  for (let i = 0; i < n; i += 1) {
    cov += (a[i]! - ma) * (b[i]! - mb);
    va += (a[i]! - ma) ** 2;
    vb += (b[i]! - mb) ** 2;
  }
  const denom = Math.sqrt(va) * Math.sqrt(vb);
  return denom === 0 ? null : cov / denom;
}
