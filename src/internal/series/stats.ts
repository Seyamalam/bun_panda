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
  if (values.length === 0) return null;
  return values.reduce((acc, value) => acc * value, 1);
}

/** Unbiased skewness (Fisher-Pearson with sample correction G1). */
export function adjustedSkew(numbers: number[]): number | null {
  const n = numbers.length;
  if (n < 3) return null;
  const mean = numbers.reduce((s, v) => s + v, 0) / n;
  const m2 = numbers.reduce((s, v) => s + (v - mean) ** 2, 0);
  const m3 = numbers.reduce((s, v) => s + (v - mean) ** 3, 0);
  const popStd = Math.sqrt(m2 / n);
  if (popStd === 0) return null;
  const g1 = m3 / popStd ** 3;
  // Sample-corrected skewness G1.
  return (Math.sqrt(n * (n - 1)) / (n - 2)) * g1;
}

/** Excess kurtosis (Fisher definition; normal distribution = 0). */
export function excessKurtosis(numbers: number[]): number | null {
  const n = numbers.length;
  if (n < 4) return null;
  const mean = numbers.reduce((s, v) => s + v, 0) / n;
  const m2 = numbers.reduce((s, v) => s + (v - mean) ** 2, 0);
  const m4 = numbers.reduce((s, v) => s + (v - mean) ** 4, 0);
  if (m2 === 0) return null;
  const sampleStd = Math.sqrt(m2 / (n - 1));
  return m4 / (sampleStd ** 4 * n) - 3;
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
