// Column-wise numeric reductions shared by DataFrame's stats methods.
// Pure functions over (columns, rows) so both DataFrame and future
// columnar paths can reuse them without touching class internals.
import type { CellValue, IndexLabel, Row } from "../../types";
import { isMissing, isNumber, median, numericValues, std, variance } from "../../utils";

export function reduceNumericColumns(
  columns: string[],
  rows: Row[],
  reduce: (values: number[]) => number | null
): Record<string, number | null> {
  const out: Record<string, number | null> = {};
  for (const column of columns) {
    out[column] = reduce(numericValues(rows.map((row) => row[column])));
  }
  return out;
}

export function columnSum(columns: string[], rows: Row[]): Record<string, number | null> {
  return reduceNumericColumns(
    columns,
    rows,
    (values) => (values.length > 0 ? values.reduce((acc, value) => acc + value, 0) : null)
  );
}

export function columnMean(columns: string[], rows: Row[]): Record<string, number | null> {
  return reduceNumericColumns(
    columns,
    rows,
    (values) =>
      values.length > 0 ? values.reduce((acc, value) => acc + value, 0) / values.length : null
  );
}

export function columnMedian(columns: string[], rows: Row[]): Record<string, number | null> {
  return reduceNumericColumns(columns, rows, median);
}

export function columnStd(columns: string[], rows: Row[]): Record<string, number | null> {
  return reduceNumericColumns(columns, rows, std);
}

export function columnVariance(columns: string[], rows: Row[]): Record<string, number | null> {
  return reduceNumericColumns(columns, rows, variance);
}

export function columnMin(columns: string[], rows: Row[]): Record<string, number | null> {
  return reduceNumericColumns(columns, rows, (values) =>
    values.length > 0 ? Math.min(...values) : null
  );
}

export function columnMax(columns: string[], rows: Row[]): Record<string, number | null> {
  return reduceNumericColumns(columns, rows, (values) =>
    values.length > 0 ? Math.max(...values) : null
  );
}

export function columnProd(columns: string[], rows: Row[]): Record<string, number | null> {
  const out: Record<string, number | null> = {};
  for (const column of columns) {
    const values = numericValues(rows.map((row) => row[column]));
    out[column] = values.length > 0 ? values.reduce((acc, value) => acc * value, 1) : null;
  }
  return out;
}

export function columnQuantile(
  columns: string[],
  rows: Row[],
  qValue: number
): Record<string, number | null> {
  const out: Record<string, number | null> = {};
  for (const column of columns) {
    const values = numericValues(rows.map((row) => row[column])).sort((a, b) => a - b);
    if (values.length === 0) {
      out[column] = null;
      continue;
    }
    const pos = (values.length - 1) * qValue;
    const lower = Math.floor(pos);
    const upper = Math.ceil(pos);
    const t = pos - lower;
    out[column] = values[lower]! + (values[upper]! - values[lower]!) * t;
  }
  return out;
}

export function columnCount(columns: string[], rows: Row[]): Record<string, number> {
  const out: Record<string, number> = {};
  for (const column of columns) {
    out[column] = rows.filter((row) => !isMissing(row[column])).length;
  }
  return out;
}

/** Pearson correlation / covariance over aligned numeric columns. */
export function pairwiseNumericMatrix(
  columns: string[],
  rows: Row[],
  fn: (a: number[], b: number[]) => number
): { rows: Row[]; columns: string[]; index: IndexLabel[] } {
  const numericCols = columns.filter((column) =>
    rows.some((row) => typeof row[column] === "number")
  );
  const outRows: Row[] = numericCols.map(() => ({}));
  for (let c = 0; c < numericCols.length; c += 1) {
    for (let r = 0; r < numericCols.length; r += 1) {
      const a = numericValues(rows.map((row) => row[numericCols[r]!]));
      const b = numericValues(rows.map((row) => row[numericCols[c]!]));
      outRows[r]![numericCols[c]!] =
        numericCols[r] === numericCols[c] ? 1 : fn(a, b);
    }
  }
  return { rows: outRows, columns: [...numericCols], index: [...numericCols] };
}

export function pearson(a: number[], b: number[]): number {
  const n = a.length;
  if (n < 2) return NaN;
  const ma = a.reduce((s, v) => s + v, 0) / n;
  const mb = b.reduce((s, v) => s + v, 0) / n;
  let cov = 0;
  let va = 0;
  let vb = 0;
  for (let i = 0; i < n; i += 1) {
    const da = a[i]! - ma;
    const dbv = b[i]! - mb;
    cov += da * dbv;
    va += da * da;
    vb += dbv * dbv;
  }
  const denom = Math.sqrt(va) * Math.sqrt(vb);
  return denom === 0 ? NaN : cov / denom;
}

export function covariance(a: number[], b: number[]): number {
  const n = a.length;
  if (n < 2) return NaN;
  const ma = a.reduce((s, v) => s + v, 0) / n;
  const mb = b.reduce((s, v) => s + v, 0) / n;
  let cov = 0;
  for (let i = 0; i < n; i += 1) {
    cov += (a[i]! - ma) * (b[i]! - mb);
  }
  return cov / (n - 1);
}

/** describe() stat table rows for the five headline statistics. */
export function describeStatRows(
  columns: string[],
  rows: Row[]
): { rows: Row[]; numericColumns: string[] } {
  const numericColumns = columns.filter((column) =>
    rows.some((row) => isNumber(row[column]))
  );
  const stats = ["count", "mean", "std", "min", "max"];
  const outRows: Row[] = stats.map((statName) => ({ stat: statName }));
  for (const column of numericColumns) {
    const values = numericValues(rows.map((row) => row[column]));
    outRows[0]![column] = values.length;
    outRows[1]![column] =
      values.length > 0 ? values.reduce((acc, value) => acc + value, 0) / values.length : null;
    outRows[2]![column] = std(values);
    outRows[3]![column] = values.length > 0 ? Math.min(...values) : null;
    outRows[4]![column] = values.length > 0 ? Math.max(...values) : null;
  }
  return { rows: outRows, numericColumns };
}

export type { CellValue };
