// Column-wise reductions shared by DataFrame's stats methods.
// Each kernel delegates to the pandas Series reduction semantics over the
// column's declared dtype so frame-level results mirror pandas 3.0.5.
// The `missing` sentinel is chosen once per call at the frame level:
// pandas unifies the result Series dtype, so a frame containing any
// nullable column surfaces pd.NA where a pure-numpy frame shows NaN.
import type { CellValue, DType, IndexLabel, Row } from "../../types";
import { isMissing, isNumber, numericValues, std } from "../../utils";
import {
  pandasAllAny,
  pandasArgExtreme,
  pandasMinMax,
  pandasProd,
  pandasSum,
  requireNumeric,
  requireQuantileable,
} from "../series/pandasOps";
import { adjustedSkew, excessKurtosis, semOfMean } from "../series/stats";

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

function colValues(rows: Row[], column: string): CellValue[] {
  return rows.map((row) => row[column]);
}

function colDType(dtypes: Partial<Record<string, DType>>, column: string): DType {
  return dtypes[column] ?? "object";
}

/** pandas float32 reductions round to single precision. */
function f32Result(dtype: DType, value: CellValue): CellValue {
  return typeof value === "number" && (dtype === "float32" || dtype === "Float32")
    ? Math.fround(value)
    : value;
}

export function columnSum(columns: string[], rows: Row[], dtypes: Partial<Record<string, DType>> = {}): Record<string, CellValue> {
  const out: Record<string, CellValue> = {};
  for (const column of columns) {
    const dtype = colDType(dtypes, column);
    out[column] = f32Result(dtype, pandasSum(colValues(rows, column), dtype));
  }
  return out;
}

export function columnMean(columns: string[], rows: Row[], dtypes: Partial<Record<string, DType>> = {}, missing: CellValue = NaN): Record<string, CellValue> {
  const out: Record<string, CellValue> = {};
  for (const column of columns) {
    const dtype = colDType(dtypes, column);
    const values = requireNumeric(colValues(rows, column), dtype, "mean");
    out[column] = values.length === 0 ? missing : f32Result(dtype, values.reduce((a, b) => a + b, 0) / values.length);
  }
  return out;
}

function linearQuantile(sorted: number[], q: number): number {
  const pos = (sorted.length - 1) * q;
  const lower = Math.floor(pos);
  const upper = Math.ceil(pos);
  return sorted[lower]! + (sorted[upper]! - sorted[lower]!) * (pos - lower);
}

export function columnMedian(columns: string[], rows: Row[], dtypes: Partial<Record<string, DType>> = {}, missing: CellValue = NaN): Record<string, CellValue> {
  const out: Record<string, CellValue> = {};
  for (const column of columns) {
    const dtype = colDType(dtypes, column);
    const values = requireNumeric(colValues(rows, column), dtype, "median").sort((a, b) => a - b);
    out[column] = values.length === 0 ? missing : f32Result(dtype, linearQuantile(values, 0.5));
  }
  return out;
}
export function columnStd(columns: string[], rows: Row[], dtypes: Partial<Record<string, DType>> = {}, missing: CellValue = NaN, ddof = 1): Record<string, CellValue> {
  const out: Record<string, CellValue> = {};
  for (const column of columns) {
    const dtype = colDType(dtypes, column);
    const values = requireNumeric(colValues(rows, column), dtype, "std");
    const s = std(values);
    out[column] = values.length <= ddof || s === null || s === undefined ? missing : f32Result(dtype, s);
  }
  return out;
}
export function columnVariance(columns: string[], rows: Row[], dtypes: Partial<Record<string, DType>> = {}, missing: CellValue = NaN, ddof = 1): Record<string, CellValue> {
  const out: Record<string, CellValue> = {};
  for (const column of columns) {
    const dtype = colDType(dtypes, column);
    const values = requireNumeric(colValues(rows, column), dtype, "var");
    if (values.length <= ddof) {
      out[column] = missing;
      continue;
    }
    const mean = values.reduce((s, v) => s + v, 0) / values.length;
    out[column] = f32Result(dtype, values.reduce((s, v) => s + (v - mean) ** 2, 0) / (values.length - ddof));
  }
  return out;
}
export function columnSem(columns: string[], rows: Row[], dtypes: Partial<Record<string, DType>> = {}, missing: CellValue = NaN): Record<string, CellValue> {
  const out: Record<string, CellValue> = {};
  for (const column of columns) {
    const dtype = colDType(dtypes, column);
    const values = requireNumeric(colValues(rows, column), dtype, "sem");
    const sem = semOfMean(values);
    out[column] = sem === null || sem === undefined ? missing : f32Result(dtype, sem);
  }
  return out;
}
export function columnSkewPandas(columns: string[], rows: Row[], dtypes: Partial<Record<string, DType>> = {}, missing: CellValue = NaN): Record<string, CellValue> {
  const out: Record<string, CellValue> = {};
  for (const column of columns) {
    const dtype = colDType(dtypes, column);
    const values = requireNumeric(colValues(rows, column), dtype, "skew");
    const sk = adjustedSkew(values);
    out[column] = sk === null || sk === undefined ? missing : f32Result(dtype, sk);
  }
  return out;
}
export function columnKurtPandas(columns: string[], rows: Row[], dtypes: Partial<Record<string, DType>> = {}, missing: CellValue = NaN): Record<string, CellValue> {
  const out: Record<string, CellValue> = {};
  for (const column of columns) {
    const dtype = colDType(dtypes, column);
    const values = requireNumeric(colValues(rows, column), dtype, "kurt");
    const kurt = excessKurtosis(values);
    out[column] = kurt === null || kurt === undefined ? missing : f32Result(dtype, kurt);
  }
  return out;
}
export function columnMin(columns: string[], rows: Row[], dtypes: Partial<Record<string, DType>> = {}, missing?: CellValue): Record<string, CellValue> {
  const out: Record<string, CellValue> = {};
  for (const column of columns) {
    const dtype = colDType(dtypes, column);
    const value = pandasMinMax(colValues(rows, column), dtype, "min");
    out[column] = missing !== undefined && typeof value === "number" && Number.isNaN(value) ? missing : value;
  }
  return out;
}
export function columnMax(columns: string[], rows: Row[], dtypes: Partial<Record<string, DType>> = {}, missing?: CellValue): Record<string, CellValue> {
  const out: Record<string, CellValue> = {};
  for (const column of columns) {
    const dtype = colDType(dtypes, column);
    const value = pandasMinMax(colValues(rows, column), dtype, "max");
    out[column] = missing !== undefined && typeof value === "number" && Number.isNaN(value) ? missing : value;
  }
  return out;
}
export function columnProd(columns: string[], rows: Row[], dtypes: Partial<Record<string, DType>> = {}): Record<string, CellValue> {
  const out: Record<string, CellValue> = {};
  for (const column of columns) {
    const dtype = colDType(dtypes, column);
    out[column] = f32Result(dtype, pandasProd(colValues(rows, column), dtype));
  }
  return out;
}
export function columnQuantile(
  columns: string[],
  rows: Row[],
  qValue: number,
  dtypes: Partial<Record<string, DType>> = {},
  missing: CellValue = NaN
): Record<string, CellValue> {
  const out: Record<string, CellValue> = {};
  for (const column of columns) {
    const dtype = colDType(dtypes, column);
    const raw = colValues(rows, column);
    requireQuantileable(raw, dtype);
    const values = requireNumeric(raw, dtype, "quantile").sort((a, b) => a - b);
    out[column] = values.length === 0 ? missing : f32Result(dtype, linearQuantile(values, qValue));
  }
  return out;
}

export function columnAllAny(
  columns: string[],
  rows: Row[],
  dtypes: Partial<Record<string, DType>>,
  skipna: boolean,
  op: "all" | "any"
): Record<string, CellValue> {
  const out: Record<string, CellValue> = {};
  for (const column of columns) {
    out[column] = pandasAllAny(colValues(rows, column), colDType(dtypes, column), skipna, op);
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

export function columnNunique(columns: string[], rows: Row[]): Record<string, number> {
  const out: Record<string, number> = {};
  for (const column of columns) {
    const seen = new Set<string>();
    for (const row of rows) {
      const value = row[column];
      if (isMissing(value)) continue;
      seen.add(`${typeof value}:${value instanceof Date ? value.toISOString() : String(value)}`);
    }
    out[column] = seen.size;
  }
  return out;
}

/** Pearson correlation / covariance over aligned numeric columns. */
export function pairwiseNumericMatrix(
  columns: string[],
  rows: Row[],
  fn: (a: number[], b: number[]) => number
): { rows: Row[]; columns: string[]; index: IndexLabel[] } {
  const numericColumns = columns.filter((column) =>
    rows.some((row) => isNumber(row[column]) || typeof row[column] === "bigint")
  );
  const vectors = new Map<string, number[]>(
    numericColumns.map((column) => [column, numericValues(rows.map((row) => row[column]))])
  );
  const outRows: Row[] = [];
  for (const left of numericColumns) {
    const row: Row = {};
    for (const right of numericColumns) {
      const a = vectors.get(left)!;
      const b = vectors.get(right)!;
      row[right] = a.length > 1 && a.length === b.length ? fn(a, b) : NaN;
    }
    outRows.push(row);
  }
  return { rows: outRows, columns: numericColumns, index: [...numericColumns] };
}

export function pearson(a: number[], b: number[]): number {
  const n = a.length;
  const ma = a.reduce((s, v) => s + v, 0) / n;
  const mb = b.reduce((s, v) => s + v, 0) / n;
  let num = 0;
  let da = 0;
  let db = 0;
  for (let i = 0; i < n; i += 1) {
    const x = a[i]! - ma;
    const y = b[i]! - mb;
    num += x * y;
    da += x * x;
    db += y * y;
  }
  const denom = Math.sqrt(da * db);
  return denom === 0 ? NaN : num / denom;
}

export function covariance(a: number[], b: number[]): number {
  const n = a.length;
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
    rows.some((row) => isNumber(row[column]) || typeof row[column] === "bigint")
  );
  const stats = ["count", "mean", "std", "min", "25%", "50%", "75%", "max"];
  const outRows: Row[] = stats.map((statName) => ({ stat: statName }));
  for (const column of numericColumns) {
    const values = numericValues(rows.map((row) => row[column])).sort((a, b) => a - b);
    outRows[0]![column] = values.length;
    outRows[1]![column] =
      values.length > 0 ? values.reduce((acc, value) => acc + value, 0) / values.length : NaN;
    outRows[2]![column] = std(values) ?? NaN;
    outRows[3]![column] = values.length > 0 ? values[0]! : NaN;
    outRows[4]![column] = values.length > 0 ? linearQuantile(values, 0.25) : NaN;
    outRows[5]![column] = values.length > 0 ? linearQuantile(values, 0.5) : NaN;
    outRows[6]![column] = values.length > 0 ? linearQuantile(values, 0.75) : NaN;
    outRows[7]![column] = values.length > 0 ? values[values.length - 1]! : NaN;
  }
  return { rows: outRows, numericColumns };
}

export type { CellValue };

/** Index label of the extreme value in each column (pandas idxmax/idxmin). */
export function columnIdx(
  rows: Row[],
  columns: string[],
  index: IndexLabel[],
  mode: "max" | "min"
): Record<string, IndexLabel | null> {
  const out: Record<string, IndexLabel | null> = {};
  for (const column of columns) {
    const pos = pandasArgExtreme(colValues(rows, column), mode);
    out[column] = index[pos]!;
  }
  return out;
}
