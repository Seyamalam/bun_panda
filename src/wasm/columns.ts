/**
 * Columnar typed-array store for bun_panda DataFrames.
 *
 * Numeric columns are held as Float64Array with NaN encoding missing
 * values; string/boolean/date columns stay as plain arrays. Built once
 * per DataFrame snapshot and cached with the same invalidation key the
 * GroupBy partition cache uses, so repeated aggregations over an
 * unchanged frame skip all marshalling.
 *
 * This is the substrate for zero-copy wasm: numeric columns can be
 * handed to kernels as raw pointers into wasm memory in one copy, or
 * later, into shared memory directly.
 */
import type { CellValue, Row } from "../types";

export interface NumericColumn {
  kind: "f64";
  values: Float64Array;
  /** Count of non-missing (non-NaN) entries. */
  nonNull: number;
}

export interface TextColumn {
  kind: "str";
  values: string[];
  /** Indices of rows whose value is null/undefined. */
  missing: Int32Array;
}

export interface MixedColumn {
  kind: "mixed";
  values: CellValue[];
}

export type AnyColumn = NumericColumn | TextColumn | MixedColumn;

export interface ColumnStore {
  columns: Map<string, AnyColumn>;
  rowCount: number;
}

const storesByRows = new WeakMap<Row[], ColumnStore>();

/** True when every defined value in the column is a finite number. */
function isNumericColumn(rows: Row[], column: string): boolean {
  for (let i = 0; i < rows.length; i += 1) {
    const value = rows[i]![column];
    if (
      value !== null &&
      value !== undefined &&
      (typeof value !== "number" || !Number.isFinite(value))
    ) {
      return false;
    }
  }
  return true;
}

function isTextColumn(rows: Row[], column: string): boolean {
  for (let i = 0; i < rows.length; i += 1) {
    const value = rows[i]![column];
    if (value !== null && value !== undefined && typeof value !== "string") {
      return false;
    }
  }
  return true;
}

function buildColumn(rows: Row[], column: string): AnyColumn {
  const n = rows.length;

  if (isNumericColumn(rows, column)) {
    let nonNull = 0;
    const values = new Float64Array(n);
    for (let i = 0; i < n; i += 1) {
      const value = rows[i]![column];
      if (value === null || value === undefined) {
        values[i] = NaN;
      } else {
        values[i] = value as number;
        nonNull += 1;
      }
    }
    return { kind: "f64", values, nonNull };
  }

  if (isTextColumn(rows, column)) {
    const values = new Array<string>(n);
    const missing: number[] = [];
    for (let i = 0; i < n; i += 1) {
      const value = rows[i]![column];
      if (value === null || value === undefined) {
        values[i] = "";
        missing.push(i);
      } else {
        values[i] = value as string;
      }
    }
    return { kind: "str", values, missing: Int32Array.from(missing) };
  }

  const values = new Array<CellValue>(n);
  for (let i = 0; i < n; i += 1) {
    values[i] = rows[i]![column];
  }
  return { kind: "mixed", values };
}

/**
 * Returns the store for one immutable row snapshot and builds only the
 * requested columns that are not already present.
 */
export function buildColumnStore(rows: Row[], columns: string[]): ColumnStore {
  let store = storesByRows.get(rows);
  if (!store || store.rowCount !== rows.length) {
    store = { columns: new Map<string, AnyColumn>(), rowCount: rows.length };
    storesByRows.set(rows, store);
  }

  for (const column of columns) {
    if (!store.columns.has(column)) {
      store.columns.set(column, buildColumn(rows, column));
    }
  }

  return store;
}

/** True when every requested column has already been materialized. */
export function hasCachedColumns(rows: Row[], columns: string[]): boolean {
  const store = storesByRows.get(rows);
  return Boolean(store && columns.every((column) => store.columns.has(column)));
}

/**
 * Seeds numeric columns supplied by a typed reader. Values are copied so
 * later mutation of caller-owned arrays cannot change the DataFrame.
 */
export function primeNumericColumns(
  rows: Row[],
  data: Record<string, CellValue[] | Float64Array>
): void {
  let store = storesByRows.get(rows);
  if (!store || store.rowCount !== rows.length) {
    store = { columns: new Map<string, AnyColumn>(), rowCount: rows.length };
    storesByRows.set(rows, store);
  }

  for (const [column, source] of Object.entries(data)) {
    if (!(source instanceof Float64Array) || source.length !== rows.length) continue;
    const values = new Float64Array(source);
    let nonNull = 0;
    for (let i = 0; i < values.length; i += 1) {
      if (!Number.isNaN(values[i]!)) nonNull += 1;
    }
    store.columns.set(column, { kind: "f64", values, nonNull });
  }
}

/** Invalidates typed columns before an in-place row mutation. */
export function invalidateColumnStore(rows: Row[]): void {
  storesByRows.delete(rows);
}
