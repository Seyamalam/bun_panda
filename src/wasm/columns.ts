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

/**
 * Builds a ColumnStore from row-major records. Numeric detection scans
 * once per column; mixed-type columns keep their original values.
 */
export function buildColumnStore(rows: Row[], columns: string[]): ColumnStore {
  const out = new Map<string, AnyColumn>();
  const n = rows.length;

  for (const column of columns) {
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
      out.set(column, { kind: "f64", values, nonNull });
      continue;
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
      out.set(column, { kind: "str", values, missing: Int32Array.from(missing) });
      continue;
    }

    const values = new Array<CellValue>(n);
    for (let i = 0; i < n; i += 1) {
      values[i] = rows[i]![column];
    }
    out.set(column, { kind: "mixed", values });
  }

  return { columns: out, rowCount: n };
}
