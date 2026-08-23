/**
 * Typed columnar ingest for Parquet files.
 *
 * Instead of materializing every row group into row-major records
 * (per-cell object writes), numeric columns are decoded straight into
 * Float64Arrays with NaN encoding missing values, and other columns are
 * gathered once into plain arrays. The result feeds `DataFrame.from_typed`
 * so downstream wasm kernels receive contiguous typed data with no
 * per-row re-marshal.
 */
import type { CellValue } from "../../types";

export interface TypedColumnData {
  /** Column name. */
  name: string;
  kind: "f64" | "str" | "other";
  values?: Float64Array;
  strings?: (string | null)[];
  others?: CellValue[];
  nonNull: number;
}

interface ShreddedColumn {
  count: number;
  values: unknown[];
  dlevels: number[];
  rlevels: number[];
}

function isFlatNumericField(schemaField: { primitiveType?: string; originalType?: string }): boolean {
  const t = schemaField.originalType ?? schemaField.primitiveType;
  return (
    t === "DOUBLE" ||
    t === "FLOAT" ||
    t === "INT64" ||
    t === "INT32" ||
    t === "INT" ||
    t === "INT96"
  );
}

/** Converts a decoded parquet value into a JS number or null. */
export function toNumericValue(value: unknown): number | null {
  if (value === null || value === undefined) {
    return null;
  }
  if (typeof value === "number") {
    return value;
  }
  if (typeof value === "bigint") {
    return Number(value);
  }
  if (value instanceof Date) {
    return value.getTime();
  }
  const n = Number(value);
  return Number.isNaN(n) ? null : n;
}

/**
 * Builds typed column buffers from one parquet row group's shredded
 * `columnData`. Flat (non-nested) fields only; nested/repeated fields
 * fall back to "other".
 */
export function buildTypedColumns(
  schema: { findField(key: string): { primitiveType?: string; originalType?: string; dLevelMax: number } },
  columnData: Record<string, ShreddedColumn>,
  rowCount: number
): TypedColumnData[] {
  const out: TypedColumnData[] = [];

  for (const [key, data] of Object.entries(columnData)) {
    // Column keys are either flat names ("id") or comma-joined paths
    // ("a,b" for nested fields). Only multi-segment paths are nested.
    const segments = typeof key === "string" ? key.split(",") : (key as unknown as string[]);
    const name = segments[segments.length - 1]!;
    const field = schema.findField(segments as never);

    if (segments.length !== 1) {
      out.push({ name, kind: "other", others: [], nonNull: 0 });
      continue;
    }

    const numeric = isFlatNumericField(field) && field.dLevelMax <= 1;

    if (numeric) {
      const values = new Float64Array(rowCount);
      values.fill(NaN); // missing slots must be NaN, not 0
      let nonNull = 0;
      let cursor = 0;
      let writePos = 0;
      for (let i = 0; i < data.count && cursor < data.values.length; i += 1) {
        const dLevel = data.dlevels[i];
        if (dLevel !== field.dLevelMax) {
          // Missing value: advance the row position only; stays NaN.
          if (writePos < rowCount) {
            writePos += 1;
          }
          continue;
        }
        if (writePos >= rowCount) {
          break;
        }
        const v = toNumericValue(data.values[cursor]);
        cursor += 1;
        if (v !== null && Number.isFinite(v)) {
          values[writePos] = v;
          nonNull += 1;
        }
        writePos += 1;
      }
      out.push({ name, kind: "f64", values, nonNull });
      continue;
    }

    // Non-numeric flat column: keep strings as strings, everything
    // else (booleans, dates, buffers) in its native decoded form.
    const cells: CellValue[] = new Array(rowCount).fill(null);
    let nonNullCount = 0;
    let cursor2 = 0;
    let writePos = 0;
    for (let i = 0; i < data.count && cursor2 < data.values.length; i += 1) {
      const dLevel = data.dlevels[i];
      if (dLevel !== field.dLevelMax) {
        // Missing value: advance the row position only.
        if (writePos < rowCount) {
          writePos += 1;
        }
        continue;
      }
      const raw = data.values[cursor2];
      cursor2 += 1;
      if (writePos >= rowCount) {
        break;
      }
      if (raw === null || raw === undefined) {
        writePos += 1;
        continue;
      }
      let cell: CellValue;
      if (typeof raw === "string") {
        cell = raw;
      } else if (typeof raw === "boolean" || typeof raw === "number" || raw instanceof Date) {
        cell = raw as CellValue;
      } else if (typeof Buffer !== "undefined" && Buffer.isBuffer(raw)) {
        cell = raw.toString("utf8");
      } else if (typeof raw === "bigint") {
        cell = Number(raw);
      } else {
        cell = String(raw) as CellValue;
      }
      cells[writePos] = cell;
      nonNullCount += 1;
      writePos += 1;
    }

    out.push({ name, kind: "other", others: cells, nonNull: nonNullCount });
  }

  return out;
}

/**
 * Assembles row-major records from typed column buffers — used only at
 * the final DataFrame boundary. Numeric cells are read straight from
 * the Float64Array without per-cell boxing beyond the row object.
 */
export function typedColumnsToRecords(
  columns: TypedColumnData[],
  rowCount: number
): Record<string, CellValue>[] {
  const records: Record<string, CellValue>[] = new Array(rowCount);
  for (let i = 0; i < rowCount; i += 1) {
    records[i] = {};
  }
  for (const column of columns) {
    if (column.kind === "f64") {
      const values = column.values!;
      for (let i = 0; i < rowCount; i += 1) {
        const v = values[i]!;
        records[i]![column.name] = Number.isNaN(v) ? null : v;
      }
    } else if (column.kind === "str") {
      const values = column.strings!;
      for (let i = 0; i < rowCount; i += 1) {
        records[i]![column.name] = values[i];
      }
    } else {
      const values = column.others!;
      for (let i = 0; i < rowCount; i += 1) {
        records[i]![column.name] = values[i] ?? null;
      }
    }
  }
  return records;
}
