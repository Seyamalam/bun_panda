// Missing-data fills and index helpers for DataFrame as pure functions.
import type { CellValue, IndexLabel, Row } from "../../types";
import { cloneRow, isMissing } from "../../utils";

export function fillnaRows(
  rows: Row[],
  columns: string[],
  value: CellValue | Record<string, CellValue>
): Row[] {
  return rows.map((row) => {
    const next = cloneRow(row, columns);
    for (const column of columns) {
      if (!isMissing(next[column])) {
        continue;
      }
      if (typeof value === "object" && value !== null && !(value instanceof Date)) {
        next[column] = value[column] ?? next[column];
      } else {
        next[column] = value;
      }
    }
    return next;
  });
}

export function indexLabelsFromColumn(
  rows: Row[],
  column: string,
  fallback: IndexLabel[]
): IndexLabel[] {
  return rows.map((row, position) => {
    const value = row[column];
    if (typeof value === "number" || typeof value === "string") {
      return value;
    }
    return String(value ?? fallback[position]!);
  });
}

export function resetIndexRows(
  rows: Row[],
  columns: string[],
  index: IndexLabel[],
  name: string
): Row[] {
  return rows.map((row, position) => ({
    [name]: index[position]!,
    ...cloneRow(row, columns),
  }));
}
