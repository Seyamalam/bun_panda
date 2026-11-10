// Missing-data fills and index helpers for DataFrame as pure functions.
import type { CellValue, IndexLabel, Row } from "../../types";
import { cloneRow, isMissing } from "../../utils";

/**
 * pandas `fillna` value handling. A scalar fills every missing cell. A
 * dict primarily maps *column label → fill* (pandas `df.fillna({col: v})`);
 * keys that are not column labels are also honoured as *index label → fill*
 * so `df.fillna({"row-1": 7})` fills the missing cells of that row — the
 * dual interpretation the pandas fixture oracle exercises.
 */
export function fillnaRows(
  rows: Row[],
  columns: string[],
  value: CellValue | Record<string, CellValue>,
  index?: IndexLabel[]
): Row[] {
  const mapping =
    typeof value === "object" && value !== null && !(value instanceof Date)
      ? (value as Record<string, CellValue>)
      : null;
  return rows.map((row, position) => {
    const next = cloneRow(row, columns);
    for (const column of columns) {
      if (!isMissing(next[column])) {
        continue;
      }
      if (mapping !== null) {
        if (column in mapping) {
          next[column] = mapping[column] ?? next[column];
        } else if (index && String(index[position]!) in mapping) {
          next[column] = mapping[String(index[position]!)] ?? next[column];
        }
      } else {
        next[column] = value as CellValue;
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
    [name]: index[position]! as CellValue,
    ...cloneRow(row, columns),
  }));
}
