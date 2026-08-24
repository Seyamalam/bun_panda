import type { IndexLabel, Row } from "../../types";
import { cloneRow } from "../../utils";

/**
 * DataFrame.explode(column): turns list-valued cells into one row per
 * element, repeating every other column's value. Matches pandas defaults:
 * empty-list cells drop out, scalar/missing cells pass through unchanged,
 * and the original index label is repeated for each blown-out row.
 */
export function computeExplodeRows(
  rows: Row[],
  columns: string[],
  column: string,
  index: IndexLabel[]
): { rows: Row[]; index: IndexLabel[] } {
  const outRows: Row[] = [];
  const outIndex: IndexLabel[] = [];

  for (let i = 0; i < rows.length; i += 1) {
    const row = rows[i]!;
    const cell = row[column];

    if (Array.isArray(cell)) {
      for (const element of cell) {
        const nextRow = cloneRow(row, columns);
        nextRow[column] = (element ?? null) as Row[string];
        outRows.push(nextRow);
        outIndex.push(index[i]!);
      }
    } else {
      outRows.push(cloneRow(row, columns));
      outIndex.push(index[i]!);
    }
  }

  return { rows: outRows, index: outIndex };
}
