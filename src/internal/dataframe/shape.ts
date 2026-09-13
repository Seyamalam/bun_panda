// Reshape helpers for DataFrame — transpose and dtype selection as
// pure functions. (melt/pivot already delegate to reshape.ts.)
import type { CellValue, IndexLabel, Row } from "../../types";

export function transposeFrame(
  rows: Row[],
  columns: string[],
  index: IndexLabel[]
): { rows: Row[]; columns: string[]; index: string[] } {
  const outRows: Row[] = columns.map((column) => {
    const row: Row = {};
    row["index"] = column;
    for (let i = 0; i < rows.length; i += 1) {
      row[String(index[i])] = rows[i]![column];
    }
    return row;
  });
  const outColumns = ["index", ...index.map((label) => String(label))];
  return { rows: outRows, columns: outColumns, index: columns.map((c) => String(c)) };
}

export function projectColumns(rows: Row[], keep: string[]): Row[] {
  return rows.map((row) => {
    const next: Row = {};
    for (const column of keep) {
      next[column] = row[column];
    }
    return next;
  });
}

export type { CellValue };
