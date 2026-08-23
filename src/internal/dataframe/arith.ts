// Element-wise binary arithmetic and comparison engines for DataFrame.
// Pure functions over (rows, columns, index); the DataFrame methods are
// thin wrappers that pass their private state in and rebuild a frame.
import type { CellValue, Row } from "../../types";

export function elementwiseBinaryOp(
  leftRows: Row[],
  leftColumns: string[],
  rightRows: Row[] | null,
  rightColumns: string[] | null,
  scalar: number | null,
  fn: (a: number, b: number) => number
): Row[] {
  const rows: Row[] = leftRows.map(() => ({}));
  for (let c = 0; c < leftColumns.length; c += 1) {
    const column = leftColumns[c]!;
    for (let r = 0; r < leftRows.length; r += 1) {
      const a = leftRows[r]![column];
      const b = rightRows ? rightRows[r]![rightColumns![c]!] : scalar;
      if (
        (typeof a !== "number" || !Number.isFinite(a)) ||
        (typeof b !== "number" || !Number.isFinite(b))
      ) {
        rows[r]![column] = null;
      } else {
        rows[r]![column] = fn(a, typeof b === "number" ? b : Number(b));
      }
    }
  }
  return rows;
}

export function assertSameShape(
  leftRows: Row[],
  leftColumns: string[],
  rightRows: Row[],
  rightColumns: string[]
): void {
  if (rightRows.length !== leftRows.length || rightColumns.length !== leftColumns.length) {
    throw new Error("Binary op requires frames of identical shape.");
  }
}

export function elementwiseCompareOp(
  leftRows: Row[],
  columns: string[],
  rightRows: Row[] | null,
  scalar: CellValue | null,
  fn: (a: number, b: number) => boolean
): Row[] {
  const rows: Row[] = leftRows.map(() => ({}));
  for (const column of columns) {
    for (let r = 0; r < leftRows.length; r += 1) {
      const a = leftRows[r]![column];
      const b = rightRows ? rightRows[r]?.[column] : scalar;
      if (
        typeof a === "number" && Number.isFinite(a) &&
        typeof b === "number" && Number.isFinite(b)
      ) {
        rows[r]![column] = fn(a, b);
      } else {
        rows[r]![column] = null;
      }
    }
  }
  return rows;
}
