// Row-shift transforms for DataFrame — shift, diff, pct_change as pure
// functions over (rows, columns).
import type { Row } from "../../types";

export function shiftRows(rows: Row[], columns: string[], periods: number): Row[] {
  const out: Row[] = new Array(rows.length).fill(null).map(() => {
    const row: Row = {};
    for (const column of columns) {
      row[column] = null;
    }
    return row;
  });
  for (let i = 0; i < rows.length; i += 1) {
    const source = i - periods;
    if (source < 0 || source >= rows.length) {
      continue;
    }
    const sourceRow = rows[source]!;
    const target = out[i]!;
    for (const column of columns) {
      target[column] = sourceRow[column];
    }
  }
  return out;
}

function pairedNumeric(
  rows: Row[],
  columns: string[],
  periods: number,
  fn: (a: number, b: number) => number
): Row[] {
  const out: Row[] = new Array(rows.length).fill(null).map(() => ({}));
  for (let i = 0; i < rows.length; i += 1) {
    const target = out[i]!;
    const current = rows[i]!;
    const previous = i - periods >= 0 ? rows[i - periods] : undefined;
    for (const column of columns) {
      const a = current[column];
      const b = previous?.[column];
      if (
        typeof a === "number" && Number.isFinite(a) &&
        typeof b === "number" && Number.isFinite(b)
      ) {
        target[column] = fn(a, b);
      } else {
        target[column] = null;
      }
    }
  }
  return out;
}

/** First discrete difference of numeric columns. */
export function diffRows(rows: Row[], columns: string[], periods: number): Row[] {
  return pairedNumeric(rows, columns, periods, (a, b) => a - b);
}

/** Percentage change between the current and prior row (numeric columns). */
export function pctChangeRows(rows: Row[], columns: string[], periods: number): Row[] {
  return pairedNumeric(rows, columns, periods, (a, b) => (a - b) / b);
}
