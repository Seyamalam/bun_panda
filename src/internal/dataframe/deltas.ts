// Row-shift transforms for DataFrame — shift, diff, pct_change as pure
// functions over (rows, columns).
import type { Row } from "../../types";
import { bigintOperand } from "../shared/valueSemantics";

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
        (typeof a === "number" && Number.isFinite(a)) || typeof a === "bigint"
      ) {
        if (
          (typeof b === "number" && Number.isFinite(b)) || typeof b === "bigint"
        ) {
          target[column] = fn(Number(a), Number(b));
        } else {
          target[column] = NaN;
        }
      } else {
        target[column] = NaN;
      }
    }
  }
  return out;
}

/** First discrete difference of numeric columns (bigint cells stay exact). */
export function diffRows(rows: Row[], columns: string[], periods: number): Row[] {
  const out: Row[] = new Array(rows.length).fill(null).map(() => ({}));
  for (let i = 0; i < rows.length; i += 1) {
    const target = out[i]!;
    const current = rows[i]!;
    const previous = i - periods >= 0 ? rows[i - periods] : undefined;
    for (const column of columns) {
      const a = current[column];
      const b = previous?.[column];
      const wa = bigintOperand(a);
      const wb = bigintOperand(b ?? null);
      if (wa !== null && wb !== null) {
        target[column] = wa - wb;
      } else if (
        (typeof a === "number" && Number.isFinite(a)) &&
        (typeof b === "number" && Number.isFinite(b))
      ) {
        target[column] = a - b;
      } else {
        target[column] = NaN;
      }
    }
  }
  return out;
}

/** Percentage change between the current and prior row (numeric columns). */
export function pctChangeRows(rows: Row[], columns: string[], periods: number): Row[] {
  return pairedNumeric(rows, columns, periods, (a, b) => (a - b) / b);
}
