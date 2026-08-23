// Fill helpers for DataFrame — pure functions for ffill / bfill / interpolate.
import type { CellValue, Row } from "../../types";
import { cloneRow, isMissing } from "../../utils";

export function ffillRows(rows: Row[], columns: string[]): Row[] {
  const next = rows.map((row) => cloneRow(row, columns));
  for (const column of columns) {
    let last: CellValue = undefined;
    let hasLast = false;
    for (let i = 0; i < next.length; i += 1) {
      const value = next[i]![column];
      if (!isMissing(value)) {
        last = value as CellValue;
        hasLast = true;
      } else if (hasLast) {
        next[i]![column] = last;
      }
    }
  }
  return next;
}

export function bfillRows(rows: Row[], columns: string[]): Row[] {
  const next = rows.map((row) => cloneRow(row, columns));
  for (const column of columns) {
    let nxt: CellValue = undefined;
    let hasNext = false;
    for (let i = next.length - 1; i >= 0; i -= 1) {
      const value = next[i]![column];
      if (!isMissing(value)) {
        nxt = value as CellValue;
        hasNext = true;
      } else if (hasNext) {
        next[i]![column] = nxt;
      }
    }
  }
  return next;
}

export function interpolateRows(
  rows: Row[],
  columns: string[],
  method: string = "linear"
): Row[] {
  void method;
  const next = rows.map((row) => cloneRow(row, columns));
  for (const column of columns) {
    // Collect indices of non-missing numeric values
    const known: { idx: number; val: number }[] = [];
    for (let i = 0; i < rows.length; i += 1) {
      const v = rows[i]![column];
      if (typeof v === "number" && Number.isFinite(v)) {
        known.push({ idx: i, val: v });
      }
    }
    if (known.length < 2) continue;
    // Fill gaps between known points with linear interpolation
    for (let k = 0; k < known.length - 1; k += 1) {
      const left = known[k]!;
      const right = known[k + 1]!;
      const span = right.idx - left.idx;
      for (let i = left.idx + 1; i < right.idx; i += 1) {
        const cur = next[i]![column];
        // Only interpolate if current is missing
        if (isMissing(cur)) {
          const t = (i - left.idx) / span;
          next[i]![column] = left.val + (right.val - left.val) * t;
        }
      }
    }
  }
  return next;
}
