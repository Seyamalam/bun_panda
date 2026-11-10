// Element-wise binary arithmetic and comparison engines for DataFrame.
// Pure functions over (rows, columns, index); the DataFrame methods are
// thin wrappers that pass their private state in and rebuild a frame.
import type { CellValue, Row } from "../../types";
import { bigintOperand } from "../shared/valueSemantics";

export function elementwiseBinaryOp(
  leftRows: Row[],
  leftColumns: string[],
  rightRows: Row[] | null,
  rightColumns: string[] | null,
  scalar: number | bigint | null,
  fn: (a: number, b: number) => number,
  bigintFn?: (a: bigint, b: bigint) => bigint | null
): Row[] {
  const rows: Row[] = leftRows.map(() => ({}));
  for (let c = 0; c < leftColumns.length; c += 1) {
    const column = leftColumns[c]!;
    for (let r = 0; r < leftRows.length; r += 1) {
      const a = leftRows[r]![column];
      const b = rightRows ? rightRows[r]![rightColumns![c]!] : scalar;
      // The bigint kernel only engages when an operand is genuinely bigint
      // and both are integer-representable; a null result falls back to the
      // number kernel (pandas' NaN/float widening for zero divisors).
      if (bigintFn && (typeof a === "bigint" || typeof b === "bigint")) {
        const wa = bigintOperand(a);
        const wb = bigintOperand(b);
        if (wa !== null && wb !== null) {
          let wide: bigint | null = null;
          try { wide = bigintFn(wa, wb); } catch { wide = null; }
          if (wide !== null) {
            rows[r]![column] = wide;
            continue;
          }
        }
      }
      const an = typeof a === "bigint" ? Number(a) : a;
      const bn = typeof b === "bigint" ? Number(b) : b;
      if (
        (typeof an !== "number" || !Number.isFinite(an)) ||
        (typeof bn !== "number" || !Number.isFinite(bn))
      ) {
        rows[r]![column] = null;
      } else {
        rows[r]![column] = fn(an, bn);
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
  fn: (a: number | bigint, b: number | bigint) => boolean
): Row[] {
  const rows: Row[] = leftRows.map(() => ({}));
  for (const column of columns) {
    for (let r = 0; r < leftRows.length; r += 1) {
      const a = leftRows[r]![column];
      const b = rightRows ? rightRows[r]?.[column] : scalar;
      const aNum = typeof a === "bigint" || (typeof a === "number" && Number.isFinite(a));
      const bNum = typeof b === "bigint" || (typeof b === "number" && Number.isFinite(b));
      if (aNum && bNum) {
        rows[r]![column] = fn(a as number | bigint, b as number | bigint);
      } else {
        rows[r]![column] = null;
      }
    }
  }
  return rows;
}
