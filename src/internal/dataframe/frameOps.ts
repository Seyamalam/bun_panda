import type { CellValue, IndexLabel, Row } from "../../types";
import { isMissing } from "../../utils";

/**
 * DataFrame.update(other): in-place cell merge. For every row aligned by
 * index label and every column present in both frames, overwrites the
 * self cell with `other`'s value when that value is non-missing.
 * Mutates `rows` in place and returns the number of overwritten cells.
 */
export function applyUpdateOverwrite(
  rows: Row[],
  index: IndexLabel[],
  columns: string[],
  otherRows: Row[],
  otherIndex: IndexLabel[],
  otherColumns: string[]
): number {
  const sharedColumns = columns.filter((column) => otherColumns.includes(column));
  if (sharedColumns.length === 0) {
    return 0;
  }

  const otherPositionByLabel = new Map<IndexLabel, number>();
  for (let i = 0; i < otherIndex.length; i += 1) {
    const label = otherIndex[i]!;
    if (!otherPositionByLabel.has(label)) {
      otherPositionByLabel.set(label, i);
    }
  }
  if (otherPositionByLabel.size === 0) {
    return 0;
  }

  let updated = 0;
  for (let i = 0; i < rows.length; i += 1) {
    const otherPosition = otherPositionByLabel.get(index[i]!);
    if (otherPosition === undefined) {
      continue;
    }
    const sourceRow = otherRows[otherPosition]!;
    const targetRow = rows[i]!;
    for (const column of sharedColumns) {
      const incoming = sourceRow[column];
      if (!isMissing(incoming)) {
        targetRow[column] = incoming;
        updated += 1;
      }
    }
  }
  return updated;
}

function toFiniteNumber(value: CellValue): number | null {
  if (typeof value === "number" && !Number.isNaN(value)) {
    return value;
  }
  if (typeof value === "boolean") {
    return value ? 1 : 0;
  }
  return null;
}

/**
 * DataFrame.dot(other): matrix product of two all-numeric frames.
 * Self must have as many columns as `other` has rows (positional match).
 * Returns an n×m grid where entry [i][j] is the dot product of self row i
 * with other column j; cells touching a missing/non-numeric operand are null.
 */
export function computeDotMatrix(
  selfRows: Row[],
  selfColumns: string[],
  otherRows: Row[],
  otherColumns: string[]
): CellValue[][] {
  const innerDimension = selfColumns.length;
  if (innerDimension !== otherRows.length) {
    throw new Error(
      `Dot product shape mismatch: left has ${selfColumns.length} columns but right has ${otherRows.length} rows.`
    );
  }

  // Pre-extract numeric matrices; null marks a missing/non-numeric operand.
  const left: Array<Array<number | null>> = new Array(selfRows.length);
  for (let i = 0; i < selfRows.length; i += 1) {
    left[i] = new Array(innerDimension);
    for (let j = 0; j < innerDimension; j += 1) {
      left[i]![j] = toFiniteNumber(selfRows[i]![selfColumns[j]!]);
    }
  }
  const right: Array<Array<number | null>> = new Array(otherRows.length);
  for (let j = 0; j < otherRows.length; j += 1) {
    right[j] = new Array(otherColumns.length);
    for (let k = 0; k < otherColumns.length; k += 1) {
      right[j]![k] = toFiniteNumber(otherRows[j]![otherColumns[k]!]);
    }
  }

  const result: CellValue[][] = new Array(selfRows.length);
  for (let i = 0; i < selfRows.length; i += 1) {
    result[i] = new Array(otherColumns.length);
    for (let k = 0; k < otherColumns.length; k += 1) {
      let sum = 0;
      let valid = true;
      for (let j = 0; j < innerDimension; j += 1) {
        const a = left[i]![j]!;
        const b = right[j]![k]!;
        if (a === null || b === null) {
          valid = false;
          break;
        }
        sum += a * b;
      }
      result[i]![k] = valid ? sum : null;
    }
  }
  return result;
}
