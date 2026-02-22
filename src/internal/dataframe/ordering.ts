import type { CellValue, Row } from "../../types";
import { compareCellValues, isMissing, range } from "../../utils";

export type RowComparer = (left: Row, right: Row) => number;

export function normalizeSortAscending(
  columnCount: number,
  ascending: boolean | boolean[]
): boolean[] {
  if (!Array.isArray(ascending)) {
    return Array.from({ length: columnCount }, () => ascending);
  }
  if (ascending.length !== columnCount) {
    throw new Error(
      `Length mismatch for ascending. Expected ${columnCount}, received ${ascending.length}.`
    );
  }
  return [...ascending];
}

export function normalizeSortLimit(limit: number | undefined, rowCount: number): number | undefined {
  if (limit === undefined) {
    return undefined;
  }
  if (!Number.isInteger(limit) || limit < 0) {
    throw new Error("limit must be a non-negative integer.");
  }
  return Math.min(limit, rowCount);
}

export function buildColumnComparer(rows: Row[], column: string, ascending: boolean): RowComparer {
  const direction = ascending ? 1 : -1;
  const sample = firstNonMissingValue(rows, column);

  if (typeof sample === "number") {
    return (left, right) => {
      const lv = left[column];
      const rv = right[column];
      if (isMissing(lv) && isMissing(rv)) {
        return 0;
      }
      if (isMissing(lv)) {
        return 1;
      }
      if (isMissing(rv)) {
        return -1;
      }
      return ((lv as number) - (rv as number)) * direction;
    };
  }

  if (typeof sample === "string") {
    return (left, right) => {
      const lv = left[column];
      const rv = right[column];
      if (isMissing(lv) && isMissing(rv)) {
        return 0;
      }
      if (isMissing(lv)) {
        return 1;
      }
      if (isMissing(rv)) {
        return -1;
      }
      const leftStr = String(lv);
      const rightStr = String(rv);
      if (leftStr === rightStr) {
        return 0;
      }
      return (leftStr < rightStr ? -1 : 1) * direction;
    };
  }

  if (typeof sample === "boolean") {
    return (left, right) => {
      const lv = left[column];
      const rv = right[column];
      if (isMissing(lv) && isMissing(rv)) {
        return 0;
      }
      if (isMissing(lv)) {
        return 1;
      }
      if (isMissing(rv)) {
        return -1;
      }
      const leftBool = lv ? 1 : 0;
      const rightBool = rv ? 1 : 0;
      return (leftBool - rightBool) * direction;
    };
  }

  if (sample instanceof Date) {
    return (left, right) => {
      const lv = left[column];
      const rv = right[column];
      if (isMissing(lv) && isMissing(rv)) {
        return 0;
      }
      if (isMissing(lv)) {
        return 1;
      }
      if (isMissing(rv)) {
        return -1;
      }
      const leftTime = lv instanceof Date ? lv.getTime() : Number.NaN;
      const rightTime = rv instanceof Date ? rv.getTime() : Number.NaN;
      if (leftTime === rightTime) {
        return 0;
      }
      if (!Number.isFinite(leftTime) || !Number.isFinite(rightTime)) {
        return compareCellValues(lv, rv) * direction;
      }
      return (leftTime - rightTime) * direction;
    };
  }

  return (left, right) => compareCellValues(left[column], right[column]) * direction;
}

export function fullSortPositions(rows: Row[], comparers: RowComparer[]): number[] {
  const positions = range(rows.length);
  positions.sort((leftPosition, rightPosition) =>
    compareRowsByComparers(rows[leftPosition]!, rows[rightPosition]!, comparers)
  );
  return positions;
}

export function selectTopKPositions(rows: Row[], comparers: RowComparer[], limit: number): number[] {
  const selected: number[] = [];

  for (let position = 0; position < rows.length; position += 1) {
    const candidateRow = rows[position]!;
    let lo = 0;
    let hi = selected.length;

    while (lo < hi) {
      const mid = (lo + hi) >> 1;
      const compared = compareRowsByComparers(candidateRow, rows[selected[mid]!]!, comparers);
      if (compared < 0) {
        hi = mid;
      } else {
        lo = mid + 1;
      }
    }

    if (selected.length < limit) {
      selected.splice(lo, 0, position);
      continue;
    }

    if (lo < limit) {
      selected.splice(lo, 0, position);
      selected.pop();
    }
  }

  return selected;
}

function compareRowsByComparers(left: Row, right: Row, comparers: RowComparer[]): number {
  for (let i = 0; i < comparers.length; i += 1) {
    const compared = comparers[i]!(left, right);
    if (compared !== 0) {
      return compared;
    }
  }
  return 0;
}

function firstNonMissingValue(rows: Row[], column: string): CellValue {
  for (const row of rows) {
    const value = row[column];
    if (!isMissing(value)) {
      return value;
    }
  }
  return undefined;
}
