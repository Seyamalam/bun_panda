import type { CellValue, Row } from "../../types";
import { compareCellValues, isMissing, range } from "../../utils";

export type RowComparer = (leftPosition: number, rightPosition: number) => number;

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

export function buildColumnComparer(
  rows: Row[],
  column: string,
  ascending: boolean,
  naPosition: "first" | "last" = "last"
): RowComparer {
  const direction = ascending ? 1 : -1;
  const values = rows.map((row) => row[column]);
  const sample = firstNonMissingValue(values);

  if (typeof sample === "number") {
    return (leftPosition, rightPosition) =>
      compareKnownNumbers(
        values[leftPosition],
        values[rightPosition],
        direction,
        naPosition
      );
  }

  if (typeof sample === "string") {
    return (leftPosition, rightPosition) =>
      compareKnownStrings(
        values[leftPosition],
        values[rightPosition],
        direction,
        naPosition
      );
  }

  if (typeof sample === "boolean") {
    return (leftPosition, rightPosition) =>
      compareKnownBooleans(
        values[leftPosition],
        values[rightPosition],
        direction,
        naPosition
      );
  }

  if (sample instanceof Date) {
    return (leftPosition, rightPosition) =>
      compareKnownDates(
        values[leftPosition],
        values[rightPosition],
        direction,
        naPosition
      );
  }

  return (leftPosition, rightPosition) =>
    compareFallback(values[leftPosition], values[rightPosition], direction, naPosition);
}

export function fullSortPositions(rows: Row[], comparers: RowComparer[]): number[] {
  const positions = range(rows.length);
  positions.sort((leftPosition, rightPosition) =>
    comparePositions(leftPosition, rightPosition, comparers)
  );
  return positions;
}

export function selectTopKPositions(rows: Row[], comparers: RowComparer[], limit: number): number[] {
  if (limit === 0) {
    return [];
  }

  if (shouldUseFullSort(limit, rows.length)) {
    return fullSortPositions(rows, comparers).slice(0, limit);
  }

  const selected: number[] = [];
  for (let position = 0; position < rows.length; position += 1) {
    let lo = 0;
    let hi = selected.length;

    while (lo < hi) {
      const mid = (lo + hi) >> 1;
      const compared = comparePositions(position, selected[mid]!, comparers);
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

function shouldUseFullSort(limit: number, rowCount: number): boolean {
  if (limit >= rowCount) {
    return true;
  }
  return limit * 3 > rowCount;
}

function comparePositions(
  leftPosition: number,
  rightPosition: number,
  comparers: RowComparer[]
): number {
  for (let i = 0; i < comparers.length; i += 1) {
    const compared = comparers[i]!(leftPosition, rightPosition);
    if (compared !== 0) {
      return compared;
    }
  }
  return 0;
}

function compareKnownNumbers(
  left: CellValue,
  right: CellValue,
  direction: number,
  naPosition: "first" | "last"
): number {
  const missingCompared = compareMissing(left, right, naPosition);
  if (missingCompared !== null) {
    return missingCompared;
  }
  return ((left as number) - (right as number)) * direction;
}

function compareKnownStrings(
  left: CellValue,
  right: CellValue,
  direction: number,
  naPosition: "first" | "last"
): number {
  const missingCompared = compareMissing(left, right, naPosition);
  if (missingCompared !== null) {
    return missingCompared;
  }
  const leftStr = String(left);
  const rightStr = String(right);
  if (leftStr === rightStr) {
    return 0;
  }
  return (leftStr < rightStr ? -1 : 1) * direction;
}

function compareKnownBooleans(
  left: CellValue,
  right: CellValue,
  direction: number,
  naPosition: "first" | "last"
): number {
  const missingCompared = compareMissing(left, right, naPosition);
  if (missingCompared !== null) {
    return missingCompared;
  }
  return ((left ? 1 : 0) - (right ? 1 : 0)) * direction;
}

function compareKnownDates(
  left: CellValue,
  right: CellValue,
  direction: number,
  naPosition: "first" | "last"
): number {
  const missingCompared = compareMissing(left, right, naPosition);
  if (missingCompared !== null) {
    return missingCompared;
  }

  const leftTime = left instanceof Date ? left.getTime() : Number.NaN;
  const rightTime = right instanceof Date ? right.getTime() : Number.NaN;
  if (leftTime === rightTime) {
    return 0;
  }
  if (!Number.isFinite(leftTime) || !Number.isFinite(rightTime)) {
    return compareCellValues(left, right) * direction;
  }
  return (leftTime - rightTime) * direction;
}

function firstNonMissingValue(values: CellValue[]): CellValue {
  for (const value of values) {
    if (!isMissing(value)) {
      return value;
    }
  }
  return undefined;
}

function compareMissing(
  left: CellValue,
  right: CellValue,
  naPosition: "first" | "last"
): number | null {
  const leftMissing = isMissing(left);
  const rightMissing = isMissing(right);
  if (leftMissing && rightMissing) {
    return 0;
  }
  if (!leftMissing && !rightMissing) {
    return null;
  }
  if (naPosition === "first") {
    return leftMissing ? -1 : 1;
  }
  return leftMissing ? 1 : -1;
}

function compareFallback(
  left: CellValue,
  right: CellValue,
  direction: number,
  naPosition: "first" | "last"
): number {
  const missingCompared = compareMissing(left, right, naPosition);
  if (missingCompared !== null) {
    return missingCompared;
  }
  return compareCellValues(left, right) * direction;
}
