import type { CellValue } from "../../types";
import { compareCellValues, isMissing } from "../../utils";

export interface CountEntry {
  values: CellValue[];
  count: number;
}

export interface CountOrderingOptions {
  sort?: boolean;
  ascending?: boolean;
  limit?: number;
}

export function orderCountEntries(entries: CountEntry[], options: CountOrderingOptions): CountEntry[] {
  const sort = options.sort ?? true;
  const ascending = options.ascending ?? false;
  const limit = options.limit;

  if (!sort) {
    if (limit === undefined) {
      return [...entries];
    }
    return entries.slice(0, limit);
  }

  return selectTopKCountEntries(entries, limit, ascending);
}

function selectTopKCountEntries(entries: CountEntry[], limit: number | undefined, ascending: boolean): CountEntry[] {
  if (limit === 0) {
    return [];
  }

  const comparator = ascending ? compareCountEntriesAscending : compareCountEntriesDescending;

  if (limit === undefined) {
    return [...entries].sort(comparator);
  }

  if (entries.length <= limit * 4) {
    return [...entries].sort(comparator).slice(0, limit);
  }

  const selected: CountEntry[] = [];
  for (const entry of entries) {
    let lo = 0;
    let hi = selected.length;
    while (lo < hi) {
      const mid = (lo + hi) >> 1;
      const compared = comparator(entry, selected[mid]!);
      if (compared < 0) {
        hi = mid;
      } else {
        lo = mid + 1;
      }
    }

    if (selected.length < limit) {
      selected.splice(lo, 0, entry);
      continue;
    }
    if (lo < limit) {
      selected.splice(lo, 0, entry);
      selected.pop();
    }
  }
  return selected;
}

function compareCountEntriesDescending(left: CountEntry, right: CountEntry): number {
  if (left.count !== right.count) {
    return right.count - left.count;
  }
  return compareCountTieValues(left.values, right.values);
}

function compareCountEntriesAscending(left: CountEntry, right: CountEntry): number {
  if (left.count !== right.count) {
    return left.count - right.count;
  }
  return compareCountTieValues(left.values, right.values);
}

function compareCountTieValues(leftValues: CellValue[], rightValues: CellValue[]): number {
  for (let i = 0; i < leftValues.length; i += 1) {
    const compared = compareCountTieValue(leftValues[i], rightValues[i]);
    if (compared !== 0) {
      return compared;
    }
  }
  return 0;
}

function compareCountTieValue(left: CellValue, right: CellValue): number {
  if (left === right) {
    return 0;
  }
  if (isMissing(left)) {
    return 1;
  }
  if (isMissing(right)) {
    return -1;
  }
  if (typeof left === "number" && typeof right === "number") {
    return left - right;
  }
  if (typeof left === "string" && typeof right === "string") {
    return left < right ? -1 : 1;
  }
  if (typeof left === "boolean" && typeof right === "boolean") {
    return (left ? 1 : 0) - (right ? 1 : 0);
  }
  if (left instanceof Date && right instanceof Date) {
    return left.getTime() - right.getTime();
  }
  return compareCellValues(left, right);
}
