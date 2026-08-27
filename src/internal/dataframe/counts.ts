import type { CellValue } from "../../types";

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
  // Array.sort is stable: equal counts retain first-observed order, matching
  // pandas value_counts rather than imposing a lexical secondary key.
  return 0;
}

function compareCountEntriesAscending(left: CountEntry, right: CountEntry): number {
  if (left.count !== right.count) {
    return left.count - right.count;
  }
  return 0;
}
