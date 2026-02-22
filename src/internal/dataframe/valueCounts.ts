import { orderCountEntries } from "./counts";
import type { CountEntry } from "./counts";
import {
  keyForPair,
  keyForValues,
  normalizeCountKey,
  type NormalizedKey,
} from "./keys";
import type { CellValue, Row } from "../../types";
import { isMissing } from "../../utils";

export interface ComputeValueCountsOptions {
  subset: string[];
  normalize: boolean;
  dropna: boolean;
  sort: boolean;
  ascending: boolean;
  limit?: number;
}

export interface ValueCountsResult {
  rows: Row[];
  valueColumnName: "count" | "proportion";
}

export function computeValueCountsRows(
  sourceRows: Row[],
  options: ComputeValueCountsOptions
): ValueCountsResult {
  const { subset, normalize, dropna, sort, ascending, limit } = options;
  const entries: CountEntry[] = [];
  let consideredRows = 0;

  if (subset.length === 1) {
    const column = subset[0]!;
    const counts = new Map<string | number | boolean | null, CountEntry>();
    for (const row of sourceRows) {
      const value = row[column];
      if (dropna && isMissing(value)) {
        continue;
      }
      consideredRows += 1;
      const key = normalizeCountKey(value);
      const entry = counts.get(key);
      if (!entry) {
        counts.set(key, { values: [value], count: 1 });
      } else {
        entry.count += 1;
      }
    }
    entries.push(...counts.values());
  } else if (subset.length === 2) {
    const firstColumn = subset[0]!;
    const secondColumn = subset[1]!;
    const sampleCount = Math.min(sourceRows.length, 512);
    const sampleUniqueFirstKeys = new Set<NormalizedKey>();
    let sampledRows = 0;

    for (let i = 0; i < sampleCount; i += 1) {
      const sampleRow = sourceRows[i]!;
      const firstValue = sampleRow[firstColumn];
      const secondValue = sampleRow[secondColumn];
      if (dropna && (isMissing(firstValue) || isMissing(secondValue))) {
        continue;
      }
      sampledRows += 1;
      sampleUniqueFirstKeys.add(normalizeCountKey(firstValue));
    }

    const useFlatMap = sampledRows > 0 && sampleUniqueFirstKeys.size / sampledRows > 0.35;

    if (useFlatMap) {
      const counts = new Map<string, CountEntry>();
      for (const row of sourceRows) {
        const firstValue = row[firstColumn];
        const secondValue = row[secondColumn];
        if (dropna && (isMissing(firstValue) || isMissing(secondValue))) {
          continue;
        }

        consideredRows += 1;
        const key = keyForPair(firstValue, secondValue);
        const entry = counts.get(key);
        if (!entry) {
          counts.set(key, { values: [firstValue, secondValue], count: 1 });
        } else {
          entry.count += 1;
        }
      }
      entries.push(...counts.values());
    } else {
      const counts = new Map<NormalizedKey, Map<NormalizedKey, CountEntry>>();
      for (const row of sourceRows) {
        const firstValue = row[firstColumn];
        const secondValue = row[secondColumn];
        if (dropna && (isMissing(firstValue) || isMissing(secondValue))) {
          continue;
        }

        consideredRows += 1;
        const firstKey = normalizeCountKey(firstValue);
        const secondKey = normalizeCountKey(secondValue);
        let inner = counts.get(firstKey);
        if (!inner) {
          inner = new Map();
          counts.set(firstKey, inner);
        }
        const entry = inner.get(secondKey);
        if (!entry) {
          inner.set(secondKey, { values: [firstValue, secondValue], count: 1 });
        } else {
          entry.count += 1;
        }
      }
      for (const inner of counts.values()) {
        entries.push(...inner.values());
      }
    }
  } else {
    const counts = new Map<string, CountEntry>();
    for (const row of sourceRows) {
      const values: CellValue[] = [];
      let hasMissing = false;
      for (const column of subset) {
        const value = row[column];
        if (dropna && isMissing(value)) {
          hasMissing = true;
          break;
        }
        values.push(value);
      }
      if (hasMissing) {
        continue;
      }

      consideredRows += 1;
      const key = keyForValues(values);
      const entry = counts.get(key);
      if (!entry) {
        counts.set(key, { values, count: 1 });
      } else {
        entry.count += 1;
      }
    }
    entries.push(...counts.values());
  }

  const valueColumnName = normalize ? "proportion" : "count";
  const orderedCounts = orderCountEntries(entries, { sort, ascending, limit });
  const rows = orderedCounts.map((entry) => {
    const row: Row = {};
    for (let i = 0; i < subset.length; i += 1) {
      row[subset[i]!] = entry.values[i];
    }
    row[valueColumnName] = normalize && consideredRows > 0 ? entry.count / consideredRows : entry.count;
    return row;
  });

  return { rows, valueColumnName };
}
