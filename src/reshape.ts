import { isAbsent, scalarKey, sortedCategories } from "./internal/shared/valueSemantics";
import { DataFrame } from "./dataframe";
import type { CellValue, Row } from "./types";

/**
 * pandas-style `crosstab`: cross-tabulation of two (or more) columns.
 * Rows = unique values of `index`, columns = unique values of `columns`.
 */
export function crosstab(
  frame: DataFrame,
  index: string,
  columns: string
): DataFrame {
  const rows = frame.to_records();
  const rowIndexOrder: CellValue[] = [];
  const rowSeen = new Set<string>();
  const colKeys: string[] = [];
  const colSeen = new Set<string>();
  const counts = new Map<string, number>();

  for (const row of rows) {
    const i = row[index];
    const c = row[columns];
    if (i === null || i === undefined || c === null || c === undefined) {
      continue;
    }
    const iKey = String(i);
    const cKey = String(c);
    if (!rowSeen.has(iKey)) {
      rowSeen.add(iKey);
      rowIndexOrder.push(i);
    }
    if (!colSeen.has(cKey)) {
      colSeen.add(cKey);
      colKeys.push(cKey);
    }
    const key = `${iKey}\u0000${cKey}`;
    counts.set(key, (counts.get(key) ?? 0) + 1);
  }

  const outRows: Row[] = rowIndexOrder.map((idx) => {
    const row: Row = {};
    row[index] = idx;
    for (const cKey of colKeys) {
      row[cKey] = counts.get(`${String(idx)}\u0000${cKey}`) ?? 0;
    }
    return row;
  });

  return new DataFrame(outRows, { columns: [index, ...colKeys] });
}

/**
 * pandas-style `cut`: bins values into equal-width intervals. Returns
 * the bin index per value (-1 for values outside all bins / missing).
 */
export function cut(
  values: number[],
  bins: number,
  options: { lower?: number; upper?: number } = {}
): { labels: number[]; edges: number[] } {
  const finite = values.filter((v) => Number.isFinite(v));
  if (finite.length === 0 && (options.lower === undefined || options.upper === undefined)) {
    throw new Error("cut requires numeric values or explicit lower/upper.");
  }
  const lower = options.lower ?? Math.min(...finite);
  const upper = options.upper ?? Math.max(...finite);
  const width = (upper - lower) / bins;
  const edges: number[] = Array.from({ length: bins + 1 }, (_, i) => lower + i * width);

  const labels = values.map((value) => {
    if (!Number.isFinite(value) || value < lower || value > upper) {
      return -1;
    }
    let binIndex = Math.floor((value - lower) / width);
    if (binIndex >= bins) {
      binIndex = bins - 1;
    }
    return binIndex;
  });

  return { labels, edges };
}

/**
 * pandas-style `qcut`: quantile-based binning. Values are ranked and
 * assigned to `bins` roughly-equal groups.
 */
export function qcut(values: number[], bins: number): number[] {
  const indexed = values
    .map((value, position) => ({ value, position }))
    .filter((entry) => Number.isFinite(entry.value))
    .sort((a, b) => a.value - b.value);

  const out: number[] = new Array(values.length).fill(-1);
  let group = -1;
  let lastValue: number | null = null;
  for (let rank = 0; rank < indexed.length; rank += 1) {
    const entry = indexed[rank]!;
    const expectedGroup = Math.floor((rank / indexed.length) * bins);
    if (lastValue === null || entry.value !== lastValue) {
      group = Math.min(expectedGroup, bins - 1);
      lastValue = entry.value;
    }
    out[entry.position] = group;
  }
  return out;
}

/**
 * pandas-style `get_dummies`: one-hot encodes the given column(s),
 * producing boolean columns named `<column>_<value>`.
 */
export function get_dummies(
  frame: DataFrame,
  columns: string | string[],
  options: { drop_first?: boolean; prefix_sep?: string; dummy_na?: boolean; dtype?: "bool" | "number" } = {}
): DataFrame {
  const cols = Array.isArray(columns) ? columns : [columns];
  for (const column of cols) if (!frame.columns.includes(column)) throw new Error(`Column '${column}' does not exist`);
  const sep = options.prefix_sep ?? "_";
  const records = frame.to_records();
  const retained = frame.columns.filter(column => !cols.includes(column));
  const encoded = cols.flatMap(column => {
    let categories = sortedCategories(records.map(record => record[column]));
    if (options.dummy_na) categories.push(null);
    if (options.drop_first) categories = categories.slice(1);
    return categories.map<{column: string; value: CellValue; name: string}>(value => ({ column, value, name: `${column}${sep}${value === null ? "nan" : typeof value === "boolean" ? (value ? "True" : "False") : String(value)}` }));
  });
  const outRows = records.map(record => {
    const row: Row = {};
    for (const column of retained) row[column] = record[column];
    for (const {column,value,name} of encoded) {
      const match = scalarKey(record[column]) === scalarKey(value);
      row[name] = options.dtype === "number" ? Number(match) : match;
    }
    return row;
  });
  return new DataFrame(outRows, { columns: [...retained,...encoded.map(entry=>entry.name)], index: frame.index });
}

/**
 * pandas-style `factorize`: maps distinct values to integer codes in
 * first-seen order. NaN/null values get code -1.
 */
export function factorize<T extends CellValue>(
  values: T[]
): { codes: number[]; uniques: T[] } {
  const uniques: T[] = [];
  const seen = new Map<string, number>();
  const codes = values.map((value) => {
    if (isAbsent(value)) {
      return -1;
    }
    const key =
      scalarKey(value);
    const existing = seen.get(key);
    if (existing !== undefined) {
      return existing;
    }
    const code = uniques.length;
    seen.set(key, code);
    uniques.push(value);
    return code;
  });
  return { codes, uniques };
}
