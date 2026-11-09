import { computeRank, type RankOptions } from "../series/rank";
import type { CellValue, Row } from "../../types";
import { normalizeKeyCell } from "./keys";

export type ReplaceInput = CellValue | CellValue[] | Record<string, CellValue>;

export type { RankOptions };

export function computeIsinRows(
  sourceRows: Row[],
  columns: string[],
  values: CellValue[] | Record<string, CellValue[]>
): Row[] {
  if (Array.isArray(values)) {
    const lookup = buildValueLookup(values);
    return sourceRows.map((row) => {
      const out: Row = {};
      for (const column of columns) {
        out[column] = lookup.has(normalizeLookupKey(row[column]));
      }
      return out;
    });
  }

  const lookupByColumn = new Map<string, Set<string>>();
  for (const [column, items] of Object.entries(values)) {
    lookupByColumn.set(column, buildValueLookup(items));
  }

  return sourceRows.map((row) => {
    const out: Row = {};
    for (const column of columns) {
      const lookup = lookupByColumn.get(column);
      out[column] = lookup ? lookup.has(normalizeLookupKey(row[column])) : false;
    }
    return out;
  });
}

export function computeClipRows(
  sourceRows: Row[],
  columns: string[],
  lower: number | undefined,
  upper: number | undefined,
  targets: Set<string>
): Row[] {
  return sourceRows.map((row) => {
    const out: Row = {};
    for (const column of columns) {
      const value = row[column];
      if (!targets.has(column) || typeof value !== "number") {
        out[column] = value;
        continue;
      }

      let next = value;
      if (lower !== undefined && next < lower) {
        next = lower;
      }
      if (upper !== undefined && next > upper) {
        next = upper;
      }
      out[column] = next;
    }
    return out;
  });
}

export function computeReplaceRows(
  sourceRows: Row[],
  columns: string[],
  toReplace: ReplaceInput,
  value?: CellValue
): Row[] {
  const replacer = buildReplacer(toReplace, value);
  return sourceRows.map((row) => {
    const out: Row = {};
    for (const column of columns) {
      out[column] = replacer(row[column]);
    }
    return out;
  });
}

export function samplePositions(
  rowCount: number,
  n: number,
  replace: boolean,
  randomState?: number
): number[] {
  if (n <= 0 || rowCount === 0) {
    return [];
  }

  const rnd = randomState === undefined
    ? Math.random
    : createSeededRandom(randomState);

  if (replace) {
    const out = new Array<number>(n);
    for (let i = 0; i < n; i += 1) {
      out[i] = Math.floor(rnd() * rowCount);
    }
    return out;
  }

  const size = Math.min(n, rowCount);
  const pool = Array.from({ length: rowCount }, (_, i) => i);
  for (let i = 0; i < size; i += 1) {
    const swapAt = i + Math.floor(rnd() * (rowCount - i));
    const next = pool[i]!;
    pool[i] = pool[swapAt]!;
    pool[swapAt] = next;
  }
  return pool.slice(0, size);
}

export function computeRankRows(
  sourceRows: Row[],
  columns: string[],
  options: RankOptions = {}
): Row[] {
  const outRows = sourceRows.map(() => ({} as Row));
  for (const column of columns) {
    const ranks = computeRank(sourceRows.map(row => row[column]), options);
    ranks.forEach((rank, i) => { outRows[i]![column] = rank; });
  }
  return outRows;
}

function buildValueLookup(values: CellValue[]): Set<string> {
  const out = new Set<string>();
  for (const value of values) {
    out.add(normalizeLookupKey(value));
  }
  return out;
}

function normalizeLookupKey(value: CellValue): string {
  return JSON.stringify(normalizeKeyCell(value));
}

function buildReplacer(
  toReplace: ReplaceInput,
  value?: CellValue
): (input: CellValue) => CellValue {
  if (Array.isArray(toReplace)) {
    if (value === undefined) {
      throw new Error("replace with an array requires a replacement value.");
    }
    const lookup = buildValueLookup(toReplace);
    return (input) =>
      lookup.has(normalizeLookupKey(input)) ? value : input;
  }

  if (isPlainObject(toReplace) && value === undefined) {
    const mapping = new Map<string, CellValue>();
    for (const [from, to] of Object.entries(toReplace)) {
      mapping.set(JSON.stringify(from), to);
    }
    return (input) => {
      const key = stringifyForReplace(input);
      return mapping.has(key) ? mapping.get(key) : input;
    };
  }

  if (value === undefined) {
    throw new Error("replace requires a replacement value for scalar input.");
  }

  const fromKey = normalizeLookupKey(toReplace as CellValue);
  return (input) => (normalizeLookupKey(input) === fromKey ? value : input);
}

function stringifyForReplace(value: CellValue): string {
  if (value instanceof Date) {
    return JSON.stringify(value.toISOString());
  }
  if (value === null || value === undefined) {
    return JSON.stringify(null);
  }
  return JSON.stringify(value);
}

function isPlainObject(input: unknown): input is Record<string, CellValue> {
  return typeof input === "object" && input !== null && !Array.isArray(input);
}

function createSeededRandom(seed: number): () => number {
  let state = seed >>> 0;
  return () => {
    state = (1664525 * state + 1013904223) >>> 0;
    return state / 2 ** 32;
  };
}
