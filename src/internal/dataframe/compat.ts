import type { CellValue, Row } from "../../types";
import { normalizeKeyCell } from "./keys";
import { compareCellValues, isMissing } from "../../utils";

export type ReplaceInput = CellValue | CellValue[] | Record<string, CellValue>;

export interface RankOptions {
  method?: "average" | "min" | "max" | "dense" | "first";
  ascending?: boolean;
  na_option?: "keep" | "top" | "bottom";
  pct?: boolean;
}

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
  const method = options.method ?? "average";
  const ascending = options.ascending ?? true;
  const naOption = options.na_option ?? "keep";
  const pct = options.pct ?? false;
  const rowCount = sourceRows.length;
  const outRows = Array.from({ length: rowCount }, () => ({} as Row));

  for (const column of columns) {
    const values = sourceRows.map((row) => row[column]);
    const missingPositions: number[] = [];
    const nonMissingPositions: number[] = [];

    for (let i = 0; i < rowCount; i += 1) {
      if (isMissing(values[i])) {
        missingPositions.push(i);
      } else {
        nonMissingPositions.push(i);
      }
    }

    nonMissingPositions.sort((left, right) => {
      const compared = compareCellValues(values[left], values[right]);
      return ascending ? compared : -compared;
    });

    const ranks = new Array<number | null>(rowCount).fill(null);
    assignNonMissingRanks(ranks, values, nonMissingPositions, method);
    applyMissingRanks(ranks, missingPositions, nonMissingPositions.length, naOption);

    const denominator = pct ? calculatePctDenominator(nonMissingPositions.length, naOption) : 1;

    for (let i = 0; i < rowCount; i += 1) {
      const rank = ranks[i];
      outRows[i]![column] =
        rank == null
          ? null
          : (pct ? rank / denominator : rank);
    }
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

function assignNonMissingRanks(
  ranks: Array<number | null>,
  values: CellValue[],
  sortedPositions: number[],
  method: NonNullable<RankOptions["method"]>
): void {
  if (method === "first") {
    for (let i = 0; i < sortedPositions.length; i += 1) {
      ranks[sortedPositions[i]!] = i + 1;
    }
    return;
  }

  let denseRank = 1;
  let cursor = 0;
  while (cursor < sortedPositions.length) {
    const start = cursor;
    let end = cursor + 1;
    while (
      end < sortedPositions.length &&
      compareCellValues(
        values[sortedPositions[start]!],
        values[sortedPositions[end]!]
      ) === 0
    ) {
      end += 1;
    }

    const minRank = start + 1;
    const maxRank = end;
    const rankValue =
      method === "min"
        ? minRank
        : method === "max"
          ? maxRank
          : method === "dense"
            ? denseRank
            : (minRank + maxRank) / 2;

    for (let i = start; i < end; i += 1) {
      ranks[sortedPositions[i]!] = rankValue;
    }

    if (method === "dense") {
      denseRank += 1;
    }
    cursor = end;
  }
}

function applyMissingRanks(
  ranks: Array<number | null>,
  missingPositions: number[],
  nonMissingCount: number,
  naOption: NonNullable<RankOptions["na_option"]>
): void {
  if (missingPositions.length === 0 || naOption === "keep") {
    return;
  }

  if (naOption === "top") {
    for (let i = 0; i < ranks.length; i += 1) {
      if (ranks[i] !== null) {
        ranks[i] = (ranks[i] as number) + missingPositions.length;
      }
    }
    for (const position of missingPositions) {
      ranks[position] = 1;
    }
    return;
  }

  const rank = nonMissingCount + 1;
  for (const position of missingPositions) {
    ranks[position] = rank;
  }
}

function calculatePctDenominator(
  nonMissingCount: number,
  naOption: NonNullable<RankOptions["na_option"]>
): number {
  if (naOption === "keep") {
    return Math.max(nonMissingCount, 1);
  }
  return Math.max(nonMissingCount + 1, 1);
}
