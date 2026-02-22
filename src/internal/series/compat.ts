import { normalizeKeyCell } from "../dataframe/keys";
import type { CellValue } from "../../types";

export type SeriesReplaceInput = CellValue | CellValue[] | Record<string, CellValue>;

export function computeSeriesIsin(
  sourceValues: CellValue[],
  values: CellValue[]
): boolean[] {
  const lookup = buildLookup(values);
  return sourceValues.map((value) => lookup.has(normalizeLookupKey(value)));
}

export function computeSeriesClip(
  sourceValues: CellValue[],
  lower: number | undefined,
  upper: number | undefined
): CellValue[] {
  return sourceValues.map((value) => {
    if (typeof value !== "number") {
      return value;
    }
    let next = value;
    if (lower !== undefined && next < lower) {
      next = lower;
    }
    if (upper !== undefined && next > upper) {
      next = upper;
    }
    return next;
  });
}

export function computeSeriesReplace(
  sourceValues: CellValue[],
  toReplace: SeriesReplaceInput,
  value?: CellValue
): CellValue[] {
  const replacer = buildReplacer(toReplace, value);
  return sourceValues.map((entry) => replacer(entry));
}

function buildLookup(values: CellValue[]): Set<string> {
  const lookup = new Set<string>();
  for (const value of values) {
    lookup.add(normalizeLookupKey(value));
  }
  return lookup;
}

function normalizeLookupKey(value: CellValue): string {
  return JSON.stringify(normalizeKeyCell(value));
}

function buildReplacer(
  toReplace: SeriesReplaceInput,
  value?: CellValue
): (input: CellValue) => CellValue {
  if (Array.isArray(toReplace)) {
    if (value === undefined) {
      throw new Error("replace with an array requires a replacement value.");
    }
    const lookup = buildLookup(toReplace);
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
      return mapping.has(key) ? mapping.get(key)! : input;
    };
  }

  if (value === undefined) {
    throw new Error("replace requires a replacement value for scalar input.");
  }

  const fromKey = normalizeLookupKey(toReplace as CellValue);
  return (input) =>
    normalizeLookupKey(input) === fromKey ? value : input;
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
