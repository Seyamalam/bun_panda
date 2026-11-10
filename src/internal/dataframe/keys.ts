import { isMissingScalar } from "../../missing";
import type { CellValue, Row } from "../../types";

export type NormalizedKey = string | number | boolean | null;

export function normalizeKeyCell(value: CellValue): NormalizedKey {
  if (isMissingScalar(value)) return null;
  if (value instanceof Date) {
    return value.toISOString();
  }
  if (value === undefined) {
    return null;
  }
  // pandas membership/key semantics treat True == 1 and False == 0.
  if (typeof value === "boolean") {
    return value ? 1 : 0;
  }
  if (
    typeof value === "string" ||
    typeof value === "number" ||
    value === null
  ) {
    return value;
  }
  // bigint keys join the number family when exactly representable; unsafe
  // values keep an exact decimal marker so nothing collides with a rounded
  // double or a like-named string.
  if (typeof value === "bigint") {
    const asNumber = Number(value);
    return Number.isSafeInteger(asNumber) && BigInt(asNumber) === value
      ? asNumber
      : `bigint:${value}`;
  }
  return String(value);
}

export function normalizeCountKey(value: CellValue): NormalizedKey {
  return normalizeKeyCell(value);
}

export function keyFragment(value: CellValue): string {
  const normalized = normalizeKeyCell(value);
  if (normalized === null) {
    return "n:;";
  }
  if (typeof normalized === "number") {
    return `d:${normalized};`;
  }
  if (typeof normalized === "boolean") {
    return `b:${normalized ? 1 : 0};`;
  }
  const text = String(normalized);
  return `s${text.length}:${text};`;
}

export function keyForValues(values: CellValue[]): string {
  let key = "";
  for (const value of values) {
    key += keyFragment(value);
  }
  return key;
}

export function keyForColumns(row: Row, keys: string[]): string {
  const values = keys.map((column) => row[column]);
  return keyForValues(values);
}

export function keyForPair(first: CellValue, second: CellValue): string {
  return keyFragment(first) + keyFragment(second);
}
