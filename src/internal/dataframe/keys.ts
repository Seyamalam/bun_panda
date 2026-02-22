import type { CellValue, Row } from "../../types";

export type NormalizedKey = string | number | boolean | null;

export function normalizeKeyCell(value: CellValue): NormalizedKey {
  if (value instanceof Date) {
    return value.toISOString();
  }
  if (value === undefined) {
    return null;
  }
  if (
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "boolean" ||
    value === null
  ) {
    return value;
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
