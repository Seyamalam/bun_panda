import type { CellValue } from "../../types";

export function stripBom(text: string): string {
  if (text.charCodeAt(0) === 0xfeff) {
    return text.slice(1);
  }
  return text;
}

export function coerceJsonCell(value: unknown): CellValue {
  if (
    value === null ||
    value === undefined ||
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "boolean"
  ) {
    return value;
  }
  return JSON.stringify(value);
}

export function normalizeExternalCell(value: unknown): CellValue {
  if (
    value === null ||
    value === undefined ||
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "boolean" ||
    value instanceof Date
  ) {
    return value;
  }

  // bigint is a first-class CellValue (int64 storage); keep it exact.
  if (typeof value === "bigint") {
    return value;
  }

  return JSON.stringify(value);
}
