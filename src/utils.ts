import type { CellValue, DType, InferredDType, Row } from "./types";

export function range(count: number): number[] {
  return Array.from({ length: count }, (_, index) => index);
}

export function cloneRow(row: Row, columns?: string[]): Row {
  if (!columns) {
    return { ...row };
  }
  const next: Row = {};
  for (const column of columns) {
    next[column] = row[column];
  }
  return next;
}

export function isMissing(value: CellValue): value is null | undefined {
  return value === null || value === undefined;
}

export function isNumber(value: CellValue): value is number {
  return typeof value === "number" && Number.isFinite(value);
}

export function numericValues(values: CellValue[]): number[] {
  return values.filter(isNumber);
}

export function compareCellValues(left: CellValue, right: CellValue): number {
  if (isMissing(left) && isMissing(right)) {
    return 0;
  }
  if (isMissing(left)) {
    return 1;
  }
  if (isMissing(right)) {
    return -1;
  }

  if (left instanceof Date && right instanceof Date) {
    return left.getTime() - right.getTime();
  }

  if (typeof left === "number" && typeof right === "number") {
    return left - right;
  }

  const leftString = String(left);
  const rightString = String(right);
  return leftString.localeCompare(rightString);
}

export function variance(values: number[]): number | null {
  const n = values.length;
  if (n <= 1) {
    return null;
  }
  const mean = values.reduce((sum, value) => sum + value, 0) / n;
  return values.reduce((sum, value) => sum + (value - mean) ** 2, 0) / (n - 1);
}

export function std(values: number[]): number | null {
  const sampleVariance = variance(values);
  return sampleVariance === null ? null : Math.sqrt(sampleVariance);
}

export function median(values: number[]): number | null {
  if (values.length === 0) {
    return null;
  }
  const sorted = [...values].sort((left, right) => left - right);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 0
    ? (sorted[mid - 1]! + sorted[mid]!) / 2
    : sorted[mid]!;
}

export function coerceValueToDType(value: CellValue, dtype: DType): CellValue {
  if (isMissing(value)) {
    return null;
  }

  if (dtype === "number") {
    if (typeof value === "number") {
      return value;
    }
    if (typeof value === "boolean") {
      return value ? 1 : 0;
    }
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }

  if (dtype === "boolean") {
    if (typeof value === "boolean") {
      return value;
    }
    if (typeof value === "number") {
      return value !== 0;
    }
    const normalized = String(value).trim().toLowerCase();
    if (["true", "1", "yes", "y"].includes(normalized)) {
      return true;
    }
    if (["false", "0", "no", "n", ""].includes(normalized)) {
      return false;
    }
    return Boolean(value);
  }

  if (dtype === "date") {
    if (value instanceof Date) {
      return value;
    }
    const parsed = new Date(String(value));
    return Number.isNaN(parsed.getTime()) ? null : parsed;
  }

  return String(value);
}

export function inferColumnDType(values: CellValue[]): InferredDType {
  const nonMissing = values.filter((value) => !isMissing(value));
  if (nonMissing.length === 0) {
    return "unknown";
  }

  const isAll = (predicate: (value: CellValue) => boolean): boolean =>
    nonMissing.every((value) => predicate(value));

  if (isAll((value) => typeof value === "number")) {
    return "number";
  }
  if (isAll((value) => typeof value === "string")) {
    return "string";
  }
  if (isAll((value) => typeof value === "boolean")) {
    return "boolean";
  }
  if (isAll((value) => value instanceof Date)) {
    return "date";
  }

  return "mixed";
}
