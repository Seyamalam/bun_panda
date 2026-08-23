// Cumulative scans for Series — pure functions over raw value arrays.
import type { CellValue } from "../../types";
import { isMissing } from "../../utils";

function toNumber(value: CellValue): number {
  return typeof value === "number" ? value : Number(value);
}

export function cumsumValues(values: CellValue[]): (number | null)[] {
  let acc = 0;
  return values.map((value): number | null => {
    if (isMissing(value)) return null;
    acc += toNumber(value);
    return acc;
  });
}

export function cummaxValues(values: CellValue[]): (number | null)[] {
  let acc: number | null = null;
  return values.map((value): number | null => {
    if (isMissing(value)) return null;
    const n = toNumber(value);
    acc = acc === null ? n : Math.max(acc, n);
    return acc;
  });
}

export function cumminValues(values: CellValue[]): (number | null)[] {
  let acc: number | null = null;
  return values.map((value): number | null => {
    if (isMissing(value)) return null;
    const n = toNumber(value);
    acc = acc === null ? n : Math.min(acc, n);
    return acc;
  });
}

export function cumprodValues(values: CellValue[]): (number | null)[] {
  let acc = 1;
  return values.map((value): number | null => {
    if (isMissing(value)) return null;
    acc *= toNumber(value);
    return acc;
  });
}

/** 0-based position within the run of non-null values (pandas cumcount). */
export function cumcountValues(values: CellValue[]): (number | null)[] {
  let count = 0;
  return values.map((value): number | null => {
    if (isMissing(value)) return null;
    return count++;
  });
}
