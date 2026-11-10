// Cumulative scans for Series — pure functions over raw value arrays.
import type { CellValue } from "../../types";
import { isMissing } from "../../utils";
import { bigintOperand } from "../shared/valueSemantics";

function toNumber(value: CellValue): number {
  return typeof value === "number" ? value : Number(value);
}

export function cumsumValues(values: CellValue[]): (number | bigint | null)[] {
  // The accumulator only promotes to bigint once a bigint cell appears;
  // number columns keep plain-number results.
  let accWide: bigint | null = null;
  let accNum = 0;
  return values.map((value): number | bigint | null => {
    if (isMissing(value)) return null;
    const wide = bigintOperand(value);
    if (accWide !== null) {
      if (wide !== null) {
        accWide += wide;
        return accWide;
      }
      // non-integer input demotes the running total to float64
      accNum = Number(accWide);
      accWide = null;
      accNum += toNumber(value);
      return accNum;
    }
    if (typeof value === "bigint") {
      accWide = value;
      return accWide;
    }
    accNum += toNumber(value);
    return accNum;
  });
}

export function cummaxValues(values: CellValue[]): (number | bigint | null)[] {
  let acc: number | bigint | null = null;
  return values.map((value): number | bigint | null => {
    if (isMissing(value)) return null;
    const n: number | bigint = typeof value === "bigint" ? value : toNumber(value);
    // mixed number/bigint comparison stays exact under relational operators.
    acc = acc === null || n > acc ? n : acc;
    return acc;
  });
}

export function cumminValues(values: CellValue[]): (number | bigint | null)[] {
  let acc: number | bigint | null = null;
  return values.map((value): number | bigint | null => {
    if (isMissing(value)) return null;
    const n: number | bigint = typeof value === "bigint" ? value : toNumber(value);
    acc = acc === null || n < acc ? n : acc;
    return acc;
  });
}

export function cumprodValues(values: CellValue[]): (number | bigint | null)[] {
  let accWide: bigint | null = null;
  let accNum = 1;
  return values.map((value): number | bigint | null => {
    if (isMissing(value)) return null;
    const wide = bigintOperand(value);
    if (accWide !== null) {
      if (wide !== null) {
        accWide *= wide;
        return accWide;
      }
      accNum = Number(accWide);
      accWide = null;
      accNum *= toNumber(value);
      return accNum;
    }
    if (typeof value === "bigint") {
      accWide = value;
      return accWide;
    }
    accNum *= toNumber(value);
    return accNum;
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
