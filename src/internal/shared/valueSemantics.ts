import { isMissingScalar, NA, NaT } from "../../missing";
import type { CellValue } from "../../types";

export function isAbsent(value: CellValue): boolean {
  return isMissingScalar(value);
}

/** pandas object equality joins boolean and numeric keys, but preserves strings. */
export function scalarKey(value: CellValue): string {
  if (isAbsent(value)) return "missing";
  // bigint joins the number family with its exact decimal form so 5n keys
  // with 5 while huge values never collide with a rounded double.
  if (typeof value === "boolean" || typeof value === "number") return `number:${Number(value)}`;
  if (typeof value === "bigint") return `number:${value.toString()}`;
  if (value instanceof Date) return `date:${value.getTime()}`;
  return `${typeof value}:${String(value)}`;
}

/** Exact integer operand for bigint kernels; null when not integer-representable. */
export function bigintOperand(cell: CellValue | null | undefined): bigint | null {
  return typeof cell === "bigint" ? cell
    : (typeof cell === "number" && Number.isInteger(cell)) || typeof cell === "boolean" ? BigInt(cell)
    : null;
}

/** pandas/numpy remainder: the result takes the divisor's sign. */
export function bigintMod(left: bigint, right: bigint): bigint {
  const r = left % right;
  return r !== 0n && (r < 0n) !== (right < 0n) ? r + right : r;
}

/** Floor division (toward -Infinity), not BigInt's truncation toward zero. */
export function bigintFloorDiv(left: bigint, right: bigint): bigint {
  const q = left / right;
  return left % right !== 0n && (left < 0n) !== (right < 0n) ? q - 1n : q;
}

/**
 * JSON.stringify replacer for cells that may hold bigint. Safe-range values
 * serialize as plain JSON numbers (pandas parity); out-of-range values
 * serialize as their exact decimal string since JSON has no 64-bit integer.
 */
export function jsonBigintReplacer(_key: string, value: unknown): unknown {
  if (typeof value !== "bigint") return value;
  return value >= BigInt(Number.MIN_SAFE_INTEGER) && value <= BigInt(Number.MAX_SAFE_INTEGER)
    ? Number(value)
    : value.toString();
}

/** Strict equality extended with exact cross-type number/bigint comparison. */
export function numberOrBigintEqual(left: CellValue, right: CellValue): boolean {
  if (left === right) return true;
  const isNumeric = (v: CellValue) => typeof v === "number" || typeof v === "bigint" || typeof v === "boolean";
  if (!isNumeric(left) || !isNumeric(right)) return false;
  const l = typeof left === "bigint" ? left
    : typeof left === "boolean" ? (left ? 1n : 0n)
    : Number.isInteger(left) ? BigInt(left) : null;
  const r = typeof right === "bigint" ? right
    : typeof right === "boolean" ? (right ? 1n : 0n)
    : Number.isInteger(right) ? BigInt(right) : null;
  return l !== null && r !== null && l === r;
}

/** Object equality distinguishes extension missing sentinels from None/NaN. */
export function scalarEquals(left: CellValue, right: CellValue): boolean {
  if (left === NA || right === NA) return left === right;
  const isNaT = (value: CellValue) => value === NaT || (value instanceof Date && Number.isNaN(value.getTime()));
  if (isNaT(left) || isNaT(right)) return isNaT(left) && isNaT(right);
  if (isAbsent(left) || isAbsent(right)) return isAbsent(left) && isAbsent(right);
  if (left instanceof Date && right instanceof Date) return left.getTime() === right.getTime();
  if ((typeof left === "number" || typeof left === "boolean") && (typeof right === "number" || typeof right === "boolean")) return Number(left) === Number(right);
  if (typeof left === "bigint" || typeof right === "bigint") return numberOrBigintEqual(left, right);
  return left === right;
}

/** Category inference sorts numeric values before strings without stringifying them. */
export function sortedCategories(values: CellValue[]): CellValue[] {
  const unique = new Map<string, CellValue>();
  for (const value of values) if (!isAbsent(value) && !unique.has(scalarKey(value))) unique.set(scalarKey(value), value);
  return [...unique.values()].sort((a,b) => {
    const an = typeof a === "boolean" || typeof a === "number" || typeof a === "bigint";
    const bn = typeof b === "boolean" || typeof b === "number" || typeof b === "bigint";
    if (an && bn) {
      if (typeof a === "bigint" || typeof b === "bigint") {
        const lhs = typeof a === "boolean" ? (a ? 1n : 0n) : a as number | bigint;
        const rhs = typeof b === "boolean" ? (b ? 1n : 0n) : b as number | bigint;
        return lhs < rhs ? -1 : lhs > rhs ? 1 : 0;
      }
      return Number(a) - Number(b);
    }
    if (an !== bn) return an ? -1 : 1;
    if (a instanceof Date && b instanceof Date) return a.getTime() - b.getTime();
    return String(a) < String(b) ? -1 : String(a) > String(b) ? 1 : 0;
  });
}

/** IEEE round-to-nearest-even, matching pandas/NumPy decimal rounding. */
export function roundToEven(value: number, decimals: number): number {
  if (!Number.isInteger(decimals)) throw new TypeError("decimals must be an integer");
  const factor = 10 ** Math.abs(decimals);
  const scaled = decimals < 0 ? value / factor : value * factor;
  const floor = Math.floor(scaled);
  const delta = scaled - floor;
  const rounded = delta === 0.5 ? (floor % 2 === 0 ? floor : floor + 1) : Math.round(scaled);
  const signed = rounded === 0 && (value < 0 || Object.is(value,-0)) ? -0 : rounded;
  return decimals < 0 ? signed * factor : signed / factor;
}
