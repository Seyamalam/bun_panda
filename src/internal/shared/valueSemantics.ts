import { isMissingScalar, NA, NaT } from "../../missing";
import type { CellValue } from "../../types";

export function isAbsent(value: CellValue): boolean {
  return isMissingScalar(value);
}

/** pandas object equality joins boolean and numeric keys, but preserves strings. */
export function scalarKey(value: CellValue): string {
  if (isAbsent(value)) return "missing";
  if (typeof value === "boolean" || typeof value === "number") return `number:${Number(value)}`;
  if (value instanceof Date) return `date:${value.getTime()}`;
  return `${typeof value}:${String(value)}`;
}

/** Object equality distinguishes extension missing sentinels from None/NaN. */
export function scalarEquals(left: CellValue, right: CellValue): boolean {
  if (left === NA || right === NA) return left === right;
  const isNaT = (value: CellValue) => value === NaT || (value instanceof Date && Number.isNaN(value.getTime()));
  if (isNaT(left) || isNaT(right)) return isNaT(left) && isNaT(right);
  if (isAbsent(left) || isAbsent(right)) return isAbsent(left) && isAbsent(right);
  if (left instanceof Date && right instanceof Date) return left.getTime() === right.getTime();
  if ((typeof left === "number" || typeof left === "boolean") && (typeof right === "number" || typeof right === "boolean")) return Number(left) === Number(right);
  return left === right;
}

/** Category inference sorts numeric values before strings without stringifying them. */
export function sortedCategories(values: CellValue[]): CellValue[] {
  const unique = new Map<string, CellValue>();
  for (const value of values) if (!isAbsent(value) && !unique.has(scalarKey(value))) unique.set(scalarKey(value), value);
  return [...unique.values()].sort((a,b) => {
    const an = typeof a === "boolean" || typeof a === "number";
    const bn = typeof b === "boolean" || typeof b === "number";
    if (an && bn) return Number(a) - Number(b);
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
