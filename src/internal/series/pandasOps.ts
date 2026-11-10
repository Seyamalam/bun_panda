// pandas scalar-reduction semantics shared by Series methods and the
// per-column DataFrame reductions. Mirrors pandas 3.0.5 behaviors:
//   - string dtype raises TypeError for numeric ops; sum concatenates.
//   - object dtype works only when every non-missing value shares one kind
//     (numeric or string); mixed kinds raise TypeError.
//   - nullable (masked) dtypes report pd.NA for empty/all-missing results
//     while numpy dtypes report NaN.
//   - all/any on strings raise TypeError; missing values under
//     skipna=false follow truthiness (NaN truthy, None falsy) for
//     unmasked dtypes, and yield NA for masked dtypes.
import { ValueError } from "../../errors";
import type { CellValue, DType, IndexLabel } from "../../types";
import { compareCellValues, isMissing, pythonOperandTypeName } from "../../utils";
import { NA } from "../../missing";

/** pandas masked dtypes: nullable Int/UInt/Float/boolean/string carry a real pd.NA. */
export function isMaskedDType(dtype: string | undefined): boolean {
  return dtype !== undefined && (/^(U?Int|Float)/.test(dtype) || dtype === "boolean" || dtype === "string");
}

/** Missing scalar for reduction results: NA for masked dtypes, NaN else. */
export function missingScalar(dtype: string | undefined): CellValue {
  return isMaskedDType(dtype) ? NA : NaN;
}

type ValueKind = "numeric" | "string" | "date" | "other";

function valueKind(value: CellValue): ValueKind {
  if (typeof value === "number" || typeof value === "bigint" || typeof value === "boolean") return "numeric";
  if (typeof value === "string") return "string";
  if (value instanceof Date) return "date";
  return "other";
}

/** Kinds present among non-missing cells. */
export function valueKinds(values: CellValue[]): Set<ValueKind> {
  const kinds = new Set<ValueKind>();
  for (const value of values) {
    if (isMissing(value)) continue;
    kinds.add(valueKind(value));
  }
  return kinds;
}

function typeNameOf(values: CellValue[]): string {
  const value = values.find((entry) => !isMissing(entry));
  return value === undefined ? "NoneType" : pythonOperandTypeName(value);
}

/**
 * pandas numeric-op guard: mean/median/std/var/sem/skew/kurt/quantile/prod
 * raise TypeError on string dtype and on object dtype holding mixed or
 * non-numeric kinds. Booleans participate as 0/1 (pandas treats bool as
 * numeric for everything except quantile — guarded separately there).
 */
export function requireNumeric(values: CellValue[], dtype: string, op: string): number[] {
  const kinds = valueKinds(values);
  const name = typeNameOf(values);
  if (dtype === "string" || dtype === "str" || kinds.has("string") || kinds.has("date") || kinds.has("other") || kinds.size > 1) {
    throw new TypeError(`'${op}' is not supported for ${name} dtype`);
  }
  return values
    .filter((value) => !isMissing(value))
    .map((value) => (typeof value === "boolean" ? (value ? 1 : 0) : Number(value)));
}

/**
 * pandas quantile guard: bool values (bool/boolean dtype, or bools inside
 * object) raise TypeError only when at least one non-missing value exists —
 * an all-missing bool column still yields the missing sentinel.
 */
export function requireQuantileable(values: CellValue[], dtype: string): void {
  const hasBool = (dtype === "bool" || dtype === "boolean" || dtype === "object")
    && values.some((value) => !isMissing(value) && typeof value === "boolean");
  if (hasBool) {
    throw new TypeError("numpy boolean subtract, the `-` operator, is not supported");
  }
}

/** pandas ordered-comparison guard (min/max/idxmax/argmax/cummin/cummax/nlargest). */
export function requireComparable(values: CellValue[], dtype: string, op: string): CellValue[] {
  const kinds = valueKinds(values);
  if (kinds.has("other") || kinds.size > 1) {
    throw new TypeError(`'${op}' is not supported between mixed operand types`);
  }
  return values.filter((value) => !isMissing(value));
}

/** pandas min/max: ordered, strings allowed, empty -> NaN/NA. */
export function pandasMinMax(values: CellValue[], dtype: DType, mode: "min" | "max"): CellValue {
  const present = requireComparable(values, dtype, mode);
  if (present.length === 0) return missingScalar(dtype);
  const sorted = [...present].sort(compareCellValues);
  return mode === "min" ? sorted[0]! : sorted[sorted.length - 1]!;
}

/** pandas argmax/argmin: position of the extreme; ValueError when empty. */
export function pandasArgExtreme(values: CellValue[], mode: "max" | "min"): number {
  const kinds = valueKinds(values);
  if (kinds.has("other") || kinds.size > 1) {
    throw new TypeError(`attempt to get arg${mode} of an empty sequence`);
  }
  let best: CellValue | null = null;
  let bestPos = -1;
  values.forEach((value, i) => {
    if (isMissing(value)) return;
    if (best === null || (mode === "max" ? compareCellValues(value, best) > 0 : compareCellValues(value, best) < 0)) {
      best = value;
      bestPos = i;
    }
  });
  if (bestPos < 0) throw new ValueError(`attempt to get arg${mode} of an empty sequence`);
  return bestPos;
}

/** pandas idxmax/idxmin: label of the extreme; ValueError when empty. */
export function pandasIdxExtreme(values: CellValue[], index: IndexLabel[], mode: "max" | "min"): IndexLabel {
  return index[pandasArgExtreme(values, mode)]!;
}

/** pandas sum: numeric sums; strings concatenate; empty -> 0. */
export function pandasSum(values: CellValue[], dtype: string): number | bigint | string {
  if (dtype === "string" || dtype === "str") {
    // Dedicated string dtype: pandas concatenates non-missing values; an
    // empty/all-missing series sums to "".
    let out = "";
    for (const value of values) if (!isMissing(value)) out += value as string;
    return out;
  }
  const kinds = valueKinds(values);
  if (kinds.has("date") || kinds.has("other") || (kinds.size > 1)) {
    throw new TypeError(`'sum' is not supported for ${typeNameOf(values)} dtype`);
  }
  if (kinds.has("string")) {
    let out = "";
    for (const value of values) if (!isMissing(value)) out += value as string;
    return out;
  }
  const cells = values.filter((value) => !isMissing(value));
  if (cells.length > 0 && cells.every((value) => typeof value === "bigint")) {
    return (cells as bigint[]).reduce((acc, value) => acc + value, 0n);
  }
  return cells
    .map((value) => (typeof value === "boolean" ? (value ? 1 : 0) : Number(value)))
    .reduce((acc, value) => acc + value, 0);
}

/** Python `*` used by object-dtype prod: int*str repeats, str*int repeats. */
function pythonMultiply(acc: CellValue, value: CellValue): CellValue {
  const aNum = typeof acc === "number" || typeof acc === "bigint" || typeof acc === "boolean";
  const bNum = typeof value === "number" || typeof value === "bigint" || typeof value === "boolean";
  if (aNum && bNum) {
    const a = typeof acc === "boolean" ? (acc ? 1 : 0) : Number(acc);
    const b = typeof value === "boolean" ? (value ? 1 : 0) : Number(value);
    return a * b;
  }
  if (aNum && typeof value === "string") {
    const n = Number(acc);
    if (Number.isInteger(n)) return value.repeat(n);
    throw new TypeError("can't multiply sequence by non-int of type 'float'");
  }
  if (typeof acc === "string" && bNum) {
    const n = Number(value);
    if (Number.isInteger(n)) return acc.repeat(n);
    throw new TypeError("can't multiply sequence by non-int of type 'float'");
  }
  throw new TypeError(`unsupported operand type(s) for *: '${typeof acc}' and '${typeof value}'`);
}

/** pandas prod: numeric per dtype; object folds Python `*`; empty -> 1. */
export function pandasProd(values: CellValue[], dtype: string): CellValue {
  if (dtype === "object" || dtype === "mixed") {
    const cells = values.filter((value) => !isMissing(value));
    let acc: CellValue = 1;
    for (const value of cells) acc = pythonMultiply(acc, value);
    return acc;
  }
  const numbers = requireNumeric(values, dtype, "prod");
  const cells = values.filter((value) => !isMissing(value));
  if (cells.length > 0 && cells.every((value) => typeof value === "bigint")) {
    return (cells as bigint[]).reduce((acc, value) => acc * value, 1n);
  }
  return numbers.reduce((acc, value) => acc * value, 1);
}

/** pandas all/any with skipna and dtype-aware missing handling. */
export function pandasAllAny(values: CellValue[], dtype: DType, skipna: boolean, op: "all" | "any"): boolean | CellValue {
  // pandas raises TypeError only for the dedicated str/string dtypes;
  // object-dtype strings evaluate by truthiness instead.
  if (dtype === "string" || dtype === "str") {
    throw new TypeError(`cannot perform ${op} with this type`);
  }
  const masked = isMaskedDType(dtype);
  let sawMissing = false;
  let sawTruthy = false;
  let sawFalsy = false;
  for (const value of values) {
    if (isMissing(value)) {
      sawMissing = true;
      if (skipna) continue;
      if (masked) continue; // masked missing defers to NA result below
      // unmasked missing under skipna=false follows truthiness
      if (typeof value === "number" && Number.isNaN(value)) sawTruthy = true;
      else if (value === null || value === undefined) sawFalsy = true;
      else sawTruthy = true; // NA/NaT sentinels behave truthy? pandas treats pd.NA as truthy-error; treat truthy
      continue;
    }
    if (value) sawTruthy = true; else sawFalsy = true;
  }
  if (op === "all") {
    if (sawFalsy) return false;
    if (!skipna && masked && sawMissing) return NA;
    return true;
  }
  if (sawTruthy) return true;
  if (!skipna && masked && sawMissing) return NA;
  return false;
}

/** pandas mode: values sharing the max count, sorted ascending. */
export function pandasMode(values: CellValue[], keyOf: (v: CellValue) => string): CellValue[] {
  const counts = new Map<string, { value: CellValue; count: number; first: number }>();
  values.forEach((value, i) => {
    if (isMissing(value)) return;
    const key = keyOf(value);
    const entry = counts.get(key);
    if (entry) entry.count += 1;
    else counts.set(key, { value, count: 1, first: i });
  });
  let max = 0;
  for (const entry of counts.values()) max = Math.max(max, entry.count);
  const modes = [...counts.values()].filter((entry) => entry.count === max).map((entry) => entry.value);
  // pandas sorts modes ascending; mixed kinds keep numeric-then-string order
  return modes.sort(compareCellValues);
}
