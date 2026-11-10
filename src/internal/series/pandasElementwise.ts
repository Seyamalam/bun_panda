// pandas elementwise semantics shared by Series methods and per-column
// DataFrame delegation. Mirrors pandas 3.0.5:
//   - diff widens narrow ints (int8/16, uint8/16 -> float32) and computes
//     object dtype literally (missing operands raise TypeError); bool
//     columns XOR; masked dtypes keep their dtype with NA propagation.
//   - pct_change maps int -> float64, keeps float32 precision, maps
//     nullable ints -> Float64; boolean raises NotImplementedError;
//     bool raises ZeroDivisionError on zero denominators.
//   - cumsum/cumprod map ints/bools -> int64, nullable -> Int64, and keep
//     float widths; string cumsum concatenates; cumprod on strings raises.
//   - cummin/cummax/abs preserve dtype.
//   - clip casts to float64 for unmasked ints with non-integral bounds and
//     raises TypeError for masked dtypes with non-integral bounds.
import { NotSupportedError, ZeroDivisionError } from "../../errors";
import { NA } from "../../missing";
import type { CellValue, DType } from "../../types";
import { compareCellValues, isMissing, isSubtractableOperand, pythonOperandTypeName } from "../../utils";
import { isMaskedDType } from "./pandasOps";

function isFloat32DType(dtype: string): boolean {
  return dtype === "float32" || dtype === "Float32";
}

function round32(value: number): number {
  return Math.fround(value);
}

/** Integer subtraction; bigints stay exact. */
function subPair(a: CellValue, b: CellValue): CellValue {
  if (typeof a === "boolean" && typeof b === "boolean") return a !== b;
  // pandas subtracts integer columns exactly before the result widens, so
  // bigint operands keep full precision (float64 cast happens downstream).
  if (typeof a === "bigint" && typeof b === "bigint") return a - b;
  if ((typeof a === "bigint" || typeof a === "number" || typeof a === "boolean") &&
      (typeof b === "bigint" || typeof b === "number" || typeof b === "boolean")) {
    return Number(a) - Number(b);
  }
  throw new TypeError(`unsupported operand type(s) for -: '${pythonOperandTypeName(a)}' and '${pythonOperandTypeName(b)}'`);
}

/** pandas dtype for diff results. */
function diffDType(dtype: DType, length: number): DType {
  if (dtype === "object") return "object";
  if (dtype === "bool") return "object";
  if (dtype === "boolean") return "boolean";
  if (dtype === "string" || dtype === "str") return length === 0 ? "object" : dtype;
  if (/^(U?Int|Float)/.test(dtype)) return dtype;
  const m = dtype.match(/^(u?int|float)(\d+)$/);
  if (m) {
    const kind = m[1]!;
    const bits = Number(m[2]!);
    if (kind === "float") return dtype;
    return bits <= 16 ? "float32" : "float64";
  }
  return dtype;
}

/** pandas Series.diff / DataFrame.diff column kernel. */
export function pandasDiff(values: CellValue[], dtype: DType, periods: number): { values: CellValue[]; dtype: DType } {
  const masked = isMaskedDType(dtype);
  const result = new Array<CellValue>(values.length).fill(null);
  const boundary: CellValue = masked ? NA : NaN;
  const f32 = isFloat32DType(dtype);
  for (let i = 0; i < values.length; i += 1) {
    const src = i - periods;
    if (src < 0 || src >= values.length) {
      result[i] = boundary;
      continue;
    }
    const a = values[i]!;
    const b = values[src]!;
    if (masked) {
      // EA dtypes: missing propagates to NA; genuine pairs still validate.
      if (isMissing(a) || isMissing(b)) {
        result[i] = NA;
        continue;
      }
    }
    if (typeof a === "boolean" && typeof b === "boolean") {
      result[i] = a !== b;
      continue;
    }
    if (!isSubtractableOperand(a) || !isSubtractableOperand(b)) {
      throw new TypeError(`unsupported operand type(s) for -: '${pythonOperandTypeName(a)}' and '${pythonOperandTypeName(b)}'`);
    }
    if (dtype === "string" || dtype === "str" || typeof a === "string" || typeof b === "string") {
      throw new TypeError(`unsupported operand type(s) for -: '${pythonOperandTypeName(a)}' and '${pythonOperandTypeName(b)}'`);
    }
    const out = subPair(a, b);
    result[i] = typeof out === "number" && f32 && Number.isFinite(out) ? round32(out) : out;
  }
  return { values: result, dtype: diffDType(dtype, values.length) };
}

/** pandas dtype for pct_change results. */
function pctChangeDType(dtype: DType, length: number): DType {
  if (dtype === "object") return "object";
  if (dtype === "bool") return "object";
  if (dtype === "boolean") return "boolean";
  if (dtype === "string" || dtype === "str") return length === 0 ? "object" : dtype;
  if (/^Float/.test(dtype)) return dtype;
  if (/^U?Int/.test(dtype)) return "Float64";
  if (dtype.startsWith("float")) return dtype;
  if (/^(u?int)/.test(dtype)) return "float64";
  return dtype;
}

/** pandas Series.pct_change / DataFrame.pct_change column kernel. */
export function pandasPctChange(values: CellValue[], dtype: DType, periods: number): { values: CellValue[]; dtype: DType } {
  const masked = isMaskedDType(dtype);
  if (dtype === "boolean") {
    throw new NotSupportedError("pct_change is not supported for boolean dtype");
  }
  const boundary = masked ? NA : NaN;
  const f32 = isFloat32DType(dtype);
  const result = new Array<CellValue>(values.length).fill(null);
  for (let i = 0; i < values.length; i += 1) {
    const src = i - periods;
    if (src < 0 || src >= values.length) {
      result[i] = boundary;
      continue;
    }
    const a = values[i]!;
    const b = values[src]!;
    if (masked) {
      if (isMissing(a) || isMissing(b)) {
        result[i] = NA;
        continue;
      }
    }
    if (dtype === "string" || dtype === "str" || typeof a === "string" || typeof b === "string") {
      if (isMissing(a) || isMissing(b)) {
        result[i] = boundary;
        continue;
      }
      throw new TypeError(`unsupported operand type(s) for /: '${pythonOperandTypeName(a)}' and '${pythonOperandTypeName(b)}'`);
    }
    // object dtype: missing operands produce NaN results without evaluating.
    if (dtype === "object" && (isMissing(a) || isMissing(b))) {
      result[i] = NaN;
      continue;
    }
    if (!isSubtractableOperand(a) || !isSubtractableOperand(b)) {
      throw new TypeError(`unsupported operand type(s) for /: '${pythonOperandTypeName(a)}' and '${pythonOperandTypeName(b)}'`);
    }
    const an = typeof a === "bigint" ? Number(a) : Number(a);
    const bn = typeof b === "bigint" ? Number(b) : Number(b);
    // bool/object keep Python semantics (0 denominator raises); numpy
    // numeric dtypes divide to inf/NaN like pandas.
    if (bn === 0 && (dtype === "bool" || dtype === "object")) {
      throw new ZeroDivisionError("integer division or modulo by zero");
    }
    // pandas computes pct_change as `curr / prev - 1` — not `(curr-prev)/prev`
    // — which differs in the last bit, e.g. 1/3 - 1 = -0.6666666666666667.
    // float32 columns evaluate in float32: round the division, subtract,
    // then round the result again.
    const out = f32 ? round32(round32(an / bn) - 1) : an / bn - 1;
    result[i] = f32 && Number.isFinite(out) ? round32(out) : out;
  }
  return { values: result, dtype: pctChangeDType(dtype, values.length) };
}

/** pandas dtype for cumsum/cumprod results. */
function cumDType(dtype: DType): DType {
  if (/^(U?Int|boolean)/.test(dtype)) return "Int64";
  if (/^(u?int|bool)/.test(dtype)) return "int64";
  return dtype;
}

/** pandas Series.cumsum/cumprod column kernel (missing values are skipped). */
export function pandasCumSumProd(values: CellValue[], dtype: DType, prod: boolean): { values: CellValue[]; dtype: DType } {
  const masked = isMaskedDType(dtype);
  const miss = masked ? (NA as CellValue) : NaN;
  if (prod && (dtype === "string" || dtype === "str")) {
    throw new TypeError("cannot perform cumprod with this type");
  }
  const result = new Array<CellValue>(values.length).fill(null);
  let acc: CellValue | undefined;
  for (let i = 0; i < values.length; i += 1) {
    const value = values[i]!;
    if (isMissing(value)) {
      result[i] = miss;
      continue;
    }
    if (acc === undefined) {
      // Seed with the first present value so string cumsum concatenates
      // ('b','ba','bac') instead of failing on 0 + 'b'.
      acc = typeof value === "boolean" ? Number(value) : value;
      result[i] = acc;
      continue;
    }
    if (prod) {
      // object dtype uses Python '*' semantics (str repetition allowed).
      const av = acc as number | bigint | string;
      const bv = value as number | bigint | string;
      if (typeof av === "number" && typeof bv === "number") {
        acc = av * bv;
      } else if (typeof av === "bigint" && typeof bv === "bigint") {
        acc = av * bv;
      } else if (typeof av === "string" && Number.isInteger(Number(bv)) && typeof bv !== "string") {
        acc = av.repeat(Math.max(0, Number(bv)));
      } else if (Number.isInteger(Number(av)) && typeof bv === "string") {
        acc = bv.repeat(Math.max(0, Number(av)));
      } else if (typeof av === "boolean" || typeof bv === "boolean") {
        acc = Number(av) * Number(bv);
      } else {
        throw new TypeError(`can't multiply sequence by non-int of type '${pythonOperandTypeName(bv)}'`);
      }
    } else {
      const av: CellValue = acc;
      const bv: CellValue = value;
      if (typeof av === "string" || typeof bv === "string") {
        if (typeof av === "string" && typeof bv === "string") acc = av + bv;
        else throw new TypeError(`unsupported operand type(s) for +: '${pythonOperandTypeName(av)}' and '${pythonOperandTypeName(bv)}'`);
      } else if (typeof av === "bigint" && typeof bv === "bigint") {
        acc = av + bv;
      } else {
        acc = Number(av) + Number(bv);
      }
    }
    result[i] = acc;
  }
  return { values: result, dtype: cumDType(dtype) };
}

/** pandas Series.cummin/cummax column kernel. */
export function pandasCumMinMax(values: CellValue[], dtype: DType, max: boolean): { values: CellValue[]; dtype: DType } {
  const masked = isMaskedDType(dtype);
  const miss = masked ? (NA as CellValue) : NaN;
  const result = new Array<CellValue>(values.length).fill(null);
  let acc: CellValue | undefined;
  let hasAcc = false;
  for (let i = 0; i < values.length; i += 1) {
    const value = values[i]!;
    if (isMissing(value)) {
      result[i] = miss;
      continue;
    }
    if (!hasAcc) {
      acc = value;
      hasAcc = true;
    } else {
      const aK = typeof acc === "number" || typeof acc === "bigint" || typeof acc === "boolean" ? "numeric" : acc instanceof Date ? "date" : typeof acc;
      const bK = typeof value === "number" || typeof value === "bigint" || typeof value === "boolean" ? "numeric" : value instanceof Date ? "date" : typeof value;
      if (aK !== bK) {
        throw new TypeError(`'<' not supported between instances of '${pythonOperandTypeName(value)}' and '${pythonOperandTypeName(acc)}'`);
      }
      const cmp = compareCellValues(value, acc);
      acc = max ? (cmp > 0 ? value : acc) : (cmp < 0 ? value : acc);
    }
    result[i] = acc!;
  }
  return { values: result, dtype };
}

/** pandas Series.abs column kernel (dtype preserved; non-numeric raises). */
export function pandasAbs(values: CellValue[], dtype: DType): { values: CellValue[]; dtype: DType } {
  const masked = isMaskedDType(dtype);
  const miss = masked ? (NA as CellValue) : NaN;
  const result = values.map((value): CellValue => {
    if (isMissing(value)) {
      if (dtype === "object") {
        throw new TypeError(`bad operand type for abs(): '${pythonOperandTypeName(value)}'`);
      }
      return miss;
    }
    if (typeof value === "number") return Math.abs(value);
    if (typeof value === "bigint") return value < 0n ? -value : value;
    if (typeof value === "boolean") return value;
    throw new TypeError(`bad operand type for abs(): '${pythonOperandTypeName(value)}'`);
  });
  return { values: result, dtype };
}

/** pandas Series.clip/DataFrame.clip column kernel. */
export function pandasClip(values: CellValue[], dtype: DType, lower: number | null | undefined, upper: number | null | undefined): { values: CellValue[]; dtype: DType } {
  const masked = isMaskedDType(dtype);
  const bounds = [lower, upper].filter((b): b is number => b !== undefined && b !== null);
  const hasFractionalBound = bounds.some((b) => !Number.isInteger(b));
  const maskedInt = /^(U?Int|boolean)/.test(dtype);
  const unmaskedInt = /^(u?int|bool)/.test(dtype);
  const miss = masked ? (NA as CellValue) : NaN;
  let anyPresent = false;
  const result = values.map((value): CellValue => {
    if (isMissing(value)) {
      // object dtype keeps the literal missing value.
      return dtype === "object" ? value : miss;
    }
    anyPresent = true;
    // comparing a string bound/value against a number raises TypeError.
    const numericBound = bounds.length > 0 && typeof value === "string";
    if (numericBound || dtype === "string" || dtype === "str") {
      throw new TypeError(`'>=' not supported between instances of 'str' and 'float'`);
    }
    let out: CellValue = value;
    if (lower !== undefined && lower !== null && compareCellValues(value, lower as CellValue) < 0) out = lower as CellValue;
    if (upper !== undefined && upper !== null && compareCellValues(value, upper as CellValue) > 0) out = upper as CellValue;
    if (out !== value) {
      // pandas only materializes the bound where clipping happens: a
      // fractional bound into a masked int/bool column raises TypeError,
      // while unclipped masked columns keep their dtype untouched.
      if (maskedInt && hasFractionalBound) {
        throw new TypeError(`cannot safely cast non-equivalent ${dtype} bounds`);
      }
      if (/^float/.test(dtype)) {
        out = typeof out === "number" ? out : Number(out);
      }
    }
    return out;
  });
  const resultDType: DType = !anyPresent ? dtype
    : dtype === "bool" && hasFractionalBound ? "object"
    : unmaskedInt && hasFractionalBound ? "float64"
    : dtype;
  return { values: result, dtype: resultDType };
}
