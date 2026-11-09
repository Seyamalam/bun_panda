import { NA, isMissingScalar } from "./missing";
import type { CellValue, DType } from "./types";

const integerPattern = /^(U?Int|u?int)(8|16|32|64)$/;
const supported = new Set<string>([
  "number", "date", "string", "str", "boolean", "bool", "object",
  "float32", "float64", "Float32", "Float64",
  ...["int", "uint", "Int", "UInt"].flatMap(prefix => [8, 16, 32, 64].map(bits => `${prefix}${bits}`)),
]);

export function assertDType(dtype: string): asserts dtype is DType {
  if (!supported.has(dtype)) throw new TypeError(`data type '${dtype}' not understood`);
}

function pythonString(value: CellValue): string {
  if (typeof value === "boolean") return value ? "True" : "False";
  if (Object.is(value, -0)) return "-0.0";
  if (value === Infinity) return "inf";
  if (value === -Infinity) return "-inf";
  return String(value);
}

function asNumber(value: CellValue): number {
  if (typeof value === "number") return value;
  if (typeof value === "boolean") return Number(value);
  if (typeof value !== "string" || value.trim() === "") throw new TypeError(`Cannot convert ${String(value)} to a number`);
  const normalized = value.trim().toLowerCase();
  if (/^[+]?inf(inity)?$/.test(normalized)) return Infinity;
  if (/^-inf(inity)?$/.test(normalized)) return -Infinity;
  if (/^[+-]?nan$/.test(normalized)) return NaN;
  if (!/^[+-]?(?:\d+\.?\d*|\.\d+)(?:e[+-]?\d+)?$/.test(normalized)) throw new TypeError(`could not convert string to float: '${value}'`);
  return Number(normalized);
}

/** Cast a complete column so dtype metadata and scalar representations agree. */
export function castColumn(values: readonly CellValue[], dtype: DType, sourceDType?: string): CellValue[] {
  assertDType(dtype);
  return values.map(value => {
    if (dtype === "object") return value;
    // Compatibility aliases retained for existing bun_panda callers.
    if (dtype === "number") {
      if (isMissingScalar(value)) return null;
      const parsed = Number(value);
      return Number.isFinite(parsed) ? parsed : null;
    }
    if (dtype === "date") {
      if (isMissingScalar(value)) return null;
      const parsed = value instanceof Date ? value : new Date(String(value));
      return Number.isNaN(parsed.getTime()) ? null : parsed;
    }
    if (dtype === "bool") {
      if (value === NA) throw new TypeError("boolean value of NA is ambiguous");
      return typeof value === "number" && Number.isNaN(value) ? true : Boolean(value);
    }
    if (dtype === "string" || dtype === "str") return isMissingScalar(value) ? (dtype === "string" ? NA : NaN) : pythonString(value);
    if (dtype === "boolean") {
      if (isMissingScalar(value)) return NA;
      if (typeof value === "boolean") return value;
      if (value === 0 || value === 1) return Boolean(value);
      throw new TypeError("Need to pass bool-like values");
    }
    const nullable = dtype[0] === dtype[0]!.toUpperCase();
    if (isMissingScalar(value)) {
      if (nullable) return NA;
      if (dtype.startsWith("float")) {
        if (value === NA && !sourceDType?.match(/^(U?Int|Float|boolean)/)) throw new TypeError("float() argument must be a string or a real number, not 'NAType'");
        return NaN;
      }
      throw new TypeError("Cannot convert non-finite values (NA or inf) to integer");
    }
    const integer = integerPattern.exec(dtype);
    if (!integer) {
      const number = asNumber(value);
      return dtype.endsWith("32") ? Math.fround(number) : number;
    }
    if (typeof value === "string" && !/^[+-]?\d+$/.test(value.trim())) {
      throw new TypeError(`invalid literal for int() with base 10: '${value}'`);
    }
    const number = asNumber(value);
    if (!Number.isFinite(number)) throw new TypeError("Cannot convert non-finite values (NA or inf) to integer");
    if ((nullable || sourceDType === undefined) && !Number.isInteger(number)) throw new TypeError(`cannot safely cast non-equivalent values to ${dtype.toLowerCase()}`);
    const truncated = Math.trunc(number);
    if (!Number.isSafeInteger(truncated)) throw new RangeError("Exact 64-bit integers outside JavaScript's safe range require bigint storage (not yet supported)");
    const bits = Number(integer[2]);
    const unsigned = dtype[0]!.toLowerCase() === "u";
    const min = unsigned ? 0 : -(2 ** (bits - 1));
    const max = unsigned ? 2 ** bits - 1 : 2 ** (bits - 1) - 1;
    if (truncated < min || truncated > max) {
      if (!nullable && sourceDType !== "object" && sourceDType !== undefined && bits < 64) {
        const wrapped = ((truncated % 2 ** bits) + 2 ** bits) % 2 ** bits;
        return unsigned || wrapped < 2 ** (bits - 1) ? wrapped : wrapped - 2 ** bits;
      }
      throw new RangeError(`Integer ${truncated} out of bounds for ${dtype}`);
    }
    return truncated === 0 ? 0 : truncated;
  });
}

/** Infer from scalar types; parsing text is exclusively an explicit cast. */
export function convertedDType(values: readonly CellValue[], source: DType | undefined, nullable: boolean): DType {
  if (source && source !== "object" && source !== "number" && source !== "date") {
    if (!nullable || /^(U?Int|Float)/.test(source) || source === "boolean" || source === "string") return source;
    if (/^u?int/.test(source)) return source.replace(/^uint/, "UInt").replace(/^int/, "Int") as DType;
    if (source === "bool") return "boolean";
    if (source === "str") return "string";
  }
  const present = values.filter(value => !isMissingScalar(value));
  if (present.length === 0) {
    if (values.some(value => typeof value === "number" && Number.isNaN(value)) && !values.includes(NA)) return nullable ? "Int64" : "float64";
    return nullable && source?.startsWith("float") ? "Int64" : source ?? "object";
  }
  if (present.every(value => typeof value === "string")) return nullable ? "string" : "str";
  if (present.every(value => typeof value === "boolean")) {
    return nullable ? "boolean" : present.length === values.length ? "bool" : "object";
  }
  if (present.every(value => typeof value === "number")) {
    if (!nullable && values.includes(NA)) return "object";
    const integral = present.every(value => typeof value === "number" && Number.isSafeInteger(value));
    if (nullable) return integral ? "Int64" : source === "float32" ? "Float32" : "Float64";
    return integral && present.length === values.length ? "int64" : "float64";
  }
  if (present.every(value => value instanceof Date)) return "date";
  return "object";
}

/** Select the result dtype after replacement, without coercing incompatible values. */
export function replacementDType(dtype: DType | undefined, values: readonly CellValue[]): DType | undefined {
  if (!dtype || dtype === "number" || dtype === "date" || dtype === "object") return dtype;
  const present = values.filter(value => !isMissingScalar(value));
  if (dtype === "string" || dtype === "str") return present.every(value => typeof value === "string") ? dtype : "object";
  if (dtype === "boolean") {
    if (present.some(value => typeof value !== "boolean")) throw new TypeError("Invalid value for dtype 'boolean'");
    return dtype;
  }
  if (dtype === "bool") return present.length === values.length && present.every(value => typeof value === "boolean") ? dtype : "object";
  const numeric = present.every(value => typeof value === "number");
  if (/^(U?Int|Float)/.test(dtype)) {
    if (!numeric) throw new TypeError(`Invalid value for dtype '${dtype}'`);
    return dtype; // castColumn enforces exact integer/range requirements.
  }
  if (!numeric) return "object";
  if (/^u?int/.test(dtype) && (present.length !== values.length || present.some(value => !Number.isInteger(value)))) return "float64";
  return dtype;
}

export type DTypeSelector = DType | "integer" | "floating" | "datetime";

function normalizedSelector(selector: DTypeSelector): string {
  if (selector === "integer" || selector === "floating" || selector === "datetime") return selector;
  assertDType(selector);
  if (selector === "boolean") return "bool";
  if (selector === "string") return "str";
  if (selector === "date") return "datetime";
  return selector.toLowerCase();
}

export function dtypeSelector(include?: DTypeSelector | DTypeSelector[], exclude?: DTypeSelector | DTypeSelector[]): (dtype: DType) => boolean {
  const normalize = (value?: DTypeSelector | DTypeSelector[]) => (value === undefined ? [] : Array.isArray(value) ? value : [value]).map(normalizedSelector);
  const included = normalize(include), excluded = normalize(exclude);
  if (included.length + excluded.length === 0) throw new TypeError("at least one of include or exclude must be nonempty");
  if (included.some(dtype => excluded.includes(dtype))) throw new TypeError("include and exclude overlap");
  const matches = (dtype: DType, selector: string): boolean => {
    const type = normalizedSelector(dtype);
    if (selector === "number") return type === "number" || /^(u?int|float)/.test(type);
    if (selector === "integer") return /^u?int/.test(type);
    if (selector === "floating") return /^float/.test(type);
    // pandas 3 includes its inferred str dtype when selecting object.
    if (selector === "object") return dtype === "object" || dtype === "str";
    return type === selector;
  };
  return dtype => (included.length === 0 || included.some(selector => matches(dtype, selector))) && !excluded.some(selector => matches(dtype, selector));
}

/** Broad scalar family for consumers that do not distinguish numeric widths. */
export function dtypeFamily(dtype: string): "number" | "boolean" | "string" | "datetime" | "unknown" | "mixed" {
  if (dtype === "number" || /^(u?int|float)/i.test(dtype)) return "number";
  if (dtype === "bool" || dtype === "boolean") return "boolean";
  if (dtype === "str" || dtype === "string") return "string";
  if (dtype === "date") return "datetime";
  return dtype === "unknown" ? "unknown" : "mixed";
}

/** Validate in-place updates without the permissive coercion of an explicit astype. */
export function updatedDType(dtype: DType, incoming: DType, values: readonly CellValue[]): DType {
  const present = values.filter(value => !isMissingScalar(value));
  const invalid = () => { throw new TypeError(`Invalid value for dtype '${dtype}'`); };
  if (dtype === "str" || dtype === "string") {
    if (present.some(value => typeof value !== "string")) invalid();
  } else if (dtype === "bool") {
    if (present.some(value => typeof value !== "boolean")) invalid();
  } else if (/^(u?int|float)/.test(dtype)) {
    if (present.some(value => typeof value !== "number")) invalid();
    if (/^u?int/.test(dtype)) {
      if (present.some(value => !Number.isInteger(value))) invalid();
      const bits = Number(dtype.match(/\d+/)![0]);
      const unsigned = dtype.startsWith("u");
      const min = unsigned ? 0 : -(2 ** (bits - 1));
      const max = unsigned ? 2 ** bits - 1 : 2 ** (bits - 1) - 1;
      if (present.some(value => (value as number) < min || (value as number) > max)) {
        return incoming.startsWith("float") ? incoming : "int64";
      }
    } else if (dtype === "float32" && present.some(value => !Object.is(Math.fround(value as number), value))) invalid();
  }
  return dtype;
}

/**
 * pandas result dtype for element-wise arithmetic between two columns.
 * True division always yields floating output; other ops preserve integer
 * families, widen through float64, and keep nullable/nullable-float sources
 * nullable. Returns undefined for non-numeric operands so callers fall back
 * to inference.
 */
export function arithResultDType(left: DType, right: DType, division: boolean): DType | undefined {
  const isPlainInt = (d: DType) => /^(u?int)/.test(d);
  const isPlainFloat = (d: DType) => /^float/.test(d) || d === "number";
  const isNullableInt = (d: DType) => /^(U?Int)/.test(d);
  const isNullableFloat = (d: DType) => /^Float/.test(d);
  const isNumeric = (d: DType) =>
    isPlainInt(d) || isPlainFloat(d) || isNullableInt(d) || isNullableFloat(d) ||
    d === "bool" || d === "boolean";
  if (!isNumeric(left) || !isNumeric(right)) return undefined;
  const anyNullable = [left, right].some(d => isNullableInt(d) || isNullableFloat(d) || d === "boolean");
  if (division) return anyNullable ? "Float64" : "float64";
  if (left === "bool" && right === "bool") return "bool";
  if (left === "boolean" && right === "boolean") return "boolean";
  if ((left === "boolean" || right === "boolean") && (left === "bool" || right === "bool")) return "boolean";
  if (isNullableFloat(left) || isNullableFloat(right)) return "Float64";
  if (isNullableInt(left) || isNullableInt(right)) {
    return [left, right].some(d => /^UInt/.test(d)) ? "UInt64" : "Int64";
  }
  if (left === "boolean" || right === "boolean") return "Int64";
  if (isPlainFloat(left) || isPlainFloat(right)) return "float64";
  return "int64";
}
