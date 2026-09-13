import { NA, isMissingScalar } from "./missing";
import type { CellValue, DType } from "./types";

const integerPattern = /^(U?Int|u?int)(8|16|32|64)$/;
const supported = new Set<string>([
  "number", "date", "string", "boolean", "bool", "object",
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
    if (dtype === "string") return isMissingScalar(value) ? NA : pythonString(value);
    if (dtype === "boolean") {
      if (isMissingScalar(value)) return NA;
      if (typeof value === "boolean") return value;
      if (value === 0 || value === 1) return Boolean(value);
      throw new TypeError("Need to pass bool-like values");
    }
    const nullable = dtype[0] === dtype[0]!.toUpperCase();
    if (isMissingScalar(value)) {
      if (nullable) return NA;
      if (dtype.startsWith("float")) return NaN;
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
