import { dtypeFamily, isCategoricalDType } from "../../dtypes";
import { isMissingScalar } from "../../missing";
import { isDatetimeDType } from "../shared/timezone";
import type { ToParquetOptions } from "../../dataframe";
import type { CellValue, InferredDType, Row } from "../../types";
import type { FrameLike } from "./frameLike";

export async function writeParquetFrame(
  frame: FrameLike,
  options: ToParquetOptions
): Promise<void> {
  if (!options.path) {
    throw new Error("to_parquet requires a file path.");
  }

  const parquet = await import("parquetjs-lite");
  await patchInt64ToPrimitive();
  const dtypeMap = frame.dtypes;
  // Records are keyed by storage columns (tuple MultiColumns use their keys).
  const records = frame.to_records();
  const storageColumns = frame.storageColumns;
  // Physical types are resolved once per column: declared dtypes map directly,
  // while category/object columns pick the physical type their values fit —
  // pandas dictionary-encodes categoricals, which parquetjs-lite cannot emit,
  // so the value scan is the closest faithful mapping (categories of ints
  // stay integers rather than degrading to text).
  const physicalTypes = resolvePhysicalTypes(storageColumns, dtypeMap, records);
  const schema = new parquet.ParquetSchema(buildSchema(storageColumns, physicalTypes));
  const writer = await parquet.ParquetWriter.openFile(schema, options.path);

  try {
    for (const record of records) {
      await writer.appendRow(serializeRecord(record, storageColumns, physicalTypes));
    }
  } finally {
    await writer.close();
  }
}

/**
 * parquetjs-lite runs every cell through toPrimitive before encoding, and
 * its INT64 converter is `parseInt` — a double, so bigint cells beyond 2^53
 * would silently round on write. Patch it once so bigint values reach the
 * writeBigInt64LE encoder untouched; numbers/strings keep the original path.
 */
let int64Patched = false;
async function patchInt64ToPrimitive(): Promise<void> {
  if (int64Patched) {
    return;
  }
  int64Patched = true;
  const { PARQUET_LOGICAL_TYPES } = await import("parquetjs-lite/lib/types.js");
  const int64 = PARQUET_LOGICAL_TYPES["INT64"];
  if (!int64) {
    return;
  }
  const original = int64.toPrimitive;
  int64.toPrimitive = (value: unknown) =>
    typeof value === "bigint" ? value : original(value);
}

function buildSchema(
  columns: string[],
  physicalTypes: Record<string, string>
): Record<string, { type: string; optional: boolean; statistics?: boolean }> {
  const schema: Record<string, { type: string; optional: boolean; statistics?: boolean }> = {};
  for (const column of columns) {
    const type = physicalTypes[column] ?? "UTF8";
    // parquetjs-lite cannot decode the stats it writes for INT64-backed
    // logical types (its TIMESTAMP_* fromPrimitive does `+value` on a
    // bigint), so timestamp columns are written without statistics.
    const statistics = type.startsWith("TIMESTAMP") ? false : undefined;
    schema[column] = statistics === false
      ? { type, optional: true, statistics }
      : { type, optional: true };
  }
  return schema;
}

function resolvePhysicalTypes(
  columns: string[],
  dtypeMap: Record<string, InferredDType>,
  records: Row[]
): Record<string, string> {
  const types: Record<string, string> = {};
  for (const column of columns) {
    const dtype = dtypeMap[column] ?? "mixed";
    const declared = parquetTypeForDType(dtype);
    if (declared !== "UTF8" || (dtype !== "mixed" && dtype !== "object" && dtype !== "unknown" && !isCategoricalDType(dtype))) {
      types[column] = declared;
      continue;
    }
    types[column] = inferPhysicalFromValues(records, column);
  }
  return types;
}

/** Physical type for a declared dtype; "UTF8" means "no direct mapping". */
function parquetTypeForDType(dtype: InferredDType): string {
  // Exact 64-bit integers map to INT64 (parquetjs-lite accepts bigint values).
  if (/^(u?int64|U?Int64)$/.test(dtype)) {
    return "INT64";
  }
  if (/^(u?int(8|16|32)|U?Int(8|16|32))$/.test(dtype)) {
    return "INT32";
  }
  if (/^float32$/i.test(dtype)) {
    return "FLOAT";
  }
  if (dtypeFamily(dtype) === "number") {
    return "DOUBLE";
  }
  if (dtypeFamily(dtype) === "boolean") {
    return "BOOLEAN";
  }
  // datetime64[ns] / datetime64[ns, zone] / legacy "date" all store UTC
  // instants as Date cells; the zone label is dtype metadata parquetjs-lite
  // cannot express (no tz-aware timestamp logical type), so the instant is
  // what lands on disk.
  if (isDatetimeDType(dtype)) {
    return "TIMESTAMP_MILLIS";
  }
  return "UTF8";
}

/**
 * Best-fit physical type for columns without a declared primitive mapping
 * (categorical, object, mixed). Mirrors what a pyarrow dictionary-encoded or
 * inferred column would store: homogeneous values keep their natural type
 * instead of being flattened to text.
 */
function inferPhysicalFromValues(records: Row[], column: string): string {
  let sawNumber = false;
  let sawNonIntegerNumber = false;
  let sawBigint = false;
  let sawBoolean = false;
  let sawDate = false;
  let sawOther = false;
  let sawValue = false;
  for (const record of records) {
    const value = record[column];
    if (isMissingScalar(value)) {
      continue;
    }
    sawValue = true;
    if (typeof value === "number") {
      sawNumber = true;
      if (!Number.isInteger(value)) sawNonIntegerNumber = true;
    } else if (typeof value === "bigint") {
      sawBigint = true;
    } else if (typeof value === "boolean") {
      sawBoolean = true;
    } else if (value instanceof Date) {
      sawDate = true;
    } else {
      sawOther = true;
    }
  }
  if (!sawValue || sawOther) {
    return "UTF8";
  }
  const families = (sawNumber || sawBigint ? 1 : 0) + (sawBoolean ? 1 : 0) + (sawDate ? 1 : 0);
  if (families !== 1) {
    return "UTF8";
  }
  if (sawBoolean) return "BOOLEAN";
  if (sawDate) return "TIMESTAMP_MILLIS";
  // Fractional values force DOUBLE even when bigints are present — pandas
  // would widen to float64, and INT64 would silently truncate them.
  if (sawNonIntegerNumber) return "DOUBLE";
  return "INT64";
}

function serializeRecord(
  row: Row,
  columns: string[],
  physicalTypes: Record<string, string>
): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const column of columns) {
    out[column] = serializeCellForParquet(row[column], physicalTypes[column] ?? "UTF8");
  }
  return out;
}

function serializeCellForParquet(value: CellValue, physicalType: string): unknown {
  if (isMissingScalar(value)) {
    return undefined;
  }

  if (physicalType === "INT64") {
    return typeof value === "bigint" ? value : BigInt(Math.trunc(Number(value)));
  }

  if (physicalType === "INT32") {
    return Math.trunc(Number(value));
  }

  if (physicalType === "DOUBLE" || physicalType === "FLOAT") {
    return typeof value === "number" ? value : Number(value);
  }

  if (physicalType === "BOOLEAN") {
    return typeof value === "boolean" ? value : Boolean(value);
  }

  if (physicalType === "TIMESTAMP_MILLIS") {
    if (value instanceof Date) {
      return value;
    }
    const date = new Date(String(value));
    return Number.isFinite(date.getTime()) ? date : undefined;
  }

  // UTF8 fallback — text columns and anything that did not fit a physical type.
  if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }

  if (value instanceof Date) {
    return value.toISOString();
  }

  // JSON.stringify cannot serialize bigint; use the exact decimal text.
  if (typeof value === "bigint") {
    return value.toString();
  }

  return JSON.stringify(value);
}
