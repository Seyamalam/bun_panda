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
  const dtypeMap = frame.dtypes();
  const schemaDef = buildSchema(frame.columns, dtypeMap);
  const schema = new parquet.ParquetSchema(schemaDef);
  const writer = await parquet.ParquetWriter.openFile(schema, options.path);

  try {
    const records = frame.to_records();
    for (const record of records) {
      await writer.appendRow(serializeRecord(record, frame.columns, dtypeMap));
    }
  } finally {
    await writer.close();
  }
}

function buildSchema(
  columns: string[],
  dtypeMap: Record<string, InferredDType>
): Record<string, { type: string; optional: boolean }> {
  const schema: Record<string, { type: string; optional: boolean }> = {};
  for (const column of columns) {
    schema[column] = {
      type: parquetTypeForDType(dtypeMap[column] ?? "mixed"),
      optional: true,
    };
  }
  return schema;
}

function parquetTypeForDType(dtype: InferredDType): string {
  if (dtype === "number") {
    return "DOUBLE";
  }
  if (dtype === "boolean") {
    return "BOOLEAN";
  }
  if (dtype === "date") {
    return "TIMESTAMP_MILLIS";
  }
  return "UTF8";
}

function serializeRecord(
  row: Row,
  columns: string[],
  dtypeMap: Record<string, InferredDType>
): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const column of columns) {
    out[column] = serializeCellForParquet(row[column], dtypeMap[column] ?? "mixed");
  }
  return out;
}

function serializeCellForParquet(value: CellValue, dtype: InferredDType): unknown {
  if (value === null || value === undefined) {
    return undefined;
  }

  if (dtype === "number") {
    return typeof value === "number" ? value : Number(value);
  }

  if (dtype === "boolean") {
    return typeof value === "boolean" ? value : Boolean(value);
  }

  if (dtype === "date") {
    if (value instanceof Date) {
      return value;
    }
    const date = new Date(String(value));
    return Number.isFinite(date.getTime()) ? date : undefined;
  }

  if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }

  if (value instanceof Date) {
    return value.toISOString();
  }

  return JSON.stringify(value);
}
