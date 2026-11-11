import { writeFileSync } from "node:fs";
import type { DataFrame } from "../../dataframe";
import { escapeCsvValue } from "./core";
import { jsonPandasReplacer, unquoteRawIntTokens } from "../shared/valueSemantics";
import { isDatetimeDType, dtypeZone } from "../shared/timezone";
import { csvDatetimeFormatter } from "../io/shared";
import type { CellValue, Row } from "../../types";
import { writeExcelFrame } from "../io/excelWrite";
import { writeParquetFrame } from "../io/parquetWrite";
import type { ToCSVOptions, ToJSONOptions, ToParquetOptions, ToExcelOptions } from "../../dataframe";

export function dataFrameToCsv(frame: DataFrame, options: ToCSVOptions = {}): string {
  const sep = options.sep ?? ",";
  const includeHeader = options.header ?? true;
  const includeIndex = options.index ?? false;
  const lines: string[] = [];
  // Export over the storage keys (record keys); tuple MultiColumn labels
  // appear as their storage-key form.
  const rows = frame.to_records();
  const columns = rows.length > 0
    ? Object.keys(rows[0]!)
    : frame.columns.map((column) => (Array.isArray(column) ? column.join(",") : String(column)));

  // pandas formats each datetime column once, at the finest resolution the
  // data needs (day / second / millis, ±HH:MM for aware dtypes); NaT renders
  // as the empty field via the missing check in escapeCsvValue.
  const dtypeMap = frame.dtypes;
  const datetimeFormats = new Map<string, (value: CellValue) => string | undefined>();
  for (const column of columns) {
    const dtype = dtypeMap[column];
    if (dtype !== undefined && isDatetimeDType(dtype)) {
      datetimeFormats.set(column, csvDatetimeFormatter(rows.map((row) => row[column] ?? null), dtype));
    }
  }
  const csvCell = (column: string, value: CellValue): string =>
    escapeCsvValue(datetimeFormats.get(column)?.(value) ?? value, sep);

  // pandas writes each index level as its own field; unnamed levels get an
  // empty header cell, named levels their index name.
  const index = frame.index;
  const indexLevels = index.map((label) => (Array.isArray(label) ? [...label] : [label]));
  const levelCount = Math.max(1, ...indexLevels.map((levels) => levels.length));
  const indexNames = frame.index_names;

  if (includeHeader) {
    const headerCells = columns.map((cell) => escapeCsvValue(cell, sep));
    if (includeIndex) {
      for (let level = levelCount - 1; level >= 0; level -= 1) {
        headerCells.unshift(escapeCsvValue(indexNames[level] ?? "", sep));
      }
    }
    lines.push(headerCells.join(sep));
  }
  for (let i = 0; i < rows.length; i += 1) {
    const row = rows[i]!;
    const rowCells = columns.map((column) => csvCell(column, row[column]!));
    if (includeIndex) {
      const levels = indexLevels[i] ?? [];
      for (let level = levelCount - 1; level >= 0; level -= 1) {
        rowCells.unshift(escapeCsvValue((levels[level] ?? "") as CellValue, sep));
      }
    }
    lines.push(rowCells.join(sep));
  }
  const csv = `${lines.join("\n")}\n`;
  if (options.path) writeFileSync(options.path, csv, "utf8");
  return csv;
}

export function dataFrameToJson(
  frame: DataFrame,
  orientOrOptions: "records" | "list" | ToJSONOptions = "records",
  space = 2
): string {
  const options = normalizeToJsonOptions(orientOrOptions, space);
  const json = buildJsonOutput(frame, options);
  if (options.path) writeFileSync(options.path, `${json}\n`, "utf8");
  return json;
}

type ResolvedToJsonOptions = ToJSONOptions & { orient: "records" | "list"; space: number; lines: boolean };

export function normalizeToJsonOptions(
  orientOrOptions: "records" | "list" | ToJSONOptions,
  space: number
): ResolvedToJsonOptions {
  if (typeof orientOrOptions === "string") return { orient: orientOrOptions, space, lines: false };
  return {
    orient: orientOrOptions.orient ?? "records",
    space: orientOrOptions.space ?? 2,
    lines: orientOrOptions.lines ?? false,
    path: orientOrOptions.path,
  };
}

// pandas to_json date_format="iso": naive datetime columns serialize as
// "2020-01-01T00:00:00.000" (no zone marker); tz-aware columns serialize the
// UTC instant with a trailing "Z". The map records, per storage column,
// whether the column is a naive datetime (vs aware / non-datetime).
function jsonDatetimeNaive(frame: DataFrame): Map<string, boolean> {
  const map = new Map<string, boolean>();
  const dtypeMap = frame.dtypes;
  for (const column of frame.storageColumns) {
    const dtype = dtypeMap[column];
    if (dtype !== undefined && isDatetimeDType(dtype)) {
      map.set(column, dtypeZone(dtype) === null);
    }
  }
  return map;
}

function jsonDatetimeCell(value: CellValue, naive: boolean): CellValue {
  return value instanceof Date && Number.isFinite(value.getTime())
    ? (naive ? value.toISOString().slice(0, -1) : value.toISOString())
    : value;
}

function jsonRecordCells(record: Row, datetimeNaive: Map<string, boolean>): Row {
  let out: Row | undefined;
  for (const [column, naive] of datetimeNaive) {
    const value = record[column];
    if (value instanceof Date && Number.isFinite(value.getTime())) {
      (out ??= { ...record })[column] = jsonDatetimeCell(value, naive);
    }
  }
  return out ?? record;
}

function stringifyJsonCells(value: unknown, space?: number): string {
  return unquoteRawIntTokens(JSON.stringify(value, jsonPandasReplacer, space));
}

function buildJsonOutput(frame: DataFrame, options: ResolvedToJsonOptions): string {
  const datetimeNaive = jsonDatetimeNaive(frame);
  if (!options.lines) {
    const shape = frame.to_dict(options.orient);
    if (options.orient === "records") {
      return stringifyJsonCells(
        (shape as Row[]).map((record) => jsonRecordCells(record, datetimeNaive)),
        options.space
      );
    }
    const columnar = shape as Record<string, CellValue[]>;
    const out: Record<string, CellValue[]> = {};
    for (const [column, values] of Object.entries(columnar)) {
      const naive = datetimeNaive.get(column);
      out[column] = naive === undefined ? values : values.map((value) => jsonDatetimeCell(value, naive));
    }
    return stringifyJsonCells(out, options.space);
  }
  if (options.orient !== "records") throw new Error("to_json with lines=true only supports orient='records'.");
  return frame.to_records()
    .map((record) => stringifyJsonCells(jsonRecordCells(record, datetimeNaive)))
    .join("\n");
}

export async function dataFrameToParquet(frame: DataFrame, options: ToParquetOptions): Promise<void> {
  await writeParquetFrame(frame, options);
}

export function dataFrameToExcel(frame: DataFrame, options: ToExcelOptions): void {
  writeExcelFrame(frame, options);
}
