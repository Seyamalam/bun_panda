import { writeFileSync } from "node:fs";
import type { DataFrame } from "../../dataframe";
import { escapeCsvValue } from "./core";
import { writeExcelFrame } from "../io/excelWrite";
import { writeParquetFrame } from "../io/parquetWrite";
import type { ToCSVOptions, ToJSONOptions, ToParquetOptions, ToExcelOptions } from "../../dataframe";

export function dataFrameToCsv(frame: DataFrame, options: ToCSVOptions = {}): string {
  const sep = options.sep ?? ",";
  const includeHeader = options.header ?? true;
  const includeIndex = options.index ?? false;
  const indexName = "index";
  const lines: string[] = [];
  const columns = frame.columns;
  const rows = frame.to_records();
  const index = frame.index;
  if (includeHeader) {
    const headerCells = includeIndex ? [indexName, ...columns] : [...columns];
    lines.push(headerCells.map((cell) => escapeCsvValue(cell, sep)).join(sep));
  }
  for (let i = 0; i < rows.length; i += 1) {
    const row = rows[i]!;
    const rowCells = columns.map((column) => escapeCsvValue(row[column], sep));
    if (includeIndex) rowCells.unshift(escapeCsvValue(index[i], sep));
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

function buildJsonOutput(frame: DataFrame, options: ResolvedToJsonOptions): string {
  if (!options.lines) return JSON.stringify(frame.to_dict(options.orient), null, options.space);
  if (options.orient !== "records") throw new Error("to_json with lines=true only supports orient='records'.");
  return frame.to_records().map((record) => JSON.stringify(record)).join("\n");
}

export async function dataFrameToParquet(frame: DataFrame, options: ToParquetOptions): Promise<void> {
  await writeParquetFrame(frame, options);
}

export function dataFrameToExcel(frame: DataFrame, options: ToExcelOptions): void {
  writeExcelFrame(frame, options);
}
