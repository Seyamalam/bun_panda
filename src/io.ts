import { readFileSync } from "node:fs";
import {
  DataFrame,
  type MergeOptions,
  type PivotTableOptions,
  type ToCSVOptions,
  type ToExcelOptions,
  type ToParquetOptions,
} from "./dataframe";
import { parseCsvText } from "./internal/io/csv";
import { readExcelFile, readExcelFileSync } from "./internal/io/excelRead";
import { writeExcelFrame } from "./internal/io/excelWrite";
import { parseJsonText } from "./internal/io/json";
import { BunPandaValidationError } from "./errors";
import { readParquetFile } from "./internal/io/parquetRead";
import { writeParquetFrame } from "./internal/io/parquetWrite";
import { promoteDTypes, widenDTypeForMissing } from "./dtypes";
import { indexLabelKey } from "./internal/shared/indexing";
import type { DType, IndexLabel, Row } from "./types";
import { range } from "./utils";

export interface ReadCSVOptions {
  sep?: string;
  header?: boolean;
  names?: string[];
  index_col?: string | number;
  na_values?: string[];
}

export type ReadTableOptions = ReadCSVOptions;

export interface ConcatOptions {
  axis?: 0 | 1;
  ignore_index?: boolean;
  join?: "outer" | "inner";
}

export interface ReadJSONOptions {
  orient?: "records" | "list";
  index_col?: string | number;
  lines?: boolean;
}

export interface ReadParquetOptions {
  columns?: string[];
  index_col?: string | number;
}

export interface ReadExcelOptions {
  sheet_name?: string | number;
  header?: boolean;
  names?: string[];
  index_col?: string | number;
}


function assertValidPath(path: unknown): asserts path is string {
  if (typeof path !== "string" || path.trim().length === 0) {
    throw new BunPandaValidationError("Expected a non-empty file path string.");
  }
}

export function validateReadPath(path: unknown): void {
  assertValidPath(path);
}

export async function read_csv(path: string, options: ReadCSVOptions = {}): Promise<DataFrame> {
  assertValidPath(path);
  const text = await Bun.file(path).text();
  return parse_csv(text, options);
}

export function read_csv_sync(path: string, options: ReadCSVOptions = {}): DataFrame {
  assertValidPath(path);
  const text = readFileSync(path, "utf8");
  return parse_csv(text, options);
}

export function parse_csv(text: string, options: ReadCSVOptions = {}): DataFrame {
  return parseCsvText(text, options);
}

export async function read_table(
  path: string,
  options: ReadTableOptions = {}
): Promise<DataFrame> {
  return read_csv(path, withTableSep(options));
}

export function read_table_sync(path: string, options: ReadTableOptions = {}): DataFrame {
  return read_csv_sync(path, withTableSep(options));
}

export function parse_table(text: string, options: ReadTableOptions = {}): DataFrame {
  return parse_csv(text, withTableSep(options));
}

export async function read_tsv(path: string, options: ReadTableOptions = {}): Promise<DataFrame> {
  return read_table(path, options);
}

export function read_tsv_sync(path: string, options: ReadTableOptions = {}): DataFrame {
  return read_table_sync(path, options);
}

export function parse_tsv(text: string, options: ReadTableOptions = {}): DataFrame {
  return parse_table(text, options);
}

export async function read_json(path: string, options: ReadJSONOptions = {}): Promise<DataFrame> {
  assertValidPath(path);
  const text = await Bun.file(path).text();
  return parse_json(text, options);
}

export function read_json_sync(path: string, options: ReadJSONOptions = {}): DataFrame {
  assertValidPath(path);
  const text = readFileSync(path, "utf8");
  return parse_json(text, options);
}

export function parse_json(text: string, options: ReadJSONOptions = {}): DataFrame {
  return parseJsonText(text, options);
}

export function to_csv(dataframe: DataFrame, options: ToCSVOptions = {}): string {
  return dataframe.to_csv(options);
}

export async function read_parquet(
  path: string,
  options: ReadParquetOptions = {}
): Promise<DataFrame> {
  assertValidPath(path);
  return readParquetFile(path, options);
}

export async function read_excel(
  path: string,
  options: ReadExcelOptions = {}
): Promise<DataFrame> {
  assertValidPath(path);
  return readExcelFile(path, options);
}

export function read_excel_sync(
  path: string,
  options: ReadExcelOptions = {}
): DataFrame {
  assertValidPath(path);
  return readExcelFileSync(path, options);
}

export async function to_parquet(
  dataframe: DataFrame,
  options: ToParquetOptions
): Promise<void> {
  await writeParquetFrame(dataframe, options);
}

export function to_excel(
  dataframe: DataFrame,
  options: ToExcelOptions
): void {
  writeExcelFrame(dataframe, options);
}

export function concat(frames: DataFrame[], options: ConcatOptions = {}): DataFrame {
  const axis = options.axis ?? 0;
  const ignoreIndex = options.ignore_index ?? false;

  if (frames.length === 0) {
    return new DataFrame([]);
  }

  if (axis === 0) {
    return concatRows(frames, ignoreIndex, options.join ?? "outer");
  }
  return concatColumns(frames, ignoreIndex);
}

export function merge(left: DataFrame, right: DataFrame, options: MergeOptions): DataFrame {
  return left.merge(right, options);
}

export function pivot_table(dataframe: DataFrame, options: PivotTableOptions): DataFrame {
  return dataframe.pivot_table(options);
}

function withTableSep(options: ReadTableOptions): ReadCSVOptions {
  return {
    ...options,
    sep: options.sep ?? "\t",
  };
}

function concatRows(frames: DataFrame[], ignoreIndex: boolean, join: "outer" | "inner"): DataFrame {
  // Row concat operates on storage keys; tuple MultiColumn labels stringify.
  const frameColumns = frames.map((frame) => frame.columns.map((column) => String(column)));
  const union: string[] = [];
  for (const columns of frameColumns) {
    for (const column of columns) {
      if (!union.includes(column)) {
        union.push(column);
      }
    }
  }
  const columns = join === "inner"
    ? union.filter((column) => frameColumns.every((cols) => cols.includes(column)))
    : union;

  // pandas concat promotes each column's dtype across the inputs; a column
  // absent from a frame introduces missing cells, which only non-nullable
  // ints/bools must widen for (masked dtypes tolerate pd.NA).
  const declared: Record<string, DType> = {};
  for (const column of columns) {
    let promoted: DType | undefined;
    let absent = false;
    frames.forEach((frame, i) => {
      if (!frameColumns[i]!.includes(column)) {
        absent = true;
        return;
      }
      promoted = promoteDTypes(promoted, (frame.dtypes() as Record<string, DType>)[column]);
    });
    const dtype = promoted ?? "object";
    declared[column] = absent ? widenDTypeForMissing(dtype) : dtype;
  }

  const records: Row[] = [];
  const index: IndexLabel[] = [];
  for (const frame of frames) {
    const frameRows = frame.to_records();
    const frameIndex = frame.index;
    for (let i = 0; i < frameRows.length; i += 1) {
      records.push(frameRows[i]!);
      index.push(frameIndex[i]!);
    }
  }

  return DataFrame.createInternal(
    records,
    columns,
    ignoreIndex ? range(records.length) : index,
    {},
    declared
  );
}

function concatColumns(frames: DataFrame[], ignoreIndex: boolean): DataFrame {
  // pandas keeps duplicate column labels: storage keys stay unique while the
  // public `columns` view repeats the label.
  const displayColumns: IndexLabel[] = [];
  const storageKeys: string[] = [];
  const perFrameMappings: Array<Array<{ source: string; target: string }>> = [];
  const seenColumns = new Map<string, number>();


  frames.forEach((frame, frameIdx) => {
    const mappings: Array<{ source: string; target: string }> = [];
    frame.columns.forEach((column, colIdx) => {
      const columnKey = indexLabelKey(column);
      const source = frame.storageColumns[colIdx] ?? String(column);
      const seen = seenColumns.get(columnKey) ?? 0;
      const target = seen === 0 ? source : `${source}__dup${seen}`;
      seenColumns.set(columnKey, seen + 1);
      displayColumns.push(column);
      storageKeys.push(target);
      mappings.push({ source, target });
    });
    perFrameMappings[frameIdx] = mappings;
  });

  const outputIndex: IndexLabel[] = [];
  const seenIndex = new Set<string>();
  const frameIndexLabels: Set<string>[] = [];
  for (const frame of frames) {
    const labels = new Set<string>();
    for (const label of frame.index) {
      labels.add(String(label));
      const key = String(label);
      if (!seenIndex.has(key)) {
        seenIndex.add(key);
        outputIndex.push(label);
      }
    }
    frameIndexLabels.push(labels);
  }

  const perFrameRows = frames.map((frame) => {
    const map = new Map<IndexLabel, Row>();
    const frameRows = frame.to_records();
    const frameIndex = frame.index;
    for (let i = 0; i < frameRows.length; i += 1) {
      map.set(frameIndex[i]!, frameRows[i]!);
    }
    return map;
  });

  const records: Row[] = [];
  for (const label of outputIndex) {
    const row: Row = {};
    for (let frameIndex = 0; frameIndex < frames.length; frameIndex += 1) {
      const sourceRow = perFrameRows[frameIndex]!.get(label);
      const mappings = perFrameMappings[frameIndex]!;
      for (const mapping of mappings) {
        row[mapping.target] = sourceRow?.[mapping.source];
      }
    }
    records.push(row);
  }

  // Each output column keeps its source dtype, widened when the frame
  // doesn't cover every output index label (missing cells are introduced).
  const declared: Record<string, DType> = {};
  frames.forEach((frame, frameIndex) => {
    const frameDtypes = frame.dtypes() as Record<string, DType>;
    const coversAll = outputIndex.every((label) => frameIndexLabels[frameIndex]!.has(String(label)));
    for (const mapping of perFrameMappings[frameIndex]!) {
      const source = frameDtypes[mapping.source] ?? "object";
      declared[mapping.target] = coversAll ? source : widenDTypeForMissing(source);
    }
  });

  return DataFrame.createInternal(
    records,
    storageKeys,
    ignoreIndex ? range(records.length) : outputIndex,
    {},
    declared,
    undefined,
    undefined,
    displayColumns
  );
}
