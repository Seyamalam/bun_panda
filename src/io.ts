import { readFileSync } from "node:fs";
import {
  DataFrame,
  type MergeOptions,
  type PivotTableOptions,
  type ToCSVOptions,
} from "./dataframe";
import { parseCsvText } from "./internal/io/csv";
import { parseJsonText } from "./internal/io/json";
import type { IndexLabel, Row } from "./types";

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
}

export interface ReadJSONOptions {
  orient?: "records" | "list";
  index_col?: string | number;
  lines?: boolean;
}

export async function read_csv(path: string, options: ReadCSVOptions = {}): Promise<DataFrame> {
  const text = await Bun.file(path).text();
  return parse_csv(text, options);
}

export function read_csv_sync(path: string, options: ReadCSVOptions = {}): DataFrame {
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
  const text = await Bun.file(path).text();
  return parse_json(text, options);
}

export function read_json_sync(path: string, options: ReadJSONOptions = {}): DataFrame {
  const text = readFileSync(path, "utf8");
  return parse_json(text, options);
}

export function parse_json(text: string, options: ReadJSONOptions = {}): DataFrame {
  return parseJsonText(text, options);
}

export function to_csv(dataframe: DataFrame, options: ToCSVOptions = {}): string {
  return dataframe.to_csv(options);
}

export function concat(frames: DataFrame[], options: ConcatOptions = {}): DataFrame {
  const axis = options.axis ?? 0;
  const ignoreIndex = options.ignore_index ?? false;

  if (frames.length === 0) {
    return new DataFrame([]);
  }

  if (axis === 0) {
    return concatRows(frames, ignoreIndex);
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

function concatRows(frames: DataFrame[], ignoreIndex: boolean): DataFrame {
  const columns: string[] = [];
  const records: Row[] = [];
  const index: IndexLabel[] = [];

  for (const frame of frames) {
    for (const column of frame.columns) {
      if (!columns.includes(column)) {
        columns.push(column);
      }
    }
  }

  for (const frame of frames) {
    const frameRows = frame.to_records();
    const frameIndex = frame.index;
    for (let i = 0; i < frameRows.length; i += 1) {
      records.push(frameRows[i]!);
      index.push(frameIndex[i]!);
    }
  }

  return new DataFrame(records, {
    columns,
    index: ignoreIndex ? undefined : index,
  });
}

function concatColumns(frames: DataFrame[], ignoreIndex: boolean): DataFrame {
  const outputColumns: string[] = [];
  const perFrameMappings: Array<Array<{ source: string; target: string }>> = [];
  const seenColumns = new Map<string, number>();

  for (const frame of frames) {
    const mappings: Array<{ source: string; target: string }> = [];
    for (const column of frame.columns) {
      const seen = seenColumns.get(column) ?? 0;
      const target = seen === 0 ? column : `${column}_${seen}`;
      seenColumns.set(column, seen + 1);
      outputColumns.push(target);
      mappings.push({ source: column, target });
    }
    perFrameMappings.push(mappings);
  }

  const outputIndex: IndexLabel[] = [];
  const seenIndex = new Set<string>();
  for (const frame of frames) {
    for (const label of frame.index) {
      const key = String(label);
      if (!seenIndex.has(key)) {
        seenIndex.add(key);
        outputIndex.push(label);
      }
    }
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

  return new DataFrame(records, {
    columns: outputColumns,
    index: ignoreIndex ? undefined : outputIndex,
  });
}
