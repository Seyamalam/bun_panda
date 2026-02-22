import { readFileSync } from "node:fs";
import {
  DataFrame,
  type MergeOptions,
  type PivotTableOptions,
  type ToCSVOptions,
} from "./dataframe";
import type { CellValue, IndexLabel, Row } from "./types";

export interface ReadCSVOptions {
  sep?: string;
  header?: boolean;
  names?: string[];
  index_col?: string | number;
  na_values?: string[];
}

export interface ConcatOptions {
  axis?: 0 | 1;
  ignore_index?: boolean;
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
  const sep = options.sep ?? ",";
  const rows = parseCsvRows(stripBom(text), sep);
  const header = options.header ?? true;
  const naValues = new Set(
    (options.na_values ?? ["", "NaN", "NA", "null", "None"]).map((value) =>
      value.trim().toLowerCase()
    )
  );

  if (rows.length === 0) {
    return new DataFrame([]);
  }

  let columns: string[];
  let startRow = 0;

  if (options.names && options.names.length > 0) {
    columns = [...options.names];
    startRow = header ? 1 : 0;
  } else if (header) {
    columns = rows[0]!.map((value) => value.trim());
    startRow = 1;
  } else {
    columns = rows[0]!.map((_, position) => `col_${position}`);
    startRow = 0;
  }

  const records: Row[] = [];
  for (let i = startRow; i < rows.length; i += 1) {
    const rawRow = rows[i]!;

    while (rawRow.length > columns.length) {
      columns.push(`col_${columns.length}`);
      for (const existing of records) {
        existing[columns.at(-1)!] = null;
      }
    }

    const record: Row = {};
    for (let columnIndex = 0; columnIndex < columns.length; columnIndex += 1) {
      const raw = rawRow[columnIndex] ?? "";
      record[columns[columnIndex]!] = inferValue(raw, naValues);
    }
    records.push(record);
  }

  let index: IndexLabel[] | undefined;

  if (options.index_col !== undefined) {
    const indexColumn =
      typeof options.index_col === "number"
        ? columns[options.index_col]
        : options.index_col;

    if (!indexColumn || !columns.includes(indexColumn)) {
      throw new Error("index_col does not match any column.");
    }

    index = records.map((record, position) => {
      const value = record[indexColumn];
      if (typeof value === "string" || typeof value === "number") {
        return value;
      }
      return String(value ?? position);
    });

    columns = columns.filter((column) => column !== indexColumn);
    for (const record of records) {
      delete record[indexColumn];
    }
  }

  return new DataFrame(records, { columns, index });
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

export function merge(left: DataFrame, right: DataFrame, options: MergeOptions): DataFrame {
  return left.merge(right, options);
}

export function pivot_table(dataframe: DataFrame, options: PivotTableOptions): DataFrame {
  return dataframe.pivot_table(options);
}

function inferValue(value: string, naValues: Set<string>): CellValue {
  const trimmed = value.trim();
  if (naValues.has(trimmed.toLowerCase())) {
    return null;
  }

  const lowered = trimmed.toLowerCase();
  if (lowered === "true") {
    return true;
  }
  if (lowered === "false") {
    return false;
  }

  if (/^[+-]?\d+(\.\d+)?([eE][+-]?\d+)?$/.test(trimmed)) {
    const parsed = Number(trimmed);
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }

  return trimmed;
}

function parseCsvRows(text: string, sep: string): string[][] {
  const rows: string[][] = [];
  let row: string[] = [];
  let cell = "";
  let inQuotes = false;

  for (let i = 0; i < text.length; i += 1) {
    const char = text[i]!;
    const next = text[i + 1];

    if (char === '"') {
      if (inQuotes && next === '"') {
        cell += '"';
        i += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }

    if (!inQuotes && char === sep) {
      row.push(cell);
      cell = "";
      continue;
    }

    if (!inQuotes && (char === "\n" || char === "\r")) {
      if (char === "\r" && next === "\n") {
        i += 1;
      }
      row.push(cell);
      rows.push(row);
      row = [];
      cell = "";
      continue;
    }

    cell += char;
  }

  const hasTrailingData = cell.length > 0 || row.length > 0;
  if (hasTrailingData) {
    row.push(cell);
    rows.push(row);
  }

  return rows.filter((entry) => entry.length > 1 || entry[0]?.trim().length);
}

function stripBom(text: string): string {
  if (text.charCodeAt(0) === 0xfeff) {
    return text.slice(1);
  }
  return text;
}
