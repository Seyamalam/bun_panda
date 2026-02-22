import { DataFrame } from "../../dataframe";
import type { CellValue, Row } from "../../types";
import type { ReadCSVOptions } from "../../io";
import { applyIndexColumn } from "./frame";
import { stripBom } from "./shared";

export function parseCsvText(text: string, options: ReadCSVOptions = {}): DataFrame {
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

  const frame = new DataFrame(records, { columns });
  return applyIndexColumn(frame, options.index_col);
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

  if (NUMERIC_PATTERN.test(trimmed)) {
    const parsed = Number(trimmed);
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }

  return trimmed;
}

function parseCsvRows(text: string, sep: string): string[][] {
  if (!text.includes('"')) {
    return parseCsvRowsFast(text, sep);
  }

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

function parseCsvRowsFast(text: string, sep: string): string[][] {
  const lines = text.split(/\r?\n/u);
  const rows: string[][] = [];

  for (const line of lines) {
    if (line.trim().length === 0) {
      continue;
    }
    rows.push(line.split(sep));
  }

  return rows;
}

const NUMERIC_PATTERN = /^[+-]?\d+(\.\d+)?([eE][+-]?\d+)?$/;
