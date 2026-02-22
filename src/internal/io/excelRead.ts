import { readFileSync } from "node:fs";
import * as XLSX from "xlsx";
import { DataFrame } from "../../dataframe";
import type { ReadExcelOptions } from "../../io";
import type { Row } from "../../types";
import { applyIndexColumn } from "./frame";
import { normalizeExternalCell } from "./shared";

export async function readExcelFile(
  path: string,
  options: ReadExcelOptions = {}
): Promise<DataFrame> {
  const bytes = await Bun.file(path).arrayBuffer();
  return parseExcelBytes(new Uint8Array(bytes), options);
}

export function readExcelFileSync(
  path: string,
  options: ReadExcelOptions = {}
): DataFrame {
  const bytes = readFileSync(path);
  return parseExcelBytes(bytes, options);
}

function parseExcelBytes(bytes: Uint8Array, options: ReadExcelOptions): DataFrame {
  const workbook = XLSX.read(bytes, {
    type: "array",
    cellDates: true,
  });

  const sheet = selectSheet(workbook, options.sheet_name);
  const matrix = XLSX.utils.sheet_to_json(sheet, {
    header: 1,
    raw: true,
    defval: null,
    blankrows: false,
  }) as unknown[][];

  if (matrix.length === 0) {
    return new DataFrame([]);
  }

  const header = options.header ?? true;
  let columns: string[];
  let startRow = 0;

  if (options.names && options.names.length > 0) {
    columns = [...options.names];
    startRow = header ? 1 : 0;
  } else if (header) {
    columns = matrix[0]!.map((value) => String(value ?? "").trim());
    startRow = 1;
  } else {
    columns = matrix[0]!.map((_, position) => `col_${position}`);
  }

  const rows: Row[] = [];
  for (let i = startRow; i < matrix.length; i += 1) {
    const source = matrix[i] ?? [];

    while (source.length > columns.length) {
      columns.push(`col_${columns.length}`);
      for (const existing of rows) {
        existing[columns.at(-1)!] = null;
      }
    }

    const row: Row = {};
    for (let j = 0; j < columns.length; j += 1) {
      row[columns[j]!] = normalizeExternalCell(source[j]);
    }
    rows.push(row);
  }

  return applyIndexColumn(new DataFrame(rows, { columns }), options.index_col);
}

function selectSheet(
  workbook: XLSX.WorkBook,
  sheetName?: string | number
): XLSX.WorkSheet {
  if (sheetName === undefined) {
    const first = workbook.SheetNames[0];
    if (!first) {
      throw new Error("Workbook has no sheets.");
    }
    return workbook.Sheets[first]!;
  }

  if (typeof sheetName === "number") {
    const nameAtIndex = workbook.SheetNames[sheetName];
    if (!nameAtIndex) {
      throw new Error(`Workbook does not contain sheet index ${sheetName}.`);
    }
    return workbook.Sheets[nameAtIndex]!;
  }

  const named = workbook.Sheets[sheetName];
  if (!named) {
    throw new Error(`Workbook does not contain sheet '${sheetName}'.`);
  }
  return named;
}
