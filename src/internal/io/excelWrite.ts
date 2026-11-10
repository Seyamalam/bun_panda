import * as XLSX from "xlsx";
import type { ToExcelOptions } from "../../dataframe";
import type { CellValue } from "../../types";
import type { FrameLike } from "./frameLike";

export function writeExcelFrame(frame: FrameLike, options: ToExcelOptions): void {
  if (!options.path) {
    throw new Error("to_excel requires a file path.");
  }

  const includeIndex = options.index ?? false;
  // Records are keyed by storage columns; tuple MultiColumn headers stringify.
  const header = includeIndex
    ? ["index", ...frame.columns.map((column) => String(column))]
    : frame.columns.map((column) => String(column));
  const matrix: CellValue[][] = [header];
  const records = frame.to_records();
  const index = frame.index;

  for (let i = 0; i < records.length; i += 1) {
    const record = records[i]!;
    const row = frame.storageColumns.map((column) => normalizeExcelCell(record[column]));
    if (includeIndex) {
      row.unshift(index[i]! as never);
    }
    matrix.push(row);
  }

  const sheet = XLSX.utils.aoa_to_sheet(matrix);
  const workbook = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(
    workbook,
    sheet,
    options.sheet_name ?? "Sheet1"
  );
  XLSX.writeFile(workbook, options.path);
}

function normalizeExcelCell(value: CellValue): CellValue {
  if (value === null || value === undefined) {
    return null;
  }
  if (
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "boolean" ||
    value instanceof Date
  ) {
    return value;
  }
  // Excel cells cannot hold 64-bit integers; write the decimal text when the
  // value exceeds the safe range so it is never silently rounded.
  if (typeof value === "bigint") {
    const asNumber = Number(value);
    return Number.isSafeInteger(asNumber) ? asNumber : value.toString();
  }
  return JSON.stringify(value);
}
