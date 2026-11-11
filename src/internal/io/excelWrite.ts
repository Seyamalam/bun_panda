import * as XLSX from "xlsx";
import type { ToExcelOptions } from "../../dataframe";
import type { CellValue, InferredDType } from "../../types";
import { isMissingScalar } from "../../missing";
import { dtypeZone, isDatetimeAwareDType, zoneOffsetMs } from "../shared/timezone";
import type { FrameLike } from "./frameLike";

export function writeExcelFrame(frame: FrameLike, options: ToExcelOptions): void {
  if (!options.path) {
    throw new Error("to_excel requires a file path.");
  }

  const includeIndex = options.index ?? false;
  const dtypeMap = frame.dtypes;
  // Aware datetime columns hold UTC instants; Excel has no tz, so cells get
  // the wall-clock time pandas users produce via tz_localize(None) (pandas
  // itself raises on tz-aware writes — wall time is the practical analogue).
  const zoneOffsets = frame.storageColumns.map((column) => {
    const dtype: InferredDType | undefined = dtypeMap[column];
    return dtype !== undefined && isDatetimeAwareDType(dtype) && dtypeZone(dtype) !== null
      ? zoneOffsetMs(dtypeZone(dtype)!)
      : 0;
  });

  // pandas writes one column per index level, headed by the level names.
  const index = frame.index;
  const indexLevels = index.map((label) => (Array.isArray(label) ? [...label] : [label]));
  const levelCount = Math.max(1, ...indexLevels.map((levels) => levels.length));
  const indexNames: (string | undefined)[] =
    "index_names" in frame && Array.isArray(frame.index_names)
      ? (frame.index_names as (string | undefined)[])
      : [];

  // Records are keyed by storage columns; tuple MultiColumn headers stringify.
  const header: CellValue[] = frame.columns.map((column) => String(column));
  if (includeIndex) {
    for (let level = levelCount - 1; level >= 0; level -= 1) {
      header.unshift(indexNames[level] ?? "");
    }
  }
  const matrix: CellValue[][] = [header];
  const records = frame.to_records();

  for (let i = 0; i < records.length; i += 1) {
    const record = records[i]!;
    const row = frame.storageColumns.map((column, position) =>
      normalizeExcelCell(record[column], zoneOffsets[position] ?? 0));
    if (includeIndex) {
      const levels = indexLevels[i] ?? [];
      for (let level = levelCount - 1; level >= 0; level -= 1) {
        row.unshift((levels[level] ?? "") as CellValue);
      }
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

function normalizeExcelCell(value: CellValue, zoneOffset = 0): CellValue {
  // NA/NaT/NaN write as empty cells, matching pandas na_rep="" for Excel.
  if (isMissingScalar(value)) {
    return null;
  }
  if (
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "boolean"
  ) {
    return value;
  }
  if (value instanceof Date) {
    return zoneOffset === 0 ? value : new Date(value.getTime() + zoneOffset);
  }
  // Excel cells cannot hold 64-bit integers; write the decimal text when the
  // value exceeds the safe range so it is never silently rounded.
  if (typeof value === "bigint") {
    const asNumber = Number(value);
    return Number.isSafeInteger(asNumber) ? asNumber : value.toString();
  }
  return JSON.stringify(value);
}
