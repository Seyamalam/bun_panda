import { DataFrame } from "../../dataframe";
import type { Row } from "../../types";
import type { ReadParquetOptions } from "../../io";
import { applyIndexColumn } from "./frame";
import { normalizeExternalCell } from "./shared";

export async function readParquetFile(
  path: string,
  options: ReadParquetOptions = {}
): Promise<DataFrame> {
  const parquet = await import("parquetjs-lite");
  const reader = await parquet.ParquetReader.openFile(path);
  const records: Row[] = [];

  try {
    const cursor = reader.getCursor();
    let next = await cursor.next();
    while (next) {
      records.push(normalizeParquetRow(next));
      next = await cursor.next();
    }
  } finally {
    await reader.close();
  }

  let frame = new DataFrame(records);
  if (options.columns && options.columns.length > 0) {
    frame = frame.select(options.columns);
  }

  return applyIndexColumn(frame, options.index_col);
}

function normalizeParquetRow(raw: Record<string, unknown>): Row {
  const row: Row = {};
  for (const [column, value] of Object.entries(raw)) {
    row[column] = normalizeExternalCell(value);
  }
  return row;
}
