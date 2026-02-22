import { Series } from "../../series";
import type { CellValue, IndexLabel, Row } from "../../types";

export type DataFrameApplyAxis = 0 | 1 | "index" | "columns";

export type DataFrameApplyColumnFn = (
  column: Series<CellValue>,
  name: string,
  position: number
) => CellValue;

export type DataFrameApplyRowFn = (
  row: Series<CellValue>,
  index: IndexLabel,
  position: number
) => CellValue;

export type DataFrameMapFn = (
  value: CellValue,
  column: string,
  index: IndexLabel,
  position: number
) => CellValue;

export function normalizeApplyAxis(axis: DataFrameApplyAxis): 0 | 1 {
  if (axis === 0 || axis === "index") {
    return 0;
  }
  if (axis === 1 || axis === "columns") {
    return 1;
  }
  throw new Error("apply axis must be 0/'index' or 1/'columns'.");
}

export function runApplyOnColumns(
  sourceRows: Row[],
  columns: string[],
  index: IndexLabel[],
  fn: DataFrameApplyColumnFn
): Series<CellValue> {
  const out: CellValue[] = [];
  for (let position = 0; position < columns.length; position += 1) {
    const column = columns[position]!;
    const values = sourceRows.map((row) => row[column]);
    const series = new Series(values, {
      index,
      name: column,
    });
    out.push(fn(series, column, position));
  }
  return new Series(out, {
    index: columns,
    name: "apply",
  });
}

export function runApplyOnRows(
  sourceRows: Row[],
  columns: string[],
  index: IndexLabel[],
  fn: DataFrameApplyRowFn
): Series<CellValue> {
  const out: CellValue[] = [];
  for (let position = 0; position < sourceRows.length; position += 1) {
    const row = sourceRows[position]!;
    const label = index[position]!;
    const values = columns.map((column) => row[column]);
    const series = new Series(values, {
      index: columns,
      name: String(label),
    });
    out.push(fn(series, label, position));
  }
  return new Series(out, {
    index,
    name: "apply",
  });
}

export function runApplyMap(
  sourceRows: Row[],
  columns: string[],
  index: IndexLabel[],
  fn: DataFrameMapFn
): Row[] {
  const out: Row[] = new Array(sourceRows.length);
  for (let rowPosition = 0; rowPosition < sourceRows.length; rowPosition += 1) {
    const source = sourceRows[rowPosition]!;
    const target: Row = {};
    for (const column of columns) {
      target[column] = fn(
        source[column],
        column,
        index[rowPosition]!,
        rowPosition
      );
    }
    out[rowPosition] = target;
  }
  return out;
}
