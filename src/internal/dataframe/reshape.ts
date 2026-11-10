import type { CellValue, Row } from "../../types";
import { compareCellValues } from "../../utils";

/**
 * pandas-style `melt`: wide -> long. Each (id_var, variable, value)
 * triple becomes a row; columns not listed as id_vars are unpivoted.
 */
export function computeMeltRows(
  rows: Row[],
  idVars: string[],
  valueVars: string[]
): { rows: Row[]; columns: string[] } {
  const out: Row[] = [];
  for (const row of rows) {
    for (const valueCol of valueVars) {
      const record: Row = {};
      for (const id of idVars) {
        record[id] = row[id];
      }
      record["variable"] = valueCol;
      record["value"] = row[valueCol];
      out.push(record);
    }
  }
  return { rows: out, columns: [...idVars, "variable", "value"] };
}

/**
 * pandas-style `pivot`: long -> wide. Rows are grouped by `index`
 * column values, spread by `columns` column values, and filled from
 * `values`. Duplicate (index, columns) pairs throw, matching pandas.
 */
export function computePivot(
  rows: Row[],
  indexCol: string,
  columnsCol: string,
  valuesCol: string,
  aggregate?: (values: CellValue[]) => CellValue
): { rows: Row[]; index: CellValue[] } {
  let rowIndexOrder: CellValue[] = [];
  const rowIndexSeen = new Set<string>();
  let colKeys: string[] = [];
  const colKeySeen = new Set<string>();
  const colValues = new Map<string, CellValue>();
  const cellIndex = new Map<string, CellValue[]>();

  for (const row of rows) {
    const idx = row[indexCol];
    const col = row[columnsCol];
    if (idx === null || idx === undefined || col === null || col === undefined) {
      continue;
    }
    const idxKey = String(idx);
    const colKey = String(col);
    if (!rowIndexSeen.has(idxKey)) {
      rowIndexSeen.add(idxKey);
      rowIndexOrder.push(idx);
    }
    if (!colKeySeen.has(colKey)) {
      colKeySeen.add(colKey);
      colKeys.push(colKey);
      colValues.set(colKey, col);
    }
    const key = `${idxKey}\u0000${colKey}`;
    const bucket = cellIndex.get(key);
    if (bucket) {
      bucket.push(row[valuesCol]);
    } else {
      cellIndex.set(key, [row[valuesCol]]);
    }
  }

  // pandas sorts both the row-index labels and the spread column keys.
  rowIndexOrder = [...rowIndexOrder].sort(compareCellValues);
  colKeys = [...colKeys].sort((a, b) =>
    compareCellValues(colValues.get(a)!, colValues.get(b)!)
  );

  const out: Row[] = rowIndexOrder.map((idx) => {
    const row: Row = {};
    row[indexCol] = idx;
    for (const colKey of colKeys) {
      const values = cellIndex.get(`${String(idx)}\u0000${colKey}`);
      if (!values) {
        row[colKey] = null;
      } else if (values.length === 1) {
        row[colKey] = values[0]!;
      } else if (aggregate) {
        row[colKey] = aggregate(values);
      } else {
        throw new Error("Index contains duplicate entries, cannot reshape.");
      }
    }
    return row;
  });

  return { rows: out, index: rowIndexOrder };
}
