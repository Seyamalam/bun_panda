import type { AggFn, AggName, CellValue, Row } from "../../types";
import { cloneRow, compareCellValues, isMissing, numericValues } from "../../utils";
import { normalizeKeyCell } from "./keys";

export function runAggregation(values: CellValue[], rows: Row[], aggfunc: AggName | AggFn): CellValue {
  if (typeof aggfunc === "function") {
    return aggfunc(values, rows);
  }

  if (aggfunc === "count") {
    return values.filter((value) => !isMissing(value)).length;
  }

  if (aggfunc === "min") {
    const nonMissing = values.filter((value) => !isMissing(value));
    if (nonMissing.length === 0) {
      return null;
    }
    return [...nonMissing].sort(compareCellValues)[0] ?? null;
  }

  if (aggfunc === "max") {
    const nonMissing = values.filter((value) => !isMissing(value));
    if (nonMissing.length === 0) {
      return null;
    }
    return [...nonMissing].sort(compareCellValues).at(-1) ?? null;
  }

  const numbers = numericValues(values);
  if (aggfunc === "sum") {
    return numbers.length > 0 ? numbers.reduce((acc, value) => acc + value, 0) : null;
  }
  if (aggfunc === "mean") {
    return numbers.length > 0 ? numbers.reduce((acc, value) => acc + value, 0) / numbers.length : null;
  }

  return null;
}

export function normalizeRecords(records: Row[], forcedColumns?: string[]): { rows: Row[]; columns: string[] } {
  const columns = forcedColumns ? [...forcedColumns] : [];
  const seen = new Set(columns);

  for (const record of records) {
    for (const column of Object.keys(record)) {
      if (!seen.has(column)) {
        seen.add(column);
        columns.push(column);
      }
    }
  }

  const rows = records.map((record) => cloneRow(record, columns));
  return { rows, columns };
}

export function normalizeColumnar(data: Record<string, CellValue[]>): { rows: Row[]; columns: string[] } {
  const columns = Object.keys(data);
  const rowCount = columns.reduce((max, column) => Math.max(max, data[column]?.length ?? 0), 0);

  const rows: Row[] = [];
  for (let i = 0; i < rowCount; i += 1) {
    const row: Row = {};
    for (const column of columns) {
      row[column] = data[column]?.[i];
    }
    rows.push(row);
  }

  return { rows, columns };
}

export function resolvePosition(position: number, length: number): number | undefined {
  if (!Number.isInteger(position)) {
    return undefined;
  }
  if (position >= 0 && position < length) {
    return position;
  }
  const resolved = length + position;
  if (resolved < 0 || resolved >= length) {
    return undefined;
  }
  return resolved;
}

export function escapeCsvValue(value: CellValue, sep: string): string {
  if (isMissing(value)) {
    return "";
  }
  const text = value instanceof Date ? value.toISOString() : String(value);
  const needsQuoting = text.includes(sep) || text.includes("\n") || text.includes('"');
  if (!needsQuoting) {
    return text;
  }
  return `"${text.replaceAll('"', '""')}"`;
}

export function uniqueColumnValues(
  values: CellValue[],
  options: { sort?: boolean; includeMissing?: boolean } = {}
): CellValue[] {
  const sort = options.sort ?? true;
  const includeMissing = options.includeMissing ?? false;
  const out: CellValue[] = [];
  const seen = new Set<string>();
  for (const value of values) {
    if (!includeMissing && isMissing(value)) {
      continue;
    }
    const key = JSON.stringify(normalizeKeyCell(value));
    if (!seen.has(key)) {
      seen.add(key);
      out.push(value);
    }
  }
  if (sort) {
    out.sort(compareCellValues);
  }
  return out;
}

export function sortRowsByColumns(rows: Row[], columns: string[], marginsName?: string): Row[] {
  const sorted = [...rows];
  sorted.sort((left, right) => {
    if (marginsName && left[columns[0]!] === marginsName) {
      return 1;
    }
    if (marginsName && right[columns[0]!] === marginsName) {
      return -1;
    }

    for (const column of columns) {
      const compared = compareCellValues(left[column], right[column]);
      if (compared !== 0) {
        return compared;
      }
    }
    return 0;
  });
  return sorted;
}

export function safeMarginsColumnName(baseName: string, existingColumns: string[]): string {
  if (!existingColumns.includes(baseName)) {
    return baseName;
  }
  let counter = 1;
  while (existingColumns.includes(`${baseName}_${counter}`)) {
    counter += 1;
  }
  return `${baseName}_${counter}`;
}

export function buildMergedRow(
  leftRow: Row | undefined,
  rightRow: Row | undefined,
  leftColumnsSource: string[],
  leftColumnsOut: string[],
  rightColumnsSource: string[],
  rightColumnsOut: string[],
  joinKeys: string[]
): Row {
  const row: Row = {};

  for (let i = 0; i < leftColumnsSource.length; i += 1) {
    const sourceColumn = leftColumnsSource[i]!;
    const outputColumn = leftColumnsOut[i]!;
    if (leftRow) {
      row[outputColumn] = leftRow[sourceColumn];
      continue;
    }
    row[outputColumn] = joinKeys.includes(sourceColumn) ? rightRow?.[sourceColumn] : undefined;
  }

  for (let i = 0; i < rightColumnsSource.length; i += 1) {
    row[rightColumnsOut[i]!] = rightRow?.[rightColumnsSource[i]!];
  }

  return row;
}
