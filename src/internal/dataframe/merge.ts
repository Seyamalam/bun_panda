import { buildMergedRow } from "./core";
import { keyForColumns } from "./keys";
import type { CellValue, Row } from "../../types";
import { compareCellValues, isMissing } from "../../utils";

export interface ComputeMergeOptions {
  leftRows: Row[];
  rightRows: Row[];
  leftColumns: string[];
  rightColumns: string[];
  keys: string[];
  how: "inner" | "left" | "right" | "outer";
  suffixes: [string, string];
}

export interface MergeResult {
  rows: Row[];
  columns: string[];
}

export function computeMergeRows(options: ComputeMergeOptions): MergeResult {
  const { leftRows, rightRows, leftColumns, rightColumns, keys, how, suffixes } = options;

  const duplicateNonKeys = new Set(
    leftColumns.filter((column) => !keys.includes(column) && rightColumns.includes(column))
  );

  const leftColumnsOut = leftColumns.map((column) =>
    duplicateNonKeys.has(column) ? `${column}${suffixes[0]}` : column
  );

  const rightColumnsSource = rightColumns.filter((column) => !keys.includes(column));
  const rightColumnsOut = rightColumnsSource.map((column) =>
    duplicateNonKeys.has(column) ? `${column}${suffixes[1]}` : column
  );

  const rightGroups = new Map<string, Array<{ row: Row; position: number }>>();
  for (let i = 0; i < rightRows.length; i += 1) {
    const row = rightRows[i]!;
    const key = keyForColumns(row, keys);
    const current = rightGroups.get(key);
    if (current) {
      current.push({ row, position: i });
    } else {
      rightGroups.set(key, [{ row, position: i }]);
    }
  }

  if (how === "right") {
    const leftGroups = new Map<string, Row[]>();
    for (const row of leftRows) {
      const key = keyForColumns(row, keys);
      const current = leftGroups.get(key);
      if (current) current.push(row);
      else leftGroups.set(key, [row]);
    }

    const rows: Row[] = [];
    for (const rightRow of rightRows) {
      const matches = leftGroups.get(keyForColumns(rightRow, keys));
      if (!matches || matches.length === 0) {
        rows.push(
          buildMergedRow(
            undefined,
            rightRow,
            leftColumns,
            leftColumnsOut,
            rightColumnsSource,
            rightColumnsOut,
            keys
          )
        );
        continue;
      }
      for (const leftRow of matches) {
        rows.push(
          buildMergedRow(
            leftRow,
            rightRow,
            leftColumns,
            leftColumnsOut,
            rightColumnsSource,
            rightColumnsOut,
            keys
          )
        );
      }
    }
    return { rows, columns: [...leftColumnsOut, ...rightColumnsOut] };
  }

  const matchedRightRows = new Set<number>();
  const rows: Row[] = [];

  for (const leftRow of leftRows) {
    const key = keyForColumns(leftRow, keys);
    const matches = rightGroups.get(key);

    if (!matches || matches.length === 0) {
      if (how === "left" || how === "outer") {
        rows.push(
          buildMergedRow(
            leftRow,
            undefined,
            leftColumns,
            leftColumnsOut,
            rightColumnsSource,
            rightColumnsOut,
            keys
          )
        );
      }
      continue;
    }

    for (const match of matches) {
      rows.push(
        buildMergedRow(
          leftRow,
          match.row,
          leftColumns,
          leftColumnsOut,
          rightColumnsSource,
          rightColumnsOut,
          keys
        )
      );
      matchedRightRows.add(match.position);
    }
  }

  if (how === "outer") {
    for (let i = 0; i < rightRows.length; i += 1) {
      if (matchedRightRows.has(i)) {
        continue;
      }
      rows.push(
        buildMergedRow(
          undefined,
          rightRows[i]!,
          leftColumns,
          leftColumnsOut,
          rightColumnsSource,
          rightColumnsOut,
          keys
        )
      );
    }
    // pandas outer merges sort by the join keys, missing labels last.
    const keyOf = (row: Row, key: string): CellValue => row[key] as CellValue;
    rows.sort((a, b) => {
      for (const key of keys) {
        const av = keyOf(a, key);
        const bv = keyOf(b, key);
        const aMiss = isMissing(av);
        const bMiss = isMissing(bv);
        if (aMiss && bMiss) continue;
        if (aMiss) return 1;
        if (bMiss) return -1;
        const cmp = compareCellValues(av, bv);
        if (cmp !== 0) return cmp;
      }
      return 0;
    });
  }

  return {
    rows,
    columns: [...leftColumnsOut, ...rightColumnsOut],
  };
}
