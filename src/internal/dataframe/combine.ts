// Combine / compare / dtype-conversion / reshape helpers for DataFrame.
// Pure functions over (rows, columns, index); the DataFrame methods are
// thin wrappers that pass their private state in and rebuild a frame.
import type { CellValue, IndexLabel, Row } from "../../types";
import { isMissing } from "../../utils";
import { pearson } from "./stats";

/** A user-supplied combiner: receives the two columns (missing = null) and
 * returns either a per-position array or a single value broadcast to all
 * positions of the result column. */
export type FrameCombineFn = (
  a: CellValue[],
  b: CellValue[]
) => CellValue | (CellValue | null)[];

export interface ShapedFrame {
  rows: Row[];
  columns: string[];
  index: IndexLabel[];
}

function alignColumn(
  rows: Row[],
  column: string,
  labelPositions: Map<string, number>,
  unionIndex: IndexLabel[]
): (CellValue | null)[] {
  return unionIndex.map((label) => {
    const pos = labelPositions.get(String(label));
    if (pos === undefined || pos >= rows.length) return null;
    const value = rows[pos]![column];
    return isMissing(value) ? null : (value as CellValue);
  });
}

function labelPositionMap(index: IndexLabel[]): Map<string, number> {
  const positions = new Map<string, number>();
  for (let i = 0; i < index.length; i += 1) {
    if (!positions.has(String(index[i]))) {
      positions.set(String(index[i]), i);
    }
  }
  return positions;
}

/**
 * pandas-style `combine`: align both frames on the union of their indexes
 * and columns, then build each output column by calling `fn` with the two
 * aligned value arrays. A scalar return broadcasts across the column.
 */
export function combineFrames(
  leftRows: Row[],
  leftColumns: string[],
  leftIndex: IndexLabel[],
  rightRows: Row[],
  rightColumns: string[],
  rightIndex: IndexLabel[],
  fn: FrameCombineFn
): ShapedFrame {
  const outIndex = [...leftIndex];
  const leftPositions = labelPositionMap(leftIndex);
  const rightPositions = labelPositionMap(rightIndex);
  for (let i = 0; i < rightIndex.length; i += 1) {
    if (!leftPositions.has(String(rightIndex[i]))) {
      outIndex.push(rightIndex[i]!);
      // The label has no left-side row: alignColumn reads past the end and
      // yields null. Its right-side position stays as mapped above.
      leftPositions.set(String(rightIndex[i]), Number.MAX_SAFE_INTEGER);
    }
  }
  const rowCount = outIndex.length;

  const seen = new Set<string>();
  for (const column of leftColumns) seen.add(column);
  const outColumns = [...leftColumns];
  for (const column of rightColumns) {
    if (!seen.has(column)) {
      outColumns.push(column);
      seen.add(column);
    }
  }

  const outRows: Row[] = Array.from({ length: rowCount }, () => ({} as Row));
  for (const column of outColumns) {
    const a = alignColumn(leftRows, column, leftPositions, outIndex);
    const b = alignColumn(rightRows, column, rightPositions, outIndex);
    const combined = fn(a, b);
    if (Array.isArray(combined)) {
      for (let r = 0; r < rowCount; r += 1) outRows[r]![column] = combined[r] ?? null;
    } else {
      for (let r = 0; r < rowCount; r += 1) outRows[r]![column] = combined;
    }
  }
  return { rows: outRows, columns: outColumns, index: outIndex };
}

/** Element-wise first-non-missing combination (`combine_first`). */
export function combineFirstFrames(
  leftRows: Row[],
  leftColumns: string[],
  leftIndex: IndexLabel[],
  rightRows: Row[],
  rightColumns: string[],
  rightIndex: IndexLabel[]
): ShapedFrame {
  const pickFirst = (a: CellValue[], b: CellValue[]) =>
    a.map((value, i) => (isMissing(value) ? b[i] ?? null : value));
  return combineFrames(
    leftRows,
    leftColumns,
    leftIndex,
    rightRows,
    rightColumns,
    rightIndex,
    pickFirst
  );
}

/**
 * pandas-style `compare`: both frames must be identically labeled. The
 * result keeps only positions whose values differ; columns come in
 * `<column>` (self) / `<column>_other` (other) pairs.
 */
export function compareFrames(
  leftRows: Row[],
  columns: string[],
  index: IndexLabel[],
  rightRows: Row[],
  options: { equal_values?: boolean } = {}
): ShapedFrame {
  const includeEqual = options.equal_values ?? false;
  const outColumns: string[] = [];
  for (const column of columns) {
    outColumns.push(column, `${column}_other`);
  }
  const outRows: Row[] = [];
  const outIndex: IndexLabel[] = [];
  for (let r = 0; r < leftRows.length; r += 1) {
    let differs = false;
    for (const column of columns) {
      if (valuesDiffer(leftRows[r]![column], rightRows[r]![column])) {
        differs = true;
        break;
      }
    }
    if (!differs && !includeEqual) continue;
    const row: Row = {};
    for (const column of columns) {
      row[column] = leftRows[r]![column];
      row[`${column}_other`] = rightRows[r]![column];
    }
    outRows.push(row);
    outIndex.push(index[r]!);
  }
  return { rows: outRows, columns: outColumns, index: outIndex };
}

function valuesDiffer(a: CellValue, b: CellValue): boolean {
  if (isMissing(a) && isMissing(b)) return false;
  if (isMissing(a) || isMissing(b)) return true;
  if (a instanceof Date && b instanceof Date) return a.getTime() !== b.getTime();
  return a !== b;
}

const NUMERIC_STRING = /^[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?$/;

function coerceCell(value: CellValue): CellValue {
  if (isMissing(value)) return value;
  if (typeof value !== "string") return value;
  const trimmed = value.trim();
  if (NUMERIC_STRING.test(trimmed)) return Number(trimmed);
  const lower = trimmed.toLowerCase();
  if (lower === "true") return true;
  if (lower === "false") return false;
  return value;
}

/**
 * pandas-style `convert_dtypes`: coerce numeric-looking and
 * boolean-looking strings to real numbers/booleans across all columns.
 */
export function convertDtypesRows(rows: Row[], columns: string[]): Row[] {
  return rows.map((row) => {
    const next: Row = {};
    for (const column of columns) next[column] = coerceCell(row[column]);
    return next;
  });
}

/**
 * pandas-style `infer_objects`: like `convert_dtypes`, but only touches
 * mixed ("object"-like) columns; homogeneous columns are left untouched.
 */
export function inferObjectsRows(rows: Row[], columns: string[]): Row[] {
  const coercible = columns.filter((column) => {
    let sawValue = false;
    for (const row of rows) {
      const value = row[column];
      if (isMissing(value)) continue;
      sawValue = true;
      if (typeof value !== "string") return false;
      const trimmed = value.trim();
      if (trimmed !== "" && Number.isNaN(Number(trimmed)) && !isBooleanString(trimmed)) return false;
    }
    return sawValue;
  });
  return rows.map((row) => {
    const next: Row = {};
    for (const column of columns) {
      next[column] = coercible.includes(column) ? coerceCell(row[column]) : row[column];
    }
    return next;
  });
}

function isBooleanString(value: string): boolean {
  return value === "true" || value === "false";
}


/**
 * pandas-style `corrwith`: Pearson correlation between the shared numeric
 * columns of two frames, pairing values by matching index label.
 */
export function corrwithValues(
  leftRows: Row[],
  leftColumns: string[],
  leftIndex: IndexLabel[],
  rightRows: Row[],
  rightColumns: string[],
  rightIndex: IndexLabel[]
): Record<string, number | null> {
  const rightByLabel = new Map<string, Row>();
  for (let i = 0; i < rightIndex.length; i += 1) {
    if (!rightByLabel.has(String(rightIndex[i]))) {
      rightByLabel.set(String(rightIndex[i]!), rightRows[i]!);
    }
  }
  const rightSet = new Set(rightColumns);
  const out: Record<string, number | null> = {};
  for (const column of leftColumns) {
    if (!rightSet.has(column)) continue;
    const a: number[] = [];
    const b: number[] = [];
    for (let r = 0; r < leftRows.length; r += 1) {
      const av = leftRows[r]![column];
      const bv = rightByLabel.get(String(leftIndex[r]))?.[column];
      if (typeof av === "number" && typeof bv === "number" && Number.isFinite(av) && Number.isFinite(bv)) {
        a.push(av);
        b.push(bv);
      }
    }
    out[column] = a.length >= 2 ? pearson(a, b) : null;
  }
  return out;
}

/**
 * pandas-style `stack`: wide -> long. Each (row, column) cell becomes one
 * long row `{ index, column, value }`; missing cells are dropped unless
 * `dropna` is false.
 */
export function stackFrame(
  rows: Row[],
  columns: string[],
  index: IndexLabel[],
  dropna = true
): ShapedFrame {
  const outRows: Row[] = [];
  const outIndex: IndexLabel[] = [];
  for (let r = 0; r < rows.length; r += 1) {
    for (const column of columns) {
      const value = rows[r]![column];
      if (dropna && isMissing(value)) continue;
      outRows.push({ index: index[r], column, value });
      outIndex.push(outRows.length - 1);
    }
  }
  return { rows: outRows, columns: ["index", "column", "value"], index: outIndex };
}

/**
 * Inverse of `stack` for single-level long frames: expects exactly three
 * columns `[rowKey, columnKey, valueKey]`. The distinct `rowKey` values
 * become the wide frame's index and the distinct `columnKey` values become
 * its columns.
 */
export function unstackFrame(rows: Row[], columns: string[]): ShapedFrame {
  if (columns.length !== 3) {
    throw new Error(
      `unstack expects a long frame with exactly 3 columns [row key, column key, value]; received ${columns.length}.`
    );
  }
  const [rowKey, columnKey, valueKey] = [columns[0]!, columns[1]!, columns[2]!];
  const rowOrder: CellValue[] = [];
  const rowSeen = new Set<string>();
  const columnOrder: string[] = [];
  const columnSeen = new Set<string>();
  const cells = new Map<string, CellValue>();

  for (const row of rows) {
    const rk = row[rowKey];
    if (isMissing(rk)) continue;
    const ck = String(row[columnKey]);
    if (!rowSeen.has(String(rk))) {
      rowSeen.add(String(rk));
      rowOrder.push(rk);
    }
    if (!columnSeen.has(ck)) {
      columnSeen.add(ck);
      columnOrder.push(ck);
    }
    cells.set(`${String(rk)}\u0000${ck}`, row[valueKey]);
  }

  const outRows: Row[] = rowOrder.map((rk) => {
    const row: Row = { [rowKey]: rk };
    for (const ck of columnOrder) {
      row[ck] = cells.get(`${String(rk)}\u0000${ck}`) ?? null;
    }
    return row;
  });
  return { rows: outRows, columns: [rowKey, ...columnOrder], index: rowOrder as IndexLabel[] };
}

/** True when the index contains duplicate labels. */
export function hasDuplicateLabels(index: IndexLabel[]): boolean {
  const seen = new Set<string>();
  for (const label of index) {
    const key = String(label);
    if (seen.has(key)) return true;
    seen.add(key);
  }
  return false;
}
