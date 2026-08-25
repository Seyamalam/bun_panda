import { GroupBy } from "./groupby";
import {
  dataFrameToCsv,
  dataFrameToJson,
  dataFrameToParquet,
  dataFrameToExcel,
} from "./internal/dataframe/io";
import {
  columnCount,
  columnMax,
  columnMean,
  columnMedian,
  columnMin,
  columnProd,
  columnQuantile,
  columnStd,
  columnSum,
  columnVariance,
  columnIdx,
  covariance,
  describeStatRows,
  pairwiseNumericMatrix,
  pearson,
} from "./internal/dataframe/stats";
import {
  duplicateKeepFlags as computeDuplicateKeepFlags,
  isNumericColumn,
  wasmFilterPositions as computeWasmFilterPositions,
  wasmSortPositions as computeWasmSortPositions,
} from "./internal/dataframe/selection";
import {
  assertSameShape,
  elementwiseBinaryOp,
  elementwiseCompareOp,
} from "./internal/dataframe/arith";
import { performJoin } from "./internal/dataframe/join";
import {
  diffRows,
  pctChangeRows,
  shiftRows,
} from "./internal/dataframe/deltas";
import {
  projectColumns,
  selectDtypeColumns,
  transposeFrame,
} from "./internal/dataframe/shape";
import {
  fillnaRows,
  indexLabelsFromColumn,
  resetIndexRows,
} from "./internal/dataframe/missing";
import { bfillRows, ffillRows, interpolateRows } from "./internal/dataframe/fill";
import {
  aggFrame,
  columnKurt,
  columnSem,
  columnSkew,
  cumminRows,
  cummaxRows,
  cumprodRows,
  frameInfo,
  frameMemoryUsage,
  modeRows,
  reindexRows,
  squeezeResult,
  takeRowsWithIndex,
  toHtmlString,
  toMarkdownString,
  truncateRows,
} from "./internal/dataframe/extended";
import type { GroupByOptions } from "./groupby";
import {
  assertRowsShape,
  normalizeColumnar,
  normalizeRecords,
  resolvePosition,
} from "./internal/dataframe/core";
import { computeMergeRows } from "./internal/dataframe/merge";
import { computeMeltRows, computePivot } from "./internal/dataframe/reshape";
import { computeRolling, computeExpanding } from "./internal/dataframe/rolling";
import { computePivotTable } from "./internal/dataframe/pivotTable";
import { computeValueCountsRows } from "./internal/dataframe/valueCounts";
import {
  normalizeApplyAxis,
  runApplyMap,
  runApplyOnColumns,
  runApplyOnRows,
  type DataFrameApplyAxis,
  type DataFrameApplyColumnFn,
  type DataFrameApplyRowFn,
  type DataFrameMapFn,
} from "./internal/dataframe/apply";
import { normalizeKeyCell } from "./internal/dataframe/keys";
import {
  applyUpdateOverwrite,
  computeDotMatrix,
} from "./internal/dataframe/frameOps";
import { computeExplodeRows } from "./internal/dataframe/explode";
import { compileFrameExpression } from "./internal/dataframe/evalExpr";
import {
  computeClipRows,
  computeIsinRows,
  computeRankRows,
  computeReplaceRows,
  samplePositions,
  type RankOptions,
  type ReplaceInput,
} from "./internal/dataframe/compat";
import {
  computeTransformRows,
  computeWhereRows,
  type TransformFn,
  type TransformInput,
  type WhereCondition,
  type WhereOther,
} from "./internal/dataframe/where";
import {
  buildColumnComparer,
  fullSortPositions,
  normalizeSortAscending,
  normalizeSortLimit,
  selectTopKPositions,
} from "./internal/dataframe/ordering";
import {
  combineFrames,
  combineFirstFrames,
  compareFrames,
  convertDtypesRows,
  corrwithValues,
  hasDuplicateLabels,
  inferObjectsRows,
  stackFrame,
  unstackFrame,
  type FrameCombineFn,
} from "./internal/dataframe/combine";
import { NotSupportedError } from "./errors";
import * as windowApi from "./internal/dataframe/windowApi";
import {
  asciiBoxplot,
  asciiHistogram,
  sparseInfo,
  svgLine,
} from "./internal/shared/plotting";
import { buildLatexTable } from "./internal/dataframe/windowTime";
import { Series } from "./series";
import type {
  AggFn,
  AggName,
  CellValue,
  DType,
  IndexLabel,
  InferredDType,
  Row,
} from "./types";
import {
  cloneRow,
  coerceValueToDType,
  compareCellValues,
  inferColumnDType,
  isMissing,
  isNumber,
  range,
} from "./utils";

export interface DataFrameOptions {
  index?: IndexLabel[];
  columns?: string[];
}

export interface ToCSVOptions {
  path?: string;
  sep?: string;
  header?: boolean;
  index?: boolean;
}

export interface ToJSONOptions {
  path?: string;
  orient?: "records" | "list";
  space?: number;
  lines?: boolean;
}

export interface ToParquetOptions {
  path: string;
}

export interface ToExcelOptions {
  path: string;
  sheet_name?: string;
  index?: boolean;
}

export interface MergeOptions {
  on: string | string[];
  how?: "inner" | "left" | "right" | "outer";
  suffixes?: [string, string];
}

export interface JoinOptions {
  on?: string;
  how?: "inner" | "left" | "right" | "outer";
  suffixes?: [string, string];
  /** Column names from `right` to include (defaults to all). */
  rsuffixOnly?: string[];
}

export interface ValueCountsOptions {
  subset?: string | string[];
  normalize?: boolean;
  dropna?: boolean;
  sort?: boolean;
  ascending?: boolean;
  limit?: number;
}

export interface SampleOptions {
  frac?: number;
  replace?: boolean;
  random_state?: number;
  ignore_index?: boolean;
}

export type DropDuplicatesKeep = "first" | "last" | false;
export type { RankOptions, ReplaceInput };
export type { FrameCombineFn };
export type {
  DataFrameApplyAxis,
  DataFrameApplyColumnFn,
  DataFrameApplyRowFn,
  DataFrameMapFn,
  TransformFn,
  TransformInput,
  WhereCondition,
  WhereOther,
};

export interface PivotTableOptions {
  index: string | string[];
  values: string | string[];
  columns?: string;
  aggfunc?: AggName | AggFn;
  fill_value?: CellValue;
  margins?: boolean;
  margins_name?: string;
  dropna?: boolean;
  sort?: boolean;
}

export interface DataFrameCompareOptions {
  /** Include rows whose values are equal (pandas `equal_values`). */
  equal_values?: boolean;
}

export interface DataFrameFlagsOptions {
  /** When false, throw if the index contains duplicate labels. */
  allows_duplicate_labels?: boolean;
}

type AssignmentValue = CellValue[] | Series<CellValue> | CellValue;

export class DataFrame {
  private readonly _rows: Row[];
  private readonly _columns: string[];
  private readonly _index: IndexLabel[];
  /** Free-form metadata dict, matching pandas DataFrame.attrs. */
  private _attrs: Record<string, unknown> = {};

  constructor(data: Row[] | Record<string, CellValue[]> = [], options: DataFrameOptions = {}) {
    const normalized = Array.isArray(data)
      ? normalizeRecords(data, options.columns)
      : normalizeColumnar(data);

    this._rows = normalized.rows;
    this._columns = normalized.columns;
    this._index = options.index ? [...options.index] : range(this._rows.length);

    assertRowsShape(this._rows, this._index);
  }

  static createInternal(rows: Row[], columns: string[], index: IndexLabel[]): DataFrame {
    assertRowsShape(rows, index);
    const frame = Object.create(DataFrame.prototype) as DataFrame;
    (frame as unknown as { _rows: Row[] })._rows = rows;
    (frame as unknown as { _columns: string[] })._columns = columns;
    (frame as unknown as { _index: IndexLabel[] })._index = index;
    return frame;
  }

  static from_records(records: Row[], options: DataFrameOptions = {}): DataFrame {
    return new DataFrame(records, options);
  }

  static from_dict(
    data: Record<string, CellValue[]> | Row[],
    options: DataFrameOptions = {}
  ): DataFrame {
    return new DataFrame(data as Record<string, CellValue[]>, options);
  }

  static from_normalized(
    rows: Row[],
    columns: string[],
    index?: IndexLabel[]
  ): DataFrame {
    return DataFrame.createInternal(rows, [...columns], index ? [...index] : range(rows.length));
  }

  /**
   * Builds a frame from column-major data where numeric columns may be
   * Float64Arrays (NaN = missing) — the typed-ingest path used by
   * read_parquet. Typed columns are copied into row-major storage once,
   * without per-cell type dispatch on plain values.
   */
  static from_typed(
    data: Record<string, CellValue[] | Float64Array>,
    options: DataFrameOptions = {}
  ): DataFrame {
    const columns = Object.keys(data);
    const rowCount = columns.reduce(
      (max, column) => Math.max(max, (data[column] as ArrayLike<CellValue>)?.length ?? 0),
      0
    );

    const rows: Row[] = new Array(rowCount);
    for (let i = 0; i < rowCount; i += 1) {
      rows[i] = {};
    }

    for (const column of columns) {
      const source = data[column];
      if (!source) {
        continue;
      }
      if (source instanceof Float64Array) {
        for (let i = 0; i < rowCount; i += 1) {
          const value = source[i] as number;
          rows[i]![column] = Number.isNaN(value) ? null : value;
        }
      } else {
        for (let i = 0; i < rowCount; i += 1) {
          rows[i]![column] = (source as CellValue[])[i] ?? null;
        }
      }
    }

    return new DataFrame(rows, options);
  }

  get columns(): string[] {
    return [...this._columns];
  }

  get index(): IndexLabel[] {
    return [...this._index];
  }

  get shape(): [number, number] {
    return [this._rows.length, this._columns.length];
  }

  get empty(): boolean {
    return this._rows.length === 0;
  }

  /** Number of axes (always 2), matching pandas ndim. */
  get ndim(): number {
    return 2;
  }

  /** Total cell count, matching pandas size. */
  get size(): number {
    return this._rows.length * this._columns.length;
  }

  /** Iterable of [columnName, Series] — alias of items for dict-like use. */
  keys(): string[] {
    return [...this._columns];
  }

  /** Index label of the first row with any non-missing value. */
  first_valid_index(): IndexLabel | null {
    for (let i = 0; i < this._rows.length; i += 1) {
      const row = this._rows[i]!;
      const hasValue = this._columns.some((column) => !isMissing(row[column]));
      if (hasValue) {
        return this._index[i]!;
      }
    }
    return null;
  }

  /** Index label of the last row with any non-missing value. */
  last_valid_index(): IndexLabel | null {
    for (let i = this._rows.length - 1; i >= 0; i -= 1) {
      const row = this._rows[i]!;
      const hasValue = this._columns.some((column) => !isMissing(row[column]));
      if (hasValue) {
        return this._index[i]!;
      }
    }
    return null;
  }

  isna(): DataFrame {
    const rows = this._rows.map((row) => {
      const next: Row = {};
      for (const column of this._columns) {
        next[column] = isMissing(row[column]) ||
          (typeof row[column] === "number" && Number.isNaN(row[column]));
      }
      return next;
    });
    return this.withRows(rows, [...this._index], this._columns, true);
  }

  isnull(): DataFrame {
    return this.isna();
  }

  notnull(): DataFrame {
    return this.notna();
  }

  add_prefix(prefix: string): DataFrame {
    return this.rename(
      Object.fromEntries(this._columns.map((column) => [column, `${prefix}${column}`]))
    );
  }

  add_suffix(suffix: string): DataFrame {
    return this.rename(
      Object.fromEntries(this._columns.map((column) => [column, `${column}${suffix}`]))
    );
  }

  copy(): DataFrame {
    return new DataFrame(this.to_records(), {
      columns: this._columns,
      index: this._index,
    });
  }

  to_records(): Row[] {
    return this._rows.map((row) => cloneRow(row, this._columns));
  }

  values(): CellValue[][] {
    return this._rows.map((row) => this._columns.map((column) => row[column]));
  }

  dtypes(): Record<string, InferredDType> {
    const out: Record<string, InferredDType> = {};
    for (const column of this._columns) {
      out[column] = inferColumnDType(this._rows.map((row) => row[column]));
    }
    return out;
  }

  nunique(dropna = true): Record<string, number> {
    const out: Record<string, number> = {};
    for (const column of this._columns) {
      const seen = new Set<string>();
      for (const row of this._rows) {
        const value = row[column];
        if (dropna && isMissing(value)) {
          continue;
        }
        seen.add(JSON.stringify(normalizeKeyCell(value)));
      }
      out[column] = seen.size;
    }
    return out;
  }

  astype(dtype: DType | Record<string, DType>): DataFrame {
    const castMap: Record<string, DType> = {};

    if (typeof dtype === "string") {
      for (const column of this._columns) {
        castMap[column] = dtype;
      }
    } else {
      for (const [column, target] of Object.entries(dtype)) {
        this.assertColumnExists(column);
        castMap[column] = target;
      }
    }

    const rows = this._rows.map((row) => {
      const next = cloneRow(row, this._columns);
      for (const [column, target] of Object.entries(castMap)) {
        next[column] = coerceValueToDType(next[column], target);
      }
      return next;
    });

    return this.withRows(rows, this._index, this._columns, true);
  }

  to_dict(orient: "records" | "list" = "records"): Row[] | Record<string, CellValue[]> {
    if (orient === "records") {
      return this.to_records();
    }
    const out: Record<string, CellValue[]> = {};
    for (const column of this._columns) {
      out[column] = this._rows.map((row) => row[column]);
    }
    return out;
  }

  to_json(
    orientOrOptions: "records" | "list" | ToJSONOptions = "records",
    space = 2
  ): string {
    return dataFrameToJson(this, orientOrOptions, space);
  }

  to_csv(options: ToCSVOptions = {}): string {
    return dataFrameToCsv(this, options);
  }

  async to_parquet(options: ToParquetOptions): Promise<void> {
    await dataFrameToParquet(this, options);
  }

  to_excel(options: ToExcelOptions): void {
    dataFrameToExcel(this, options);
  }

  head(n = 5): DataFrame {
    const count = Math.max(0, n);
    return this.withRows(
      this._rows.slice(0, count),
      this._index.slice(0, count),
      this._columns,
      true
    );
  }

  tail(n = 5): DataFrame {
    const count = Math.max(0, n);
    return this.withRows(
      this._rows.slice(-count),
      this._index.slice(-count),
      this._columns,
      true
    );
  }

  get(column: string): Series<CellValue> {
    this.assertColumnExists(column);
    return new Series(this._rows.map((row) => row[column]), {
      name: column,
      index: this._index,
    });
  }

  set(column: string, values: CellValue[] | Series<CellValue>): DataFrame {
    return this.assign({ [column]: values });
  }

  at(index: IndexLabel, column: string): CellValue {
    this.assertColumnExists(column);
    const rowPosition = this._index.findIndex((entry) => entry === index);
    if (rowPosition < 0) {
      return undefined;
    }
    return this._rows[rowPosition]?.[column];
  }

  iloc(selector: number | number[]): Row | DataFrame | undefined {
    if (typeof selector === "number") {
      const position = resolvePosition(selector, this._rows.length);
      if (position === undefined) {
        return undefined;
      }
      return cloneRow(this._rows[position]!, this._columns);
    }

    const rows: Row[] = [];
    const index: IndexLabel[] = [];
    for (const requestedPosition of selector) {
      const position = resolvePosition(requestedPosition, this._rows.length);
      if (position === undefined) {
        continue;
      }
      rows.push(cloneRow(this._rows[position]!, this._columns));
      index.push(this._index[position]!);
    }
    return this.withRows(rows, index, this._columns, true);
  }

  loc(selector: IndexLabel | IndexLabel[]): Row | DataFrame | undefined {
    if (!Array.isArray(selector)) {
      const position = this._index.findIndex((entry) => entry === selector);
      if (position < 0) {
        return undefined;
      }
      return cloneRow(this._rows[position]!, this._columns);
    }

    const rows: Row[] = [];
    const index: IndexLabel[] = [];
    for (const label of selector) {
      const position = this._index.findIndex((entry) => entry === label);
      if (position < 0) {
        continue;
      }
      rows.push(cloneRow(this._rows[position]!, this._columns));
      index.push(this._index[position]!);
    }
    return this.withRows(rows, index, this._columns, true);
  }

  assign(assignments: Record<string, AssignmentValue>): DataFrame {
    const rows = this.to_records();
    const columns = [...this._columns];
    const rowCount = this._rows.length;

    for (const [column, value] of Object.entries(assignments)) {
      const values = this.resolveAssignment(column, value, rowCount);
      for (let i = 0; i < rowCount; i += 1) {
        rows[i]![column] = values[i];
      }
      if (!columns.includes(column)) {
        columns.push(column);
      }
    }

    return this.withRows(rows, this._index, columns, true);
  }

  insert(
    loc: number,
    column: string,
    value: CellValue | CellValue[] | Series<any>
  ): DataFrame {
    if (this._columns.includes(column)) {
      throw new Error(`Column '${column}' already exists.`);
    }
    const position = Math.min(Math.max(loc, 0), this._columns.length);
    const values = this.resolveAssignment(column, value, this._rows.length);

    const nextColumns = [...this._columns];
    nextColumns.splice(position, 0, column);

    const rows = this._rows.map((row, i) => {
      const next: Row = {};
      for (const entry of nextColumns) {
        next[entry] = entry === column ? values[i]! : row[entry];
      }
      return next;
    });

    return this.withRows(rows, this._index, nextColumns, true);
  }

  pop(column: string): Series<CellValue> {
    this.assertColumnExists(column);
    const values = this._rows.map((row) => row[column]);
    const series = new Series(values, { name: column, index: this._index });

    const nextColumns = this._columns.filter((entry) => entry !== column);
    (this as unknown as { _rows: Row[] })._rows = this._rows.map((row) =>
      cloneRow(row, nextColumns)
    );
    (this as unknown as { _columns: string[] })._columns = nextColumns;

    return series;
  }

  select(columns: string[]): DataFrame {
    for (const column of columns) {
      this.assertColumnExists(column);
    }
    const rows = this._rows.map((row) => cloneRow(row, columns));
    return this.withRows(rows, this._index, columns, true);
  }

  drop(columns: string | string[]): DataFrame {
    const removed = new Set(Array.isArray(columns) ? columns : [columns]);
    for (const column of removed) {
      this.assertColumnExists(column);
    }

    const nextColumns = this._columns.filter((column) => !removed.has(column));
    const nextRows = this._rows.map((row) => cloneRow(row, nextColumns));
    return this.withRows(nextRows, this._index, nextColumns, true);
  }

  rename(columns: Record<string, string>): DataFrame {
    const renamedColumns = this._columns.map((column) => columns[column] ?? column);
    if (new Set(renamedColumns).size !== renamedColumns.length) {
      throw new Error("Column rename would create duplicates.");
    }

    const nextRows = this._rows.map((row) => {
      const next: Row = {};
      for (const column of this._columns) {
        const renamed = columns[column] ?? column;
        next[renamed] = row[column];
      }
      return next;
    });

    return this.withRows(nextRows, this._index, renamedColumns, true);
  }

  /** Renames the axis (index) labels via a mapper or new label list. */
  rename_axis(mapper: Record<string, IndexLabel> | ((label: IndexLabel) => IndexLabel)): DataFrame {
    const mapLabel = (label: IndexLabel): IndexLabel => {
      if (typeof mapper === "function") {
        return mapper(label);
      }
      const key = String(label);
      return key in mapper ? mapper[key]! : label;
    };
    return this.withIndex(this._index.map(mapLabel));
  }

  /** Assigns a new index label list (pandas set_axis). */
  set_axis(labels: IndexLabel[], axis: "index" | "columns" = "index"): DataFrame {
    if (axis === "columns") {
      if (labels.length !== this._columns.length) {
        throw new Error("set_axis length must match column count.");
      }
      const nextRows = this._rows.map((row) => {
        const next: Row = {};
        this._columns.forEach((column, i) => {
          next[String(labels[i])] = row[column];
        });
        return next;
      });
      return this.withRows(nextRows, this._index, [...labels] as string[], true);
    }
    if (labels.length !== this._index.length) {
      throw new Error("set_axis length must match index length.");
    }
    return this.withIndex([...labels]);
  }

  /**
   * Functional chaining helper (pandas pipe): applies `fn` to the frame
   * with extra args, returning its result.
   */
  pipe<T>(fn: (frame: DataFrame, ...args: never[]) => T, ...args: never[]): T {
    return fn(this, ...args);
  }

  /** True for every numeric column (skipping nulls); false otherwise. */
  all(): Record<string, boolean> {
    const out: Record<string, boolean> = {};
    for (const column of this._columns) {
      out[column] = this._rows.every((row) => {
        const value = row[column];
        return value !== null && value !== undefined && value !== 0 && value !== false;
      });
    }
    return out;
  }

  /** True for at least one truthy entry per numeric column. */
  any(): Record<string, boolean> {
    const out: Record<string, boolean> = {};
    for (const column of this._columns) {
      out[column] = this._rows.some((row) => {
        const value = row[column];
        return value !== null && value !== undefined && value !== 0 && value !== false;
      });
    }
    return out;
  }

  /** Index labels of the maximum in each numeric column. */
  idxmax(): Record<string, IndexLabel | null> {
    return columnIdx(this._rows, this._columns, this._index, "max");
  }

  /** Index labels of the minimum in each numeric column. */
  idxmin(): Record<string, IndexLabel | null> {
    return columnIdx(this._rows, this._columns, this._index, "min");
  }

  /** First `n` rows ordered by `by` descending (pandas nlargest). */
  nlargest(n: number, by: string | string[]): DataFrame {
    return this.orderByColumns(by, false).head(n);
  }

  /** First `n` rows ordered by `by` ascending (pandas nsmallest). */
  nsmallest(n: number, by: string | string[]): DataFrame {
    return this.orderByColumns(by, true).head(n);
  }

  private orderByColumns(by: string | string[], ascendingFirst: boolean): DataFrame {
    const columns = Array.isArray(by) ? by : [by];
    for (const column of columns) {
      this.assertColumnExists(column);
    }
    const positions = [...range(this._rows.length)];
    positions.sort((leftPos, rightPos) => {
      for (const column of columns) {
        const compared = compareCellValues(
          this._rows[leftPos]![column],
          this._rows[rightPos]![column]
        );
        if (compared !== 0) {
          return ascendingFirst ? compared : -compared;
        }
      }
      return 0;
    });
    return this.withRows(
      positions.map((position) => this._rows[position]!),
      positions.map((position) => this._index[position]!),
      this._columns,
      true
    );
  }

  // ---- element-wise arithmetic (numeric columns; nulls propagate) ----

  private binaryOp(
    other: number | DataFrame,
    fn: (a: number, b: number) => number
  ): DataFrame {
    const otherFrame = other instanceof DataFrame ? other : null;
    if (otherFrame) {
      assertSameShape(this._rows, this._columns, otherFrame._rows, otherFrame._columns);
    }
    const rows = elementwiseBinaryOp(
      this._rows,
      this._columns,
      otherFrame?._rows ?? null,
      otherFrame?._columns ?? null,
      otherFrame ? null : (other as number),
      fn
    );
    return this.withRows(rows, [...this._index], [...this._columns], true);
  }

  add(other: number | DataFrame): DataFrame {
    return this.binaryOp(other, (a, b) => a + b);
  }

  sub(other: number | DataFrame): DataFrame {
    return this.binaryOp(other, (a, b) => a - b);
  }

  rsub(other: number | DataFrame): DataFrame {
    return this.binaryOp(other, (a, b) => b - a);
  }

  mul(other: number | DataFrame): DataFrame {
    return this.binaryOp(other, (a, b) => a * b);
  }

  div(other: number | DataFrame): DataFrame {
    return this.binaryOp(other, (a, b) => a / b);
  }

  truediv(other: number | DataFrame): DataFrame {
    return this.div(other);
  }

  mod(other: number | DataFrame): DataFrame {
    return this.binaryOp(other, (a, b) => a % b);
  }

  pow(other: number | DataFrame): DataFrame {
    return this.binaryOp(other, (a, b) => a ** b);
  }

  floordiv(other: number | DataFrame): DataFrame {
    return this.binaryOp(other, (a, b) => Math.floor(a / b));
  }

  radd(other: number | DataFrame): DataFrame {
    return this.add(other);
  }

  rmul(other: number | DataFrame): DataFrame {
    return this.mul(other);
  }

  rdiv(other: number | DataFrame): DataFrame {
    return this.binaryOp(other, (a, b) => b / a);
  }

  rpow(other: number | DataFrame): DataFrame {
    return this.binaryOp(other, (a, b) => b ** a);
  }

  rmod(other: number | DataFrame): DataFrame {
    return this.binaryOp(other, (a, b) => b % a);
  }

  // ---- element-wise comparisons (boolean frames) ----

  private compareOp(
    other: CellValue | DataFrame,
    fn: (a: number, b: number) => boolean
  ): DataFrame {
    const otherFrame = other instanceof DataFrame ? other : null;
    const rows = elementwiseCompareOp(
      this._rows,
      this._columns,
      otherFrame?._rows ?? null,
      otherFrame ? null : (other as CellValue),
      fn
    );
    return this.withRows(rows, [...this._index], [...this._columns], true);
  }

  eq(other: CellValue | DataFrame): DataFrame {
    return this.compareOp(other, (a, b) => a === b);
  }

  ne(other: CellValue | DataFrame): DataFrame {
    return this.compareOp(other, (a, b) => a !== b);
  }

  lt(other: CellValue | DataFrame): DataFrame {
    return this.compareOp(other, (a, b) => a < b);
  }

  le(other: CellValue | DataFrame): DataFrame {
    return this.compareOp(other, (a, b) => a <= b);
  }

  gt(other: CellValue | DataFrame): DataFrame {
    return this.compareOp(other, (a, b) => a > b);
  }

  ge(other: CellValue | DataFrame): DataFrame {
    return this.compareOp(other, (a, b) => a >= b);
  }

  filter(mask: boolean[] | ((row: Row, index: IndexLabel, position: number) => boolean)): DataFrame {
    if (Array.isArray(mask)) {
      if (mask.length !== this._rows.length) {
        throw new Error("Mask length must match row count.");
      }
      // Fast path for the common boolean-array form: compress indices
      // in wasm, then gather rows in one pass.
      const wasmIndices = computeWasmFilterPositions(mask);
      if (wasmIndices !== null) {
        const rows: Row[] = new Array(wasmIndices.length);
        const index: IndexLabel[] = new Array(wasmIndices.length);
        for (let i = 0; i < wasmIndices.length; i += 1) {
          const pos = wasmIndices[i] as number;
          rows[i] = this._rows[pos]!;
          index[i] = this._index[pos]!;
        }
        return this.withRows(rows, index, this._columns, true);
      }
      const rows: Row[] = [];
      const index: IndexLabel[] = [];
      for (let i = 0; i < mask.length; i += 1) {
        if (mask[i]) {
          rows.push(this._rows[i]!);
          index.push(this._index[i]!);
        }
      }
      return this.withRows(rows, index, this._columns, true);
    }

    {
      const rows: Row[] = [];
      const index: IndexLabel[] = [];
      for (let i = 0; i < this._rows.length; i += 1) {
        const row = this._rows[i]!;
        const label = this._index[i]!;
        if (mask(row, label, i)) {
          rows.push(row);
          index.push(label);
        }
      }
      return this.withRows(rows, index, this._columns, true);
    }
  }


  query(predicate: (row: Row, index: IndexLabel, position: number) => boolean): DataFrame {
    return this.filter(predicate);
  }

  // ---- iteration ----

  /** Returns [index, row] pairs like pandas `DataFrame.iterrows()`. */
  iterrows(): IterableIterator<[IndexLabel, Row]> {
    return this.iterrowsGenerator();
  }

  private *iterrowsGenerator(): Generator<[IndexLabel, Row]> {
    for (let i = 0; i < this._rows.length; i += 1) {
      yield [this._index[i]!, cloneRow(this._rows[i]!, this._columns)];
    }
  }

  /**
   * Returns named-tuple-like objects per row like pandas
   * `DataFrame.itertuples()`: `{ Index, <col1>, <col2>, ... }`.
   */
  itertuples(): IterableIterator<Record<string, CellValue | IndexLabel>> {
    return this.itertuplesGenerator();
  }

  private *itertuplesGenerator(): Generator<Record<string, CellValue | IndexLabel>> {
    for (let i = 0; i < this._rows.length; i += 1) {
      const tuple: Record<string, CellValue | IndexLabel> = { Index: this._index[i]! };
      const row = this._rows[i]!;
      for (const column of this._columns) {
        tuple[column] = row[column];
      }
      yield tuple;
    }
  }

  /** Returns [columnName, Series] pairs like pandas `DataFrame.items()`. */
  items(): IterableIterator<[string, Series<CellValue>]> {
    return this.itemsGenerator();
  }

  private *itemsGenerator(): Generator<[string, Series<CellValue>]> {
    for (const column of this._columns) {
      yield [column, this.columnSeries(column)];
    }
  }

  /**
   * Positional cell accessor like pandas `DataFrame.iat`, used as
   * `df.iat[rowPosition][columnPosition]`. Read-only Proxy view over the
   * frame; out-of-range positions yield undefined.
   */
  get iat(): Record<number, Record<number, CellValue>> {
    const self = this;
    return new Proxy({} as Record<number, Record<number, CellValue>>, {
      get(_target, rowProp: string | symbol) {
        const rowPosition = resolvePosition(Number(rowProp), self._rows.length);
        if (rowPosition === undefined) {
          // pandas iat[oor] returns a row of NaN/None rather than throwing on
          // the outer lookup; mirror with an always-undefined inner proxy.
          return new Proxy({} as Record<number, CellValue>, {
            get() {
              return undefined;
            },
          });
        }
        return new Proxy({} as Record<number, CellValue>, {
          get(_innerTarget, colProp: string | symbol) {
            const colPosition = resolvePosition(Number(colProp), self._columns.length);
            if (colPosition === undefined) {
              return undefined;
            }
            return self._rows[rowPosition]?.[self._columns[colPosition]!];
          },
        });
      },
    });
  }

  /** Axis labels as `[indexLabels, columnNames]`, matching pandas axes. */
  get axes(): [IndexLabel[], string[]] {
    return [[...this._index], [...this._columns]];
  }

  /** Free-form metadata dict, matching pandas DataFrame.attrs. */
  get attrs(): Record<string, unknown> {
    if (!this._attrs) {
      this._attrs = {};
    }
    return this._attrs;
  }

  set attrs(value: Record<string, unknown>) {
    this._attrs = value ?? {};
  }

  /**
   * Cross-section selector. A key naming a column returns that column as a
   * Series (pandas `xs(key, axis=1)`); a key naming an index label returns
   * that row as a record; anything else raises.
   */
  xs(key: IndexLabel): Series<CellValue> | Row {
    if (this._columns.includes(key as string)) {
      return this.columnSeries(key as string);
    }
    const rowPosition = this._index.findIndex((label) => label === key);
    if (rowPosition >= 0) {
      return cloneRow(this._rows[rowPosition]!, this._columns);
    }
    throw new Error(`Key '${String(key)}' not found in columns or index.`);
  }

  /**
   * In-place cell merge like pandas `DataFrame.update(other)`: where index
   * labels and columns align, non-missing values from `other` overwrite
   * this frame's cells in place.
   */
  update(other: DataFrame): void {
    applyUpdateOverwrite(
      this._rows,
      this._index,
      this._columns,
      other._rows,
      other._index,
      other._columns
    );
  }

  /**
   * Blows list-valued cells of `column` out into one row per element,
   * repeating every other column's values and the original index label.
   * Empty lists drop out; scalar cells pass through unchanged.
   */
  explode(column: string): DataFrame {
    this.assertColumnExists(column);
    const { rows, index } = computeExplodeRows(
      this._rows,
      this._columns,
      column,
      this._index
    );
    return this.withRows(rows, index, this._columns, true);
  }

  /**
   * Matrix product with another frame: self must have as many columns as
   * `other` has rows. Result keeps self's index and adopts other's columns;
   * cells touching missing/non-numeric operands become null.
   */
  dot(other: DataFrame): DataFrame {
    const grid = computeDotMatrix(
      this._rows,
      this._columns,
      other._rows,
      other._columns
    );
    const rows: Row[] = grid.map((line) => {
      const row: Row = {};
      other._columns.forEach((column, k) => {
        row[column] = line[k]!;
      });
      return row;
    });
    return DataFrame.from_normalized(rows, other.columns, this.index);
  }

  /**
   * Evaluates a simple column expression such as `"a + b"` or `"a > 2"`
   * safely — tokenized and compiled by hand, no `eval()` involved.
   * Returns a Series aligned to this frame's index; arithmetic/comparison
   * touching a missing cell yields null.
   */
  eval(expr: string): Series<CellValue> {
    const compiled = compileFrameExpression(expr);
    for (const column of compiled.referencedColumns) {
      this.assertColumnExists(column);
    }
    const values = this._rows.map((row) => compiled.evaluate(row));
    return new Series(values, { index: this.index, name: expr });
  }

  private columnSeries(column: string): Series<CellValue> {
    return new Series(
      this._rows.map((row) => row[column]),
      {
        index: [...this._index],
        name: column,
      }
    );
  }

  // ---- shifts and deltas ----

  /**
   * Shifts values by `periods` rows. Positive shifts move values down
   * (introducing nulls at the top), matching pandas.
   */
  shift(periods = 1): DataFrame {
    const rows = shiftRows(this._rows, this._columns, periods);
    return this.withRows(rows, [...this._index], this._columns, true);
  }

  /** First discrete difference of numeric columns. */
  diff(periods = 1): DataFrame {
    const rows = diffRows(this._rows, this._columns, periods);
    return this.withRows(rows, [...this._index], this._columns, true);
  }

  /** Percentage change between the current and prior row (numeric columns). */
  pct_change(periods = 1): DataFrame {
    const rows = pctChangeRows(this._rows, this._columns, periods);
    return this.withRows(rows, [...this._index], this._columns, true);
  }

  // ---- window functions ----

  private applyWindow(
    values: CellValue[],
    result: CellValue[]
  ): void {
    void values;
    void result;
  }

  /**
   * Rolling window aggregations over numeric columns:
   * `df.rolling(7).mean()`. Non-numeric columns are dropped.
   */
  rolling(window: number, minPeriods?: number): {
    mean(): DataFrame;
    sum(): DataFrame;
    min(): DataFrame;
    max(): DataFrame;
    count(): DataFrame;
    std(): DataFrame;
    median(): DataFrame;
    aggregate(aggregator: (values: number[]) => number | null): DataFrame;
  } {
    const numericCols = this._columns.filter((column) =>
      this._rows.some((row) => typeof row[column] === "number" && Number.isFinite(row[column]))
    );
    const columnsByResult = new Map<string, Map<string, CellValue[]>>();
    for (const column of numericCols) {
      const rolling = computeRolling(
        this._rows.map((row) => row[column]),
        window,
        minPeriods
      );
      for (const [name, fn] of Object.entries(rolling)) {
        if (typeof fn !== "function") continue;
        const result = (rolling as unknown as Record<string, () => CellValue[]>)[name]!();
        if (!columnsByResult.has(name)) {
          columnsByResult.set(name, new Map());
        }
        columnsByResult.get(name)!.set(column, result);
      }
    }

    const build = (name: string): DataFrame => {
      const perColumn = columnsByResult.get(name)!;
      const rows: Row[] = this._rows.map(() => ({}));
      for (const [column, values] of perColumn) {
        values.forEach((value, i) => {
          rows[i]![column] = value;
        });
      }
      return this.withRows(rows, [...this._index], numericCols, true);
    };

    return {
      mean: () => build("mean"),
      sum: () => build("sum"),
      min: () => build("min"),
      max: () => build("max"),
      count: () => build("count"),
      std: () => build("std"),
      median: () => build("median"),
      aggregate: (aggregator) => {
        const rows: Row[] = this._rows.map(() => ({}));
        for (const column of numericCols) {
          const _rolling = computeRolling(
            this._rows.map((row) => row[column]),
            window,
            minPeriods
          );
          // Reuse the sum windows as raw slices via count/mean trick is
          // lossy; instead recompute slices directly.
          let index = 0;
          for (let i = 0; i < this._rows.length; i += 1) {
            const slice: number[] = [];
            for (let j = Math.max(0, i - window + 1); j <= i; j += 1) {
              const v = this._rows[j]![column];
              if (typeof v === "number" && Number.isFinite(v)) {
                slice.push(v);
              }
            }
            const min = minPeriods ?? window;
            rows[i]![column] = slice.length >= min ? aggregator(slice) : null;
            index += 1;
          }
          void index;
        }
        return this.withRows(rows, [...this._index], numericCols, true);
      },
    };
  }

  /** Expanding (cumulative from start) aggregations over numeric columns. */
  expanding(minPeriods = 1): {
    mean(): DataFrame;
    sum(): DataFrame;
    min(): DataFrame;
    max(): DataFrame;
    count(): DataFrame;
    std(): DataFrame;
    median(): DataFrame;
  } {
    const numericCols = this._columns.filter((column) =>
      this._rows.some((row) => typeof row[column] === "number" && Number.isFinite(row[column]))
    );
    const results = new Map<string, Map<string, CellValue[]>>();
    for (const column of numericCols) {
      const expanding = computeExpanding(
        this._rows.map((row) => row[column]),
        minPeriods
      );
      for (const [name, fn] of Object.entries(expanding)) {
        if (typeof fn !== "function") continue;
        const result = (expanding as unknown as Record<string, () => CellValue[]>)[name]!();
        if (!results.has(name)) {
          results.set(name, new Map());
        }
        results.get(name)!.set(column, result);
      }
    }

    const build = (name: string): DataFrame => {
      const perColumn = results.get(name)!;
      const rows: Row[] = this._rows.map(() => ({}));
      for (const [column, values] of perColumn) {
        values.forEach((value, i) => {
          rows[i]![column] = value;
        });
      }
      return this.withRows(rows, [...this._index], numericCols, true);
    };

    return {
      mean: () => build("mean"),
      sum: () => build("sum"),
      min: () => build("min"),
      max: () => build("max"),
      count: () => build("count"),
      std: () => build("std"),
      median: () => build("median"),
    };
  }

  /** Inverse of isna. */
  notna(): DataFrame {
    return this.isna().mapValues((value) => !value);
  }

  private mapValues(fn: (value: CellValue) => CellValue): DataFrame {
    const rows = this._rows.map((row) => {
      const next: Row = {};
      for (const column of this._columns) {
        next[column] = fn(row[column]);
      }
      return next;
    });
    return this.withRows(rows, [...this._index], this._columns, true);
  }

  /**
   * Unpivots the frame from wide to long format. Columns listed in
   * `id_vars` are repeated; every other column (or those in
   * `value_vars`) becomes one (`variable`, `value`) pair per row.
   */
  melt(options: { id_vars?: string | string[]; value_vars?: string | string[] } = {}): DataFrame {
    const idVarsRaw = options.id_vars ?? [];
    const idVars = Array.isArray(idVarsRaw) ? idVarsRaw : [idVarsRaw];
    for (const id of idVars) {
      this.assertColumnExists(id);
    }

    let valueVars: string[];
    if (options.value_vars !== undefined) {
      valueVars = Array.isArray(options.value_vars) ? options.value_vars : [options.value_vars];
      for (const col of valueVars) {
        this.assertColumnExists(col);
      }
    } else {
      valueVars = this._columns.filter((column) => !idVars.includes(column));
    }

    const melted = computeMeltRows(this._rows, idVars, valueVars);
    return this.withRows(melted.rows, range(melted.rows.length), melted.columns, true);
  }

  /**
   * Pivots the frame from long to wide. Rows are grouped by the
   * `index` column, spread by `columns`, and filled from `values`.
   */
  pivot(
    index: string,
    columns: string,
    values: string,
    options: { aggregate?: (values: CellValue[]) => CellValue } = {}
  ): DataFrame {
    for (const column of [index, columns, values]) {
      this.assertColumnExists(column);
    }
    const pivoted = computePivot(this._rows, index, columns, values, options.aggregate);
    return new DataFrame(pivoted.rows, { index: pivoted.index as IndexLabel[] });
  }

  /**
   * Transposes the frame: columns become rows and vice versa. Column
   * labels become the new index as strings.
   */
  transpose(): DataFrame {
    const shaped = transposeFrame(this._rows, this._columns, this._index);
    return DataFrame.createInternal(shaped.rows, shaped.columns, shaped.index);
  }

  /**
   * Keeps only columns whose inferred dtype matches any of the given
   * ones (`"number"`, `"string"`, `"boolean"`, `"date"`).
   */
  select_dtypes(include: DType | DType[]): DataFrame {
    const keep = selectDtypeColumns(this._rows, this._columns, include);
    if (keep.length === 0) {
      throw new Error("select_dtypes matched no columns.");
    }
    return this.withRows(projectColumns(this._rows, keep), [...this._index], keep, true);
  }

  apply(
    fn: DataFrameApplyColumnFn,
    axis?: 0 | "index"
  ): Series<CellValue>;
  apply(
    fn: DataFrameApplyRowFn,
    axis: 1 | "columns"
  ): Series<CellValue>;
  apply(
    fn: DataFrameApplyColumnFn | DataFrameApplyRowFn,
    axis: DataFrameApplyAxis = 0
  ): Series<CellValue> {
    const normalizedAxis = normalizeApplyAxis(axis);
    if (normalizedAxis === 0) {
      return runApplyOnColumns(
        this._rows,
        this._columns,
        this._index,
        fn as DataFrameApplyColumnFn
      );
    }
    return runApplyOnRows(
      this._rows,
      this._columns,
      this._index,
      fn as DataFrameApplyRowFn
    );
  }

  applymap(fn: DataFrameMapFn): DataFrame {
    const rows = runApplyMap(
      this._rows,
      this._columns,
      this._index,
      fn
    );
    return this.withRows(rows, this._index, this._columns, true);
  }

  map(fn: DataFrameMapFn): DataFrame {
    return this.applymap(fn);
  }

  where(cond: WhereCondition, other: WhereOther = null): DataFrame {
    const rows = computeWhereRows(this._rows, this._columns, this._index, cond, other, false);
    return this.withRows(rows, this._index, this._columns, true);
  }

  mask(cond: WhereCondition, other: WhereOther = null): DataFrame {
    const rows = computeWhereRows(this._rows, this._columns, this._index, cond, other, true);
    return this.withRows(rows, this._index, this._columns, true);
  }

  transform(input: TransformInput): DataFrame {
    const rows = computeTransformRows(this._rows, this._columns, this._index, input);
    const columns =
      typeof input === "function" ? this._columns : Object.keys(input);
    if (typeof input !== "function") {
      for (const column of columns) {
        this.assertColumnExists(column);
      }
    }
    return this.withRows(rows, this._index, columns, true);
  }

  isin(values: CellValue[] | Record<string, CellValue[]>): DataFrame {
    const rows = computeIsinRows(this._rows, this._columns, values);
    return this.withRows(rows, this._index, this._columns, true);
  }

  clip(
    lower?: number,
    upper?: number,
    columns?: string | string[]
  ): DataFrame {
    if (lower !== undefined && upper !== undefined && lower > upper) {
      throw new Error("clip lower bound cannot exceed upper bound.");
    }
    const targetColumns = columns
      ? (Array.isArray(columns) ? columns : [columns])
      : this._columns;
    for (const column of targetColumns) {
      this.assertColumnExists(column);
    }
    const rows = computeClipRows(
      this._rows,
      this._columns,
      lower,
      upper,
      new Set(targetColumns)
    );
    return this.withRows(rows, this._index, this._columns, true);
  }

  replace(toReplace: ReplaceInput, value?: CellValue): DataFrame {
    const rows = computeReplaceRows(this._rows, this._columns, toReplace, value);
    return this.withRows(rows, this._index, this._columns, true);
  }

  sample(n = 1, options: SampleOptions = {}): DataFrame {
    const replace = options.replace ?? false;
    const ignoreIndex = options.ignore_index ?? false;

    let sampleSize = n;
    if (options.frac !== undefined) {
      if (options.frac < 0) {
        throw new Error("sample frac must be non-negative.");
      }
      sampleSize = Math.round(options.frac * this._rows.length);
    }

    if (!replace && sampleSize > this._rows.length) {
      throw new Error("sample size cannot exceed row count when replace=false.");
    }
    if (!Number.isInteger(sampleSize) || sampleSize < 0) {
      throw new Error("sample size must be a non-negative integer.");
    }

    const positions = samplePositions(
      this._rows.length,
      sampleSize,
      replace,
      options.random_state
    );
    return this.withRows(
      positions.map((position) => this._rows[position]!),
      ignoreIndex ? undefined : positions.map((position) => this._index[position]!),
      this._columns,
      true
    );
  }

  rank(options: RankOptions = {}): DataFrame {
    const rows = computeRankRows(this._rows, this._columns, options);
    return this.withRows(rows, this._index, this._columns, true);
  }

  sort_values(
    by: string | string[],
    ascending: boolean | boolean[] = true,
    limit?: number,
    na_position: "first" | "last" = "last"
  ): DataFrame {
    const columns = Array.isArray(by) ? by : [by];
    for (const column of columns) {
      this.assertColumnExists(column);
    }
    if (na_position !== "first" && na_position !== "last") {
      throw new Error("na_position must be 'first' or 'last'.");
    }

    const ascendingPerColumn = normalizeSortAscending(columns.length, ascending);
    // Single-column numeric sort: delegate to the wasm argsort kernel
    // (NaN-last by default, stable, ~2x faster at 25k rows).
    if (
      columns.length === 1 &&
      na_position === "last" &&
      isNumericColumn(this._rows, columns[0]!)
    ) {
      const wasmPos = computeWasmSortPositions(this._rows, columns[0]!, ascendingPerColumn[0]!);
      if (wasmPos) {
        const sliced =
          limit !== undefined
            ? wasmPos.slice(
                0,
                normalizeSortLimit(limit, this._rows.length) ?? wasmPos.length
              )
            : wasmPos;
        if (sliced.length === 0) {
          return this.withRows([], [], this._columns, true);
        }
        return this.withRows(
          Array.from(sliced, (position) => this._rows[position as number]!),
          Array.from(sliced, (position) => this._index[position as number]!),
          this._columns,
          true
        );
      }
    }

    const comparers = columns.map((column, i) =>
      buildColumnComparer(this._rows, column, ascendingPerColumn[i]!, na_position)
    );
    const normalizedLimit = normalizeSortLimit(limit, this._rows.length);
    if (normalizedLimit === 0) {
      return this.withRows([], [], this._columns, true);
    }

    const positions =
      normalizedLimit !== undefined && normalizedLimit < this._rows.length
        ? selectTopKPositions(this._rows, comparers, normalizedLimit)
        : fullSortPositions(this._rows, comparers);

    return this.withRows(
      positions.map((position) => this._rows[position]!),
      positions.map((position) => this._index[position]!),
      this._columns,
      true
    );
  }

  sort_index(ascending = true): DataFrame {
    const positions = range(this._rows.length);
    positions.sort((leftPosition, rightPosition) => {
      const compared = compareCellValues(this._index[leftPosition], this._index[rightPosition]);
      return ascending ? compared : -compared;
    });

    return this.withRows(
      positions.map((position) => this._rows[position]!),
      positions.map((position) => this._index[position]!),
      this._columns,
      true
    );
  }

  drop_duplicates(
    subset?: string | string[],
    keep: DropDuplicatesKeep = "first",
    ignore_index = false
  ): DataFrame {
    const include = computeDuplicateKeepFlags(this._rows, subset ? (Array.isArray(subset) ? subset : [subset]) : this._columns, keep);

    const rows: Row[] = [];
    const index: IndexLabel[] = [];
    for (let i = 0; i < this._rows.length; i += 1) {
      if (!include[i]) {
        continue;
      }
      rows.push(this._rows[i]!);
      index.push(this._index[i]!);
    }

    return this.withRows(rows, ignore_index ? undefined : index, this._columns, true);
  }

  duplicated(subset?: string | string[], keep: DropDuplicatesKeep = "first"): Series<boolean> {
    const include = computeDuplicateKeepFlags(this._rows, subset ? (Array.isArray(subset) ? subset : [subset]) : this._columns, keep);
    return new Series(
      include.map((flag) => !flag),
      { name: "duplicated", index: this._index }
    );
  }

  value_counts(options: ValueCountsOptions = {}): DataFrame {
    const subset = options.subset
      ? (Array.isArray(options.subset) ? options.subset : [options.subset])
      : this._columns;
    const normalize = options.normalize ?? false;
    const dropna = options.dropna ?? true;
    const sort = options.sort ?? true;
    const ascending = options.ascending ?? false;
    const limit = normalizeSortLimit(options.limit, Number.MAX_SAFE_INTEGER);

    for (const column of subset) {
      this.assertColumnExists(column);
    }

    const counts = computeValueCountsRows(this._rows, {
      subset,
      normalize,
      dropna,
      sort,
      ascending,
      limit,
    });

    return this.withRows(counts.rows, undefined, [...subset, counts.valueColumnName], true);
  }

  dropna(subset?: string[]): DataFrame {
    const columns = subset && subset.length > 0 ? subset : this._columns;
    for (const column of columns) {
      this.assertColumnExists(column);
    }

    return this.filter((row) => columns.every((column) => !isMissing(row[column])));
  }

  fillna(value: CellValue | Record<string, CellValue>): DataFrame {
    return this.withRows(fillnaRows(this._rows, this._columns, value), this._index, this._columns, true);
  }

  set_index(column: string, drop = true): DataFrame {
    this.assertColumnExists(column);
    const index = indexLabelsFromColumn(this._rows, column, this._index);

    if (!drop) {
      return this.withRows(this.to_records(), index, this._columns, true);
    }
    return this.drop(column).withIndex(index);
  }

  reset_index(name = "index"): DataFrame {
    if (this._columns.includes(name)) {
      throw new Error(`Column '${name}' already exists.`);
    }
    const rows = resetIndexRows(this._rows, this._columns, this._index, name);
    return new DataFrame(rows, { columns: [name, ...this._columns] });
  }

  sum(): Record<string, number | null> {
    return columnSum(this._columns, this._rows);
  }

  mean(): Record<string, number | null> {
    return columnMean(this._columns, this._rows);
  }

  median(): Record<string, number | null> {
    return columnMedian(this._columns, this._rows);
  }

  std(): Record<string, number | null> {
    return columnStd(this._columns, this._rows);
  }

  var(): Record<string, number | null> {
    return columnVariance(this._columns, this._rows);
  }

  min(): Record<string, number | null> {
    return columnMin(this._columns, this._rows);
  }

  max(): Record<string, number | null> {
    return columnMax(this._columns, this._rows);
  }

  /** Product of numeric column values (pandas prod/product). */
  prod(): Record<string, number | null> {
    return columnProd(this._columns, this._rows);
  }

  product(): Record<string, number | null> {
    return this.prod();
  }

  /**
   * Quantile of each numeric column. `q` in [0, 1] (default 0.5 = median),
   * or an array of quantiles to get a frame indexed by q.
   */
  quantile(q: number | number[] = 0.5): Record<string, number | null> | DataFrame {
    if (!Array.isArray(q)) {
      return columnQuantile(this._columns, this._rows, q);
    }
    const rows: Row[] = [];
    const index: IndexLabel[] = [];
    for (const qValue of q) {
      const record: Row = { q: qValue };
      for (const [column, value] of Object.entries(columnQuantile(this._columns, this._rows, qValue))) {
        if (column !== "q") record[column] = value;
      }
      rows.push(record);
      index.push(qValue);
    }
    return DataFrame.createInternal(rows, ["q", ...this._columns], index);
  }

  /** Pearson correlation between numeric column pairs. */
  corr(): DataFrame {
    return this.pairwiseNumeric(pearson);
  }

  /** Covariance between numeric column pairs. */
  cov(): DataFrame {
    return this.pairwiseNumeric(covariance);
  }

  private pairwiseNumeric(fn: (a: number[], b: number[]) => number): DataFrame {
    const result = pairwiseNumericMatrix(this._columns, this._rows, fn);
    return DataFrame.createInternal(result.rows, result.columns, result.index);
  }

  count(): Record<string, number> {
    return columnCount(this._columns, this._rows);
  }

  round(decimals = 0): DataFrame {
    const factor = 10 ** decimals;
    const rows = this._rows.map((row) => {
      const next = cloneRow(row, this._columns);
      for (const column of this._columns) {
        const value = next[column];
        if (isNumber(value)) {
          next[column] = Math.round((value + Number.EPSILON) * factor) / factor;
        }
      }
      return next;
    });
    return this.withRows(rows, this._index, this._columns, true);
  }

  abs(columns?: string | string[]): DataFrame {
    const targets = new Set(this.resolveTargetColumns(columns));
    const rows = this._rows.map((row) => {
      const next = cloneRow(row, this._columns);
      for (const column of targets) {
        const value = row[column];
        if (isNumber(value)) {
          next[column] = Math.abs(value);
        }
      }
      return next;
    });
    return this.withRows(rows, this._index, this._columns, true);
  }

  cumsum(columns?: string | string[]): DataFrame {
    const targets = this.resolveTargetColumns(columns);
    const running = new Map<string, number>();
    const rows = this._rows.map((row) => {
      const next = cloneRow(row, this._columns);
      for (const column of targets) {
        const value = row[column];
        if (!isNumber(value)) {
          next[column] = null;
          continue;
        }
        const total = (running.get(column) ?? 0) + value;
        running.set(column, total);
        next[column] = total;
      }
      return next;
    });
    return this.withRows(rows, this._index, this._columns, true);
  }

  equals(other: DataFrame): boolean {
    if (JSON.stringify(this._columns) !== JSON.stringify(other._columns)) {
      return false;
    }
    if (this._rows.length !== other._rows.length) {
      return false;
    }
    for (let i = 0; i < this._rows.length; i += 1) {
      if (this._index[i] !== other._index[i]) {
        return false;
      }
      for (const column of this._columns) {
        if (
          JSON.stringify(normalizeKeyCell(this._rows[i]![column])) !==
          JSON.stringify(normalizeKeyCell(other._rows[i]![column]))
        ) {
          return false;
        }
      }
    }
    return true;
  }

  describe(): DataFrame {
    const { rows, numericColumns } = describeStatRows(this._columns, this._rows);
    return new DataFrame(rows, {
      columns: ["stat", ...numericColumns],
      index: rows.map((row) => row.stat as string),
    });
  }

  groupby(by: string | string[], options: GroupByOptions = {}): GroupBy {
    return new GroupBy(this, Array.isArray(by) ? by : [by], this._rows, this._columns, options);
  }

  pivot_table(options: PivotTableOptions): DataFrame {
    const index = Array.isArray(options.index) ? options.index : [options.index];
    const values = Array.isArray(options.values) ? options.values : [options.values];
    const columns = options.columns;

    for (const indexColumn of index) {
      this.assertColumnExists(indexColumn);
    }
    for (const valueColumn of values) {
      this.assertColumnExists(valueColumn);
    }
    if (columns) {
      this.assertColumnExists(columns);
    }

    const result = computePivotTable({
      sourceRows: this._rows,
      index,
      values,
      columns,
      aggfunc: options.aggfunc ?? "mean",
      fillValue: options.fill_value,
      margins: options.margins ?? false,
      marginsName: options.margins_name ?? "All",
      dropna: options.dropna ?? true,
      sort: options.sort ?? true,
    });

    return new DataFrame(result.rows, { columns: result.columns });
  }

  merge(right: DataFrame, options: MergeOptions): DataFrame {
    const keys = Array.isArray(options.on) ? options.on : [options.on];

    for (const key of keys) {
      this.assertColumnExists(key);
      right.assertColumnExists(key);
    }

    const result = computeMergeRows({
      leftRows: this._rows,
      rightRows: right._rows,
      leftColumns: this._columns,
      rightColumns: right._columns,
      keys,
      how: options.how ?? "inner",
      suffixes: options.suffixes ?? ["_x", "_y"],
    });

    return new DataFrame(result.rows, { columns: result.columns });
  }

  /**
   * pandas-style `join`: merges on the caller's index against the
   * other frame's index (or `on` column). Convenience wrapper over
   * `merge` with index-based keying. The result keeps this frame's
   * row order; for `on` joins the key column appears once (unsuffixed).
   */
  join(right: DataFrame, options: JoinOptions = {}): DataFrame {
    return performJoin(
      {
        rows: () => this._rows,
        columns: () => this._columns,
        index: () => this._index,
        assertColumnExists: (column) => this.assertColumnExists(column),
        withKeyColumn: (keyColumn, sourceColumn, labels) =>
          this.withKeyColumn(keyColumn, sourceColumn, labels),
        withRows: (rows, index, columns, normalized) =>
          this.withRows(rows, index, columns, normalized ?? false),
      },
      right,
      options
    );
  }

  private withKeyColumn(
    keyColumn: string,
    sourceColumn?: string,
    labels?: IndexLabel[]
  ): DataFrame {
    const rows = this._rows.map((row, i) => {
      const next = cloneRow(row, this._columns);
      next[keyColumn] =
        sourceColumn !== undefined ? row[sourceColumn] : labels ? labels[i] : this._index[i];
      return next;
    });
    return DataFrame.createInternal(rows, [...this._columns, keyColumn], [...this._index]);
  }

  to_string(maxRows = 10): string {
    const [rowCount] = this.shape;
    const rows = this.head(maxRows).to_records();
    const preview = rows.map((row) => JSON.stringify(row)).join("\n");
    const suffix = rowCount > maxRows ? `\n... (${rowCount - maxRows} more rows)` : "";
    return preview + suffix;
  }

  // ---- missing-data fills ----
  ffill(): DataFrame {
    return this.withRows(ffillRows(this._rows, this._columns), [...this._index], this._columns, true);
  }

  bfill(): DataFrame {
    return this.withRows(bfillRows(this._rows, this._columns), [...this._index], this._columns, true);
  }

  interpolate(method = "linear"): DataFrame {
    return this.withRows(interpolateRows(this._rows, this._columns, method), [...this._index], this._columns, true);
  }

  // ---- extended stats ----
  skew(): Record<string, number | null> {
    return columnSkew(this._columns, this._rows);
  }

  kurt(): Record<string, number | null> {
    return columnKurt(this._columns, this._rows);
  }

  kurtosis(): Record<string, number | null> {
    return this.kurt();
  }

  sem(): Record<string, number | null> {
    return columnSem(this._columns, this._rows);
  }

  mode(): DataFrame {
    const result = modeRows(this._rows, this._columns);
    return this.withRows(result.rows, undefined, result.columns, true);
  }

  // ---- cumulative ----
  cummax(): DataFrame {
    return this.withRows(cummaxRows(this._rows, this._columns), [...this._index], this._columns, true);
  }

  cummin(): DataFrame {
    return this.withRows(cumminRows(this._rows, this._columns), [...this._index], this._columns, true);
  }

  cumprod(): DataFrame {
    return this.withRows(cumprodRows(this._rows, this._columns), [...this._index], this._columns, true);
  }

  // ---- export ----
  to_numpy(): CellValue[][] {
    return this.values();
  }

  to_html(): string {
    return toHtmlString(this._rows, this._columns, this._index);
  }

  to_markdown(): string {
    return toMarkdownString(this._rows, this._columns, this._index);
  }

  // ---- reshaping / selection ----
  squeeze(): DataFrame | Series<CellValue> | CellValue | null {
    const result = squeezeResult(this._rows, this._columns, this._index);
    if (result.kind === "scalar") return result.value;
    if (result.kind === "column") return new Series(result.values, { index: result.index, name: result.name });
    if (result.kind === "row") return new Series(result.values, { index: result.index });
    return this.withRows(result.rows, result.index, result.columns, true);
  }

  take(indices: number[]): DataFrame {
    const result = takeRowsWithIndex(this._rows, this._columns, this._index, indices);
    return this.withRows(result.rows, result.index, this._columns, true);
  }

  truncate(beforeOrOptions?: IndexLabel | { before?: IndexLabel; after?: IndexLabel }, after?: IndexLabel): DataFrame {
    let before: IndexLabel | undefined;
    let afterLabel: IndexLabel | undefined;
    if (
      beforeOrOptions !== undefined &&
      typeof beforeOrOptions === "object" &&
      !(beforeOrOptions instanceof Date) &&
      ("before" in (beforeOrOptions as Record<string, unknown>) || "after" in (beforeOrOptions as Record<string, unknown>))
    ) {
      const opts = beforeOrOptions as { before?: IndexLabel; after?: IndexLabel };
      before = opts.before;
      afterLabel = opts.after;
    } else {
      before = beforeOrOptions as IndexLabel | undefined;
      afterLabel = after;
    }
    const result = truncateRows(this._rows, this._columns, this._index, before, afterLabel);
    return this.withRows(result.rows, result.index, this._columns, true);
  }

  reindex(options: { index?: IndexLabel[]; columns?: string[]; fill_value?: CellValue } = {}): DataFrame {
    const result = reindexRows(
      this._rows,
      this._columns,
      this._index,
      options.index,
      options.columns,
      options.fill_value ?? null
    );
    return this.withRows(result.rows, result.index, result.columns, true);
  }

  info(): string {
    return frameInfo(this._rows, this._columns, this._index);
  }

  memory_usage(): Record<string, number> {
    return frameMemoryUsage(this._rows, this._columns);
  }

  // ---- additional aliases ----
  rfloordiv(other: number | DataFrame): DataFrame {
    return this.binaryOp(other, (a, b) => Math.floor(b / a));
  }

  rtruediv(other: number | DataFrame): DataFrame {
    return this.binaryOp(other, (a, b) => b / a);
  }

  agg(spec: import("./types").AggName | Record<string, import("./types").AggName | import("./types").AggFn>): Record<string, CellValue> {
    return aggFrame(this._rows, this._columns, spec as import("./types").AggName | Record<string, import("./types").AggName>);
  }

  aggregate(spec: import("./types").AggName | Record<string, import("./types").AggName | import("./types").AggFn>): Record<string, CellValue> {
    return this.agg(spec);
  }

  private resolveAssignment(column: string, value: AssignmentValue, rowCount: number): CellValue[] {
    if (value instanceof Series) {
      return this._index.map((label) => value.loc(label));
    }
    if (Array.isArray(value)) {
      if (value.length !== rowCount) {
        throw new Error(
          `Length mismatch for column '${column}'. Expected ${rowCount}, received ${value.length}.`
        );
      }
      return [...value];
    }
    return Array.from({ length: rowCount }, () => value);
  }


  private resolveTargetColumns(columns?: string | string[]): string[] {
    const targets = columns ? (Array.isArray(columns) ? columns : [columns]) : this._columns;
    for (const column of targets) {
      this.assertColumnExists(column);
    }
    return targets;
  }


  private assertColumnExists(column: string): void {
    if (!this._columns.includes(column)) {
      throw new Error(`Column '${column}' does not exist.`);
    }
  }

  private withRows(
    rows: Row[],
    index?: IndexLabel[],
    columns?: string[],
    rowsAreNormalized = false
  ): DataFrame {
    const nextColumns = columns ? [...columns] : [...this._columns];
    const nextRows = rowsAreNormalized
      ? rows
      : rows.map((row) => cloneRow(row, nextColumns));
    const nextIndex = index ? [...index] : range(nextRows.length);
    return DataFrame.createInternal(nextRows, nextColumns, nextIndex);
  }

  private withIndex(index: IndexLabel[]): DataFrame {
    return this.withRows(this.to_records(), index, this._columns, true);
  }

  // ---- combine / compare / dtype conversion ----

  /**
   * pandas-style `combine`: align both frames on the union of their
   * indexes/columns and build each column by calling `fn` with the two
   * aligned value arrays. A scalar return broadcasts across the column.
   */
  combine(other: DataFrame, fn: FrameCombineFn): DataFrame {
    const result = combineFrames(
      this._rows,
      this._columns,
      this._index,
      other._rows,
      other._columns,
      other._index,
      fn
    );
    return this.withRows(result.rows, result.index, result.columns, true);
  }

  /** pandas-style `combine_first`: first non-missing value per cell. */
  combine_first(other: DataFrame): DataFrame {
    const result = combineFirstFrames(
      this._rows,
      this._columns,
      this._index,
      other._rows,
      other._columns,
      other._index
    );
    return this.withRows(result.rows, result.index, result.columns, true);
  }

  /**
   * pandas-style `compare`: element-wise diff against an identically
   * labeled frame. Differing positions become rows with `<column>` /
   * `<column>_other` value pairs.
   */
  compare(other: DataFrame, options: DataFrameCompareOptions = {}): DataFrame {
    if (
      JSON.stringify(this._columns) !== JSON.stringify(other._columns) ||
      this._rows.length !== other._rows.length ||
      JSON.stringify(this._index) !== JSON.stringify(other._index)
    ) {
      throw new Error(
        "Can only compare identically-labeled (both index and columns) DataFrame objects."
      );
    }
    const result = compareFrames(this._rows, this._columns, this._index, other._rows, options);
    return this.withRows(result.rows, result.index, result.columns, true);
  }

  /**
   * pandas-style `convert_dtypes`: coerce numeric-looking and
   * boolean-looking strings to real numbers/booleans in every column.
   */
  convert_dtypes(): DataFrame {
    return this.withRows(
      convertDtypesRows(this._rows, this._columns),
      [...this._index],
      this._columns,
      true
    );
  }

  /**
   * pandas-style `infer_objects`: like `convert_dtypes`, but only mixed
   * ("object"-like) columns are converted; homogeneous columns pass through.
   */
  infer_objects(): DataFrame {
    return this.withRows(
      inferObjectsRows(this._rows, this._columns),
      [...this._index],
      this._columns,
      true
    );
  }

  /**
   * pandas-style `corrwith`: Pearson correlation between the shared numeric
   * columns of two frames, pairing values by matching index label.
   */
  corrwith(other: DataFrame): Record<string, number | null> {
    return corrwithValues(
      this._rows,
      this._columns,
      this._index,
      other._rows,
      other._columns,
      other._index
    );
  }

  /** pandas-style `reindex_like`: reindex to another frame's index+columns. */
  reindex_like(other: DataFrame): DataFrame {
    const result = reindexRows(
      this._rows,
      this._columns,
      this._index,
      [...other._index],
      [...other._columns],
      null
    );
    return this.withRows(result.rows, result.index, result.columns, true);
  }

  // ---- reshaping: stack / unstack ----

  /**
   * pandas-style `stack` (single-level columns): pivot each column into
   * long rows `{ index, column, value }`; missing cells dropped unless
   * `dropna` is false.
   */
  stack(dropna = true): DataFrame {
    const result = stackFrame(this._rows, this._columns, this._index, dropna);
    return this.withRows(result.rows, result.index, result.columns, true);
  }

  /**
   * Inverse of `stack` for single-level long frames: expects exactly three
   * columns `[row key, column key, value]`; distinct row keys become the
   * wide frame's index and distinct column keys its columns.
   */
  unstack(): DataFrame {
    const result = unstackFrame(this._rows, this._columns);
    return this.withRows(result.rows, result.index, result.columns, true);
  }

  // ---- export / identity shims ----

  /** Serializes the frame to JSON and returns it as a `Buffer`. */
  to_pickle(): Buffer {
    return Buffer.from(
      JSON.stringify({ columns: this._columns, index: this._index, rows: this.to_records() })
    );
  }

  /** Identity shim for period-conversion parity. */
  to_period(): DataFrame {
    return this.copy();
  }

  /** Identity shim for timestamp-conversion parity. */
  to_timestamp(): DataFrame {
    return this.copy();
  }

  /**
   * pandas-style `set_flags`: currently only validates
   * `allows_duplicate_labels`; always returns a copy of the frame.
   */
  set_flags(options: DataFrameFlagsOptions = {}): DataFrame {
    if (options.allows_duplicate_labels === false && hasDuplicateLabels(this._index)) {
      throw new Error("Initialization failed: index has duplicates.");
    }
    return this.copy();
  }

  // ---- wasm-backed helpers ----
  private view(): import("./internal/dataframe/windowApi").HostView {
    return {
      rowsSnapshot: () => this._rows,
      columnsSnapshot: () => this._columns,
      labels: () => this._index,
      withRows: (rows: Row[], index?: IndexLabel[], columns?: string[], normalized?: boolean) =>
        this.withRows(rows, index, columns, normalized),
      iloc: (position: number) => this.iloc(position),
      reindex: (options: { index?: IndexLabel[]; columns?: string[]; fill_value?: CellValue }) =>
        this.reindex(options),
      copy: () => this.copy(),
      to_html: () => this.to_html(),
    };
  }

  // ---- window / time / export parity (delegates to internal/dataframe/windowApi) ----

  align(other: DataFrame, join: "outer" | "inner" = "outer"): [DataFrame, DataFrame] {
    return windowApi.align(this.view(), other, join);
  }
  asfreq(freq: string): DataFrame {
    return windowApi.asfreq(this.view(), freq);
  }
  asof(where: IndexLabel): CellValue | Row {
    return windowApi.asof(this.view(), where);
  }
  at_time(time: string): DataFrame {
    return windowApi.at_time(this.view(), time);
  }
  between_time(start: string, end: string, inclusive: "both" | "neither" | "left" | "right" = "both"): DataFrame {
    return windowApi.between_time(this.view(), start, end, inclusive);
  }
  ewm(span: number, options: { min_periods?: number } = {}): ReturnType<typeof windowApi.ewm> {
    return windowApi.ewm(this.view(), span, options);
  }
  resample(rule: string): ReturnType<typeof windowApi.resample> {
    return windowApi.resample(this.view(), rule);
  }
  droplevel(level = 0): DataFrame {
    return windowApi.droplevel(this.view(), level);
  }
  reorder_levels(...order: number[]): DataFrame {
    return windowApi.reorder_levels(this.view(), ...order);
  }
  swaplevel(): DataFrame {
    return windowApi.swaplevel(this.view());
  }
  isetitem(position: number, value: CellValue[] | Series<CellValue>): DataFrame {
    return windowApi.isetitem(this.view(), position, value);
  }
  tz_localize(tz: string): DataFrame {
    return windowApi.tz_localize(this.view(), tz);
  }
  tz_convert(tz: string): DataFrame {
    return windowApi.tz_convert(this.view(), tz);
  }
  to_clipboard(sep = ","): string {
    return windowApi.to_clipboard(this.view(), sep);
  }
  to_feather(): Buffer {
    return windowApi.to_feather(this.view());
  }
  to_orc(): Buffer {
    return windowApi.to_orc(this.view());
  }
  to_hdf(key = "frame"): Buffer {
    return windowApi.to_hdf(this.view(), key);
  }
  to_stata(): string {
    return windowApi.to_stata(this.view());
  }
  to_sql(tableName: string): string {
    return windowApi.to_sql(this.view(), tableName);
  }
  to_xarray(): object {
    return windowApi.to_xarray(this.view());
  }
  to_latex(): string {
    return buildLatexTable(this._rows, this._columns, this._index);
  }
  get html(): string {
    return this.to_html();
  }
  /** Per-column density report (pandas sparse accessor). */
  get sparse(): Record<string, { density: number; filled: number; missing: number }> {
    const out: Record<string, { density: number; filled: number; missing: number }> = {};
    for (const c of this._columns) {
      out[c] = sparseInfo(this._rows.map((r) => r[c] as CellValue));
    }
    return out;
  }
  style(): never {
    throw new NotSupportedError("Styled rendering is not supported; use to_html().");
  }
  /** ASCII histogram for one numeric column. */
  hist(column: string, bins = 10): string {
    this.assertColumnExists(column);
    return asciiHistogram(this._rows.map((r) => r[column] as CellValue), bins);
  }
  /** ASCII boxplot for one numeric column. */
  boxplot(column: string, width = 50): string {
    this.assertColumnExists(column);
    return asciiBoxplot(this._rows.map((r) => r[column] as CellValue), width);
  }
  /** SVG line plot for one numeric column. */
  plot(column: string, height = 120): string {
    this.assertColumnExists(column);
    return svgLine(this._rows.map((r) => r[column] as CellValue), height);
  }
}

