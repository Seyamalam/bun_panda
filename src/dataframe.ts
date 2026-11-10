import { alignedPositions, compareIndexLabels, indexLabelEquals, indexLabelKey, indexLabelsEqual, unionIndexLabels } from "./internal/shared/indexing";
import { NA, NaT } from "./missing";
import { scalarEquals, numberOrBigintEqual, bigintOperand, bigintMod, bigintFloorDiv, jsonBigintReplacer, scalarKey } from "./internal/shared/valueSemantics";
import { arithResultDType, assertDType, castColumn, convertedDType, floordivResultDType, promoteDTypes, replacementDType, dtypeSelector, type DTypeSelector, scalarOperandDType, updatedDType, widenDTypeForMissing } from "./dtypes";
import { isDatetime64DType } from "./internal/shared/timezone";
import { roundToEven } from "./internal/shared/valueSemantics";
import { GroupBy } from "./groupby";
import {
  dataFrameToCsv,
  dataFrameToJson,
  dataFrameToParquet,
  dataFrameToExcel,
} from "./internal/dataframe/io";
import {
  columnAllAny,
  columnCount,
  columnKurtPandas,
  columnMax,
  columnMean,
  columnMedian,
  columnMin,
  columnNunique,
  columnProd,
  columnQuantile,
  columnSem as columnSemPandas,
  columnSkewPandas,
  columnStd,
  columnSum,
  columnVariance,
  columnIdx,
  covariance,
  pairwiseNumericMatrix,
  pearson,
} from "./internal/dataframe/stats";
import {
  duplicateKeepFlags as computeDuplicateKeepFlags,
  isNumericColumn,
  wasmFilterPositions as computeWasmFilterPositions,
  wasmSortPositions as computeWasmSortPositions,
} from "./internal/dataframe/selection";
import { performJoin } from "./internal/dataframe/join";
import {
  isMaskedDType,
  pandasMode,
  requireNumeric,
} from "./internal/series/pandasOps";
import {
  pandasAbs,
  pandasClip,
  pandasCumMinMax,
  pandasCumSumProd,
  pandasDiff,
  pandasPctChange,
} from "./internal/series/pandasElementwise";
import {
  shiftRows,
} from "./internal/dataframe/deltas";
import {
  projectColumns,
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
  frameInfo,
  frameMemoryUsage,
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
import { keyForColumns, normalizeKeyCell } from "./internal/dataframe/keys";
import {
  applyUpdateOverwrite,
  computeDotMatrix,
} from "./internal/dataframe/frameOps";
import { computeExplodeRows } from "./internal/dataframe/explode";
import { compileFrameExpression } from "./internal/dataframe/evalExpr";
import {
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
  corrwithValues,
  hasDuplicateLabels,
  stackFrame,
  unstackFrame,
  type FrameCombineFn,
} from "./internal/dataframe/combine";
import { IndexError, KeyError, NotSupportedError, ValueError } from "./errors";
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
  compareCellValues,
  isMissing,
  isNumber,
  pythonOperandTypeName,
  range,
  std,
} from "./utils";
import { invalidateColumnStore, primeNumericColumns } from "./wasm/columns";

export interface DataFrameOptions {
  dtype?: DType;
  index?: IndexLabel[];
  indexName?: string;
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
  private readonly _dtypes: Partial<Record<string, DType>>;
  private readonly _rows: Row[];
  private readonly _columns: string[];
  private readonly _index: IndexLabel[];
  /** pandas `df.index.name` for single-level indexes. */
  private readonly _indexName: string | undefined;
  /** pandas `df.index.names` — one per level; set for MultiIndex frames. */
  private readonly _indexNames: (string | undefined)[] | undefined;
  /** Dtype fallback used only when an output column has no present values. */
  private readonly _dtypeHints: Partial<Record<string, InferredDType>>;
  /** pandas-visible column names when storage keys differ (duplicate labels). */
  private _displayColumns: string[] | undefined;
  /** Free-form metadata dict, matching pandas DataFrame.attrs. */
  private _attrs: Record<string, unknown> = {};

  constructor(data: Row[] | Record<string, CellValue[]> = [], options: DataFrameOptions = {}) {
    const normalized = Array.isArray(data)
      ? normalizeRecords(data, options.columns)
      : normalizeColumnar(data);

    const prepared = DataFrame.prepareColumns(normalized.rows, normalized.columns,
      Object.fromEntries(normalized.columns.flatMap(column => options.dtype ? [[column, options.dtype]] : !Array.isArray(data) && data[column]?.length === 0 ? [[column, "float64" as DType]] : [])));
    if (options.dtype) assertDType(options.dtype);
    this._rows = prepared.rows;
    this._columns = normalized.columns;
    this._index = options.index ? [...options.index] : range(this._rows.length);
    this._indexName = options.indexName;
    this._dtypeHints = {};
    this._dtypes = prepared.dtypes;

    assertRowsShape(this._rows, this._index);
  }

  static createInternal(
    rows: Row[],
    columns: string[],
    index: IndexLabel[],
    dtypeHints: Partial<Record<string, InferredDType>> = {},
    dtypes: Partial<Record<string, DType>> = {},
    indexName?: string,
    indexNames?: (string | undefined)[],
    displayColumns?: string[]
  ): DataFrame {
    assertRowsShape(rows, index);
    const prepared = DataFrame.prepareColumns(rows, columns, dtypes, dtypeHints);
    const frame = Object.create(DataFrame.prototype) as DataFrame;
    (frame as unknown as { _rows: Row[] })._rows = prepared.rows;
    (frame as unknown as { _columns: string[] })._columns = columns;
    (frame as unknown as { _index: IndexLabel[] })._index = index;
    (frame as unknown as { _indexName: string | undefined })._indexName = indexName ?? indexNames?.[0];
    (frame as unknown as { _indexNames: (string | undefined)[] | undefined })._indexNames = indexNames;
    (frame as unknown as { _dtypeHints: Partial<Record<string, InferredDType>> })._dtypeHints = {
      ...dtypeHints,
    };
    (frame as unknown as { _dtypes: Partial<Record<string, DType>> })._dtypes = prepared.dtypes;
    frame._displayColumns = displayColumns;
    frame._attrs = {};
    return frame;
  }

  /** Establish physical missing values and dtype metadata together, before exposing a frame. */
  private static prepareColumns(rows: Row[], columns: string[], declared: Partial<Record<string, DType>>, hints: Partial<Record<string, InferredDType>> = {}): { rows: Row[]; dtypes: Record<string, DType> } {
    const result = rows.map(row => cloneRow(row, columns));
    const dtypes: Record<string, DType> = {};
    for (const column of columns) {
      const values = rows.map(row => row[column]);
      const inferred = convertedDType(values, undefined, false);
      const hint = hints[column];
      const fallback = hint && hint !== "unknown" && hint !== "mixed" ? replacementDType(hint === "number" ? "float64" : hint, values) : undefined;
      const dtype = declared[column] ?? (values.every(isMissing) ? fallback ?? inferred : inferred);
      const cast = castColumn(values, dtype);
      result.forEach((row, i) => { row[column] = cast[i]; });
      dtypes[column] = dtype;
    }
    return { rows: result, dtypes };
  }

  static from_records(records: Row[], options: DataFrameOptions = {}): DataFrame {
    return new DataFrame(records, options);
  }

  static from_arrow(records: Row[], options: DataFrameOptions = {}): DataFrame {
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

    const frame = new DataFrame(rows, options);
    primeNumericColumns(frame._rows, data);
    return frame;
  }

  get columns(): string[] {
    // _displayColumns carries pandas-visible names when duplicate column
    // labels (e.g. concat axis=1) are stored under unique storage keys.
    return [...(this._displayColumns ?? this._columns)];
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
    return [...(this._displayColumns ?? this._columns)];
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
    return DataFrame.createInternal(this.to_records(), [...this._columns], [...this._index], this._dtypeHints, this._dtypes);
  }

  to_records(): Row[] {
    return this._rows.map((row) => cloneRow(row, this._columns));
  }

  values(): CellValue[][] {
    return this._rows.map((row) => this._columns.map((column) => row[column]));
  }

  dtypes(): Record<string, InferredDType> {
    return Object.fromEntries(
      this._columns.map((column, i) => [(this._displayColumns?.[i] ?? column), this._dtypes[column]!])
    );
  }

  nunique(dropna = true): Record<string, number> {
    if (dropna) {
      return columnNunique(this._columns, this._rows);
    }
    const out: Record<string, number> = {};
    for (const column of this._columns) {
      const seen = new Set<string>();
      for (const row of this._rows) {
        const value = row[column];
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

    const rows = this.to_records();
    for (const [column, target] of Object.entries(castMap)) {
      const values = castColumn(this._rows.map(row => row[column]), target, this.dtypes()[column]);
      rows.forEach((row, i) => { row[column] = values[i]; });
    }
    if (typeof dtype === "string") assertDType(dtype);
    return DataFrame.createInternal(rows, [...this._columns], [...this._index], this._dtypeHints, { ...this._dtypes, ...castMap });
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
    const count = n < 0 ? Math.max(0, this._rows.length + n) : n;
    return this.withSelection(
      this._rows.slice(0, count),
      this._index.slice(0, count),
      this._columns,
      true
    );
  }

  tail(n = 5): DataFrame {
    const start = n === 0 ? this._rows.length : n < 0 ? Math.min(-n, this._rows.length) : Math.max(0, this._rows.length - n);
    return this.withSelection(
      this._rows.slice(start),
      this._index.slice(start),
      this._columns,
      true
    );
  }

  get(column: string): Series<CellValue> {
    this.assertColumnExists(column);
    // With duplicate display names pandas returns the first matching column
    // via getitem; resolve display names back to their storage key.
    const storageColumn = this._displayColumns
      ? this._columns[this._displayColumns.indexOf(column)] ?? column
      : column;
    return new Series(this._rows.map((row) => row[storageColumn]), {
      name: column,
      dtype: this._dtypes[storageColumn],
      index: this._index,
      indexName: this._indexName,
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

  iloc(selector: number | number[]): Row | DataFrame {
    if (typeof selector === "number") {
      const position = resolvePosition(selector, this._rows.length);
      if (position === undefined) {
        throw new IndexError("single positional indexer is out-of-bounds");
      }
      return cloneRow(this._rows[position]!, this._columns);
    }

    const rows: Row[] = [];
    const index: IndexLabel[] = [];
    for (const requestedPosition of selector) {
      const position = resolvePosition(requestedPosition, this._rows.length);
      if (position === undefined) {
        throw new IndexError("positional indexers are out-of-bounds");
      }
      rows.push(cloneRow(this._rows[position]!, this._columns));
      index.push(this._index[position]!);
    }
    return this.withSelection(rows, index, this._columns, true);
  }

  loc(selector: IndexLabel | IndexLabel[]): Row | DataFrame {
    if (!Array.isArray(selector)) {
      const position = this._index.findIndex((entry) => indexLabelEquals(entry, selector));
      if (position < 0) {
        throw new KeyError(`'${String(selector)}'`);
      }
      return cloneRow(this._rows[position]!, this._columns);
    }

    const rows: Row[] = [];
    const index: IndexLabel[] = [];
    for (const label of selector) {
      const position = this._index.findIndex((entry) => indexLabelEquals(entry, label));
      if (position < 0) {
        throw new KeyError(`[${JSON.stringify(label)}] not found in axis`);
      }
      rows.push(cloneRow(this._rows[position]!, this._columns));
      index.push(this._index[position]!);
    }
    return this.withSelection(rows, index, this._columns, true);
  }

  assign(assignments: Record<string, AssignmentValue>): DataFrame {
    const rows = this.to_records();
    const columns = [...this._columns];
    const dtypes = { ...this._dtypes };
    for (const [column, value] of Object.entries(assignments)) {
      const resolved = this.assignmentColumn(column, value);
      rows.forEach((row, i) => { row[column] = resolved.values[i]; });
      dtypes[column] = resolved.dtype;
      if (!columns.includes(column)) columns.push(column);
    }
    return DataFrame.createInternal(rows, columns, [...this._index], {}, dtypes);
  }

  private assignmentColumn(column: string, value: AssignmentValue): { values: CellValue[]; dtype: DType } {
    if (value instanceof Series) {
      const aligned = value.reindex(this._index);
      return { values: aligned.to_list(), dtype: aligned.dtype };
    }
    const values = this.resolveAssignment(column, value, this._rows.length);
    // Empty arrays in pandas DataFrame assignment carry the float64 default.
    const dtype = values.length === 0 && Array.isArray(value) ? "float64" : convertedDType(values.length ? values : [value as CellValue], undefined, false);
    return { values, dtype };
  }

  /**
   * In-place column insertion like pandas `DataFrame.insert(loc, column, value)`:
   * inserts `column` at position `loc`, aligning Series values by index label.
   */
  insert(
    loc: number,
    column: string,
    value: CellValue | CellValue[] | Series<any>
  ): void {
    if (this._columns.includes(column)) {
      throw new Error(`Column '${column}' already exists.`);
    }
    const position = Math.min(Math.max(loc, 0), this._columns.length);
    const assigned = this.assignmentColumn(column, value);
    const values = assigned.values;

    const nextColumns = [...this._columns];
    nextColumns.splice(position, 0, column);

    const rows = this._rows.map((row, i) => {
      const next: Row = {};
      for (const entry of nextColumns) {
        next[entry] = entry === column ? values[i]! : row[entry];
      }
      return next;
    });

    const prepared = DataFrame.prepareColumns(rows, nextColumns, { ...this._dtypes, [column]: assigned.dtype });
    invalidateColumnStore(this._rows);
    (this as unknown as { _rows: Row[] })._rows = prepared.rows;
    (this as unknown as { _columns: string[] })._columns = nextColumns;
    Object.assign(this._dtypes, prepared.dtypes);
  }

  pop(column: string): Series<CellValue> {
    this.assertColumnExists(column);
    const values = this._rows.map((row) => row[column]);
    const series = new Series(values, { name: column, index: this._index, dtype: this._dtypes[column] });

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
    return this.withSelection(rows, this._index, columns, true);
  }

  drop(columns: string | string[]): DataFrame {
    const removed = new Set(Array.isArray(columns) ? columns : [columns]);
    for (const column of removed) {
      this.assertColumnExists(column);
    }

    const nextColumns = this._columns.filter((column) => !removed.has(column));
    const nextRows = this._rows.map((row) => cloneRow(row, nextColumns));
    return this.withSelection(nextRows, this._index, nextColumns, true);
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

    const dtypes = Object.fromEntries(this._columns.map(column => [columns[column] ?? column, this._dtypes[column]]));
    return DataFrame.createInternal(nextRows, renamedColumns, [...this._index], {}, dtypes);
  }

  /** pandas `rename_axis`: sets the index name. A mapper arg keeps the legacy label-mapping behavior. */
  rename_axis(mapper: string | Record<string, IndexLabel> | ((label: IndexLabel) => IndexLabel)): DataFrame {
    if (typeof mapper === "string") {
      return DataFrame.createInternal(this.to_records(), this._columns, [...this._index], this._dtypeHints, this._dtypes, mapper);
    }
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
      const dtypes = Object.fromEntries(this._columns.map((column, i) => [String(labels[i]), this._dtypes[column]]));
      return DataFrame.createInternal(nextRows, labels.map(String), [...this._index], {}, dtypes);
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
  all(skipna = true): Record<string, CellValue> {
    return columnAllAny(this._columns, this._rows, this._dtypes, skipna, "all");
  }

  /** True for at least one truthy entry per column. */
  any(skipna = true): Record<string, CellValue> {
    return columnAllAny(this._columns, this._rows, this._dtypes, skipna, "any");
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
    this.requireOrderableColumns(by, "nlargest");
    // pandas excludes rows whose ordering column is missing entirely.
    const kept = this.dropMissingInColumns(by);
    return kept.orderByColumns(by, false).head(n);
  }

  /** First `n` rows ordered by `by` ascending (pandas nsmallest). */
  nsmallest(n: number, by: string | string[]): DataFrame {
    this.requireOrderableColumns(by, "nsmallest");
    const kept = this.dropMissingInColumns(by);
    return kept.orderByColumns(by, true).head(n);
  }

  private dropMissingInColumns(by: string | string[]): DataFrame {
    const columns = Array.isArray(by) ? by : [by];
    const keep: number[] = [];
    for (let i = 0; i < this._rows.length; i += 1) {
      if (columns.every((column) => !isMissing(this._rows[i]![column]))) keep.push(i);
    }
    return this.withSelection(
      keep.map((position) => this._rows[position]!),
      keep.map((position) => this._index[position]!),
      this._columns,
      true
    );
  }

  private requireOrderableColumns(by: string | string[], method: string): void {
    for (const column of Array.isArray(by) ? by : [by]) {
      const dtype = this._dtypes[column];
      if (dtype === "object" || dtype === "string" || dtype === "str") {
        throw new TypeError(`Column '${column}' has dtype ${dtype}, cannot use method '${method}' with this dtype`);
      }
    }
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
    return this.withSelection(
      positions.map((position) => this._rows[position]!),
      positions.map((position) => this._index[position]!),
      this._columns,
      true
    );
  }

  // ---- element-wise arithmetic (numeric columns; nulls propagate) ----

  /**
   * Element-wise numeric op. Frame operands align on the union of both
   * indexes and columns like pandas (missing cells propagate); scalars
   * broadcast. Result dtypes follow pandas promotion rules per column,
   * including float64 widening where union alignment introduces gaps.
   */
  private binaryOp(
    other: number | bigint | DataFrame,
    fn: (a: number, b: number) => number,
    division = false,
    boolArithmetic = false,
    floordivDtype = false,
    narrowBool = false,
    stringOp: "error" | "concat" | "repeat" = "error",
    powExponent: "left" | "right" | null = null,
    boolSub = false,
    bigintFn?: (a: bigint, b: bigint) => bigint | null
  ): DataFrame {
    const otherFrame = other instanceof DataFrame ? other : null;
    const scalar = otherFrame ? null : (other as number | bigint);
    let index: IndexLabel[];
    let leftPos: number[];
    let rightPos: number[];
    if (otherFrame) {
      if (indexLabelsEqual(this._index, otherFrame._index)) {
        index = [...this._index];
        leftPos = this._index.map((_, i) => i);
        rightPos = leftPos;
      } else {
        const aligned = alignedPositions(this._index, otherFrame._index, compareIndexLabels);
        index = aligned.index;
        leftPos = aligned.leftPos;
        rightPos = aligned.rightPos;
      }
    } else {
      index = [...this._index];
      leftPos = this._index.map((_, i) => i);
      rightPos = [];
    }
    const leftSet = new Set(this._columns);
    const rightSet = new Set(otherFrame?._columns ?? []);
    const columns = otherFrame
      ? [...this._columns, ...otherFrame._columns.filter(column => !leftSet.has(column))]
      : [...this._columns];
    const anyShared = this._columns.some(column => rightSet.has(column));
    const leftGap = leftPos.some(position => position < 0);
    const rightGap = rightPos.some(position => position < 0);
    // A plain-int column that gains missing cells through union alignment is
    // reindexed to float64 by pandas before the op runs.
    const widenOnGap = (dtype: DType, gap: boolean): DType =>
      gap && /^(u?int|bool)/.test(dtype) ? "float64" : dtype;
    const isStrDType = (dtype: DType | undefined) => /^(str|string)$/.test(dtype ?? "");
    const isIntFamily = (dtype: DType) => /^(u?int|U?Int|bool|boolean)/.test(dtype);
    const rows: Row[] = index.map(() => ({}));
    const declared: Partial<Record<string, DType>> = {};
    for (const column of columns) {
      const inLeft = leftSet.has(column);
      const inRight = otherFrame ? rightSet.has(column) : true;
      const leftD: DType = inLeft ? this._dtypes[column] ?? "object" : "float64";
      const rightD: DType = otherFrame
        ? (inRight ? otherFrame._dtypes[column] ?? "object" : "float64")
        : scalarOperandDType(leftD, scalar!, narrowBool);
      if (boolSub && leftD === "bool" && rightD === "bool") {
        throw new TypeError("numpy boolean subtract, the `-` operator, is not supported, use the bitwise_xor, the `^` operator, or the logical_xor function instead.");
      }
      let values: (CellValue | null)[] = new Array(index.length).fill(null);
      for (let r = 0; r < index.length; r += 1) {
        const aGap = !inLeft || leftPos[r]! < 0;
        const bGap = otherFrame ? !inRight || rightPos[r]! < 0 : false;
        let a: CellValue | null = aGap ? null : this._rows[leftPos[r]!]![column] ?? null;
        let b: CellValue | null | number | bigint = otherFrame
          ? (bGap ? null : otherFrame._rows[rightPos[r]!]![column] ?? null)
          : scalar;
        // Object columns treat non-finite cells as missing like pandas'
        // NaN coercion of object data.
        const aMiss = isMissing(a) || (leftD === "object" && typeof a === "number" && !Number.isFinite(a));
        const bMiss = isMissing(b) || (rightD === "object" && typeof b === "number" && !Number.isFinite(b));
        if (aMiss || bMiss) {
          // pandas materializes a structurally absent operand (unshared
          // label) as NaN and still evaluates the kernel — 1**NaN = 1 is
          // not missing. Genuine missing cells keep propagating, and
          // string columns stay missing rather than erroring on NaN.
          if (stringOp !== "error" || isStrDType(leftD) || isStrDType(rightD)) continue;
          if (aGap) a = NaN;
          if (bGap) b = NaN;
          if ((aMiss && !aGap) || (bMiss && !bGap)) continue;
        }
        const aStr = typeof a === "string";
        const bStr = typeof b === "string";
        // String kernels/errors apply to str-dtyped columns; stray strings
        // inside object columns simply miss, like pandas' NaN coercion.
        if ((aStr || bStr) && (isStrDType(leftD) || isStrDType(rightD))) {
          if (aStr && bStr && stringOp === "concat") {
            values[r] = (a as string) + (b as string);
            continue;
          }
          const repeat = (text: string, count: number) => count <= 0 ? "" : text.repeat(count);
          if (aStr && stringOp === "repeat" && typeof b === "number" && Number.isInteger(b)) {
            values[r] = repeat(a as string, b);
            continue;
          }
          if (bStr && stringOp === "repeat" && typeof a === "number" && Number.isInteger(a)) {
            values[r] = repeat(b as string, a);
            continue;
          }
          throw new TypeError(
            `unsupported operand type(s) for frame arithmetic: ${pythonOperandTypeName(a)} and ${pythonOperandTypeName(b)}`
          );
        }
        // Integer-preserving ops run in bigint when either operand already
        // is one (64-bit ints stay exact); a null result or zero divisor
        // falls back to the number kernel, mirroring pandas' NaN widening.
        if (bigintFn && (typeof a === "bigint" || typeof b === "bigint")) {
          const wa = bigintOperand(a);
          const wb = bigintOperand(b);
          if (wa !== null && wb !== null) {
            let wide: bigint | null = null;
            try { wide = bigintFn(wa, wb); } catch { wide = null; }
            if (wide !== null) {
              values[r] = wide;
              continue;
            }
          }
        }
        if ((typeof a !== "number" && typeof a !== "boolean" && typeof a !== "bigint") ||
            (typeof b !== "number" && typeof b !== "boolean" && typeof b !== "bigint")) {
          continue;
        }
        const av = Number(a);
        const bv = Number(b);
        const exponent = powExponent === "left" ? av : bv;
        if (powExponent !== null && isIntFamily(leftD) && isIntFamily(rightD) &&
            Number.isInteger(exponent) && exponent < 0) {
          throw new ValueError("Integers to negative integer powers are not allowed.");
        }
        values[r] = fn(av, bv);
      }
      let dtype: DType | undefined;
      if (inLeft && inRight) {
        const effL = widenOnGap(leftD, leftGap);
        const effR = widenOnGap(rightD, rightGap);
        dtype = floordivDtype
          ? floordivResultDType(effL, effR)
          : arithResultDType(effL, effR, division, boolArithmetic);
      } else {
        // pandas materializes the missing side as float64 NaN; unshared
        // columns in partially overlapping frames collapse to float64,
        // while pow (like fully disjoint frames) promotes against it.
        const ownerD = widenOnGap(inLeft ? leftD : rightD, inLeft ? leftGap : rightGap);
        dtype = anyShared && powExponent === null
          ? "float64"
          : (floordivDtype ? floordivResultDType(ownerD, "float64") : arithResultDType(ownerD, "float64", division, boolArithmetic)) ?? "float64";
      }
      if ((dtype === undefined || isStrDType(leftD) || isStrDType(rightD)) &&
          values.some(value => typeof value === "string")) {
        dtype = isStrDType(leftD) ? leftD : isStrDType(rightD) ? rightD : "string";
      }
      if (dtype !== undefined && (/^U?Int/.test(dtype) || dtype === "boolean")) {
        // pandas nullable int frames keep the dtype and emit 0 on a zero
        // divisor; genuine fractions still widen to Float64.
        values = values.map(value => typeof value === "number" && !Number.isFinite(value) ? 0 : value);
        if (values.some(value => typeof value === "number" && !Number.isInteger(value))) dtype = "Float64";
      } else if (dtype !== undefined && (/^u?int/.test(dtype) || dtype === "bool")) {
        if (values.some(value => typeof value === "number" && (!Number.isFinite(value) || !Number.isInteger(value)))) dtype = "float64";
      }
      // pandas floordiv on a float-family left computes through float64 and
      // widens when non-integral results appear (Float32 // int -> Float64).
      if (floordivDtype && dtype !== undefined && /^[fF]loat|^number$/.test(widenOnGap(leftD, leftGap)) &&
          values.some(value => typeof value === "number" && !Number.isInteger(value))) {
        dtype = /^F/.test(dtype) ? "Float64" : "float64";
      }
      // numpy narrow-int frame arithmetic wraps on overflow (uint8 - 2 -> 255).
      if (dtype !== undefined && /^u?int(8|16|32)$/.test(dtype)) {
        const bits = Number(dtype.match(/\d+$/)![0]);
        const modulus = 2 ** bits;
        const signed = dtype[0] !== "u";
        values = values.map(value => {
          if (typeof value === "bigint") {
            const big = BigInt(modulus);
            const wrapped = ((value % big) + big) % big;
            return Number(signed && wrapped >= big / 2n ? wrapped - big : wrapped);
          }
          if (typeof value !== "number" || !Number.isFinite(value)) return value;
          const wrapped = ((Math.trunc(value) % modulus) + modulus) % modulus;
          return signed && wrapped >= modulus / 2 ? wrapped - modulus : wrapped;
        });
      }
      if (dtype === "bool" || dtype === "boolean") {
        values = values.map(value => typeof value === "number" ? value !== 0 : value);
      }
      const out = (dtype === "Float32" || dtype === "float32")
        ? values.map(value => typeof value === "number" && Number.isFinite(value) ? Math.fround(value) : value)
        : dtype === "object"
          ? values.map(value => value === null ? NaN : value)
          : values;
      for (let r = 0; r < index.length; r += 1) rows[r]![column] = out[r] ?? null;
      if (dtype) declared[column] = dtype;
    }
    return DataFrame.createInternal(rows, columns, index, {}, declared,
      otherFrame && this._indexName !== otherFrame._indexName ? undefined : this._indexName);
  }

  add(other: number | bigint | DataFrame): DataFrame {
    return this.binaryOp(other, (a, b) => a + b, false, true, false, false, "concat", null, false, (a, b) => a + b);
  }

  sub(other: number | bigint | DataFrame): DataFrame {
    return this.binaryOp(other, (a, b) => a - b, false, false, false, false, "error", null, true, (a, b) => a - b);
  }

  rsub(other: number | bigint | DataFrame): DataFrame {
    return this.binaryOp(other, (a, b) => b - a, false, false, false, false, "error", null, true, (a, b) => b - a);
  }

  mul(other: number | bigint | DataFrame): DataFrame {
    return this.binaryOp(other, (a, b) => a * b, false, true, false, false, "repeat", null, false, (a, b) => a * b);
  }

  div(other: number | bigint | DataFrame): DataFrame {
    return this.binaryOp(other, (a, b) => a / b, true);
  }

  truediv(other: number | bigint | DataFrame): DataFrame {
    return this.div(other);
  }

  mod(other: number | bigint | DataFrame): DataFrame {
    return this.binaryOp(other, (a, b) => a % b, false, false, false, false, "error", null, false,
      (a, b) => bigintMod(a, b));
  }

  pow(other: number | bigint | DataFrame): DataFrame {
    // JS `**` yields NaN for any NaN operand; IEEE pow(1, NaN) = pow(x, 0) = 1.
    const kernel = (a: number, b: number) => a === 1 || b === 0 ? 1 : a ** b;
    return this.binaryOp(other, kernel, false, false, false, true, "error", "right", false,
      (a, b) => (b < 0n ? null : a ** b));
  }

  floordiv(other: number | bigint | DataFrame): DataFrame {
    return this.binaryOp(other, (a, b) => Math.floor(a / b), false, false, true, false, "error", null, false,
      (a, b) => bigintFloorDiv(a, b));
  }

  radd(other: number | bigint | DataFrame): DataFrame {
    return this.add(other);
  }

  rmul(other: number | bigint | DataFrame): DataFrame {
    return this.mul(other);
  }

  rdiv(other: number | bigint | DataFrame): DataFrame {
    return this.binaryOp(other, (a, b) => b / a, true);
  }

  rpow(other: number | bigint | DataFrame): DataFrame {
    const kernel = (a: number, b: number) => b === 1 || a === 0 ? 1 : b ** a;
    return this.binaryOp(other, kernel, false, false, false, false, "error", "left", false,
      (a, b) => (a < 0n ? null : b ** a));
  }

  rmod(other: number | bigint | DataFrame): DataFrame {
    return this.binaryOp(other, (a, b) => b % a, false, false, false, false, "error", null, false,
      (a, b) => bigintMod(b, a));
  }

  // ---- element-wise comparisons (boolean frames) ----

  /**
   * Element-wise comparison. Frame operands align on the union of both
   * axes like pandas. Missing cells compare with IEEE NaN semantics on
   * plain-dtype columns (eq -> False) and propagate NA on nullable
   * columns, so results are `bool` or `boolean` per column.
   */
  private compareOp(
    other: CellValue | DataFrame,
    fn: (a: CellValue, b: CellValue) => boolean,
    ordering = false
  ): DataFrame {
    const otherFrame = other instanceof DataFrame ? other : null;
    let index: IndexLabel[];
    let leftPos: number[];
    let rightPos: number[];
    if (otherFrame) {
      if (indexLabelsEqual(this._index, otherFrame._index)) {
        index = [...this._index];
        leftPos = this._index.map((_, i) => i);
        rightPos = leftPos;
      } else {
        const aligned = alignedPositions(this._index, otherFrame._index, compareIndexLabels);
        index = aligned.index;
        leftPos = aligned.leftPos;
        rightPos = aligned.rightPos;
      }
    } else {
      index = [...this._index];
      leftPos = this._index.map((_, i) => i);
      rightPos = [];
    }
    const leftSet = new Set(this._columns);
    const rightSet = new Set(otherFrame?._columns ?? []);
    const columns = otherFrame
      ? [...this._columns, ...otherFrame._columns.filter(column => !leftSet.has(column))]
      : [...this._columns];
    const isStrDType = (dtype: DType | undefined) => /^(str|string)$/.test(dtype ?? "");
    const isNullableDType = (dtype: DType | undefined) =>
      dtype !== undefined && (/^(U?Int|Float)/.test(dtype) || dtype === "boolean" || isStrDType(dtype));
    const rows: Row[] = index.map(() => ({}));
    const declared: Partial<Record<string, DType>> = {};
    for (const column of columns) {
      const inLeft = leftSet.has(column);
      const inRight = otherFrame ? rightSet.has(column) : true;
      const leftD = inLeft ? this._dtypes[column] : undefined;
      const rightD = otherFrame ? (inRight ? otherFrame._dtypes[column] : undefined) : undefined;
      const rightStr = otherFrame ? (inRight && isStrDType(rightD)) : typeof other === "string";
      // pandas ordering comparisons between string and non-string dtypes raise.
      if (ordering && inLeft && inRight && isStrDType(leftD) !== rightStr) {
        throw new TypeError("unsupported operand type(s) for frame comparison");
      }
      const nullable = isNullableDType(leftD) || isNullableDType(rightD) ||
        (!otherFrame && typeof other === "string");
      declared[column] = nullable ? "boolean" : "bool";
      // Object columns holding text propagate missing (legacy object
      // semantics); purely numeric object columns keep pandas' IEEE
      // false-on-missing comparisons.
      const leftTextObject = leftD === "object" &&
        this._rows.some(row => typeof row[column] === "string");
      const rightTextObject = otherFrame !== null && rightD === "object" &&
        otherFrame._rows.some(row => typeof row[column] === "string");
      for (let r = 0; r < index.length; r += 1) {
        const a = inLeft && leftPos[r]! >= 0 ? this._rows[leftPos[r]!]![column] : null;
        const b = otherFrame
          ? (inRight && rightPos[r]! >= 0 ? otherFrame._rows[rightPos[r]!]![column] : null)
          : other;
        // Strings inside object columns compare as missing, while real
        // str-dtyped columns compare normally.
        const aMiss = isMissing(a) || (typeof a === "string" && !isStrDType(leftD));
        const bMiss = isMissing(b) ||
          (otherFrame !== null && typeof b === "string" && !isStrDType(rightD));
        if ((aMiss || bMiss) &&
            (nullable || (aMiss && leftTextObject) || (bMiss && rightTextObject))) {
          rows[r]![column] = null;
          continue;
        }
        // pandas compares booleans numerically against numbers.
        const coerce = (value: CellValue) => typeof value === "boolean" ? Number(value) : value;
        rows[r]![column] = fn(aMiss ? NaN : coerce(a as CellValue), bMiss ? NaN : coerce(b as CellValue));
      }
      // bool cannot hold the nulls missing-object propagation leaves behind;
      // object-typed results keep them (pandas emits bool when none exist).
      if (declared[column] === "bool" && rows.some(row => row[column] === null)) {
        declared[column] = "object";
      }
    }
    return DataFrame.createInternal(rows, columns, index, {}, declared,
      otherFrame && this._indexName !== otherFrame._indexName ? undefined : this._indexName);
  }

  eq(other: CellValue | DataFrame): DataFrame {
    // numberOrBigintEqual keeps bigint/number comparisons exact (=== is false
    // for equal number/bigint pairs).
    return this.compareOp(other, (a, b) => numberOrBigintEqual(a, b));
  }

  ne(other: CellValue | DataFrame): DataFrame {
    return this.compareOp(other, (a, b) => !numberOrBigintEqual(a, b));
  }

  lt(other: CellValue | DataFrame): DataFrame {
    return this.compareOp(other, (a, b) => (a as number) < (b as number), true);
  }

  le(other: CellValue | DataFrame): DataFrame {
    return this.compareOp(other, (a, b) => (a as number) <= (b as number), true);
  }

  gt(other: CellValue | DataFrame): DataFrame {
    return this.compareOp(other, (a, b) => (a as number) > (b as number), true);
  }

  ge(other: CellValue | DataFrame): DataFrame {
    return this.compareOp(other, (a, b) => (a as number) >= (b as number), true);
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
        return this.withSelection(rows, index, this._columns, true);
      }
      const rows: Row[] = [];
      const index: IndexLabel[] = [];
      for (let i = 0; i < mask.length; i += 1) {
        if (mask[i]) {
          rows.push(this._rows[i]!);
          index.push(this._index[i]!);
        }
      }
      return this.withSelection(rows, index, this._columns, true);
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
      return this.withSelection(rows, index, this._columns, true);
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
    const rowPosition = this._index.findIndex((label) => indexLabelEquals(label, key));
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
    const rows = this.to_records();
    const changed = applyUpdateOverwrite(rows, this._index, this._columns, other._rows, other._index, other._columns);
    if (!changed) return;
    const dtypes = { ...this._dtypes };
    for (const column of this._columns) {
      if (!other._columns.includes(column)) continue;
      dtypes[column] = updatedDType(this._dtypes[column]!, other._dtypes[column]!, rows.map(row => row[column]));
    }
    const prepared = DataFrame.prepareColumns(rows, this._columns, dtypes);
    invalidateColumnStore(this._rows);
    (this as unknown as { _rows: Row[] })._rows = prepared.rows;
    Object.assign(this._dtypes, prepared.dtypes);
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
        dtype: this._dtypes[column],
      }
    );
  }

  // ---- shifts and deltas ----

  /**
   * Shifts values by `periods` rows. Positive shifts move values down
   * (introducing nulls at the top), matching pandas.
   */
  shift(periods = 1): DataFrame {
    if (!Number.isInteger(periods)) {
      throw new TypeError("slice indices must be integers or None or have an __index__ method");
    }
    const rows = shiftRows(this._rows, this._columns, periods);
    for (const column of this._columns) {
      if (this._dtypes[column] === "bool") for (const row of rows) if (row[column] === null) row[column] = NaN;
    }
    const result = this.withReplacement(rows, [...this._index], this._columns, true);
    if (periods !== 0 && rows.length === 0) {
      for (const column of this._columns) {
        const dtype = replacementDType(this._dtypes[column], [NaN]);
        if (dtype) result._dtypes[column] = dtype;
      }
    }
    return result;
  }

  /** First discrete difference of numeric columns. */
  /**
   * Applies a pandas column kernel (Series elementwise semantics) to every
   * column, preserving the resulting dtype per column.
   */
  private applyColumnKernel(
    kernel: (values: CellValue[], dtype: DType, column: string) => { values: CellValue[]; dtype: DType }
  ): DataFrame {
    const dtypes: Record<string, DType> = {};
    const columns: CellValue[][] = this._columns.map((column) => {
      const dtype = (this._dtypes[column] ?? "object") as DType;
      const result = kernel(this._rows.map((row) => row[column]!), dtype, column);
      dtypes[column] = result.dtype;
      return result.values;
    });
    const rows: Row[] = this._rows.map((_row, i) =>
      Object.fromEntries(this._columns.map((column, j) => [column, columns[j]![i]!])) as Row);
    return DataFrame.createInternal(rows, [...this._columns], [...this._index], {}, dtypes, this._indexName);
  }

  diff(periods = 1): DataFrame {
    if (!Number.isInteger(periods)) {
      throw new ValueError("periods must be an integer");
    }
    return this.applyColumnKernel((values, dtype) => pandasDiff(values, dtype, periods));
  }

  /** Percentage change between the current and prior row (numeric columns). */
  pct_change(periods = 1): DataFrame {
    if (!Number.isInteger(periods)) {
      throw new TypeError("slice indices must be integers or None or have an __index__ method");
    }
    return this.applyColumnKernel((values, dtype) => {
      // pandas raises NotImplementedError for empty bool columns on frames,
      // while a bool Series returns an empty object-dtype result.
      if (dtype === "bool" && values.every((value) => isMissing(value))) {
        throw new NotSupportedError("cannot apply pct_change to bool dtype");
      }
      return pandasPctChange(values, dtype, periods);
    });
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
    // A transposed column mixes the source dtypes: pandas keeps a uniform
    // source dtype and falls back to object for mixed frames.
    const sourceDtypes = new Set(this._columns.map((column) => this._dtypes[column] ?? "object"));
    const transposed: DType = sourceDtypes.size === 1 ? [...sourceDtypes][0]! as DType : "object";
    const dtypes = Object.fromEntries(shaped.columns.map((column) => [column, transposed]));
    return DataFrame.createInternal(shaped.rows, shaped.columns, shaped.index, {}, dtypes);
  }

  /** Select columns by stored dtype, numeric family, and optional exclusions. */
  select_dtypes(include?: DTypeSelector | DTypeSelector[], exclude?: DTypeSelector | DTypeSelector[]): DataFrame {
    const matches = dtypeSelector(include, exclude);
    const keep = this._columns.filter(column => matches(this._dtypes[column] ?? convertedDType(this._rows.map(row => row[column]), undefined, false)));
    return this.withSelection(projectColumns(this._rows, keep), [...this._index], keep, true);
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

  where(cond: WhereCondition, other?: WhereOther): DataFrame {
    const rows = computeWhereRows(this._rows, this._columns, this._index, cond, other === undefined ? (Object.keys(this._dtypes).length ? NaN : null) : other, false);
    return this.withReplacement(rows, this._index, this._columns, true);
  }

  mask(cond: WhereCondition, other?: WhereOther): DataFrame {
    if (Array.isArray(cond) && cond.length === 0 && this._rows.length === 0) throw new TypeError("Cannot invert an empty untyped condition array");
    const rows = computeWhereRows(this._rows, this._columns, this._index, cond, other === undefined ? (Object.keys(this._dtypes).length ? NaN : null) : other, true);
    return this.withReplacement(rows, this._index, this._columns, true);
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
    // pandas df.isin(): nullable numeric/boolean columns stay BooleanDtype;
    // everything else (including StringDtype) yields plain bool columns.
    const dtypes = Object.fromEntries(this._columns.map((column) => {
      const src = this._dtypes[column] ?? "object";
      return [column, (isMaskedDType(src) && src !== "string" ? "boolean" : "bool") as DType];
    }));
    return DataFrame.createInternal(rows, [...this._columns], [...this._index], this._dtypeHints, dtypes, this._indexName);
  }

  clip(
    lower?: number | null,
    upper?: number | null,
    columns?: string | string[]
  ): DataFrame {
    if (lower != null && upper != null && lower > upper) {
      throw new Error("clip lower bound cannot exceed upper bound.");
    }
    const targetColumns = new Set(columns
      ? (Array.isArray(columns) ? columns : [columns])
      : this._columns);
    for (const column of targetColumns) {
      this.assertColumnExists(column);
    }
    return this.applyColumnKernel((values, dtype, column) =>
      targetColumns.has(column)
        ? pandasClip(values, dtype, lower, upper)
        : { values, dtype });
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
    return this.withSelection(
      positions.map((position) => this._rows[position]!),
      ignoreIndex ? undefined : positions.map((position) => this._index[position]!),
      this._columns,
      true
    );
  }

  rank(options: RankOptions = {}): DataFrame {
    const method = options.method ?? "average";
    const naOption = options.na_option ?? "keep";
    if (!["average", "min", "max", "first", "dense"].includes(method)) {
      throw new KeyError(`'${method}'`);
    }
    if (!["keep", "top", "bottom"].includes(naOption)) {
      throw new ValueError("na_option must be one of 'keep', 'top', or 'bottom'");
    }
    const rows = computeRankRows(this._rows, this._columns, options);
    const dtypes = Object.fromEntries(this._columns.map(column => [column, "float64" as const]));
    return DataFrame.createInternal(rows, [...this._columns], [...this._index], {}, dtypes);
  }

  sort_values(
    by: string | string[],
    ascending: boolean | boolean[] | { ascending?: boolean | boolean[]; limit?: number; na_position?: "first" | "last" } = true,
    limit?: number,
    na_position: "first" | "last" = "last"
  ): DataFrame {
    if (typeof ascending === "object" && !Array.isArray(ascending)) {
      const options = ascending;
      ascending = options.ascending ?? true;
      limit = options.limit ?? limit;
      na_position = options.na_position ?? na_position;
    }
    const columns = Array.isArray(by) ? by : [by];
    for (const column of columns) {
      if (!this._columns.includes(column)) throw new KeyError(`'${column}'`);
    }
    if (na_position !== "first" && na_position !== "last") {
      throw new Error("na_position must be 'first' or 'last'.");
    }

    const ascendingPerColumn = normalizeSortAscending(columns.length, ascending);
    // Single-column numeric sort: the adaptive policy uses Wasm only in
    // measured call shapes. The TypeScript path remains the fallback.
    if (
      columns.length === 1 &&
      na_position === "last" &&
      isNumericColumn(this._rows, columns[0]!)
    ) {
      const wasmPos = computeWasmSortPositions(
        this._rows,
        columns[0]!,
        ascendingPerColumn[0]!,
        limit
      );
      if (wasmPos) {
        const sliced =
          limit !== undefined
            ? wasmPos.slice(
                0,
                normalizeSortLimit(limit, this._rows.length) ?? wasmPos.length
              )
            : wasmPos;
        if (sliced.length === 0) {
          return this.withSelection([], [], this._columns, true);
        }
        return this.withSelection(
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
      return this.withSelection([], [], this._columns, true);
    }

    const positions =
      normalizedLimit !== undefined && normalizedLimit < this._rows.length
        ? selectTopKPositions(this._rows, comparers, normalizedLimit)
        : fullSortPositions(this._rows, comparers);

    return this.withSelection(
      positions.map((position) => this._rows[position]!),
      positions.map((position) => this._index[position]!),
      this._columns,
      true
    );
  }

  sort_index(ascending = true): DataFrame {
    const positions = range(this._rows.length);
    positions.sort((leftPosition, rightPosition) => {
      const compared = compareIndexLabels(this._index[leftPosition]!, this._index[rightPosition]!);
      return ascending ? compared : -compared;
    });

    return this.withSelection(
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
    const columns = subset ? (Array.isArray(subset) ? subset : [subset]) : this._columns;
    const missingSubset = columns.filter((column) => !this._columns.includes(column));
    if (missingSubset.length > 0) {
      throw new KeyError(`Index([${missingSubset.map((column) => `'${column}'`).join(", ")}], dtype='str')`);
    }
    const include = computeDuplicateKeepFlags(this._rows, columns, keep);

    const rows: Row[] = [];
    const index: IndexLabel[] = [];
    for (let i = 0; i < this._rows.length; i += 1) {
      if (!include[i]) {
        continue;
      }
      rows.push(this._rows[i]!);
      index.push(this._index[i]!);
    }

    return this.withSelection(rows, ignore_index ? undefined : index, this._columns, true);
  }

  duplicated(subset?: string | string[], keep: DropDuplicatesKeep = "first"): Series<boolean> {
    const include = computeDuplicateKeepFlags(this._rows, subset ? (Array.isArray(subset) ? subset : [subset]) : this._columns, keep);
    // pandas df.duplicated() yields an unnamed bool Series.
    return new Series(
      include.map((flag) => !flag),
      { index: this._index, dtype: "bool" }
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
      if (!this._columns.includes(column)) throw new KeyError(`'${column}'`);
    }

    const counts = computeValueCountsRows(this._rows, {
      subset,
      normalize,
      dropna,
      sort,
      ascending,
      limit,
    });

    return DataFrame.createInternal(
      counts.rows,
      [...subset, counts.valueColumnName],
      range(counts.rows.length),
      {},
      { [counts.valueColumnName]: normalize ? "float64" : "int64" }
    );
  }

  dropna(subset?: string[]): DataFrame {
    const columns = subset && subset.length > 0 ? subset : this._columns;
    const missingSubset = columns.filter((column) => !this._columns.includes(column));
    if (missingSubset.length > 0) {
      throw new KeyError(`[${missingSubset.map((column) => `'${column}'`).join(", ")}]`);
    }

    const result = this.filter((row) => columns.every((column) => !isMissing(row[column])));
    const sourceDtypes = this.dtypes();
    const dtypeHints = Object.fromEntries(
      this._columns.flatMap((column) => {
        const dtype = sourceDtypes[column] ?? "unknown";
        if (
          result._rows.length === 0 ||
          dtype === "number" || dtype === "string" || dtype === "date"
        ) {
          return [[column, dtype] as const];
        }
        return [];
      })
    );
    return DataFrame.createInternal(
      result._rows,
      result._columns,
      result._index,
      dtypeHints,
      this._dtypes
    );
  }

  fillna(value: CellValue | Record<string, CellValue>): DataFrame {
    return this.withReplacement(fillnaRows(this._rows, this._columns, value), this._index, this._columns, true);
  }

  set_index(column: string | string[], drop = true): DataFrame {
    const columns = Array.isArray(column) ? column : [column];
    columns.forEach(c => this.assertColumnExists(c));
    const index: IndexLabel[] = columns.length === 1
      ? indexLabelsFromColumn(this._rows, columns[0]!, this._index)
      : this._rows.map(row => columns.map(c => row[c] as string | number));

    const frame = drop
      ? this.drop(columns).withIndex(index)
      : this.withSelection(this.to_records(), index, this._columns, true);
    return DataFrame.createInternal(frame._rows, frame._columns, frame._index, frame._dtypeHints, frame._dtypes, columns[0], columns);
  }

  /** pandas `df.index.name` for single-level indexes. */
  get index_name(): string | undefined {
    return this._indexName;
  }

  /** pandas `df.index.names` — one entry per index level. */
  get index_names(): (string | undefined)[] {
    return this._indexNames ?? [this._indexName];
  }

  /** pandas `df.index.nlevels`. */
  get nlevels(): number {
    const tuple = this._index.find(l => Array.isArray(l));
    return tuple ? (tuple as (string | number)[]).length : 1;
  }

  reset_index(name?: string): DataFrame {
    if (this.nlevels > 1) {
      // pandas emits one column per level, named by index.names or level_N.
      const names = Array.from({ length: this.nlevels },
        (_, i) => this.index_names[i] ?? `level_${i}`);
      for (const columnName of names) {
        if (this._columns.includes(columnName)) {
          throw new Error(`Column '${columnName}' already exists.`);
        }
      }
      const rows = this._rows.map((row, i) => {
        const tuple = (this._index[i] ?? []) as (string | number)[];
        const out: Row = { ...row };
        names.forEach((columnName, level) => { out[columnName] = tuple[level] as CellValue; });
        return out;
      });
      return DataFrame.createInternal(rows, [...names, ...this._columns], range(rows.length), {}, this._dtypes);
    }
    const columnName = name ?? this._indexName ?? "index";
    if (this._columns.includes(columnName)) {
      throw new Error(`Column '${columnName}' already exists.`);
    }
    const rows = resetIndexRows(this._rows, this._columns, this._index, columnName);
    // Existing columns keep their declared dtype; the new index column's
    // dtype is inferred from the labels (str for strings, object if empty).
    return DataFrame.createInternal(rows, [columnName, ...this._columns], range(rows.length), {}, this._dtypes);
  }

  /**
   * pandas unifies a frame reduction's result Series dtype: any nullable
   * (masked) column makes the whole result nullable, so a missing result
   * surfaces as pd.NA rather than NaN.
   */
  private reductionMissing(): CellValue {
    return this._columns.some((column) => isMaskedDType(this._dtypes[column])) ? NA : NaN;
  }

  sum(): Record<string, CellValue> {
    return columnSum(this._columns, this._rows, this._dtypes);
  }

  mean(): Record<string, CellValue> {
    return columnMean(this._columns, this._rows, this._dtypes, this.reductionMissing());
  }

  median(): Record<string, CellValue> {
    return columnMedian(this._columns, this._rows, this._dtypes, this.reductionMissing());
  }

  std(ddof = 1): Record<string, CellValue> {
    return columnStd(this._columns, this._rows, this._dtypes, this.reductionMissing(), ddof);
  }

  var(ddof = 1): Record<string, CellValue> {
    return columnVariance(this._columns, this._rows, this._dtypes, this.reductionMissing(), ddof);
  }

  min(): Record<string, CellValue> {
    return columnMin(this._columns, this._rows, this._dtypes, this.reductionMissing());
  }

  max(): Record<string, CellValue> {
    return columnMax(this._columns, this._rows, this._dtypes, this.reductionMissing());
  }

  /** Product of column values (pandas prod/product). */
  prod(): Record<string, CellValue> {
    return columnProd(this._columns, this._rows, this._dtypes);
  }

  product(): Record<string, CellValue> {
    return this.prod();
  }

  /**
   * Quantile of each numeric column. `q` in [0, 1] (default 0.5 = median),
   * or an array of quantiles to get a frame indexed by q.
   */
  quantile(q: number | number[] = 0.5): Record<string, CellValue> | DataFrame {
    if (!Array.isArray(q)) {
      return columnQuantile(this._columns, this._rows, q, this._dtypes, this.reductionMissing());
    }
    const rows: Row[] = [];
    const index: IndexLabel[] = [];
    for (const qValue of q) {
      const record: Row = { q: qValue };
      for (const [column, value] of Object.entries(columnQuantile(this._columns, this._rows, qValue, this._dtypes, this.reductionMissing()))) {
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
    const rows = this._rows.map((row) => {
      const next = cloneRow(row, this._columns);
      for (const column of this._columns) {
        if (this._dtypes[column] === "object") continue;
        const value = next[column];
        if (isNumber(value)) {
          next[column] = roundToEven(value, decimals);
        }
      }
      return next;
    });
    return this.withReplacement(rows, this._index, this._columns, true);
  }

  abs(columns?: string | string[]): DataFrame {
    const targets = new Set(this.resolveTargetColumns(columns));
    return this.applyColumnKernel((values, dtype, column) =>
      targets.has(column) ? pandasAbs(values, dtype) : { values, dtype });
  }

  cumsum(columns?: string | string[]): DataFrame {
    const targets = new Set(this.resolveTargetColumns(columns));
    return this.applyColumnKernel((values, dtype, column) =>
      targets.has(column) ? pandasCumSumProd(values, dtype, false) : { values, dtype });
  }

  /** pandas `equals`: same shape, labels, column dtypes and cell values. */
  equals(other: DataFrame): boolean {
    for (const column of this._columns) {
      const mine = this._dtypes[column] ?? convertedDType(this._rows.map(row => row[column]), undefined, false);
      const theirs = other._dtypes[column] ?? convertedDType(other._rows.map(row => row[column]), undefined, false);
      if (mine !== theirs) return false;
    }
    if (JSON.stringify(this._columns) !== JSON.stringify(other._columns)) {
      return false;
    }
    if (this._rows.length !== other._rows.length) {
      return false;
    }
    for (let i = 0; i < this._rows.length; i += 1) {
      if (!indexLabelEquals(this._index[i]!, other._index[i]!)) {
        return false;
      }
      for (const column of this._columns) {
        if (
          !scalarEquals(this._rows[i]![column], other._rows[i]![column])
        ) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * pandas `describe`: when any numeric (non-bool) column exists the result
   * covers only those columns with the 8 numeric stats; otherwise object-like
   * columns are described by [count, unique, top, freq].
   */
  describe(): DataFrame {
    const isNumericDtype = (dtype: DType | undefined): boolean =>
      dtype !== undefined && (/^(u?int|float)/.test(dtype) || /^(U?Int|Float)/.test(dtype));
    const numericColumns = this._columns.filter((column) => isNumericDtype(this._dtypes[column]));
    if (numericColumns.length > 0) {
      const labels = ["count", "mean", "std", "min", "25%", "50%", "75%", "max"];
      const rows: Row[] = labels.map(() => ({}));
      const dtypes: Record<string, DType> = {};
      for (const column of numericColumns) {
        const dtype = this._dtypes[column] ?? "object";
        dtypes[column] = isMaskedDType(dtype) ? "Float64" : "float64";
        const values = requireNumeric(this._rows.map((row) => row[column]!), dtype, "describe").sort((a, b) => a - b);
        rows[0]![column] = values.length;
        const quantile = (q: number): number => {
          const pos = (values.length - 1) * q;
          const lower = Math.floor(pos);
          const upper = Math.ceil(pos);
          return values[lower]! + (values[upper]! - values[lower]!) * (pos - lower);
        };
        // Float32/float32 source columns report float32-rounded statistics.
        const round = (v: number): number =>
          dtype === "float32" || dtype === "Float32" ? Math.fround(v) : v;
        const cells = [NaN, NaN, NaN, NaN, NaN, NaN, NaN];
        if (values.length > 0) {
          cells[0] = round(values.reduce((acc, value) => acc + value, 0) / values.length);
          cells[1] = values.length > 1 ? (round(std(values) ?? NaN)) : NaN;
          cells[2] = values[0]!;
          cells[3] = round(quantile(0.25));
          cells[4] = round(quantile(0.5));
          cells[5] = round(quantile(0.75));
          cells[6] = values[values.length - 1]!;
        }
        for (let i = 0; i < 7; i += 1) rows[i + 1]![column] = cells[i]!;
      }
      return DataFrame.createInternal(rows, numericColumns, labels as IndexLabel[], {}, dtypes);
    }
    const objectColumns = this._columns.filter((column) => !isNumericDtype(this._dtypes[column]));
    const labels = ["count", "unique", "top", "freq"];
    const rows: Row[] = labels.map(() => ({}));
    for (const column of objectColumns) {
      const present = this._rows.map((row) => row[column]!).filter((value) => !isMissing(value));
      rows[0]![column] = present.length;
      const counts = new Map<string, { value: CellValue; count: number }>();
      for (const value of present) {
        const key = scalarKey(value);
        const entry = counts.get(key);
        if (entry) entry.count += 1;
        else counts.set(key, { value, count: 1 });
      }
      rows[1]![column] = counts.size;
      if (counts.size === 0) {
        rows[2]![column] = NaN;
        rows[3]![column] = NaN;
      } else {
        // pandas top = first-encountered value among the max-count ties.
        let top: CellValue = NaN;
        let maxCount = 0;
        for (const entry of counts.values()) {
          if (entry.count > maxCount) {
            maxCount = entry.count;
            top = entry.value;
          }
        }
        rows[2]![column] = top;
        rows[3]![column] = maxCount;
      }
    }
    const objectDtypes = Object.fromEntries(objectColumns.map((column) => [column, "object" as DType]));
    return DataFrame.createInternal(rows, objectColumns, labels as IndexLabel[], {}, objectDtypes);
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
      if (!this._columns.includes(key) || !right._columns.includes(key)) throw new KeyError(`'${key}'`);
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

    const leftDtypes = this.dtypes();
    const rightDtypes = right.dtypes();
    const suffixes = options.suffixes ?? ["_x", "_y"];
    const duplicateNonKeys = new Set(
      this._columns.filter((column) => !keys.includes(column) && right._columns.includes(column))
    );
    const dtypeHints: Partial<Record<string, InferredDType>> = {};
    const declaredDTypes: Partial<Record<string, DType>> = {};
    // pandas preserves a column's dtype through merges; nullable masked
    // dtypes tolerate pd.NA and stay, while non-nullable ints widen to
    // float64 and bool to object only when missing values are introduced.
    const mergedColumnDType = (source: DType | "mixed", hasMissing: boolean): DType => {
      if (source === "mixed") return "object";
      return hasMissing ? widenDTypeForMissing(source) : source;
    };

    for (const column of this._columns) {
      if (keys.includes(column)) continue;
      const output = duplicateNonKeys.has(column) ? `${column}${suffixes[0]}` : column;
      const dtype = leftDtypes[column] ?? "unknown";
      if (dtype === "unknown") {
        if (result.rows.every((row) => isMissing(row[output]))) {
          dtypeHints[output] = "object";
        }
        continue;
      }
      declaredDTypes[output] = mergedColumnDType(
        dtype,
        result.rows.some((row) => isMissing(row[output]))
      );
    }
    for (const column of right._columns) {
      if (keys.includes(column)) {
        // pandas' merge key dtype is led by the left key. If the left key is
        // object-like and an inner result is empty, a typed right key does not
        // make the empty output string-typed.
        continue;
      }
      const output = duplicateNonKeys.has(column) ? `${column}${suffixes[1]}` : column;
      const dtype = rightDtypes[column] ?? "unknown";
      if (dtype === "unknown") {
        if (result.rows.every((row) => isMissing(row[output]))) {
          dtypeHints[output] = "object";
        }
        continue;
      }
      declaredDTypes[output] = mergedColumnDType(
        dtype,
        result.rows.some((row) => isMissing(row[output]))
      );
    }
    // A side's key only contributes its dtype to the merged key if at least
    // one of its rows lands in the output: the left key always contributes
    // except in a right join, where it only does via matched (including
    // null==null) rows; symmetrically for the right key.
    const leftKeySet = new Set(this._rows.map((row) => keyForColumns(row, keys)));
    const rightKeySet = new Set(right._rows.map((row) => keyForColumns(row, keys)));
    const leftMatched = [...leftKeySet].some((k) => rightKeySet.has(k));
    const how = options.how ?? "inner";
    const leftContributes = how !== "right" || leftMatched;
    const rightContributes = how === "right" || how === "outer" || leftMatched;

    for (const key of keys) {
      // pandas' merged key dtype upcasts to object when a contributing side's
      // key is object-like; an all-missing source column reads as object too.
      const effectiveKeyDtype = (dtype: InferredDType, rows: Row[]): InferredDType =>
        dtype === "unknown" && rows.every((row) => isMissing(row[key])) ? "object" : dtype;
      const leftKey = effectiveKeyDtype(leftDtypes[key] ?? "unknown", this._rows);
      const rightKey = effectiveKeyDtype(rightDtypes[key] ?? "unknown", right._rows);
      // Outer merges promote both contributing key dtypes (int16+int64 ->
      // int64, Int16+Int64 -> Int64); one-sided joins keep the leading key.
      const leader = how === "right" ? rightKey
        : how === "outer" && leftContributes && rightContributes
          ? promoteDTypes(leftKey as DType, rightKey as DType) as InferredDType
          : leftKey;
      const isObjectLike = (dtype: InferredDType) => dtype === "object" || dtype === "mixed";
      const anyObject =
        (leftContributes && isObjectLike(leftKey)) ||
        (rightContributes && isObjectLike(rightKey));
      if (
        leader !== "unknown" &&
        result.rows.every((row) => isMissing(row[key]))
      ) {
        // All-missing key (including empty inner results): pandas keeps the
        // leading key's dtype.
        dtypeHints[key] = anyObject ? "object" : leader;
      } else if (leader !== "unknown") {
        declaredDTypes[key] = anyObject
          ? "object"
          : mergedColumnDType(
            leader as DType,
            result.rows.some((row) => isMissing(row[key]))
          );
      }
    }

    return DataFrame.createInternal(
      result.rows,
      result.columns,
      range(result.rows.length),
      dtypeHints,
      declaredDTypes
    );
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
        sourceColumn !== undefined ? row[sourceColumn] : (labels ? labels[i] : this._index[i]) as CellValue;
      return next;
    });
    // Preserve declared dtypes so index-joins merge with the source's dtypes.
    return DataFrame.createInternal(rows, [...this._columns, keyColumn], [...this._index], this._dtypeHints, this._dtypes, this._indexName);
  }

  to_string(maxRows = 10): string {
    const [rowCount] = this.shape;
    const rows = this.head(maxRows).to_records();
    const preview = rows.map((row) => JSON.stringify(row, jsonBigintReplacer)).join("\n");
    const suffix = rowCount > maxRows ? `\n... (${rowCount - maxRows} more rows)` : "";
    return preview + suffix;
  }

  // ---- missing-data fills ----
  ffill(): DataFrame {
    return this.withReplacement(ffillRows(this._rows, this._columns), [...this._index], this._columns, true);
  }

  bfill(): DataFrame {
    return this.withReplacement(bfillRows(this._rows, this._columns), [...this._index], this._columns, true);
  }

  interpolate(method = "linear"): DataFrame {
    return this.withReplacement(interpolateRows(this._rows, this._columns, method), [...this._index], this._columns, true);
  }

  // ---- extended stats ----
  skew(): Record<string, CellValue> {
    return columnSkewPandas(this._columns, this._rows, this._dtypes, this.reductionMissing());
  }

  kurt(): Record<string, CellValue> {
    return columnKurtPandas(this._columns, this._rows, this._dtypes, this.reductionMissing());
  }

  kurtosis(): Record<string, CellValue> {
    return this.kurt();
  }

  sem(): Record<string, CellValue> {
    return columnSemPandas(this._columns, this._rows, this._dtypes, this.reductionMissing());
  }

  /** pandas `mode`: per-column modes, sorted ascending, padded missing. */
  mode(): DataFrame {
    const perCol = new Map<string, CellValue[]>();
    let maxLen = 0;
    for (const column of this._columns) {
      const modes = pandasMode(this._rows.map((row) => row[column]!), (v) => scalarKey(v));
      perCol.set(column, modes);
      maxLen = Math.max(maxLen, modes.length);
    }
    const rows: Row[] = [];
    for (let i = 0; i < maxLen; i += 1) {
      const row: Row = {};
      for (const column of this._columns) {
        const modes = perCol.get(column)!;
        // Padding follows the column's missing scalar: NA for nullable
        // dtypes, NaT for datetime64, NaN otherwise.
        const colDtype = this._dtypes[column] ?? "object";
        row[column] = i < modes.length
          ? modes[i]!
          : (isMaskedDType(colDtype) ? NA : isDatetime64DType(colDtype) ? NaT : NaN);
      }
      rows.push(row);
    }
    // pandas df.mode() upcasts a bool column to object; everything else
    // keeps its declared dtype.
    const dtypes = Object.fromEntries(
      this._columns.map((column) => [column, this._dtypes[column] === "bool" ? "object" as DType : (this._dtypes[column] ?? "object" as DType)])
    );
    return DataFrame.createInternal(rows, [...this._columns], range(maxLen), {}, dtypes);
  }

  // ---- cumulative ----
  cummax(): DataFrame {
    return this.applyColumnKernel((values, dtype) => pandasCumMinMax(values, dtype, true));
  }

  cummin(): DataFrame {
    return this.applyColumnKernel((values, dtype) => pandasCumMinMax(values, dtype, false));
  }

  cumprod(): DataFrame {
    return this.applyColumnKernel((values, dtype) => pandasCumSumProd(values, dtype, true));
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
    if (result.kind === "column") return new Series(result.values, { index: result.index, name: result.name, dtype: this._dtypes[result.name] });
    if (result.kind === "row") return new Series(result.values, { index: result.index });
    return this.withRows(result.rows, result.index, result.columns, true);
  }

  take(indices: number[]): DataFrame {
    const result = takeRowsWithIndex(this._rows, this._columns, this._index, indices);
    return this.withSelection(result.rows, result.index, this._columns, true);
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
    return this.withSelection(result.rows, result.index, this._columns, true);
  }

  reindex(options: { index?: IndexLabel[]; columns?: string[]; fill_value?: CellValue } = {}): DataFrame {
    const result = reindexRows(
      this._rows,
      this._columns,
      this._index,
      options.index,
      options.columns,
      options.fill_value === undefined ? (Object.keys(this._dtypes).length ? NaN : null) : options.fill_value
    );
    return this.withReplacement(result.rows, result.index, result.columns, true);
  }

  info(): string {
    return frameInfo(this._rows, this._columns, this._index);
  }

  memory_usage(): Record<string, number> {
    return frameMemoryUsage(this._rows, this._columns);
  }

  // ---- additional aliases ----
  rfloordiv(other: number | DataFrame): DataFrame {
    return this.binaryOp(other, (a, b) => Math.floor(b / a), false, false, true);
  }

  rtruediv(other: number | DataFrame): DataFrame {
    return this.binaryOp(other, (a, b) => b / a, true);
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

  private withReplacement(rows: Row[], index?: IndexLabel[], columns?: string[], _normalized = false): DataFrame {
    const nextColumns = columns ?? this._columns;
    const dtypes = Object.fromEntries(nextColumns.map(column => [column, replacementDType(this._dtypes[column], rows.map(row => row[column]))]));
    return DataFrame.createInternal(rows, [...nextColumns], index ? [...index] : range(rows.length), this._dtypeHints, dtypes);
  }

  /** Row/column selection preserves each retained column's dtype before casting. */
  private withSelection(rows: Row[], index?: IndexLabel[], columns?: string[], _normalized = false): DataFrame {
    return DataFrame.createInternal(rows, [...(columns ?? this._columns)], index ? [...index] : range(rows.length), this._dtypeHints, this._dtypes, index ? this._indexName : undefined);
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
    const nextIndexName = index ? this._indexName : undefined;
    return DataFrame.createInternal(nextRows, nextColumns, nextIndex, {}, {}, nextIndexName);
  }

  private withIndex(index: IndexLabel[]): DataFrame {
    return this.withSelection(this.to_records(), index, this._columns, true);
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
    // Result dtypes follow pandas find_common_type per column: shared
    // columns promote numerically (bool/object/string mixes -> object),
    // while a column owned by one side only widens to float64 when union
    // alignment introduces gaps it cannot hold.
    const { leftPos, rightPos } = indexLabelsEqual(this._index, other._index)
      ? { leftPos: this._index.map((_, i) => i), rightPos: this._index.map((_, i) => i) }
      : alignedPositions(this._index, other._index, compareIndexLabels);
    const leftGap = leftPos.some(position => position < 0);
    const rightGap = rightPos.some(position => position < 0);
    const widenOnGap = (dtype: DType, gap: boolean): DType =>
      gap && /^(u?int|bool)/.test(dtype) ? "float64" : dtype;
    const nonNumeric = (dtype: DType) => /^(bool|boolean|str|string|object)$/.test(dtype);
    const commonDType = (leftD: DType, rightD: DType): DType => {
      if (leftD === rightD) return leftD;
      if (nonNumeric(leftD) || nonNumeric(rightD)) {
        return /^(bool|boolean)$/.test(leftD) && /^(bool|boolean)$/.test(rightD) ? "boolean" : "object";
      }
      return arithResultDType(leftD, rightD, false) ?? "object";
    };
    const leftSet = new Set(this._columns);
    const rightSet = new Set(other._columns);
    const declared: Partial<Record<string, DType>> = {};
    for (const column of result.columns) {
      const inLeft = leftSet.has(column);
      const inRight = rightSet.has(column);
      const leftD = inLeft ? this._dtypes[column] ?? "object" : undefined;
      const rightD = inRight ? other._dtypes[column] ?? "object" : undefined;
      const dtype = leftD !== undefined && rightD !== undefined
        ? commonDType(leftD, rightD)
        : widenOnGap((inLeft ? leftD : rightD)!, inLeft ? leftGap : rightGap);
      if (dtype) declared[column] = dtype;
      // pandas object results keep NA from nullable-dtype sources.
      if (dtype === "object" && leftD !== "object" && rightD !== "object") {
        for (const row of result.rows) {
          if (isMissing(row[column])) row[column] = NA;
        }
      }
    }
    // pandas combine_first produces the sorted union index; reorder the
    // aligned rows when the combine helper emitted first-seen order.
    let outRows = result.rows;
    let outIndex = result.index;
    if (!indexLabelsEqual(this._index, other._index)) {
      const wanted = unionIndexLabels(this._index, other._index, compareIndexLabels);
      if (wanted.length === outIndex.length) {
        const positions = new Map(outIndex.map((label, i) => [indexLabelKey(label), i] as const));
        if (positions.size === outIndex.length && wanted.some((label, i) => !indexLabelEquals(label, outIndex[i]!))) {
          const order = wanted.map(label => positions.get(indexLabelKey(label))!);
          outRows = order.map(i => result.rows[i]!);
          outIndex = wanted;
        }
      }
    }
    return DataFrame.createInternal(outRows, result.columns, outIndex, {}, declared,
      this._indexName === other._indexName ? this._indexName : undefined);
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

  /** Convert scalar types to nullable dtypes without parsing string values. */
  convert_dtypes(): DataFrame {
    return this.convertColumnDtypes(true);
  }

  /** Infer more specific scalar types for object columns. */
  infer_objects(): DataFrame {
    return this.convertColumnDtypes(false);
  }

  private convertColumnDtypes(nullable: boolean): DataFrame {
    const targets = Object.fromEntries(this._columns.map(column => [column,
      convertedDType(this._rows.map(row => row[column]), this._dtypes[column], nullable),
    ]));
    return this.astype(targets);
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
      JSON.stringify({ columns: this._columns, index: this._index, rows: this.to_records() }, jsonBigintReplacer)
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
      indexNamesSnapshot: () => this.index_names,
      dtypesSnapshot: () => this._dtypes,
      withIndexAndNames: (index: IndexLabel[], names?: (string | undefined)[]) =>
        DataFrame.createInternal(this.to_records(), [...this._columns], index, this._dtypeHints, this._dtypes, names?.[0], names),
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
  swaplevel(a = 0, b = -1): DataFrame {
    return windowApi.swaplevel(this.view(), a, b);
  }
  /**
   * In-place positional assignment like pandas `DataFrame.isetitem(loc, value)`:
   * replaces the column at `position`, aligning Series values by index label
   * and adopting the incoming dtype.
   */
  isetitem(position: number, value: CellValue[] | Series<CellValue>): void {
    const resolved = resolvePosition(position, this._columns.length);
    if (resolved === undefined) throw new Error(`isetitem: position ${String(position)} out of bounds.`);
    const column = this._columns[resolved]!;
    const assigned = this.assignmentColumn(column, value);
    const rows = this._rows.map((row, i): Row => ({ ...row, [column]: assigned.values[i]! }));
    const prepared = DataFrame.prepareColumns(rows, this._columns, { ...this._dtypes, [column]: assigned.dtype });
    invalidateColumnStore(this._rows);
    (this as unknown as { _rows: Row[] })._rows = prepared.rows;
    Object.assign(this._dtypes, prepared.dtypes);
  }
  tz_localize(tz: string | null): DataFrame {
    return windowApi.tz_localize(this.view(), tz);
  }
  tz_convert(tz: string | null): DataFrame {
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
