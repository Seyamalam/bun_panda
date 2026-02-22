import { writeFileSync } from "node:fs";
import { GroupBy } from "./groupby";
import type { GroupByOptions } from "./groupby";
import {
  escapeCsvValue,
  normalizeColumnar,
  normalizeRecords,
  resolvePosition,
} from "./internal/dataframe/core";
import { computeMergeRows } from "./internal/dataframe/merge";
import { computePivotTable } from "./internal/dataframe/pivotTable";
import { computeValueCountsRows } from "./internal/dataframe/valueCounts";
import {
  keyForColumns,
} from "./internal/dataframe/keys";
import {
  buildColumnComparer,
  fullSortPositions,
  normalizeSortAscending,
  normalizeSortLimit,
  selectTopKPositions,
} from "./internal/dataframe/ordering";
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
  numericValues,
  range,
  std,
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

export interface MergeOptions {
  on: string | string[];
  how?: "inner" | "left" | "right" | "outer";
  suffixes?: [string, string];
}

export interface ValueCountsOptions {
  subset?: string | string[];
  normalize?: boolean;
  dropna?: boolean;
  sort?: boolean;
  ascending?: boolean;
  limit?: number;
}

export type DropDuplicatesKeep = "first" | "last" | false;

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

type AssignmentValue = CellValue[] | Series<CellValue> | CellValue;

export class DataFrame {
  private readonly _rows: Row[];
  private readonly _columns: string[];
  private readonly _index: IndexLabel[];

  constructor(data: Row[] | Record<string, CellValue[]> = [], options: DataFrameOptions = {}) {
    const normalized = Array.isArray(data)
      ? normalizeRecords(data, options.columns)
      : normalizeColumnar(data);

    this._rows = normalized.rows;
    this._columns = normalized.columns;
    this._index = options.index ? [...options.index] : range(this._rows.length);

    if (this._index.length !== this._rows.length) {
      throw new Error("DataFrame index length must match row count.");
    }
  }

  private static createInternal(rows: Row[], columns: string[], index: IndexLabel[]): DataFrame {
    if (index.length !== rows.length) {
      throw new Error("DataFrame index length must match row count.");
    }
    const frame = Object.create(DataFrame.prototype) as DataFrame;
    (frame as unknown as { _rows: Row[] })._rows = rows;
    (frame as unknown as { _columns: string[] })._columns = columns;
    (frame as unknown as { _index: IndexLabel[] })._index = index;
    return frame;
  }

  static from_records(records: Row[], options: DataFrameOptions = {}): DataFrame {
    return new DataFrame(records, options);
  }

  static from_dict(data: Record<string, CellValue[]>, options: DataFrameOptions = {}): DataFrame {
    return new DataFrame(data, options);
  }

  static from_normalized(
    rows: Row[],
    columns: string[],
    index?: IndexLabel[]
  ): DataFrame {
    return DataFrame.createInternal(rows, [...columns], index ? [...index] : range(rows.length));
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

  to_json(orient: "records" | "list" = "records", space = 2): string {
    return JSON.stringify(this.to_dict(orient), null, space);
  }

  to_csv(options: ToCSVOptions = {}): string {
    const sep = options.sep ?? ",";
    const includeHeader = options.header ?? true;
    const includeIndex = options.index ?? false;
    const indexName = "index";

    const lines: string[] = [];

    if (includeHeader) {
      const headerCells = includeIndex ? [indexName, ...this._columns] : [...this._columns];
      lines.push(headerCells.map((cell) => escapeCsvValue(cell, sep)).join(sep));
    }

    for (let i = 0; i < this._rows.length; i += 1) {
      const row = this._rows[i]!;
      const rowCells = this._columns.map((column) => escapeCsvValue(row[column], sep));
      if (includeIndex) {
        rowCells.unshift(escapeCsvValue(this._index[i], sep));
      }
      lines.push(rowCells.join(sep));
    }

    const csv = `${lines.join("\n")}\n`;
    if (options.path) {
      writeFileSync(options.path, csv, "utf8");
    }
    return csv;
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

  filter(mask: boolean[] | ((row: Row, index: IndexLabel, position: number) => boolean)): DataFrame {
    const rows: Row[] = [];
    const index: IndexLabel[] = [];

    if (Array.isArray(mask)) {
      if (mask.length !== this._rows.length) {
        throw new Error("Mask length must match row count.");
      }
      for (let i = 0; i < mask.length; i += 1) {
        if (mask[i]) {
          rows.push(this._rows[i]!);
          index.push(this._index[i]!);
        }
      }
      return this.withRows(rows, index, this._columns, true);
    }

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

  query(predicate: (row: Row, index: IndexLabel, position: number) => boolean): DataFrame {
    return this.filter(predicate);
  }

  sort_values(
    by: string | string[],
    ascending: boolean | boolean[] = true,
    limit?: number
  ): DataFrame {
    const columns = Array.isArray(by) ? by : [by];
    for (const column of columns) {
      this.assertColumnExists(column);
    }

    const ascendingPerColumn = normalizeSortAscending(columns.length, ascending);
    const comparers = columns.map((column, i) =>
      buildColumnComparer(this._rows, column, ascendingPerColumn[i]!)
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
    const columns = subset ? (Array.isArray(subset) ? subset : [subset]) : this._columns;
    for (const column of columns) {
      this.assertColumnExists(column);
    }

    const include = new Array(this._rows.length).fill(false);

    if (keep === false) {
      const counts = new Map<string, number>();
      for (const row of this._rows) {
        const key = keyForColumns(row, columns);
        counts.set(key, (counts.get(key) ?? 0) + 1);
      }
      for (let i = 0; i < this._rows.length; i += 1) {
        include[i] = (counts.get(keyForColumns(this._rows[i]!, columns)) ?? 0) === 1;
      }
    } else if (keep === "last") {
      const seen = new Set<string>();
      for (let i = this._rows.length - 1; i >= 0; i -= 1) {
        const key = keyForColumns(this._rows[i]!, columns);
        if (!seen.has(key)) {
          seen.add(key);
          include[i] = true;
        }
      }
    } else {
      const seen = new Set<string>();
      for (let i = 0; i < this._rows.length; i += 1) {
        const key = keyForColumns(this._rows[i]!, columns);
        if (!seen.has(key)) {
          seen.add(key);
          include[i] = true;
        }
      }
    }

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
    const rows = this._rows.map((row) => {
      const next = cloneRow(row, this._columns);
      for (const column of this._columns) {
        if (!isMissing(next[column])) {
          continue;
        }
        if (typeof value === "object" && value !== null && !(value instanceof Date)) {
          next[column] = value[column] ?? next[column];
        } else {
          next[column] = value;
        }
      }
      return next;
    });
    return this.withRows(rows, this._index, this._columns, true);
  }

  set_index(column: string, drop = true): DataFrame {
    this.assertColumnExists(column);
    const index: IndexLabel[] = this._rows.map((row, position) => {
      const value = row[column];
      if (typeof value === "number" || typeof value === "string") {
        return value;
      }
      return String(value ?? this._index[position]!);
    });

    if (!drop) {
      return this.withRows(this.to_records(), index, this._columns, true);
    }
    return this.drop(column).withIndex(index);
  }

  reset_index(name = "index"): DataFrame {
    if (this._columns.includes(name)) {
      throw new Error(`Column '${name}' already exists.`);
    }
    const rows = this._rows.map((row, position) => ({
      [name]: this._index[position]!,
      ...cloneRow(row, this._columns),
    }));
    return new DataFrame(rows, { columns: [name, ...this._columns] });
  }

  sum(): Record<string, number | null> {
    const out: Record<string, number | null> = {};
    for (const column of this._columns) {
      const values = numericValues(this._rows.map((row) => row[column]));
      out[column] = values.length > 0 ? values.reduce((acc, value) => acc + value, 0) : null;
    }
    return out;
  }

  mean(): Record<string, number | null> {
    const out: Record<string, number | null> = {};
    for (const column of this._columns) {
      const values = numericValues(this._rows.map((row) => row[column]));
      out[column] =
        values.length > 0 ? values.reduce((acc, value) => acc + value, 0) / values.length : null;
    }
    return out;
  }

  describe(): DataFrame {
    const numericColumns = this._columns.filter((column) =>
      this._rows.some((row) => isNumber(row[column]))
    );

    const stats = ["count", "mean", "std", "min", "max"];
    const rows: Row[] = stats.map((statName) => ({ stat: statName }));

    for (const column of numericColumns) {
      const values = numericValues(this._rows.map((row) => row[column]));
      rows[0]![column] = values.length;
      rows[1]![column] =
        values.length > 0 ? values.reduce((acc, value) => acc + value, 0) / values.length : null;
      rows[2]![column] = std(values);
      rows[3]![column] = values.length > 0 ? Math.min(...values) : null;
      rows[4]![column] = values.length > 0 ? Math.max(...values) : null;
    }

    return new DataFrame(rows, {
      columns: ["stat", ...numericColumns],
      index: stats,
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

  to_string(maxRows = 10): string {
    const [rowCount] = this.shape;
    const rows = this.head(maxRows).to_records();
    const preview = rows.map((row) => JSON.stringify(row)).join("\n");
    const suffix = rowCount > maxRows ? `\n... (${rowCount - maxRows} more rows)` : "";
    return preview + suffix;
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
}
