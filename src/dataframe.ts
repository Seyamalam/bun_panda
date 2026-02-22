import { writeFileSync } from "node:fs";
import { GroupBy } from "./groupby";
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
}

export type DropDuplicatesKeep = "first" | "last" | false;

export interface PivotTableOptions {
  index: string | string[];
  values: string | string[];
  columns?: string;
  aggfunc?: AggName | AggFn;
  fill_value?: CellValue;
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

  static from_records(records: Row[], options: DataFrameOptions = {}): DataFrame {
    return new DataFrame(records, options);
  }

  static from_dict(data: Record<string, CellValue[]>, options: DataFrameOptions = {}): DataFrame {
    return new DataFrame(data, options);
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

    return this.withRows(rows, this._index, this._columns);
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
    return this.withRows(this._rows.slice(0, count), this._index.slice(0, count), this._columns);
  }

  tail(n = 5): DataFrame {
    const count = Math.max(0, n);
    return this.withRows(this._rows.slice(-count), this._index.slice(-count), this._columns);
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
    return this.withRows(rows, index, this._columns);
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
    return this.withRows(rows, index, this._columns);
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

    return this.withRows(rows, this._index, columns);
  }

  select(columns: string[]): DataFrame {
    for (const column of columns) {
      this.assertColumnExists(column);
    }
    const rows = this._rows.map((row) => cloneRow(row, columns));
    return this.withRows(rows, this._index, columns);
  }

  drop(columns: string | string[]): DataFrame {
    const removed = new Set(Array.isArray(columns) ? columns : [columns]);
    for (const column of removed) {
      this.assertColumnExists(column);
    }

    const nextColumns = this._columns.filter((column) => !removed.has(column));
    const nextRows = this._rows.map((row) => cloneRow(row, nextColumns));
    return this.withRows(nextRows, this._index, nextColumns);
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

    return this.withRows(nextRows, this._index, renamedColumns);
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
          rows.push(cloneRow(this._rows[i]!, this._columns));
          index.push(this._index[i]!);
        }
      }
      return this.withRows(rows, index, this._columns);
    }

    for (let i = 0; i < this._rows.length; i += 1) {
      const row = this._rows[i]!;
      const label = this._index[i]!;
      if (mask(cloneRow(row, this._columns), label, i)) {
        rows.push(cloneRow(row, this._columns));
        index.push(label);
      }
    }
    return this.withRows(rows, index, this._columns);
  }

  query(predicate: (row: Row, index: IndexLabel, position: number) => boolean): DataFrame {
    return this.filter(predicate);
  }

  sort_values(by: string, ascending = true): DataFrame {
    this.assertColumnExists(by);

    const pairs = this._rows.map((row, position) => ({
      row: cloneRow(row, this._columns),
      index: this._index[position]!,
    }));

    pairs.sort((left, right) => {
      const compared = compareCellValues(left.row[by], right.row[by]);
      return ascending ? compared : -compared;
    });

    return this.withRows(
      pairs.map((pair) => pair.row),
      pairs.map((pair) => pair.index),
      this._columns
    );
  }

  sort_index(ascending = true): DataFrame {
    const pairs = this._rows.map((row, position) => ({
      row: cloneRow(row, this._columns),
      index: this._index[position]!,
    }));

    pairs.sort((left, right) => {
      const compared = compareCellValues(left.index, right.index);
      return ascending ? compared : -compared;
    });

    return this.withRows(
      pairs.map((pair) => pair.row),
      pairs.map((pair) => pair.index),
      this._columns
    );
  }

  drop_duplicates(subset?: string | string[], keep: DropDuplicatesKeep = "first"): DataFrame {
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
      rows.push(cloneRow(this._rows[i]!, this._columns));
      index.push(this._index[i]!);
    }

    return this.withRows(rows, index, this._columns);
  }

  value_counts(options: ValueCountsOptions = {}): DataFrame {
    const subset = options.subset
      ? (Array.isArray(options.subset) ? options.subset : [options.subset])
      : this._columns;
    const normalize = options.normalize ?? false;
    const dropna = options.dropna ?? true;

    for (const column of subset) {
      this.assertColumnExists(column);
    }

    const counts = new Map<string, { values: CellValue[]; count: number }>();
    let consideredRows = 0;

    for (const row of this._rows) {
      const values = subset.map((column) => row[column]);
      if (dropna && values.some((value) => isMissing(value))) {
        continue;
      }

      consideredRows += 1;
      const key = JSON.stringify(values.map((value) => normalizeKeyCell(value)));
      const entry = counts.get(key);
      if (!entry) {
        counts.set(key, { values, count: 1 });
      } else {
        entry.count += 1;
      }
    }

    const valueColumnName = normalize ? "proportion" : "count";
    const countRows = [...counts.values()]
      .sort((left, right) => {
        if (left.count !== right.count) {
          return right.count - left.count;
        }
        for (let i = 0; i < left.values.length; i += 1) {
          const compared = compareCellValues(left.values[i], right.values[i]);
          if (compared !== 0) {
            return compared;
          }
        }
        return 0;
      })
      .map((entry) => {
        const row: Row = {};
        for (let i = 0; i < subset.length; i += 1) {
          row[subset[i]!] = entry.values[i];
        }
        row[valueColumnName] =
          normalize && consideredRows > 0 ? entry.count / consideredRows : entry.count;
        return row;
      });

    return new DataFrame(countRows, {
      columns: [...subset, valueColumnName],
    });
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
    return this.withRows(rows, this._index, this._columns);
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
      return this.withRows(this.to_records(), index, this._columns);
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

  groupby(by: string | string[]): GroupBy {
    return new GroupBy(this, Array.isArray(by) ? by : [by]);
  }

  pivot_table(options: PivotTableOptions): DataFrame {
    const index = Array.isArray(options.index) ? options.index : [options.index];
    const values = Array.isArray(options.values) ? options.values : [options.values];
    const columns = options.columns;
    const aggfunc = options.aggfunc ?? "mean";
    const fillValue = options.fill_value;

    for (const indexColumn of index) {
      this.assertColumnExists(indexColumn);
    }
    for (const valueColumn of values) {
      this.assertColumnExists(valueColumn);
    }
    if (columns) {
      this.assertColumnExists(columns);
    }

    if (!columns) {
      const grouped = this.aggregateRows(index, values, aggfunc);
      return new DataFrame(grouped, {
        columns: [...index, ...values],
      });
    }

    const grouped = this.aggregateRows([...index, columns], values, aggfunc);
    const pivotColumnValues = uniqueColumnValues(this._rows.map((row) => row[columns]));
    const valueColumnsOut =
      values.length === 1
        ? pivotColumnValues.map((value) => String(value))
        : values.flatMap((valueColumn) =>
            pivotColumnValues.map((value) => `${valueColumn}_${String(value)}`)
          );

    const tableRows = new Map<string, Row>();
    const orderedKeys: string[] = [];

    for (const row of grouped) {
      const indexValues = index.map((column) => row[column]);
      const tableKey = JSON.stringify(indexValues.map((value) => normalizeKeyCell(value)));
      const columnValue = row[columns];

      let tableRow = tableRows.get(tableKey);
      if (!tableRow) {
        tableRow = {};
        for (let i = 0; i < index.length; i += 1) {
          tableRow[index[i]!] = indexValues[i];
        }
        tableRows.set(tableKey, tableRow);
        orderedKeys.push(tableKey);
      }

      for (const valueColumn of values) {
        const outputColumn =
          values.length === 1 ? String(columnValue) : `${valueColumn}_${String(columnValue)}`;
        tableRow[outputColumn] = row[valueColumn];
      }
    }

    const outputRows = orderedKeys.map((key) => {
      const row = tableRows.get(key)!;
      for (const valueColumn of valueColumnsOut) {
        if (row[valueColumn] === undefined && fillValue !== undefined) {
          row[valueColumn] = fillValue;
        }
      }
      return row;
    });

    return new DataFrame(outputRows, {
      columns: [...index, ...valueColumnsOut],
    });
  }

  merge(right: DataFrame, options: MergeOptions): DataFrame {
    const keys = Array.isArray(options.on) ? options.on : [options.on];
    const how = options.how ?? "inner";
    const suffixes = options.suffixes ?? ["_x", "_y"];

    for (const key of keys) {
      this.assertColumnExists(key);
      right.assertColumnExists(key);
    }

    const duplicateNonKeys = new Set(
      this._columns.filter((column) => !keys.includes(column) && right._columns.includes(column))
    );

    const leftColumnsOut = this._columns.map((column) =>
      duplicateNonKeys.has(column) ? `${column}${suffixes[0]}` : column
    );

    const rightColumnsSource = right._columns.filter((column) => !keys.includes(column));
    const rightColumnsOut = rightColumnsSource.map((column) =>
      duplicateNonKeys.has(column) ? `${column}${suffixes[1]}` : column
    );

    const rightGroups = new Map<string, Array<{ row: Row; position: number }>>();
    for (let i = 0; i < right._rows.length; i += 1) {
      const row = right._rows[i]!;
      const key = keyForColumns(row, keys);
      const current = rightGroups.get(key);
      if (current) {
        current.push({ row, position: i });
      } else {
        rightGroups.set(key, [{ row, position: i }]);
      }
    }

    const matchedRightRows = new Set<number>();
    const rows: Row[] = [];
    for (const leftRow of this._rows) {
      const key = keyForColumns(leftRow, keys);
      const matches = rightGroups.get(key);

      if (!matches || matches.length === 0) {
        if (how === "left" || how === "outer") {
          rows.push(
            buildMergedRow(
              leftRow,
              undefined,
              this._columns,
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
            this._columns,
            leftColumnsOut,
            rightColumnsSource,
            rightColumnsOut,
            keys
          )
        );
        matchedRightRows.add(match.position);
      }
    }

    if (how === "right" || how === "outer") {
      for (let i = 0; i < right._rows.length; i += 1) {
        if (matchedRightRows.has(i)) {
          continue;
        }
        rows.push(
          buildMergedRow(
            undefined,
            right._rows[i]!,
            this._columns,
            leftColumnsOut,
            rightColumnsSource,
            rightColumnsOut,
            keys
          )
        );
      }
    }

    return new DataFrame(rows, {
      columns: [...leftColumnsOut, ...rightColumnsOut],
    });
  }

  to_string(maxRows = 10): string {
    const [rowCount] = this.shape;
    const rows = this.head(maxRows).to_records();
    const preview = rows.map((row) => JSON.stringify(row)).join("\n");
    const suffix = rowCount > maxRows ? `\n... (${rowCount - maxRows} more rows)` : "";
    return preview + suffix;
  }

  private aggregateRows(groupColumns: string[], valueColumns: string[], aggfunc: AggName | AggFn): Row[] {
    const groups = new Map<string, { groupValues: CellValue[]; rows: Row[] }>();

    for (const sourceRow of this._rows) {
      const groupValues = groupColumns.map((column) => sourceRow[column]);
      const key = JSON.stringify(groupValues.map((value) => normalizeKeyCell(value)));
      const group = groups.get(key);
      if (!group) {
        groups.set(key, { groupValues, rows: [sourceRow] });
      } else {
        group.rows.push(sourceRow);
      }
    }

    const rows: Row[] = [];
    for (const group of groups.values()) {
      const row: Row = {};
      for (let i = 0; i < groupColumns.length; i += 1) {
        row[groupColumns[i]!] = group.groupValues[i];
      }

      for (const valueColumn of valueColumns) {
        const values = group.rows.map((entry) => entry[valueColumn]);
        row[valueColumn] = runAggregation(values, group.rows, aggfunc);
      }
      rows.push(row);
    }
    return rows;
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

  private withRows(rows: Row[], index?: IndexLabel[], columns?: string[]): DataFrame {
    return new DataFrame(rows, {
      columns: columns ?? this._columns,
      index,
    });
  }

  private withIndex(index: IndexLabel[]): DataFrame {
    return new DataFrame(this.to_records(), {
      columns: this._columns,
      index,
    });
  }
}

function runAggregation(values: CellValue[], rows: Row[], aggfunc: AggName | AggFn): CellValue {
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

function normalizeRecords(records: Row[], forcedColumns?: string[]): { rows: Row[]; columns: string[] } {
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

function normalizeColumnar(data: Record<string, CellValue[]>): { rows: Row[]; columns: string[] } {
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

function resolvePosition(position: number, length: number): number | undefined {
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

function escapeCsvValue(value: CellValue, sep: string): string {
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

function keyForColumns(row: Row, keys: string[]): string {
  return JSON.stringify(keys.map((key) => normalizeKeyCell(row[key])));
}

function normalizeKeyCell(value: CellValue): string | number | boolean | null {
  if (value instanceof Date) {
    return value.toISOString();
  }
  if (value === undefined) {
    return null;
  }
  if (typeof value === "string" || typeof value === "number" || typeof value === "boolean" || value === null) {
    return value;
  }
  return String(value);
}

function uniqueColumnValues(values: CellValue[]): CellValue[] {
  const out: CellValue[] = [];
  const seen = new Set<string>();
  for (const value of values) {
    if (isMissing(value)) {
      continue;
    }
    const key = JSON.stringify(normalizeKeyCell(value));
    if (!seen.has(key)) {
      seen.add(key);
      out.push(value);
    }
  }
  return out;
}

function buildMergedRow(
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
