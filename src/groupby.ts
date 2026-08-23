import { DataFrame } from "./dataframe";
import { keyFragment, normalizeKeyCell } from "./internal/dataframe/keys";
import { Series } from "./series";
import type { AggFn, AggName, AggSpec, CellValue, IndexLabel, Row } from "./types";
import {
  wasmAggregateColumn,
  wasmAggMultiF64,
  wasmGroupIds,
} from "./wasm/kernel";
import { buildColumnStore } from "./wasm/columns";
import {
  compareCellValues,
  isMissing,
  isNumber,
  median,
  numericValues,
  range,
  std,
  variance,
} from "./utils";

interface GroupEntry {
  keyValues: CellValue[];
  rows: Row[];
}

interface NamedAggPlan {
  column: string;
  name: AggName;
}

interface CustomAggPlan {
  column: string;
  fn: AggFn;
}

interface FastGroupState {
  keyValues: CellValue[];
  counts: number[];
  sums: number[];
  hasAny: boolean[];
  seen: boolean[];
  best: CellValue[];
}

export interface GroupByOptions {
  dropna?: boolean;
  sort?: boolean;
  as_index?: boolean;
}

export type GroupTransformFn = (
  column: Series<CellValue>,
  name: string,
  position: number
) => CellValue | CellValue[];

interface TransformGroupEntry {
  keyValues: CellValue[];
  positions: number[];
}

export class GroupBy {
  private static readonly groupedCache = new WeakMap<
    DataFrame,
    Map<string, Map<string, GroupEntry>>
  >();

  private readonly source: DataFrame;
  private readonly by: string[];
  private grouped: Map<string, GroupEntry> | null;
  private readonly groupedCacheKey: string;
  private readonly sourceRows: Row[];
  private readonly sourceColumns: string[];
  private readonly options: {
    dropna: boolean;
    sort: boolean;
    as_index: boolean;
  };

  constructor(
    source: DataFrame,
    by: string[],
    sourceRows?: Row[],
    sourceColumns?: string[],
    options: GroupByOptions = {}
  ) {
    const availableColumns = sourceColumns ?? source.columns;
    if (by.length === 0) {
      throw new Error("groupby requires at least one key column.");
    }
    for (const column of by) {
      if (!availableColumns.includes(column)) {
        throw new Error(`Column '${column}' does not exist.`);
      }
    }

    this.source = source;
    this.by = by;
    this.sourceRows = sourceRows ?? source.to_records();
    this.sourceColumns = availableColumns;
    this.options = {
      dropna: options.dropna ?? true,
      sort: options.sort ?? true,
      as_index: options.as_index ?? false,
    };
    this.groupedCacheKey = groupCacheKey(this.by, this.options.dropna);
    this.grouped = this.readGroupCache();
  }

  agg(spec: AggSpec): DataFrame {
    const aggColumns = Object.keys(spec);
    const namedPlans: NamedAggPlan[] = [];
    const customPlans: CustomAggPlan[] = [];

    for (const [column, aggregator] of Object.entries(spec)) {
      if (typeof aggregator === "function") {
        customPlans.push({ column, fn: aggregator as AggFn });
      } else {
        namedPlans.push({ column, name: aggregator as AggName });
      }
    }

    if (
      customPlans.length === 0 &&
      namedPlans.length > 0 &&
      namedPlans.every((plan) => FAST_AGG_NAMES.has(plan.name))
    ) {
      return this.fastNamedAgg(namedPlans, aggColumns);
    }

    const groups = this.sortGroups([...this.getGroups().values()]);
    const rows: Row[] = [];

    for (const group of groups) {
      const row: Row = {};
      for (let i = 0; i < this.by.length; i += 1) {
        row[this.by[i]!] = group.keyValues[i];
      }

      const namedValues = namedPlans.map(() => [] as CellValue[]);
      const customValues = customPlans.map(() => [] as CellValue[]);

      for (const sourceRow of group.rows) {
        for (let i = 0; i < namedPlans.length; i += 1) {
          const plan = namedPlans[i]!;
          namedValues[i]!.push(sourceRow[plan.column]);
        }
        for (let i = 0; i < customPlans.length; i += 1) {
          const plan = customPlans[i]!;
          customValues[i]!.push(sourceRow[plan.column]);
        }
      }

      for (let i = 0; i < namedPlans.length; i += 1) {
        const plan = namedPlans[i]!;
        row[plan.column] = finalizeNamedAggValues(plan.name, namedValues[i]!);
      }
      for (let i = 0; i < customPlans.length; i += 1) {
        const plan = customPlans[i]!;
        row[plan.column] = plan.fn(customValues[i]!, group.rows);
      }

      rows.push(row);
    }

    return this.materializeGroupedRows(rows, aggColumns);
  }

  private fastNamedAgg(namedPlans: NamedAggPlan[], aggColumns: string[]): DataFrame {
    if (this.shouldTryWasm()) {
      const wasmResult = this.tryWasmNamedAgg(namedPlans, aggColumns);
      if (wasmResult) {
        return wasmResult;
      }
    }
    const states = new Map<string, FastGroupState>();
    const planCodes = namedPlans.map((plan) => aggCodeForName(plan.name));
    const byLength = this.by.length;

    if (byLength === 1) {
      const keyColumn = this.by[0]!;
      for (const sourceRow of this.sourceRows) {
        const keyValue = sourceRow[keyColumn];
        if (this.options.dropna && isMissing(keyValue)) {
          continue;
        }
        const key = keyForSingleValue(keyValue);
        let state = states.get(key);
        if (!state) {
          state = {
            keyValues: [keyValue],
            counts: new Array(namedPlans.length).fill(0),
            sums: new Array(namedPlans.length).fill(0),
            hasAny: new Array(namedPlans.length).fill(false),
            seen: new Array(namedPlans.length).fill(false),
            best: new Array(namedPlans.length).fill(null),
          };
          states.set(key, state);
        }
        updateFastGroupStates(state, namedPlans, planCodes, sourceRow);
      }
    } else {
      for (const sourceRow of this.sourceRows) {
        if (this.options.dropna && hasMissingByValue(sourceRow, this.by)) {
          continue;
        }
        const key = keyForRow(sourceRow, this.by);
        let state = states.get(key);
        if (!state) {
          const keyValues = new Array<CellValue>(byLength);
          for (let i = 0; i < byLength; i += 1) {
            keyValues[i] = sourceRow[this.by[i]!];
          }
          state = {
            keyValues,
            counts: new Array(namedPlans.length).fill(0),
            sums: new Array(namedPlans.length).fill(0),
            hasAny: new Array(namedPlans.length).fill(false),
            seen: new Array(namedPlans.length).fill(false),
            best: new Array(namedPlans.length).fill(null),
          };
          states.set(key, state);
        }
        updateFastGroupStates(state, namedPlans, planCodes, sourceRow);
      }
    }

    const groups = this.sortFastStates([...states.values()]);
    const rows: Row[] = [];
    for (const group of groups) {
      const row: Row = {};
      for (let i = 0; i < byLength; i += 1) {
        row[this.by[i]!] = group.keyValues[i];
      }
      for (let i = 0; i < namedPlans.length; i += 1) {
        const plan = namedPlans[i]!;
        const code = planCodes[i]!;
        if (code === AGG_COUNT) {
          row[plan.column] = group.counts[i]!;
        } else if (code === AGG_SUM) {
          row[plan.column] = group.hasAny[i] ? group.sums[i] : null;
        } else if (code === AGG_MEAN) {
          row[plan.column] = group.counts[i]! > 0
            ? group.sums[i]! / group.counts[i]!
            : null;
        } else {
          row[plan.column] = group.seen[i] ? group.best[i] : null;
        }
      }
      rows.push(row);
    }

    return this.materializeGroupedRows(rows, aggColumns);
  }

  /**
   * Whether the wasm groupby path should be attempted for this call.
   *
   * On by default since the single-pass key packing optimization: the
   * wasm path measures ~1.1x faster at 25k rows and ~1.25x at 100k rows,
   * with the gap widening as row counts grow. Set BUN_PANDA_WASM=0 to
   * force the pure-TS path.
   */
  private shouldTryWasm(): boolean {
    if (!this.options.dropna) {
      return false;
    }
    const env = (process as unknown as { env?: Record<string, string> }).env;
    return env?.BUN_PANDA_WASM !== "0";
  }

  /**
   * WASM fast path for named aggregations over numeric columns.
   *
   * Returns null (caller falls back to the TS path) whenever the shape
   * isn't supported: kernel unavailable, a plan references a column with
   * non-numeric values, or the wasm group count disagrees with the TS
   * grouping semantics. Result ordering matches `sort: true` output.
   */
  private tryWasmNamedAgg(
    namedPlans: NamedAggPlan[],
    aggColumns: string[]
  ): DataFrame | null {
    const grouped = wasmGroupIds(this.sourceRows, this.by);
    if (!grouped || grouped.groupCount === 0) {
      return null;
    }

    // Collect representative key values per group from the first row of
    // each group id, preserving source order; sort afterwards like the
    // TS path does when options.sort is on.
    const firstRowOfGroup = new Int32Array(grouped.groupCount).fill(-1);
    for (let i = 0; i < grouped.ids.length; i += 1) {
      const g = grouped.ids[i]!;
      if (g >= 0 && firstRowOfGroup[g] === -1) {
        firstRowOfGroup[g] = i;
      }
    }
    if (firstRowOfGroup.some((row) => row === -1)) {
      return null;
    }

    interface GroupResult {
      keyValues: CellValue[];
      values: (number | null)[];
    }

    // Columnar fast path: build a typed store once, verify all plan
    // columns are numeric, then run one fused kernel call for every
    // aggregation plan instead of per-column marshalling. Duplicate
    // columns in a spec resolve to the same store entry.
    const planColumns = [...new Set(namedPlans.map((plan) => plan.column))];
    const store = buildColumnStore(this.sourceRows, planColumns);
    const numericColumns: Float64Array[] = [];
    const wasmCodes: number[] = [];
    let columnar = true;
    for (const plan of namedPlans) {
      const code = wasmCodeForName(plan.name);
      const col = store.columns.get(plan.column);
      if (code === null || !col || col.kind !== "f64") {
        columnar = false;
        break;
      }
      numericColumns.push(col.values);
      wasmCodes.push(code);
    }

    if (columnar && numericColumns.length > 0) {
      const fused = wasmAggMultiF64(
        numericColumns,
        wasmCodes,
        grouped.ids,
        grouped.groupCount
      );
      if (fused) {
        const groups: GroupResult[] = [];
        for (let g = 0; g < grouped.groupCount; g += 1) {
          const sourceRow = this.sourceRows[firstRowOfGroup[g]!]!;
          const keyValues: CellValue[] = [];
          for (let i = 0; i < this.by.length; i += 1) {
            keyValues.push(sourceRow[this.by[i]!]);
          }
          const values = namedPlans.map((_, planIndex) => {
            const raw = fused.results[planIndex * grouped.groupCount + g]!;
            return Number.isNaN(raw) ? null : raw;
          });
          groups.push({ keyValues, values });
        }
        return this.materializeOrderedGroups(groups, aggColumns);
      }
    }

    const perPlanResults: (Float64Array | null)[] = [];

    for (const plan of namedPlans) {
      const code = wasmCodeForName(plan.name);
      if (code === null) {
        return null;
      }
      let allNumeric = true;
      for (let i = 0; i < this.sourceRows.length; i += 1) {
        const value = this.sourceRows[i]![plan.column];
        if (!(value === null || value === undefined || isNumber(value))) {
          allNumeric = false;
          break;
        }
      }
      if (!allNumeric) {
        return null;
      }
      perPlanResults.push(
        wasmAggregateColumn(
          this.sourceRows,
          plan.column,
          grouped.ids,
          grouped.groupCount,
          code
        )
      );
    }

    if (perPlanResults.some((result) => result === null)) {
      return null;
    }

    const groups: GroupResult[] = [];
    for (let g = 0; g < grouped.groupCount; g += 1) {
      const sourceRow = this.sourceRows[firstRowOfGroup[g]!]!;
      const keyValues: CellValue[] = [];
      for (let i = 0; i < this.by.length; i += 1) {
        keyValues.push(sourceRow[this.by[i]!]);
      }
      const values = namedPlans.map((_, planIndex) => {
        const raw = perPlanResults[planIndex]![g]!;
        return Number.isNaN(raw) ? null : raw;
      });
      groups.push({ keyValues, values });
    }

    const ordered = this.options.sort
      ? groups.sort((left, right) =>
          compareKeyValues(left.keyValues, right.keyValues)
        )
      : groups;

    const rows: Row[] = [];
    for (const group of ordered) {
      const row: Row = {};
      for (let i = 0; i < this.by.length; i += 1) {
        row[this.by[i]!] = group.keyValues[i];
      }
      namedPlans.forEach((plan, planIndex) => {
        row[plan.column] = group.values[planIndex];
      });
      rows.push(row);
    }

    return this.materializeGroupedRows(rows, aggColumns);
  }

  /** Sorts wasm-path group results and materializes the output frame. */
  private materializeOrderedGroups(
    groups: { keyValues: CellValue[]; values: (number | null)[] }[],
    aggColumns: string[]
  ): DataFrame {
    interface GroupResult {
      keyValues: CellValue[];
      values: (number | null)[];
    }
    const ordered = this.options.sort
      ? (groups as GroupResult[]).sort((left, right) =>
          compareKeyValues(left.keyValues, right.keyValues)
        )
      : groups;

    const rows: Row[] = [];
    for (const group of ordered) {
      const row: Row = {};
      for (let i = 0; i < this.by.length; i += 1) {
        row[this.by[i]!] = group.keyValues[i];
      }
      group.values.forEach((value, i) => {
        row[aggColumns[i]!] = value;
      });
      rows.push(row);
    }

    return this.materializeGroupedRows(rows, aggColumns);
  }

  private getGroups(): Map<string, GroupEntry> {
    if (!this.grouped) {
      this.grouped = this.buildGroups();
      this.writeGroupCache(this.grouped);
    }
    return this.grouped;
  }

  private readGroupCache(): Map<string, GroupEntry> | null {
    const sourceCache = GroupBy.groupedCache.get(this.source);
    if (!sourceCache) {
      return null;
    }
    return sourceCache.get(this.groupedCacheKey) ?? null;
  }

  private writeGroupCache(grouped: Map<string, GroupEntry>): void {
    let sourceCache = GroupBy.groupedCache.get(this.source);
    if (!sourceCache) {
      sourceCache = new Map();
      GroupBy.groupedCache.set(this.source, sourceCache);
    }
    sourceCache.set(this.groupedCacheKey, grouped);
  }

  private sortGroups(groups: GroupEntry[]): GroupEntry[] {
    if (!this.options.sort) {
      return groups;
    }
    return groups.sort((left, right) =>
      compareKeyValues(left.keyValues, right.keyValues)
    );
  }

  private sortFastStates(states: FastGroupState[]): FastGroupState[] {
    if (!this.options.sort) {
      return states;
    }
    return states.sort((left, right) =>
      compareKeyValues(left.keyValues, right.keyValues)
    );
  }

  count(columns?: string[]): DataFrame {
    const candidates = columns ?? this.source.columns.filter((column) => !this.by.includes(column));
    const spec: AggSpec = {};
    for (const column of candidates) {
      spec[column] = "count";
    }
    return this.agg(spec);
  }

  sum(columns?: string[]): DataFrame {
    const candidates = columns ?? this.numericColumns();
    const spec: AggSpec = {};
    for (const column of candidates) {
      spec[column] = "sum";
    }
    return this.agg(spec);
  }

  mean(columns?: string[]): DataFrame {
    const candidates = columns ?? this.numericColumns();
    const spec: AggSpec = {};
    for (const column of candidates) {
      spec[column] = "mean";
    }
    return this.agg(spec);
  }

  min(columns?: string[]): DataFrame {
    return this.namedColumnAgg("min", columns ?? this.numericColumns());
  }

  max(columns?: string[]): DataFrame {
    return this.namedColumnAgg("max", columns ?? this.numericColumns());
  }

  median(columns?: string[]): DataFrame {
    return this.namedColumnAgg("median", columns ?? this.numericColumns());
  }

  std(columns?: string[]): DataFrame {
    return this.namedColumnAgg("std", columns ?? this.numericColumns());
  }

  var(columns?: string[]): DataFrame {
    return this.namedColumnAgg("var", columns ?? this.numericColumns());
  }

  nunique(columns?: string[]): DataFrame {
    return this.namedColumnAgg("nunique", columns ?? this.nonKeyColumns());
  }

  first(columns?: string[]): DataFrame {
    return this.namedColumnAgg("first", columns ?? this.nonKeyColumns());
  }

  last(columns?: string[]): DataFrame {
    return this.namedColumnAgg("last", columns ?? this.nonKeyColumns());
  }

  /** Cumulative sums within each group, aligned to source rows. */
  cumsum(columns?: string[]): DataFrame {
    return this.groupCumulative("cumsum", columns);
  }

  /** Cumulative max within each group. */
  cummax(columns?: string[]): DataFrame {
    return this.groupCumulative("cummax", columns);
  }

  /** Cumulative min within each group. */
  cummin(columns?: string[]): DataFrame {
    return this.groupCumulative("cummin", columns);
  }

  private groupCumulative(
    kind: "cumsum" | "cummax" | "cummin",
    columns?: string[]
  ): DataFrame {
    const candidates = columns ?? this.numericColumns();
    const groups = this.buildGroupPositions();
    const outRows: Row[] = new Array(this.sourceRows.length);

    for (const group of groups.values()) {
      const accs = new Map<string, number>();
      for (const position of group.positions) {
        const sourceRow = this.sourceRows[position]!;
        const next: Row = {};
        for (const column of this.by) {
          next[column] = sourceRow[column];
        }
        for (const column of candidates) {
          const value = sourceRow[column];
          if (typeof value !== "number" || !Number.isFinite(value)) {
            next[column] = null;
            continue;
          }
          const prev = accs.get(column) ?? 0;
          let updated: number;
          if (kind === "cumsum") {
            updated = prev + value;
          } else if (kind === "cummax") {
            updated = accs.has(column) ? Math.max(prev, value) : value;
          } else {
            updated = accs.has(column) ? Math.min(prev, value) : value;
          }
          accs.set(column, updated);
          next[column] = updated;
        }
        outRows[position] = next;
      }
    }

    const outColumns = [...this.by, ...candidates];
    return DataFrame.from_normalized(outRows as Row[], outColumns, range(outRows.length));
  }

  /** Per-group quantile of numeric columns. */
  quantile(q = 0.5, columns?: string[]): DataFrame {
    const candidates = columns ?? this.numericColumns();
    const spec: AggSpec = {};
    for (const column of candidates) {
      spec[column] = (values: CellValue[]) => {
        const nums = values
          .filter((v): v is number => typeof v === "number" && Number.isFinite(v))
          .sort((a, b) => a - b);
        if (nums.length === 0) return null;
        const pos = (nums.length - 1) * q;
        const lower = Math.floor(pos);
        const upper = Math.ceil(pos);
        return nums[lower]! + (nums[upper]! - nums[lower]!) * (pos - lower);
      };
    }
    return this.agg(spec);
  }

  /** Filters whole groups by a predicate over the group frame. */
  filter(predicate: (group: DataFrame) => boolean): DataFrame {
    const groups = this.sortGroups([...this.getGroups().values()]);
    const kept: Row[] = [];
    for (const group of groups) {
      const groupFrame = new DataFrame(group.rows);
      if (predicate(groupFrame)) {
        kept.push(...group.rows);
      }
    }
    return new DataFrame(kept);
  }

  /** Per-group describe: count/mean/std/min/quantiles/max as rows. */
  describe(): DataFrame {
    const stats = ["count", "mean", "std", "min", "25%", "50%", "75%", "max"];
    const groups = this.sortGroups([...this.getGroups().values()]);
    const outRows: Row[] = [];

    const quantileOf = (sorted: number[], q: number): number => {
      const pos = (sorted.length - 1) * q;
      const lower = Math.floor(pos);
      const upper = Math.ceil(pos);
      return sorted[lower]! + (sorted[upper]! - sorted[lower]!) * (pos - lower);
    };

    for (const group of groups) {
      for (const stat of stats) {
        const row: Row = {};
        this.by.forEach((key, i) => {
          row[key] = group.keyValues[i];
        });
        row.stat = stat;
        for (const column of this.numericColumns()) {
          const values = numericValues(group.rows.map((r) => r[column])).sort((a, b) => a - b);
          let value: number | null = null;
          if (stat === "count") {
            value = values.length;
          } else if (values.length > 0) {
            switch (stat) {
              case "mean":
                value = values.reduce((s, v) => s + v, 0) / values.length;
                break;
              case "std":
                value =
                  values.length > 1
                    ? std(values)
                    : null;
                break;
              case "min":
                value = values[0]!;
                break;
              case "25%":
                value = quantileOf(values, 0.25);
                break;
              case "50%":
                value = quantileOf(values, 0.5);
                break;
              case "75%":
                value = quantileOf(values, 0.75);
                break;
              case "max":
                value = values[values.length - 1]!;
                break;
            }
          }
          row[column] = value;
        }
        outRows.push(row);
      }
    }

    const columns = [...this.by, "stat", ...this.numericColumns()];
    return new DataFrame(outRows, { columns });
  }

  /**
   * Per-group value counts of one column, with the group keys and the
   * counted value as extra columns.
   */
  value_counts(column: string): DataFrame {
    if (!this.sourceColumns.includes(column)) {
      throw new Error(`Column '${column}' does not exist.`);
    }
    const groups = this.sortGroups([...this.getGroups().entries()].map(([_, entry]) => entry));
    const outRows: Row[] = [];
    for (const group of this.sortGroups([...this.getGroups().values()])) {
      const counts = new Map<string, { value: CellValue; count: number }>();
      for (const row of group.rows) {
        const value = row[column];
        if (isMissing(value)) {
          continue;
        }
        const key = String(value);
        const entry = counts.get(key);
        if (entry) {
          entry.count += 1;
        } else {
          counts.set(key, { value, count: 1 });
        }
      }
      for (const entry of counts.values()) {
        const row: Row = {};
        this.by.forEach((key, i) => {
          row[key] = group.keyValues[i];
        });
        row[column] = entry.value;
        row.count = entry.count;
        outRows.push(row);
      }
    }
    return new DataFrame(outRows, { columns: [...this.by, column, "count"] });
  }

  /** Applies fn to each group frame and concatenates the results. */
  apply(fn: (group: DataFrame) => DataFrame): DataFrame {
    const groups = this.sortGroups([...this.getGroups().values()]);
    const kept: Row[] = [];
    for (const group of groups) {
      kept.push(...fn(new DataFrame(group.rows)).to_records());
    }
    return new DataFrame(kept);
  }

  /** Alias of agg (pandas aggregate). */
  aggregate(spec: AggSpec): DataFrame {
    return this.agg(spec);
  }

  /** Functional chaining helper over the grouped frame. */
  pipe<T>(fn: (groupby: any, ...args: never[]) => T, ...args: never[]): T {
    return fn(this, ...args);
  }

  /**
   * Shifts values within each group by `periods`, aligned to source
   * rows like pandas `groupby().shift()`.
   */
  shift(periods = 1, columns?: string[]): DataFrame {
    const candidates = columns ?? this.nonKeyColumns();
    const groups = this.buildGroupPositions();
    const outRows: Row[] = new Array(this.sourceRows.length);

    for (const group of groups.values()) {
      for (let i = 0; i < group.positions.length; i += 1) {
        const targetPosition = group.positions[i]!;
        const sourcePosition = i - periods >= 0 ? group.positions[i - periods] : undefined;
        const sourceRow =
          sourcePosition !== undefined ? this.sourceRows[sourcePosition]! : null;
        const next: Row = {};
        for (const column of this.by) {
          next[column] = this.sourceRows[targetPosition]![column];
        }
        for (const column of candidates) {
          next[column] = sourceRow ? sourceRow[column] : null;
        }
        outRows[targetPosition] = next;
      }
    }

    const outColumns = [...this.by, ...candidates];
    return DataFrame.from_normalized(outRows as Row[], outColumns, range(outRows.length));
  }

  /** First difference within each group. */
  diff(periods = 1, columns?: string[]): DataFrame {
    const candidates = columns ?? this.numericColumns();
    const groups = this.buildGroupPositions();
    const outRows: Row[] = new Array(this.sourceRows.length);

    for (const group of groups.values()) {
      for (let i = 0; i < group.positions.length; i += 1) {
        const targetPosition = group.positions[i]!;
        const current = this.sourceRows[targetPosition]!;
        const prevPos = i - periods >= 0 ? group.positions[i - periods] : undefined;
        const previous = prevPos !== undefined ? this.sourceRows[prevPos] : undefined;
        const next: Row = {};
        for (const column of this.by) {
          next[column] = current[column];
        }
        for (const column of candidates) {
          const a = current[column];
          const b = previous?.[column];
          if (
            typeof a === "number" && Number.isFinite(a) &&
            typeof b === "number" && Number.isFinite(b)
          ) {
            next[column] = a - b;
          } else {
            next[column] = null;
          }
        }
        outRows[targetPosition] = next;
      }
    }

    const outColumns = [...this.by, ...candidates];
    return DataFrame.from_normalized(outRows as Row[], outColumns, range(outRows.length));
  }

  private namedColumnAgg(name: AggName, candidates: string[]): DataFrame {
    const spec: AggSpec = {};
    for (const column of candidates) {
      spec[column] = name;
    }
    return this.agg(spec);
  }

  private nonKeyColumns(): string[] {
    return this.sourceColumns.filter((column) => !this.by.includes(column));
  }

  size(): DataFrame {
    const rows: Row[] = [];
    const groups = this.sortGroups([...this.getGroups().values()]);

    for (const group of groups) {
      const row: Row = {};
      for (let i = 0; i < this.by.length; i += 1) {
        row[this.by[i]!] = group.keyValues[i];
      }
      row.size = group.rows.length;
      rows.push(row);
    }

    return this.materializeGroupedRows(rows, ["size"]);
  }

  transform(spec: AggSpec | GroupTransformFn): DataFrame {
    const groups = this.sortTransformGroups([...this.buildGroupPositions().values()]);
    const outColumns =
      typeof spec === "function"
        ? this.sourceColumns.filter((column) => !this.by.includes(column))
        : Object.keys(spec);

    const rows: Row[] = new Array(this.sourceRows.length);
    for (let i = 0; i < rows.length; i += 1) {
      const row: Row = {};
      for (const column of outColumns) {
        row[column] = null;
      }
      rows[i] = row;
    }

    for (const group of groups) {
      const groupRows = group.positions.map((position) => this.sourceRows[position]!);
      if (typeof spec === "function") {
        for (let c = 0; c < outColumns.length; c += 1) {
          const column = outColumns[c]!;
          const subSeries = new Series(
            groupRows.map((row) => row[column] ?? null),
            { name: column }
          );
          const result = spec(subSeries, column, c);
          if (Array.isArray(result)) {
            if (result.length !== groupRows.length) {
              throw new Error("transform function must return a value per group row.");
            }
            for (let i = 0; i < groupRows.length; i += 1) {
              rows[group.positions[i]!]![column] = result[i] ?? null;
            }
          } else {
            for (const position of group.positions) {
              rows[position]![column] = result;
            }
          }
        }
      } else {
        for (const [column, aggregator] of Object.entries(spec)) {
          const values = groupRows.map((row) => row[column] ?? null);
          const aggregated =
            typeof aggregator === "function"
              ? aggregator(values, groupRows)
              : finalizeNamedAggValues(aggregator as AggName, values);
          for (const position of group.positions) {
            rows[position]![column] = aggregated;
          }
        }
      }
    }

    return DataFrame.from_normalized(rows, outColumns, [...this.source.index]);
  }

  private buildGroupPositions(): Map<string, TransformGroupEntry> {
    const groups = new Map<string, TransformGroupEntry>();

    if (this.by.length === 1) {
      const keyColumn = this.by[0]!;
      for (let position = 0; position < this.sourceRows.length; position += 1) {
        const keyValue = this.sourceRows[position]![keyColumn];
        if (this.options.dropna && isMissing(keyValue)) {
          continue;
        }
        const key = keyForSingleValue(keyValue);
        const existing = groups.get(key);
        if (existing) {
          existing.positions.push(position);
        } else {
          groups.set(key, { keyValues: [keyValue], positions: [position] });
        }
      }
      return groups;
    }

    for (let position = 0; position < this.sourceRows.length; position += 1) {
      const row = this.sourceRows[position]!;
      if (this.options.dropna && hasMissingByValue(row, this.by)) {
        continue;
      }
      const key = keyForRow(row, this.by);
      const existing = groups.get(key);
      if (existing) {
        existing.positions.push(position);
      } else {
        const keyValues = new Array<CellValue>(this.by.length);
        for (let i = 0; i < this.by.length; i += 1) {
          keyValues[i] = row[this.by[i]!];
        }
        groups.set(key, { keyValues, positions: [position] });
      }
    }
    return groups;
  }

  private sortTransformGroups(groups: TransformGroupEntry[]): TransformGroupEntry[] {
    if (!this.options.sort) {
      return groups;
    }
    return groups.sort((left, right) =>
      compareKeyValues(left.keyValues, right.keyValues)
    );
  }

  private buildGroups(): Map<string, GroupEntry> {
    const groups = new Map<string, GroupEntry>();

    if (this.by.length === 1) {
      const keyColumn = this.by[0]!;
      for (const row of this.sourceRows) {
        const keyValue = row[keyColumn];
        if (this.options.dropna && isMissing(keyValue)) {
          continue;
        }
        const key = keyForSingleValue(keyValue);
        const existing = groups.get(key);
        if (existing) {
          existing.rows.push(row);
        } else {
          groups.set(key, {
            keyValues: [keyValue],
            rows: [row],
          });
        }
      }
      return groups;
    }

    for (const row of this.sourceRows) {
      if (this.options.dropna && hasMissingByValue(row, this.by)) {
        continue;
      }
      const key = keyForRow(row, this.by);
      const existing = groups.get(key);
      if (existing) {
        existing.rows.push(row);
      } else {
        const keyValues = new Array<CellValue>(this.by.length);
        for (let i = 0; i < this.by.length; i += 1) {
          keyValues[i] = row[this.by[i]!];
        }
        groups.set(key, {
          keyValues,
          rows: [row],
        });
      }
    }
    return groups;
  }

  private numericColumns(): string[] {
    return this.sourceColumns.filter((column) => {
      if (this.by.includes(column)) {
        return false;
      }
      for (const row of this.sourceRows) {
        if (isNumber(row[column])) {
          return true;
        }
      }
      return false;
    });
  }

  private materializeGroupedRows(rows: Row[], valueColumns: string[]): DataFrame {
    if (!this.options.as_index) {
      return DataFrame.from_normalized(rows, [...this.by, ...valueColumns]);
    }
    if (this.by.length !== 1) {
      throw new Error("groupby(as_index=true) with multiple keys requires MultiIndex support.");
    }

    const keyColumn = this.by[0]!;
    const outRows = rows.map((row) => {
      const out: Row = {};
      for (const column of valueColumns) {
        out[column] = row[column];
      }
      return out;
    });

    const index = rows.map((row, position) => toIndexLabel(row[keyColumn], position));
    return DataFrame.from_normalized(outRows, valueColumns, index);
  }
}

const FAST_AGG_NAMES = new Set<AggName>(["count", "sum", "mean", "min", "max"]);

function finalizeNamedAggValues(name: AggName, values: CellValue[]): CellValue {
  if (name === "count") {
    let count = 0;
    for (const value of values) {
      if (!isMissing(value)) {
        count += 1;
      }
    }
    return count;
  }

  if (name === "sum" || name === "mean") {
    const numbers = numericValues(values);
    if (numbers.length === 0) {
      return null;
    }
    const total = numbers.reduce((sum, value) => sum + value, 0);
    return name === "sum" ? total : total / numbers.length;
  }

  if (name === "min" || name === "max") {
    let best: CellValue = null;
    for (const value of values) {
      if (isMissing(value)) {
        continue;
      }
      if (best === null) {
        best = value;
        continue;
      }
      const compared = compareCellValues(value, best);
      if ((name === "min" && compared < 0) || (name === "max" && compared > 0)) {
        best = value;
      }
    }
    return best;
  }

  if (name === "median") {
    return median(numericValues(values));
  }

  if (name === "std") {
    return std(numericValues(values));
  }

  if (name === "var") {
    return variance(numericValues(values));
  }

  if (name === "first") {
    for (const value of values) {
      if (!isMissing(value)) {
        return value;
      }
    }
    return null;
  }

  if (name === "last") {
    for (let i = values.length - 1; i >= 0; i -= 1) {
      const value = values[i]!;
      if (!isMissing(value)) {
        return value;
      }
    }
    return null;
  }

  const seen = new Set<string>();
  for (const value of values) {
    if (isMissing(value)) {
      continue;
    }
    seen.add(JSON.stringify(normalizeKeyCell(value)));
  }
  return seen.size;
}

function keyForRow(row: Row, columns: string[]): string {
  let key = "";
  for (const column of columns) {
    key += keyFragment(row[column]);
  }
  return key;
}

function keyForSingleValue(value: CellValue): string {
  return keyFragment(value);
}

function hasMissingByValue(row: Row, columns: string[]): boolean {
  for (const column of columns) {
    if (isMissing(row[column])) {
      return true;
    }
  }
  return false;
}

function updateFastGroupStates(
  state: FastGroupState,
  plans: NamedAggPlan[],
  planCodes: number[],
  row: Row
): void {
  for (let i = 0; i < plans.length; i += 1) {
    const plan = plans[i]!;
    const code = planCodes[i]!;
    const value = row[plan.column];
    if (code === AGG_COUNT) {
      if (!isMissing(value)) {
        state.counts[i]! += 1;
      }
      continue;
    }

    if (code === AGG_SUM || code === AGG_MEAN) {
      if (isNumber(value)) {
        state.hasAny[i] = true;
        state.counts[i]! += 1;
        state.sums[i]! += value;
      }
      continue;
    }

    if (isMissing(value)) {
      continue;
    }
    if (!state.seen[i]) {
      state.best[i] = value;
      state.seen[i] = true;
      continue;
    }

    const compared = compareCellValues(value, state.best[i]);
    if (
      (code === AGG_MIN && compared < 0) ||
      (code === AGG_MAX && compared > 0)
    ) {
      state.best[i] = value;
    }
  }
}

const AGG_COUNT = 1;
const AGG_SUM = 2;
const AGG_MEAN = 3;
const AGG_MIN = 4;
const AGG_MAX = 5;

function aggCodeForName(name: AggName): number {
  if (name === "count") {
    return AGG_COUNT;
  }
  if (name === "sum") {
    return AGG_SUM;
  }
  if (name === "mean") {
    return AGG_MEAN;
  }
  if (name === "min") {
    return AGG_MIN;
  }
  return AGG_MAX;
}

/** Maps an AggName to the wasm kernel's aggregation code, or null. */
function wasmCodeForName(name: AggName): number | null {
  // Kernel codes: sum=0, mean=1, min=2, max=3, count=4.
  if (name === "count") {
    return 4;
  }
  if (name === "sum") {
    return 0;
  }
  if (name === "mean") {
    return 1;
  }
  if (name === "min") {
    return 2;
  }
  if (name === "max") {
    return 3;
  }
  return null;
}

function compareKeyValues(left: CellValue[], right: CellValue[]): number {
  const size = Math.min(left.length, right.length);
  for (let i = 0; i < size; i += 1) {
    const compared = compareCellValues(left[i], right[i]);
    if (compared !== 0) {
      return compared;
    }
  }
  return left.length - right.length;
}

function toIndexLabel(value: CellValue, fallback: number): IndexLabel {
  if (typeof value === "number" || typeof value === "string") {
    return value;
  }
  return String(value ?? fallback);
}

function groupCacheKey(by: string[], dropna: boolean): string {
  return `${dropna ? "1" : "0"}|${by.join("\u001f")}`;
}
