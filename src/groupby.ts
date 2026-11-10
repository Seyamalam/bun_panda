import { DataFrame } from "./dataframe";
import { Series } from "./series";
import type { AggFn, AggName, AggSpec, CellValue, DType, Row } from "./types";
import { hasCachedColumns } from "./wasm/columns";
import {
  compareKeyValues,
  finalizeNamedAggValues,
  hasMissingByValue,
  keyForRow,
  keyForSingleValue,
  keyForValues,
  shouldTryWasm as shouldTryWasmFast,
  toIndexLabel,
  tryWasmNamedAgg as tryWasmNamedAggFast,
  updateFastGroupStates,
} from "./internal/groupby/fastAgg";
import {
  isMissing,
  isNumber,
  numericValues,
  range,
  std,
} from "./utils";
import {
  adjustedSkew,
  excessKurtosis,
  seriesVariance,
} from "./internal/series/stats";
import {
  isMaskedDType,
  missingScalar,
  pandasMinMax,
  pandasProd,
  pandasSum,
  requireNumeric,
} from "./internal/series/pandasOps";
import { scalarKey } from "./internal/shared/valueSemantics";
import { computeRank } from "./internal/series/rank";
import { KeyError, ValueError } from "./errors";
import { computeRolling } from "./internal/dataframe/rolling";
import { covariance, pearson } from "./internal/dataframe/stats";

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
  wideSums: (bigint | null)[];
  comps: number[];
  hasAny: boolean[];
  seen: boolean[];
  best: CellValue[];
}

export interface GroupByOptions {
  dropna?: boolean;
  sort?: boolean;
  as_index?: boolean;
  /**
   * pandas `groupby(level=...)`: index level position, name, or list of
   * either. Mutually exclusive with `by`.
   */
  level?: number | string | (number | string)[];
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

export interface RollingGroupBy {
  mean(): DataFrame;
  sum(): DataFrame;
  min(): DataFrame;
  max(): DataFrame;
  count(): DataFrame;
  std(): DataFrame;
}

export interface RankOptions {
  method?: "average" | "min" | "max" | "first" | "dense";
  ascending?: boolean;
  na_option?: "keep" | "top" | "bottom";
  pct?: boolean;
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
  /**
   * Per-row key tuples for `groupby(level=...)` — values pulled from index
   * levels rather than columns, aligned to `sourceRows` by position.
   */
  private readonly levelKeys: CellValue[][] | undefined;
  /** Index level names for level grouping, used for `as_index` results. */
  private readonly levelIndexNames: (string | undefined)[] | undefined;
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
    options: GroupByOptions = {},
    levelKeys?: CellValue[][],
    levelIndexNames?: (string | undefined)[]
  ) {
    const availableColumns = (sourceColumns ?? source.columns).map((column) => String(column));
    if (by.length === 0) {
      throw new Error("groupby requires at least one key column.");
    }
    if (!levelKeys) {
      for (const column of by) {
        if (!availableColumns.includes(column)) {
          throw new KeyError(`'${column}'`);
        }
      }
    }

    this.source = source;
    this.by = by;
    this.sourceRows = sourceRows ?? source.to_records();
    this.sourceColumns = availableColumns;
    this.levelKeys = levelKeys;
    this.levelIndexNames = levelIndexNames;
    this.options = {
      dropna: options.dropna ?? true,
      sort: options.sort ?? true,
      as_index: options.as_index ?? false,
    };
    this.groupedCacheKey = `${levelKeys ? "level|" : ""}${groupCacheKey(this.by, this.options.dropna)}`;
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
    if (this.shouldTryWasm(namedPlans)) {
      const wasmResult = this.tryWasmNamedAgg(namedPlans, aggColumns);
      if (wasmResult) {
        return wasmResult;
      }
    }
    const states = new Map<string, FastGroupState>();
    const planCodes = namedPlans.map((plan) => aggCodeForName(plan.name));
    const byLength = this.by.length;
    const levelKeys = this.levelKeys;

    if (levelKeys) {
      // groupby(level=...): keys come from index levels, not row columns.
      for (let position = 0; position < this.sourceRows.length; position += 1) {
        const keyValues = levelKeys[position]!;
        if (this.options.dropna && keyValues.some((value) => isMissing(value))) {
          continue;
        }
        const key = keyForValues(keyValues);
        let state = states.get(key);
        if (!state) {
          state = {
            keyValues: [...keyValues],
            counts: new Array(namedPlans.length).fill(0),
            sums: new Array(namedPlans.length).fill(0),
            wideSums: new Array(namedPlans.length).fill(null),
            comps: new Array(namedPlans.length).fill(0),
            hasAny: new Array(namedPlans.length).fill(false),
            seen: new Array(namedPlans.length).fill(false),
            best: new Array(namedPlans.length).fill(null),
          };
          states.set(key, state);
        }
        updateFastGroupStates(state, namedPlans, planCodes, this.sourceRows[position]!);
      }
    } else if (byLength === 1) {
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
            wideSums: new Array(namedPlans.length).fill(null),
            comps: new Array(namedPlans.length).fill(0),
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
            wideSums: new Array(namedPlans.length).fill(null),
            comps: new Array(namedPlans.length).fill(0),
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
          row[plan.column] = group.wideSums[i] !== null
            ? group.wideSums[i]
            : group.hasAny[i] ? group.sums[i] : null;
        } else if (code === AGG_MEAN) {
          row[plan.column] = group.counts[i]! > 0
            ? group.wideSums[i] !== null
              ? Number(group.wideSums[i]) / group.counts[i]!
              : group.sums[i]! / group.counts[i]!
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
   * Adaptive mode retains TypeScript until calibration demonstrates a
   * benefit from typed-column reuse. BUN_PANDA_WASM=1 forces the Wasm
   * experiment path; BUN_PANDA_WASM=0 forces TypeScript.
   */
  private shouldTryWasm(namedPlans: NamedAggPlan[]): boolean {
    const planColumns = [...new Set(namedPlans.map((plan) => plan.column))];
    return shouldTryWasmFast(
      this.options.dropna ?? true,
      this.sourceRows.length,
      namedPlans.length,
      hasCachedColumns(this.sourceRows, planColumns)
    );
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
    return tryWasmNamedAggFast(
      this.sourceRows,
      this.by,
      namedPlans,
      aggColumns,
      (groups, columns) => this.materializeOrderedGroups(groups, columns)
    );
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
    return this.aggregatePandas("count", columns);
  }

  sum(columns?: string[]): DataFrame {
    return this.aggregatePandas("sum", columns);
  }

  mean(columns?: string[]): DataFrame {
    return this.aggregatePandas("mean", columns);
  }

  prod(columns?: string[]): DataFrame {
    return this.aggregatePandas("prod", columns);
  }

  min(columns?: string[]): DataFrame {
    return this.aggregatePandas("min", columns);
  }

  max(columns?: string[]): DataFrame {
    return this.aggregatePandas("max", columns);
  }

  median(columns?: string[]): DataFrame {
    return this.aggregatePandas("median", columns);
  }

  std(columns?: string[]): DataFrame {
    return this.aggregatePandas("std", columns);
  }

  var(columns?: string[]): DataFrame {
    return this.aggregatePandas("var", columns);
  }

  nunique(columns?: string[]): DataFrame {
    return this.aggregatePandas("nunique", columns);
  }

  first(columns?: string[]): DataFrame {
    return this.aggregatePandas("first", columns);
  }

  last(columns?: string[]): DataFrame {
    return this.aggregatePandas("last", columns);
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

  /** Cumulative product within each group. */
  cumprod(columns?: string[]): DataFrame {
    return this.groupCumulative("cumprod", columns);
  }

  private groupCumulative(
    kind: "cumsum" | "cummax" | "cummin" | "cumprod",
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
          const prev = accs.get(column) ?? (kind === "cumprod" ? 1 : 0);
          let updated: number;
          if (kind === "cumsum") {
            updated = prev + value;
          } else if (kind === "cummax") {
            updated = accs.has(column) ? Math.max(prev, value) : value;
          } else if (kind === "cummin") {
            updated = accs.has(column) ? Math.min(prev, value) : value;
          } else {
            updated = accs.has(column) ? prev * value : value;
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
      throw new KeyError(`'${column}'`);
    }
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

  // ── 13 missing GroupBy APIs ──────────────────────────────────────────

  /** Per-group unbiased skewness (sample-corrected Fisher-Pearson G1). */
  skew(columns?: string[]): DataFrame {
    return this.aggregatePandas("skew", columns);
  }

  /** Per-group excess kurtosis (Fisher, kurtosis() alias). */
  kurt(columns?: string[]): DataFrame {
    return this.kurtosis(columns);
  }

  kurtosis(columns?: string[]): DataFrame {
    return this.aggregatePandas("kurt", columns);
  }

  /** Per-group standard error of the mean (sample std / sqrt(n)). */
  sem(columns?: string[]): DataFrame {
    return this.aggregatePandas("sem", columns);
  }

  /** Per-group percentage change within each group (same periods as pct_change). */
  pct_change(periods = 1, columns?: string[]): DataFrame {
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
            typeof b === "number" && Number.isFinite(b) &&
            b !== 0
          ) {
            next[column] = (a - b) / b;
          } else {
            next[column] = null;
          }
        }
        outRows[targetPosition] = next;
      }
    }
    return DataFrame.from_normalized(outRows as Row[], [...this.by, ...candidates], range(outRows.length));
  }

  /** Per-group rank (aligned to source rows, same signature as DataFrame.rank). */
  rank(options: RankOptions = {}, columns?: string[]): DataFrame {
    const candidates = columns ?? this.numericColumns();
    const groups = this.buildGroupPositions();
    const outRows: Row[] = new Array(this.sourceRows.length);
    for (let i = 0; i < outRows.length; i += 1) {
      outRows[i] = {} as Row;
      for (const col of this.by) {
        outRows[i]![col] = this.sourceRows[i]![col];
      }
      for (const col of candidates) {
        outRows[i]![col] = null;
      }
    }
    for (const group of groups.values()) {
      for (const col of candidates) {
        const values = group.positions.map((p) => this.sourceRows[p]![col] ?? null);
        const ranks = computeRank(values, options as any);
        group.positions.forEach((pos, idx) => {
          outRows[pos]![col] = ranks[idx] ?? null;
        });
      }
    }
    return DataFrame.from_normalized(outRows as Row[], [...this.by, ...candidates], [...this.source.index]);
  }

  /** Per-group index label of the max value (returns aggregated frame, one row per group). */
  idxmax(columns?: string[]): DataFrame {
    return this.groupIdx(columns, "max");
  }

  /** Per-group index label of the min value (returns aggregated frame, one row per group). */
  idxmin(columns?: string[]): DataFrame {
    return this.groupIdx(columns, "min");
  }

  /** Per-group pairwise correlation (numeric columns, same as df.corr() per group — returns stacked frame). */
  corr(): DataFrame {
    return this.pairwisePerGroup(pearson);
  }

  /** Per-group pairwise covariance (numeric columns). */
  cov(): DataFrame {
    return this.pairwisePerGroup(covariance);
  }

  /** OHLC summary per group: open/high/low/close of each numeric column. */
  ohlc(columns?: string[]): DataFrame {
    const candidates = columns ?? this.numericColumns();
    const groups = this.sortGroups([...this.getGroups().values()]);
    const rows: Row[] = [];
    for (const group of groups) {
      const row: Row = {};
      for (let i = 0; i < this.by.length; i += 1) row[this.by[i]!] = group.keyValues[i];
      for (const col of candidates) {
        const nums = group.rows.map((r) => r[col]).filter((v): v is number => typeof v === "number" && Number.isFinite(v));
        if (nums.length === 0) {
          row[`${col}_open`] = null; row[`${col}_high`] = null; row[`${col}_low`] = null; row[`${col}_close`] = null;
        } else {
          row[`${col}_open`] = nums[0]; row[`${col}_high`] = Math.max(...nums); row[`${col}_low`] = Math.min(...nums); row[`${col}_close`] = nums[nums.length - 1]!;
        }
      }
      rows.push(row);
    }
    const valueCols = candidates.flatMap((c) => [`${c}_open`, `${c}_high`, `${c}_low`, `${c}_close`]);
    return this.materializeGroupedRows(rows, valueCols);
  }

  /** Stub — GroupBy resampling requires a DatetimeIndex (MutliIndex/Rolling path not yet implemented). */
  resample(_rule: string): never {
    throw new Error("GroupBy.resample() requires a DatetimeIndex — not yet implemented (MultiIndex/Rolling resample path).");
  }

  /**
   * GroupBy rolling — groupby().rolling(window).mean() pattern.
   * Each group's values are rolled independently, aligned to source index.
   */
  rolling(window: number, minPeriods?: number): RollingGroupBy {
    const candidates = this.numericColumns();
    const groups = this.buildGroupPositions();
    const wrap = (key: string) => {
      return () => {
        const outRows: Row[] = new Array(this.sourceRows.length);
        for (let i = 0; i < outRows.length; i += 1) {
          outRows[i] = {} as Row;
          for (const col of this.by) outRows[i]![col] = this.sourceRows[i]![col];
          for (const col of candidates) outRows[i]![col] = null;
        }
        for (const group of groups.values()) {
          for (const col of candidates) {
            const vals = group.positions.map((p) => this.sourceRows[p]![col]);
            const rollingVals = computeRolling(vals, window, minPeriods);
            const res = (() => {
              switch (key) {
                case "mean": return rollingVals.mean();
                case "sum": return rollingVals.sum();
                case "min": return rollingVals.min();
                case "max": return rollingVals.max();
                case "count": return rollingVals.count();
                case "std": return rollingVals.std();
                default: return rollingVals.mean();
              }
            })();
            group.positions.forEach((pos, idx) => { outRows[pos]![col] = res[idx] ?? null; });
          }
        }
        return DataFrame.from_normalized(outRows as Row[], [...this.by, ...candidates], [...this.source.index]);
      };
    };
    return {
      mean: wrap("mean"),
      sum: wrap("sum"),
      min: wrap("min"),
      max: wrap("max"),
      count: wrap("count"),
      std: wrap("std"),
    };
  }

  // ── helpers for the new APIs ─────────────────────────────────────────

  private groupAggNumeric(
    columns: string[] | undefined,
    reducer: (nums: number[]) => number | null
  ): DataFrame {
    const candidates = columns ?? this.numericColumns();
    const groups = this.sortGroups([...this.getGroups().values()]);
    const rows: Row[] = [];
    for (const group of groups) {
      const row: Row = {};
      for (let i = 0; i < this.by.length; i += 1) row[this.by[i]!] = group.keyValues[i];
      for (const col of candidates) {
        const nums = numericValues(group.rows.map((r) => r[col]));
        row[col] = reducer(nums);
      }
      rows.push(row);
    }
    return this.materializeGroupedRows(rows, candidates);
  }

  private groupIdx(columns: string[] | undefined, mode: "max" | "min"): DataFrame {
    const candidates = columns ?? this.numericColumns();
    const groups = this.sortGroups([...this.getGroups().values()]);
    const rows: Row[] = [];
    for (const group of groups) {
      const row: Row = {};
      for (let i = 0; i < this.by.length; i += 1) row[this.by[i]!] = group.keyValues[i];
      for (const col of candidates) {
        // Find index position (source index label) of the extreme within the group
        let best: number | null = null;
        let bestPos = -1;
        group.rows.forEach((r) => {
          const v = r[col];
          if (typeof v !== "number" || !Number.isFinite(v)) return;
          if (best === null || (mode === "max" ? v > best : v < best)) { best = v; bestPos = this.sourceRows.indexOf(r); }
        });
        row[col] = bestPos >= 0 ? (this.source.index[bestPos] ?? bestPos) as CellValue : null;
      }
      rows.push(row);
    }
    return this.materializeGroupedRows(rows, candidates);
  }

  private pairwisePerGroup(fn: (a: number[], b: number[]) => number): DataFrame {
    const numericCols = this.numericColumns();
    const groups = this.sortGroups([...this.getGroups().values()]);
    const rows: Row[] = [];
    for (const group of groups) {
      for (const rowVar of numericCols) {
        const row: Row = {};
        for (let i = 0; i < this.by.length; i += 1) row[this.by[i]!] = group.keyValues[i];
        row["variable"] = rowVar;
        for (const colVar of numericCols) {
          if (rowVar === colVar) { row[colVar] = 1; continue; }
          const pairs = group.rows
            .map((r) => [r[rowVar], r[colVar]] as const)
            .filter((pair): pair is [number, number] =>
              typeof pair[0] === "number" && Number.isFinite(pair[0]) &&
              typeof pair[1] === "number" && Number.isFinite(pair[1])
            );
          if (pairs.length < 2) { row[colVar] = null; continue; }
          const aa = pairs.map((p) => p[0]);
          const bb = pairs.map((p) => p[1]);
          const val = fn(aa, bb);
          row[colVar] = Number.isNaN(val) ? null : val;
        }
        rows.push(row);
      }
    }
    const valueCols = [...numericCols, "variable"];
    if (rows.length === 0) return this.materializeGroupedRows([], valueCols);
    const sampleRow = rows[0]!;
    const allCols = Object.keys(sampleRow);
    // Re-materialize directly to avoid materializeGroupedRows re-filtering
    if (!this.options.as_index) {
      return DataFrame.from_normalized(rows, [...this.by, ...allCols.filter((c) => !this.by.includes(c))]);
    }
    return DataFrame.from_normalized(rows, [...this.by, ...allCols.filter((c) => !this.by.includes(c))]);
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

  /**
   * pandas-faithful named aggregation: every non-key column is attempted
   * (a column that can't be aggregated raises, like pandas), the declared
   * source dtype drives both the value semantics and the result dtype.
   */
  private aggregatePandas(op: PandasAggOp, columns?: string[]): DataFrame {
    const candidates = columns ?? this.nonKeyColumns();
    const srcDtypes = this.source.dtypes() as Record<string, DType>;
    const groups = this.sortGroups([...this.getGroups().values()]);
    const rows: Row[] = [];
    for (const group of groups) {
      const row: Row = {};
      for (let i = 0; i < this.by.length; i += 1) {
        row[this.by[i]!] = group.keyValues[i];
      }
      for (const col of candidates) {
        const dtype = (srcDtypes[col] ?? "object") as DType;
        row[col] = groupbyAggValue(op, group.rows.map((r) => r[col]), dtype);
      }
      rows.push(row);
    }
    const declared: Record<string, DType> = {};
    for (const key of this.by) {
      if (srcDtypes[key]) declared[key] = srcDtypes[key]!;
    }
    for (const col of candidates) {
      declared[col] = groupbyResultDType(op, (srcDtypes[col] ?? "object") as DType);
    }
    if (!this.options.as_index) {
      return DataFrame.createInternal(rows, [...this.by, ...candidates], range(rows.length), {}, declared);
    }

    // pandas as_index=true: keys move into the index (tuple keys for
    // multi-key groups) and the value columns carry the declared dtypes.
    const outRows = rows.map((row) => {
      const out: Row = {};
      for (const col of candidates) {
        out[col] = row[col];
      }
      return out;
    });
    const outDeclared = Object.fromEntries(
      candidates.map((col) => [col, declared[col]!])
    );
    if (this.by.length === 1) {
      const keyColumn = this.by[0]!;
      const index = rows.map((row, position) => toIndexLabel(row[keyColumn], position));
      return DataFrame.createInternal(
        outRows,
        candidates,
        index,
        {},
        outDeclared,
        this.levelIndexNames?.[0] ?? this.by[0]
      );
    }
    const index = rows.map((row, position) =>
      this.by.map((keyColumn) => toIndexLabel(row[keyColumn], position) as string | number));
    return DataFrame.createInternal(
      outRows,
      candidates,
      index,
      {},
      outDeclared,
      undefined,
      this.levelIndexNames ?? this.by
    );
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
          const subSeries = new Series<CellValue>(
            groupRows.map((row) => row[column] ?? null) as CellValue[],
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
    const levelKeys = this.levelKeys;

    if (levelKeys) {
      for (let position = 0; position < this.sourceRows.length; position += 1) {
        const keyValues = levelKeys[position]!;
        if (this.options.dropna && keyValues.some((value) => isMissing(value))) {
          continue;
        }
        const key = keyForValues(keyValues);
        const existing = groups.get(key);
        if (existing) {
          existing.positions.push(position);
        } else {
          groups.set(key, { keyValues: [...keyValues], positions: [position] });
        }
      }
      return groups;
    }

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
    const levelKeys = this.levelKeys;

    if (levelKeys) {
      for (let position = 0; position < this.sourceRows.length; position += 1) {
        const keyValues = levelKeys[position]!;
        if (this.options.dropna && keyValues.some((value) => isMissing(value))) {
          continue;
        }
        const key = keyForValues(keyValues);
        const existing = groups.get(key);
        if (existing) {
          existing.rows.push(this.sourceRows[position]!);
        } else {
          groups.set(key, {
            keyValues: [...keyValues],
            rows: [this.sourceRows[position]!],
          });
        }
      }
      return groups;
    }

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
        if (isNumber(row[column]) || typeof row[column] === "bigint") {
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
    const outRows = rows.map((row) => {
      const out: Row = {};
      for (const column of valueColumns) {
        out[column] = row[column];
      }
      return out;
    });

    if (this.by.length === 1) {
      const keyColumn = this.by[0]!;
      const index = rows.map((row, position) => toIndexLabel(row[keyColumn], position));
      const frame = DataFrame.from_normalized(outRows, valueColumns, index);
      if (this.levelIndexNames?.[0] !== undefined) {
        return frame.rename_axis(this.levelIndexNames[0]);
      }
      return frame;
    }

    // Multi-key groups produce a tuple index like pandas MultiIndex.
    const index = rows.map((row, position) =>
      this.by.map((keyColumn) => toIndexLabel(row[keyColumn], position) as string | number));
    return DataFrame.createInternal(outRows, valueColumns, index, {}, {}, this.levelIndexNames?.[0], this.levelIndexNames ?? this.by);
  }
}


type PandasAggOp =
  | "sum" | "prod" | "mean" | "median" | "min" | "max"
  | "count" | "nunique" | "first" | "last"
  | "std" | "var" | "sem" | "skew" | "kurt";

/** pandas groupby result dtype per (op, source dtype). */
function groupbyResultDType(op: PandasAggOp, src: DType): DType {
  switch (op) {
    case "sum":
    case "prod":
      if (src === "bool") return "int64";
      if (src === "boolean") return "Int64";
      return src;
    case "mean":
    case "median":
    case "std":
    case "var":
    case "sem":
    case "skew":
    case "kurt":
      if (src === "float32") return "float32";
      if (src === "Float32") return "Float32";
      return isMaskedDType(src) ? "Float64" : "float64";
    case "count":
      // Nullable numeric/boolean columns count into Int64; the StringDtype
      // column counts into plain int64.
      return isMaskedDType(src) && src !== "string" ? "Int64" : "int64";
    case "nunique":
      return "int64";
    default:
      // min/max/first/last preserve the source dtype.
      return src;
  }
}

/** Per-group value for a column, following pandas Series semantics. */
function groupbyAggValue(op: PandasAggOp, values: CellValue[], dtype: DType): CellValue {
  const resDtype = groupbyResultDType(op, dtype);
  const sentinel = (): CellValue => missingScalar(resDtype);
  const f32 = resDtype === "float32" || resDtype === "Float32";
  const round = (v: number | null | undefined): CellValue =>
    v === null || v === undefined ? sentinel() : (f32 ? Math.fround(v) : v);
  switch (op) {
    case "sum": {
      const v = pandasSum(values, dtype);
      return typeof v === "number" && f32 ? Math.fround(v) : v;
    }
    case "prod": {
      const v = pandasProd(values, dtype);
      return typeof v === "number" && f32 ? Math.fround(v) : v;
    }
    case "count":
      return values.filter((v) => !isMissing(v)).length;
    case "nunique": {
      const seen = new Set<string>();
      for (const v of values) if (!isMissing(v)) seen.add(scalarKey(v));
      return seen.size;
    }
    case "first": {
      const first = values.find((v) => !isMissing(v));
      return first === undefined ? missingScalar(dtype) : first;
    }
    case "last": {
      for (let i = values.length - 1; i >= 0; i -= 1) {
        if (!isMissing(values[i])) return values[i]!;
      }
      return missingScalar(dtype);
    }
    case "min":
      return pandasMinMax(values, dtype, "min");
    case "max":
      return pandasMinMax(values, dtype, "max");
    case "mean": {
      const nums = requireNumeric(values, dtype, "mean");
      return nums.length === 0 ? sentinel() : round(nums.reduce((a, b) => a + b, 0) / nums.length);
    }
    case "median": {
      const nums = requireNumeric(values, dtype, "median").sort((a, b) => a - b);
      if (nums.length === 0) return sentinel();
      const pos = (nums.length - 1) * 0.5;
      const lo = Math.floor(pos);
      const hi = Math.ceil(pos);
      return round(nums[lo]! + (nums[hi]! - nums[lo]!) * (pos - lo));
    }
    case "std": {
      if (dtype === "object") throw new ValueError("could not convert string to float");
      const nums = requireNumeric(values, dtype, "std");
      return nums.length <= 1 ? sentinel() : round(std(nums));
    }
    case "var": {
      const nums = requireNumeric(values, dtype, "var");
      return nums.length <= 1 ? sentinel() : round(seriesVariance(nums, 1));
    }
    case "sem": {
      if (dtype === "object") throw new ValueError("could not convert string to float");
      const nums = requireNumeric(values, dtype, "sem");
      const variance = seriesVariance(nums, 1);
      return nums.length <= 1 || variance === null ? sentinel() : round(Math.sqrt(variance / nums.length));
    }
    case "skew": {
      if (dtype === "object") throw new ValueError("could not convert string to float");
      const nums = requireNumeric(values, dtype, "skew");
      return round(adjustedSkew(nums));
    }
    case "kurt": {
      if (dtype === "object") throw new ValueError("could not convert string to float");
      const nums = requireNumeric(values, dtype, "kurt");
      return round(excessKurtosis(nums));
    }
  }
}

const FAST_AGG_NAMES = new Set<AggName>(["count", "sum", "mean", "min", "max"]);

const AGG_COUNT = 1;
const AGG_SUM = 2;
const AGG_MEAN = 3;
const AGG_MIN = 4;
const AGG_MAX = 5;

function aggCodeForName(name: AggName): number {
  if (name === "count") return AGG_COUNT;
  if (name === "sum") return AGG_SUM;
  if (name === "mean") return AGG_MEAN;
  if (name === "min") return AGG_MIN;
  return AGG_MAX;
}

function groupCacheKey(by: string[], dropna: boolean): string {
  return `${dropna ? "1" : "0"}|${by.join("\u001f")}`;
}
