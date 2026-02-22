import { DataFrame } from "./dataframe";
import { keyFragment } from "./internal/dataframe/keys";
import type { AggFn, AggName, AggSpec, CellValue, IndexLabel, Row } from "./types";
import { compareCellValues, isMissing, isNumber } from "./utils";

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

interface NamedAggState {
  count: number;
  sum: number;
  hasAny: boolean;
  seen: boolean;
  best: CellValue;
}

interface FastGroupState {
  keyValues: CellValue[];
  namedStates: NamedAggState[];
}

export interface GroupByOptions {
  dropna?: boolean;
  sort?: boolean;
  as_index?: boolean;
}

export class GroupBy {
  private readonly source: DataFrame;
  private readonly by: string[];
  private grouped: Map<string, GroupEntry> | null;
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
    if (by.length === 0) {
      throw new Error("groupby requires at least one key column.");
    }
    for (const column of by) {
      if (!source.columns.includes(column)) {
        throw new Error(`Column '${column}' does not exist.`);
      }
    }

    this.source = source;
    this.by = by;
    this.sourceRows = sourceRows ?? source.to_records();
    this.sourceColumns = sourceColumns ?? source.columns;
    this.options = {
      dropna: options.dropna ?? true,
      sort: options.sort ?? true,
      as_index: options.as_index ?? false,
    };
    this.grouped = null;
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

    if (customPlans.length === 0 && namedPlans.length > 0) {
      return this.fastNamedAgg(namedPlans, aggColumns);
    }

    const groups = this.sortGroups([...this.getGroups().values()]);
    const rows: Row[] = [];

    for (const group of groups) {
      const row: Row = {};
      for (let i = 0; i < this.by.length; i += 1) {
        row[this.by[i]!] = group.keyValues[i];
      }

      const namedStates = namedPlans.map(() => createNamedAggState());
      const customValues = customPlans.map(() => [] as CellValue[]);

      for (const sourceRow of group.rows) {
        for (let i = 0; i < namedPlans.length; i += 1) {
          const plan = namedPlans[i]!;
          const state = namedStates[i]!;
          updateNamedAggState(state, plan.name, sourceRow[plan.column]);
        }
        for (let i = 0; i < customPlans.length; i += 1) {
          const plan = customPlans[i]!;
          customValues[i]!.push(sourceRow[plan.column]);
        }
      }

      for (let i = 0; i < namedPlans.length; i += 1) {
        const plan = namedPlans[i]!;
        row[plan.column] = finalizeNamedAggState(namedStates[i]!, plan.name);
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
    const states = new Map<string, FastGroupState>();
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
            namedStates: namedPlans.map(() => createNamedAggState()),
          };
          states.set(key, state);
        }
        updateNamedPlans(state.namedStates, namedPlans, sourceRow);
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
            namedStates: namedPlans.map(() => createNamedAggState()),
          };
          states.set(key, state);
        }
        updateNamedPlans(state.namedStates, namedPlans, sourceRow);
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
        row[plan.column] = finalizeNamedAggState(group.namedStates[i]!, plan.name);
      }
      rows.push(row);
    }

    return this.materializeGroupedRows(rows, aggColumns);
  }

  private getGroups(): Map<string, GroupEntry> {
    if (!this.grouped) {
      this.grouped = this.buildGroups();
    }
    return this.grouped;
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

function createNamedAggState(): NamedAggState {
  return {
    count: 0,
    sum: 0,
    hasAny: false,
    seen: false,
    best: null,
  };
}

function updateNamedAggState(state: NamedAggState, name: AggName, value: CellValue): void {
  if (name === "count") {
    if (!isMissing(value)) {
      state.count += 1;
    }
    return;
  }

  if (name === "sum" || name === "mean") {
    if (isNumber(value)) {
      state.hasAny = true;
      state.count += 1;
      state.sum += value;
    }
    return;
  }

  if (name === "min") {
    if (!isMissing(value) && (!state.seen || compareCellValues(value, state.best) < 0)) {
      state.best = value;
      state.seen = true;
    }
    return;
  }

  if (name === "max") {
    if (!isMissing(value) && (!state.seen || compareCellValues(value, state.best) > 0)) {
      state.best = value;
      state.seen = true;
    }
  }
}

function finalizeNamedAggState(state: NamedAggState, name: AggName): CellValue {
  if (name === "count") {
    return state.count;
  }
  if (name === "sum") {
    return state.hasAny ? state.sum : null;
  }
  if (name === "mean") {
    return state.count > 0 ? state.sum / state.count : null;
  }
  if (name === "min" || name === "max") {
    return state.seen ? state.best : null;
  }
  return null;
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

function updateNamedPlans(
  states: NamedAggState[],
  plans: NamedAggPlan[],
  row: Row
): void {
  for (let i = 0; i < plans.length; i += 1) {
    const plan = plans[i]!;
    updateNamedAggState(states[i]!, plan.name, row[plan.column]);
  }
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
