import { DataFrame } from "./dataframe";
import type { AggFn, AggName, AggSpec, CellValue, Row } from "./types";
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

export class GroupBy {
  private readonly source: DataFrame;
  private readonly by: string[];
  private readonly grouped: Map<string, GroupEntry>;
  private readonly sourceRows: Row[];
  private readonly sourceColumns: string[];

  constructor(source: DataFrame, by: string[], sourceRows?: Row[], sourceColumns?: string[]) {
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
    this.grouped = this.buildGroups();
  }

  agg(spec: AggSpec): DataFrame {
    const rows: Row[] = [];
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

    for (const group of this.grouped.values()) {
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

    return DataFrame.from_normalized(rows, [...this.by, ...aggColumns]);
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

  private buildGroups(): Map<string, GroupEntry> {
    const groups = new Map<string, GroupEntry>();

    if (this.by.length === 1) {
      const keyColumn = this.by[0]!;
      for (const row of this.sourceRows) {
        const keyValue = row[keyColumn];
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

function keyForRow(row: Row, columns: string[]): string {
  let key = "";
  for (const column of columns) {
    const normalized = normalizeKeyCell(row[column]);
    if (normalized === null) {
      key += "n:;";
    } else if (typeof normalized === "number") {
      key += `d:${normalized};`;
    } else if (typeof normalized === "boolean") {
      key += `b:${normalized ? 1 : 0};`;
    } else {
      key += `s${normalized.length}:${normalized};`;
    }
  }
  return key;
}

function keyForSingleValue(value: CellValue): string {
  const normalized = normalizeKeyCell(value);
  if (normalized === null) {
    return "n:;";
  }
  if (typeof normalized === "number") {
    return `d:${normalized};`;
  }
  if (typeof normalized === "boolean") {
    return `b:${normalized ? 1 : 0};`;
  }
  return `s${normalized.length}:${normalized};`;
}
