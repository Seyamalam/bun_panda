import { DataFrame } from "./dataframe";
import type { AggFn, AggName, AggSpec, CellValue, Row } from "./types";
import { compareCellValues, isMissing, isNumber } from "./utils";

interface GroupEntry {
  keyValues: CellValue[];
  rows: Row[];
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

    for (const group of this.grouped.values()) {
      const row: Row = {};
      for (let i = 0; i < this.by.length; i += 1) {
        row[this.by[i]!] = group.keyValues[i];
      }

      for (const [column, aggregator] of Object.entries(spec)) {
        if (typeof aggregator === "function") {
          const values: CellValue[] = [];
          for (const entry of group.rows) {
            values.push(entry[column]);
          }
          row[column] = (aggregator as AggFn)(values, group.rows);
        } else {
          row[column] = runNamedAggregationOnRows(group.rows, column, aggregator as AggName);
        }
      }

      rows.push(row);
    }

    return new DataFrame(rows, {
      columns: [...this.by, ...aggColumns],
    });
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
    for (const row of this.sourceRows) {
      const keyValues = this.by.map((column) => row[column]);
      const key = keyForValues(keyValues);
      const existing = groups.get(key);
      if (existing) {
        existing.rows.push(row);
      } else {
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

function runNamedAggregationOnRows(rows: Row[], column: string, name: AggName): CellValue {
  if (name === "count") {
    let count = 0;
    for (const row of rows) {
      if (!isMissing(row[column])) {
        count += 1;
      }
    }
    return count;
  }

  if (name === "sum") {
    let hasAny = false;
    let sum = 0;
    for (const row of rows) {
      const value = row[column];
      if (isNumber(value)) {
        hasAny = true;
        sum += value;
      }
    }
    return hasAny ? sum : null;
  }

  if (name === "mean") {
    let count = 0;
    let sum = 0;
    for (const row of rows) {
      const value = row[column];
      if (isNumber(value)) {
        count += 1;
        sum += value;
      }
    }
    return count > 0 ? sum / count : null;
  }

  if (name === "min") {
    let best: CellValue = null;
    let seen = false;
    for (const row of rows) {
      const value = row[column];
      if (isMissing(value)) {
        continue;
      }
      if (!seen || compareCellValues(value, best) < 0) {
        best = value;
        seen = true;
      }
    }
    return seen ? best : null;
  }

  if (name === "max") {
    let best: CellValue = null;
    let seen = false;
    for (const row of rows) {
      const value = row[column];
      if (isMissing(value)) {
        continue;
      }
      if (!seen || compareCellValues(value, best) > 0) {
        best = value;
        seen = true;
      }
    }
    return seen ? best : null;
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

function keyForValues(values: CellValue[]): string {
  let key = "";
  for (const value of values) {
    const normalized = normalizeKeyCell(value);
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
