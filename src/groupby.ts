import { DataFrame } from "./dataframe";
import type { AggFn, AggName, AggSpec, CellValue, Row } from "./types";
import { compareCellValues, isMissing, numericValues } from "./utils";

interface GroupEntry {
  keyValues: CellValue[];
  rows: Row[];
}

export class GroupBy {
  private readonly source: DataFrame;
  private readonly by: string[];
  private readonly grouped: Map<string, GroupEntry>;

  constructor(source: DataFrame, by: string[]) {
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
        const values = group.rows.map((entry) => entry[column]);
        row[column] = typeof aggregator === "function"
          ? (aggregator as AggFn)(values, group.rows)
          : runNamedAggregation(values, aggregator as AggName);
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
    for (const row of this.source.to_records()) {
      const keyValues = this.by.map((column) => row[column]);
      const key = JSON.stringify(keyValues.map((value) => normalizeKeyCell(value)));
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
    return this.source.columns.filter((column) => {
      if (this.by.includes(column)) {
        return false;
      }
      const values = this.source.get(column).values;
      return numericValues(values).length > 0;
    });
  }
}

function runNamedAggregation(values: CellValue[], name: AggName): CellValue {
  if (name === "count") {
    return values.filter((value) => !isMissing(value)).length;
  }

  const numbers = numericValues(values);
  if (name === "sum") {
    return numbers.length > 0 ? numbers.reduce((acc, value) => acc + value, 0) : null;
  }

  if (name === "mean") {
    return numbers.length > 0 ? numbers.reduce((acc, value) => acc + value, 0) / numbers.length : null;
  }

  const nonMissing = values.filter((value) => !isMissing(value));
  if (nonMissing.length === 0) {
    return null;
  }

  if (name === "min") {
    return [...nonMissing].sort(compareCellValues)[0];
  }

  if (name === "max") {
    return [...nonMissing].sort(compareCellValues).at(-1) ?? null;
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
