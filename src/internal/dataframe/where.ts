import type { DataFrame } from "../../dataframe";
import { Series } from "../../series";
import type {
  AggFn,
  AggName,
  AggSpec,
  CellValue,
  IndexLabel,
  Row,
} from "../../types";
import { isMissing, median, numericValues, std, variance } from "../../utils";
import { normalizeKeyCell } from "./keys";

export type WherePredicate = (
  row: Row,
  label: IndexLabel,
  position: number
) => boolean;

export type WhereCondition =
  | WherePredicate
  | boolean[]
  | Record<string, boolean[]>
  | DataFrame;

export type WhereOtherFn = (
  value: CellValue,
  column: string,
  label: IndexLabel,
  position: number
) => CellValue;

export type WhereOther = CellValue | Record<string, CellValue> | WhereOtherFn;

export type TransformFn = (
  column: Series<CellValue>,
  name: string,
  position: number
) => CellValue | CellValue[];

export type TransformInput = TransformFn | AggSpec;

type CellDecision = (
  row: Row,
  column: string,
  label: IndexLabel,
  position: number
) => boolean;

export function computeWhereRows(
  sourceRows: Row[],
  columns: string[],
  index: IndexLabel[],
  cond: WhereCondition,
  other: WhereOther,
  invert: boolean
): Row[] {
  const decision = resolveCondition(sourceRows, columns, cond);

  return sourceRows.map((row, position) => {
    const label = index[position]!;
    const out: Row = {};
    for (const column of columns) {
      const value = row[column];
      const holds = decision(row, column, label, position);
      const keep = invert ? !holds : holds;
      out[column] = keep ? value : resolveOther(other, value, column, label, position);
    }
    return out;
  });
}

export function computeTransformRows(
  sourceRows: Row[],
  columns: string[],
  index: IndexLabel[],
  input: TransformInput
): Row[] {
  const rowCount = sourceRows.length;
  const out: Row[] = Array.from({ length: rowCount }, () => ({}));

  if (typeof input === "function") {
    for (let position = 0; position < columns.length; position += 1) {
      const column = columns[position]!;
      const values = sourceRows.map((row) => row[column]);
      const series = new Series(values, { index, name: column });
      const result = input(series, column, position);
      if (Array.isArray(result)) {
        if (result.length !== rowCount) {
          throw new Error("transform function must return a value per row.");
        }
        for (let i = 0; i < rowCount; i += 1) {
          out[i]![column] = result[i]!;
        }
      } else {
        for (let i = 0; i < rowCount; i += 1) {
          out[i]![column] = result;
        }
      }
    }
    return out;
  }

  for (const [column, spec] of Object.entries(input)) {
    const values = sourceRows.map((row) => row[column]);
    const aggregated =
      typeof spec === "function"
        ? (spec as AggFn)(values, sourceRows)
        : finalizeNamedAggValues(spec as AggName, values);
    for (let i = 0; i < rowCount; i += 1) {
      out[i]![column] = aggregated;
    }
  }
  return out;
}

export function finalizeNamedAggValues(name: AggName, values: CellValue[]): CellValue {
  switch (name) {
    case "count":
      return values.filter((value) => !isMissing(value)).length;
    case "nunique": {
      const seen = new Set<string>();
      for (const value of values) {
        if (isMissing(value)) {
          continue;
        }
        seen.add(JSON.stringify(normalizeKeyCell(value)));
      }
      return seen.size;
    }
    case "first":
      return values.find((value) => !isMissing(value)) ?? null;
    case "last": {
      for (let i = values.length - 1; i >= 0; i -= 1) {
        if (!isMissing(values[i])) {
          return values[i]!;
        }
      }
      return null;
    }
    default:
      return finalizeNumericAggValues(name, numericValues(values));
  }
}

function finalizeNumericAggValues(name: AggName, numbers: number[]): CellValue {
  switch (name) {
    case "sum":
      return numbers.length > 0 ? numbers.reduce((acc, value) => acc + value, 0) : null;
    case "mean":
      return numbers.length > 0
        ? numbers.reduce((acc, value) => acc + value, 0) / numbers.length
        : null;
    case "min":
      return numbers.length > 0 ? Math.min(...numbers) : null;
    case "max":
      return numbers.length > 0 ? Math.max(...numbers) : null;
    case "median":
      return median(numbers);
    case "std":
      return std(numbers);
    case "var":
      return variance(numbers);
    default:
      throw new Error(`Unsupported aggregation '${name}'.`);
  }
}

function resolveCondition(
  sourceRows: Row[],
  columns: string[],
  cond: WhereCondition
): CellDecision {
  if (typeof cond === "function") {
    return (row, _column, label, position) => cond(row, label, position);
  }

  if (Array.isArray(cond)) {
    if (cond.length !== sourceRows.length) {
      throw new Error("Mask length must match row count.");
    }
    return (_row, _column, _label, position) => cond[position]!;
  }

  if (isBooleanFrame(cond)) {
    const records = cond.to_records();
    if (
      JSON.stringify(cond.columns) !== JSON.stringify(columns) ||
      records.length !== sourceRows.length
    ) {
      throw new Error("where/mask condition shape must match frame.");
    }
    const grid = records.map((record) =>
      columns.map((column) => record[column] === true)
    );
    return (_row, column, _label, position) => grid[position]![columns.indexOf(column)]!;
  }

  const masks = cond as Record<string, boolean[]>;
  for (const mask of Object.values(masks)) {
    if (mask.length !== sourceRows.length) {
      throw new Error("Mask length must match row count.");
    }
  }
  return (_row, column, _label, position) => masks[column]?.[position] ?? true;
}

function isBooleanFrame(
  input: Exclude<WhereCondition, WherePredicate | boolean[]>
): input is DataFrame {
  return typeof (input as DataFrame).to_records === "function";
}

function resolveOther(
  other: WhereOther,
  value: CellValue,
  column: string,
  label: IndexLabel,
  position: number
): CellValue {
  if (typeof other === "function") {
    return other(value, column, label, position);
  }
  if (isPlainObject(other)) {
    return other[column] ?? value;
  }
  return other;
}

function isPlainObject(input: WhereOther): input is Record<string, CellValue> {
  return (
    typeof input === "object" && input !== null && !(input instanceof Date)
  );
}
