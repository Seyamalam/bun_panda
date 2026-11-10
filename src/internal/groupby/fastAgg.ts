import { DataFrame } from "../../dataframe";
import { keyFragment, normalizeKeyCell } from "../dataframe/keys";
import { bigintOperand } from "../shared/valueSemantics";
import {
  wasmAggregateColumn,
  wasmAggMultiF64,
  wasmGroupIds,
} from "../../wasm/kernel";
import { buildColumnStore } from "../../wasm/columns";
import { chooseExecutionPath } from "../../wasm/dispatch";
import type { AggName, CellValue, IndexLabel, Row } from "../../types";
import {
  compareCellValues,
  isMissing,
  isNumber,
  kahanSum,
  median,
  numericValues,
  std,
  variance,
} from "../../utils";

interface NamedAggPlan {
  column: string;
  name: AggName;
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

const AGG_COUNT = 1;
const AGG_SUM = 2;
const AGG_MEAN = 3;
const AGG_MIN = 4;
const AGG_MAX = 5;

export function fastAggCodeForName(name: AggName): number {
  if (name === "count") return AGG_COUNT;
  if (name === "sum") return AGG_SUM;
  if (name === "mean") return AGG_MEAN;
  if (name === "min") return AGG_MIN;
  return AGG_MAX;
}

export function fastWasmCodeForName(name: AggName): number | null {
  if (name === "count") return 4;
  if (name === "sum") return 0;
  if (name === "mean") return 1;
  if (name === "min") return 2;
  if (name === "max") return 3;
  return null;
}

export function finalizeNamedAggValues(name: AggName, values: CellValue[]): CellValue {
  if (name === "count") {
    let count = 0;
    for (const value of values) if (!isMissing(value)) count += 1;
    return count;
  }
  if (name === "sum" || name === "mean") {
    if (values.some((value) => typeof value === "bigint")) {
      // Exact accumulation: bigint cells start a bigint total; integer
      // numbers join it; non-integer numbers demote it to float.
      let wide: bigint | null = null;
      let counted = 0;
      const numbers: number[] = [];
      for (const value of values) {
        if (typeof value === "bigint") { wide = (wide ?? 0n) + value; counted += 1; continue; }
        const w = bigintOperand(value);
        if (wide !== null && w !== null) { wide += w; counted += 1; continue; }
        if (typeof value === "number" && Number.isFinite(value)) {
          if (wide !== null) { numbers.push(Number(wide)); wide = null; }
          numbers.push(value);
          counted += 1;
        }
      }
      if (wide !== null) {
        return name === "sum" ? wide : Number(wide) / counted;
      }
      if (numbers.length === 0) return null;
      const total = kahanSum(numbers);
      return name === "sum" ? total : total / numbers.length;
    }
    const numbers = numericValues(values);
    if (numbers.length === 0) return null;
    const total = kahanSum(numbers);
    return name === "sum" ? total : total / numbers.length;
  }
  if (name === "min" || name === "max") {
    let best: CellValue = null;
    for (const value of values) {
      if (isMissing(value)) continue;
      if (best === null) { best = value; continue; }
      const compared = compareCellValues(value, best);
      if ((name === "min" && compared < 0) || (name === "max" && compared > 0)) best = value;
    }
    return best;
  }
  if (name === "median") return median(numericValues(values));
  if (name === "std") return std(numericValues(values));
  if (name === "var") return variance(numericValues(values));
  if (name === "first") {
    for (const value of values) if (!isMissing(value)) return value;
    return null;
  }
  if (name === "last") {
    for (let i = values.length - 1; i >= 0; i -= 1) if (!isMissing(values[i]!)) return values[i]!;
    return null;
  }
  const seen = new Set<string>();
  for (const value of values) if (!isMissing(value)) seen.add(JSON.stringify(normalizeKeyCell(value)));
  return seen.size;
}

export function keyForRow(row: Row, columns: string[]): string {
  let key = "";
  for (const column of columns) key += keyFragment(row[column]);
  return key;
}

/** Same fragment encoding as keyForRow, but over an explicit value tuple. */
export function keyForValues(values: CellValue[]): string {
  let key = "";
  for (const value of values) key += keyFragment(value);
  return key;
}

export function keyForSingleValue(value: CellValue): string {
  return keyFragment(value);
}

export function hasMissingByValue(row: Row, columns: string[]): boolean {
  for (const column of columns) if (isMissing(row[column])) return true;
  return false;
}

export function updateFastGroupStates(
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
      if (!isMissing(value)) state.counts[i]! += 1;
      continue;
    }
    if (code === AGG_SUM || code === AGG_MEAN) {
      const wide = bigintOperand(value);
      if (typeof value === "bigint") {
        state.wideSums[i] = (state.wideSums[i] ?? 0n) + value;
        state.hasAny[i] = true;
        state.counts[i]! += 1;
        continue;
      }
      if (state.wideSums[i] !== null && wide !== null) {
        state.wideSums[i] = (state.wideSums[i] ?? 0n) + wide;
        state.hasAny[i] = true;
        state.counts[i]! += 1;
        continue;
      }
      if (isNumber(value)) {
        if (state.wideSums[i] !== null) {
          // A non-integer value demotes the running bigint total to float.
          state.sums[i]! += Number(state.wideSums[i]);
          state.wideSums[i] = null;
        }
        state.hasAny[i] = true;
        state.counts[i]! += 1;
        const y = value - state.comps[i]!;
        const t = state.sums[i]! + y;
        state.comps[i] = (t - state.sums[i]!) - y;
        state.sums[i] = t;
      }
      continue;
    }
    if (isMissing(value)) continue;
    if (!state.seen[i]) { state.best[i] = value; state.seen[i] = true; continue; }
    const compared = compareCellValues(value, state.best[i]);
    if ((code === AGG_MIN && compared < 0) || (code === AGG_MAX && compared > 0)) state.best[i] = value;
  }
}

export function shouldTryWasm(
  dropna: boolean,
  rowCount: number,
  planCount: number,
  typedColumnsReused = false
): boolean {
  return chooseExecutionPath({
    operation: "groupby-fused",
    rowCount,
    planCount,
    dropna,
    typedColumnsReused,
  }).path === "wasm";
}

export function tryWasmNamedAgg(
  sourceRows: Row[],
  by: string[],
  namedPlans: NamedAggPlan[],
  aggColumns: string[],
  materialize: (groups: { keyValues: CellValue[]; values: (number | null)[] }[], aggColumns: string[]) => DataFrame
): DataFrame | null {
  // Mirror GroupBy.tryWasmNamedAgg, but as a pure function.
  const grouped = wasmGroupIds(sourceRows, by);
  if (!grouped || grouped.groupCount === 0) return null;
  const firstRowOfGroup = new Int32Array(grouped.groupCount).fill(-1);
  for (let i = 0; i < grouped.ids.length; i += 1) {
    const g = grouped.ids[i]!;
    if (g >= 0 && firstRowOfGroup[g] === -1) firstRowOfGroup[g] = i;
  }
  if (firstRowOfGroup.some((row) => row === -1)) return null;
  const planColumns = [...new Set(namedPlans.map((plan) => plan.column))];
  const store = buildColumnStore(sourceRows, planColumns);
  const numericColumns: Float64Array[] = [];
  const wasmCodes: number[] = [];
  let columnar = true;
  for (const plan of namedPlans) {
    const code = fastWasmCodeForName(plan.name);
    const col = store.columns.get(plan.column);
    if (code === null || !col || col.kind !== "f64") { columnar = false; break; }
    numericColumns.push(col.values);
    wasmCodes.push(code);
  }
  if (columnar && numericColumns.length > 0) {
    const fused = wasmAggMultiF64(numericColumns, wasmCodes, grouped.ids, grouped.groupCount);
    if (fused) {
      const groups = [];
      for (let g = 0; g < grouped.groupCount; g += 1) {
        const sourceRow = sourceRows[firstRowOfGroup[g]!]!;
        const keyValues: CellValue[] = [];
        for (let i = 0; i < by.length; i += 1) keyValues.push(sourceRow[by[i]!]);
        const values = namedPlans.map((_, planIndex) => {
          const raw = fused.results[planIndex * grouped.groupCount + g]!;
          return Number.isNaN(raw) ? null : raw;
        });
        groups.push({ keyValues, values });
      }
      return materialize(groups, aggColumns);
    }
  }
  const perPlanResults: (Float64Array | null)[] = [];
  for (const plan of namedPlans) {
    const code = fastWasmCodeForName(plan.name);
    if (code === null) return null;
    let allNumeric = true;
    for (let i = 0; i < sourceRows.length; i += 1) {
      const value = sourceRows[i]![plan.column];
      if (!(value === null || value === undefined || isNumber(value))) { allNumeric = false; break; }
    }
    if (!allNumeric) return null;
    const result = wasmAggregateColumn(sourceRows, plan.column, grouped.ids, grouped.groupCount, code);
    if (!result) return null;
    perPlanResults.push(result);
  }
  const groups: { keyValues: CellValue[]; values: (number | null)[] }[] = [];
  for (let g = 0; g < grouped.groupCount; g += 1) {
    const sourceRow = sourceRows[firstRowOfGroup[g]!]!;
    const keyValues: CellValue[] = [];
    for (let i = 0; i < by.length; i += 1) keyValues.push(sourceRow[by[i]!]);
    const values = perPlanResults.map((result) => {
      const raw = result![g]!;
      return Number.isNaN(raw) ? null : raw;
    });
    groups.push({ keyValues, values });
  }
  return materialize(groups, aggColumns);
}

export function compareKeyValues(left: CellValue[], right: CellValue[]): number {
  const size = Math.min(left.length, right.length);
  for (let i = 0; i < size; i += 1) {
    const compared = compareCellValues(left[i], right[i]);
    if (compared !== 0) return compared;
  }
  return left.length - right.length;
}

export function toIndexLabel(value: CellValue, fallback: number): IndexLabel {
  if (typeof value === "number" || typeof value === "string") return value;
  return String(value ?? fallback);
}
