// DataFrame-side window/time/export parity helpers built on shared engines.
import type { CellValue, IndexLabel, Row } from "../../types";
import { isMissing } from "../../utils";
import {
  ewmValues,
  parseFreqMs,
  resampleBins,
} from "../shared";

export type EwmKind = "mean" | "sum" | "std";

/** Column-wise ewm over a frame's numeric values. */
export function ewmColumn(
  values: CellValue[],
  span: number,
  minPeriods: number,
  kind: EwmKind
): (number | null)[] {
  const nums = values.map((v) =>
    typeof v === "number" && Number.isFinite(v) ? v : null
  );
  return ewmValues(nums, span, minPeriods, kind);
}

export interface ResampledBin {
  binStartMs: number;
  rows: Row[];
}

/** Bins frame rows on their first datetime-like column. */
export function resampleFrameRows(
  rows: Row[],
  columns: string[],
  rule: string
): ResampledBin[] {
  const freqMs = parseFreqMs(rule);
  const dateColumn = columns.find((c) =>
    rows.some((r) => r[c] instanceof Date)
  );
  if (!dateColumn) {
    throw new Error("resample: frame has no datetime-like column to bin on.");
  }
  const bins = resampleBins(
    rows.map((r) => r[dateColumn] as CellValue),
    freqMs
  );
  return bins.map((b) => ({
    binStartMs: b.binStartMs,
    rows: b.positions.map((p) => rows[p]!),
  }));
}

/** Reduces a binned column's numeric values. */
export function reduceBinColumn(
  bin: ResampledBin,
  column: string,
  fn: (nums: number[]) => number
): CellValue {
  const nums: number[] = [];
  for (const row of bin.rows) {
    const v = row[column];
    if (!isMissing(v) && typeof v === "number" && Number.isFinite(v)) nums.push(v);
  }
  return nums.length > 0 ? fn(nums) : null;
}

/** INSERT statements for the frame (to_sql). */
export function buildInsertStatements(
  rows: Row[],
  columns: string[],
  tableName: string
): string {
  return rows
    .map(
      (row) =>
        `INSERT INTO ${tableName} (${columns.join(", ")}) VALUES (${columns
          .map((c) => JSON.stringify(row[c] ?? null))
          .join(", ")});`
    )
    .join("\n");
}

/** Booktabs-style LaTeX table (to_latex). */
export function buildLatexTable(rows: Row[], columns: string[], index: IndexLabel[]): string {
  const escape = (v: unknown): string => latexCell(v);
  const header = `\\begin{tabular}{l${"r".repeat(columns.length)}}\n\\toprule\nindex & ${columns
    .map(escape)
    .join(" & ")} \\\\\n\\midrule`;
  const body = rows
    .map((row, i) => `${escape(index[i])} & ${columns.map((c) => escape(row[c])).join(" & ")} \\\\`)
    .join("\n");
  return `${header}\n${body}\n\\bottomrule\n\\end{tabular}`;
}

function latexCell(value: unknown): string {
  if (value === null || value === undefined) return "";
  if (value instanceof Date) return value.toISOString();
  const text = String(value);
  return text.replace(/\\/g, "\\textbackslash{}").replace(/([#$%&_{}])/g, "\\$1");
}

/** Nested xarray-like object tree keyed by index then columns. */
export function buildXarray(rows: Row[], columns: string[], index: IndexLabel[]): object {
  const tree: Record<string, Record<string, CellValue>> = {};
  for (let i = 0; i < rows.length; i += 1) {
    const entry: Record<string, CellValue> = {};
    for (const c of columns) entry[c] = rows[i]![c] as CellValue;
    tree[String(index[i])] = entry;
  }
  return { coords: Object.keys(tree), data_vars: tree };
}
