// Extended helpers for DataFrame — pure functions over rows/columns/index.
// Stats, cumulative, export, reshaping helpers.

import type { CellValue, IndexLabel, Row } from "../../types";
import { cloneRow, isMissing, numericValues, inferColumnDType } from "../../utils";
import { adjustedSkew, excessKurtosis, semOfMean } from "../series/stats";

// ---- stats ----
export function columnSkew(columns: string[], rows: Row[]): Record<string, number | null> {
  const out: Record<string, number | null> = {};
  for (const c of columns) out[c] = adjustedSkew(numericValues(rows.map((r) => r[c])));
  return out;
}
export function columnKurt(columns: string[], rows: Row[]): Record<string, number | null> {
  const out: Record<string, number | null> = {};
  for (const c of columns) out[c] = excessKurtosis(numericValues(rows.map((r) => r[c])));
  return out;
}
export function columnSem(columns: string[], rows: Row[]): Record<string, number | null> {
  const out: Record<string, number | null> = {};
  for (const c of columns) out[c] = semOfMean(numericValues(rows.map((r) => r[c])));
  return out;
}

// ---- cumulative ----
function toNum(v: CellValue): number {
  return typeof v === "number" ? v : Number(v);
}

export function cummaxRows(rows: Row[], columns: string[]): Row[] {
  const acc = new Map<string, number | null>();
  const out: Row[] = rows.map((r) => cloneRow(r, columns));
  for (let i = 0; i < rows.length; i += 1) {
    for (const c of columns) {
      const val = rows[i]![c];
      if (isMissing(val)) { out[i]![c] = null; continue; }
      const n = toNum(val as CellValue);
      const cur = acc.get(c) ?? null;
      const nxt = cur === null ? n : Math.max(cur, n);
      acc.set(c, nxt);
      out[i]![c] = nxt;
    }
  }
  return out;
}
export function cumminRows(rows: Row[], columns: string[]): Row[] {
  const acc = new Map<string, number | null>();
  const out: Row[] = rows.map((r) => cloneRow(r, columns));
  for (let i = 0; i < rows.length; i += 1) {
    for (const c of columns) {
      const val = rows[i]![c];
      if (isMissing(val)) { out[i]![c] = null; continue; }
      const n = toNum(val as CellValue);
      const cur = acc.get(c) ?? null;
      const nxt = cur === null ? n : Math.min(cur, n);
      acc.set(c, nxt);
      out[i]![c] = nxt;
    }
  }
  return out;
}
export function cumprodRows(rows: Row[], columns: string[]): Row[] {
  const acc = new Map<string, number>();
  for (const c of columns) acc.set(c, 1);
  const out: Row[] = rows.map((r) => cloneRow(r, columns));
  for (let i = 0; i < rows.length; i += 1) {
    for (const c of columns) {
      const val = rows[i]![c];
      if (isMissing(val)) { out[i]![c] = null; continue; }
      const n = toNum(val as CellValue);
      const cur = acc.get(c)! * n;
      acc.set(c, cur);
      out[i]![c] = cur;
    }
  }
  return out;
}

// ---- mode ----
export function modeRows(rows: Row[], columns: string[]): { rows: Row[]; columns: string[] } {
  const perCol = new Map<string, CellValue[]>();
  let maxLen = 0;
  for (const col of columns) {
    const counts = new Map<string, { value: CellValue; count: number; first: number }>();
    rows.forEach((r, i) => {
      const v = r[col];
      if (isMissing(v)) return;
      const key = v instanceof Date ? `date:${(v as Date).toISOString()}` : `${typeof v}:${String(v)}`;
      const e = counts.get(key);
      if (!e) counts.set(key, { value: v as CellValue, count: 1, first: i });
      else e.count += 1;
    });
    if (counts.size === 0) { perCol.set(col, []); continue; }
    let maxCount = 0;
    for (const e of counts.values()) maxCount = Math.max(maxCount, e.count);
    const modes = [...counts.values()].filter((e) => e.count === maxCount).sort((a, b) => a.first - b.first).map((e) => e.value);
    perCol.set(col, modes);
    maxLen = Math.max(maxLen, modes.length);
  }
  if (maxLen === 0) return { rows: [], columns: [...columns] };
  const outRows: Row[] = [];
  for (let i = 0; i < maxLen; i += 1) {
    const row: Row = {};
    for (const c of columns) row[c] = perCol.get(c)![i] ?? null;
    outRows.push(row);
  }
  return { rows: outRows, columns: [...columns] };
}

// ---- export helpers ----
export function toHtmlString(rows: Row[], columns: string[], index: IndexLabel[]): string {
  const esc = (v: unknown): string => String(v ?? "").replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;").replaceAll('"', "&quot;");
  let html = '<table border="1" class="dataframe">\n  <thead>\n    <tr style="text-align: right;">\n      <th></th>\n';
  for (const c of columns) html += `      <th>${esc(c)}</th>\n`;
  html += "    </tr>\n  </thead>\n  <tbody>\n";
  for (let i = 0; i < rows.length; i += 1) {
    html += `    <tr>\n      <th>${esc(index[i])}</th>\n`;
    for (const c of columns) {
      const v = rows[i]![c];
      const txt = v instanceof Date ? (v as Date).toISOString() : v === null || v === undefined ? "" : String(v);
      html += `      <td>${esc(txt)}</td>\n`;
    }
    html += "    </tr>\n";
  }
  html += "  </tbody>\n</table>";
  return html;
}

export function toMarkdownString(rows: Row[], columns: string[], index: IndexLabel[]): string {
  const headers = ["", ...columns];
  const lines: string[] = [];
  lines.push(`| ${headers.join(" | ")} |`);
  lines.push(`| ${headers.map(() => "---").join(" | ")} |`);
  for (let i = 0; i < rows.length; i += 1) {
    const cells: string[] = [String(index[i] ?? "")];
    for (const c of columns) {
      const v = rows[i]![c];
      cells.push(v instanceof Date ? (v as Date).toISOString() : v === null || v === undefined ? "" : String(v));
    }
    lines.push(`| ${cells.join(" | ")} |`);
  }
  return lines.join("\n");
}

export function frameInfo(rows: Row[], columns: string[], index: IndexLabel[]): string {
  const lines: string[] = [];
  lines.push(`<class 'DataFrame'>`);
  lines.push(`Index: ${rows.length} entries, ${index[0] ?? ""} to ${index[index.length - 1] ?? ""}`);
  lines.push(`Data columns (total ${columns.length} columns):`);
  for (let i = 0; i < columns.length; i += 1) {
    const c = columns[i]!;
    const vals = rows.map((r) => r[c]);
    const nonNull = vals.filter((v) => !isMissing(v)).length;
    const nonMissing = vals.filter((v) => !isMissing(v));
    let dtype: string = "unknown";
    if (nonMissing.length > 0) dtype = inferColumnDType(vals as CellValue[]);
    lines.push(` ${i}   ${c}  ${nonNull} non-null  ${dtype}`);
  }
  lines.push(`dtypes: ${columns.length > 0 ? "mixed" : ""}`);
  const approx = rows.length * columns.length * 8;
  lines.push(`memory usage: ${approx} bytes`);
  return lines.join("\n");
}

export function frameMemoryUsage(rows: Row[], columns: string[]): Record<string, number> {
  const out: Record<string, number> = {};
  for (const c of columns) out[c] = rows.length * 8;
  out["Index"] = rows.length * 8;
  return out;
}

// ---- selection helpers ----
export function takeRowsWithIndex(
  rows: Row[],
  columns: string[],
  index: IndexLabel[],
  positions: number[]
): { rows: Row[]; index: IndexLabel[] } {
  const n = rows.length;
  const outRows: Row[] = [];
  const outIdx: IndexLabel[] = [];
  for (const p of positions) {
    let pos = p;
    if (p < 0) pos = n + p;
    if (pos < 0 || pos >= n) throw new Error(`take position ${p} out of bounds for axis with size ${n}`);
    outRows.push(cloneRow(rows[pos]!, columns));
    outIdx.push(index[pos]!);
  }
  return { rows: outRows, index: outIdx };
}

export function truncateRows(
  rows: Row[],
  _columns: string[],
  index: IndexLabel[],
  before?: IndexLabel,
  after?: IndexLabel
): { rows: Row[]; index: IndexLabel[] } {
  void _columns;
  let start = 0;
  let end = rows.length;
  if (before !== undefined) {
    const pos = index.findIndex((v) => v === before);
    if (pos < 0) throw new Error(`truncate before label '${String(before)}' not found`);
    start = pos;
  }
  if (after !== undefined) {
    const pos = index.findIndex((v) => v === after);
    if (pos < 0) throw new Error(`truncate after label '${String(after)}' not found`);
    end = pos + 1;
  }
  if (start > end) return { rows: [], index: [] };
  return { rows: rows.slice(start, end).map((r) => ({ ...r })), index: index.slice(start, end) };
}

export function reindexRows(
  rows: Row[],
  columns: string[],
  index: IndexLabel[],
  newIndex?: IndexLabel[],
  newColumns?: string[],
  fillValue: CellValue = null
): { rows: Row[]; columns: string[]; index: IndexLabel[] } {
  const outColumns = newColumns ? [...newColumns] : [...columns];
  const outIndex = newIndex ? [...newIndex] : [...index];
  const indexToRow = new Map<string, Row>();
  for (let i = 0; i < index.length; i += 1) indexToRow.set(String(index[i]), rows[i]!);
  const outRows: Row[] = outIndex.map((label) => {
    const src = indexToRow.get(String(label));
    const row: Row = {};
    for (const c of outColumns) {
      if (src && c in src) row[c] = src[c] as CellValue;
      else row[c] = fillValue;
    }
    return row;
  });
  return { rows: outRows, columns: outColumns, index: outIndex };
}

export type SqueezeKind =
  | { kind: "scalar"; value: CellValue }
  | { kind: "column"; values: CellValue[]; name: string; index: IndexLabel[] }
  | { kind: "row"; values: CellValue[]; index: string[] }
  | { kind: "frame"; rows: Row[]; columns: string[]; index: IndexLabel[] };

export function squeezeResult(
  rows: Row[],
  columns: string[],
  index: IndexLabel[]
): SqueezeKind {
  if (rows.length === 1 && columns.length === 1) {
    return { kind: "scalar", value: rows[0]![columns[0]!] as CellValue };
  }
  if (columns.length === 1) {
    return { kind: "column", values: rows.map((r) => r[columns[0]!] as CellValue), name: columns[0]!, index: [...index] };
  }
  if (rows.length === 1) {
    return { kind: "row", values: columns.map((c) => rows[0]![c] as CellValue), index: [...columns] };
  }
  return { kind: "frame", rows: rows.map((r) => cloneRow(r, columns)), columns: [...columns], index: [...index] };
}

// ---- DataFrame agg helper ----
import type { AggName, AggFn } from "../../types";
import { finalizeNamedAggValues } from "./where";
import { numericValues as _nv } from "../../utils";

export function aggFrame(
  rows: Row[],
  columns: string[],
  spec: AggName | Record<string, AggName | AggFn>
): Record<string, CellValue> {
  if (typeof spec === "string") {
    const out: Record<string, CellValue> = {};
    for (const c of columns) {
      const vals = rows.map((r) => r[c] as CellValue);
      out[c] = typeof spec === "function" ? (spec as AggFn)(vals, rows) : finalizeNamedAggValues(spec as AggName, vals);
    }
    return out;
  }
  const out: Record<string, CellValue> = {};
  for (const [col, fn] of Object.entries(spec)) {
    if (!columns.includes(col)) throw new Error(`Column '${col}' does not exist.`);
    const vals = rows.map((r) => r[col] as CellValue);
    out[col] = typeof fn === "function" ? (fn as AggFn)(vals, rows) : finalizeNamedAggValues(fn as AggName, vals);
  }
  return out;
}
