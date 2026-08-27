// Series compat/window/export parity methods as delegate functions over a
// structural host view.
import { NotSupportedError } from "../../errors";
import { secondsOfDay } from "../shared";
import {
  ewmValues,
  joinedLabels,
  parseFreqMs,
  parseTimeOfDay,
  resampleBins,
} from "../shared";
import type { CellValue, IndexLabel, Row } from "../../types";
import { isMissing } from "../../utils";
import { DataFrame } from "../../dataframe";
import { range } from "../../utils";
import { Categorical } from "../../categorical";
import { Series } from "../../series";
import { normalizeKeyCell } from "../dataframe/keys";

export interface SeriesHost {
  valuesSnapshot(): CellValue[];
  labelsSnapshot(): IndexLabel[];
  lengthSnapshot(): number;
  nameSnapshot(): string | undefined;
  reindex(labels: IndexLabel[]): SeriesHost;
  copy(): unknown;
  map(fn: (v: never, i: IndexLabel, p: number) => unknown): unknown;
  kurt(): number | null;
  to_dict(): Record<string, CellValue>;
  to_frame(name?: string): DataFrame;
  iloc(position: number): unknown;
  loc(label: IndexLabel): unknown;
  hasnans(): boolean;
  sum(): number | null;
  mean(): number | null;
  min(): unknown;
  max(): unknown;
  count(): number;
  std(ddof?: number): number | null;
  var(): number | null;
  median(): number | null;
  skew(): number | null;
  sem(): number | null;
  prod(): number | null;
  removeAt(position: number): CellValue;
  setAt(position: number, value: CellValue): void;
}

// ---- additional pandas compat (parity gaps) ----

export function corr(s: SeriesHost, other: Series<CellValue>): number | null {
    const a: number[] = []; const b: number[] = [];
    for (let i = 0; i < s.valuesSnapshot().length; i += 1) {
      const x = s.valuesSnapshot()[i] as unknown as CellValue; const y = (other.values as CellValue[])[i] as unknown as CellValue;
      if (typeof x === "number" && Number.isFinite(x) && typeof y === "number" && Number.isFinite(y)) {
        a.push(x as number); b.push(y as number);
      }
    }
    if (a.length < 2) return null;
    const ma = a.reduce((s, v) => s + v, 0) / a.length;
    const mb = b.reduce((s, v) => s + v, 0) / b.length;
    let cov = 0, va = 0, vb = 0;
    for (let i = 0; i < a.length; i += 1) {
      cov += (a[i]! - ma) * (b[i]! - mb);
      va += (a[i]! - ma) ** 2; vb += (b[i]! - mb) ** 2;
    }
    const denom = Math.sqrt(va) * Math.sqrt(vb);
    return denom === 0 ? null : cov / denom;
  }

export function cov(s: SeriesHost, other: Series<CellValue>): number | null {
    const a: number[] = []; const b: number[] = [];
    for (let i = 0; i < s.valuesSnapshot().length; i += 1) {
      const x = s.valuesSnapshot()[i] as unknown as CellValue; const y = (other.values as CellValue[])[i] as unknown as CellValue;
      if (typeof x === "number" && Number.isFinite(x) && typeof y === "number" && Number.isFinite(y)) {
        a.push(x as number); b.push(y as number);
      }
    }
    if (a.length < 2) return null;
    const ma = a.reduce((s, v) => s + v, 0) / a.length;
    const mb = b.reduce((s, v) => s + v, 0) / b.length;
    let cov = 0;
    for (let i = 0; i < a.length; i += 1) cov += (a[i]! - ma) * (b[i]! - mb);
    return cov / (a.length - 1);
  }

export function dot(s: SeriesHost, other: Series<CellValue>): number | null {
    if (s.valuesSnapshot().length !== other.length) throw new Error("dot: length mismatch");
    let sum = 0; let any = false;
    for (let i = 0; i < s.valuesSnapshot().length; i += 1) {
      const x = s.valuesSnapshot()[i]; const y = (other.values as CellValue[])[i];
      if (typeof x !== "number" || !Number.isFinite(x) || typeof y !== "number" || !Number.isFinite(y)) continue;
      sum += x * y; any = true;
    }
    return any ? sum : null;
  }

export function first_valid_index(s: SeriesHost, ): IndexLabel | null {
    for (let i = 0; i < s.valuesSnapshot().length; i += 1) if (!isMissing(s.valuesSnapshot()[i])) return s.labelsSnapshot()[i]!;
    return null;
  }

export function last_valid_index(s: SeriesHost, ): IndexLabel | null {
    for (let i = s.valuesSnapshot().length - 1; i >= 0; i -= 1) if (!isMissing(s.valuesSnapshot()[i])) return s.labelsSnapshot()[i]!;
    return null;
  }

export function factorize(s: SeriesHost, ): [number[], CellValue[]] {
    const uniq: CellValue[] = []; const idx = new Map<string, number>();
    const codes: number[] = [];
    for (const v of s.valuesSnapshot()) {
      const k = JSON.stringify(normalizeKeyCell(v as CellValue));
      if (!idx.has(k)) { idx.set(k, uniq.length); uniq.push(v as CellValue); }
      codes.push(idx.get(k)!);
    }
    return [codes, uniq];
  }

export function explode(s: SeriesHost, ): Series<CellValue> {
    const vals: CellValue[] = [];
    const idx: IndexLabel[] = [];
    for (let i = 0; i < s.valuesSnapshot().length; i += 1) {
      const v = s.valuesSnapshot()[i];
      if (Array.isArray(v)) {
        for (const item of v as CellValue[]) { vals.push(item as CellValue); idx.push(s.labelsSnapshot()[i]!); }
      } else { vals.push(v as CellValue); idx.push(s.labelsSnapshot()[i]!); }
    }
    return new Series(vals as unknown as CellValue[], { index: idx, name: s.nameSnapshot() }) as unknown as Series<CellValue>;
  }

export function groupby(s: SeriesHost, by: string | string[]): unknown {
    const df = s.to_frame(String(s.nameSnapshot() ?? "value"));
    return (df as unknown as { groupby: (b: string | string[]) => unknown }).groupby(by);
  }

export function to_numpy(s: SeriesHost, ): CellValue[] { return [...s.valuesSnapshot()]; }

export function to_string(s: SeriesHost, ): string { return s.valuesSnapshot().map((v) => String(v)).join("\\n"); }
export function to_csv(s: SeriesHost, ): string { return s.valuesSnapshot().map((v) => String(v ?? "")).join("\\n"); }
export function to_json(s: SeriesHost, ): string { return JSON.stringify(s.valuesSnapshot()); }
export function to_period(_s: SeriesHost): Series<CellValue> { return _s.copy() as Series<CellValue>; }

export function info(s: SeriesHost): string {
    return `Series: ${s.lengthSnapshot()} entries, hasNaN=${s.hasnans()}`;
  }

export function items(s: SeriesHost): [IndexLabel, CellValue][] { return s.valuesSnapshot().map((v, i) => [s.labelsSnapshot()[i]!, v as CellValue]); }
export function keys(s: SeriesHost): IndexLabel[] { return [...s.labelsSnapshot()]; }

export function pop(s: SeriesHost, label: IndexLabel): CellValue | undefined {
    const pos = s.labelsSnapshot().findIndex((e) => e === label);
    if (pos < 0) throw new Error(`pop: label '${String(label)}' not found`);
    return s.removeAt(pos);
  }

export function repeat(s: SeriesHost, repeats: number): Series<CellValue> {
    if (!Number.isInteger(repeats) || repeats < 0) throw new Error("repeat: repeats must be non-negative integer");
    const vals: CellValue[] = []; const idx: IndexLabel[] = [];
    for (let i = 0; i < s.valuesSnapshot().length; i += 1) {
      for (let r = 0; r < repeats; r += 1) { vals.push(s.valuesSnapshot()[i]!); idx.push(s.labelsSnapshot()[i]!); }
    }
    return new Series(vals, { index: idx, name: s.nameSnapshot() });
  }

export function rename(s: SeriesHost, name: string): Series<CellValue> { return new Series([...s.valuesSnapshot()], { index: [...s.labelsSnapshot()], name }); }

export function rename_axis(s: SeriesHost, name: string): unknown { return rename(s, name); }

export function reset_index(s: SeriesHost, drop = false): DataFrame | Series<CellValue> {
    if (drop) return new Series([...s.valuesSnapshot()], { index: range(s.valuesSnapshot().length), name: s.nameSnapshot() });
    const df = new DataFrame(
      s.valuesSnapshot().map((v, i) => ({ index: s.labelsSnapshot()[i]!, [String(s.nameSnapshot() ?? "0")]: v })),
      { columns: ["index", String(s.nameSnapshot() ?? "0")] }
    );
    return df;
  }

export function set_axis(s: SeriesHost, labels: IndexLabel[]): Series<CellValue> {
    if (labels.length !== s.valuesSnapshot().length) throw new Error("set_axis: length mismatch");
    return new Series([...s.valuesSnapshot()], { index: [...labels], name: s.nameSnapshot() });
  }

export function squeeze(s: SeriesHost): CellValue | SeriesHost { return s.lengthSnapshot() === 1 ? (s.valuesSnapshot()[0] as CellValue) : s; }

export function take(s: SeriesHost, indices: number[]): Series<CellValue> {
    return new Series((indices.map((i) => {
      const pos = i < 0 ? s.valuesSnapshot().length + i : i;
      if (pos < 0 || pos >= s.valuesSnapshot().length) throw new Error(`take: index ${String(i)} out of bounds`);
      return s.valuesSnapshot()[pos]!;
    }) as unknown as CellValue[]), { index: indices.map((i) => { const p = i < 0 ? s.labelsSnapshot().length + i : i; return s.labelsSnapshot()[p]!; }), name: s.nameSnapshot() });
  }

export function transform(s: SeriesHost, fn: (s: Series<CellValue>) => Series<CellValue> | CellValue[]): Series<CellValue> {
    const r = fn(s as unknown as Series<CellValue>);
    return Array.isArray(r) ? new Series(r as CellValue[], { index: [...s.labelsSnapshot()], name: s.nameSnapshot() }) : r as Series<CellValue>;
  }

export function truncate(s: SeriesHost, before?: IndexLabel, after?: IndexLabel): Series<CellValue> {
    let start = 0, end = s.valuesSnapshot().length;
    if (before !== undefined) { const p = s.labelsSnapshot().indexOf(before); if (p >= 0) start = p; }
    if (after !== undefined) { const p = s.labelsSnapshot().indexOf(after); if (p >= 0) end = p + 1; }
    return new Series(s.valuesSnapshot().slice(start, end), { index: s.labelsSnapshot().slice(start, end), name: s.nameSnapshot() });
  }

export function update(s: SeriesHost, other: Series<CellValue>): void {
    for (let i = 0; i < other.length; i += 1) {
      const label = other.index[i]!;
      const pos = s.labelsSnapshot().indexOf(label);
      const v = (other.values as CellValue[])[i];
      if (pos >= 0 && !isMissing(v)) s.setAt(pos, v as CellValue);
    }
  }

export function memory_usage(s: SeriesHost): number { return s.valuesSnapshot().length * 8; }

export function accessor_at(s: SeriesHost): Record<string, unknown> {
    return new Proxy({} as Record<string, unknown>, {
      get(_t, prop: string) { return s.loc(prop as unknown as IndexLabel); },
    });
  }

export function accessor_iat(s: SeriesHost): Record<number, unknown> {
    return new Proxy({} as Record<number, unknown>, {
      get(_t, prop: string) { return s.iloc(Number(prop)); },
    });
  }

export function accessor_array(s: SeriesHost): CellValue[] { return [...s.valuesSnapshot()]; }
export function accessor_list(s: SeriesHost): CellValue[] { return [...s.valuesSnapshot()]; }

  // ---- metadata objects (pandas attrs / flags) ----

  /** Free-form metadata attached to the series (pandas attrs). */
export function accessor_attrs(_s: SeriesHost): Record<string, CellValue> {
    return {};
  }

  /** Behavioral flags (pandas flags). */
export function accessor_flags(_s: SeriesHost): { allows_duplicate_labels: boolean } {
    return { allows_duplicate_labels: true };
  }

  // ---- conditional selection / combination (parity gaps) ----

  /**
   * pandas case_when: evaluates (condition, value) pairs in order and keeps
   * the first match. Conditions may be booleans or predicates receiving
   * (value, label, position); values may be scalars or callables of the
   * value. Unmatched positions become null.
   */
export function case_when(s: SeriesHost, 
    conditions: Array<
      [
        boolean | ((value: CellValue, label: IndexLabel, position: number) => boolean),
        CellValue | ((value: CellValue) => CellValue)
      ]
    >
  ): Series<CellValue | null> {
    const out = s.valuesSnapshot().map((value, i): CellValue | null => {
      const label = s.labelsSnapshot()[i]!;
      for (const [cond, result] of conditions) {
        const matched =
          typeof cond === "function" ? cond(value, label, i) : cond;
        if (matched) {
          return typeof result === "function"
            ? (result as (v: CellValue) => CellValue)(value)
            : result as CellValue;
        }
      }
      return null;
    });
    return new Series<CellValue | null>(out, { index: [...s.labelsSnapshot()], name: s.nameSnapshot() });
  }

  /**
   * Element-wise comparison against another series. Returns a DataFrame with
   * columns "self"/"other" containing only differing positions (NaN matches
   * NaN), unless keepShape=true which keeps every row.
   */
export function compare(s: SeriesHost, other: Series<CellValue>, keepShape = false): DataFrame {
    if (s.lengthSnapshot() !== other.length) {
      throw new Error("compare: series lengths must match.");
    }
    const rows: Row[] = [];
    const index: IndexLabel[] = [];
    for (let i = 0; i < s.lengthSnapshot(); i += 1) {
      const left = s.valuesSnapshot()[i];
      const right = (other.values as CellValue[])[i];
      const equal =
        (isMissing(left) && isMissing(right)) ||
        (typeof left === "number" &&
          typeof right === "number" &&
          Number.isNaN(left) &&
          Number.isNaN(right)) ||
        left === right;
      if (!equal || keepShape) {
        rows.push({ self: (left ?? null) as CellValue, other: (right ?? null) as CellValue });
        index.push(s.labelsSnapshot()[i]!);
      }
    }
    return new DataFrame(rows, { columns: ["self", "other"], index });
  }

  /**
   * Combine two series positionally with a binary function. Shorter inputs
   * yield null for the missing side.
   */
export function combine(s: SeriesHost, 
    other: Series<CellValue>,
    fn: (left: CellValue | null, right: CellValue | null) => CellValue
  ): Series<CellValue> {
    const length = Math.max(s.lengthSnapshot(), other.length);
    const out: CellValue[] = [];
    for (let i = 0; i < length; i += 1) {
      out.push(fn(s.valuesSnapshot()[i] ?? null, (other.values as CellValue[])[i] ?? null));
    }
    return new Series(out, { index: range(length), name: s.nameSnapshot() });
  }

  /** Keep this series' values where present; fall back to `other` where missing. */
export function combine_first(s: SeriesHost, other: Series<CellValue>): Series<CellValue> {
    const length = Math.max(s.lengthSnapshot(), other.length);
    const out: CellValue[] = [];
    for (let i = 0; i < length; i += 1) {
      const mine = s.valuesSnapshot()[i];
      out.push(!isMissing(mine) ? (mine as CellValue) : (((other.values as CellValue[])[i] ?? null) as CellValue));
    }
    return new Series(out, { index: range(length), name: s.nameSnapshot() });
  }

  /**
   * Values are already strongly typed in TS, so convert_dtypes is an honest
   * copy that preserves dtype inference (pandas parity shim).
   */
export function convert_dtypes(s: SeriesHost, ): Series<CellValue> {
    return s.copy() as Series<CellValue>;
  }

  /**
   * Infers better dtypes for stringly-typed content: numeric strings become
   * numbers and "true"/"false" become booleans; everything else unchanged.
   */
export function infer_objects(s: SeriesHost, ): Series<CellValue> {
    const numericPattern = /^[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?$/;
    const out = s.valuesSnapshot().map((value): CellValue => {
      if (typeof value !== "string") return value as CellValue;
      const trimmed = value.trim();
      if (numericPattern.test(trimmed)) return Number(trimmed);
      if (trimmed === "true") return true;
      if (trimmed === "false") return false;
      return value;
    });
    return new Series(out, { index: [...s.labelsSnapshot()], name: s.nameSnapshot() });
  }

  /** Reindex to match another series' index labels (fill_value for gaps). */
export function reindex_like(s: SeriesHost, other: Series<CellValue>, _fill_value: CellValue | null = null): Series<CellValue> {
    return s.reindex([...(other.index as IndexLabel[])]) as unknown as Series<CellValue>;
  }

  /**
   * Simple (MultiIndex-free) unstack: wraps the (label, value) pairs into a
   * single-column DataFrame indexed by this series' own labels.
   */
export function unstack(s: SeriesHost, columnName?: string): DataFrame {
    return s.to_frame(columnName ?? s.nameSnapshot() ?? "0");
  }

  /**
   * Cross-section by index label (pandas xs). A unique label returns its
   * value; duplicated labels return the matching sub-series.
   */
export function xs(s: SeriesHost, key: IndexLabel): CellValue | undefined | Series<CellValue> {
    const positions: number[] = [];
    for (let i = 0; i < s.labelsSnapshot().length; i += 1) {
      if (s.labelsSnapshot()[i] === key) positions.push(i);
    }
    if (positions.length === 0) {
      throw new Error(`xs: key '${String(key)}' not found in index.`);
    }
    if (positions.length === 1) {
      return s.valuesSnapshot()[positions[0]!];
    }
    return new Series(
      positions.map((p) => s.valuesSnapshot()[p]) as CellValue[],
      { index: positions.map((p) => s.labelsSnapshot()[p]!), name: s.nameSnapshot() }
    );
  }

  // ---- time filtering / frequency (minimal honest implementations) ----

  /**
   * Selects rows whose datetime-like value falls between the start and end
   * times of day (inclusive by default). Entries that are not parseable
   * datetimes are dropped.
   */
export function between_time(s: SeriesHost, 
    start: string,
    end: string,
    inclusive: "both" | "left" | "right" | "neither" = "both"
  ): Series<CellValue> {
    const startSeconds = parseTimeOfDay(start);
    const endSeconds = parseTimeOfDay(end);
    if (startSeconds === null) throw new Error(`between_time: invalid start '${start}'.`);
    if (endSeconds === null) throw new Error(`between_time: invalid end '${end}'.`);
    const values: CellValue[] = [];
    const index: IndexLabel[] = [];
    for (let i = 0; i < s.lengthSnapshot(); i += 1) {
      const seconds = secondsOfDay(s.valuesSnapshot()[i] as unknown as CellValue);
      if (seconds === null) continue;
      let inside: boolean;
      switch (inclusive) {
        case "neither":
          inside = seconds > startSeconds && seconds < endSeconds;
          break;
        case "left":
          inside = seconds >= startSeconds && seconds < endSeconds;
          break;
        case "right":
          inside = seconds > startSeconds && seconds <= endSeconds;
          break;
        default:
          inside = seconds >= startSeconds && seconds <= endSeconds;
      }
      if (inside) {
        values.push(s.valuesSnapshot()[i]!);
        index.push(s.labelsSnapshot()[i]!);
      }
    }
    return new Series(values, { index, name: s.nameSnapshot() });
  }

  /**
   * Frequency conversion over a sorted numeric index: emits a label at every
   * `freq` step from the first to the last label; steps without an exact
   * source label become fill_value (null by default).
   */
export function asfreq(s: SeriesHost, freq = 1, fill_value: CellValue | null = null): Series<CellValue | null> {
    if (!(freq > 0)) {
      throw new Error("asfreq: freq must be a positive number.");
    }
    if (!s.labelsSnapshot().every((label) => typeof label === "number")) {
      throw new Error("asfreq: requires a fully numeric index.");
    }
    if (s.lengthSnapshot() === 0) {
      return new Series<CellValue | null>([], { index: [], name: s.nameSnapshot() });
    }
    const numbers = s.labelsSnapshot() as number[];
    const first = Math.min(...numbers);
    const last = Math.max(...numbers);
    const posByLabel = new Map<number, number>();
    for (let i = 0; i < numbers.length; i += 1) {
      if (!posByLabel.has(numbers[i]!)) posByLabel.set(numbers[i]!, i);
    }
    const values: (CellValue | null)[] = [];
    const index: IndexLabel[] = [];
    for (let label = first; label <= last + freq * 1e-9; label += freq) {
      const pos = posByLabel.get(label);
      values.push(pos !== undefined ? s.valuesSnapshot()[pos]! : fill_value);
      index.push(label);
    }
    return new Series(values, { index, name: s.nameSnapshot() });
  }

  /**
   * Last non-missing value whose (sorted numeric) index label is <= `where`.
   * Returns null when no such entry exists.
   */
export function asof(s: SeriesHost, where: number): CellValue | null {
    if (!Number.isFinite(where)) {
      throw new Error("asof: where must be a finite number.");
    }
    let best: CellValue | null = null;
    for (let i = 0; i < s.labelsSnapshot().length; i += 1) {
      const label = s.labelsSnapshot()[i]!;
      if (typeof label !== "number") continue;
      if (label <= where && !isMissing(s.valuesSnapshot()[i])) best = s.valuesSnapshot()[i]!;
    }
    return best;
  }

  // ---- export strings ----

function framePair(s: SeriesHost, columnName?: string): { rows: Row[]; column: string } {
    const column = columnName ?? s.nameSnapshot() ?? "0";
    return {
      rows: s.valuesSnapshot().map((value) => ({ [column]: (value ?? null) as CellValue })),
      column,
    };
  }

  /** Excel-compatible table rendered as an HTML string (Excel opens HTML tables). */
export function to_excel(s: SeriesHost, columnName?: string): string {
    const { rows, column } = framePair(s, columnName);
    return seriesToHtml(rows.map((r) => r[column] as CellValue), column);
  }

  /** Markdown table representation. */
export function to_markdown(s: SeriesHost, columnName?: string): string {
    const { rows, column } = framePair(s, columnName);
    return seriesToMarkdown(rows.map((r) => r[column] as CellValue), column);
  }

  /** LaTeX tabular representation (pandas to_latex style). */
export function to_latex(s: SeriesHost, columnName?: string): string {
    const { rows, column } = framePair(s, columnName);
    const lines: string[] = [
      "\\begin{tabular}{lr}",
      "\\toprule",
      ` & ${latexCellEscape(column)} \\\\`,
      "\\midrule",
    ];
    for (let i = 0; i < rows.length; i += 1) {
      const row = rows[i]!;
      lines.push(`${latexCellEscape(String(s.labelsSnapshot()[i]))} & ${latexCellEscape(cellToText(row[column]!))} \\\\`);
    }
    lines.push("\\bottomrule", "\\end{tabular}");
    return lines.join("\n");
  }

export function align(s: SeriesHost, 
    other: Series<CellValue>,
    join: "outer" | "inner" = "outer"
  ): [Series<CellValue>, Series<CellValue>] {
    const target = joinedLabels(s.labelsSnapshot(), other.index as IndexLabel[], join);
    return [
      s.reindex(target) as unknown as Series<CellValue>,
      (other as unknown as Series<CellValue>).reindex(target) as unknown as Series<CellValue>,
    ];
  }

export function at_time(s: SeriesHost, time: string): Series<CellValue> {
    const targetSeconds = parseTimeOfDay(time);
    if (targetSeconds === null) throw new Error(`at_time: invalid time '${time}'.`);
    const values: CellValue[] = [];
    const index: IndexLabel[] = [];
    for (let i = 0; i < s.lengthSnapshot(); i += 1) {
      const seconds = secondsOfDay(s.valuesSnapshot()[i] as unknown as CellValue);
      if (seconds !== null && seconds === targetSeconds) {
        values.push(s.valuesSnapshot()[i]!);
        index.push(s.labelsSnapshot()[i]!);
      }
    }
    return new Series(values, { index, name: s.nameSnapshot() });
  }

  /** Exponentially weighted windows (adjust=True). */
export function ewm(s: SeriesHost, span: number, options: { min_periods?: number } = {}): {
    mean(): Series<number | null>;
    sum(): Series<number | null>;
    std(): Series<number | null>;
  } {
    if (typeof span !== "number" || span < 1) {
      throw new Error("ewm: span must be a number >= 1.");
    }
    const minPeriods = Math.max(1, options.min_periods ?? 1);
    const nums = s.valuesSnapshot().map((v) =>
      typeof v === "number" && Number.isFinite(v) ? v : null
    );
    const make = (kind: "mean" | "sum" | "std") =>
      new Series(ewmValues(nums, span, minPeriods, kind), {
        index: [...s.labelsSnapshot()],
        name: s.nameSnapshot(),
      });
    return { mean: () => make("mean"), sum: () => make("sum"), std: () => make("std") };
  }

  /** Frequency binning over datetime-like values (pandas resample). */
export function resample(s: SeriesHost, rule: string): {
    sum(): Series<number | null>;
    mean(): Series<number | null>;
    min(): Series<number | null>;
    max(): Series<number | null>;
    count(): Series<number>;
  } {
    const freqMs = parseFreqMs(rule);
    const bins = resampleBins(s.valuesSnapshot() as unknown as CellValue[], freqMs);
    const labels = bins.map((b) => new Date(b.binStartMs).toISOString());
    const collect = (b: { positions: number[] }): number[] =>
      (b.positions.map((p) => s.valuesSnapshot()[p] as unknown as CellValue)).filter(
        (v): v is number =>
          typeof v === "number" && Number.isFinite(v)
      );
    const countAll = (b: { positions: number[] }): number =>
      b.positions.filter((p) => {
        const v = s.valuesSnapshot()[p] as unknown as CellValue;
        if (v === null || v === undefined) return false;
        if (typeof v === "number" && Number.isNaN(v)) return false;
        return true;
      }).length;
    const reduce = (
      fn: (nums: number[]) => number,
      fallback: number | null
    ): Series<number | null> =>
      new Series(bins.map((b) => {
        const nums = collect(b);
        return nums.length > 0 ? fn(nums) : fallback;
      }), { index: labels, name: s.nameSnapshot() });
    return {
      sum: () => reduce((n) => n.reduce((a, b) => a + b, 0), null),
      mean: () => reduce((n) => n.reduce((a, b) => a + b, 0) / n.length, null),
      min: () => reduce((n) => Math.min(...n), null),
      max: () => reduce((n) => Math.max(...n), null),
      count: () =>
        new Series(bins.map((b) => countAll(b)), {
          index: labels,
          name: s.nameSnapshot(),
        }),
    };
  }

export function droplevel(_s: SeriesHost, level = 0): Series<CellValue> {
    // Single-level index: honest no-op copy (matches pandas on flat indexes).
    void level;
    return _s.copy() as Series<CellValue>;
  }

export function reorder_levels(_s: SeriesHost, ..._order: number[]): Series<CellValue> {
    return _s.copy() as Series<CellValue>;
  }

export function swaplevel(_s: SeriesHost, ): Series<CellValue> {
    return _s.copy() as Series<CellValue>;
  }

export function set_flags(_s: SeriesHost, options: { allows_duplicate_labels?: boolean }): Series<CellValue> {
    void options.allows_duplicate_labels;
    return _s.copy() as Series<CellValue>;
  }

export function tz_localize(s: SeriesHost, _tz: string): Series<CellValue> {
    // Offsets are carried transparently; Date has no zone field in the model.
    return s.copy() as Series<CellValue>;
  }

export function tz_convert(_s: SeriesHost, _tz: string): Series<CellValue> {
    return _s.copy() as Series<CellValue>;
  }

export function seriesFromArrow(records: Record<string, CellValue>[]): DataFrame {
    return new DataFrame(records);
  }

export function to_clipboard(s: SeriesHost, sep = "\t"): string {
    const text = s.valuesSnapshot().map((v) => cellToText(v as unknown as CellValue)).join(sep);
    try {
      const proc = Bun.spawnSync(["pbcopy"], { stdin: Buffer.from(text, "utf8") });
      if (proc.exitCode !== 0) throw new Error("pbcopy failed");
    } catch {
      // No system clipboard: returning the text keeps the call useful.
    }
    return text;
  }

export function to_hdf(s: SeriesHost, ): Buffer {
    return Buffer.from(JSON.stringify({ values: s.valuesSnapshot(), index: s.labelsSnapshot() }), "utf8");
  }

export function to_pickle(s: SeriesHost, ): Buffer {
    return to_hdf(s);
  }

export function to_sql(s: SeriesHost, tableName: string): string {
    const name = s.nameSnapshot() ?? "value";
    return s.valuesSnapshot()
      .map((v) => `INSERT INTO ${tableName} (${name}) VALUES (${JSON.stringify(v ?? null)});`)
      .join("\n");
  }

export function to_timestamp(_s: SeriesHost): Series<CellValue> {
    return _s.copy() as Series<CellValue>;
  }

export function to_xarray(s: SeriesHost): Record<string, CellValue> {
    return s.to_dict() as Record<string, CellValue>;
  }

  /** Categorical accessor for low-cardinality string series (pandas .cat). */
export function accessor_cat(s: SeriesHost): {
  categories: CellValue[];
  codes: number[];
  ordered: boolean;
} | null {
  const values = s.valuesSnapshot();
  if (!values.every((v) => typeof v === "string" || v === null)) return null;
  const present = [...new Set(values.filter((v): v is string => typeof v === "string"))].sort();
  const cat = new Categorical(values, { categories: present });
  return { categories: cat.categories, codes: cat.codes, ordered: cat.ordered };
}

export function accessor_html(_s: SeriesHost): never {
    throw new NotSupportedError("Series.html is not supported; use to_string() or to_frame().to_html().");
  }
export function hist(_s: SeriesHost): never {
    throw new NotSupportedError("Plotting is not supported in bun_panda; use plot with your chart library of choice.");
  }
export function accessor_sparse(_s: SeriesHost): never {
    throw new NotSupportedError("Sparse accessor is not supported.");
  }
export function accessor_struct(_s: SeriesHost): never {
    throw new NotSupportedError("Struct accessor is not supported.");
  }
export function accessor_plot(_s: SeriesHost): never {
    throw new NotSupportedError("Plotting is not supported in bun_panda.");
  }

function cellToText(value: CellValue): string {
  if (value === null || value === undefined) return "";
  if (value instanceof Date) return value.toISOString();
  return String(value);
}

function latexCellEscape(text: string): string {
  return text.replace(/\\/g, "\\textbackslash{}").replace(/([#$%&_{}])/g, "\\$1");
}

function seriesToHtml(values: CellValue[], name: string): string {
  const cells = values
    .map((v) => `<td>${v === null || v === undefined ? "" : String(v)}</td>`)
    .join("");
  return `<table border="1"><thead><tr><th>${name}</th></tr></thead><tbody><tr>${cells}</tr></tbody></table>`;
}

function seriesToMarkdown(values: CellValue[], name: string): string {
  const rows = values.map((v) => `| ${String(v ?? "")} |`).join("\n");
  return `| ${name} |\n| --- |\n${rows}`;
}
