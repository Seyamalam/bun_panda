import type { CellValue, DType, IndexLabel } from "./types";
import { DataFrame } from "./dataframe";
import {
  coerceValueToDType,
  compareCellValues,
  isMissing,
  numericValues,
  range,
} from "./utils";
import {
  computeSeriesClip,
  computeSeriesIsin,
  computeSeriesReplace,
  type SeriesReplaceInput,
} from "./internal/series/compat";
import {
  adjustedSkew,
  autocorrelation,
  excessKurtosis,
  semOfMean,
  seriesCount,
  seriesProd,
  seriesVariance,
} from "./internal/series/stats";
import {
  cumcountValues,
  cummaxValues,
  cumminValues,
  cumprodValues,
  cumsumValues,
} from "./internal/series/cumulative";
import { StringMethods } from "./internal/series/stringMethods";
import { DatetimeMethods } from "./internal/series/datetimeMethods";
import { computeExpanding, computeRolling, type RollingWindow } from "./internal/dataframe/rolling";

export type SeriesDType = DType;

export interface SeriesOptions {
  name?: string;
  index?: IndexLabel[];
}

export type { SeriesReplaceInput };

export class Series<T extends CellValue = CellValue> {
  public readonly name: string | undefined;
  private readonly _values: T[];
  private readonly _index: IndexLabel[];

  constructor(values: T[], options: SeriesOptions = {}) {
    this._values = [...values];
    this._index = options.index ? [...options.index] : range(values.length);
    this.name = options.name;

    if (this._index.length !== this._values.length) {
      throw new Error("Series index length must match values length.");
    }
  }

  get values(): T[] {
    return [...this._values];
  }

  get index(): IndexLabel[] {
    return [...this._index];
  }

  get length(): number {
    return this._values.length;
  }

  to_list(): T[] {
    return this.values;
  }

  to_dict(): Record<string, T> {
    const out: Record<string, T> = {};
    for (let i = 0; i < this.length; i += 1) {
      const label = this._index[i];
      const value = this._values[i];
      if (label !== undefined && value !== undefined) {
        out[String(label)] = value;
      }
    }
    return out;
  }

  iloc(position: number): T | undefined {
    const resolved = this.resolvePosition(position);
    if (resolved === undefined) {
      return undefined;
    }
    return this._values[resolved];
  }

  loc(label: IndexLabel): T | undefined {
    const position = this._index.findIndex((entry) => entry === label);
    if (position < 0) {
      return undefined;
    }
    return this._values[position];
  }

  head(n = 5): Series<T> {
    const count = Math.max(0, n);
    return new Series(this._values.slice(0, count), {
      index: this._index.slice(0, count),
      name: this.name,
    });
  }

  tail(n = 5): Series<T> {
    const count = Math.max(0, n);
    return new Series(this._values.slice(-count), {
      index: this._index.slice(-count),
      name: this.name,
    });
  }

  map<U extends CellValue>(
    fn: (value: T, index: IndexLabel, position: number) => U,
    name = this.name
  ): Series<U> {
    const values = this._values.map((value, position) =>
      fn(value, this._index[position]!, position)
    );
    return new Series(values, { index: this._index, name });
  }

  apply<U extends CellValue>(
    fn: (value: T, index: IndexLabel, position: number) => U,
    name = this.name
  ): Series<U> {
    return this.map(fn, name);
  }

  filter(fn: (value: T, index: IndexLabel, position: number) => boolean): Series<T> {
    const values: T[] = [];
    const index: IndexLabel[] = [];

    for (let i = 0; i < this.length; i += 1) {
      const value = this._values[i]!;
      const label = this._index[i]!;
      if (fn(value, label, i)) {
        values.push(value);
        index.push(label);
      }
    }

    return new Series(values, { index, name: this.name });
  }

  fillna(value: T): Series<T> {
    return this.map((entry) => (isMissing(entry) ? value : entry) as T);
  }

  /** Forward-fill: replaces missing values with the last seen value. */
  ffill(): Series<T> {
    let last: T | null = null;
    const out = this._values.map((value): T | null => {
      if (isMissing(value)) {
        return last;
      }
      last = value;
      return value;
    });
    return new Series(out as unknown as T[], { index: this._index, name: this.name });
  }

  /** Backward-fill: replaces missing values with the next seen value. */
  bfill(): Series<T> {
    let next: T | null = null;
    const out: (T | null)[] = new Array(this._values.length).fill(null);
    for (let i = this._values.length - 1; i >= 0; i -= 1) {
      const value = this._values[i]!;
      if (!isMissing(value)) {
        next = value;
        out[i] = value;
      } else {
        out[i] = next;
      }
    }
    return new Series(out as unknown as T[], { index: this._index, name: this.name });
  }

  isna(): Series<boolean> {
    const out = this._values.map(
      (value) => isMissing(value) || (typeof value === "number" && Number.isNaN(value))
    );
    return new Series(out, { index: this._index, name: this.name });
  }

  notna(): Series<boolean> {
    return this.isna().map((value) => !value) as Series<boolean>;
  }

  /** q-quantile of the numeric values (linear interpolation). */
  quantile(q = 0.5): number | null {
    const values = numericValues(this._values).sort((a, b) => a - b);
    if (values.length === 0) {
      return null;
    }
    const pos = (values.length - 1) * q;
    const lower = Math.floor(pos);
    const upper = Math.ceil(pos);
    return values[lower]! + (values[upper]! - values[lower]!) * (pos - lower);
  }

  /** Most frequent value(s); ties return all of them in first-seen order. */
  mode(): T[] {
    const counts = new Map<string, { value: T; count: number; first: number }>();
    this._values.forEach((value, i) => {
      if (isMissing(value)) {
        return;
      }
      const key = this.valueKey(value);
      const entry = counts.get(key);
      if (entry) {
        entry.count += 1;
      } else {
        counts.set(key, { value, count: 1, first: i });
      }
    });
    let max = 0;
    for (const entry of counts.values()) {
      max = Math.max(max, entry.count);
    }
    return [...counts.values()]
      .filter((entry) => entry.count === max)
      .sort((a, b) => a.first - b.first)
      .map((entry) => entry.value);
  }

  /** Functional chaining helper (pandas pipe). */
  pipe<T>(fn: (series: any, ...args: never[]) => T, ...args: never[]): T {
    return fn(this, ...args);
  }

  /** Index label of the maximum numeric value (pandas idxmax). */
  idxmax(): IndexLabel | null {
    let best: number | null = null;
    let bestIndex: IndexLabel | null = null;
    this._values.forEach((value, i) => {
      if (isMissing(value) || typeof value !== "number") return;
      if (best === null || value > best) {
        best = value;
        bestIndex = this._index[i]!;
      }
    });
    return bestIndex;
  }

  /** Index label of the minimum numeric value (pandas idxmin). */
  idxmin(): IndexLabel | null {
    let best: number | null = null;
    let bestIndex: IndexLabel | null = null;
    this._values.forEach((value, i) => {
      if (isMissing(value) || typeof value !== "number") return;
      if (best === null || value < best) {
        best = value;
        bestIndex = this._index[i]!;
      }
    });
    return bestIndex;
  }

  /**
   * Positions that would keep the values sorted (pandas argsort).
   * Stable; missing values go last.
   */
  argsort(): number[] {
    const indexed = this._values
      .map((value, position) => ({ value, position }))
      .filter((entry) => !isMissing(entry.value));
    indexed.sort((a, b) => compareCellValues(a.value, b.value));
    return indexed.map((entry) => entry.position);
  }

  /**
   * Position where `value` would be inserted to keep the (sorted)
   * series sorted: "left" for the first equal slot, "right" for the last.
   */
  searchsorted(
    value: number,
    side: "left" | "right" = "left"
  ): number {
    const numbers = numericValues(this._values).sort((a, b) => a - b);
    let low = 0;
    let high = numbers.length;
    while (low < high) {
      const mid = Math.floor((low + high) / 2);
      const current = numbers[mid]!;
      if (side === "left" ? current < value : current <= value) {
        low = mid + 1;
      } else {
        high = mid;
      }
    }
    return low;
  }

  /** True when the numeric values are in non-decreasing order. */
  is_monotonic_increasing(): boolean {
    const numbers = numericValues(this._values);
    return numbers.every((value, i) => i === 0 || numbers[i - 1]! <= value);
  }

  /** True when the numeric values are in non-increasing order. */
  is_monotonic_decreasing(): boolean {
    const numbers = numericValues(this._values);
    return numbers.every((value, i) => i === 0 || numbers[i - 1]! >= value);
  }

  /** True when every non-missing value occurs exactly once. */
  is_unique(): boolean {
    return new Set(this.dropna().to_list().map(this.valueKey, this)).size ===
      this.dropna().length;
  }

  /** True when the series contains any missing value. */
  hasnans(): boolean {
    return this._values.some((value) => isMissing(value));
  }

  /** Number of non-missing values (pandas count). */
  count(): number {
    return seriesCount(this._values);
  }

  median(): number | null {
    return this.quantile(0.5);
  }

  var(): number | null {
    return seriesVariance(numericValues(this._values));
  }

  prod(): number | null {
    return seriesProd(numericValues(this._values));
  }

  product(): number | null {
    return this.prod();
  }

  /** Unbiased skewness (Fisher-Pearson with sample correction G1). */
  skew(): number | null {
    return adjustedSkew(numericValues(this._values));
  }


  /** Excess kurtosis (Fisher definition; normal distribution = 0). */
  kurt(): number | null {
    return excessKurtosis(numericValues(this._values));
  }

  /** Standard error of the mean (sample std / sqrt(n)). */
  sem(): number | null {
    return semOfMean(numericValues(this._values));
  }

  /** Lag-N autocorrelation (default lag 1). */
  autocorr(lag = 1): number | null {
    return autocorrelation(this._values, lag);
  }

  /** True for values within [lower, upper] inclusive. */
  between(lower: number, upper: number, inclusive: "both" | "neither" | "left" | "right" = "both"): Series<boolean> {
    const out = this._values.map((value): boolean | null => {
      if (isMissing(value) || typeof value !== "number") {
        return null;
      }
      switch (inclusive) {
        case "neither":
          return value > lower && value < upper;
        case "left":
          return value >= lower && value < upper;
        case "right":
          return value > lower && value <= upper;
        default:
          return value >= lower && value <= upper;
      }
    });
    return new Series(out as never, { index: this._index, name: this.name });
  }

  /** First element as a scalar (pandas item / iat shortcut). */
  item(): T | undefined {
    return this._values[0];
  }

  /** Wraps the series in a single-column DataFrame. */
  to_frame(name?: string): DataFrame {
    return new DataFrame(
      this._values.map((value) => ({ [name ?? this.name ?? "0"]: value })),
      { columns: [name ?? this.name ?? "0"], index: [...this._index] }
    );
  }

  dropna(): Series<T> {
    return this.filter((entry) => !isMissing(entry));
  }

  sum(): number | null {
    const numbers = numericValues(this._values);
    if (numbers.length === 0) {
      return null;
    }
    return numbers.reduce((acc, value) => acc + value, 0);
  }

  mean(): number | null {
    const numbers = numericValues(this._values);
    if (numbers.length === 0) {
      return null;
    }
    return numbers.reduce((acc, value) => acc + value, 0) / numbers.length;
  }

  min(): T | null {
    const nonMissing = this._values.filter((value) => !isMissing(value));
    if (nonMissing.length === 0) {
      return null;
    }
    return [...nonMissing].sort(compareCellValues)[0] as T;
  }

  max(): T | null {
    const nonMissing = this._values.filter((value) => !isMissing(value));
    if (nonMissing.length === 0) {
      return null;
    }
    return [...nonMissing].sort(compareCellValues).at(-1) as T;
  }

  unique(): T[] {
    const seen = new Set<string>();
    const out: T[] = [];
    for (const value of this._values) {
      const key = this.valueKey(value);
      if (!seen.has(key)) {
        seen.add(key);
        out.push(value);
      }
    }
    return out;
  }

  value_counts(dropna = true): Series<number> {
    const counts = new Map<string, { value: T; count: number }>();

    for (const value of this._values) {
      if (dropna && isMissing(value)) {
        continue;
      }
      const key = this.valueKey(value);
      const entry = counts.get(key);
      if (!entry) {
        counts.set(key, { value, count: 1 });
      } else {
        entry.count += 1;
      }
    }

    const sorted = [...counts.values()].sort((left, right) => right.count - left.count);
    return new Series(
      sorted.map((entry) => entry.count),
      {
        index: sorted.map((entry) => String(entry.value)),
        name: this.name ? `${this.name}_counts` : "counts",
      }
    );
  }

  astype(dtype: SeriesDType): Series<CellValue> {
    return this.map((value) => coerceValueToDType(value, dtype), this.name);
  }

  isin(values: CellValue[]): Series<boolean> {
    return new Series(computeSeriesIsin(this._values, values), {
      index: this._index,
      name: this.name,
    });
  }

  clip(lower?: number, upper?: number): Series<CellValue> {
    if (lower !== undefined && upper !== undefined && lower > upper) {
      throw new Error("clip lower bound cannot exceed upper bound.");
    }
    return new Series(computeSeriesClip(this._values, lower, upper), {
      index: this._index,
      name: this.name,
    });
  }

  replace(toReplace: SeriesReplaceInput, value?: CellValue): Series<CellValue> {
    return new Series(computeSeriesReplace(this._values, toReplace, value), {
      index: this._index,
      name: this.name,
    });
  }

  private resolvePosition(position: number): number | undefined {
    if (!Number.isInteger(position)) {
      return undefined;
    }
    if (position >= 0 && position < this.length) {
      return position;
    }
    const resolved = this.length + position;
    if (resolved < 0 || resolved >= this.length) {
      return undefined;
    }
    return resolved;
  }

  // ---- arithmetic (pandas element-wise, null-propagating) ----

  /** Applies a numeric op; missing values stay missing. */
  private numericOp(
    other: number | Series<T>,
    fn: (left: number, right: number) => number,
    name?: string
  ): Series<number> {
    const otherValues =
      other instanceof Series ? other._values : new Array(this.length).fill(other);
    const out = this._values.map((value, i): number | null => {
      const right = otherValues[i];
      if (isMissing(value) || isMissing(right)) {
        return null;
      }
      const left = typeof value === "number" ? value : Number(value);
      const r = typeof right === "number" ? right : Number(right);
      return fn(left, r);
    });
    return new Series(out as never, { index: this._index, name: name ?? this.name });
  }

  add(other: number | Series<T>): Series<number> {
    return this.numericOp(other, (a, b) => a + b);
  }

  sub(other: number | Series<T>): Series<number> {
    return this.numericOp(other, (a, b) => a - b);
  }

  rsub(other: number | Series<T>): Series<number> {
    return this.numericOp(other, (a, b) => b - a);
  }

  mul(other: number | Series<T>): Series<number> {
    return this.numericOp(other, (a, b) => a * b);
  }

  div(other: number | Series<T>): Series<number> {
    return this.numericOp(other, (a, b) => a / b);
  }

  mod(other: number | Series<T>): Series<number> {
    return this.numericOp(other, (a, b) => a % b);
  }

  pow(other: number | Series<T>): Series<number> {
    return this.numericOp(other, (a, b) => a ** b);
  }

  // pandas long-form aliases
  subtract(other: number | Series<T>): Series<number> {
    return this.sub(other);
  }
  multiply(other: number | Series<T>): Series<number> {
    return this.mul(other);
  }
  truediv(other: number | Series<T>): Series<number> {
    return this.div(other);
  }
  floordiv(other: number | Series<T>): Series<number> {
    return this.numericOp(other, (a, b) => Math.floor(a / b));
  }
  radd(other: number | Series<T>): Series<number> {
    return this.add(other);
  }
  rmul(other: number | Series<T>): Series<number> {
    return this.mul(other);
  }
  rdiv(other: number | Series<T>): Series<number> {
    return this.numericOp(other, (a, b) => b / a);
  }
  rtruediv(other: number | Series<T>): Series<number> {
    return this.rdiv(other);
  }

  neg(): Series<number> {
    const out = this._values.map((value): number | null => {
      if (isMissing(value)) {
        return null;
      }
      return -(typeof value === "number" ? value : Number(value));
    });
    return new Series(out as never, { index: this._index, name: this.name });
  }

  abs(): Series<CellValue> {
    const out = this._values.map((value) => {
      if (isMissing(value)) {
        return null;
      }
      return Math.abs(typeof value === "number" ? value : Number(value));
    });
    return new Series(out, { index: this._index, name: this.name });
  }

  round(decimals = 0): Series<CellValue> {
    const factor = 10 ** decimals;
    const out = this._values.map((value) => {
      if (isMissing(value)) {
        return null;
      }
      const n = typeof value === "number" ? value : Number(value);
      return Math.round(n * factor) / factor;
    });
    return new Series(out, { index: this._index, name: this.name });
  }

  // ---- comparisons (return boolean Series) ----

  private compareOp(
    other: CellValue | Series<T>,
    fn: (left: number | string | boolean, right: number | string | boolean) => boolean
  ): Series<boolean> {
    const otherValues =
      other instanceof Series ? other._values : new Array(this.length).fill(other);
    const out = this._values.map((value, i): boolean | null => {
      const right = otherValues[i];
      if (isMissing(value) || isMissing(right)) {
        return null;
      }
      return fn(value as never, right as never);
    });
    return new Series(out as never, { index: this._index, name: this.name });
  }

  eq(other: CellValue | Series<T>): Series<boolean> {
    return this.compareOp(other, (a, b) => a === b);
  }

  ne(other: CellValue | Series<T>): Series<boolean> {
    return this.compareOp(other, (a, b) => a !== b);
  }

  lt(other: CellValue | Series<T>): Series<boolean> {
    return this.compareOp(other, (a, b) => (a as number) < (b as number));
  }

  le(other: CellValue | Series<T>): Series<boolean> {
    return this.compareOp(other, (a, b) => (a as number) <= (b as number));
  }

  gt(other: CellValue | Series<T>): Series<boolean> {
    return this.compareOp(other, (a, b) => (a as number) > (b as number));
  }

  ge(other: CellValue | Series<T>): Series<boolean> {
    return this.compareOp(other, (a, b) => (a as number) >= (b as number));
  }

  // ---- cumulative ----

  cumsum(): Series<number> {
    return new Series(cumsumValues(this._values) as unknown as number[], {
      index: this._index,
      name: this.name,
    });
  }

  cummax(): Series<T> {
    return new Series(cummaxValues(this._values) as unknown as T[], {
      index: this._index,
      name: this.name,
    });
  }

  cummin(): Series<T> {
    return new Series(cumminValues(this._values) as unknown as T[], {
      index: this._index,
      name: this.name,
    });
  }

  cumprod(): Series<number> {
    return new Series(cumprodValues(this._values) as unknown as number[], {
      index: this._index,
      name: this.name,
    });
  }

  /** 0-based position within the run of non-null values (pandas cumcount). */
  cumcount(): Series<number> {
    return new Series(cumcountValues(this._values) as unknown as number[], {
      index: this._index,
      name: this.name,
    });
  }

  isnull(): Series<boolean> {
    return this.isna();
  }

  notnull(): Series<boolean> {
    return this.notna();
  }

  /**
   * Rolling window aggregations (pandas rolling): `rolling(3).mean()`.
   * Windows at the start shorter than `min_periods ?? window` are null.
   */
  rolling(window: number, minPeriods?: number): RollingWindow {
    return computeRolling(this._values, window, minPeriods);
  }

  /** Expanding (cumulative) window aggregations. */
  expanding(minPeriods = 1): RollingWindow {
    return computeExpanding(this._values, minPeriods);
  }

  // ---- selection helpers ----

  nlargest(n = 5): Series<T> {
    const pairs = this._values
      .map((value, i) => ({ value, index: this._index[i]!, num: Number(value) }))
      .filter((pair) => !isMissing(pair.value) && Number.isFinite(pair.num))
      .sort((a, b) => b.num - a.num)
      .slice(0, Math.max(0, n));
    return new Series(pairs.map((p) => p.value), {
      index: pairs.map((p) => p.index),
      name: this.name,
    });
  }

  nsmallest(n = 5): Series<T> {
    const pairs = this._values
      .map((value, i) => ({ value, index: this._index[i]!, num: Number(value) }))
      .filter((pair) => !isMissing(pair.value) && Number.isFinite(pair.num))
      .sort((a, b) => a.num - b.num)
      .slice(0, Math.max(0, n));
    return new Series(pairs.map((p) => p.value), {
      index: pairs.map((p) => p.index),
      name: this.name,
    });
  }

  /**
   * pandas-style `.str` accessor for element-wise string operations.
   */
  get str(): StringMethods {
    return new StringMethods(this._values as CellValue[]);
  }

  /**
   * pandas-style `.dt` accessor for element-wise datetime parts.
   * Accepts Date values and parseable date strings.
   */
  get dt(): DatetimeMethods {
    return new DatetimeMethods(this._values as CellValue[]);
  }

  /**
   * Converts values to Date (parseable strings, epoch millis) with
   * null propagation — pandas `to_datetime` on a Series.
   */
  to_datetime(): Series<Date | null> {
    const out = (this._values as CellValue[]).map((value) => {
      if (value instanceof Date) {
        return Number.isNaN(value.getTime()) ? null : value;
      }
      if (typeof value === "string" || typeof value === "number") {
        const parsed = new Date(value);
        return Number.isNaN(parsed.getTime()) ? null : parsed;
      }
      return null;
    });
    return new Series(out, { index: this._index, name: this.name });
  }

  /**
   * Computes ranks alongside pandas: average method by default.
   * `method`: "average" | "min" | "max" | "first" | "dense".
   * NaN/null values get rank null. Ascending=false reverses order.
   */
  rank(
    options: {
      method?: "average" | "min" | "max" | "first" | "dense";
      ascending?: boolean;
      na_option?: "keep" | "bottom" | "top";
    } = {}
  ): Series<number | null> {
    const method = options.method ?? "average";
    const ascending = options.ascending !== false;
    const naOption = options.na_option ?? "keep";

    const scored = this._values
      .map((value, position) => ({ value, position, num: Number(value) }))
      .filter((entry) => !isMissing(entry.value) && Number.isFinite(entry.num));

    scored.sort((a, b) => (ascending ? a.num - b.num : b.num - a.num));

    const ranks = new Array<number | null>(this._values.length).fill(null);
    if (naOption === "top") {
      // Non-null values start after all null positions.
      const nullCount = this._values.length - scored.length;
      assignRanks(scored, ranks, method, nullCount);
    } else {
      assignRanks(scored, ranks, method, 0);
    }
    return new Series(ranks, { index: this._index, name: this.name });

    function assignRanks(
      entries: { value: unknown; position: number; num: number }[],
      target: (number | null)[],
      m: "average" | "min" | "max" | "first" | "dense",
      offset: number
    ): void {
      let i = 0;
      let denseRank = 0;
      while (i < entries.length) {
        let j = i;
        while (j + 1 < entries.length && entries[j + 1]!.num === entries[i]!.num) {
          j += 1;
        }
        denseRank += 1;
        // Ranks are 1-based over non-null entries.
        const first = offset + i + 1;
        const last = offset + j + 1;
        let rankValue: number;
        switch (m) {
          case "min":
            rankValue = first;
            break;
          case "max":
            rankValue = last;
            break;
          case "first":
            rankValue = first;
            for (let k = i; k <= j; k += 1) {
              target[entries[k]!.position] = offset + k + 1;
            }
            i = j + 1;
            continue;
          case "dense":
            rankValue = denseRank;
            break;
          default:
            rankValue = (first + last) / 2;
        }
        for (let k = i; k <= j; k += 1) {
          target[entries[k]!.position] = rankValue;
        }
        i = j + 1;
      }
    }
  }

  private valueKey(value: T): string {
    if (value instanceof Date) {
      return `date:${value.toISOString()}`;
    }
    return `${typeof value}:${String(value)}`;
  }
}
