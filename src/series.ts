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
  seriesAll,
  seriesAny,
  seriesArgmax,
  seriesArgmin,
  seriesCount,
  seriesDescribe,
  seriesEquals,
  seriesProd,
  seriesStd,
  seriesVariance,
} from "./internal/series/stats";
import { samplePositions } from "./internal/dataframe/compat";
import { normalizeKeyCell } from "./internal/dataframe/keys";
import {
  cumcountValues,
  cummaxValues,
  cumminValues,
  cumprodValues,
  cumsumValues,
} from "./internal/series/cumulative";
import { computeRank } from "./internal/series/rank";
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

  rmod(other: number | Series<T>): Series<number> {
    return this.numericOp(other, (a, b) => b % a);
  }

  rpow(other: number | Series<T>): Series<number> {
    return this.numericOp(other, (a, b) => b ** a);
  }

  rfloordiv(other: number | Series<T>): Series<number> {
    return this.numericOp(other, (a, b) => Math.floor(b / a));
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
    const out: CellValue[] = this._values.map((value) => {
      if (isMissing(value)) {
        return null;
      }
      return Math.abs(typeof value === "number" ? value : Number(value));
    });
    return new Series(out as unknown as CellValue[], { index: this._index, name: this.name }) as unknown as Series<CellValue>;
  }

  round(decimals = 0): Series<CellValue> {
    const factor = 10 ** decimals;
    const out: CellValue[] = this._values.map((value) => {
      if (isMissing(value)) {
        return null;
      }
      const n = typeof value === "number" ? value : Number(value as CellValue);
      return Math.round(n * factor) / factor;
    });
    return new Series(out as unknown as CellValue[], { index: this._index, name: this.name }) as unknown as Series<CellValue>;
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

  /** Alias for isna — pandas isna/isnull. */
  is_null(): Series<boolean> {
    return this.isna();
  }

  // ---- required task APIs ----

  add_prefix(prefix: string): Series<T> {
    return new Series([...this._values], {
      index: this._index.map((label) => `${prefix}${String(label)}`),
      name: this.name,
    });
  }

  add_suffix(suffix: string): Series<T> {
    return new Series([...this._values], {
      index: this._index.map((label) => `${String(label)}${suffix}`),
      name: this.name,
    });
  }

  equals(other: Series): boolean {
    return seriesEquals(this._values as CellValue[], this._index, (other as Series<CellValue>)._values, (other as Series<CellValue>)._index);
  }

  get(key: IndexLabel, defaultVal?: T): T | undefined {
    const pos = this._index.findIndex((entry) => entry === key);
    if (pos < 0) return defaultVal as T | undefined;
    return this._values[pos];
  }

  all(skipna = true): boolean {
    return seriesAll(this._values as CellValue[], skipna);
  }

  any(skipna = true): boolean {
    return seriesAny(this._values as CellValue[], skipna);
  }

  argmax(): number | null {
    return seriesArgmax(this._values as CellValue[]);
  }

  argmin(): number | null {
    return seriesArgmin(this._values as CellValue[]);
  }

  copy(): Series<T> {
    return new Series([...this._values], { index: [...this._index], name: this.name });
  }

  std(ddof = 1): number | null {
    return seriesStd(numericValues(this._values as CellValue[]), ddof);
  }

  describe(): Series<CellValue> {
    const result = seriesDescribe(this._values as CellValue[]);
    return new Series(result.values, { index: result.keys, name: this.name });
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
  rank(options: {
    method?: "average" | "min" | "max" | "first" | "dense";
    ascending?: boolean;
    na_option?: "keep" | "bottom" | "top";
  } = {}): Series<number | null> {
    return new Series(computeRank(this._values, options), {
      index: this._index,
      name: this.name,
    });
  }

  // ---- pandas parity: missing ops ----

  get nbytes(): number {
    // Approximate: 8 bytes per entry (float64 width), matching common pandas reporting for numeric series
    return this._values.length * 8;
  }

  get empty(): boolean {
    return this._values.length === 0;
  }

  get ndim(): number {
    return 1;
  }

  get size(): number {
    return this._values.length;
  }

  get shape(): [number] {
    return [this._values.length];
  }

  /** Alias for kurt — pandas has both kurt and kurtosis. */
  kurtosis(): number | null {
    return this.kurt();
  }

  interpolate(method: "linear" = "linear"): Series<T> {
    if (method !== "linear") throw new Error(`interpolate: unsupported method '${method}'`);
    if (this._values.length === 0) return this.copy();
    const nums = this._values.map((v) => (typeof v === "number" && Number.isFinite(v) ? v : null));
    const out: (T | null)[] = [...this._values] as (T | null)[];
    // linear between numeric anchors
    let left = -1;
    for (let i = 0; i < nums.length; i += 1) {
      if (nums[i] !== null) { left = i; continue; }
      let right = i + 1;
      while (right < nums.length && nums[right] === null) right += 1;
      const lv = left >= 0 ? nums[left]! : null;
      const rv = right < nums.length ? nums[right] : null;
      if (lv !== null && rv !== null) {
        out[i] = (lv + (rv - lv) * ((i - left) / (right - left))) as unknown as T;
      } else if (lv !== null) {
        out[i] = lv as unknown as T;
      } else if (rv !== null) {
        out[i] = rv as unknown as T;
      }
    }
    return new Series(out as T[], { index: [...this._index], name: this.name });
  }

  /** True-shape helpers for parity (pandas reports these on Series). */
  get dtype(): string {
    return this.inferSeriesDType();
  }

  get dtypes(): string {
    return this.dtype;
  }

  agg(fn: string | ((values: CellValue[]) => CellValue)): CellValue {
    if (typeof fn === "function") return fn([...this._values]);
    const map: Record<string, () => CellValue> = {
      sum: () => this.sum(), mean: () => this.mean(), min: () => this.min() as CellValue,
      max: () => this.max() as CellValue, count: () => this.count(), std: () => this.std(),
      var: () => this.var(), median: () => this.median(), skew: () => this.skew(), kurt: () => this.kurt(),
      sem: () => this.sem(), prod: () => this.prod(),
    };
    if (!(fn in map)) throw new Error(`agg: unknown aggregator '${fn}'`);
    return map[fn]!();
  }

  aggregate(fn: string | ((values: CellValue[]) => CellValue)): CellValue {
    return this.agg(fn);
  }

  private inferSeriesDType(): string {
    let hasNumber = false, hasString = false, hasBool = false, hasDate = false;
    for (const v of this._values) {
      if (v === null || v === undefined || (typeof v === "number" && Number.isNaN(v as number))) continue;
      if (typeof v === "number") hasNumber = true;
      else if (typeof v === "string") hasString = true;
      else if (typeof v === "boolean") hasBool = true;
      else if (v instanceof Date) hasDate = true;
    }
    if (hasDate) return "date"; if (hasString) return "string"; if (hasBool) return "boolean"; if (hasNumber) return "number"; return "object";
  }

  nunique(dropna = true): number {
    const seen = new Set<string>();
    for (const value of this._values) {
      if (dropna && isMissing(value)) continue;
      seen.add(JSON.stringify(normalizeKeyCell(value as CellValue)));
    }
    return seen.size;
  }

  drop(labels: IndexLabel | IndexLabel[]): Series<T> {
    const toRemove = new Set(Array.isArray(labels) ? labels : [labels]);
    const values: T[] = [];
    const index: IndexLabel[] = [];
    for (let i = 0; i < this.length; i += 1) {
      if (toRemove.has(this._index[i]!)) continue;
      values.push(this._values[i]!);
      index.push(this._index[i]!);
    }
    // Mirror pandas: if no label matched, return copy unchanged
    return new Series(values, { index, name: this.name });
  }

  sort_values(options: { ascending?: boolean; na_position?: "first" | "last" } | boolean = true): Series<T> {
    const ascending = typeof options === "boolean" ? options : (options.ascending ?? true);
    const naPosition = typeof options === "boolean" ? "last" : (options.na_position ?? "last");
    if (naPosition !== "first" && naPosition !== "last") {
      throw new Error("na_position must be 'first' or 'last'.");
    }
    const indexed = this._values.map((value, position) => ({ value, position, label: this._index[position]! }));
    indexed.sort((a, b) => {
      const aMiss = isMissing(a.value);
      const bMiss = isMissing(b.value);
      if (aMiss && bMiss) return 0;
      if (aMiss) return naPosition === "last" ? 1 : -1;
      if (bMiss) return naPosition === "last" ? -1 : 1;
      const cmp = compareCellValues(a.value as CellValue, b.value as CellValue);
      return ascending ? cmp : -cmp;
    });
    return new Series(indexed.map((e) => e.value), {
      index: indexed.map((e) => e.label),
      name: this.name,
    });
  }

  sort_index(ascending = true): Series<T> {
    const indexed = this._values.map((value, position) => ({ value, position, label: this._index[position]! }));
    indexed.sort((a, b) => {
      const cmp = compareCellValues(a.label as CellValue, b.label as CellValue);
      return ascending ? cmp : -cmp;
    });
    return new Series(indexed.map((e) => e.value), {
      index: indexed.map((e) => e.label),
      name: this.name,
    });
  }

  where(cond: boolean[] | ((value: T, label: IndexLabel, position: number) => boolean), other: T | null = null): Series<T> {
    const decision = this.resolveWhereCond(cond);
    const out = this._values.map((value, i) => {
      const label = this._index[i]!;
      return decision(value, label, i) ? value : (other as T);
    });
    return new Series(out, { index: [...this._index], name: this.name });
  }

  mask(cond: boolean[] | ((value: T, label: IndexLabel, position: number) => boolean), other: T | null = null): Series<T> {
    const decision = this.resolveWhereCond(cond);
    const out = this._values.map((value, i) => {
      const label = this._index[i]!;
      return decision(value, label, i) ? (other as T) : value;
    });
    return new Series(out, { index: [...this._index], name: this.name });
  }

  private resolveWhereCond(
    cond: boolean[] | ((value: T, label: IndexLabel, position: number) => boolean)
  ): (value: T, label: IndexLabel, position: number) => boolean {
    if (Array.isArray(cond)) {
      if (cond.length !== this.length) throw new Error("where/mask condition length must match series length.");
      return (_v, _l, i) => Boolean(cond[i]);
    }
    return cond;
  }

  sample(n = 1, options: { replace?: boolean; random_state?: number; frac?: number; ignore_index?: boolean } = {}): Series<T> {
    const replace = options.replace ?? false;
    const ignoreIndex = options.ignore_index ?? false;
    let sampleSize = n;
    if (options.frac !== undefined) {
      if (options.frac < 0) throw new Error("sample frac must be non-negative.");
      sampleSize = Math.round(options.frac * this.length);
    }
    if (!replace && sampleSize > this.length) {
      throw new Error("sample size cannot exceed series length when replace=false.");
    }
    if (!Number.isInteger(sampleSize) || sampleSize < 0) {
      throw new Error("sample size must be a non-negative integer.");
    }
    const positions = samplePositions(this.length, sampleSize, replace, options.random_state);
    const values = positions.map((p) => this._values[p]!);
    const index = ignoreIndex ? range(values.length) : positions.map((p) => this._index[p]!);
    return new Series(values as unknown as T[], { index, name: this.name });
  }

  reindex(newIndex: IndexLabel[], fill_value: T | null = null): Series<T> {
    const posByLabel = new Map<IndexLabel, number>();
    for (let i = 0; i < this._index.length; i += 1) {
      // keep first occurrence, matching pandas
      if (!posByLabel.has(this._index[i]!)) posByLabel.set(this._index[i]!, i);
    }
    const values = newIndex.map((label) => {
      const pos = posByLabel.get(label);
      return pos !== undefined ? this._values[pos]! : (fill_value as T);
    });
    return new Series(values, { index: [...newIndex], name: this.name });
  }

  shift(periods = 1): Series<T | null> {
    const out: (T | null)[] = new Array(this.length).fill(null);
    for (let i = 0; i < this.length; i += 1) {
      const src = i - periods;
      if (src >= 0 && src < this.length) out[i] = this._values[src]! as unknown as T | null;
      else out[i] = null;
    }
    return new Series(out as unknown as T[], { index: [...this._index], name: this.name }) as unknown as Series<T | null>;
  }

  diff(periods = 1): Series<number | null> {
    const out: (number | null)[] = new Array(this.length).fill(null);
    for (let i = 0; i < this.length; i += 1) {
      const src = i - periods;
      if (src < 0 || src >= this.length) { out[i] = null; continue; }
      const a = this._values[i] as unknown as CellValue;
      const b = this._values[src] as unknown as CellValue;
      if (typeof a === "number" && Number.isFinite(a) && typeof b === "number" && Number.isFinite(b)) {
        out[i] = a - b;
      } else {
        out[i] = null;
      }
    }
    return new Series(out as unknown as CellValue[], { index: [...this._index], name: this.name }) as unknown as Series<number | null>;
  }

  pct_change(periods = 1): Series<number | null> {
    const out: (number | null)[] = new Array(this.length).fill(null);
    for (let i = 0; i < this.length; i += 1) {
      const src = i - periods;
      if (src < 0 || src >= this.length) { out[i] = null; continue; }
      const a = this._values[i] as unknown as CellValue;
      const b = this._values[src] as unknown as CellValue;
      if (typeof a === "number" && Number.isFinite(a) && typeof b === "number" && Number.isFinite(b) && b !== 0) {
        out[i] = (a - b) / b;
      } else {
        out[i] = null;
      }
    }
    return new Series(out as unknown as CellValue[], { index: [...this._index], name: this.name }) as unknown as Series<number | null>;
  }

  drop_duplicates(keep: "first" | "last" | false = "first"): Series<T> {
    const keepFlags = this.duplicateKeepFlags(keep);
    const values: T[] = [];
    const index: IndexLabel[] = [];
    for (let i = 0; i < this.length; i += 1) {
      if (!keepFlags[i]) continue;
      values.push(this._values[i]!);
      index.push(this._index[i]!);
    }
    return new Series(values, { index, name: this.name });
  }

  duplicated(keep: "first" | "last" | false = "first"): Series<boolean> {
    const keepFlags = this.duplicateKeepFlags(keep);
    return new Series(keepFlags.map((f) => !f) as unknown as boolean[], {
      index: [...this._index],
      name: "duplicated",
    });
  }

  private duplicateKeepFlags(keep: "first" | "last" | false): boolean[] {
    const include = new Array(this.length).fill(false);
    if (keep === false) {
      const counts = new Map<string, number>();
      for (const v of this._values) {
        const k = JSON.stringify(normalizeKeyCell(v as CellValue));
        counts.set(k, (counts.get(k) ?? 0) + 1);
      }
      for (let i = 0; i < this.length; i += 1) {
        const k = JSON.stringify(normalizeKeyCell(this._values[i] as CellValue));
        include[i] = (counts.get(k) ?? 0) === 1;
      }
    } else if (keep === "last") {
      const seen = new Set<string>();
      for (let i = this.length - 1; i >= 0; i -= 1) {
        const k = JSON.stringify(normalizeKeyCell(this._values[i] as CellValue));
        if (!seen.has(k)) { seen.add(k); include[i] = true; }
      }
    } else {
      const seen = new Set<string>();
      for (let i = 0; i < this.length; i += 1) {
        const k = JSON.stringify(normalizeKeyCell(this._values[i] as CellValue));
        if (!seen.has(k)) { seen.add(k); include[i] = true; }
      }
    }
    return include;
  }

  private valueKey(value: T): string {
    if (value instanceof Date) {
      return `date:${value.toISOString()}`;
    }
    return `${typeof value}:${String(value)}`;
  }
  // ---- additional pandas compat (parity gaps) ----

  corr(other: Series<CellValue>): number | null {
    const a: number[] = []; const b: number[] = [];
    for (let i = 0; i < this._values.length; i += 1) {
      const x = this._values[i] as unknown as CellValue; const y = (other as unknown as Series<CellValue>)._values[i] as unknown as CellValue;
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

  cov(other: Series<CellValue>): number | null {
    const a: number[] = []; const b: number[] = [];
    for (let i = 0; i < this._values.length; i += 1) {
      const x = this._values[i] as unknown as CellValue; const y = (other as unknown as Series<CellValue>)._values[i] as unknown as CellValue;
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

  dot(other: Series<CellValue>): number | null {
    if (this._values.length !== other.length) throw new Error("dot: length mismatch");
    let sum = 0; let any = false;
    for (let i = 0; i < this._values.length; i += 1) {
      const x = this._values[i]; const y = (other as Series<CellValue>)._values[i];
      if (typeof x !== "number" || !Number.isFinite(x) || typeof y !== "number" || !Number.isFinite(y)) continue;
      sum += x * y; any = true;
    }
    return any ? sum : null;
  }

  first_valid_index(): IndexLabel | null {
    for (let i = 0; i < this._values.length; i += 1) if (!isMissing(this._values[i])) return this._index[i]!;
    return null;
  }

  last_valid_index(): IndexLabel | null {
    for (let i = this._values.length - 1; i >= 0; i -= 1) if (!isMissing(this._values[i])) return this._index[i]!;
    return null;
  }

  factorize(): [number[], CellValue[]] {
    const uniq: CellValue[] = []; const idx = new Map<string, number>();
    const codes: number[] = [];
    for (const v of this._values) {
      const k = JSON.stringify(normalizeKeyCell(v as CellValue));
      if (!idx.has(k)) { idx.set(k, uniq.length); uniq.push(v as CellValue); }
      codes.push(idx.get(k)!);
    }
    return [codes, uniq];
  }

  explode(): Series<CellValue> {
    const vals: CellValue[] = [];
    const idx: IndexLabel[] = [];
    for (let i = 0; i < this._values.length; i += 1) {
      const v = this._values[i];
      if (Array.isArray(v)) {
        for (const item of v as CellValue[]) { vals.push(item as CellValue); idx.push(this._index[i]!); }
      } else { vals.push(v as CellValue); idx.push(this._index[i]!); }
    }
    return new Series(vals as unknown as T[], { index: idx, name: this.name }) as unknown as Series<CellValue>;
  }

  groupby(by: string | string[]): import("./groupby").GroupBy {
    const df = this.to_frame(String(this.name ?? "value"));
    return df.groupby(by);
  }

  to_numpy(): CellValue[] { return [...this._values]; }

  to_string(): string { return this._values.map((v) => String(v)).join("\\n"); }
  to_csv(): string { return this._values.map((v) => String(v ?? "")).join("\\n"); }
  to_json(): string { return JSON.stringify(this._values); }
  to_period(): Series<T> { return this.copy(); }

  info(): string {
    return `Series: ${this.length} entries, dtype=${this.dtype}, hasNaN=${this.hasnans()}`;
  }

  items(): [IndexLabel, T][] { return this._values.map((v, i) => [this._index[i]!, v]); }
  keys(): IndexLabel[] { return [...this._index]; }

  pop(label: IndexLabel): T | undefined {
    const pos = this._index.findIndex((e) => e === label);
    if (pos < 0) throw new Error(`pop: label '${String(label)}' not found`);
    const v = this._values[pos]!;
    (this as unknown as { _values: T[] })._values.splice(pos, 1);
    (this as unknown as { _index: IndexLabel[] })._index.splice(pos, 1);
    return v;
  }

  repeat(repeats: number): Series<T> {
    if (!Number.isInteger(repeats) || repeats < 0) throw new Error("repeat: repeats must be non-negative integer");
    const vals: T[] = []; const idx: IndexLabel[] = [];
    for (let i = 0; i < this._values.length; i += 1) {
      for (let r = 0; r < repeats; r += 1) { vals.push(this._values[i]!); idx.push(this._index[i]!); }
    }
    return new Series(vals, { index: idx, name: this.name });
  }

  rename(name: string): Series<T> { return new Series([...this._values], { index: [...this._index], name }); }

  rename_axis(name: string): Series<T> { return this.rename(name); }

  reset_index(drop = false): DataFrame | Series<T> {
    if (drop) return new Series([...this._values], { index: range(this._values.length), name: this.name });
    const df = new DataFrame(
      this._values.map((v, i) => ({ index: this._index[i]!, [String(this.name ?? "0")]: v })),
      { columns: ["index", String(this.name ?? "0")] }
    );
    return df;
  }

  set_axis(labels: IndexLabel[]): Series<T> {
    if (labels.length !== this._values.length) throw new Error("set_axis: length mismatch");
    return new Series([...this._values], { index: [...labels], name: this.name });
  }

  squeeze(): T | Series<T> { return this.length === 1 ? this._values[0]! : this; }

  take(indices: number[]): Series<T> {
    return new Series((indices.map((i) => {
      const pos = i < 0 ? this._values.length + i : i;
      if (pos < 0 || pos >= this._values.length) throw new Error(`take: index ${String(i)} out of bounds`);
      return this._values[pos]!;
    }) as unknown as T[]), { index: indices.map((i) => { const p = i < 0 ? this._index.length + i : i; return this._index[p]!; }), name: this.name });
  }

  transform(fn: (s: Series<T>) => Series<T> | CellValue[]): Series<T> {
    const r = fn(this);
    return Array.isArray(r) ? new Series(r as T[], { index: [...this._index], name: this.name }) : r as Series<T>;
  }

  truncate(before?: IndexLabel, after?: IndexLabel): Series<T> {
    let start = 0, end = this._values.length;
    if (before !== undefined) { const p = this._index.indexOf(before); if (p >= 0) start = p; }
    if (after !== undefined) { const p = this._index.indexOf(after); if (p >= 0) end = p + 1; }
    return new Series(this._values.slice(start, end), { index: this._index.slice(start, end), name: this.name });
  }

  update(other: Series<T>): void {
    for (let i = 0; i < other.length; i += 1) {
      const label = other.index[i]!;
      const pos = this._index.indexOf(label);
      const v = other._values[i];
      if (pos >= 0 && !isMissing(v)) (this as unknown as { _values: T[] })._values[pos] = v;
    }
  }

  memory_usage(): number { return this._values.length * 8; }

  get at(): Record<string, T | undefined> {
    const self = this;
    return new Proxy({} as Record<string, T | undefined>, {
      get(_t, prop: string) { return self.loc(prop as unknown as IndexLabel); },
    });
  }

  get iat(): Record<number, T | undefined> {
    const self = this;
    return new Proxy({} as Record<number, T | undefined>, {
      get(_t, prop: string) { return self.iloc(Number(prop)); },
    });
  }

  get array(): CellValue[] { return [...this._values]; }
  get list(): CellValue[] { return [...this._values]; }

}
