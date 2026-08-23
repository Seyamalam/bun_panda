import type { CellValue, DType, IndexLabel } from "./types";
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
import { StringMethods } from "./internal/series/stringMethods";
import { DatetimeMethods } from "./internal/series/datetimeMethods";

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
    let acc = 0;
    const out = this._values.map((value): number | null => {
      if (isMissing(value)) {
        return null;
      }
      acc += typeof value === "number" ? value : Number(value);
      return acc;
    });
    return new Series(out as unknown as number[], { index: this._index, name: this.name });
  }

  cummax(): Series<T> {
    let acc: number | null = null;
    const out = this._values.map((value): T | null => {
      if (isMissing(value)) {
        return null;
      }
      const n = typeof value === "number" ? value : Number(value);
      acc = acc === null ? n : Math.max(acc, n);
      return acc as unknown as T;
    });
    return new Series(out as unknown as T[], { index: this._index, name: this.name });
  }

  cummin(): Series<T> {
    let acc: number | null = null;
    const out = this._values.map((value): T | null => {
      if (isMissing(value)) {
        return null;
      }
      const n = typeof value === "number" ? value : Number(value);
      acc = acc === null ? n : Math.min(acc, n);
      return acc as unknown as T;
    });
    return new Series(out as unknown as T[], { index: this._index, name: this.name });
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
