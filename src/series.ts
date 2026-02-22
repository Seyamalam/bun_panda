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

  private valueKey(value: T): string {
    if (value instanceof Date) {
      return `date:${value.toISOString()}`;
    }
    return `${typeof value}:${String(value)}`;
  }
}
