import { alignedPositions, compareIndexLabels, indexLabelEquals, indexLabelsEqual, reindexPositions } from "./internal/shared/indexing";
import {
  crossSectionPositions,
  dropTupleLevels,
  indexTuple,
  indexTuples,
  prefixPositions,
  resolveIndexLevel,
  unstackCellKey,
  unstackGrid,
} from "./internal/shared/multiindex";
import { arithResultDType, castColumn, convertedDType, floordivResultDType, isCategoricalDType, replacementDType, scalarOperandDType } from "./dtypes";
import { Categorical, CategoricalDtype, castCategoricalValues, categoryIndex, inferCategoricalDtype, sameCategorySet } from "./categorical";
import { isAbsent, scalarEquals, scalarKey, roundToEven, numberOrBigintEqual, bigintOperand, bigintMod, bigintFloorDiv } from "./internal/shared/valueSemantics";
import type { CellValue, DType, IndexLabel } from "./types";
import type { GroupByOptions } from "./groupby";
import { DataFrame } from "./dataframe";
import {
  compareCellValues,
  isMissing,
  numericValues,
  pythonOperandTypeName,
  range,
} from "./utils";
import {
  computeSeriesIsin,
  computeSeriesReplace,
  type SeriesReplaceInput,
} from "./internal/series/compat";
import {
  adjustedSkew,
  autocorrelation,
  excessKurtosis,
  seriesCount,
  seriesDescribe,
  seriesEquals,
  seriesStd,
  seriesVariance,
} from "./internal/series/stats";
import { samplePositions } from "./internal/dataframe/compat";
import {
  cumcountValues,
} from "./internal/series/cumulative";
import { computeRank, type RankOptions } from "./internal/series/rank";
import { KeyError, NotSupportedError, ValueError, ZeroDivisionError } from "./errors";
import * as seriesApi from "./internal/series/seriesApi";
import {
  isMaskedDType,
  missingScalar,
  pandasAllAny,
  pandasArgExtreme,
  pandasIdxExtreme,
  pandasMinMax,
  pandasMode,
  pandasProd,
  pandasSum,
  requireComparable,
  requireNumeric,
  requireQuantileable,
} from "./internal/series/pandasOps";
import {
  pandasAbs,
  pandasClip,
  pandasCumMinMax,
  pandasCumSumProd,
  pandasDiff,
  pandasPctChange,
} from "./internal/series/pandasElementwise";
import { NA, NaT } from "./missing";
import { Period, isPeriodDType } from "./period";
import {
  asciiBoxplot,
  asciiHistogram,
  sparseInfo,
  svgHistogram,
  svgLine,
} from "./internal/shared/plotting";
import {
} from "./internal/shared";
import { StringMethods } from "./internal/series/stringMethods";
import { DatetimeMethods } from "./internal/series/datetimeMethods";
import { CategoricalMethods } from "./internal/series/categoricalMethods";
import { assertWindowAggregable, computeExpanding, computeRolling, type RollingWindow } from "./internal/dataframe/rolling";

export type SeriesDType = DType | CategoricalDtype;

export interface SeriesOptions {
  /** pandas dtype spelling, or a CategoricalDtype carrying category metadata. */
  dtype?: DType | CategoricalDtype;
  name?: string;
  index?: IndexLabel[];
  indexName?: string;
  /** One name per level for tuple-labeled MultiIndex series. */
  indexNames?: (string | undefined)[];
}

export type { SeriesReplaceInput };

export class Series<T extends CellValue = CellValue> {
  private readonly _dtype: DType;
  /** Category metadata; set exactly when `_dtype === "category"`. */
  private readonly _catDtype: CategoricalDtype | undefined;
  private readonly _name: string | undefined;
  private readonly _values: T[];
  private readonly _index: IndexLabel[];
  private readonly _indexName: string | undefined;
  /** pandas `series.index.names` — one per level; set for MultiIndex series. */
  private readonly _indexNames: (string | undefined)[] | undefined;

  constructor(values: T[] | Categorical, options: SeriesOptions = {}) {
    const sourceValues: T[] = (values instanceof Categorical ? values.to_list() : values) as T[];
    // pandas `Series(values, dtype=pd.CategoricalDtype(...))` and
    // `Series(pd.Categorical(...))` both produce category dtype.
    const suppliedCat = options.dtype instanceof CategoricalDtype ? options.dtype
      : (options.dtype === undefined || isCategoricalDType(options.dtype)) && values instanceof Categorical ? values.dtype
      : undefined;
    this._catDtype = suppliedCat ?? (isCategoricalDType(options.dtype as DType | undefined)
      ? inferCategoricalDtype(sourceValues) : undefined);
    this._dtype = this._catDtype !== undefined ? "category" : (options.dtype as DType | undefined ?? convertedDType(sourceValues, undefined, false));
    this._values = (this._catDtype !== undefined
      ? castCategoricalValues(sourceValues, this._catDtype)
      : castColumn(sourceValues, this._dtype)) as T[];
    this._index = options.index ? [...options.index] : range(sourceValues.length);
    this._indexName = options.indexName;
    this._indexNames = options.indexNames;
    this._name = options.name;

    if (this._index.length !== this._values.length) {
      throw new Error("Series index length must match values length.");
    }
  }

  static from_arrow<T extends CellValue = CellValue>(values: T[], options: SeriesOptions = {}): Series<T> {
    return new Series(values, options);
  }

  get values(): T[] {
    return [...this._values];
  }

  get index(): IndexLabel[] {
    return [...this._index];
  }

  /** Series name (pandas exposes this as a plain attribute). */
  get name(): string | undefined {
    return this._name;
  }

  /** pandas `series.index.name` for single-level indexes. */
  get index_name(): string | undefined {
    return this._indexName;
  }

  /** pandas `series.index.names` — one entry per index level. */
  get index_names(): (string | undefined)[] {
    if (this._indexNames) {
      return this._indexNames;
    }
    const levels = this.nlevels;
    if (levels <= 1) {
      return [this._indexName];
    }
    return Array.from({ length: levels }, (_, i) => (i === 0 ? this._indexName : undefined));
  }

  /** pandas `series.index.nlevels`. */
  get nlevels(): number {
    const tuple = this._index.find(l => Array.isArray(l));
    return tuple ? (tuple as (string | number)[]).length : 1;
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

  /**
   * Label lookup like pandas `s.loc[...]`:
   * - `loc("a")` on a flat index returns the matching value or undefined.
   * - `loc("a")` on a MultiIndex takes the outer-level slice — a Series with
   *   the matched leading levels dropped (pandas `s.loc["a"]`).
   * - `loc("a", 1)` / `loc([["a", 1]])` address tuple keys; a unique
   *   full-length match returns the value.
   * - `loc(["a", "b"])` is a label list: scalars select level-0 groups
   *   keeping the full tuple index (pandas `s.loc[["a","b"]]`).
   */
  loc(label: IndexLabel | IndexLabel[], ...rest: (string | number)[]): T | Series<T> | undefined {
    if (rest.length > 0) {
      return this.locLevels([label as string | number, ...rest]);
    }
    if (Array.isArray(label)) {
      return this.locList(label);
    }
    if (this.nlevels > 1) {
      return this.locLevels([label as string | number]);
    }
    const position = this._index.findIndex((entry) => indexLabelEquals(entry, label));
    if (position < 0) {
      return undefined;
    }
    return this._values[position];
  }

  /** MultiIndex `.loc` prefix selection matching the leading `parts` levels. */
  private locLevels(parts: (string | number)[]): T | Series<T> | undefined {
    const nlevels = this.nlevels;
    if (parts.length > nlevels) {
      throw new KeyError(`'(${parts.join(", ")})'`);
    }
    const positions = prefixPositions(indexTuples(this._index), parts);
    if (positions.length === 0) {
      throw new KeyError(`'${parts.length === 1 ? String(parts[0]) : `(${parts.join(", ")})`}'`);
    }
    if (parts.length === nlevels) {
      if (positions.length === 1) {
        return this._values[positions[0]!];
      }
      return this.withIndexAndValues(
        positions.map((position) => this._index[position]!),
        positions.map((position) => this._values[position]!),
        this._indexNames
      );
    }
    const dropped = new Set(range(parts.length));
    const nextNames = this.index_names.filter((_, i) => !dropped.has(i));
    return this.withIndexAndValues(
      positions.map((position) => dropTupleLevels(indexTuple(this._index[position]!), dropped)),
      positions.map((position) => this._values[position]!),
      nextNames
    );
  }

  /** `.loc[list]` — flat labels select exactly; on a MultiIndex scalars match level 0. */
  private locList(selector: IndexLabel[]): Series<T> {
    const nlevels = this.nlevels;
    if (nlevels <= 1 || selector.some((element) => Array.isArray(element))) {
      // Full-key matching: every element must match a complete label.
      const index: IndexLabel[] = [];
      const values: T[] = [];
      for (const element of selector) {
        if (nlevels > 1 && !Array.isArray(element)) {
          throw new KeyError(`'[${JSON.stringify(element)}] not in index'`);
        }
        const position = this._index.findIndex((entry) => indexLabelEquals(entry, element));
        if (position < 0) {
          throw new KeyError(`'[${JSON.stringify(element)}] not in index'`);
        }
        index.push(this._index[position]!);
        values.push(this._values[position]!);
      }
      return this.withIndexAndValues(index, values, this._indexNames);
    }

    const tuples = indexTuples(this._index);
    const missing = selector.filter((label) =>
      !tuples.some((tuple) => indexLabelEquals(tuple[0] as IndexLabel, label))
    );
    if (missing.length > 0) {
      throw new KeyError(`'[${missing.map((label) => JSON.stringify(label)).join(", ")}] not in index'`);
    }
    const index: IndexLabel[] = [];
    const values: T[] = [];
    for (const label of selector) {
      tuples.forEach((tuple, position) => {
        if (indexLabelEquals(tuple[0] as IndexLabel, label)) {
          index.push(this._index[position]!);
          values.push(this._values[position]!);
        }
      });
    }
    return this.withIndexAndValues(index, values, this._indexNames);
  }

  /** Rebuild this series over a new index, keeping dtype/name metadata. */
  private withIndexAndValues(index: IndexLabel[], values: T[], indexNames?: (string | undefined)[]): Series<T> {
    return new Series(values, {
      index,
      indexName: indexNames?.[0] ?? this._indexName,
      indexNames: indexNames ?? this._indexNames,
      name: this.name,
      dtype: this._catDtype ?? this._dtype,
    });
  }

  head(n = 5): Series<T> {
    const count = n < 0 ? Math.max(0,this.length + n) : n;
    return new Series(this._values.slice(0, count), {
      index: this._index.slice(0, count),
      name: this.name,
      dtype: this._catDtype ?? this._dtype,
    });
  }

  tail(n = 5): Series<T> {
    const start = n === 0 ? this.length : n < 0 ? Math.min(-n,this.length) : Math.max(0,this.length-n);
    return new Series(this._values.slice(start), {
      index: this._index.slice(start),
      name: this.name,
      dtype: this._catDtype ?? this._dtype,
    });
  }

  map<U extends CellValue>(
    fn: (value: T, index: IndexLabel, position: number) => U,
    name = this.name
  ): Series<U> {
    const values = this._values.map((value, position) =>
      fn(value, this._index[position]!, position)
    );
    return new Series(values, { index: this._index, indexName: this._indexName, name });
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

    return new Series(values, { index, name: this.name, dtype: this._catDtype ?? this._dtype });
  }

  /**
   * pandas `Series.fillna`: a scalar fills every missing cell; a plain
   * object fills cells whose *index label* matches a key; lists raise the
   * pandas TypeError.
   */
  fillna(value: T): Series<T> {
    if (Array.isArray(value)) {
      throw new TypeError(`"value" parameter must be a scalar or dict, but you passed a "list"`);
    }
    if (typeof value === "object" && value !== null && !(value instanceof Date) && !(value instanceof Period)) {
      const mapping = value as Record<string, CellValue>;
      const filled = this._values.map((entry, i) => {
        if (!isMissing(entry)) return entry;
        const key = String(this._index[i]!);
        return key in mapping ? (mapping[key] as T) : entry;
      }) as T[];
      // pandas str/string dtypes store missing as NaN, so widening to
      // object keeps NaN (not the NA sentinel other nullable dtypes use).
      const widened = (this._dtype === "string" || this._dtype === "str") &&
        replacementDType(this._dtype, filled) === "object"
        ? filled.map((entry) => (isMissing(entry) ? (NaN as T) : entry))
        : filled;
      return this.withReplacement(widened);
    }
    return this.withReplacement(this._values.map(entry => isMissing(entry) ? value : entry));
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
    return this.withReplacement(out as T[]);
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
    return this.withReplacement(out as T[]);
  }

  isna(): Series<boolean> {
    const out = this._values.map(
      (value) => isMissing(value) || (typeof value === "number" && Number.isNaN(value))
    );
    return new Series(out, { index: this._index, indexName: this._indexName, name: this.name });
  }

  notna(): Series<boolean> {
    return this.isna().map((value) => !value) as Series<boolean>;
  }

  /** q-quantile of the numeric values (linear interpolation). */
  quantile(q = 0.5): CellValue {
    if (this._catDtype !== undefined) return this.catQuantile(q);
    if (this._dtype === "string" || this._dtype === "str") {
      // pandas: empty string series raises ValueError, non-empty TypeError.
      if (this._values.every((value) => isMissing(value))) {
        throw new ValueError("cannot do a non-empty take from an empty axes.");
      }
      throw new TypeError("'quantile' is not supported for string dtype");
    }
    requireQuantileable(this._values as CellValue[], this._dtype);
    const values = requireNumeric(this._values as CellValue[], this._dtype, "quantile").sort((a, b) => a - b);
    if (values.length === 0) {
      return missingScalar(this._dtype);
    }
    const pos = (values.length - 1) * q;
    const lower = Math.floor(pos);
    const upper = Math.ceil(pos);
    return values[lower]! + (values[upper]! - values[lower]!) * (pos - lower);
  }

  /** Most frequent value(s); pandas returns them sorted ascending. */
  mode(): T[] {
    return pandasMode(this._values as CellValue[], (v) => this.valueKey(v as T)) as T[];
  }

  /** Functional chaining helper (pandas pipe). */
  pipe<T>(fn: (series: any, ...args: never[]) => T, ...args: never[]): T {
    return fn(this, ...args);
  }

  /** Index label of the maximum value (pandas idxmax). */
  idxmax(): IndexLabel {
    if (this._catDtype !== undefined) {
      const extreme = this.catExtreme("max");
      if (extreme === undefined) throw new ValueError("attempt to get argmax of an empty sequence");
      return this._index[extreme.position]!;
    }
    return pandasIdxExtreme(this._values as CellValue[], this._index, "max");
  }

  /** Index label of the minimum value (pandas idxmin). */
  idxmin(): IndexLabel {
    if (this._catDtype !== undefined) {
      const extreme = this.catExtreme("min");
      if (extreme === undefined) throw new ValueError("attempt to get argmin of an empty sequence");
      return this._index[extreme.position]!;
    }
    return pandasIdxExtreme(this._values as CellValue[], this._index, "min");
  }

  /**
   * Positions that would keep the values sorted (pandas argsort).
   * Stable; missing values go last.
   */
  argsort(): number[] {
    // pandas argsort keeps missing values, placing them last in original order.
    this.requireSortable();
    const indexed = this._values
      .map((value, position) => ({ value, position }));
    const present = indexed.filter((entry) => !isMissing(entry.value));
    present.sort((a, b) => compareCellValues(a.value, b.value));
    return [
      ...present.map((entry) => entry.position),
      ...indexed.filter((entry) => isMissing(entry.value)).map((entry) => entry.position),
    ];
  }

  /** object dtype with incomparable mixed kinds raises TypeError, like Python. */
  private requireSortable(): void {
    if (this._dtype !== "object") {
      return;
    }
    const kinds = new Set<string>();
    for (const value of this._values) {
      if (isMissing(value)) {
        continue;
      }
      if (typeof value === "number" || typeof value === "bigint" || typeof value === "boolean") {
        kinds.add("numeric");
      } else if (typeof value === "string") {
        kinds.add("string");
      } else if (value instanceof Date) {
        kinds.add("date");
      } else {
        kinds.add("other");
      }
    }
    if (kinds.size > 1) {
      throw new TypeError("'<' not supported between instances of different types");
    }
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

  private isMonotonic(increasing: boolean): boolean {
    if (this._values.some(isAbsent)) return false;
    for (let i = 1; i < this._values.length; i += 1) {
      const a = this._values[i-1]; const b = this._values[i];
      const numeric = (typeof a === "number" || typeof a === "boolean") && (typeof b === "number" || typeof b === "boolean");
      const strings = typeof a === "string" && typeof b === "string";
      const dates = a instanceof Date && b instanceof Date;
      const periods = a instanceof Period && b instanceof Period;
      if (!numeric && !strings && !dates && !periods) return false;
      const cmp = numeric ? Number(a) - Number(b) : strings ? (a < b ? -1 : a > b ? 1 : 0) : compareCellValues(a,b);
      if (increasing ? cmp > 0 : cmp < 0) return false;
    }
    return true;
  }

  is_monotonic_increasing(): boolean { return this.isMonotonic(true); }
  is_monotonic_decreasing(): boolean { return this.isMonotonic(false); }

  /** Missing values participate in uniqueness. */
  is_unique(): boolean {
    return new Set(this._values.map(scalarKey)).size === this._values.length;
  }

  hasnans(): boolean { return this._values.some(isAbsent); }

  /** Number of non-missing values (pandas count). */
  count(): number {
    return seriesCount(this._values);
  }

  /** pandas float32 reductions round to single precision. */
  private maybeF32(value: CellValue): CellValue {
    return (this._dtype === "float32" || this._dtype === "Float32") && typeof value === "number"
      ? Math.fround(value)
      : value;
  }

  median(): CellValue {
    if (this._catDtype !== undefined) this.catUnsupported("median");
    // Unlike quantile(), bool values participate: pandas bool median -> float.
    const values = requireNumeric(this._values as CellValue[], this._dtype, "median").sort((a, b) => a - b);
    if (values.length === 0) {
      return missingScalar(this._dtype);
    }
    const pos = (values.length - 1) * 0.5;
    const lower = Math.floor(pos);
    const upper = Math.ceil(pos);
    return this.maybeF32(values[lower]! + (values[upper]! - values[lower]!) * (pos - lower));
  }

  var(ddof = 1): CellValue {
    if (this._catDtype !== undefined) this.catUnsupported("var");
    const numbers = requireNumeric(this._values as CellValue[], this._dtype, "var");
    return numbers.length === 0 ? missingScalar(this._dtype) : (this.maybeF32(seriesVariance(numbers, ddof) ?? missingScalar(this._dtype)));
  }

  prod(): CellValue {
    if (this._catDtype !== undefined) this.catUnsupported("prod");
    return this.maybeF32(pandasProd(this._values as CellValue[], this._dtype));
  }

  product(): CellValue {
    return this.prod();
  }

  /** Unbiased skewness (Fisher-Pearson with sample correction G1). */
  skew(): CellValue {
    if (this._catDtype !== undefined) this.catUnsupported("skew");
    const numbers = requireNumeric(this._values as CellValue[], this._dtype, "skew");
    return numbers.length === 0 ? missingScalar(this._dtype) : (this.maybeF32(adjustedSkew(numbers) ?? missingScalar(this._dtype)));
  }


  /** Excess kurtosis (Fisher definition; normal distribution = 0). */
  kurt(): CellValue {
    if (this._catDtype !== undefined) this.catUnsupported("kurt");
    const numbers = requireNumeric(this._values as CellValue[], this._dtype, "kurt");
    return numbers.length === 0 ? missingScalar(this._dtype) : (this.maybeF32(excessKurtosis(numbers) ?? missingScalar(this._dtype)));
  }

  /** Standard error of the mean (sample std / sqrt(n)). */
  sem(ddof = 1): CellValue {
    if (this._catDtype !== undefined) this.catUnsupported("sem");
    const numbers = requireNumeric(this._values as CellValue[], this._dtype, "sem");
    if (numbers.length === 0) return missingScalar(this._dtype);
    const std = seriesStd(numbers, ddof);
    if (std === null) return missingScalar(this._dtype);
    // float32 dtypes round the std to f32 before dividing by sqrt(n).
    const roundedStd = this.maybeF32(std) as number;
    return this.maybeF32(roundedStd / Math.sqrt(numbers.length));
  }

  /** Lag-N autocorrelation (default lag 1). */
  autocorr(lag = 1): number | null {
    return autocorrelation(this._values, lag);
  }

  /** True for values within [lower, upper] inclusive. */
  between(lower: number | bigint, upper: number | bigint, inclusive: "both" | "neither" | "left" | "right" = "both"): Series<boolean> {
    const out = this._values.map((value): boolean | null => {
      if (isMissing(value) || (typeof value !== "number" && typeof value !== "bigint")) {
        return false;
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
    return new Series(out as never, { index: this._index, indexName: this._indexName, name: this.name });
  }

  /** First element as a scalar (pandas item / iat shortcut). */
  item(): T | undefined {
    return this._values[0];
  }

  /** Wraps the series in a single-column DataFrame. */
  to_frame(name?: string): DataFrame {
    return new DataFrame(
      this._values.map((value) => ({ [name ?? this.name ?? "0"]: value })),
      {
        columns: [name ?? this.name ?? "0"],
        index: [...this._index],
        dtype: this._catDtype ?? this._dtype,
        indexName: this._indexName,
        indexNames: this._indexNames,
      }
    );
  }

  dropna(): Series<T> {
    return this.filter((entry) => !isMissing(entry));
  }

  sum(): CellValue {
    if (this._catDtype !== undefined) this.catUnsupported("sum");
    return this.maybeF32(pandasSum(this._values as CellValue[], this._dtype));
  }

  mean(): CellValue {
    if (this._catDtype !== undefined) this.catUnsupported("mean");
    const numbers = requireNumeric(this._values as CellValue[], this._dtype, "mean");
    if (numbers.length === 0) {
      return missingScalar(this._dtype);
    }
    return this.maybeF32(numbers.reduce((acc, value) => acc + value, 0) / numbers.length);
  }

  /**
   * pandas Categorical min/max/idxmin/idxmax/argmin/argmax: ordering ops on an
   * unordered categorical raise TypeError; ordered categoricals resolve the
   * extreme by category code (not cell value). Returns undefined when empty.
   */
  private catExtreme(mode: "min" | "max"): { value: CellValue; position: number } | undefined {
    const cat = this._catDtype!;
    if (!cat.ordered) {
      throw new TypeError(
        `Categorical is not ordered for operation ${mode}\nyou can use .as_ordered() to change the Categorical to an ordered one`
      );
    }
    const index = categoryIndex(cat);
    let best = -1;
    let bestPos = -1;
    this._values.forEach((value, i) => {
      if (isMissing(value)) return;
      const code = index.get(scalarKey(value)) ?? -1;
      if (code < 0) return;
      if (bestPos < 0 || (mode === "min" ? code < best : code > best)) {
        best = code;
        bestPos = i;
      }
    });
    return bestPos < 0 ? undefined : { value: this._values[bestPos]!, position: bestPos };
  }

  /** pandas: numeric reductions on categorical raise TypeError naming the op. */
  private catUnsupported(op: string): void {
    throw new TypeError(`'Categorical' with dtype category does not support operation '${op}'`);
  }

  /** pandas Categorical quantile: interpolates category CODES and returns a float. */
  private catQuantile(q: number): CellValue {
    const cat = this._catDtype!;
    if (!cat.ordered) {
      throw new TypeError(
        `Categorical is not ordered for operation quantile\nyou can use .as_ordered() to change the Categorical to an ordered one`
      );
    }
    const index = categoryIndex(cat);
    const codes = this._values
      .map((value) => (isMissing(value) ? -1 : (index.get(scalarKey(value)) ?? -1)))
      .filter((code) => code >= 0)
      .sort((a, b) => a - b);
    if (codes.length === 0) return NaN;
    const pos = (codes.length - 1) * q;
    const lower = Math.floor(pos);
    const upper = Math.ceil(pos);
    return codes[lower]! + (codes[upper]! - codes[lower]!) * (pos - lower);
  }

  /** pandas ordered-categorical cummax/cummin: running extreme of category codes. */
  private catCumExtreme(mode: "cummax" | "cummin"): Series<CellValue> {
    const cat = this._catDtype!;
    if (!cat.ordered) {
      throw new TypeError(
        `Categorical is not ordered for operation ${mode}\nyou can use .as_ordered() to change the Categorical to an ordered one`
      );
    }
    const index = categoryIndex(cat);
    let best = -1;
    const out = this._values.map((value): CellValue => {
      if (isMissing(value)) return NaN;
      const code = index.get(scalarKey(value)) ?? -1;
      if (code >= 0) {
        best = best < 0 ? code : (mode === "cummax" ? Math.max(best, code) : Math.min(best, code));
      }
      return best < 0 ? NaN : (cat.categories[best] as CellValue);
    });
    return new Series(out, {
      index: this._index,
      indexName: this._indexName,
      name: this.name,
      dtype: cat,
    });
  }

  min(): CellValue {
    if (this._catDtype !== undefined) return this.catExtreme("min")?.value ?? NaN;
    return this.maybeF32(pandasMinMax(this._values as CellValue[], this._dtype, "min"));
  }

  max(): CellValue {
    if (this._catDtype !== undefined) return this.catExtreme("max")?.value ?? NaN;
    return this.maybeF32(pandasMinMax(this._values as CellValue[], this._dtype, "max"));
  }

  unique(): T[] {
    const seen = new Set<string>();
    const out: T[] = [];
    // pandas np.asarray() on a nullable numeric/boolean EA converts to
    // float64/object, so unique() surfaces NaN there while string/boolean
    // and object keep their stored sentinel (NA / None).
    const numericMissing = /^[UIF]/.test(this._dtype);
    for (const value of this._values) {
      const key = this.valueKey(value);
      if (!seen.has(key)) {
        seen.add(key);
        out.push((numericMissing && isMissing(value) ? NaN : value) as T);
      }
    }
    return out;
  }

  value_counts(
    options: boolean | { normalize?: boolean; sort?: boolean; ascending?: boolean; dropna?: boolean } = true
  ): Series<CellValue> {
    const opts = typeof options === "boolean" ? { dropna: options } : options;
    const dropna = opts.dropna ?? true;
    const normalize = opts.normalize ?? false;
    const sort = opts.sort ?? true;
    const ascending = opts.ascending ?? false;
    if (this._catDtype !== undefined) {
      // pandas categorical value_counts lists every declared category
      // (unobserved included with zero counts) in category order; sorting
      // reorders by count with a stable tie-break on that order.
      const counts = new Map<string, number>();
      let missingCount = 0;
      for (const value of this._values) {
        if (isMissing(value)) { missingCount += 1; continue; }
        const key = scalarKey(value);
        counts.set(key, (counts.get(key) ?? 0) + 1);
      }
      const ordered = this._catDtype.categories.map(category => ({
        value: category,
        count: counts.get(scalarKey(category)) ?? 0,
      }));
      if (!dropna && missingCount > 0) ordered.push({ value: NaN, count: missingCount });
      if (sort) ordered.sort((a, b) => ascending ? a.count - b.count : b.count - a.count);
      const total = ordered.reduce((acc, entry) => acc + entry.count, 0);
      const values = normalize
        ? ordered.map(entry => entry.count / total)
        : ordered.map(entry => entry.count);
      return new Series(values as CellValue[], {
        index: ordered.map(entry => entry.value as IndexLabel),
        name: normalize ? "proportion" : "count",
        indexName: this.name,
        dtype: normalize ? "float64" : "int64",
      });
    }
    const masked = isMaskedDType(this._dtype);

    const counts = new Map<string, { value: CellValue; count: number; first: number }>();
    let missingCount = 0;
    let missingFirst = -1;
    this._values.forEach((value, i) => {
      if (isMissing(value)) {
        if (missingCount === 0) missingFirst = i;
        missingCount += 1;
        return;
      }
      const key = this.valueKey(value);
      const entry = counts.get(key);
      if (!entry) counts.set(key, { value, count: 1, first: i });
      else entry.count += 1;
    });

    // pandas orders entries by first occurrence; for masked dtypes the NA
    // bucket always trails the non-NA buckets.
    const entries = [...counts.values()];
    // pandas uses the dtype-appropriate missing sentinel in the index:
    // pd.NA for masked dtypes, None for object, NaN otherwise.
    const missingValue: CellValue = (masked ? NA : this._dtype === "object" ? null : NaN) as CellValue;
    const missingEntry: { value: CellValue; count: number; first: number } | null = !dropna && missingCount > 0
      ? { value: missingValue, count: missingCount, first: missingFirst }
      : null;
    const ordered: { value: CellValue; count: number; first: number }[] = [];
    // Nullable Int/Float/boolean dtypes append the NA bucket last; all other
    // dtypes (numpy, string, object) keep it at its first-occurrence slot.
    if (missingEntry && (!masked || this._dtype === "string")) {
      // the missing bucket occupies its first-occurrence slot
      let inserted = false;
      for (const entry of entries) {
        if (!inserted && missingEntry.first < entry.first) { ordered.push(missingEntry); inserted = true; }
        ordered.push(entry);
      }
      if (!inserted) ordered.push(missingEntry);
    } else {
      ordered.push(...entries);
      if (missingEntry) ordered.push(missingEntry);
    }
    if (sort) {
      ordered.sort((a, b) => ascending ? a.count - b.count : b.count - a.count);
    }

    const total = ordered.reduce((acc, entry) => acc + entry.count, 0);
    const values = normalize
      ? ordered.map((entry) => entry.count / total)
      : ordered.map((entry) => entry.count);
    const dtype = normalize
      ? (masked ? "Float64" : "float64")
      : (masked ? "Int64" : "int64");
    return new Series(values as CellValue[], {
      index: ordered.map((entry) => entry.value as IndexLabel),
      name: normalize ? "proportion" : "count",
      indexName: this.name,
      dtype,
    });
  }

  astype(dtype: SeriesDType): Series<CellValue> {
    if (dtype instanceof CategoricalDtype || isCategoricalDType(dtype as DType)) {
      // pandas astype('category') keeps an existing categorical's categories;
      // otherwise categories are inferred as the sorted unique observed values.
      const catDtype = dtype instanceof CategoricalDtype ? dtype
        : (this._catDtype ?? inferCategoricalDtype(this._values as CellValue[]));
      return new Series(this._values as CellValue[], {
        index: this._index, indexName: this._indexName, name: this.name, dtype: catDtype,
      });
    }
    const values = castColumn(this._values, dtype, this.dtype);
    return new Series(values, { index: this._index, indexName: this._indexName, name: this.name, dtype });
  }

  isin(values: CellValue[]): Series<boolean> {
    // pandas returns a plain bool dtype for string/str inputs (StringDtype's
    // isin lands on a numpy bool array) and masked boolean only for the
    // nullable numeric/boolean dtypes.
    const masked = isMaskedDType(this._dtype) && this._dtype !== "string";
    return new Series(computeSeriesIsin(this._values, values), {
      index: this._index, indexName: this._indexName,
      name: this.name,
      dtype: masked ? "boolean" : "bool",
    });
  }

  clip(lower?: number | null, upper?: number | null): Series<CellValue> {
    // pandas swaps scalar lower>upper bounds (GH 2747) inside pandasClip.
    const { values, dtype } = pandasClip(this._values as CellValue[], this._dtype, lower, upper);
    return new Series(values, { index: this._index, indexName: this._indexName, name: this.name, dtype });
  }

  replace(toReplace: SeriesReplaceInput, value?: CellValue): Series<CellValue> {
    return new Series(computeSeriesReplace(this._values, toReplace, value), {
      index: this._index, indexName: this._indexName,
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

  /**
   * Align two operand vectors for a binary op. Series operands use pandas
   * union alignment on index labels (raising on duplicate labels via
   * reindexPositions); scalars broadcast over the existing index.
   */
  private alignBinary(other: CellValue | Series<T>): {
    leftValues: readonly (CellValue | null)[];
    rightValues: readonly (CellValue | null)[];
    resultIndex: IndexLabel[];
    missingIntroduced: boolean;
  } {
    if (!(other instanceof Series) || indexLabelsEqual(this._index, other._index)) {
      return {
        leftValues: this._values,
        rightValues: other instanceof Series ? other._values : new Array(this.length).fill(other),
        resultIndex: this._index,
        missingIntroduced: false,
      };
    }
    const { index, leftPos, rightPos } = alignedPositions(this._index, other._index, compareIndexLabels);
    return {
      leftValues: index.map((_, i) => leftPos[i]! < 0 ? null : this._values[leftPos[i]!]),
      rightValues: index.map((_, i) => rightPos[i]! < 0 ? null : other._values[rightPos[i]!]),
      resultIndex: index,
      missingIntroduced: leftPos.some(p => p < 0) || rightPos.some(p => p < 0),
    };
  }

  /** Applies a numeric op; missing values stay missing. Series operands align on index labels like pandas. */
  private numericOp(
    other: number | bigint | Series<T>,
    fn: (left: number, right: number) => number,
    name?: string,
    division = false,
    boolArithmetic = false,
    floordiv = false,
    narrowBool = false,
    zeroDivisorError = false,
    bigintFn?: (left: bigint, right: bigint) => bigint | null
  ): Series<number | bigint> {
    const rightDtype0: DType = other instanceof Series ? other._dtype : "number";
    if (/^(str|string)$/.test(this._dtype) || (other instanceof Series && /^(str|string)$/.test(rightDtype0))) {
      throw new TypeError("unsupported operand type(s) for Series arithmetic");
    }
    const { leftValues, rightValues, resultIndex, missingIntroduced } = this.alignBinary(other);
    // pandas object columns divide element-wise and raise ZeroDivisionError.
    if (zeroDivisorError && this._dtype === "object" &&
        rightValues.some((v, i) => v === 0 && !isMissing(leftValues[i]))) {
      throw new ZeroDivisionError("integer division or modulo by zero");
    }
    // Integer-preserving ops on exact bigint cells run in bigint; a null
    // result or non-integer operand falls back to the number kernel.
    const out = leftValues.map((value, i): number | bigint | null => {
      const right = rightValues[i];
      if (isMissing(value) || isMissing(right)) {
        return null;
      }
      // The bigint kernel only engages when an operand is genuinely bigint;
      // integer number columns keep plain-number results.
      if (bigintFn && (typeof value === "bigint" || typeof right === "bigint")) {
        const lw = bigintOperand(value);
        const rw = bigintOperand(right);
        if (lw !== null && rw !== null) {
          const wide = bigintFn(lw, rw);
          if (wide !== null) return wide;
        }
      }
      const left = typeof value === "number" ? value : Number(value);
      const r = typeof right === "number" ? right : Number(right);
      return fn(left, r);
    });
    const rightDtype: DType = other instanceof Series
      ? other._dtype
      : scalarOperandDType(this._dtype, other, narrowBool);
    let dtype = floordiv
      ? floordivResultDType(this._dtype, rightDtype)
      : arithResultDType(this._dtype, rightDtype, division, boolArithmetic);
    if (missingIntroduced && dtype !== undefined && /^(u?int|bool|boolean)/.test(dtype)) {
      dtype = "float64";
    }
    // mod/floordiv by zero and negative-exponent pow yield non-integer results;
    // pandas widens int dtypes to float64/Float64 when they appear.
    if (dtype !== undefined && /^(u?int|U?Int)/.test(dtype) &&
        out.some(value => typeof value === "number" && !Number.isInteger(value))) {
      dtype = /^U?Int/.test(dtype) ? "Float64" : "float64";
    }
    // pandas floordiv on a float-family left computes through float64 and
    // widens when non-integral results appear (Float32 // int -> Float64).
    if (floordiv && /^[fF]loat|^number$/.test(this._dtype) &&
        out.some(value => typeof value === "number" && !Number.isInteger(value))) {
      dtype = /^Float/.test(this._dtype) ? "Float64" : "float64";
    }
    // pandas Float32/float32 columns compute in float32 precision.
    const values = (dtype === "Float32" || dtype === "float32")
      ? out.map(v => typeof v === "number" && Number.isFinite(v) ? Math.fround(v) : v)
      : dtype === "object" ? out.map(v => v === null ? NaN : v) : out;
    return new Series(values as never, {
      index: resultIndex, indexName: this._indexName,
      name: name ?? this.name,
      ...(dtype ? { dtype } : {}),
    });
  }

  add(other: number | bigint | Period | Series<T>): Series<number | bigint | Period> {
    // pandas `period series + int` shifts each cell by whole freq units.
    if (isPeriodDType(this._dtype)) return this.periodAddSub(other, "add") as Series<number | bigint | Period>;
    return this.numericOp(other as number | bigint | Series<T>, (a, b) => a + b, undefined, false, true, false, false, false, (a, b) => a + b) as Series<number | bigint | Period>;
  }

  sub(other: number | bigint | Period | Series<T>): Series<number | bigint | Period> {
    if (isPeriodDType(this._dtype)) return this.periodAddSub(other, "sub") as Series<number | bigint | Period>;
    if (this._dtype === "bool" && other instanceof Series && other._dtype === "bool") {
      throw new TypeError("numpy boolean subtract, the `-` operator, is not supported, use the bitwise_xor, the `^` operator, or the logical_xor function instead.");
    }
    return this.numericOp(other as number | bigint | Series<T>, (a, b) => a - b, undefined, false, false, false, false, false, (a, b) => a - b) as Series<number | bigint | Period>;
  }

  /**
   * pandas period arithmetic: `s ± int` shifts every cell by whole freq
   * units (NaT propagates, `period[<f>]` dtype kept); `s - period operand`
   * yields the ordinal difference in freq steps (pandas returns an offset
   * object — the integer step count is the honest scalar form here).
   */
  private periodAddSub(other: number | bigint | Period | Series<T>, op: "add" | "sub"): Series<CellValue> {
    const sign = op === "add" ? 1 : -1;
    // period - period → integer ordinal differences.
    if (op === "sub" && (other instanceof Period || (other instanceof Series && isPeriodDType(other._dtype)))) {
      const { leftValues, rightValues, resultIndex } = this.alignBinary(other as CellValue | Series<T>);
      const out = leftValues.map((value, i): number | typeof NaT => {
        const right = rightValues[i];
        if (isMissing(value) || isMissing(right)) return NaN;
        const l = value as Period; const r = right as Period;
        if (r.freq !== l.freq) {
          throw new TypeError(`Input has different freq=${r.freqstr} from Period(freq=${l.freqstr})`);
        }
        return l.ordinal - r.ordinal;
      });
      return new Series(out as CellValue[], {
        index: resultIndex, indexName: this._indexName, name: this.name,
      });
    }
    if (other instanceof Period) {
      throw new TypeError(`unsupported operand type(s) for +: 'Period' and 'Period'`);
    }
    const intOperand = (v: CellValue | null): number | null => {
      if (typeof v === "number" && Number.isInteger(v)) return v;
      if (typeof v === "bigint") return Number(v);
      return null;
    };
    if (other instanceof Series) {
      const { leftValues, rightValues, resultIndex } = this.alignBinary(other);
      const out = leftValues.map((value, i): CellValue => {
        const right = rightValues[i];
        if (isMissing(value) || isMissing(right)) return NaT;
        const n = intOperand(right as CellValue | null);
        if (n === null) throw new TypeError(`unsupported operand type(s) for ${op === "add" ? "+" : "-"}: 'Period' and '${pythonOperandTypeName(right as CellValue)}'`);
        return new Period((value as Period).ordinal + sign * n, (value as Period).freq);
      });
      return new Series(out, {
        index: resultIndex, indexName: this._indexName, name: this.name, dtype: this._dtype,
      });
    }
    const n = intOperand(other as CellValue | null);
    if (n === null) {
      throw new TypeError(`unsupported operand type(s) for ${op === "add" ? "+" : "-"}: 'Period' and '${pythonOperandTypeName(other as CellValue)}'`);
    }
    const out = this._values.map((value): CellValue =>
      isMissing(value) ? NaT : new Period((value as Period).ordinal + sign * n, (value as Period).freq));
    return new Series(out, {
      index: [...this._index], indexName: this._indexName, name: this.name, dtype: this._dtype,
    });
  }

  rsub(other: number | bigint | Series<T>): Series<number | bigint> {
    return this.numericOp(other, (a, b) => b - a, undefined, false, false, false, false, false, (a, b) => b - a);
  }

  mul(other: number | bigint | Series<T>): Series<number | bigint> {
    // pandas repeats strings by integer operands: 'a' * 2 -> 'aa'.
    if (/^(str|string)$/.test(this._dtype)) {
      const { leftValues, rightValues, resultIndex } = this.alignBinary(other);
      const out = leftValues.map((value, i): string | null => {
        const right = rightValues[i];
        if (isMissing(value) || isMissing(right)) return null;
        const count = typeof right === "number" ? right : Number(right);
        return count > 0 ? String(value).repeat(count) : "";
      });
      return new Series(out as never, { index: resultIndex, indexName: this._indexName, name: this.name, dtype: "string" });
    }
    return this.numericOp(other, (a, b) => a * b, undefined, false, true, false, false, false, (a, b) => a * b);
  }

  div(other: number | bigint | Series<T>): Series<number | bigint> {
    return this.numericOp(other, (a, b) => a / b, undefined, true, false, false, false, true);
  }

  mod(other: number | bigint | Series<T>): Series<number | bigint> {
    // pandas integer mod emits 0 on a zero divisor, keeping the int dtype.
    const intOperands = /^(u?int|U?Int|bool|boolean)/.test(this._dtype) &&
      (!(other instanceof Series) || /^(u?int|U?Int|bool|boolean)/.test(other._dtype)) &&
      !(/^(u?int|^bool)/.test(this._dtype) && other instanceof Series &&
        /^(U?Int|boolean)/.test(other._dtype));
    return this.numericOp(other, intOperands
      ? (a, b) => (b === 0 ? 0 : a % b)
      : (a, b) => a % b, undefined, false, false, false, false, true, intOperands
      ? (a, b) => (b === 0n ? 0n : bigintMod(a, b))
      : (a, b) => (b === 0n ? null : bigintMod(a, b)));
  }

  pow(other: number | bigint | Series<T>): Series<number | bigint> {
    return this.numericOp(other, (a, b) => a ** b, undefined, false, false, false, true, false,
      // negative exponents widen to float64 through the number kernel.
      (a, b) => (b < 0n ? null : a ** b));
  }

  // pandas long-form aliases
  subtract(other: number | bigint | Period | Series<T>): Series<number | bigint | Period> {
    return this.sub(other);
  }
  multiply(other: number | bigint | Series<T>): Series<number | bigint> {
    return this.mul(other);
  }
  truediv(other: number | bigint | Series<T>): Series<number | bigint> {
    return this.div(other);
  }
  floordiv(other: number | bigint | Series<T>): Series<number | bigint> {
    // pandas keeps integer floordiv integer, emitting 0 on a zero divisor.
    const intOperands = /^(u?int|U?Int|bool|boolean)/.test(this._dtype) &&
      (!(other instanceof Series) || /^(u?int|U?Int|bool|boolean)/.test(other._dtype)) &&
      !(/^(u?int|^bool)/.test(this._dtype) && other instanceof Series &&
        /^(U?Int|boolean)/.test(other._dtype));
    return this.numericOp(other, intOperands
      ? (a, b) => (b === 0 ? 0 : Math.floor(a / b))
      : (a, b) => Math.floor(a / b), undefined, false, false, true, false, true, intOperands
      ? (a, b) => (b === 0n ? 0n : bigintFloorDiv(a, b))
      : (a, b) => (b === 0n ? null : bigintFloorDiv(a, b)));
  }
  radd(other: number | bigint | Period | Series<T>): Series<number | bigint | Period> {
    return this.add(other);
  }
  rmul(other: number | bigint | Series<T>): Series<number | bigint> {
    return this.mul(other);
  }
  rdiv(other: number | bigint | Series<T>): Series<number | bigint> {
    return this.numericOp(other, (a, b) => b / a, undefined, true, false, false, false, true);
  }
  rtruediv(other: number | bigint | Series<T>): Series<number | bigint> {
    return this.rdiv(other);
  }

  rmod(other: number | bigint | Series<T>): Series<number | bigint> {
    const intOperands = /^(u?int|U?Int|bool|boolean)/.test(this._dtype) &&
      (!(other instanceof Series) || /^(u?int|U?Int|bool|boolean)/.test(other._dtype)) &&
      !(/^(u?int|^bool)/.test(this._dtype) && other instanceof Series &&
        /^(U?Int|boolean)/.test(other._dtype));
    return this.numericOp(other, intOperands
      ? (a, b) => (a === 0 ? 0 : b % a)
      : (a, b) => b % a, undefined, false, false, false, false, true, intOperands
      ? (a, b) => (a === 0n ? 0n : bigintMod(b, a))
      : (a, b) => (a === 0n ? null : bigintMod(b, a)));
  }

  rpow(other: number | bigint | Series<T>): Series<number | bigint> {
    return this.numericOp(other, (a, b) => b ** a, undefined, false, false, false, true, false,
      (a, b) => (a < 0n ? null : b ** a));
  }

  rfloordiv(other: number | bigint | Series<T>): Series<number | bigint> {
    const intOperands = /^(u?int|U?Int|bool|boolean)/.test(this._dtype) &&
      (!(other instanceof Series) || /^(u?int|U?Int|bool|boolean)/.test(other._dtype)) &&
      !(/^(u?int|^bool)/.test(this._dtype) && other instanceof Series &&
        /^(U?Int|boolean)/.test(other._dtype));
    return this.numericOp(other, intOperands
      ? (a, b) => (a === 0 ? 0 : Math.floor(b / a))
      : (a, b) => Math.floor(b / a), undefined, false, false, true, false, true, intOperands
      ? (a, b) => (a === 0n ? 0n : bigintFloorDiv(b, a))
      : (a, b) => (a === 0n ? null : bigintFloorDiv(b, a)));
  }

  neg(): Series<number | bigint> {
    const out = this._values.map((value): number | bigint | null => {
      if (isMissing(value)) {
        return null;
      }
      return typeof value === "bigint" ? -value : -(typeof value === "number" ? value : Number(value));
    });
    return new Series(out as never, { index: this._index, indexName: this._indexName, name: this.name });
  }

  abs(): Series<CellValue> {
    if (this._catDtype !== undefined) {
      throw new TypeError("Object with dtype category cannot perform the numpy op absolute");
    }
    const { values, dtype } = pandasAbs(this._values as CellValue[], this._dtype);
    return new Series(values, { index: this._index, indexName: this._indexName, name: this.name, dtype });
  }

  round(decimals = 0): Series<CellValue> {
    if (!Number.isInteger(decimals)) throw new TypeError("decimals must be an integer");
    if (this._dtype === "object") {
      const values = this._values.map(value => {
        if (typeof value !== "number" && typeof value !== "boolean") throw new TypeError(`Cannot round ${String(value)}`);
        return roundToEven(Number(value), decimals);
      });
      return new Series<CellValue>(values, { index: this._index, indexName: this._indexName, name: this.name, dtype: "object" });
    }
    if (!/^(u?int|float|number)/i.test(this._dtype ?? "")) return new Series<CellValue>(this._values, { index: this._index, indexName: this._indexName, name: this.name, dtype: this._catDtype ?? this._dtype });
    const out: CellValue[] = this._values.map((value) => {
      if (isMissing(value)) {
        return null;
      }
      // integers are unchanged by non-negative decimal rounding; keep bigint exact.
      if (typeof value === "bigint" && decimals >= 0) {
        return value;
      }
      const n = typeof value === "number" ? value : Number(value as CellValue);
      return roundToEven(n, decimals);
    });
    return new Series(out, { index: this._index, indexName: this._indexName, name: this.name, dtype: this.declaredDType(replacementDType(this._dtype, out)) });
  }

  // ---- comparisons (return boolean Series) ----

  private compareOp(
    other: CellValue | Series<T>,
    fn: (left: number | string | boolean | bigint, right: number | string | boolean | bigint) => boolean,
    ordering = false
  ): Series<boolean> {
    const isStrDtype = (d: string) => /^(str|string)$/.test(d);
    const leftStr = isStrDtype(this._dtype);
    const rightDtype = other instanceof Series ? other._dtype : null;
    const rightStr = rightDtype !== null ? isStrDtype(rightDtype) : typeof other === "string";
    // pandas ordering comparisons between string and non-string dtypes raise.
    if (ordering && leftStr !== rightStr) {
      throw new TypeError("unsupported operand type(s) for Series comparison");
    }
    const { leftValues, rightValues, resultIndex } = this.alignBinary(other);
    // pandas comparisons with missing operands use IEEE NaN semantics (False)
    // for plain dtypes and NA propagation for nullable dtypes; string
    // comparisons produce the nullable boolean dtype.
    const leftNullable = /^(U?Int|Float)|^boolean/.test(this._dtype);
    const rightNullable = other instanceof Series && /^(U?Int|Float)|^boolean/.test(other._dtype);
    const nullableCompare = leftNullable || rightNullable || leftStr || rightStr;
    const out = leftValues.map((value, i): boolean | null => {
      const right = rightValues[i];
      const aMiss = isMissing(value);
      const bMiss = isMissing(right);
      if ((aMiss || bMiss) && nullableCompare) {
        return null;
      }
      return fn((aMiss ? NaN : value) as never, (bMiss ? NaN : right) as never);
    });
    // pandas nullable comparisons always produce the nullable boolean dtype.
    return new Series(out as never, {
      index: resultIndex,
      indexName: this._indexName,
      name: this.name,
      dtype: nullableCompare ? "boolean" : "bool",
    });
  }

  /**
   * pandas Categorical comparison: equality compares decoded values (a scalar
   * outside the categories is simply never equal); ordering comparisons run
   * on category codes, require `ordered=True`, and reject non-categorical
   * Series operands. Missing cells yield False for eq/ordering, True for ne.
   */
  private categoricalCompare(
    other: CellValue | Series<T>,
    op: "eq" | "ne" | "lt" | "le" | "gt" | "ge"
  ): Series<boolean> {
    const cat = this._catDtype!;
    const equality = op === "eq" || op === "ne";
    if (!equality && !cat.ordered) {
      throw new TypeError("Unordered Categoricals can only compare equality or not");
    }
    const otherCat = other instanceof Series ? other._catDtype : undefined;
    if (other instanceof Series && otherCat !== undefined) {
      // pandas: when either side is ordered the dtypes must be identical
      // (same categories in the same order); between two unordered
      // categoricals the same category SET suffices for ==/!=.
      const compatible = cat.ordered || otherCat.ordered
        ? cat.equals(otherCat)
        : sameCategorySet(cat, otherCat);
      if (!compatible) {
        throw new TypeError("Categoricals can only be compared if 'categories' are the same.");
      }
    }
    if (!equality) {
      if (other instanceof Series && otherCat === undefined) {
        throw new TypeError(`Invalid comparison between dtype=category and ${other._dtype}`);
      }
      if (!(other instanceof Series) && (isMissing(other) || (categoryIndex(cat).get(scalarKey(other)) ?? -1) < 0)) {
        throw new TypeError(`Invalid comparison between dtype=category and ${pythonOperandTypeName(other)}`);
      }
    }
    const { leftValues, rightValues, resultIndex } = this.alignBinary(other);
    const index = categoryIndex(cat);
    const out = leftValues.map((value, i): boolean => {
      const right = rightValues[i]!;
      const aMiss = isMissing(value);
      const bMiss = isMissing(right);
      if (aMiss || bMiss) {
        // pandas: NaN == x -> False, NaN != x -> True, ordered ops -> False.
        return equality ? op === "ne" : false;
      }
      if (equality) {
        const equal = scalarEquals(value, right);
        return op === "eq" ? equal : !equal;
      }
      const leftCode = index.get(scalarKey(value)) ?? -1;
      const rightCode = index.get(scalarKey(right)) ?? -1;
      if (leftCode < 0 || rightCode < 0) return false;
      switch (op) {
        case "lt": return leftCode < rightCode;
        case "le": return leftCode <= rightCode;
        case "gt": return leftCode > rightCode;
        default: return leftCode >= rightCode;
      }
    });
    return new Series(out, { index: resultIndex, indexName: this._indexName, name: this.name, dtype: "bool" });
  }

  /** True when the comparison should run through the Period kernel. */
  private isPeriodCompare(other: CellValue | Series<T>): boolean {
    return isPeriodDType(this._dtype) || other instanceof Period ||
      (other instanceof Series && isPeriodDType(other._dtype));
  }

  /**
   * pandas period comparisons: eq/ne compare (freq, ordinal) identity
   * (cross-freq is simply unequal); ordering compares ordinals and raises
   * pandas `IncompatibleFrequency` on a freq mismatch. NaT operands follow
   * pandas NaT semantics: False for eq/ordering, True for ne.
   */
  private periodCompare(
    other: CellValue | Series<T>,
    op: "eq" | "ne" | "lt" | "le" | "gt" | "ge"
  ): Series<boolean> {
    const { leftValues, rightValues, resultIndex } = this.alignBinary(other);
    const out = leftValues.map((value, i): boolean => {
      const right = rightValues[i];
      const aMiss = isMissing(value);
      const bMiss = isMissing(right);
      if (aMiss || bMiss) return op === "ne";
      if (!(value instanceof Period) || !(right instanceof Period)) {
        // Period vs non-Period scalar: eq is False, ordering raises.
        if (op === "eq") return false;
        if (op === "ne") return true;
        throw new TypeError("unsupported operand type(s) for Series comparison");
      }
      if (op === "eq" || op === "ne") {
        const equal = value.equals(right);
        return op === "eq" ? equal : !equal;
      }
      const cmp = value.compareTo(right); // freq mismatch → ValueError
      switch (op) {
        case "lt": return cmp < 0;
        case "le": return cmp <= 0;
        case "gt": return cmp > 0;
        default: return cmp >= 0;
      }
    });
    return new Series(out, {
      index: resultIndex, indexName: this._indexName, name: this.name, dtype: "bool",
    });
  }

  eq(other: CellValue | Series<T>): Series<boolean> {
    if (this._catDtype !== undefined) return this.categoricalCompare(other, "eq");
    if (this.isPeriodCompare(other)) return this.periodCompare(other, "eq");
    // numberOrBigintEqual keeps bigint/number comparisons exact (=== is false
    // for mixed types even when the values are numerically equal).
    return this.compareOp(other, (a, b) => numberOrBigintEqual(a, b));
  }

  ne(other: CellValue | Series<T>): Series<boolean> {
    if (this._catDtype !== undefined) return this.categoricalCompare(other, "ne");
    if (this.isPeriodCompare(other)) return this.periodCompare(other, "ne");
    return this.compareOp(other, (a, b) => !numberOrBigintEqual(a, b));
  }

  lt(other: CellValue | Series<T>): Series<boolean> {
    if (this._catDtype !== undefined) return this.categoricalCompare(other, "lt");
    if (this.isPeriodCompare(other)) return this.periodCompare(other, "lt");
    return this.compareOp(other, (a, b) => (a as number) < (b as number), true);
  }

  le(other: CellValue | Series<T>): Series<boolean> {
    if (this._catDtype !== undefined) return this.categoricalCompare(other, "le");
    if (this.isPeriodCompare(other)) return this.periodCompare(other, "le");
    return this.compareOp(other, (a, b) => (a as number) <= (b as number), true);
  }

  gt(other: CellValue | Series<T>): Series<boolean> {
    if (this._catDtype !== undefined) return this.categoricalCompare(other, "gt");
    if (this.isPeriodCompare(other)) return this.periodCompare(other, "gt");
    return this.compareOp(other, (a, b) => (a as number) > (b as number), true);
  }

  ge(other: CellValue | Series<T>): Series<boolean> {
    if (this._catDtype !== undefined) return this.categoricalCompare(other, "ge");
    return this.compareOp(other, (a, b) => (a as number) >= (b as number), true);
  }

  // ---- cumulative ----

  cumsum(): Series<CellValue> {
    if (this._catDtype !== undefined) {
      throw new TypeError("Accumulation cumsum not supported for <class 'pandas.Categorical'>");
    }
    const { values, dtype } = pandasCumSumProd(this._values as CellValue[], this._dtype, false);
    return new Series(values, { index: this._index, indexName: this._indexName, name: this.name, dtype });
  }

  cummax(): Series<CellValue> {
    if (this._catDtype !== undefined) return this.catCumExtreme("cummax");
    const { values, dtype } = pandasCumMinMax(this._values as CellValue[], this._dtype, true);
    return new Series(values, { index: this._index, indexName: this._indexName, name: this.name, dtype });
  }

  cummin(): Series<CellValue> {
    if (this._catDtype !== undefined) return this.catCumExtreme("cummin");
    const { values, dtype } = pandasCumMinMax(this._values as CellValue[], this._dtype, false);
    return new Series(values, { index: this._index, indexName: this._indexName, name: this.name, dtype });
  }

  cumprod(): Series<CellValue> {
    if (this._catDtype !== undefined) {
      throw new TypeError("Accumulation cumprod not supported for <class 'pandas.Categorical'>");
    }
    const { values, dtype } = pandasCumSumProd(this._values as CellValue[], this._dtype, true);
    return new Series(values, { index: this._index, indexName: this._indexName, name: this.name, dtype });
  }

  /** 0-based position within the run of non-null values (pandas cumcount). */
  cumcount(): Series<number> {
    return new Series(cumcountValues(this._values) as unknown as number[], {
      index: this._index, indexName: this._indexName,
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

  equals<U extends CellValue>(other: Series<U>): boolean {
    const mine = this._dtype;
    const theirs = other._dtype;
    if (mine !== theirs) return false;
    // pandas requires identical categorical dtype (categories + orderedness).
    if (this._catDtype !== undefined || other._catDtype !== undefined) {
      if (this._catDtype === undefined || other._catDtype === undefined) return false;
      if (!this._catDtype.equals(other._catDtype)) return false;
    }
    return seriesEquals(this._values as CellValue[], this._index, other._values as CellValue[], other._index);
  }

  get(key: IndexLabel, defaultVal?: T): T | undefined {
    const pos = this._index.findIndex((entry) => indexLabelEquals(entry, key));
    if (pos < 0) return defaultVal as T | undefined;
    return this._values[pos];
  }

  all(skipna = true): CellValue {
    if (this._catDtype !== undefined) this.catUnsupported("all");
    return pandasAllAny(this._values as CellValue[], this._dtype, skipna, "all");
  }

  any(skipna = true): CellValue {
    if (this._catDtype !== undefined) this.catUnsupported("any");
    return pandasAllAny(this._values as CellValue[], this._dtype, skipna, "any");
  }

  argmax(): number {
    if (this._catDtype !== undefined) {
      const extreme = this.catExtreme("max");
      if (extreme === undefined) throw new ValueError("attempt to get argmax of an empty sequence");
      return extreme.position;
    }
    return pandasArgExtreme(this._values as CellValue[], "max");
  }

  argmin(): number {
    if (this._catDtype !== undefined) {
      const extreme = this.catExtreme("min");
      if (extreme === undefined) throw new ValueError("attempt to get argmin of an empty sequence");
      return extreme.position;
    }
    return pandasArgExtreme(this._values as CellValue[], "min");
  }

  copy(): Series<T> {
    return new Series([...this._values], { index: [...this._index], name: this.name, dtype: this._catDtype ?? this._dtype });
  }

  std(ddof = 1): CellValue {
    if (this._catDtype !== undefined) this.catUnsupported("std");
    const numbers = requireNumeric(this._values as CellValue[], this._dtype, "std");
    const result = seriesStd(numbers, ddof);
    return result === null || result === undefined ? missingScalar(this._dtype) : this.maybeF32(result);
  }

  describe(): Series<CellValue> {
    if (this._catDtype !== undefined) {
      // pandas categorical describe: count/unique/top/freq where top is the
      // most frequent *observed* category, tie-broken in category order.
      const counts = new Map<string, { value: CellValue; count: number }>();
      let observed = 0;
      for (const value of this._values) {
        if (isMissing(value)) continue;
        observed += 1;
        const key = scalarKey(value);
        const entry = counts.get(key);
        if (entry) entry.count += 1;
        else counts.set(key, { value, count: 1 });
      }
      const index = categoryIndex(this._catDtype);
      let top: CellValue = NaN;
      let freq = 0;
      for (const entry of counts.values()) {
        const code = index.get(scalarKey(entry.value)) ?? Number.MAX_SAFE_INTEGER;
        const topCode = isMissing(top) ? Number.MAX_SAFE_INTEGER : index.get(scalarKey(top)) ?? Number.MAX_SAFE_INTEGER;
        if (entry.count > freq || (entry.count === freq && code < topCode)) {
          top = entry.value;
          freq = entry.count;
        }
      }
      return new Series([observed, counts.size, top, freq], {
        index: ["count", "unique", "top", "freq"], name: this.name, dtype: "object",
      });
    }
    const result = seriesDescribe(this._values as CellValue[], this._dtype);
    return new Series(result.values, { index: result.keys, name: this.name, dtype: result.dtype as DType });
  }

  /**
   * Rolling window aggregations (pandas rolling): `rolling(3).mean()`.
   * Windows at the start shorter than `min_periods ?? window` are null.
   * Numeric aggregations raise the pandas `DataError` equivalent when a
   * present cell is not number-like; `count` tallies any non-missing value.
   */
  rolling(window: number, minPeriods?: number): RollingWindow {
    const rolling = computeRolling(this._values, window, minPeriods);
    return wrapWindowAggregations(rolling, this._values as CellValue[], this._dtype);
  }

  /** Expanding (cumulative) window aggregations. */
  expanding(minPeriods = 1): RollingWindow {
    const expanding = computeExpanding(this._values, minPeriods);
    return wrapWindowAggregations(expanding, this._values as CellValue[], this._dtype);
  }

  // ---- selection helpers ----

  private nlargestSmallest(n: number, largest: boolean): Series<T> {
    // pandas only supports nlargest/nsmallest on ordered dtypes; strings and
    // arbitrary objects raise TypeError.
    requireComparable(this._values as CellValue[], this._dtype, largest ? "nlargest" : "nsmallest");
    if (this._dtype === "string" || this._dtype === "str" || this._dtype === "object") {
      throw new TypeError(`Column '${this.name ?? ""}' has dtype ${this._dtype}, cannot use method '${largest ? "nlargest" : "nsmallest"}' with this dtype`);
    }
    // Missing values sort behind every present value and fill the tail when
    // fewer than n valid entries exist.
    const valid = this._values
      .map((value, i) => ({ value, index: this._index[i]! }))
      .filter((pair) => !isMissing(pair.value))
      .sort((a, b) => {
        const cmp = compareCellValues(a.value as CellValue, b.value as CellValue);
        return largest ? -cmp : cmp;
      });
    const missing = this._values
      .map((value, i) => ({ value, index: this._index[i]! }))
      .filter((pair) => isMissing(pair.value));
    const pairs = [...valid, ...missing].slice(0, Math.max(0, n));
    return new Series(pairs.map((p) => p.value), {
      index: pairs.map((p) => p.index),
      indexName: this._indexName,
      name: this.name,
      dtype: this._catDtype ?? this._dtype,
    });
  }

  nlargest(n = 5): Series<T> {
    return this.nlargestSmallest(n, true);
  }

  nsmallest(n = 5): Series<T> {
    return this.nlargestSmallest(n, false);
  }

  /**
   * pandas-style `.str` accessor for element-wise string operations.
   */
  get str(): StringMethods {
    return new StringMethods(this._values as CellValue[], this._dtype);
  }

  /**
   * pandas-style `.dt` accessor for element-wise datetime parts.
   * Accepts Date values and parseable date strings.
   */
  get dt(): DatetimeMethods {
    return new DatetimeMethods(this._values as CellValue[], this._dtype);
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
    return new Series(out, { index: this._index, indexName: this._indexName, name: this.name });
  }

  /**
   * Computes ranks alongside pandas: average method by default.
   * `method`: "average" | "min" | "max" | "first" | "dense".
   * Missing values retain a missing rank by default. Ascending=false reverses order.
   */
  rank(options: RankOptions = {}): Series<CellValue> {
    const method = options.method ?? "average";
    const naOption = options.na_option ?? "keep";
    if (!["average", "min", "max", "first", "dense"].includes(method)) {
      throw new KeyError(`'${method}'`);
    }
    if (!["keep", "top", "bottom"].includes(naOption)) {
      throw new ValueError("na_option must be one of 'keep', 'top', or 'bottom'");
    }
    const nullable = /^(U?Int|Float)/.test(this.dtype) || this.dtype === "boolean";
    const integral = !options.pct && (options.method ?? "average") !== "average";
    return new Series<CellValue>(computeRank(this._values, options), {
      index: this._index, indexName: this._indexName,
      name: this.name,
      dtype: nullable ? (integral ? "UInt64" : "Float64") : "float64",
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
  kurtosis(): CellValue {
    return this.kurt();
  }

  interpolate(method: "linear" = "linear"): Series<T> {
    if (method !== "linear") throw new Error(`interpolate: unsupported method '${method}'`);
    if (this._values.length === 0) return this.copy();
    const nums = this._values.map((v) => (typeof v === "number" && Number.isFinite(v) ? v : typeof v === "bigint" ? Number(v) : null));
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
  get dtype(): DType {
    return this._dtype;
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
      sem: () => this.sem(), prod: () => this.prod(), size: () => this.length,
      nunique: () => this.nunique(),
    };
    if (!(fn in map)) throw new Error(`agg: unknown aggregator '${fn}'`);
    return map[fn]!();
  }

  aggregate(fn: string | ((values: CellValue[]) => CellValue)): CellValue {
    return this.agg(fn);
  }

  nunique(dropna = true): number {
    const seen = new Set<string>();
    for (const value of this._values) {
      if (dropna && isMissing(value)) continue;
      seen.add(scalarKey(value as CellValue));
    }
    return seen.size;
  }

  drop(labels: IndexLabel | IndexLabel[]): Series<T> {
    const toRemove = new Set(Array.isArray(labels) ? labels : [labels]);
    // pandas raises KeyError when a requested label is absent from the axis.
    const missing = [...toRemove].filter((label) => !this._index.some((entry) => indexLabelEquals(entry, label)));
    if (missing.length > 0) {
      throw new KeyError(`${JSON.stringify(missing)} not found in axis`);
    }
    const values: T[] = [];
    const index: IndexLabel[] = [];
    for (let i = 0; i < this.length; i += 1) {
      if (toRemove.has(this._index[i]!)) continue;
      values.push(this._values[i]!);
      index.push(this._index[i]!);
    }
    return new Series(values, { index, indexName: this._indexName, name: this.name, dtype: this._catDtype ?? this._dtype });
  }

  sort_values(options: { ascending?: boolean; na_position?: "first" | "last" } | boolean = true): Series<T> {
    const ascending = typeof options === "boolean" ? options : (options.ascending ?? true);
    const naPosition = typeof options === "boolean" ? "last" : (options.na_position ?? "last");
    if (naPosition !== "first" && naPosition !== "last") {
      throw new Error("na_position must be 'first' or 'last'.");
    }
    this.requireSortable();
    // Categoricals sort by category code order (pandas), not by cell value.
    const catIndex = this._catDtype !== undefined ? categoryIndex(this._catDtype) : undefined;
    const indexed = this._values.map((value, position) => ({ value, position, label: this._index[position]! }));
    indexed.sort((a, b) => {
      const aMiss = isMissing(a.value);
      const bMiss = isMissing(b.value);
      if (aMiss && bMiss) return 0;
      if (aMiss) return naPosition === "last" ? 1 : -1;
      if (bMiss) return naPosition === "last" ? -1 : 1;
      const cmp = catIndex !== undefined
        ? (catIndex.get(scalarKey(a.value)) ?? -1) - (catIndex.get(scalarKey(b.value)) ?? -1)
        : compareCellValues(a.value as CellValue, b.value as CellValue);
      return ascending ? cmp : -cmp;
    });
    // pandas 3 re-infers a sorted all-string object column as str.
    const present = indexed.map((e) => e.value).filter((v) => !isMissing(v));
    const outDtype = this._dtype === "object" && present.length > 0 && present.every((v) => typeof v === "string")
      ? "str"
      : this._dtype;
    return new Series(indexed.map((e) => e.value), {
      index: indexed.map((e) => e.label),
      name: this.name, indexName: this._indexName, dtype: this.declaredDType(outDtype),
    });
  }

  /**
   * Sort by index label (pandas sort_index). On a MultiIndex the full tuple
   * sorts lexicographically; `options.level` restricts the sort key to the
   * given level position/name(s) first, then the remaining levels in order.
   */
  sort_index(
    ascendingOrOptions: boolean | { ascending?: boolean; level?: number | string | (number | string)[] } = true
  ): Series<T> {
    const ascending = typeof ascendingOrOptions === "boolean"
      ? ascendingOrOptions
      : (ascendingOrOptions.ascending ?? true);
    const levelOption = typeof ascendingOrOptions === "boolean" ? undefined : ascendingOrOptions.level;

    let keyOf: (label: IndexLabel) => IndexLabel = (label) => label;
    if (levelOption !== undefined) {
      const nlevels = this.nlevels;
      const names = this.index_names;
      const lead = (Array.isArray(levelOption) ? levelOption : [levelOption])
        .map((level) => resolveIndexLevel(level, nlevels, names));
      const rest = range(nlevels).filter((i) => !lead.includes(i));
      const order = [...lead, ...rest];
      keyOf = (label) => {
        const tuple = indexTuple(label);
        const projected = order.map((i) => tuple[i] ?? "");
        return projected.length === 1 ? projected[0]! : projected;
      };
    }

    const indexed = this._values.map((value, position) => ({ value, position, label: this._index[position]! }));
    indexed.sort((a, b) => {
      const cmp = compareIndexLabels(keyOf(a.label), keyOf(b.label));
      return ascending ? cmp : -cmp;
    });
    return new Series(indexed.map((e) => e.value), {
      index: indexed.map((e) => e.label),
      name: this.name, indexName: this._indexName, indexNames: this._indexNames, dtype: this._catDtype ?? this._dtype,
    });
  }

  where(cond: boolean[] | ((value: T, label: IndexLabel, position: number) => boolean), other?: T | null): Series<T> {
    const replacement = other === undefined ? (this._dtype ? NaN : null) : other;
    const decision = this.resolveWhereCond(cond);
    const out = this._values.map((value, i) => {
      const label = this._index[i]!;
      return decision(value, label, i) ? value : (replacement as T);
    });
    return this.withReplacement(out);
  }

  mask(cond: boolean[] | ((value: T, label: IndexLabel, position: number) => boolean), other?: T | null): Series<T> {
    if (Array.isArray(cond) && cond.length === 0 && this.length === 0) throw new TypeError("Cannot invert an empty untyped condition array");
    const replacement = other === undefined ? (this._dtype ? NaN : null) : other;
    const decision = this.resolveWhereCond(cond);
    const out = this._values.map((value, i) => {
      const label = this._index[i]!;
      return decision(value, label, i) ? (replacement as T) : value;
    });
    return this.withReplacement(out);
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
    return new Series(values as unknown as T[], { index, name: this.name, dtype: this._catDtype ?? this._dtype });
  }

  reindex(newIndex: IndexLabel[], fill_value?: T | null): Series<T> {
    const fill = fill_value === undefined ? (this._dtype ? NaN : null) : fill_value;
    const values = reindexPositions(this._index, newIndex).map(position => position < 0 ? fill as T : this._values[position]!);
    return this.withReplacement(values, newIndex);
  }

  shift(periods = 1): Series<T | null> {
    if (!Number.isInteger(periods)) throw new TypeError("slice indices must be integers or None or have an __index__ method");
    const out: (T | null)[] = new Array(this.length).fill(null);
    for (let i = 0; i < this.length; i += 1) {
      const src = i - periods;
      if (src >= 0 && src < this.length) out[i] = this._values[src]! as unknown as T | null;
      else out[i] = (this._dtype === "bool" ? NaN : null) as T | null;
    }
    return this.withReplacement(out as T[], this._index, periods !== 0 && this.length === 0 ? replacementDType(this._dtype, [NaN]) : this._dtype) as Series<T | null>;
  }

  diff(periods = 1): Series<CellValue> {
    if (this._catDtype !== undefined) {
      throw new TypeError("Categorical has no 'diff' method. Convert to a suitable dtype prior to calling 'diff'.");
    }
    if (!Number.isInteger(periods)) throw new ValueError("periods must be an integer");
    const { values, dtype } = pandasDiff(this._values as CellValue[], this._dtype, periods);
    return new Series(values, { index: [...this._index], indexName: this._indexName, name: this.name, dtype });
  }

  pct_change(periods = 1): Series<CellValue> {
    if (!Number.isInteger(periods)) throw new TypeError("slice indices must be integers or None or have an __index__ method");
    const { values, dtype } = pandasPctChange(this._values as CellValue[], this._dtype, periods);
    return new Series(values, { index: [...this._index], indexName: this._indexName, name: this.name, dtype });
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
    return new Series(values, { index, name: this.name, dtype: this._catDtype ?? this._dtype });
  }

  duplicated(keep: "first" | "last" | false = "first"): Series<boolean> {
    const keepFlags = this.duplicateKeepFlags(keep);
    return new Series(keepFlags.map((f) => !f) as unknown as boolean[], {
      index: [...this._index],
      indexName: this._indexName,
      name: this.name,
      dtype: "bool",
    });
  }

  private duplicateKeepFlags(keep: "first" | "last" | false): boolean[] {
    const include = new Array(this.length).fill(false);
    if (keep === false) {
      const counts = new Map<string, number>();
      for (const v of this._values) {
        const k = scalarKey(v as CellValue);
        counts.set(k, (counts.get(k) ?? 0) + 1);
      }
      for (let i = 0; i < this.length; i += 1) {
        const k = scalarKey(this._values[i] as CellValue);
        include[i] = (counts.get(k) ?? 0) === 1;
      }
    } else if (keep === "last") {
      const seen = new Set<string>();
      for (let i = this.length - 1; i >= 0; i -= 1) {
        const k = scalarKey(this._values[i] as CellValue);
        if (!seen.has(k)) { seen.add(k); include[i] = true; }
      }
    } else {
      const seen = new Set<string>();
      for (let i = 0; i < this.length; i += 1) {
        const k = scalarKey(this._values[i] as CellValue);
        if (!seen.has(k)) { seen.add(k); include[i] = true; }
      }
    }
    return include;
  }

  private valueKey(value: T): string {
    return scalarKey(value);
  }
  // ---- compat / window / export parity (delegates to internal/series/seriesApi) ----

  /** The dtype to hand a derived series: CategoricalDtype rides along when the result stays categorical. */
  private declaredDType(dtype: DType | undefined): DType | CategoricalDtype | undefined {
    return isCategoricalDType(dtype) ? (this._catDtype ?? dtype) : dtype;
  }

  private withReplacement(values: T[], index = this._index, sourceDType = this._dtype): Series<T> {
    const dtype = replacementDType(sourceDType, values);
    return new Series(values, { index, name: this.name, dtype: this.declaredDType(dtype) });
  }

  private view(): import("./internal/series/seriesApi").SeriesHost {
    return {
      valuesSnapshot: () => this._values as unknown as CellValue[],
      labelsSnapshot: () => this._index,
      lengthSnapshot: () => this.length,
      nameSnapshot: () => this.name,
      indexNameSnapshot: () => this._indexName,
      dtypeSnapshot: () => this._dtype,
      reindex: (labels) => this.reindex(labels) as never,
      copy: () => this.copy(),
      map: (fn) => this.map(fn as never),
      kurt: () => this.kurt(),
      to_dict: () => this.to_dict() as Record<string, CellValue>,
      to_frame: (name) => this.to_frame(name),
      iloc: (position) => this.iloc(position),
      loc: (label) => this.loc(label),
      hasnans: () => this.hasnans(),
      sum: () => this.sum(),
      mean: () => this.mean(),
      min: () => this.min(),
      max: () => this.max(),
      count: () => this.count(),
      std: (ddof) => this.std(ddof),
      var: () => this.var(),
      median: () => this.median(),
      skew: () => this.skew(),
      sem: () => this.sem(),
      prod: () => this.prod(),
      removeAt: (position) => {
        this._index.splice(position, 1);
        return this._values.splice(position, 1)[0] as CellValue;
      },
      setAt: (position, value) => {
        this._values[position] = value as T;
      },
    };
  }

  corr(other: Series<CellValue>): number | null { return seriesApi.corr(this.view(), other); }
  cov(other: Series<CellValue>): number | null { return seriesApi.cov(this.view(), other); }
  dot(other: Series<CellValue>): number | null { return seriesApi.dot(this.view(), other); }
  first_valid_index(): IndexLabel | null { return seriesApi.first_valid_index(this.view()); }
  last_valid_index(): IndexLabel | null { return seriesApi.last_valid_index(this.view()); }
  factorize(): [number[], CellValue[]] { return seriesApi.factorize(this.view()); }
  explode(): Series<CellValue> { return seriesApi.explode(this.view()); }
  /**
   * pandas `Series.groupby`: `by` names column(s) of the backing frame;
   * `options.level` groups by index level(s) instead, like
   * `s.groupby(level=0)`.
   */
  groupby(by?: string | string[] | null, options?: GroupByOptions): unknown {
    if (options?.level !== undefined) {
      return (this.to_frame() as unknown as { groupby: (b: null, o: unknown) => unknown })
        .groupby(null, options);
    }
    return seriesApi.groupby(this.view(), by ?? [], options);
  }
  to_numpy(): CellValue[] { return seriesApi.to_numpy(this.view()); }
  to_string(): string { return seriesApi.to_string(this.view()); }
  to_csv(): string { return seriesApi.to_csv(this.view()); }
  to_json(): string { return seriesApi.to_json(this.view()); }
  /**
   * pandas `Series.to_period(freq)`: converts period-capable index labels
   * (Dates → Periods, Periods → `asfreq(freq)`); `period[<f>]` values also
   * re-base to `freq`. For value conversion on datetime cells see
   * `s.dt.to_period(freq)`.
   */
  to_period(freq = "D"): Series<CellValue> { return seriesApi.to_period(this.view(), freq); }
  info(): string { return seriesApi.info(this.view()); }
  items(): [IndexLabel, CellValue][] { return seriesApi.items(this.view()); }
  keys(): IndexLabel[] { return seriesApi.keys(this.view()); }
  pop(label: IndexLabel): CellValue | undefined { return seriesApi.pop(this.view(), label); }
  repeat(repeats: number): Series<CellValue> { return seriesApi.repeat(this.view(), repeats) as Series<CellValue>; }
  rename(name: string): Series<CellValue> { return seriesApi.rename(this.view(), name) as Series<CellValue>; }
  /**
   * pandas `Series.rename_axis` renames the *index*. On a MultiIndex pass a
   * list with one name per level (or a single name for level 0).
   */
  rename_axis(name: string | (string | undefined)[]): Series<CellValue> {
    if (Array.isArray(name)) {
      return new Series([...this._values], {
        index: [...this._index],
        indexName: name[0],
        indexNames: [...name],
        name: this.name,
        dtype: this._catDtype ?? this._dtype,
      }) as unknown as Series<CellValue>;
    }
    if (this.nlevels > 1) {
      return new Series([...this._values], {
        index: [...this._index],
        indexName: name,
        indexNames: this.index_names.map((existing, i) => (i === 0 ? name : existing)),
        name: this.name,
        dtype: this._catDtype ?? this._dtype,
      }) as unknown as Series<CellValue>;
    }
    return seriesApi.rename_axis(this.view(), name) as Series<CellValue>;
  }
  reset_index(drop = false): unknown { return seriesApi.reset_index(this.view(), drop); }
  set_axis(labels: IndexLabel[]): Series<CellValue> { return seriesApi.set_axis(this.view(), labels) as Series<CellValue>; }
  squeeze(): CellValue | Series<CellValue> { return seriesApi.squeeze(this.view()) as CellValue | Series<CellValue>; }
  take(indices: number[]): Series<CellValue> { return seriesApi.take(this.view(), indices); }
  transform(fn: (s: Series<T>) => Series<T> | CellValue[]): Series<T> { return seriesApi.transform(this.view(), fn as never) as never; }
  truncate(before?: IndexLabel, after?: IndexLabel): Series<T> { return seriesApi.truncate(this.view(), before, after) as never; }
  update(other: Series<T>): void { seriesApi.update(this.view(), other as never); }
  memory_usage(): number { return seriesApi.memory_usage(this.view()); }
  get at(): Record<string, unknown> { return seriesApi.accessor_at(this.view()); }
  get iat(): Record<number, unknown> { return seriesApi.accessor_iat(this.view()); }
  get array(): CellValue[] { return seriesApi.accessor_array(this.view()); }
  get list(): CellValue[] { return seriesApi.accessor_list(this.view()); }
  get attrs(): Record<string, CellValue> { return seriesApi.accessor_attrs(this.view()); }
  get flags(): { allows_duplicate_labels: boolean } { return seriesApi.accessor_flags(this.view()); }
  case_when(conditions: never): unknown { return seriesApi.case_when(this.view(), conditions); }
  compare(other: Series<CellValue>, keepShape = false): DataFrame {
    return seriesApi.compare(this.view(), other, keepShape);
  }
  combine(other: Series<CellValue>, fn: (left: CellValue | null, right: CellValue | null) => CellValue): Series<CellValue> {
    return seriesApi.combine(this.view(), other, fn);
  }
  combine_first(other: Series<CellValue>): Series<CellValue> { return seriesApi.combine_first(this.view(), other); }
  convert_dtypes(): Series<CellValue> { return this.astype(convertedDType(this._values, this._dtype, true)); }
  infer_objects(): Series<CellValue> { return this.astype(convertedDType(this._values, this._dtype, false)); }
  reindex_like(other: Series<CellValue>, fill_value: CellValue | null = null): Series<CellValue> {
    return seriesApi.reindex_like(this.view(), other, fill_value);
  }
  /**
   * pandas `Series.unstack`: move one index level (default the last) into
   * the columns of a DataFrame. Row index = remaining levels (sorted unique
   * tuples), columns = sorted unique moved-level values; absent
   * combinations fill with `fill_value ?? NaN`. Single-level series keep
   * the legacy single-column frame wrap (pandas would raise there).
   */
  unstack(level: number | string = -1, fillValue?: CellValue): DataFrame {
    if (this.nlevels <= 1) {
      return seriesApi.unstack(this.view(), typeof level === "string" ? level : undefined);
    }
    const names = this.index_names;
    const levelPos = resolveIndexLevel(level, this.nlevels, names);
    const grid = unstackGrid(this._index, levelPos);
    const fill = fillValue === undefined ? NaN : fillValue;

    const storageColumns = grid.colKeys.map((colKey, i) => `__mi_col_${i}_${String(colKey)}`);
    const displayColumns: IndexLabel[] = grid.colKeys.map((colKey) => colKey);
    const rows = grid.rowLabels.map(() => ({}) as Record<string, CellValue>);
    grid.rowLabels.forEach((rowLabel, rowIdx) => {
      const rawRow = Array.isArray(rowLabel) ? rowLabel : [rowLabel];
      grid.colKeys.forEach((colKey, colIdx) => {
        const sourcePosition = grid.cellPosition.get(unstackCellKey(rawRow, colKey));
        const cell = sourcePosition === undefined ? undefined : this._values[sourcePosition];
        rows[rowIdx]![storageColumns[colIdx]!] = cell === undefined ? fill : cell;
      });
    });

    const nextNames = names.filter((_, i) => grid.remainingLevels.includes(i));
    const frame = DataFrame.createInternal(
      rows,
      storageColumns,
      grid.rowLabels,
      {},
      {},
      nextNames[0],
      nextNames,
      displayColumns
    );
    return DataFrame.markUnstacked(frame, names[levelPos], [...grid.colKeys]);
  }

  /**
   * pandas `Series.xs` — cross-section by index label. With `options.level`
   * (position or name; a list pairs with a tuple key) selects entries whose
   * level equals `key`; `options.drop_level` (default true) removes the
   * matched level(s). A fully-specified key returns the value itself.
   */
  xs(
    key: IndexLabel,
    options: { level?: number | string | (number | string)[]; drop_level?: boolean } = {}
  ): CellValue | undefined | Series<CellValue> {
    if (this.nlevels <= 1 && options.level === undefined) {
      return seriesApi.xs(this.view(), key);
    }
    const tuples = indexTuples(this._index);
    const nlevels = this.nlevels;
    let levelPositions: number[];
    let keyParts: (string | number)[];
    if (options.level !== undefined) {
      const levelRefs = Array.isArray(options.level) ? options.level : [options.level];
      levelPositions = levelRefs.map((level) => resolveIndexLevel(level, nlevels, this.index_names));
      keyParts = Array.isArray(key) ? [...key] : [key as string | number];
      if (keyParts.length !== levelPositions.length) {
        throw new KeyError(`'${Array.isArray(key) ? `(${key.join(", ")})` : String(key)}'`);
      }
    } else {
      keyParts = Array.isArray(key) ? [...key] : [key as string | number];
      if (keyParts.length > nlevels) {
        throw new KeyError(`'${Array.isArray(key) ? `(${key.join(", ")})` : String(key)}'`);
      }
      levelPositions = range(keyParts.length);
    }

    const positions = crossSectionPositions(tuples, levelPositions, keyParts);
    if (positions.length === 0) {
      throw new KeyError(`'${Array.isArray(key) ? `(${key.join(", ")})` : String(key)}'`);
    }
    if (positions.length === 1 && levelPositions.length === nlevels) {
      return this._values[positions[0]!];
    }
    const dropLevel = options.drop_level ?? true;
    const dropped = dropLevel ? new Set(levelPositions) : new Set<number>();
    const nextIndex = positions.map((position) =>
      dropLevel ? dropTupleLevels(tuples[position]!, dropped) : this._index[position]!
    );
    const nextNames = this.index_names.filter((_, i) => !dropped.has(i));
    return new Series(
      positions.map((position) => this._values[position]!) as CellValue[],
      {
        index: nextIndex,
        indexName: nextNames[0],
        indexNames: nextNames.length === nlevels ? this._indexNames : nextNames,
        name: this.name,
        dtype: this._catDtype ?? this._dtype,
      }
    );
  }
  align(other: Series<CellValue>, join: "outer" | "inner" = "outer"): [Series<T>, Series<CellValue>] {
    return seriesApi.align(this.view(), other as never, join) as never;
  }
  at_time(time: string): Series<T> { return seriesApi.at_time(this.view(), time) as never; }
  between_time(start: string, end: string, inclusive: "both" | "neither" | "left" | "right" = "both"): Series<T> {
    return seriesApi.between_time(this.view(), start, end, inclusive) as never;
  }
  asfreq(freq = 1, fill_value: CellValue | null = null): Series<CellValue | null> {
    return seriesApi.asfreq(this.view(), freq, fill_value);
  }
  asof(where: number): CellValue | null { return seriesApi.asof(this.view(), where); }
  ewm(span: number, options: { min_periods?: number } = {}): ReturnType<typeof seriesApi.ewm> {
    return seriesApi.ewm(this.view(), span, options);
  }
  resample(rule: string): ReturnType<typeof seriesApi.resample> {
    return seriesApi.resample(this.view(), rule);
  }
  /**
   * pandas `droplevel`: remove one index level by position or name. On a
   * single-level index it stays an honest no-op copy.
   */
  droplevel(level: number | string = 0): Series<T> {
    if (this.nlevels <= 1) {
      return seriesApi.droplevel(this.view(), 0) as never;
    }
    const position = resolveIndexLevel(level, this.nlevels, this.index_names);
    const dropped = new Set([position]);
    const nextNames = this.index_names.filter((_, i) => i !== position);
    return new Series(this._values.map((value) => value) as T[], {
      index: this._index.map((label) => dropTupleLevels(indexTuple(label), dropped)),
      indexName: nextNames[0],
      indexNames: nextNames.length === 1 ? undefined : nextNames,
      name: this.name,
      dtype: this._catDtype ?? this._dtype,
    });
  }

  /** pandas `reorder_levels`: permute the index levels in `order`. */
  reorder_levels(...order: (number | number[])[]): Series<T> {
    const list = (order.length === 1 && Array.isArray(order[0]) ? order[0] : order) as number[];
    if (this.nlevels <= 1 || list.length === 0) {
      return seriesApi.reorder_levels(this.view(), ...list) as never;
    }
    const nlevels = this.nlevels;
    const positions = list.map((level) => resolveIndexLevel(level, nlevels, this.index_names));
    const nextNames = positions.map((position) => this.index_names[position]);
    return new Series(this._values.map((value) => value) as T[], {
      index: this._index.map((label) => {
        const tuple = indexTuple(label);
        const projected = positions.map((position) => tuple[position] ?? "");
        return projected.length === 1 ? projected[0]! : projected;
      }),
      indexName: nextNames[0],
      indexNames: nextNames.length === 1 ? undefined : nextNames,
      name: this.name,
      dtype: this._catDtype ?? this._dtype,
    });
  }

  /** pandas `swaplevel`: swap the last two levels (defaults i=-2, j=-1). */
  swaplevel(i = -2, j = -1): Series<T> {
    if (this.nlevels <= 1) {
      return seriesApi.swaplevel(this.view()) as never;
    }
    const nlevels = this.nlevels;
    const a = resolveIndexLevel(i, nlevels, this.index_names);
    const b = resolveIndexLevel(j, nlevels, this.index_names);
    const order = range(nlevels).map((position) =>
      position === a ? b : position === b ? a : position
    );
    const nextNames = order.map((position) => this.index_names[position]);
    return new Series(this._values.map((value) => value) as T[], {
      index: this._index.map((label) => {
        const tuple = indexTuple(label);
        return order.map((position) => tuple[position] ?? "");
      }),
      indexName: nextNames[0],
      indexNames: nextNames,
      name: this.name,
      dtype: this._catDtype ?? this._dtype,
    });
  }
  set_flags(options: { allows_duplicate_labels?: boolean }): Series<T> { return seriesApi.set_flags(this.view(), options) as never; }
  tz_localize(tz: string | null): Series<T> { return seriesApi.tz_localize(this.view(), tz) as never; }
  tz_convert(tz: string | null): Series<T> { return seriesApi.tz_convert(this.view(), tz) as never; }
  to_clipboard(sep = "\t"): string { return seriesApi.to_clipboard(this.view(), sep); }
  to_hdf(): Buffer { return seriesApi.to_hdf(this.view()); }
  to_pickle(): Buffer { return seriesApi.to_pickle(this.view()); }
  to_sql(tableName: string): string { return seriesApi.to_sql(this.view(), tableName); }
  /**
   * Reverse of `to_period`: Period index labels become boundary Dates and
   * `period[<f>]` values become `datetime64[ns]` instants (`how='S'` start,
   * `how='E'` end). Non-period content returns a copy.
   */
  to_timestamp(how: "S" | "E" = "S"): Series<CellValue> { return seriesApi.to_timestamp(this.view(), how); }
  to_xarray(): Record<string, CellValue> { return seriesApi.to_xarray(this.view()); }
  to_excel(columnName?: string): string { return seriesApi.to_excel(this.view(), columnName); }
  to_markdown(columnName?: string): string { return seriesApi.to_markdown(this.view(), columnName); }
  to_latex(columnName?: string): string { return seriesApi.to_latex(this.view(), columnName); }

  /**
   * pandas `.cat` accessor: category metadata + recoding methods that return
   * new categorical Series. Non-categorical series get an inferred view.
   */
  get cat(): CategoricalMethods {
    return new CategoricalMethods(this as unknown as Series<CellValue>);
  }

  /** The CategoricalDtype backing a category-dtype series; undefined otherwise. */
  get catDtype(): CategoricalDtype | undefined {
    return this._catDtype;
  }
  get html(): string { return this.to_frame().to_html(); }
  /** ASCII histogram of the numeric values. */
  hist(bins = 10): string { return asciiHistogram(this._values as unknown as CellValue[], bins); }
  /** SVG line plot over positions. */
  plot(height = 120): string { return svgLine(this._values as unknown as CellValue[], height); }
  /** SVG histogram. */
  plot_hist(bins = 10, height = 120): string {
    return svgHistogram(this._values as unknown as CellValue[], bins, height);
  }
  boxplot(width = 50): string { return asciiBoxplot(this._values as unknown as CellValue[], width); }
  get sparse(): { density: number; filled: number; missing: number } {
    return sparseInfo(this._values as unknown as CellValue[]);
  }
  get style(): never {
    throw new NotSupportedError("Styled rendering is not supported; use to_string()/to_html().");
  }
  get struct(): never {
    throw new NotSupportedError("Struct accessor is not supported.");
  }
}


/** Renders a cell for text exports ("nan" for missing, ISO for dates). */
/** Escapes LaTeX special characters in plain text. */

/**
 * Wrap a computed window so numeric aggregations assert pandas' DataError
 * on non-numeric values; `count` tallies any non-missing cell (pandas exempts it).
 */
function wrapWindowAggregations(window: RollingWindow, values: CellValue[], dtype: DType): RollingWindow {
  const aggregable = (fn: () => CellValue[]): (() => CellValue[]) =>
    () => {
      assertWindowAggregable(values, dtype ?? "object");
      return fn();
    };
  return {
    mean: aggregable(() => window.mean()),
    sum: aggregable(() => window.sum()),
    min: aggregable(() => window.min()),
    max: aggregable(() => window.max()),
    std: aggregable(() => window.std()),
    median: aggregable(() => window.median()),
    count: () => window.count(),
  };
}
