// pandas-style Categorical / CategoricalDtype.
import { DataFrame } from "./dataframe";
import type { CellValue } from "./types";

/**
 * Dtype descriptor for categorical data: the fixed category list plus an
 * orderedness flag. Mirrors `pandas.CategoricalDtype`.
 */
export class CategoricalDtype {
  readonly categories: CellValue[];
  readonly ordered: boolean;

  constructor(categories?: CellValue[], ordered = false) {
    const seen = new Set<string>();
    const unique: CellValue[] = [];
    for (const value of categories ?? []) {
      const key = String(value);
      if (!seen.has(key)) {
        seen.add(key);
        unique.push(value);
      }
    }
    this.categories = unique;
    this.ordered = ordered;
  }

  /** True when both dtypes list the same categories in the same order with matching orderedness. */
  equals(other: unknown): boolean {
    if (!(other instanceof CategoricalDtype)) return false;
    if (this.ordered !== other.ordered) return false;
    return (
      this.categories.length === other.categories.length &&
      this.categories.every(
        (c, i) => String(c) === String(other.categories[i])
      )
    );
  }

  toString(): string {
    return `categorical${this.ordered ? "" : ", non-ordered"}[${this.categories
      .map((c) => `'${String(c)}'`)
      .join(", ")}]`;
  }
}

function categoryIndex(dtype: CategoricalDtype): Map<string, number> {
  const map = new Map<string, number>();
  dtype.categories.forEach((category, position) => map.set(String(category), position));
  return map;
}

/**
 * Memory-conscious representation of values drawn from a fixed category set.
 * Stores integer codes into `categories`; unknown values get code -1.
 */
export class Categorical {
  private readonly _codes: number[];
  readonly dtype: CategoricalDtype;

  constructor(
    values: CellValue[],
    options: { categories?: CellValue[]; ordered?: boolean; dtype?: CategoricalDtype } = {}
  ) {
    if (options.dtype !== undefined) {
      if (options.categories !== undefined || options.ordered !== undefined) {
        throw new Error("Cannot specify both `dtype` and `categories`/`ordered`.");
      }
      this.dtype = options.dtype;
    } else {
      // Unspecified categories default to the sorted unique observed values.
      const provided = options.categories ?? [...new Set(values.map(String))].sort();
      this.dtype = new CategoricalDtype(provided, options.ordered ?? false);
    }
    const index = categoryIndex(this.dtype);
    this._codes = values.map((value) => index.get(String(value)) ?? -1);
  }

  /** The dtype's category list. */
  get categories(): CellValue[] {
    return [...this.dtype.categories];
  }

  /** Integer codes into `categories`; -1 marks a missing/unseen value. */
  get codes(): number[] {
    return [...this._codes];
  }

  get ordered(): boolean {
    return this.dtype.ordered;
  }

  get length(): number {
    return this._codes.length;
  }

  /** Decode back to the original category values (-1 becomes null). */
  to_list(): CellValue[] {
    return this._codes.map((code) =>
      code < 0 ? null : (this.dtype.categories[code] as CellValue)
    );
  }

  /**
   * Summary frame with one row per category: the category value, its count,
   * and its relative frequency.
   */
  describe(): DataFrame {
    const counts = new Array<number>(this.dtype.categories.length).fill(0);
    for (const code of this._codes) {
      if (code >= 0) counts[code]! += 1;
    }
    const total = counts.reduce((sum, count) => sum + count, 0);
    const rows = this.dtype.categories.map((category, i) => ({
      categories: category,
      counts: counts[i]!,
      freqs: total > 0 ? counts[i]! / total : 0,
    }));
    return new DataFrame(rows, { columns: ["categories", "counts", "freqs"] });
  }

  /** True when both categoricals share the same dtype and codes. */
  equals(other: unknown): boolean {
    if (!(other instanceof Categorical)) return false;
    if (!this.dtype.equals(other.dtype)) return false;
    return (
      this._codes.length === other._codes.length &&
      this._codes.every((code, i) => code === other._codes[i])
    );
  }

  /**
   * Map each category through `mapper` and re-categorize on the mapped values.
   * Accepts a function or a plain record keyed by category.
   */
  map(mapper: ((value: CellValue) => CellValue) | Record<string, CellValue>): Categorical {
    const lookup =
      typeof mapper === "function"
        ? mapper
        : (value: CellValue) => mapper[String(value)];
    const mappedValues = this.to_list().map((value) =>
      value === null ? null : lookup(value)
    );
    return new Categorical(mappedValues);
  }

  toString(): string {
    const decoded = this.to_list().map((v) => (v === null ? "NaN" : String(v)));
    return `Categorical([${decoded.join(", ")})], categories=${this.dtype.toString()})`;
  }
}
