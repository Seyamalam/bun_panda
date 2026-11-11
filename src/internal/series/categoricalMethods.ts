// pandas `.cat` accessor: category metadata + recoding operations on a
// categorical Series. Recoding methods return new categorical Series like
// pandas (`s.cat.as_ordered()` does not mutate in place).
import { Categorical, CategoricalDtype, castCategoricalValues, inferCategoricalDtype } from "../../categorical";
import { Series } from "../../series";
import type { CellValue } from "../../types";
import { scalarKey } from "../shared/valueSemantics";

export class CategoricalMethods {
  private readonly series: Series<CellValue>;
  private readonly cat: Categorical;

  constructor(series: Series<CellValue>) {
    this.series = series;
    // Non-categorical series get an inferred view (compat: pandas raises
    // AttributeError there; this library exposes `.cat` on any series).
    const dtype = series.catDtype ?? inferCategoricalDtype(series.values as CellValue[]);
    this.cat = new Categorical(series.values as CellValue[], { dtype });
  }

  private wrap(catDtype: CategoricalDtype, values?: CellValue[]): Series<CellValue> {
    return new Series(values ?? this.cat.to_list(), {
      index: this.series.index,
      indexName: this.series.index_name,
      name: this.series.name,
      dtype: catDtype,
    });
  }

  /** The dtype's category list, in category order. */
  get categories(): CellValue[] {
    return this.cat.categories;
  }

  /** Integer codes into `categories`; -1 marks a missing value (pandas int8). */
  get codes(): Series<CellValue> {
    return new Series(this.cat.codes as CellValue[], {
      index: this.series.index,
      name: this.series.name,
      dtype: "int8",
    });
  }

  /** Whether the categories carry an ordering. */
  get ordered(): boolean {
    return this.cat.ordered;
  }

  /** The underlying CategoricalDtype. */
  get dtype(): CategoricalDtype {
    return this.cat.dtype;
  }

  /** Number of observations backing the accessor. */
  get length(): number {
    return this.cat.length;
  }

  /** Decoded values; missing categories decode to NaN. */
  to_list(): CellValue[] {
    return this.series.values;
  }

  /** Returns a copy flagged as ordered. */
  as_ordered(): Series<CellValue> {
    if (this.cat.ordered) return this.series.copy();
    return this.wrap(new CategoricalDtype(this.cat.categories, true));
  }

  /** Returns a copy flagged as unordered. */
  as_unordered(): Series<CellValue> {
    if (!this.cat.ordered) return this.series.copy();
    return this.wrap(new CategoricalDtype(this.cat.categories, false));
  }

  /**
   * Rename categories: a positional array (same length as the current list) or
   * a record mapping old -> new category (unmapped categories are kept).
   */
  rename_categories(newCategories: CellValue[] | Record<string, CellValue>): Series<CellValue> {
    const current = this.cat.categories;
    const mapped = Array.isArray(newCategories)
      ? (() => {
          if (newCategories.length !== current.length) {
            throw new RangeError(
              `new categories need to have the same number of items as the old categories!`
            );
          }
          return [...newCategories];
        })()
      : current.map(category => {
          const mapped = newCategories[String(category)];
          return mapped === undefined ? category : mapped;
        });
    const dtype = new CategoricalDtype(mapped, this.cat.ordered);
    const values = this.cat.codes.map(code => (code < 0 ? NaN : mapped[code]!));
    return this.wrap(dtype, values);
  }

  /**
   * Replace the category list entirely: values that are not members of the new
   * list become NaN; unobserved new categories are preserved (no reordering of
   * the codes — pandas recodes against the new list).
   */
  set_categories(newCategories: CellValue[]): Series<CellValue> {
    const dtype = new CategoricalDtype([...newCategories], this.cat.ordered);
    return this.wrap(dtype, castCategoricalValues(this.cat.to_list(), dtype));
  }

  /** Append unseen categories; members of the current list are rejected. */
  add_categories(newCategories: CellValue[]): Series<CellValue> {
    const existing = new Set(this.cat.categories.map(scalarKey));
    for (const category of newCategories) {
      if (existing.has(scalarKey(category))) {
        throw new TypeError(`Cannot append categories that already exist: '${String(category)}'`);
      }
    }
    return this.wrap(
      new CategoricalDtype([...this.cat.categories, ...newCategories], this.cat.ordered)
    );
  }

  /** Drop categories; observed values in removed categories become NaN. */
  remove_categories(removals: CellValue[]): Series<CellValue> {
    const existing = new Set(this.cat.categories.map(scalarKey));
    const missing = removals.filter(category => !existing.has(scalarKey(category)));
    if (missing.length > 0) {
      throw new TypeError(
        `removals must all be in old categories: {${missing.map(category => `'${String(category)}'`).join(", ")}}`
      );
    }
    const removed = new Set(removals.map(scalarKey));
    const kept = this.cat.categories.filter(category => !removed.has(scalarKey(category)));
    const dtype = new CategoricalDtype(kept, this.cat.ordered);
    return this.wrap(dtype, castCategoricalValues(this.cat.to_list(), dtype));
  }

  /** Reorder the existing categories; the category set must stay identical. */
  reorder_categories(newCategories: CellValue[]): Series<CellValue> {
    const current = [...new Set(this.cat.categories.map(scalarKey))].sort();
    const proposed = [...new Set(newCategories.map(scalarKey))].sort();
    if (current.length !== proposed.length || current.some((key, i) => key !== proposed[i])) {
      throw new TypeError("items in new_categories are not the same as in old categories");
    }
    return this.wrap(new CategoricalDtype([...newCategories], this.cat.ordered));
  }

  /** Drop categories that never appear in the data. */
  remove_unused_categories(): Series<CellValue> {
    const used = new Set(
      this.cat.codes.filter(code => code >= 0).map(code => scalarKey(this.cat.categories[code]))
    );
    const kept = this.cat.categories.filter(category => used.has(scalarKey(category)));
    if (kept.length === this.cat.categories.length) return this.series.copy();
    return this.wrap(new CategoricalDtype(kept, this.cat.ordered));
  }
}
