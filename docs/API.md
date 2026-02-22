# API Reference (v0.1.11)

## Core Classes

### `DataFrame`

Constructors:

- `new DataFrame(rowsOrColumns, options?)`
- `DataFrame.from_records(records, options?)`
- `DataFrame.from_dict(data, options?)`

Key properties:

- `columns`
- `index`
- `shape`
- `empty`

Key methods:

- Access/select: `get`, `select`, `iloc`, `loc`, `at`
- Transform: `assign`, `drop`, `rename`, `filter`, `query`, `sort_values` (single/multi-column, optional `limit` for top-k)
- Index-aware transforms: `sort_index`, `drop_duplicates` (`ignore_index` supported), `value_counts` (`limit`, `sort`, `ascending`)
- Missing values: `dropna`, `fillna`
- Indexing: `set_index`, `reset_index`
- Typing: `dtypes`, `astype`
- Summary: `sum`, `mean`, `describe`, `pivot_table`
- Distinct counts: `nunique(dropna?)`
- Grouping: `groupby`
- Joins: `merge`
- Serialization: `to_records`, `to_dict`, `to_json` (`lines` supported with `orient="records"`), `to_csv`, `to_string`

### `Series`

Constructors:

- `new Series(values, options?)`

Key properties:

- `values`
- `index`
- `length`

Key methods:

- Access: `iloc`, `loc`, `head`, `tail`
- Transform: `map`, `apply`, `filter`, `astype`
- Missing values: `fillna`, `dropna`
- Summary: `sum`, `mean`, `min`, `max`, `unique`, `value_counts`
- Serialization: `to_list`, `to_dict`

### `GroupBy`

Constructed via `dataframe.groupby(by, options?)`.

GroupBy options:

- `dropna?: boolean` (default: `true`)
- `sort?: boolean` (default: `true`)
- `as_index?: boolean` (default: `false`)

Methods:

- `agg(spec)`
- `count(columns?)`
- `sum(columns?)`
- `mean(columns?)`
- `size()`

## Top-Level Functions

From `bun_panda`:

- `read_csv(path, options?)`
- `read_csv_sync(path, options?)`
- `parse_csv(text, options?)`
- `read_table(path, options?)` (tab-separated default, pandas-style alias)
- `read_table_sync(path, options?)`
- `parse_table(text, options?)`
- `read_tsv(path, options?)` (alias of `read_table`)
- `read_tsv_sync(path, options?)`
- `parse_tsv(text, options?)`
- `read_json(path, options?)`
- `read_json_sync(path, options?)`
- `parse_json(text, options?)` (`lines: true` supported)
- `to_csv(dataframe, options?)`
- `concat(frames, options?)`
- `merge(left, right, options)`
- `pivot_table(dataframe, options)`

## Notes

1. API naming intentionally mirrors pandas where practical.
2. Not all pandas features are implemented in `v0.1.11`.

## `pivot_table` Options (focused subset)

- `index: string | string[]`
- `values: string | string[]`
- `columns?: string`
- `aggfunc?: "sum" | "mean" | "min" | "max" | "count" | AggFn`
- `fill_value?: CellValue`
- `margins?: boolean`
- `margins_name?: string` (default: `"All"`)
- `dropna?: boolean` (default: `true`)
- `sort?: boolean` (default: `true`)
