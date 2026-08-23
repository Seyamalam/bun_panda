# API Reference (v0.2.x)

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
- Transform: `assign`, `drop`, `rename`, `filter`, `query`, `sort_values` (single/multi-column, optional `limit` for top-k, `na_position`)
- Apply/map: `apply` (`axis=0|1` and aliases), `applymap`, `map`
- Where/mask/transform: `where(cond, other?)`, `mask(cond, other?)` (function or per-column conditions; `other` defaults to `null`), `transform(input)` (function or per-column transforms)
- Column mutation: `insert(loc, column, value)`, `pop(column)` (returns a `Series`)
- Index-aware transforms: `sort_index`, `drop_duplicates` (`ignore_index` supported), `value_counts` (`limit`, `sort`, `ascending`)
- Compatibility helpers: `isin`, `clip`, `replace`, `sample`, `rank`
- Missing values: `dropna`, `fillna`
- Indexing: `set_index`, `reset_index`
- Typing: `dtypes`, `astype`
- Summary: `sum`, `mean`, `median`, `std`, `var`, `min`, `max`, `count`, `describe`, `pivot_table`
- Elementwise: `round(decimals?)`, `abs(columns?)`, `cumsum(columns?)`
- Duplicate/identity: `duplicated(subset?, keep?)`, `equals(other)`
- Distinct counts: `nunique(dropna?)`
- Grouping: `groupby`
- Joins: `merge`
- Serialization: `to_records`, `to_dict`, `to_json` (`lines` supported with `orient="records"`), `to_csv`, `to_parquet`, `to_excel`, `to_string`

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
- Compatibility helpers: `isin`, `clip`, `replace`
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
- `min(columns?)`, `max(columns?)`, `median(columns?)`, `std(columns?)`, `var(columns?)`, `nunique(columns?)`, `first(columns?)`, `last(columns?)`
- `transform(spec)` (function or dict mode; result aligned to source rows, pandas skipna semantics)
- `size()`

WASM fast path (default): numeric named aggregations run through the wasm kernels in `src/wasm/bun_panda_core.wasm` (~1.4x faster on `groupby_mean`); set `BUN_PANDA_WASM=0` to use the pure-TS implementation (identical semantics).

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
- `read_parquet(path, options?)`
- `to_parquet(dataframe, options)`
- `read_excel(path, options?)`
- `read_excel_sync(path, options?)`
- `to_excel(dataframe, options)`
- `to_csv(dataframe, options?)`
- `concat(frames, options?)`
- `merge(left, right, options)`
- `pivot_table(dataframe, options)`

## Notes

1. API naming intentionally mirrors pandas where practical.
2. Not all pandas features are implemented in `v0.2.0`.
3. WASM acceleration: numeric groupby aggregations run through the `crates/core` → `src/wasm/` kernels by default (`BUN_PANDA_WASM=0` opts out).

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
