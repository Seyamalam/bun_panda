# Feature List

## Implemented (v0.2.x)

### Data Structures

- `DataFrame`
- `Series`
- `GroupBy`

### DataFrame Operations

- Construction from records and columnar objects
- `columns`, `index`, `shape`, `empty`
- `head`, `tail`, `copy`
- `get`, `set`, `select`
- `iloc`, `loc`, `at`
- `assign`, `drop`, `rename`
- `filter`, `query`
- `sort_values` (single and multi-column with per-column ascending flags, optional top-k `limit`, `na_position`)
- `apply` (`axis=0|1` and aliases), `applymap`, `map`
- `where`/`mask` (function and column-map conditions; `other` defaults to `null`), `transform`
- `insert(loc, column, value)`, `pop(column)`
- `sort_index`
- `drop_duplicates` (`ignore_index` supported)
- `value_counts` (`limit`, `sort`, `ascending`)
- `isin`, `clip`, `replace`, `sample`, `rank`
- `dropna`, `fillna`
- `set_index`, `reset_index`
- `dtypes`, `astype`
- `sum`, `mean`, `median`, `std`, `var`, `min`, `max`, `count`, `describe`
- `round(decimals?)`, `abs(columns?)`, `cumsum(columns?)`
- `duplicated(subset?, keep?)`, `equals(other)`
- `nunique(dropna?)`
- `pivot_table` (`margins`, `sort`, `dropna`, `fill_value`)
- `to_records`, `to_dict`, `to_json`, `to_csv`, `to_parquet`, `to_excel`, `to_string`
- `merge`

### Series Operations

- `head`, `tail`
- `iloc`, `loc`
- `map`, `apply`, `filter`
- `fillna`, `dropna`
- `sum`, `mean`, `min`, `max`
- `unique`, `value_counts`
- `isin`, `clip`, `replace`
- `astype`
- `to_list`, `to_dict`

### GroupBy

- `agg` (supports `sum`/`mean`/`min`/`max`/`median`/`std`/`var`/`nunique`/`first`/`last`/`count` plus custom functions)
- `count`, `sum`, `mean`, `min`, `max`, `median`, `std`, `var`, `nunique`, `first`, `last` (each with optional column list)
- `transform(spec)` (function and dict modes; aligned to source rows)
- pandas-like options: `dropna`, `sort`
- pandas-like options: `as_index` (single-key output supported)
- `size`
- WASM fast path for numeric named aggregations via `src/wasm/kernel.ts` → `src/wasm/bun_panda_core.wasm` (default; `BUN_PANDA_WASM=0` falls back to pure TS)

### IO and Utilities

- `read_csv` (async)
- `read_csv_sync`
- `parse_csv`
- `read_table` (async, tab-separated default)
- `read_table_sync`
- `parse_table`
- `read_tsv` (alias)
- `read_tsv_sync` (alias)
- `parse_tsv` (alias)
- `read_json` (async)
- `read_json_sync`
- `parse_json` (`lines: true` JSON-lines support)
- `read_parquet` (async)
- `read_excel` (async)
- `read_excel_sync`
- `to_parquet`
- `to_excel`
- `to_csv`
- `concat`
- `merge`
- `pivot_table`

### Tooling and Quality

- Benchmark harness in `bench/compare.js` comparing against Arquero.
- Expanded benchmark coverage (`73` cases) including skewed, wide, high-cardinality, and missing-value datasets.
- Python benchmark companion (`bench/pandas_compare.py`) for pandas comparison.
- Performance regression gate script (`bench/assert-regression.js`) for CI.
- GitHub Actions CI in `.github/workflows/ci.yml` for typecheck/tests.
- Extended test suite with utility-level unit tests in `test/unit-utils.test.ts`.

## Compatibility Goal

API naming follows pandas as closely as practical for a JS/TS runtime.

## Known Differences vs pandas

1. No MultiIndex yet.
2. Dtype support is focused (`number`/`string`/`boolean`/`date`) rather than pandas-complete.
3. No lazy execution.
4. No SQL/database connectors yet.
5. No full statistical or time-series API yet.
