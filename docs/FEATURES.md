# Feature List

## Implemented (v0.2.0-preview)

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
- `sort_values`
- `sort_index`
- `drop_duplicates`
- `value_counts`
- `dropna`, `fillna`
- `set_index`, `reset_index`
- `dtypes`, `astype`
- `sum`, `mean`, `describe`
- `pivot_table` (`margins`, `sort`, `dropna`, `fill_value`)
- `to_records`, `to_dict`, `to_json`, `to_csv`, `to_string`
- `merge`

### Series Operations

- `head`, `tail`
- `iloc`, `loc`
- `map`, `apply`, `filter`
- `fillna`, `dropna`
- `sum`, `mean`, `min`, `max`
- `unique`, `value_counts`
- `astype`
- `to_list`, `to_dict`

### GroupBy

- `agg`
- `count`
- `sum`
- `mean`

### IO and Utilities

- `read_csv` (async)
- `read_csv_sync`
- `parse_csv`
- `to_csv`
- `concat`
- `merge`
- `pivot_table`

### Tooling and Quality

- Benchmark harness in `bench/compare.js` comparing against Arquero.
- GitHub Actions CI in `.github/workflows/ci.yml` for typecheck/tests.

## Compatibility Goal

API naming follows pandas as closely as practical for a JS/TS runtime.

## Known Differences vs pandas

1. No MultiIndex yet.
2. Dtype support is focused (`number`/`string`/`boolean`/`date`) rather than pandas-complete.
3. No lazy execution.
4. No Parquet/Excel/SQL connectors.
5. No full statistical or time-series API yet.
