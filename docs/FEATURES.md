# Feature List

## Implemented (v0.1.4)

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
- `sort_values` (single and multi-column with per-column ascending flags, optional top-k `limit`)
- `sort_index`
- `drop_duplicates` (`ignore_index` supported)
- `value_counts` (optional top-k `limit`)
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
4. No Parquet/Excel/SQL connectors.
5. No full statistical or time-series API yet.
