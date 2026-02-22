# Changelog

All notable changes to this project will be documented in this file.

The format loosely follows Keep a Changelog and Semantic Versioning.

## [0.1.3] - 2026-02-22

### Added

- `value_counts({ ..., limit })` for top-k counting without sorting all groups.
- New tests for `value_counts` limit behavior and validation.
- Benchmark case `value_counts_group_city_top10`.

### Changed

- `groupby().agg()` named aggregations now use lower-allocation single-pass reducers.
- Additional benchmark-oriented optimization work for count/sort heavy workflows.

## [0.1.2] - 2026-02-22

### Added

- `sort_values(by, ascending, limit)` optional top-k `limit` parameter for partial-sort workflows.
- New test coverage for `sort_values` top-k correctness and limit validation.

### Changed

- Top-N benchmark scenarios now use partial sort in `bun_panda` (`sort_values(..., ..., limit)`).
- Additional hot-path optimizations in `sort_values`, `query/filter`, `head`, and `tail`.

## [0.1.1] - 2026-02-22

### Added

- `sort_values` now supports multi-column sorting with per-column ascending flags.
- `drop_duplicates` now supports `ignore_index`.
- `DataFrame` additions:
  - `value_counts`
  - `sort_index`
  - `drop_duplicates`
  - `dtypes`
  - `astype`
  - focused `pivot_table` with `margins`, `dropna`, and `sort`
- Merge mode expansion:
  - `how: "right" | "outer"`
- Top-level helper:
  - `pivot_table(dataframe, options)`
- Benchmark harness (`bench/compare.js`) comparing against Arquero.
- Expanded benchmark suite with multi-key groupby/sort, dedup, skewed, and wide-table cases.
- Benchmark methodology update:
  - operation benchmarks reuse pre-built frames/tables
  - `construct_only` isolates creation overhead
  - row-count measurement (`shape[0]` / `numRows()`) avoids materialization bias
- GitHub Actions workflow for CI checks and manual benchmark runs.
- Expanded test suite from 6 to 32 tests.

### Changed

- Unified dtype coercion logic across `Series.astype` and `DataFrame.astype`.
- Performance improvements in hot paths:
  - `groupby` avoids full record cloning
  - `sort_values` and `sort_index` sort by row positions before cloning
  - `filter` avoids duplicate row clones for predicate paths
  - faster key generation in `value_counts` and duplicate detection
  - internal `withRows` path avoids redundant DataFrame re-normalization
- CSV parsing improvements:
  - UTF-8 BOM stripping
  - case-insensitive `na_values` matching

## [0.1.0] - 2026-02-22

### Added

- Initial `DataFrame` implementation with pandas-style operations:
  - `head`, `tail`, `iloc`, `loc`
  - `assign`, `drop`, `rename`, `select`, `query`, `filter`
  - `dropna`, `fillna`, `sort_values`
  - `set_index`, `reset_index`
  - numeric summaries: `sum`, `mean`, `describe`
- Initial `Series` implementation:
  - `head`, `tail`, `iloc`, `loc`
  - `map`, `apply`, `filter`
  - `fillna`, `dropna`, `sum`, `mean`, `min`, `max`
  - `unique`, `value_counts`, `astype`
- Grouping and aggregation:
  - `groupby(...).agg(...)`, `count`, `sum`, `mean`
- IO and top-level helpers:
  - `read_csv`, `read_csv_sync`, `parse_csv`, `to_csv`, `concat`, `merge`
- Test suite for core dataframe, groupby, join, concat, and CSV behavior.
- Project docs for scope, API, features, TODO, contributing, and security.
