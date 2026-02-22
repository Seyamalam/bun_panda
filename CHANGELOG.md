# Changelog

All notable changes to this project will be documented in this file.

The format loosely follows Keep a Changelog and Semantic Versioning.

## [Unreleased]

### Added

- `sort_values` now supports multi-column sorting with per-column ascending flags.
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
- GitHub Actions workflow for CI checks and manual benchmark runs.
- Expanded test suite from 6 to 29 tests.

### Changed

- Unified dtype coercion logic across `Series.astype` and `DataFrame.astype`.
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
