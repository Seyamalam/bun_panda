# Changelog

All notable changes to this project will be documented in this file.

The format loosely follows Keep a Changelog and Semantic Versioning.

## [0.1.10] - 2026-02-22

### Added

- JSON IO support:
  - `read_json(path, options?)`
  - `read_json_sync(path, options?)`
  - `parse_json(text, options?)`
- `DataFrame.to_json({ path, orient, space })` options form for writing JSON files directly.

### Changed

- Expanded IO test coverage with JSON read/parse/write scenarios.

## [0.1.9] - 2026-02-22

### Added

- `DataFrame.nunique(dropna = true)` for per-column distinct counts.
- `GroupBy.size()` for pandas-like group size output.
- `groupby(..., { as_index })` option:
  - supports `as_index: true` for single-key groupby outputs
  - explicitly throws for multi-key `as_index: true` until MultiIndex support is added

### Changed

- Expanded tests for `nunique`, `GroupBy.size()`, and `groupby(as_index)` behavior.

## [0.1.8] - 2026-02-22

### Changed

- Further split `DataFrame` operation internals into dedicated modules:
  - `src/internal/dataframe/valueCounts.ts`
  - `src/internal/dataframe/pivotTable.ts`
  - `src/internal/dataframe/merge.ts`
- Simplified `src/dataframe.ts` by delegating large operation logic to internal modules.
- Reduced `src/dataframe.ts` size from ~1120 lines (before split work) to ~740 lines.

## [0.1.7] - 2026-02-22

### Added

- pandas-compatibility options for `value_counts`:
  - `sort?: boolean`
  - `ascending?: boolean`
- pandas-compatibility options for `groupby`:
  - `dropna?: boolean`
  - `sort?: boolean`
- New tests for `groupby(..., options)` and `value_counts({ sort, ascending })`.

### Changed

- Refactored `DataFrame` internals into smaller helper modules:
  - `src/internal/dataframe/core.ts`
  - `src/internal/dataframe/keys.ts`
  - `src/internal/dataframe/ordering.ts`
  - `src/internal/dataframe/counts.ts`
- `groupby` now reuses shared key helpers from dataframe internals.
- Improved codebase structure to reduce monolithic helper blocks in `src/dataframe.ts`.

## [0.1.6] - 2026-02-22

### Changed

- Benchmark stability improvements:
  - `bench/compare.js` now reports median-of-rounds (configurable via `BUN_PANDA_BENCH_ROUNDS`, default `3`).
  - JSON benchmark outputs now include round-level measurements.
- Pandas comparison fairness improvements in `bench/pandas_compare.py`:
  - uses median-of-rounds timing
  - uses `groupby(..., sort=False)` for closer parity
  - uses `nlargest` for single-column top-k sorting cases
- Updated README benchmark snapshot with refreshed measurements from stabilized methodology.

## [0.1.5] - 2026-02-22

### Added

- Performance regression CI gate (`bench/assert-regression.js`) with configurable ratio threshold.
- Python benchmark companion (`bench/pandas_compare.py`) with pandas workloads aligned to core library cases.
- Cross-runtime comparison builder (`bench/compare-pandas.js`) producing `bun_panda` vs pandas benchmark tables/JSON.
- README benchmark snapshot generator (`bench/update-readme.js`) with CI integration.
- Python benchmark dependency file (`bench/requirements.txt`).

### Changed

- `bench/compare.js` now supports structured JSON output via `BUN_PANDA_BENCH_JSON`.
- CI workflow now runs:
  - typecheck + tests
  - bun_panda vs Arquero benchmark
  - pandas benchmark and merged comparison output
  - performance regression gate
  - README benchmark snapshot refresh (with optional auto-commit on `workflow_dispatch`).
- `package.json` scripts expanded for benchmark automation (`bench:arquero`, `bench:pandas`, `bench:compare:pandas`, `bench:gate`, `bench:refresh-readme`, `bench:ci`).

## [0.1.4] - 2026-02-22

### Added

- Expanded benchmark suite to `73` cases across `base`, `skewed`, `wide`, `high_card`, and `missing` datasets.
- Added benchmark coverage for:
  - additional top-k sort scenarios
  - normalized and `dropna` variants of `value_counts`
  - extra groupby aggregation mixes
- Added `test/unit-utils.test.ts` with focused unit tests for utility helpers.
- Added behavior tests for:
  - `groupby().agg()` named-aggregation missing-value handling
  - `value_counts` top-k + normalize semantics

### Changed

- Optimized `DataFrame.value_counts` internals:
  - adaptive two-column counting strategy (flat map vs nested map by observed cardinality)
  - lower-overhead tie comparison for top-k count ordering
- Optimized `GroupBy` internals:
  - lower-allocation multi-key grouping
  - single-pass named aggregation state updates

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
