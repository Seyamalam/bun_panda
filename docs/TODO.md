# TODO

## Completed in v0.1.x

1. Added `DataFrame.value_counts`.
2. Added focused `pivot_table`.
3. Improved dtype consistency with shared coercion (`Series.astype` + `DataFrame.astype` + `DataFrame.dtypes`).
4. Added `sort_index`.
5. Added `drop_duplicates`.
6. Expanded merge options with `right` and `outer` joins.
7. Improved `read_csv` parser edge handling (BOM stripping, case-insensitive `na_values`).
8. Added benchmark harness comparing `bun_panda` and Arquero.
9. Added GitHub Actions CI for typecheck + tests, with manual benchmark job.
10. Expanded `pivot_table` options (`margins`, `margins_name`, `dropna`, `sort`).
11. Added multi-column `sort_values` with per-column ascending options.
12. Added `drop_duplicates(ignore_index=true)`.
13. Optimized internals for better benchmark performance (`groupby`, `sort_values`, `filter`, `value_counts`, `withRows` path).
14. Added top-k partial sorting via `sort_values(..., ..., limit)`.
15. Added top-k partial counting via `value_counts({ ..., limit })`.
16. Optimized `groupby().agg()` named aggregations to reduce per-group allocations.
17. Expanded benchmark suite to 73 cases across varied dataset shapes.
18. Added utility-focused unit test suite (`test/unit-utils.test.ts`).
19. Added adaptive two-column `value_counts` strategy for better low/high-cardinality performance.
20. Added CI performance regression gate and automated benchmark snapshot pipeline.
21. Added pandas benchmark companion and cross-runtime comparison reporting.
22. Refactored dataframe internals into smaller helper modules under `src/internal/dataframe/`.

## Next Milestone (v0.2.x)

1. Improve `read_csv` performance for large files.
2. Add more benchmark scenarios (joins, wider tables, skewed group distributions).
3. Add a CI baseline/perf threshold report for benchmark drift.

## Mid-Term (v0.3.x+)

1. Add datetime-focused helpers.
2. Add categorical dtype support.
3. Add optional Arrow interoperability layer.
4. Add package build output for npm dist targets.
5. Add docs website with runnable examples.

## Quality Backlog

1. Raise test coverage and add more edge-case suites.
2. Add property-based tests for CSV parser behavior.
3. Add performance regression checks.
