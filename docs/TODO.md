# TODO

## Completed in v0.2.0-preview

1. Added `DataFrame.value_counts`.
2. Added focused `pivot_table`.
3. Improved dtype consistency with shared coercion (`Series.astype` + `DataFrame.astype` + `DataFrame.dtypes`).
4. Added `sort_index`.
5. Added `drop_duplicates`.
6. Expanded merge options with `right` and `outer` joins.
7. Improved `read_csv` parser edge handling (BOM stripping, case-insensitive `na_values`).

## Next Milestone (v0.2.x)

1. Add benchmarks against common JS dataframe libraries.
2. Expand `pivot_table` options (`columns` with richer MultiIndex-like output modes, margins).
3. Add `sort_values` multi-column sorting.
4. Add `drop_duplicates` `ignore_index` option.
5. Improve `read_csv` performance for large files.

## Mid-Term (v0.3.x+)

1. Add datetime-focused helpers.
2. Add categorical dtype support.
3. Add optional Arrow interoperability layer.
4. Add package build output for npm dist targets.
5. Add docs website with runnable examples.

## Quality Backlog

1. Raise test coverage and add more edge-case suites.
2. Add property-based tests for CSV parser behavior.
3. Add CI workflow for lint/typecheck/test.
4. Add performance regression checks.
