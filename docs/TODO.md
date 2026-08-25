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
23. Further split large dataframe operation logic into dedicated modules (`valueCounts`, `pivotTable`, `merge`).
24. Added pandas-style `groupby(as_index)`, `GroupBy.size()`, and `DataFrame.nunique()`.
25. Added JSON IO support (`read_json`, `read_json_sync`, `parse_json`) and `to_json({ path })`.
26. Added pandas-style table/TSV IO wrappers (`read_table`, `read_tsv`, and sync/parse variants) plus JSON-lines compatibility (`lines=true`) for `read_json`, `parse_json`, and `to_json`.
27. Added Parquet and Excel filetype support (`read_parquet`, `read_excel`, `read_excel_sync`, `to_parquet`, `to_excel`) and IO benchmark suite for parser performance.
28. Added pandas-style compatibility helpers (`isin`, `clip`, `replace`, `sample`, `rank`) and `sort_values(..., na_position)` support.
29. Optimized CSV ingestion for large unquoted inputs by adding a direct record-building parse path.
30. Expanded comparative benchmark coverage with join/merge scenarios and added CI drift reporting artifacts.
31. Added pandas-style apply APIs (`DataFrame.apply`, `DataFrame.applymap`, `DataFrame.map`) and Series helpers (`isin`, `clip`, `replace`).

## Completed in v0.3.x–v0.4.0 (parity sprint)

1. Pandas API parity 50% → 99% (255/505 → 500/505); GroupBy and Top-level at 100%.
2. Self-contained parity audit (`bun run parity` — committed baseline, refresh script included).
3. Categorical dtype support: `Categorical`, `CategoricalDtype`, `.cat` accessor.
4. Datetime helpers: `resample`, `ewm`, `at_time`, `between_time`, `asof`, `asfreq`, tz shims, `Interval`.
5. IO expansion: HTML tables, fixed-width, JSON lines, XML, clipboard, pickle/feather/orc/hdf bridges, async SQL family.

## Next Milestone (v0.4.x)

1. Extract the window/time/export method blocks from `dataframe.ts`/`series.ts` back into `src/internal/` modules (class files regressed to 2600/1930 LOC during the sprint).
2. Replace the five plotting stubs (`plot hist boxplot style sparse`) with text/SVG renderers or a pluggable backend.
3. Re-benchmark after the API expansion; verify the WASM groupby fast path still wins.
4. npm publish prep: `prepublishOnly` gates + `npm pack` smoke test.

## Mid-Term (v0.5.x+)

1. Optional Arrow interoperability layer (real binary Feather/IPC read/write).
2. Package build output for npm dist targets.
3. Docs website with runnable examples.

## Quality Backlog

1. Raise test coverage past 85%.
2. Property-based tests for CSV/JSON parsers.
3. Performance regression checks in CI (benchmark drift gate).
