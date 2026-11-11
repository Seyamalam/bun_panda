# Changelog

All notable changes to this project will be documented in this file.

The format loosely follows Keep a Changelog and Semantic Versioning.

## [Unreleased]

## [0.6.0] - 2026-11-11

- Add MultiIndex support: tuple index labels, per-level names (`index.names`/`nlevels`), multi-column `set_index`, multi-key `groupby(as_index=true)`, `droplevel`/`reorder_levels`/`swaplevel`, per-level `reset_index`, `.loc` tuple/outer-level selection, `xs`, `unstack`/`stack`, `sort_index level=`, and `groupby(level=)`.

- Add timezone-aware `datetime64` storage: `datetime64[ns[, zone]]` dtypes, `NaT` sentinel, real `tz_localize`/`tz_convert` on Series, DataFrame, `Timestamp` and `DatetimeIndex`, and zone-aware `.dt` accessors using `Intl.DateTimeFormat` (real DST rules). Naive-zone coverage is a documented static-offset table; precision is milliseconds.

- Add exact bigint-backed `int64`/`Int64`/`uint64`/`UInt64` storage: values beyond 2^53 store exactly as `bigint` (replacing the previous `RangeError`), with BigInt arithmetic through add/sub/mul and wide-sum accumulators in groupby reductions.

- Expand the pandas 3.0.5 oracle to 7,966 generated dtype/value contracts across seventeen mode families (arithmetic promotion, label alignment, window/string/datetime accessors, pivots, merges, rank, inference). All cases converge. Suite now runs 12,926 tests.

- Apply pandas-faithful arithmetic and alignment semantics: exact-width dtype promotion, scalar width preservation, IEEE comparison rules, duplicate-label broadcast alignment, bool/string/object op tables, `floordiv`/`mod` zero-divisor rules, and `combine`/`combine_first` reindex semantics.

- Fix groupby `as_index` (was silently ignored for all groupbys), merge masked-dtype preservation and outer-merge key ordering, `isin`/`value_counts`/`fillna` dtype propagation, `pivot`/`pivot_table` dtype rules and sorted output, float32 `sem`/`describe` rounding, exact bigint subtraction (`2^60+7 - 2^60`), and `wrapWindowAggregations` on rolling/ewm paths.

- `DataFrame.sort_values` now accepts an options object (`{ascending, na_position, limit}`) alongside the positional signature.

- Add `DataFrame.query(expr)` and pandas-faithful `DataFrame.eval(expr)`: string expressions over column names with comparisons, `and`/`or`/`not`, `in`/`not in`, `@var` resolvers, backtick-quoted column names, `.isna()`/`.isin()`/`.str.contains()` chains, `col = expr` assignment, and pandas error classes (`UndefinedVariableError`, `NotImplementedError`, `AttributeError`).

- Add `Categorical` dtype support: `dtype: "category"`, `.cat` accessor (`categories`, `codes`, `ordered`, `rename_categories`, `set_categories`, `add_categories`, `remove_categories`, `reorder_categories`, `as_ordered`/`as_unordered`), ordered code-order comparisons, pandas unordered-comparison errors, and `astype("category")`.

- **Breaking**: `DataFrame.dtypes` is now a pandas-style property (getter) instead of a method — `df.dtypes` replaces `df.dtypes()`.

- Oracle grows to 8,318 pandas 3.0.5 contracts: pairwise `corr`/`cov`/`corrwith`/`autocorr` with label alignment, `clip` bound semantics, `.str.cat` modes, and `diff`/`pct_change`/cumulative dtype rules.

- Add `Period` support: `Period(value, freq)` with pandas-exact epoch ordinals for `Y`/`Q`/`M`/`W`/`D`/`h` frequencies, `period[<freq>]` dtypes, `PeriodIndex`, `period_range`, `Timestamp.to_period`/`Series.dt.to_period`/`Series.to_period`/`to_timestamp`, ordinal comparisons/sorting/shift arithmetic, `NaT` propagation, and period-keyed groupby/join.

- Fix IO roundtrips for wide and typed values: CSV parses >2^53 integers as exact `bigint`, writes MultiIndex levels as separate columns with index-name headers, and formats datetimes pandas-style per resolution; JSON emits unsafe bigints as raw digits and `NaT`/`NA` as `null`; Parquet writes `int64` as `INT64` (was `DOUBLE`) and datetime as `TIMESTAMP_MILLIS` with exact round-trip; Excel no longer drops interior empty rows.

## [0.5.0] - 2026-11-09

- Raise pandas-typed errors with pandas-exact messages: `KeyError` for missing labels (including `Index([...], dtype='str')` and subset-list formats), `ValueError` for invalid rank options and non-integer `diff` periods, and `TypeError` slice/operand errors for `shift`, `pct_change` and `diff` on non-subtractable columns. New exported `KeyError` class. `diff` and `pct_change` now reject string/mixed columns the way pandas does instead of returning nulls.

- Accumulate groupby sum/mean with Kahan compensated summation in both the TypeScript and Rust/Wasm kernels, matching pandas bit-for-bit. Rebuilt `bun_panda_core.wasm` (7,728 bytes).

- Emit `NaN` for merge/diff missing fills and keep all-missing merged columns object-typed, matching pandas missing representation and merged dtypes.

- Match pandas rank semantics: string and date ordering, `pct` output, and nullable UInt64/Float64 Series results while DataFrame.rank stays float64. Add 585 pandas rank contracts.

- **Breaking**: `DataFrame.insert` and `DataFrame.isetitem` now mutate in place and return `undefined`, matching pandas. They previously returned a new frame.

- Keep numeric and string index labels distinct, match NaN labels, and reject reindexing duplicate source labels with pandas ValueError text. Add 162 scalar-index contracts and preserve exact-label matching in combine/corrwith.

- Validate in-place DataFrame updates against stored dtypes, including float32 precision, nullable casts and numeric-width promotion. Add 232 pandas update contracts.

- Preserve untouched column dtypes during assignment and align assigned Series by label while retaining their dtype. Validate assign/insert/isetitem values and dtypes against 1,566 pandas cases.

- Infer pandas scalar dtypes in DataFrame columns and normalize missing values before storage. Preserve dtypes through selections, update Parquet dtype handling, and exclude all missing sentinels from Wasm grouping and crosstab keys. Add 105 pandas-generated frame inference/selection contracts.

- Infer common pandas dtypes in Series constructors, including float/NaN and str/NaN defaults. Preserve float dtype and negative zero during rounding, honor object-rounding behavior, and retain declared input dtypes in pandas oracle adapters.

- Make equality dtype-aware and distinguish NA/NaT from object nulls. Add numeric-family and exclusion support to select_dtypes, validated by 93 pandas query contracts.

- Preserve or promote explicit dtypes in shift, reindex, where/mask and fill operations. Reject incompatible nullable replacements and expand dtype contracts to 1,808 pandas cases.

- Preserve explicit dtypes through row/column selection, sorting, copying and renaming. Correct DataFrame negative head/tail counts and tail(0); extend pandas dtype/selection contracts to 1,379 cases.

- Correct Series/DataFrame dtype inference: preserve numeric-looking strings, select nullable types from scalar values, and distinguish object nulls, NA and NaN. Expand pinned dtype contracts to 1,106 cases.

- Add explicit numeric/nullable, boolean, string and object dtype casts and metadata, checked against 528 pandas-generated constructor/cast cases. Nullable missing values use NA; float missing values use NaN. Exact integers outside the JavaScript safe range are rejected.

### Added

- Add 524 pinned pandas 3.0.5 value and metadata contracts, an API-by-API
  evidence inventory, and strict conformance reporting. Mismatches and invalid
  observation sets now fail the conformance command. Live results go to `out/`.

- Added the browser-based Wasm Analytics Studio example with deterministic
  workload generation, TypeScript/Wasm comparisons, Web Worker execution,
  SHA-256 correctness checks, adaptive-dispatch explanations, capability and
  bundle telemetry, local run history, and downloadable JSON reports.
- Added seeded CSV/JSON property tests and numeric, IO, reshape, categorical,
  and datetime contract tests. Raised the line-coverage gate from 70% to 85%.

### Changed

- Match tested pandas defaults for dummy encoding, categorical validation and
  typed values, missing-value factorization, empty Series reductions, constant
  moments, ties-to-even rounding, Series slicing/name preservation, and string
  operations. These intentionally change earlier behavior; migration details
  and remaining incompatibilities are in `docs/COMPATIBILITY.md`.

### Fixed

- Recognize exported `NA`/`NaT`, floating NaN, null/undefined and invalid dates
  consistently as missing, including typed-column preparation and key encoding.

- Quote carriage returns in CSV fields so exported records round-trip.
- Correct the sample-size normalization of skewness and the unbiased Fisher
  correction for excess kurtosis.
- Honor `get_dummies({ drop_first: true })` and use the missing-value code for
  `NaN` in top-level `factorize`.
- Parse `ms` duration tokens as milliseconds rather than minutes.
- Recover from malformed browser run history and keep completed showcase runs
  usable when browser storage is unavailable.

## [0.4.1] - 2026-08-27

- Corrected the pandas conformance oracle so merge comparisons preserve left-key group order while treating duplicate match pairing as unordered, matching the documented pandas contract. The tested corpus now agrees in 2,500 of 2,500 observations.

### Added

- Added `DataFrame.from_arrow` and `Series.from_arrow` constructors.

### Changed

- Prepared npm publishing with real repository metadata, a `prepublishOnly` check, a smaller package that omits Rust build artifacts, and a packed-package install/import smoke test.

### Fixed

- Restored 23 public Series delegates that were lost when their implementations moved into `src/internal/series/seriesApi.ts`.
- Fixed delegated `Series.pop` and `Series.update` so they mutate the owning Series through the structural host view.
- Updated the parity auditor to detect static and async class methods; the verified report is 505/505.
- Moved the 70% coverage threshold into `bunfig.toml`; the old CLI form treated `70` as a test-file filter on Bun 1.4.

## [0.4.0] - 2026-08-25

### Added

- **Pandas API parity: 84% → 99% (426/505 → 500/505).** The remaining 5 APIs are plotting-only stubs.
  - Top-level closed out at **100% (75/75)** (`711a224`): `Interval` with closure-aware `contains`/`overlaps`, top-level `align`, `array` namespace, bare-name `melt`/`pivot`, pandas-named `test`.
  - Categorical + IO batch (`3bbfe08`): `Categorical`/`CategoricalDtype` (codes/categories/equals/map/describe), `read_html` table scraper, `read_fwf` colspecs, `read_json_lines`, `read_xml`, clipboard/pickle/feather/orc/hdf bridges, async `read_sql` family, `show_versions`.
  - Window/time batch (`f8a4d44`): `ewm(span)` with pandas adjust=True seeding on both Series and DataFrame, `resample(rule)` binning with sum/mean/min/max/count/ohlc/asfreq, `at_time`/`between_time` time-of-day filters, `align`, `asof`, level ops (`droplevel`/`swaplevel`/`reorder_levels`), `isetitem`, tz helpers, export shims (`to_clipboard`/`to_sql`/`to_xarray`/`to_latex`/…), `cat` accessor, `from_arrow`. Shared engines extracted to `src/internal/shared/{time,windows}.ts`.

### Tests

- Suite grows from 306 to **352 tests / 743 expects** across 22 files; coverage 81% lines against the 70% gate.

## [0.3.0] - 2026-08-24

### Added

- **Pandas API parity climb: 50% → 84% (255/505 → 426/505 tracked APIs)** across two batches (`584d79e`, `0a4e4ba`):
  - DataFrame (+48): fills/cumulative/stats exports (`ffill`/`bfill`/`interpolate`, `cummax`/`cummin`/`cumprod`, `skew`/`kurt`/`sem`, `mode`), iteration (`items`/`iterrows`/`itertuples`/`iat`/`axes`/`attrs`), selection (`xs`, `take`, `truncate`, `reindex`, `reindex_like`), frame algebra (`combine`, `combine_first`, `compare`, `dot`, `corrwith`, `update`, `explode`, safe `eval` expression parser, `stack`/`unstack`), dtype tools (`convert_dtypes`, `infer_objects`), export (`to_numpy`/`to_html`/`to_markdown`), plus `squeeze`/`info`/`memory_usage` and arithmetic aliases.
  - Series (+70): property getters (`empty`/`ndim`/`size`/`shape`/`dtype`/`name`), stats aliases (`kurtosis`, `agg`), ops (`nunique`, `sort_values`/`sort_index`, `where`/`mask`, `sample`, `reindex`, `shift`/`diff`/`pct_change`, `drop_duplicates`/`duplicated`, `case_when`, `compare`, `combine*`), datetime helpers (`between_time`, `asfreq`, `asof`, `.dt` expansion), pair ops (`corr`/`cov`/`dot`), structural (`factorize`, `explode`, `groupby`, `unstack`, `xs`, `set_axis`, `repeat`, `rename`, `reset_index`, `transform`, `update`, `pop`), and string exports (`to_csv`/`to_json`/`to_string`/`to_excel`/`to_markdown`/`to_latex`).
  - GroupBy (+13 → **100% of the audited surface**): `skew`/`kurt`/`sem`/`pct_change`/`rank`/`idxmax`/`idxmin`/`cumprod`/`rolling`/`corr`/`cov`/`ohlc`/`resample`.
  - Top-level module (`src/top-level.ts`, +22): options registry (`get_option`/`set_option`/`reset_option`/`describe_option`/`option_context`), `NA`/`NaT` sentinels, scalar & index types (`Timestamp`, `Timedelta`, `Period`, `Index`, `MultiIndex`, `DatetimeIndex`, `TimedeltaIndex`, `PeriodIndex`), range builders (`date_range` positional form, `timedelta_range`, `interval_range`, `period_range`), reshape wrappers (`melt_frame`, `pivot_frame`, `lreshape`, `wide_to_long`) and ordered merges (`merge_asof`, `merge_ordered`).

### Fixed

- `DataFrame.combine`/`combine_first`: column alignment now projects both sides onto the index union instead of each side's own labels (`alignColumn` in `src/internal/dataframe/combine.ts`).
- `DataFrame.iat` out-of-bounds row lookup returns an undefined-yielding proxy rather than throwing on the inner access.
- `Series.infer_objects` / `DataFrame.infer_objects` coerce all-string numeric columns honestly (previous mixed-kind heuristic left them untouched).
- `date_range("2026-01-01", "2026-01-05")` positional call shape no longer throws (options-object and positional forms both supported).

### Tests

- Suite grows from 255 to **306 tests / 627 expects** across 18 files; coverage 81% lines against the 70% gate.

## [0.2.1] - 2026-08-23

### Added

- Validation layer (`src/errors.ts:1`, `src/internal/dataframe/core.ts:1`, `src/io.ts:51`):
  - `BunPandaValidationError` typed error class with `name === "BunPandaValidationError"`.
  - `assertRowsShape(rows, index)` helper and two call sites (`DataFrame` constructor, `createInternal`) so mismatched index lengths raise a single typed error.
  - typed `assertValidPath` guard in every `read_*` entry point (`read_csv`/`read_parquet`/`read_excel`/`read_json`); `test/validation.test.ts` covers the helper and each entry point.
- GroupBy extracted helper (`src/internal/groupby/fastAgg.ts:1`, `test/groupby-fastagg.test.ts:1`): `shouldTryWasm` + ordering helpers lifted into a dedicated module with its own test so `src/groupby.ts` drops from 1255 → 1067 LOC.
- DataFrame IO module (`src/internal/dataframe/io.ts:1`): `to_csv`/`to_json` pure formatting helpers extracted so `src/dataframe.ts` drops from 2253 → 2190 LOC.
- Coverage gate (`package.json:41`, `.github/workflows/ci.yml`): `bun test --coverage --coverage-threshold 70` step.
- Lockfile (`bun.lock`) now committed so `bun install --frozen-lockfile` is reproducible on `master`.

### Changed

- Linter is now **oxlint** (up to 50× faster than eslint): `bun run lint` uses `oxlint --deny-warnings` (`oxlint 1.79.0`, fixed config in `.oxlintrc.json`), `bun run check` now runs `typecheck + lint + test`. `README.md` installation now spells out the frozen-lockfile workflow and `tsconfig.json` excludes `scripts/` so the fresh-clone check is green.
- `GroupBy` fast path now delegates `shouldTryWasm` to `src/internal/groupby/fastAgg.ts`; wasm default-on semantics unchanged (`BUN_PANDA_WASM=0` opts out).

### Fixed

- Fresh-clone build/test now works from the `README` alone (reproducible via `bun.lock` + `.oxlintrc.json`).

## [0.2.0] - 2026-08-22

### Added

- pandas-parity `DataFrame` transforms:
  - `DataFrame.where(cond, other?)` and `DataFrame.mask(cond, other?)` (function or column-map conditions, `null` fill default).
  - `DataFrame.transform(input)` with function and column-map modes.
  - `DataFrame.insert(loc, column, value)` and `DataFrame.pop(column)`.
- pandas-parity Series operations (`src/series.ts`):
  - element-wise arithmetic: `add`, `sub`, `rsub`, `mul`, `div`, `mod`, `pow` (scalar or Series; nulls propagate), `neg`, `abs`, `round`
  - comparisons returning boolean Series: `eq`, `ne`, `lt`, `le`, `gt`, `ge`
  - cumulative: `cumsum`, `cummax`, `cummin`; selection: `nlargest`, `nsmallest`
  - `.str` accessor (`upper`, `lower`, `capitalize`, `title`, `strip/lstrip/rstrip`, `zfill`, `pad`, `slice`, `replace`, `contains`, `startswith`, `endswith`, `match`, `find`, `len`, `get`, `count`, `split`, `cat`) with null propagation
- pandas-parity DataFrame iteration and deltas:
  - `iterrows()`, `itertuples()`, `items()`
  - `shift(periods)`, `diff(periods)`, `pct_change(periods)`
- Numeric parity helpers on `DataFrame`:
  - `duplicated(subset?, keep?)`, `equals(other)`.
  - `median()`, `std()`, `var()`, `min()`, `max()`, `count()` column summaries.
  - `round(decimals?)`, `abs(columns?)`, `cumsum(columns?)`.
- Expanded `AggName` set and shared median/variance stats helpers in `src/utils.ts`.
- `GroupBy` convenience aggregations: `min`, `max`, `median`, `std`, `var`, `nunique`, `first`, `last` (each accepts an optional column list).
- `GroupBy.transform(spec)` aligned to source rows, supporting function and dict modes with pandas skipna semantics.
- `agg` support for `median`, `std`, `var`, `first`, `last`, and `nunique` via the unified finalize path.
- Parity test coverage in `test/dataframe-parity.test.ts` and `test/groupby-parity.test.ts`.
- Rust/WASM core (`crates/core` → `src/wasm/bun_panda_core.wasm`, 3.3KB):
  - flat C ABI (`bp_alloc`/`bp_group_ids`/`bp_agg_f64`/`bp_agg_multi_f64`/`bp_argsort_f64`/`bp_filter_indices`/`bp_free_all`), no bindgen/wasm-bindgen
  - `src/wasm/kernel.ts` loader with lazy init and pure-TS fallback; new high-level wrappers `wasmAggMultiF64`, `wasmArgsortF64`, `wasmFilterIndices`
  - default-on for numeric groupby aggregations after single-pass key packing (~1.4x faster `groupby_mean` vs prior TS-only build; `BUN_PANDA_WASM=0` opts out); parity verified against TS path (`test/wasm-kernel.test.ts`)
  - columnar typed-array substrate `src/wasm/columns.ts` (`Float64Array` with NaN = missing) — one fused kernel call per `groupby(...).agg({...})` instead of per-column marshalling
  - single-column numeric `DataFrame.sort_values` and boolean-mask `DataFrame.filter` delegated to wasm kernels (stable argsort, `BUN_PANDA_WASM=0` falls back)
- Typed columnar parquet ingest (`src/internal/io/parquetTyped.ts`): numeric columns decode straight into `Float64Array`s (NaN = missing) per row group, skipping per-cell conversion dispatch — ~20% faster end-to-end `read_parquet`; null handling verified by round-trip tests
- `DataFrame.from_typed(data)` constructor accepting `Float64Array` columns (NaN = missing) for column-major ingestion
  - `bun run build:wasm` rebuilds the artifact

### Changed

- CI re-enabled (`.github/workflows/ci.yml`): Rust toolchain + `bun run build:wasm` + `git diff --exit-code src/wasm/bun_panda_core.wasm` guard; `bun test` runs on both the wasm-default and `BUN_PANDA_WASM=0` pure-TS paths.

## [0.1.19] - 2026-02-22

### Added

- DataFrame apply/map compatibility APIs:
  - `DataFrame.apply(fn, axis?)` with pandas axis semantics (`0/"index"` for columns, `1/"columns"` for rows).
  - `DataFrame.applymap(fn)` and alias `DataFrame.map(fn)`.
- Series compatibility helpers:
  - `Series.isin(values)`
  - `Series.clip(lower?, upper?)`
  - `Series.replace(toReplace, value?)`
- New focused compatibility tests in `test/pandas-apply-series-compat.test.ts`.

### Changed

- Improved code structure with new internal modules:
  - `src/internal/dataframe/apply.ts`
  - `src/internal/series/compat.ts`

## [0.1.18] - 2026-02-22

### Added

- Benchmark coverage expansion:
  - added join/merge benchmark cases (`inner`, `left`, `outer`) across base/wide/skewed/high-card datasets.
- New drift reporting utility:
  - `bench/report-drift.js` (family p50/p90/max ratios + slow-case table).
  - `bench:drift` script and CI artifact output (`bench/results/drift.json`, `bench/results/drift.txt`).

### Changed

- Updated regression gating to support a dedicated merge-family ratio threshold (`BUN_PANDA_BENCH_MERGE_MAX_RATIO`).
- Benchmark CI now runs Arquero gate, drift report, IO gate, and pandas gate in sequence.

## [0.1.17] - 2026-02-22

### Changed

- Improved `read_csv` / `parse_csv` large-file behavior for common unquoted inputs:
  - added direct unquoted parse path that materializes records without a full `rows[][]` intermediate
  - reduced temporary allocation pressure in CSV ingestion hot path

## [0.1.16] - 2026-02-22

### Changed

- Optimized `GroupBy` hot paths for repeated workloads:
  - cached group partitions per DataFrame + key set + `dropna`
  - reduced per-row overhead in named-aggregation fast path with plan-code updates
- Improved pandas comparison in headline `groupby_mean` scenario.

## [0.1.15] - 2026-02-22

### Added

- New pandas-style DataFrame helpers:
  - `isin(values)`
  - `clip(lower?, upper?, columns?)`
  - `replace(toReplace, value?)`
  - `sample(n?, options?)`
  - `rank(options?)`
- `sort_values(..., na_position)` compatibility option (`"first"` or `"last"`).
- Compatibility test coverage for new APIs and null-order sorting behavior.

## [0.1.14] - 2026-02-22

### Added

- New CI performance gates:
  - `bench/assert-io-regression.js` for IO parser headline cases.
  - `bench/assert-pandas-regression.js` for tracked `bun_panda` vs pandas ratio ceilings.
- New scripts:
  - `bench:gate:io`
  - `bench:gate:pandas`

### Changed

- Benchmark workflow now runs three gates in CI:
  - Arquero regression gate (`bench:gate`)
  - IO regression gate (`bench:gate:io`)
  - pandas ratio gate (`bench:gate:pandas`)

## [0.1.13] - 2026-02-22

### Changed

- Performance optimizations for core workloads:
  - `GroupBy` now uses lazy grouping and a one-pass fast path for named aggregations (`count`, `sum`, `mean`, `min`, `max`) to reduce allocations.
  - `sort_values` comparers now precompute column value arrays and compare by row positions to reduce property-access overhead.
  - top-k sort selection strategy tuned to avoid full-sort overhead on common benchmark limits.

## [0.1.12] - 2026-02-22

### Added

- Filetype support:
  - `read_parquet(path, options?)`
  - `read_excel(path, options?)`
  - `read_excel_sync(path, options?)`
  - `to_parquet(dataframe, options)`
  - `to_excel(dataframe, options)`
- DataFrame methods:
  - `DataFrame.to_parquet({ path })`
  - `DataFrame.to_excel({ path, sheet_name?, index? })`
- New filetype integration tests for Parquet and Excel round-trips.
- IO benchmark suite (`bench/io.js`) with 20 parser-focused cases.

### Changed

- Improved CSV parse performance for common unquoted input via a fast row-splitting path.
- Refactored shared IO index handling into `src/internal/io/frame.ts`.

## [0.1.11] - 2026-02-22

### Added

- pandas-style tabular IO helpers:
  - `read_table(path, options?)`
  - `read_table_sync(path, options?)`
  - `parse_table(text, options?)`
  - `read_tsv/read_tsv_sync/parse_tsv` aliases
- JSON-lines compatibility:
  - `read_json(..., { lines: true })`
  - `parse_json(..., { lines: true })`
  - `DataFrame.to_json({ lines: true, orient: "records" })`

### Changed

- Refactored IO parsing into smaller modules:
  - `src/internal/io/csv.ts`
  - `src/internal/io/json.ts`
  - `src/internal/io/shared.ts`
- Added focused IO compatibility tests for table/TSV and JSON-lines behavior.

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
