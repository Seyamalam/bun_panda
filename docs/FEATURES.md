# Feature List

## Implemented (v0.4.x — 99% of the audited 505-API pandas surface)

### Data Structures

- `DataFrame`
- `Series`
- `GroupBy` (100% of the audited GroupBy surface)
- Top-level types: `Timestamp`, `Timedelta`, `Period`, `Index`, `MultiIndex`, `DatetimeIndex`, `TimedeltaIndex`, `PeriodIndex`, `Interval`, `Categorical`, `CategoricalDtype`

### DataFrame Operations

- Construction from records, columnar objects, typed arrays; `from_records`/`from_dict` factories
- Selection: `get select iloc loc at iat xs take truncate reindex reindex_like sample align`
- Transform: `assign drop rename filter query sort_values sort_index drop_duplicates melt pivot stack unstack transpose select_dtypes explode eval` (safe expression parser)
- Apply: `apply applymap map pipe transform`
- Where/mask: function or per-column conditions
- Arithmetic: full elementwise set + r-variants + comparisons + `dot`
- Missing data: `dropna fillna ffill bfill interpolate isna notna`
- Stats: `sum mean median std var min max count prod quantile describe skew kurt sem corr cov corrwith mode idxmax idxmin nunique`
- Cumulative: `cumsum cummax cummin cumprod cumcount`
- Windows & time: `rolling expanding ewm resample(at_time between_time asof asfreq)` with sum/mean/min/max/count/std/median/ohlc/asfreq aggregations
- Frame algebra: `combine combine_first compare merge join update`
- Levels/index: `set_index reset_index set_axis rename_axis droplevel swaplevel reorder_levels isetitem insert pop`
- Typing: `dtypes astype convert_dtypes infer_objects`
- Export: CSV/JSON/HTML/LaTeX/Markdown/parquet/excel/clipboard/pickle/hdf/orc/feather/stata/sql/xarray/numpy
- Info & iteration: `info memory_usage keys items iterrows itertuples squeeze agg aggregate`

### Series Operations

- Full elementwise arithmetic with r-variants and long-form aliases
- Comparisons, `between`, `case_when`
- Missing data incl. linear interpolation
- Stats incl. skew/kurt/sem/autocorr/corr/cov/dot/describe/mode
- Windows: `rolling expanding ewm resample shift diff pct_change`
- Time filters: `at_time between_time asfreq asof`
- Accessors: `.str` (~40 methods), `.dt`, `.cat` (categories/codes/describe/map)
- Structural: `factorize explode groupby unstack repeat rename rename_axis set_axis reset_index transform update pop case_when compare combine_first from_arrow`
- Ordering: `rank sort_values sort_index argsort searchsorted nlargest nsmallest monotonic checks duplicated drop_duplicates`
- Export mirrors the DataFrame export surface

### GroupBy

Complete audited API: named aggs, cumulative, windowed (`rolling`), statistical (`skew kurt sem corr cov ohlc`), positional (`rank idxmax idxmin shift diff pct_change cumprod`), plus `filter/apply/pipe/transform/describe/value_counts`. WASM fast path for numeric named aggs (default-on; `BUN_PANDA_WASM=0` opts out).

### Top-level

- Options registry (`get_option` … `option_context`), `NA`/`NaT` sentinels
- Range builders: `date_range bdate_range period_range timedelta_range interval_range`
- Readers: CSV/TSV/table/Excel/JSON(+lines)/fixed-width/HTML tables/XML/parquet/feather/orc/hdf/pickle/sas/spss/stata/clipboard/sql/gbq
- Reshape & merge: `concat merge merge_ordered merge_asof pivot crosstab cut qcut get_dummies factorize lreshape wide_to_long melt`
- Utilities: `to_datetime to_numeric to_timedelta unique value_counts isin map align array show_versions test`

### Platform

- Rust/WASM core for numeric hot paths (groupby aggregation, argsort), flat C ABI over linear memory, no wasm-bindgen; pure-TS fallback always available
- Typed errors: `BunPandaValidationError`, `NotSupportedError`
- Gates: typecheck + oxlint + 352-test suite + 70% coverage floor in CI
