# API Reference (v0.4.x)

The measurable pandas-parity audit (`bun run parity` → docs/PARITY.md) tracks
**500/505 APIs (99%)**: DataFrame 98%, Series 99%, Top-level 100%, GroupBy 100%.
The remaining 5 are plotting-only stubs that throw `NotSupportedError`.

## Core Classes

### `DataFrame`

Constructors:

- `new DataFrame(rowsOrColumns, options?)`
- `from_records`, `from_dict`, `from_normalized`, `from_typed` (accepts `Float64Array` columns, NaN = missing)

Key properties:

- `columns`, `index`, `shape`, `empty`
- `axes` (index + column labels)

Selection & indexing: `get`, `select`, `iloc`, `loc`, `at`/`iat` (Proxy-based, out-of-bounds yields undefined), `xs(key)`, `take(indices)`, `truncate(before?, after?)`, `reindex({index, columns, fill_value})`, `reindex_like(other)`

Transform & reshape: `assign`, `drop`, `rename`, `filter`, `query`, `sort_values`, `sort_index`, `drop_duplicates`, `melt`, `pivot`, `pivot_table`, `stack`, `unstack`, `transpose`, `select_dtypes`, `explode(column)`, `eval(expr)` (safe parser — no JS eval)

Apply & map: `apply` (`axis=0|1`), `applymap`, `map`, `pipe`, `transform`

Where/mask: `where(cond, other?)`, `mask(cond, other?)`

Arithmetic & comparison: `add/sub/mul/div/truediv/floordiv/mod/pow` + r-variants (`rsub`, `rtruediv`, `rfloordiv`, …), `eq/ne/lt/le/gt/ge`, `dot(other)`

Missing data: `dropna`, `fillna(value)`, `ffill`, `bfill`, `interpolate(method="linear")`, `isna/isnull/notna/notnull`

Stats: `sum mean median std var min max count prod quantile describe skew kurt kurtosis sem corr cov corrwith mode idxmax idxmin nunique`

Cumulative: `cumsum cummax cummin cumprod cumcount`

Windows & time: `rolling(window, min_periods?)` / `expanding()` with `mean sum min max count std median`; `ewm(span)` with `mean/sum/std`; `resample(rule)` with `sum mean min max count ohlc asfreq` (bins on the first datetime column); `at_time`, `between_time`, `asof`, `asfreq`; `align(other, join)`

Frame algebra: `combine(other, fn)`, `combine_first(other)`, `compare(other)`, `merge`, `join`, `update(other)`, `concat` (top-level)

Indexing & levels: `set_index`, `reset_index`, `set_axis`, `rename_axis`, `droplevel`, `swaplevel`, `reorder_levels`, `isetitem(position, values)`, `insert(loc, column, value)`, `pop(column)`

Typing: `dtypes`, `astype`, `convert_dtypes`, `infer_objects`

Duplicates & identity: `duplicated(subset?, keep?)`, `equals(other)`, `nunique(dropna?)`, `sample(n, {replace, random_state, frac})`, `rank(options)`

Grouping: `groupby(by)` → GroupBy (see below)

Serialization & export: `to_records to_dict to_json to_csv to_string to_html to_latex to_markdown to_numpy to_parquet to_excel to_clipboard to_feather to_orc to_hdf to_stata to_sql(table) to_xarray to_pickle`

Info: `info()`, `memory_usage()`, `keys()`, `items()`, `iterrows()`, `itertuples()`, `squeeze()`, `agg`/`aggregate`, `pipe`

Intentional stubs (`NotSupportedError`): `plot`, `hist`, `boxplot`, `style`, `sparse`

### `Series`

Constructors: `new Series(values, options?)`, `Series.from_arrow(records)`

Properties: `values index length name empty ndim size shape dtype dtypes nbytes array list attrs flags`

Access: `iloc loc at iat get head tail item keys items where mask xs take truncate reindex reindex_like sample filter unique isin between`

Arithmetic: `add sub mul div mod pow floordiv truediv neg abs round` + r-variants (`radd rsub rmul rdiv rtruediv rmod rpow rfloordiv`) + long aliases; comparisons `eq ne lt le gt ge`

Missing data: `isna isnull notna notnull is_null dropna ffill bfill fillna interpolate`

Stats: `sum mean median min max count var std prod product quantile skew kurt kurtosis sem autocorr corr cov dot describe mode agg aggregate idxmax idxmin nunique first_valid_index last_valid_index memory_usage`

Cumulative: `cumsum cummax cummin cumprod cumcount`

Ranking/order: `rank sort_values sort_index argsort searchsorted nlargest nsmallest is_monotonic_increasing/decreasing is_unique duplicated drop_duplicates factorize explode repeat`

Windows & time: `rolling expanding ewm(span) resample(rule) shift diff pct_change at_time between_time asfreq asof align dt accessor str accessor cat accessor (categories/codes/describe/map/rename_categories/as_ordered)`

Structural: `map apply filter pipe copy to_frame to_dict to_list reset_index rename rename_axis set_axis squeeze transform update groupby unstack pop case_when compare combine combine_first convert_dtypes convert_dtypes infer_objects astype clip replace value_counts to_datetime`

Export: `to_list to_dict to_json to_csv to_string to_excel to_markdown to_latex to_numpy to_clipboard to_hdf to_pickle to_sql to_timestamp to_xarray`

Intentional stubs: `hist plot sparse struct html`

### `GroupBy`

Full audited surface (38/38): `agg aggregate count size sum mean median min max std var skew kurt kurtosis sem first last nth cumsum cummax cummin cumprod quantile filter apply pipe transform shift diff describe value_counts corr cov ohlc pct_change rank idxmax idxmin rolling resample`

## Top-level module

- **Options registry**: `get_option set_option reset_option describe_option option_context`
- **Sentinels**: `NA NaT`
- **Scalar/index types**: `Timestamp Timedelta Period Index MultiIndex DatetimeIndex TimedeltaIndex PeriodIndex Interval` (closure-aware `contains`/`overlaps`)
- **Range builders**: `date_range` (options-object or positional), `bdate_range timedelta_range interval_range period_range`
- **Readers**: `read_csv read_csv_sync read_table read_tsv read_excel read_json read_json_lines read_fwf read_html read_xml read_parquet read_feather read_orc read_hdf read_pickle read_sas read_spss read_stata read_clipboard read_sql read_sql_query read_sql_table read_gbq`
- **Writers**: `to_csv to_excel to_parquet`
- **Reshape/merge**: `concat merge merge_ordered merge_asof melt pivot pivot_table crosstab cut qcut get_dummies factorize lreshape wide_to_long unique value_counts isin map align array`
- **Meta**: `show_versions test Categorical CategoricalDtype`

## Errors

- `BunPandaValidationError` — malformed input (bad shapes, missing paths)
- `NotSupportedError` — intentional stubs (plotting, exotic binary formats)
