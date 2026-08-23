# pandas API Parity Audit

Generated 2026-08-23 — pandas methods scraped from the official API reference (frame.html, series.html), bun_panda surface extracted from `src/`.

| Surface | pandas API | bun_panda | Parity |
| --- | ---: | ---: | ---: |
| DataFrame | 197 | 71 | 36% |
| Series | 195 | 46 | 24% |
| Top-level | 75 | 14 | 19% |
| GroupBy | 38 | 14 | 37% |
| **Total** | **505** | **145** | **29%** |

## DataFrame — missing (126)

### arithmetic (17)

`add`, `add_prefix`, `add_suffix`, `div`, `floordiv`, `mod`, `mode`, `mul`, `pow`, `radd`, `rdiv`, `rmod`, `rmul`, `rpow`, `rsub`, `sub`, `truediv`

### comparison (6)

`eq`, `ge`, `gt`, `le`, `lt`, `ne`

### export (15)

`to_clipboard`, `to_feather`, `to_hdf`, `to_html`, `to_latex`, `to_markdown`, `to_numpy`, `to_orc`, `to_parquet`, `to_period`, `to_pickle`, `to_sql`, `to_stata`, `to_timestamp`, `to_xarray`

### missing data (7)

`bfill`, `ffill`, `interpolate`, `isna`, `isnull`, `notna`, `notnull`

### other (71)

`agg`, `aggregate`, `align`, `all`, `any`, `asfreq`, `asof`, `at_time`, `attrs`, `axes`, `between_time`, `boxplot`, `combine`, `combine_first`, `compare`, `convert_dtypes`, `corr`, `corrwith`, `cov`, `dot`, `droplevel`, `eval`, `ewm`, `explode`, `first_valid_index`, `from_arrow`, `from_dict`, `from_records`, `hist`, `html`, `iat`, `infer_objects`, `info`, `isetitem`, `items`, `iterrows`, `itertuples`, `keys`, `kurt`, `kurtosis`, `last_valid_index`, `memory_usage`, `ndim`, `pipe`, `prod`, `product`, `quantile`, `reindex`, `reindex_like`, `rename_axis`, `reorder_levels`, `resample`, `rfloordiv`, `rtruediv`, `sem`, `set_axis`, `set_flags`, `size`, `skew`, `sparse`, `squeeze`, `stack`, `style`, `swaplevel`, `take`, `truncate`, `tz_convert`, `tz_localize`, `unstack`, `update`, `xs`

### plotting (1)

`plot`

### selection (4)

`idxmax`, `idxmin`, `nlargest`, `nsmallest`

### window/cumulative (5)

`cummax`, `cummin`, `cumprod`, `expanding`, `rolling`

## Series — missing (149)

### arithmetic (10)

`add_prefix`, `add_suffix`, `floordiv`, `mode`, `radd`, `rdiv`, `rmod`, `rmul`, `rpow`, `truediv`

### comparison (2)

`equals`, `get`

### export (15)

`to_clipboard`, `to_csv`, `to_excel`, `to_frame`, `to_hdf`, `to_json`, `to_latex`, `to_markdown`, `to_numpy`, `to_period`, `to_pickle`, `to_sql`, `to_string`, `to_timestamp`, `to_xarray`

### missing data (7)

`bfill`, `ffill`, `interpolate`, `isna`, `isnull`, `notna`, `notnull`

### other (108)

`agg`, `aggregate`, `align`, `all`, `any`, `argmax`, `argmin`, `argsort`, `array`, `asfreq`, `asof`, `at`, `at_time`, `attrs`, `autocorr`, `between`, `between_time`, `case_when`, `cat`, `combine`, `combine_first`, `compare`, `convert_dtypes`, `copy`, `corr`, `count`, `cov`, `describe`, `diff`, `dot`, `drop`, `drop_duplicates`, `droplevel`, `dtype`, `dtypes`, `duplicated`, `empty`, `ewm`, `explode`, `factorize`, `first_valid_index`, `flags`, `from_arrow`, `groupby`, `hasnans`, `hist`, `html`, `iat`, `infer_objects`, `info`, `is_monotonic_decreasing`, `is_monotonic_increasing`, `is_unique`, `item`, `items`, `keys`, `kurt`, `kurtosis`, `last_valid_index`, `list`, `mask`, `median`, `memory_usage`, `name`, `nbytes`, `ndim`, `nunique`, `pct_change`, `pipe`, `pop`, `prod`, `product`, `quantile`, `reindex`, `reindex_like`, `rename`, `rename_axis`, `reorder_levels`, `repeat`, `resample`, `reset_index`, `rfloordiv`, `rtruediv`, `searchsorted`, `sem`, `set_axis`, `set_flags`, `shape`, `shift`, `size`, `skew`, `sort_index`, `sort_values`, `sparse`, `squeeze`, `std`, `struct`, `swaplevel`, `take`, `transform`, `truncate`, `tz_convert`, `tz_localize`, `unstack`, `update`, `var`, `where`, `xs`

### plotting (1)

`plot`

### selection (3)

`idxmax`, `idxmin`, `sample`

### window/cumulative (3)

`cumprod`, `expanding`, `rolling`

## Top-level — missing (61)

### comparison (2)

`get_dummies`, `get_option`

### export (2)

`to_numeric`, `to_timedelta`

### io (16)

`read_clipboard`, `read_feather`, `read_fwf`, `read_gbq`, `read_hdf`, `read_html`, `read_json_lines`, `read_orc`, `read_pickle`, `read_sas`, `read_spss`, `read_sql`, `read_sql_query`, `read_sql_table`, `read_stata`, `read_xml`

### missing data (2)

`isnull`, `notnull`

### other (39)

`Categorical`, `CategoricalDtype`, `DatetimeIndex`, `Index`, `Interval`, `MultiIndex`, `NA`, `NaT`, `Period`, `PeriodIndex`, `Timedelta`, `TimedeltaIndex`, `Timestamp`, `align`, `array`, `bdate_range`, `crosstab`, `cut`, `describe_option`, `factorize`, `interval_range`, `isin`, `lreshape`, `map`, `melt`, `merge_asof`, `merge_ordered`, `option_context`, `period_range`, `pivot`, `qcut`, `reset_option`, `set_option`, `show_versions`, `test`, `timedelta_range`, `unique`, `value_counts`, `wide_to_long`

## GroupBy — missing (24)

### other (17)

`apply`, `corr`, `cov`, `describe`, `diff`, `filter`, `kurt`, `ohlc`, `pct_change`, `pipe`, `quantile`, `rank`, `resample`, `sem`, `shift`, `skew`, `value_counts`

### selection (2)

`idxmax`, `idxmin`

### window/cumulative (5)

`cummax`, `cummin`, `cumprod`, `cumsum`, `rolling`

