# pandas API Parity Audit

Generated 2026-08-23 — pandas methods scraped from the official API reference (frame.html, series.html), bun_panda surface extracted from `src/`.

| Surface | pandas API | bun_panda | Parity |
| --- | ---: | ---: | ---: |
| DataFrame | 197 | 118 | 60% |
| Series | 195 | 84 | 43% |
| Top-level | 75 | 28 | 37% |
| GroupBy | 38 | 25 | 66% |
| **Total** | **505** | **255** | **50%** |

## DataFrame — missing (79)

### arithmetic (1)

`mode`

### export (15)

`to_clipboard`, `to_feather`, `to_hdf`, `to_html`, `to_latex`, `to_markdown`, `to_numpy`, `to_orc`, `to_parquet`, `to_period`, `to_pickle`, `to_sql`, `to_stata`, `to_timestamp`, `to_xarray`

### missing data (3)

`bfill`, `ffill`, `interpolate`

### other (56)

`agg`, `aggregate`, `align`, `asfreq`, `asof`, `at_time`, `attrs`, `axes`, `between_time`, `boxplot`, `combine`, `combine_first`, `compare`, `convert_dtypes`, `corrwith`, `dot`, `droplevel`, `eval`, `ewm`, `explode`, `from_arrow`, `from_dict`, `from_records`, `hist`, `html`, `iat`, `infer_objects`, `info`, `isetitem`, `items`, `iterrows`, `itertuples`, `kurt`, `kurtosis`, `memory_usage`, `reindex`, `reindex_like`, `reorder_levels`, `resample`, `rfloordiv`, `rtruediv`, `sem`, `set_flags`, `skew`, `sparse`, `squeeze`, `stack`, `style`, `swaplevel`, `take`, `truncate`, `tz_convert`, `tz_localize`, `unstack`, `update`, `xs`

### plotting (1)

`plot`

### window/cumulative (3)

`cummax`, `cummin`, `cumprod`

## Series — missing (111)

### arithmetic (4)

`add_prefix`, `add_suffix`, `rmod`, `rpow`

### comparison (2)

`equals`, `get`

### export (14)

`to_clipboard`, `to_csv`, `to_excel`, `to_hdf`, `to_json`, `to_latex`, `to_markdown`, `to_numpy`, `to_period`, `to_pickle`, `to_sql`, `to_string`, `to_timestamp`, `to_xarray`

### missing data (1)

`interpolate`

### other (88)

`agg`, `aggregate`, `align`, `all`, `any`, `argmax`, `argmin`, `array`, `asfreq`, `asof`, `at`, `at_time`, `attrs`, `between_time`, `case_when`, `cat`, `combine`, `combine_first`, `compare`, `convert_dtypes`, `copy`, `corr`, `cov`, `describe`, `diff`, `dot`, `drop`, `drop_duplicates`, `droplevel`, `dtype`, `dtypes`, `duplicated`, `empty`, `ewm`, `explode`, `factorize`, `first_valid_index`, `flags`, `from_arrow`, `groupby`, `hist`, `html`, `iat`, `infer_objects`, `info`, `items`, `keys`, `kurtosis`, `last_valid_index`, `list`, `mask`, `memory_usage`, `name`, `nbytes`, `ndim`, `nunique`, `pct_change`, `pop`, `reindex`, `reindex_like`, `rename`, `rename_axis`, `reorder_levels`, `repeat`, `resample`, `reset_index`, `rfloordiv`, `set_axis`, `set_flags`, `shape`, `shift`, `size`, `sort_index`, `sort_values`, `sparse`, `squeeze`, `std`, `struct`, `swaplevel`, `take`, `transform`, `truncate`, `tz_convert`, `tz_localize`, `unstack`, `update`, `where`, `xs`

### plotting (1)

`plot`

### selection (1)

`sample`

## Top-level — missing (47)

### comparison (1)

`get_option`

### io (16)

`read_clipboard`, `read_feather`, `read_fwf`, `read_gbq`, `read_hdf`, `read_html`, `read_json_lines`, `read_orc`, `read_pickle`, `read_sas`, `read_spss`, `read_sql`, `read_sql_query`, `read_sql_table`, `read_stata`, `read_xml`

### other (30)

`Categorical`, `CategoricalDtype`, `DatetimeIndex`, `Index`, `Interval`, `MultiIndex`, `NA`, `NaT`, `Period`, `PeriodIndex`, `Timedelta`, `TimedeltaIndex`, `Timestamp`, `align`, `array`, `describe_option`, `interval_range`, `lreshape`, `melt`, `merge_asof`, `merge_ordered`, `option_context`, `period_range`, `pivot`, `reset_option`, `set_option`, `show_versions`, `test`, `timedelta_range`, `wide_to_long`

## GroupBy — missing (13)

### other (9)

`corr`, `cov`, `kurt`, `ohlc`, `pct_change`, `rank`, `resample`, `sem`, `skew`

### selection (2)

`idxmax`, `idxmin`

### window/cumulative (2)

`cumprod`, `rolling`

