# pandas API Parity Audit

Generated 2026-08-23 — pandas methods scraped from the official API reference (frame.html, series.html), bun_panda surface extracted from `src/`.

| Surface | pandas API | bun_panda | Parity |
| --- | ---: | ---: | ---: |
| DataFrame | 197 | 142 | 72% |
| Series | 195 | 154 | 79% |
| Top-level | 75 | 28 | 37% |
| GroupBy | 38 | 38 | 100% |
| **Total** | **505** | **362** | **72%** |

## DataFrame — missing (55)

### export (12)

`to_clipboard`, `to_feather`, `to_hdf`, `to_latex`, `to_orc`, `to_parquet`, `to_period`, `to_pickle`, `to_sql`, `to_stata`, `to_timestamp`, `to_xarray`

### other (42)

`align`, `asfreq`, `asof`, `at_time`, `attrs`, `axes`, `between_time`, `boxplot`, `combine`, `combine_first`, `compare`, `convert_dtypes`, `corrwith`, `dot`, `droplevel`, `eval`, `ewm`, `explode`, `from_arrow`, `from_dict`, `from_records`, `hist`, `html`, `iat`, `infer_objects`, `isetitem`, `items`, `iterrows`, `itertuples`, `reindex_like`, `reorder_levels`, `resample`, `set_flags`, `sparse`, `stack`, `style`, `swaplevel`, `tz_convert`, `tz_localize`, `unstack`, `update`, `xs`

### plotting (1)

`plot`

## Series — missing (41)

### export (9)

`to_clipboard`, `to_excel`, `to_hdf`, `to_latex`, `to_markdown`, `to_pickle`, `to_sql`, `to_timestamp`, `to_xarray`

### other (31)

`align`, `asfreq`, `asof`, `at_time`, `attrs`, `between_time`, `case_when`, `cat`, `combine`, `combine_first`, `compare`, `convert_dtypes`, `droplevel`, `ewm`, `flags`, `from_arrow`, `hist`, `html`, `infer_objects`, `name`, `reindex_like`, `reorder_levels`, `resample`, `set_flags`, `sparse`, `struct`, `swaplevel`, `tz_convert`, `tz_localize`, `unstack`, `xs`

### plotting (1)

`plot`

## Top-level — missing (47)

### comparison (1)

`get_option`

### io (16)

`read_clipboard`, `read_feather`, `read_fwf`, `read_gbq`, `read_hdf`, `read_html`, `read_json_lines`, `read_orc`, `read_pickle`, `read_sas`, `read_spss`, `read_sql`, `read_sql_query`, `read_sql_table`, `read_stata`, `read_xml`

### other (30)

`Categorical`, `CategoricalDtype`, `DatetimeIndex`, `Index`, `Interval`, `MultiIndex`, `NA`, `NaT`, `Period`, `PeriodIndex`, `Timedelta`, `TimedeltaIndex`, `Timestamp`, `align`, `array`, `describe_option`, `interval_range`, `lreshape`, `melt`, `merge_asof`, `merge_ordered`, `option_context`, `period_range`, `pivot`, `reset_option`, `set_option`, `show_versions`, `test`, `timedelta_range`, `wide_to_long`

## GroupBy — missing (0)

