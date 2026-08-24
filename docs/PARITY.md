# pandas API Parity Audit

Generated 2026-08-24 — pandas methods scraped from the official API reference (frame.html, series.html), bun_panda surface extracted from `src/`.

| Surface | pandas API | bun_panda | Parity |
| --- | ---: | ---: | ---: |
| DataFrame | 197 | 166 | 84% |
| Series | 195 | 172 | 88% |
| Top-level | 75 | 50 | 67% |
| GroupBy | 38 | 38 | 100% |
| **Total** | **505** | **426** | **84%** |

## DataFrame — missing (31)

### export (9)

`to_clipboard`, `to_feather`, `to_hdf`, `to_latex`, `to_orc`, `to_parquet`, `to_sql`, `to_stata`, `to_xarray`

### other (21)

`align`, `asfreq`, `asof`, `at_time`, `between_time`, `boxplot`, `droplevel`, `ewm`, `from_arrow`, `from_dict`, `from_records`, `hist`, `html`, `isetitem`, `reorder_levels`, `resample`, `sparse`, `style`, `swaplevel`, `tz_convert`, `tz_localize`

### plotting (1)

`plot`

## Series — missing (23)

### export (6)

`to_clipboard`, `to_hdf`, `to_pickle`, `to_sql`, `to_timestamp`, `to_xarray`

### other (16)

`align`, `at_time`, `cat`, `droplevel`, `ewm`, `from_arrow`, `hist`, `html`, `reorder_levels`, `resample`, `set_flags`, `sparse`, `struct`, `swaplevel`, `tz_convert`, `tz_localize`

### plotting (1)

`plot`

## Top-level — missing (25)

### io (16)

`read_clipboard`, `read_feather`, `read_fwf`, `read_gbq`, `read_hdf`, `read_html`, `read_json_lines`, `read_orc`, `read_pickle`, `read_sas`, `read_spss`, `read_sql`, `read_sql_query`, `read_sql_table`, `read_stata`, `read_xml`

### other (9)

`Categorical`, `CategoricalDtype`, `Interval`, `align`, `array`, `melt`, `pivot`, `show_versions`, `test`

## GroupBy — missing (0)

