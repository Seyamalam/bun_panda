# pandas API Parity Audit

Generated 2026-08-24 — pandas methods scraped from the official API reference (frame.html, series.html), bun_panda surface extracted from `src/`.

| Surface | pandas API | bun_panda | Parity |
| --- | ---: | ---: | ---: |
| DataFrame | 197 | 193 | 98% |
| Series | 195 | 194 | 99% |
| Top-level | 75 | 75 | 100% |
| GroupBy | 38 | 38 | 100% |
| **Total** | **505** | **500** | **99%** |

## DataFrame — missing (4)

### export (1)

`to_parquet`

### other (3)

`from_arrow`, `from_dict`, `from_records`

## Series — missing (1)

### other (1)

`from_arrow`

## Top-level — missing (0)

## GroupBy — missing (0)

