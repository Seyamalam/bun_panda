# bun_panda MVP Scope

## Core Problem
Give Bun/TypeScript developers a familiar pandas-style API for common in-memory data analysis tasks.

## Success Criteria
- Users can create `DataFrame`/`Series` objects and run common transforms.
- CSV workflows (`read_csv`, `to_csv`) work out of the box.
- Grouping and joining patterns are available with pandas-like names.

## In Scope (v1)
- `DataFrame` and `Series` core classes.
- Selection/indexing (`get`, `select`, `iloc`, `loc`, `set_index`, `reset_index`).
- Transform utilities (`assign`, `rename`, `drop`, `dropna`, `fillna`, `sort_values`).
- Aggregations (`sum`, `mean`, `describe`).
- Grouping (`groupby().agg()`, `count`, `sum`, `mean`).
- IO and helpers (`read_csv`, `read_csv_sync`, `to_csv`, `concat`, `merge`).

## Explicitly Out of Scope
- Full pandas feature parity.
- MultiIndex and advanced categorical/timedelta dtypes.
- Lazy query planning and out-of-core execution.
- Excel, Parquet, and SQL connectors.
