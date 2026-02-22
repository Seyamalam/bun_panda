# API Reference (v0.1.0)

## Core Classes

### `DataFrame`

Constructors:

- `new DataFrame(rowsOrColumns, options?)`
- `DataFrame.from_records(records, options?)`
- `DataFrame.from_dict(data, options?)`

Key properties:

- `columns`
- `index`
- `shape`
- `empty`

Key methods:

- Access/select: `get`, `select`, `iloc`, `loc`, `at`
- Transform: `assign`, `drop`, `rename`, `filter`, `query`, `sort_values`
- Missing values: `dropna`, `fillna`
- Indexing: `set_index`, `reset_index`
- Summary: `sum`, `mean`, `describe`
- Grouping: `groupby`
- Joins: `merge`
- Serialization: `to_records`, `to_dict`, `to_json`, `to_csv`, `to_string`

### `Series`

Constructors:

- `new Series(values, options?)`

Key properties:

- `values`
- `index`
- `length`

Key methods:

- Access: `iloc`, `loc`, `head`, `tail`
- Transform: `map`, `apply`, `filter`, `astype`
- Missing values: `fillna`, `dropna`
- Summary: `sum`, `mean`, `min`, `max`, `unique`, `value_counts`
- Serialization: `to_list`, `to_dict`

### `GroupBy`

Constructed via `dataframe.groupby(by)`.

Methods:

- `agg(spec)`
- `count(columns?)`
- `sum(columns?)`
- `mean(columns?)`

## Top-Level Functions

From `bun_panda`:

- `read_csv(path, options?)`
- `read_csv_sync(path, options?)`
- `parse_csv(text, options?)`
- `to_csv(dataframe, options?)`
- `concat(frames, options?)`
- `merge(left, right, options)`

## Notes

1. API naming intentionally mirrors pandas where practical.
2. Not all pandas features are implemented in `v0.1.0`.
