# bun_panda

`bun_panda` is a pandas-inspired TypeScript library for Bun/JS runtimes.

The goal is API familiarity first, so JS/TS developers can use dataframe workflows without learning a new mental model.

## Why This Library

- Familiar naming:
- `DataFrame`
- `Series`
- `read_csv`, `read_table`, `read_tsv`, `read_json`, `read_parquet`, `read_excel`, `concat`, `merge`, `pivot_table`
- `head`, `tail`, `iloc`, `loc`, `groupby`, `agg`, `dropna`, `fillna`, `sort_values`
- `value_counts`, `sort_index`, `drop_duplicates`, `dtypes`, `astype`, `to_parquet`, `to_excel`
- pandas-like options where practical (`groupby(..., { dropna, sort })`, `value_counts({ sort, ascending })`)
- more pandas-style helpers (`nunique`, `groupby(..., { as_index })`, `groupby().size()`)
- Lightweight, in-memory transforms for Bun + TypeScript.
- Fast local iteration and strong type checks.

## Installation

```bash
bun install
```

## Quick Start

```bash
bun run index.ts
```

```ts
import { DataFrame, read_csv_sync, merge } from "bun_panda";

const sales = new DataFrame([
  { id: 1, team: "A", amount: 100 },
  { id: 2, team: "A", amount: 150 },
  { id: 3, team: "B", amount: 90 },
]);

const byTeam = sales.groupby("team").agg({ amount: "mean" });
console.log(byTeam.to_records());
// [{ team: "A", amount: 125 }, { team: "B", amount: 90 }]

const users = read_csv_sync("./users.csv", { index_col: "id" });
const joined = merge(users.reset_index("id"), sales, { on: "id", how: "left" });
console.log(joined.head(5).to_records());
```

## bun_panda vs Arquero Example

Same analysis task in both libraries:

```ts
// bun_panda
import { DataFrame } from "bun_panda";

const out = new DataFrame(data)
  .query((row) => Boolean(row.active) && Number(row.value) > 300)
  .groupby(["group", "city"])
  .agg({ value: "mean", revenue: "sum" })
  .sort_values(["group", "city"])
  .to_records();
```

```ts
// Arquero
import * as aq from "arquero";

const op = aq.op;
const out = aq
  .from(data)
  .filter((d) => d.active && d.value > 300)
  .groupby("group", "city")
  .rollup({
    value: (d) => op.mean(d.value),
    revenue: (d) => op.sum(d.revenue),
  })
  .orderby("group", "city")
  .objects();
```

## Development

```bash
bun test
bun run typecheck
bun run check
bun run bench
bun run bench:io
python -m pip install -r bench/requirements.txt
python bench/pandas_compare.py
```

Current suite: `80` tests for dataframe ops, merge modes, pivoting, dtypes, and CSV/TSV/JSON/Parquet/Excel IO edge cases.
Benchmark suite: `73` comparative cases against Arquero (`bun run bench`).

## Documentation

- `docs/API.md`: current API surface and examples.
- `docs/FEATURES.md`: implemented features and parity notes.
- `docs/TODO.md`: prioritized backlog.
- `docs/BENCHMARKS.md`: benchmark harness and comparison notes.
- `SCOPE.md`: v1 product scope.
- `CONTRIBUTING.md`: contribution workflow.
- `SECURITY.md`: reporting vulnerabilities.
- `CHANGELOG.md`: release history.

CI: GitHub Actions workflow at `.github/workflows/ci.yml` runs typecheck/tests plus benchmark + regression gates on push/PR, and can auto-refresh benchmark snapshots on `workflow_dispatch`.

<!-- BENCHMARKS:START -->
### Automated Benchmark Snapshot

Generated from benchmark scripts (rows=25000, iterations=8).
bun_panda vs Arquero: faster or equal in 73/73 cases.
bun_panda vs pandas: faster or equal in 3/10 tracked cases.

#### bun_panda vs Arquero (headline cases)

| case | dataset | bun_panda avg | arquero avg | ratio (bun/aq) |
| --- | --- | ---: | ---: | ---: |
| groupby_mean | base | 3.70ms | 5.16ms | 0.72x |
| filter_sort_top100 | base | 0.53ms | 2.26ms | 0.23x |
| sort_top1000 | base | 10.13ms | 19.69ms | 0.51x |
| sort_multicol_top800 | base | 11.53ms | 34.97ms | 0.33x |
| value_counts_city | base | 1.00ms | 12.37ms | 0.08x |
| value_counts_group_city_top10 | base | 5.01ms | 22.24ms | 0.23x |
| value_counts_missing_city_dropna_false | missing | 0.81ms | 6.28ms | 0.13x |
| value_counts_high_card_city_top20 | high_card | 30.48ms | 49.36ms | 0.62x |
| value_counts_high_card_user_top100 | high_card | 19.06ms | 33.66ms | 0.57x |

#### bun_panda vs pandas

| case | dataset | bun_panda avg | pandas avg | ratio (bun/pd) |
| --- | --- | ---: | ---: | ---: |
| groupby_mean | base | 3.70ms | 1.09ms | 3.39x |
| filter_sort_top100 | base | 0.53ms | 1.39ms | 0.38x |
| sort_top1000 | base | 10.13ms | 2.29ms | 4.42x |
| sort_multicol_top800 | base | 11.53ms | 4.39ms | 2.62x |
| value_counts_city | base | 1.00ms | 0.81ms | 1.23x |
| value_counts_group_city_top10 | base | 5.01ms | 1.48ms | 3.39x |
| value_counts_missing_city_dropna_false | missing | 0.81ms | 1.32ms | 0.61x |
| groupby_missing_city_mean | missing | 2.74ms | 1.48ms | 1.86x |
| value_counts_high_card_city_top20 | high_card | 30.48ms | 13.00ms | 2.34x |
| value_counts_high_card_user_top100 | high_card | 19.06ms | 23.32ms | 0.82x |

<!-- BENCHMARKS:END -->

## Status

This is an early library release (`0.1.12`). The API is intentionally pandas-like but not pandas-complete yet.

## License

MIT. See `LICENSE`.
