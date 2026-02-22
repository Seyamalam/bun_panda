# bun_panda

`bun_panda` is a pandas-inspired TypeScript library for Bun/JS runtimes.

The goal is API familiarity first, so JS/TS developers can use dataframe workflows without learning a new mental model.

## Why This Library

- Familiar naming:
- `DataFrame`
- `Series`
- `read_csv`, `concat`, `merge`, `pivot_table`
- `head`, `tail`, `iloc`, `loc`, `groupby`, `agg`, `dropna`, `fillna`, `sort_values`
- `value_counts`, `sort_index`, `drop_duplicates`, `dtypes`, `astype`
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
python -m pip install -r bench/requirements.txt
python bench/pandas_compare.py
```

Current suite: `60` tests for dataframe ops, merge modes, pivoting, dtypes, CSV edge cases, and core utility behavior.
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

Generated from benchmark scripts (rows=15000, iterations=6).
bun_panda vs Arquero: faster or equal in 73/73 cases.
bun_panda vs pandas: faster or equal in 3/10 tracked cases.

#### bun_panda vs Arquero (headline cases)

| case | dataset | bun_panda avg | arquero avg | ratio (bun/aq) |
| --- | --- | ---: | ---: | ---: |
| groupby_mean | base | 1.90ms | 3.29ms | 0.58x |
| filter_sort_top100 | base | 0.30ms | 1.43ms | 0.21x |
| sort_top1000 | base | 2.11ms | 4.23ms | 0.50x |
| sort_multicol_top800 | base | 6.48ms | 18.92ms | 0.34x |
| value_counts_city | base | 1.03ms | 7.05ms | 0.15x |
| value_counts_group_city_top10 | base | 2.18ms | 11.86ms | 0.18x |
| value_counts_missing_city_dropna_false | missing | 0.53ms | 4.05ms | 0.13x |
| value_counts_high_card_city_top20 | high_card | 19.50ms | 30.42ms | 0.64x |
| value_counts_high_card_user_top100 | high_card | 10.22ms | 17.77ms | 0.58x |

#### bun_panda vs pandas

| case | dataset | bun_panda avg | pandas avg | ratio (bun/pd) |
| --- | --- | ---: | ---: | ---: |
| groupby_mean | base | 1.90ms | 0.76ms | 2.49x |
| filter_sort_top100 | base | 0.30ms | 1.04ms | 0.29x |
| sort_top1000 | base | 2.11ms | 1.14ms | 1.85x |
| sort_multicol_top800 | base | 6.48ms | 2.14ms | 3.03x |
| value_counts_city | base | 1.03ms | 0.56ms | 1.85x |
| value_counts_group_city_top10 | base | 2.18ms | 0.99ms | 2.21x |
| value_counts_missing_city_dropna_false | missing | 0.53ms | 0.90ms | 0.59x |
| groupby_missing_city_mean | missing | 1.41ms | 1.00ms | 1.41x |
| value_counts_high_card_city_top20 | high_card | 19.50ms | 7.55ms | 2.58x |
| value_counts_high_card_user_top100 | high_card | 10.22ms | 12.38ms | 0.83x |

<!-- BENCHMARKS:END -->

## Status

This is an early library release (`0.1.6`). The API is intentionally pandas-like but not pandas-complete yet.

## License

MIT. See `LICENSE`.
