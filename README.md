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
bun_panda vs pandas: faster or equal in 5/10 tracked cases.

#### bun_panda vs Arquero (headline cases)

| case | dataset | bun_panda avg | arquero avg | ratio (bun/aq) |
| --- | --- | ---: | ---: | ---: |
| groupby_mean | base | 1.75ms | 3.76ms | 0.47x |
| filter_sort_top100 | base | 0.32ms | 1.41ms | 0.23x |
| sort_top1000 | base | 2.09ms | 4.21ms | 0.50x |
| sort_multicol_top800 | base | 2.44ms | 6.74ms | 0.36x |
| value_counts_city | base | 0.46ms | 3.26ms | 0.14x |
| value_counts_group_city_top10 | base | 0.83ms | 5.14ms | 0.16x |
| value_counts_missing_city_dropna_false | missing | 1.80ms | 4.70ms | 0.38x |
| value_counts_high_card_city_top20 | high_card | 18.57ms | 36.27ms | 0.51x |
| value_counts_high_card_user_top100 | high_card | 6.76ms | 19.46ms | 0.35x |

#### bun_panda vs pandas

| case | dataset | bun_panda avg | pandas avg | ratio (bun/pd) |
| --- | --- | ---: | ---: | ---: |
| groupby_mean | base | 1.75ms | 0.84ms | 2.09x |
| filter_sort_top100 | base | 0.32ms | 0.77ms | 0.41x |
| sort_top1000 | base | 2.09ms | 1.74ms | 1.20x |
| sort_multicol_top800 | base | 2.44ms | 2.52ms | 0.97x |
| value_counts_city | base | 0.46ms | 0.57ms | 0.80x |
| value_counts_group_city_top10 | base | 0.83ms | 1.10ms | 0.76x |
| value_counts_missing_city_dropna_false | missing | 1.80ms | 0.92ms | 1.97x |
| groupby_missing_city_mean | missing | 1.42ms | 1.07ms | 1.32x |
| value_counts_high_card_city_top20 | high_card | 18.57ms | 7.61ms | 2.44x |
| value_counts_high_card_user_top100 | high_card | 6.76ms | 12.40ms | 0.55x |

<!-- BENCHMARKS:END -->

## Status

This is an early library release (`0.1.5`). The API is intentionally pandas-like but not pandas-complete yet.

## License

MIT. See `LICENSE`.
