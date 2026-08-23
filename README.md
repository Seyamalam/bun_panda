# bun_panda

`bun_panda` is a pandas-inspired TypeScript library for Bun/JS runtimes.

The goal is API familiarity first, so JS/TS developers can use dataframe workflows without learning a new mental model.

## Why This Library

- Familiar naming:
- `DataFrame`
- `Series`
- `read_csv`, `read_table`, `read_tsv`, `read_json`, `read_parquet`, `read_excel`, `concat`, `merge`, `pivot_table`
- `head`, `tail`, `iloc`, `loc`, `groupby`, `agg`, `dropna`, `fillna`, `sort_values`, `sample`, `rank`, `where`, `mask`, `transform`, `insert`, `pop`, `round`, `abs`, `cumsum`, `duplicated`, `equals`
- `value_counts`, `sort_index`, `drop_duplicates`, `dtypes`, `astype`, `apply`, `applymap`, `map`, `isin`, `clip`, `replace`, `to_parquet`, `to_excel`
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
bun test                  # includes wasm kernel parity tests (src/wasm/*)
bun run typecheck
bun run check
bun run build:wasm        # rebuild crates/core -> src/wasm/bun_panda_core.wasm
BUN_PANDA_WASM=0 bun test # force the pure-TS groupby path
```
bun run bench
bun run bench:io
bun run bench:gate
bun run bench:gate:io
bun run bench:pandas
bun run bench:compare:pandas
bun run bench:gate:pandas
python -m pip install -r bench/requirements.txt
python bench/pandas_compare.py
```

Current suite: `86` tests for dataframe ops, merge modes, pivoting, dtypes, compatibility helpers, and CSV/TSV/JSON/Parquet/Excel IO edge cases.
Benchmark suite: `82` comparative cases against Arquero (`bun run bench`).

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
bun_panda vs Arquero: faster or equal in 71/82 cases.
bun_panda vs pandas: faster or equal in 5/10 tracked cases.

#### bun_panda vs Arquero (headline cases)

| case | dataset | bun_panda avg | arquero avg | ratio (bun/aq) |
| --- | --- | ---: | ---: | ---: |
| groupby_mean | base | 1.65ms | 2.34ms | 0.70x |
| filter_sort_top100 | base | 0.41ms | 1.00ms | 0.41x |
| sort_top1000 | base | 2.58ms | 3.56ms | 0.72x |
| sort_multicol_top800 | base | 3.66ms | 6.93ms | 0.53x |
| value_counts_city | base | 0.20ms | 2.37ms | 0.09x |
| value_counts_group_city_top10 | base | 0.90ms | 3.90ms | 0.23x |
| value_counts_missing_city_dropna_false | missing | 0.31ms | 1.23ms | 0.25x |
| value_counts_high_card_city_top20 | high_card | 5.40ms | 10.69ms | 0.51x |
| value_counts_high_card_user_top100 | high_card | 3.01ms | 8.18ms | 0.37x |

#### bun_panda vs pandas

| case | dataset | bun_panda avg | pandas avg | ratio (bun/pd) |
| --- | --- | ---: | ---: | ---: |
| groupby_mean | base | 1.65ms | 1.13ms | 1.46x |
| filter_sort_top100 | base | 0.41ms | 0.93ms | 0.44x |
| sort_top1000 | base | 2.58ms | 0.31ms | 8.27x |
| sort_multicol_top800 | base | 3.66ms | 3.00ms | 1.22x |
| value_counts_city | base | 0.20ms | 0.86ms | 0.24x |
| value_counts_group_city_top10 | base | 0.90ms | 1.41ms | 0.64x |
| value_counts_missing_city_dropna_false | missing | 0.31ms | 0.46ms | 0.66x |
| groupby_missing_city_mean | missing | 1.11ms | 0.37ms | 3.04x |
| value_counts_high_card_city_top20 | high_card | 5.40ms | 2.61ms | 2.07x |
| value_counts_high_card_user_top100 | high_card | 3.01ms | 8.90ms | 0.34x |

<!-- BENCHMARKS:END -->

## Status

This is an early library release (`0.2.0`). The API is intentionally pandas-like but not pandas-complete yet.
A Rust/WASM core (`crates/core` → `src/wasm/bun_panda_core.wasm`) powers numeric groupby aggregations and the single-column numeric `sort_values` / `filter` paths; `BUN_PANDA_WASM=0` opts back into pure TS. The columnar store in `src/wasm/columns.ts` (`Float64Array` with NaN = missing) feeds one fused `bp_agg_multi_f64` call per agg spec.

### pandas API parity: ~27%

Measured against the official pandas API reference (505 public methods/functions across DataFrame, Series, GroupBy, and top-level functions) — see [docs/PARITY.md](docs/PARITY.md) for the full per-surface breakdown and the exact list of what's missing. Regenerate with `bun run parity`.

## License

MIT. See `LICENSE`.
