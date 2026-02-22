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

## Development

```bash
bun test
bun run typecheck
bun run check
bun run bench
```

Current suite: `29` tests for dataframe ops, merge modes, pivoting, dtypes, and CSV edge cases.

## Documentation

- `docs/API.md`: current API surface and examples.
- `docs/FEATURES.md`: implemented features and parity notes.
- `docs/TODO.md`: prioritized backlog.
- `docs/BENCHMARKS.md`: benchmark harness and comparison notes.
- `SCOPE.md`: v1 product scope.
- `CONTRIBUTING.md`: contribution workflow.
- `SECURITY.md`: reporting vulnerabilities.
- `CHANGELOG.md`: release history.

CI: GitHub Actions workflow at `.github/workflows/ci.yml` runs typecheck + tests on push/PR, and supports manual benchmark runs.

## Status

This is an early library release (`0.1.0`). The API is intentionally pandas-like but not pandas-complete yet.

## License

MIT. See `LICENSE`.
