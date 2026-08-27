# Matched competitor study

This study compares bun_panda 0.4.0 with Arquero 8.0.3, Danfo.js 1.2.0,
nodejs-polars 0.26.0, and DuckDB-Wasm v1.5.4 from npm package
`@duckdb/duckdb-wasm@1.33.1-dev57.0`.

The systems execute the same deterministic inputs and four shared operations:
grouped sum, numeric filter with top-100 ordering, group counts, and an inner
join. Each adapter returns a small canonical relation. The worker compares its
SHA-256 digest with a separate TypeScript reference before it prints a timing.

Run a short validation:

```sh
BUN_PANDA_COMPETITOR_SIZES=10000 \
BUN_PANDA_COMPETITOR_PROCESSES=1 \
BUN_PANDA_COMPETITOR_WARMUPS=1 \
BUN_PANDA_COMPETITOR_ITERATIONS=2 \
BUN_PANDA_COMPETITOR_SCOPES=operation \
bun run paper/artifact/competitors/run-study.ts
```

The default run uses two row counts and five fresh processes per cell. For the
submission run, use at least 20 processes per cell on the x86-64 Linux host and
retain the raw observations.
