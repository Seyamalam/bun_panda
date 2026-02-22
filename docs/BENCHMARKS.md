# Benchmarks

`bun_panda` includes a benchmark harness that compares core operations against Arquero.

## Run

```bash
bun run bench
```

Optional tuning:

```bash
BUN_PANDA_BENCH_ROWS=50000 BUN_PANDA_BENCH_ITERS=20 bun run bench
```

IO parser benchmark:

```bash
bun run bench:io
```

Stability tuning:

```bash
BUN_PANDA_BENCH_ROUNDS=5 bun run bench
```

Pandas companion benchmark:

```bash
python -m pip install -r bench/requirements.txt
python bench/pandas_compare.py
bun run bench:compare:pandas
```

## Current Cases

The current harness runs `73` cases across five datasets:

1. `base`
2. `skewed`
3. `wide`
4. `high_card`
5. `missing`

Separate IO harness runs `20` parser-focused cases across CSV, TSV, and JSON payload shapes.

Case families include:

1. `groupby` mean/sum/count variants
2. top-k `sort_values` (single and multi-column)
3. filtered top-k sort workloads
4. `value_counts` with subset/limit/normalize/dropna variants
5. high-cardinality and missing-value stress cases

## Example Output

```text
# bun_panda benchmark
rows=25000, iterations=8

| case | dataset | bun_panda avg | arquero avg | delta | ratio (bun/aq) |
| --- | --- | ---: | ---: | ---: | ---: |
| groupby_mean | base | 3.30ms | 5.02ms | -1.72ms | 0.66x |
| sort_multicol_top800 | base | 3.50ms | 11.61ms | -8.11ms | 0.30x |
| value_counts_high_card_user_city_top100 | high_card | 25.93ms | 46.51ms | -20.58ms | 0.56x |
```

## Methodology Notes

1. For operation benchmarks, both libraries reuse a pre-built in-memory table/frame.
2. Cases measure operation result row counts (`shape[0]` / `numRows()`) to avoid adding object materialization costs.
3. Top-N sort cases use `sort_values(..., ..., limit)` in `bun_panda` to benchmark partial-sort behavior.
4. Top-N count cases use `value_counts({ ..., limit })` in `bun_panda`.
5. Normalize/dropna variants are included to exercise counting semantics, not just raw speed.
6. CI regression gate enforces `ratio (bun/aq) <= 1.05` for benchmark cases.
7. CI also enforces:
   - IO headline parser ceilings via `bench/assert-io-regression.js`
   - tracked pandas ratio ceilings via `bench/assert-pandas-regression.js`
8. CI can refresh the README benchmark snapshot from generated JSON outputs.
9. Reported benchmark time is median-of-rounds to reduce run-to-run noise.

## Notes

1. This is an early performance snapshot for development guidance.
2. Results vary by CPU, runtime version, and dataset shape.
3. Use multiple runs and larger row counts before making optimization decisions.
