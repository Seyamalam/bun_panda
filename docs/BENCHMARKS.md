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

## Current Cases

1. `groupby_mean`
2. `construct_only`
3. `groupby_mean_2keys`
4. `filter_sort_top100`
5. `filter_sort_multicol_top200`
6. `value_counts_city`
7. `value_counts_group_city`
8. `drop_duplicates_group_city`
9. `skewed_groupby_mean`
10. `wide_groupby_sum`
11. `wide_filter_sort`

## Example Output

```text
# bun_panda benchmark
rows=25000, iterations=12

| case | dataset | bun_panda avg | arquero avg | delta | ratio (bun/aq) |
| --- | --- | ---: | ---: | ---: | ---: |
| construct_only | base | 4.78ms | 1.55ms | +3.23ms | 3.08x |
| groupby_mean | base | 6.27ms | 6.08ms | +0.19ms | 1.03x |
| wide_filter_sort | wide | 12.52ms | 5.84ms | +6.68ms | 2.14x |
```

## Methodology Notes

1. For operation benchmarks, both libraries reuse a pre-built in-memory table/frame.
2. `construct_only` isolates construction overhead.
3. Cases measure operation result row counts (`shape[0]` / `numRows()`) to avoid adding object materialization costs.
4. Top-N sort cases use `sort_values(..., ..., limit)` in `bun_panda` to benchmark partial-sort behavior.

## Notes

1. This is an early performance snapshot for development guidance.
2. Results vary by CPU, runtime version, and dataset shape.
3. Use multiple runs and larger row counts before making optimization decisions.
