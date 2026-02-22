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
2. `groupby_mean_2keys`
3. `filter_sort_top100`
4. `filter_sort_multicol_top200`
5. `value_counts_city`
6. `value_counts_group_city`
7. `drop_duplicates_group_city`
8. `skewed_groupby_mean`
9. `wide_groupby_sum`
10. `wide_filter_sort`

## Example Output

```text
# bun_panda benchmark
rows=25000, iterations=12

| case | dataset | bun_panda avg | arquero avg | delta | ratio (bun/aq) |
| --- | --- | ---: | ---: | ---: | ---: |
| groupby_mean | base | 7.67ms | 5.05ms | +2.62ms | 1.52x |
| filter_sort_top100 | base | 6.25ms | 2.10ms | +4.15ms | 2.98x |
| value_counts_city | base | 4.70ms | 2.65ms | +2.05ms | 1.77x |
```

## Notes

1. This is an early performance snapshot for development guidance.
2. Results vary by CPU, runtime version, and dataset shape.
3. Use multiple runs and larger row counts before making optimization decisions.
