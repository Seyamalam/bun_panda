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
2. `filter_sort`
3. `value_counts_city`

## Example Output

```text
# bun_panda benchmark
rows=25000, iterations=12

| case | bun_panda avg | arquero avg | delta |
| --- | ---: | ---: | ---: |
| groupby_mean | 11.73ms | 6.27ms | +5.46ms |
| filter_sort | 8.36ms | 2.75ms | +5.61ms |
| value_counts_city | 7.62ms | 3.93ms | +3.69ms |
```

## Notes

1. This is an early performance snapshot for development guidance.
2. Results vary by CPU, runtime version, and dataset shape.
3. Use multiple runs and larger row counts before making optimization decisions.
