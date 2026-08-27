# Fresh-process Wasm ablation

**Run date:** 2026-08-26

**Machine:** Apple M5 Pro, arm64 macOS

**Raw artifact:** `paper/data/fresh-process-ablation.json`

The local study contains 720 independently started Bun processes and 14,400
measured operation iterations. It uses 20 processes per
workload/scale/backend cell, 5 warm-up iterations and 20 measured iterations
inside each process, deterministic randomized cell order, paired input seeds,
and SHA-256 output equality. Intervals are 95% hierarchical percentile
bootstrap intervals from 2,000 process-first, iteration-second resamples.

The effect is `TypeScript mean / candidate mean`; values above one favor the
candidate. A win requires the complete interval to exceed one.

| workload | rows | forced Wasm speedup [95% CI] | result | adaptive speedup [95% CI] | result |
| --- | ---: | ---: | --- | ---: | --- |
| fused GroupBy (4 aggs, reused typed columns) | 10,000 | 1.523 [1.069, 2.306] | win | 1.636 [1.223, 2.436] | win |
| fused GroupBy (4 aggs, reused typed columns) | 100,000 | 1.442 [1.285, 1.642] | win | 1.476 [1.316, 1.657] | win |
| fused GroupBy (4 aggs, reused typed columns) | 250,000 | 1.599 [1.323, 1.903] | win | 1.433 [1.056, 1.866] | win |
| full numeric sort | 10,000 | 1.713 [1.471, 1.983] | win | 1.645 [1.387, 1.956] | win |
| full numeric sort | 100,000 | 1.669 [1.298, 2.087] | win | 1.861 [1.639, 2.165] | win |
| full numeric sort | 250,000 | 1.752 [1.370, 2.186] | win | 2.003 [1.687, 2.383] | win |
| numeric top-1,000 | 10,000 | 1.458 [1.182, 1.744] | win | 1.614 [1.339, 1.884] | win |
| numeric top-1,000 | 100,000 | 0.901 [0.737, 1.081] | unresolved | 0.963 [0.797, 1.134] | unresolved |
| numeric top-1,000 | 250,000 | 0.869 [0.594, 1.258] | unresolved | 1.289 [0.930, 1.760] | unresolved |
| boolean-mask filter | 10,000 | 0.199 [0.163, 0.241] | loss | 0.870 [0.735, 1.034] | unresolved |
| boolean-mask filter | 100,000 | 0.547 [0.459, 0.651] | loss | 1.136 [0.922, 1.395] | unresolved |
| boolean-mask filter | 250,000 | 0.308 [0.269, 0.358] | loss | 1.129 [0.982, 1.289] | unresolved |

These results support Wasm for full numeric sort and fused numeric GroupBy
when typed columns are already materialized. They support TypeScript for
filtering and for top-1,000 above 10,000 rows. The versioned dispatcher was
narrowed accordingly. The adaptive/TypeScript intervals for routes that both
select TypeScript cross one, as expected from independent-process noise; they
must not be reported as adaptive speedups.

This is one-machine evidence. It does not satisfy the planned x86-64 Linux
replication or comparable cgroup-v2 peak-memory/capacity study.
