# Workload inputs

The primary suite uses deterministic synthetic relations and needs no external
data. The public-data check uses UCI Bank Marketing, dataset 222, under CC BY
4.0. UCI reports 45,211 records and assigns DOI 10.24432/C5K306.

Prepare and verify the public input:

```sh
bun run paper/workloads/prepare-public.ts
```

Run the matched five-system study on all public records:

```sh
BUN_PANDA_COMPETITOR_DATASET=uci_bank \
BUN_PANDA_COMPETITOR_SIZES=45211 \
BUN_PANDA_COMPETITOR_OUTPUT=paper/data/competitor-uci-bank.json \
bun run paper/artifact/competitors/run-study.ts
```

The manifest pins the download, nested archive, and extracted CSV by SHA-256.
The generated data directory is reproducible and does not need to be included
in the npm package.
