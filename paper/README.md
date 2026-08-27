# bun_panda research manuscript

This directory contains a reproducible Springer Nature journal manuscript and its empirical artifact.

## Template provenance

- Official package: Springer Nature journal article LaTeX template, Version 3.1, December 2024.
- Source: <https://www.springernature.com/gp/authors/campaigns/latex-author-support>
- Preserved archive: `vendor/springer-nature-template-dec-2024.zip`
- Archive SHA-256: `812e76dcaa9c28dc1bff1fb6065d51729b67d4ea140552a05088317414a3ecae`
- Working class: `manuscript/sn-jnl.cls`
- Working reference style: `sn-basic,Numbered`. The selected journal's instructions override this choice.

## Reproduce the data

Run from the repository root:

```bash
bun run build:wasm
bun test
bun run paper/artifact/collect-environment.ts
bun run paper/artifact/run-wasm-ablation.ts
BUN_PANDA_BENCH_JSON=paper/data/arquero-25k.json bun run bench/compare.js
bench/.venv/bin/python bench/pandas_compare.py --rows 25000 --iters 8 --rounds 3 --json-out paper/data/pandas-25k.json
BUN_PANDA_BENCH_INPUT=paper/data/arquero-25k.json BUN_PANDA_PANDAS_INPUT=paper/data/pandas-25k.json BUN_PANDA_PANDAS_COMPARE_JSON=paper/data/pandas-compare-25k.json bun run bench/compare-pandas.js
bun run paper/artifact/analyze-package-size.ts
bun run paper/artifact/summarize-study.ts
```

The generated `manuscript/generated-results.tex` supplies headline values to `main.tex`. Raw observations remain in `data/`.

## Before journal submission

This is a submission-grade technical draft, not a guarantee of acceptance or journal quartile. Complete these venue-dependent items:

1. Choose the journal and apply its bibliography, word-count, structure, and data/code policies.
2. Confirm that the target journal accepts ``N/A'' for acknowledgements and funding, and retain the conflict-of-interest declaration.
3. Archive the evaluated commit and artifact under a persistent DOI.
4. Run multiple fresh-process replications on at least one additional architecture and add confidence intervals.
5. Flatten `generated-results.tex` into `main.tex` if the submission system requires a single-directory, single-source package.
6. Obtain an independent technical and language review.
