# Humanizer and unslop audit

## Draft audit

The technical draft had several obvious machine-writing liabilities:

- stale claims still reported 87.4% conformance, 314 mismatches, and unmeasured browser timing after the underlying evidence had changed;
- broad phrases such as "competitive counting and filtering" hid the public workload's contrary ranking;
- comparison scopes were easy to collapse into one favorable headline;
- the prose listed many capabilities without distinguishing implemented behavior, kernel-only browser support, and future backends;
- repeated summary language made the discussion sound more certain than the process and context counts allowed.

## Changes made

- Replaced every stale semantic and browser claim with generated values.
- Reported the single remaining merge-order case precisely.
- Added the UCI result that changes the synthetic ranking.
- Separated operation-only from load-plus-operation timing and stated why DuckDB-Wasm construction is expensive in the measured scope.
- Kept negative findings for joins, Firefox, filtering, large top-k selection, RAM limits, out-of-core processing, and GPU support.
- Rewrote the abstract, introduction, contribution list, results, threats, and conclusion around bounded claims.
- Removed em-dash punctuation from manuscript prose. Page ranges remain ordinary Springer bibliography ranges generated from ASCII source.
- Preserved the author's direct, neutral academic voice without promotional language or acceptance claims.

## Final self-audit

**What would still make this look obviously AI-generated?** Excessive completeness, repeated numerical recaps, and a generic claim that every new experiment strengthens the system. The final draft counters those signals by keeping the public-data reversal, reporting browser uncertainty with intervals, and ending with concrete missing evidence rather than a triumphal summary.

The remaining density is methodological rather than decorative: each number has a raw artifact, regeneration command, and stated scope.
