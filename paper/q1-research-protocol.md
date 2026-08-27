# Evaluation and artifact protocol for a stronger `bun_panda` systems paper

**Status:** implementation-ready protocol, checked 2026-08-26. Following it cannot guarantee acceptance or a journal quartile; it addresses the present paper's largest evidence gaps. All external sources below are standards, official project documentation, or original research.

## 1. Evidence contract

The revised paper should make one central, falsifiable claim:

> A representation-aware dispatcher can predict when a dataframe operation should remain in TypeScript or cross into WebAssembly, reducing end-to-end time without changing the declared pandas-inspired semantics.

Freeze the evaluated commit, versions, hypotheses, workload manifest, exclusion rules, and analysis script **after a pilot and before the main run**. Keep pilot and main data separate. Never replace a preregistered primary metric after seeing results.

Use six evidence gates:

1. **Semantic:** name coverage, callable/signature coverage, and behavioral agreement are reported separately.
2. **Performance:** every principal comparison has independent process replication and a 95% interval for the effect size.
3. **Architecture:** TypeScript, always-Wasm, current/static dispatch, proposed adaptive dispatch, and an offline oracle are ablated.
4. **Memory:** comparable process-level peak memory and maximum successful scale are measured; internal heap figures are diagnostic only.
5. **Browser:** browser capability is claimed only after a browser-safe package is tested in real browser engines.
6. **Artifact:** every quantitative table and figure can be regenerated from immutable raw samples in an archival release.

These requirements follow the original managed-runtime benchmarking results of [Georges, Buytaert, and Eeckhout](https://doi.org/10.1145/1297027.1297033), the hierarchical uncertainty model of [Kalibera and Jones](https://doi.org/10.1145/2464157.2464160), and the finding by [Barrett et al.](https://doi.org/10.1145/3133876) that JIT warm-up does not reliably converge to a simple peak steady state.

## 2. Systems and experimental controls

### Required configurations

Evaluate, from the same frozen input fixtures:

| Role | Configuration |
|---|---|
| Semantic reference | CPython + pinned pandas 3.0.5 |
| JavaScript baseline | Pinned Arquero; add Danfo.js where an operation and output contract can be matched |
| Analytical Wasm baseline | Pinned DuckDB-Wasm for relational/query cases, explicitly labelled as a SQL engine rather than a dataframe API |
| `bun_panda` baselines | forced TypeScript; always-Wasm where eligible; current static dispatch |
| Proposed system | typed-column cache + buffer reuse + adaptive dispatcher |
| Upper bound | offline per-cell oracle choosing the faster of TypeScript and Wasm after measurement |

Add Node/V8 only after a compatibility smoke test and only if the package has a supported Node loader. A cross-runtime pandas comparison is legitimate, but it must not be presented as isolating language or kernel quality.

### Machines and controls

The minimum defensible matrix is one physical arm64 macOS machine and one physical x86-64 Linux machine; a third independent x86-64 machine is preferred. Run the comparable memory study on Linux. Record CPU model and microcode, physical/logical cores, RAM, storage, OS/kernel, power mode/governor, swap, thermal state, runtime and compiler versions, package lock hash, build flags, thread counts, and Git commit.

Use a quiet machine, AC power, fixed performance policy where available, and one benchmark process at a time. Randomize system order within workload/scale blocks and balance order across repetitions. Mytkowicz et al. showed that seemingly innocuous environment and layout changes can reverse benchmark conclusions in ["Producing Wrong Data Without Doing Anything Obviously Wrong!"](https://doi.org/10.1145/1508244.1508275). Do not pool absolute latencies from different machines. Report each machine, then combine within-machine ratios only if their direction and magnitude are consistent.

## 3. Statistical benchmarking for JIT runtimes

### Pilot

For every runtime and a representative cell from each workload family:

1. Start 10 fresh processes.
2. Record every in-process iteration, including initialization, for up to 200 iterations.
3. Apply the changepoint procedure and classifications from [Barrett et al.](https://doi.org/10.1145/3133876), or publish an equivalent pre-specified implementation.
4. Estimate within-process and between-process variance as distinct levels, following [Kalibera and Jones](https://doi.org/10.1145/2464157.2464160).
5. Before the main experiment, freeze a family-specific warm-up rule and a process count chosen to target a 95% effect-size interval with at most 5% relative half-width. Use at least 20 and at most 60 fresh processes; if the target is not reached at the cap, report that uncertainty rather than silently adding runs.

If a configuration does not stabilize in the pilot, do not discard an arbitrary prefix and call the remainder steady state. Report cold/start-up and whole-process distributions for that configuration.

### Main run

- The independent experimental unit is a **fresh process**, not an in-process loop.
- Within each process, execute the frozen warm-up rule, then at least 20 measured iterations or enough iterations to accumulate one second of measured work, whichever is larger. Cap iterations in advance.
- Recreate mutable inputs between iterations. Consume a deterministic output digest so no system can avoid materialization.
- Include representation conversion, Wasm copies, dispatch, and result construction in the primary operation latency. Report kernel-only timing only as a secondary decomposition.
- Measure cold import/initialization separately from warmed operation latency.
- Retain all successful observations. Exclude only a logged, externally verifiable interruption under a rule frozen before the main run; publish exclusions and failures.

### Analysis and reporting

For each workload cell, report process-level means and medians with 95% intervals. Make the ratio of means the primary speedup estimate. Compute its hierarchical bootstrap interval by resampling processes first and iterations second. Also publish empirical CDFs or violin/box plots for representative families. A "win" requires the entire 95% ratio interval to be on the favorable side of 1.0. Otherwise, report the case as unresolved. If hypothesis tests are added, correct within each declared family and keep effect sizes and intervals primary.

For the dispatcher, report:

- prediction accuracy and false-Wasm/false-TypeScript counts;
- latency regret versus the offline oracle;
- crossover-row prediction error;
- calibration/training cost;
- benefit from typed-column caching and buffer reuse as separate ablations; and
- results on held-out shapes and workloads that were excluded from calibration.

Publish the complete per-case results. A geometric mean can summarize a predeclared family, but it must never replace individual ratios and uncertainty.

## 4. Dataframe workload coverage

The current 25,000-row synthetic suite is insufficient. Generate a versioned manifest that covers the following factors with a pairwise-covering design plus explicit boundary cases; do not cherry-pick cells after measuring them.

| Factor | Required levels |
|---|---|
| Rows | 1k, 10k, 100k, 1M, then geometric growth for the memory-limit study |
| Width | 4, 16, 64 columns |
| Type mix | numeric; string-heavy; boolean/categorical; datetime; mixed |
| Missingness | 0%, 1%, 10%, 50%, including missing keys |
| Group cardinality | 10, approximately sqrt(n), 10% of n, nearly unique |
| Distribution/order | uniform; 90/10 skew; sorted; reverse-sorted; random |
| Join relationship | one-to-one, one-to-many, many-to-many, matched and unmatched keys |
| Requested output | full result; top-k with several k/n ratios; scalar/small aggregation |

Required operation families are construction and conversion, projection/alignment, arithmetic, filtering, full sort and top-k, value counts, group-by reductions, joins/merges, reshape/pivot, rolling/window, string, categorical, datetime/timezone, missing-data operations, and CSV/JSON/Parquet I/O. Report unsupported cells; do not substitute an easier operation.

Add three external-validity layers:

1. Adapt the group-by and join shapes from the authors' [H2O.ai `db-benchmark`](https://github.com/h2oai/db-benchmark), while labelling any changes.
2. Implement every feasible query from [TPC-H 3.0.1](https://www.tpc.org/TPC_Documents_Current_Versions/pdf/TPC-H_v3.0.1.pdf). Because dataframe translations change the official query language and execution conditions, call these **TPC-H-derived pipelines**, never compliant TPC-H results. TPC publishes the [current specification and tools](https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp).
3. Match a subset of operations in pandas' official [ASV benchmark suite](https://github.com/pandas-dev/pandas/blob/main/web/pandas/community/benchmarks.md), pinning the exact suite revision and input contract.
4. Use at least two immutable, citable real datasets. Choose one mixed-type dataset with missing values and one wide numeric dataset. Archive exact inputs or retrieval hashes and preprocessing scripts.

For Wasm boundary analysis, separately time typed-column construction/cache hit, allocation, copy-in, kernel, copy-out, and result materialization. Apache Arrow's [columnar format specification](https://arrow.apache.org/docs/format/Columnar.html) provides the relevant buffer/type vocabulary; it does not by itself prove that a path is zero-copy.

## 5. Semantic differential conformance

Freeze pandas 3.0.5 as the oracle and archive the exact [public API reference](https://pandas.pydata.org/docs/reference/) used to generate the inventory. The original differential-testing method compares independently implemented programs on the same generated inputs and investigates disagreements ([McKeeman 1998](https://www.cs.tufts.edu/comp/150FP/archive/bill-mckeeman/DifferentailTesting.pdf)).

### Test generator

Create a language-neutral case format with explicit tags for integer width, float values including NaN and infinities, strings, booleans, null kinds, datetimes/timezones, categoricals, labels, duplicate labels, and index structure. The official [dataframe interchange protocol](https://data-apis.org/dataframe-protocol/latest/) defines a useful vocabulary for dtype, nulls, buffers, chunks, and copy behavior. Use recorded seeds and shrink failing cases. For every method claimed as supported, generate at least 200 valid cases and 50 invalid/boundary cases, supplemented by hand-written regressions for every discovered mismatch.

Cover:

- empty, singleton, duplicated, monotonic, and non-monotonic indexes;
- duplicate columns and labels with different scalar types;
- integer, floating, boolean, string/object, categorical, datetime, timezone, and mixed frames;
- missing values in data and keys;
- stable ordering and tie behavior;
- exceptions/warnings and invalid argument combinations;
- mutation, views/copies, aliasing, and input preservation; and
- MultiIndex and extension-like behavior only where the manuscript claims support.

### Comparison contract

Compare values, shape, row and column labels, label order, dtype, missing-value mask, categorical metadata, datetime unit/timezone, output ordering, exception category, warning category, and mutation state. Use exact equality for integers, strings, booleans, labels, masks, and ordering. For floating reductions, freeze operation-specific tolerances and publish absolute/relative error; pandas' official [`assert_frame_equal`](https://pandas.pydata.org/docs/reference/api/pandas.testing.assert_frame_equal.html) exposes the dimensions that its own tests compare, including dtypes, labels, categoricals, frequency, exactness, and tolerances.

Run two differential layers:

1. `bun_panda` versus pandas for declared shared semantics.
2. forced-TypeScript versus Wasm versus adaptive dispatch for every accelerated case; these outputs must agree before timing is accepted.

Add metamorphic properties where no exact pandas analogue exists: sort idempotence, partition/concatenation reconstruction, permutation invariance for declared commutative reductions, and serialize/deserialize round trips. Publish a machine-readable mismatch ledger with the categories `bug`, `unsupported`, `intentional divergence`, `oracle ambiguity`, and `harness limitation`. The paper must state denominators for API names, signatures, generated cases, and passed behaviors separately.

## 6. Memory and maximum-scale protocol

Use Linux cgroup v2 as the cross-runtime primary measurement surface. The kernel defines `memory.current`, the hard limit `memory.max`, and resettable high-water mark `memory.peak` in its [authoritative cgroup v2 documentation](https://www.kernel.org/doc/html/latest/admin-guide/cgroup-v2.html).

For each system/workload/scale, use a fresh cgroup and process and record:

1. baseline after runtime import;
2. resident dataframe after construction;
3. `memory.peak` during the operation;
4. retained memory after result release and an explicitly labelled full-GC diagnostic; and
5. output cardinality/bytes so a system returning less data cannot appear more efficient.

Report absolute peak bytes, incremental peak above imported-runtime baseline, and bytes per input/output row. Repeat in at least 20 fresh processes and give 95% intervals. Keep the same cgroup scope, input ownership, result materialization, thread count, and cache state across systems.

Collect runtime-specific diagnostics without treating them as directly comparable totals: Bun exposes JavaScriptCore heap and process statistics through [`bun:jsc`](https://bun.sh/docs/project/benchmarking); Node defines RSS, heap, external, and ArrayBuffer fields in [`process.memoryUsage()`](https://nodejs.org/api/process.html). Instrument Wasm `memory.buffer.byteLength`, page-growth count, copied input/output bytes, cache bytes, and cache evictions. The [Wasm JavaScript API](https://www.w3.org/TR/wasm-js-api-1/) defines linear `Memory` and its buffer.

For capacity, test fixed `memory.max` limits of 2, 4, and 8 GiB with swap disabled, growing rows geometrically until success, timeout, or cgroup OOM. Record the largest successful input, wall time, peak memory, and failure mode. This can support a bounded "maximum in-memory scale" claim. It cannot support "larger than memory" or out-of-core claims unless a streaming/spill execution path is implemented and tested.

## 7. Browser and WebAssembly evaluation

Browser claims require a separate browser-safe ESM entry: no Bun/Node imports in the reachable graph, asynchronous Wasm loading, and a dedicated-worker path. The WHATWG [Web Workers specification](https://html.spec.whatwg.org/dev/workers.html) defines background execution and transferable buffers; the W3C [Wasm Web API](https://www.w3.org/TR/wasm-web-api-1/) defines streaming compilation/instantiation.

Test pinned stable Chromium, Firefox, and WebKit/Safari on the same machine. Use the official [Tachometer](https://github.com/google/tachometer) runner to round-robin candidates. Start with its default minimum of 50 browser samples and freeze its precision/effect stopping rule before the main run. Run cold-cache and warm-cache experiments separately. Keep headless/headed mode fixed and disclose it. Evaluate main-thread and dedicated-worker modes, with cloning and transferable `ArrayBuffer` variants.

Measure these phases separately with W3C [User Timing](https://www.w3.org/TR/user-timing/):

- JavaScript and Wasm transfer bytes/time;
- module parse/evaluation;
- Wasm compile and instantiate;
- worker startup;
- input serialization/transfer or clone;
- first operation;
- warmed operation;
- return transfer and result materialization; and
- end-to-end user-visible completion.

Use [Resource Timing](https://www.w3.org/TR/resource-timing/) for encoded, decoded, and transfer sizes. Record raw, gzip, and Brotli bytes for JavaScript, Wasm, worker, and optional adapters. Observe main-thread tasks over 50 ms with the W3C [Long Tasks API](https://www.w3.org/TR/longtasks-1/) where supported; report support gaps rather than fabricating a cross-browser value.

Compare browser-safe `bun_panda` TypeScript/Wasm, Arquero, Danfo.js, DuckDB-Wasm, and Pyodide+pandas on the semantically matched subset. Separate package/runtime download from operation latency: Pyodide's larger runtime is a deployment result, not a kernel-latency penalty after warm-up.

Browser memory is secondary and engine-specific. In Chromium, use the WICG [`measureUserAgentSpecificMemory`](https://github.com/WICG/performance-measure-memory) proposal under the required cross-origin isolation and sample repeatedly; the proposal itself warns that individual calls depend on page events and garbage collection. Do not compare that number directly with Linux cgroup peaks or imply three-engine support.

GPU acceleration should remain future work unless real WebGPU kernels, transfer costs, device-memory limits, CPU/TypeScript/Wasm baselines, correctness tolerances, and break-even sizes are all implemented and measured.

## 8. Archival artifact and reproducibility

Target ACM's `Artifacts Evaluated-Functional`, `Artifacts Evaluated-Reusable`, `Artifacts Available`, and, after an independent run, `Results Reproduced` criteria. ACM requires functional artifacts to be documented, consistent, complete, and exercisable. Availability requires a public archival repository and persistent identifier, rather than a personal page or mutable Git branch ([ACM SIGSIM artifact guidance](https://sigsim.acm.org/conf/pads/2024/blog/artifact-evaluation/)).

The release must contain:

- immutable Git tag and commit; published npm tarball and its integrity hash;
- Zenodo or equivalent archive with a version DOI and a reserved DOI inserted into the manuscript;
- `CITATION.cff`, SPDX-compatible license, `SHA256SUMS`, dependency lockfiles, and toolchain pins;
- complete source plus the exact generated JS/Wasm evaluated;
- workload generators, frozen manifests, real-data hashes, and preprocessing;
- every raw process/iteration sample, failures, exclusions, and environment record;
- one script per quantitative table/figure and a claim-to-artifact map;
- a quick smoke reproduction and a full one-command reproduction, with expected time, RAM, disk, and outputs; and
- a clean-machine reproduction report written by someone other than the author.

[Zenodo's official GitHub integration](https://support.zenodo.org/help/en-gb/24-github-integration) supports archiving releases and reserving a DOI. The official [Citation File Format](https://citation-file-format.github.io/) makes software/version/authorship/DOI metadata machine-readable and is consumed by GitHub and Zenodo. Also archive the source with Software Heritage and record its content-addressed [SWHID](https://docs.softwareheritage.org/devel/swh-model/persistent-identifiers.html).

Before deposit, reproduce from a clean Linux environment with no uncommitted files and no global dependencies beyond those named in the README. Verify raw-data hashes, regenerate every table/figure, compare generated values with the manuscript, and store the verification log. Archive any macOS/browser automation separately with pinned browser versions and screenshots/logs because those portions cannot be made fully equivalent by a Linux container.

## 9. Completion gates for the revised paper

The evaluation is ready to support a substantially stronger submission only when all are true:

- [ ] The adaptive dispatcher and its cost/representation model exist and beat both static dispatch and single-backend baselines on held-out cases with bounded regret.
- [ ] The semantic ledger reports behavioral agreement, keeps the 505-name census as a separate metric, and every accelerated result passes the TypeScript/Wasm differential oracle.
- [ ] Main results use independent fresh-process replication on arm64 macOS and x86-64 Linux with 95% effect-size intervals.
- [ ] The workload manifest includes scales, types, missingness, cardinality, skew, join multiplicity, standardized derived workloads, and real datasets.
- [ ] Comparable Linux peak-memory and fixed-limit capacity results replace all speculative RAM/large-data claims.
- [ ] Browser execution is real, measured in three engines, and separates transfer, initialization, copies, compute, responsiveness, and memory limitations.
- [ ] Every headline claim maps to immutable raw data and a regeneration script.
- [ ] The npm package, evaluated Git tag, archival DOI, citation metadata, and independent clean-machine reproduction are public before submission.

If a gate is not met, narrow the corresponding claim. Keep negative results such as joins losing, no browser support, or Wasm losing after copies. They define the architecture's actual boundary.
