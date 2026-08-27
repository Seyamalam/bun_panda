# Research dossier for a `bun_panda` systems paper

Date checked: 2026-08-26 (Asia/Dhaka)

This note is an evidence map, not manuscript prose. It records which claims can be supported by which primary sources, how to frame a defensible contribution, and which empirical evidence must still be generated. A submission-quality paper cannot honestly contain invented benchmark values, fabricated citations, unverified “first” claims, or a claimed journal quartile without a named index, category, and year.

## 1. Current official Springer Nature journal template

The current official package is the **Springer Nature LaTeX authoring template**, also called the **Journal article LaTeX authoring template** in its manual. The package is **Version 3.1, December 2024**, and its document class is `sn-jnl.cls`.

- Official author-support page: <https://www.springernature.com/gp/authors/campaigns/latex-author-support>
- Official support article (modified 2024-11-26): <https://support.springernature.com/en/support/solutions/articles/6000250920-latex-template-package-for-article-book-submissions>
- Stable official download landing page: <https://www.springernature.com/gp/authors/campaigns/latex-author-support/see-where-our-services-will-take-you/18782940>
- Direct official ZIP (December 2024 package): <https://cms-resources.apps.public.k8s.springernature.io/springer-cms/rest/v1/content/18782940/data/v12>
- Overleaf entry linked by Springer Nature: <https://www.overleaf.com/latex/templates/springer-nature-latex-template/gsvvftmrppwq>

The `v12` suffix in the direct URL is the CMS asset revision, not the template version. The ZIP's `sn-article.tex` begins with `Version 3.1 December 2024`. The `sn-jnl.cls` file retains an old `2019/11/18 v0.1` class header; that header is not the release version of the package.

Springer Nature states that this generic journal template can be used for Springer, Nature Portfolio, and BMC journals, but it also states that the target journal's instructions take precedence. It is **not** the LNCS/conference-proceedings template.

### Bibliography choice

There is no universal “computer science” style in the package. The package includes:

| Class option | Bundled `.bst` | Mode |
|---|---|---|
| `sn-basic` | `sn-basic.bst` | author-year; add `Numbered` for numeric |
| `sn-mathphys-num` | `sn-mathphys-num.bst` | numeric |
| `sn-mathphys-ay` | `sn-mathphys-ay.bst` | author-year |
| `sn-aps` | `sn-aps.bst` | numeric |
| `sn-vancouver-num` | `sn-vancouver-num.bst` | numeric |
| `sn-vancouver-ay` | `sn-vancouver-ay.bst` | author-year |
| `sn-apa` | `sn-apacite.bst` | author-year |
| `sn-chicago` | `sn-chicago.bst` | author-year; `Numbered` also supported |
| `sn-nature` | `sn-nature.bst` | numeric, Nature Portfolio only |

For an untargeted computer-systems draft, `\documentclass[pdflatex,sn-mathphys-num]{sn-jnl}` is a reasonable working choice because it provides compact numbered citations and is the uncommented example in Version 3.1. `sn-basic,Numbered` is another defensible generic Springer choice. The final class option must be changed if the selected journal says otherwise. Use `sn-nature` only for a Nature Portfolio journal.

Other author-facing options include `referee` (double spacing), `lineno` (line numbers), `iicol` (two columns), and `pdflatex` (enabled by default). Springer Nature asks authors to compile without errors, avoid custom fonts, and—in some submission systems—submit a single-directory ZIP. The support page explicitly warns that subdirectories can cause figures to be omitted during submission conversion.

## 2. Defensible research framing

### Candidate problem statement

Scientific and data-engineering work in JavaScript/TypeScript lacks a compact, strongly typed dataframe layer that simultaneously targets pandas-like semantics, Bun-native deployment, and selectively accelerated Rust/WebAssembly kernels. Existing systems occupy different points in the design space: pandas supplies the reference user model in Python; Arquero offers a column-oriented fluent JavaScript query API; Danfo.js deliberately resembles pandas; and DuckDB-Wasm brings a SQL/OLAP engine to WebAssembly. The paper should test—not assume—whether `bun_panda` occupies a useful point among semantic fidelity, throughput, interoperability, startup cost, and distribution size.

### Research questions that can yield a real systems contribution

1. **RQ1 — Semantic fidelity:** How much of a frozen, explicitly defined pandas `Series`/`DataFrame` surface is implemented, and how often do outputs, labels, dtypes, missing-value behavior, ordering, and exceptions agree on shared inputs?
2. **RQ2 — Architectural value of WebAssembly:** Which operation classes benefit from Rust/Wasm kernels after boundary-conversion costs, and at what row counts, widths, and data-type mixes does acceleration break even?
3. **RQ3 — Performance and resource behavior:** How does `bun_panda` compare with pandas, Arquero, Danfo.js, and DuckDB-Wasm on semantically equivalent dataframe/data-wrangling workloads across input scale, group cardinality, sortedness, width, and missingness?
4. **RQ4 — Portability and interchange:** What Arrow IPC/Parquet/CSV/JSON capabilities work across Bun and browser-like WebAssembly embeddings, what copies occur, and which data types round-trip losslessly?
5. **RQ5 — Deployability:** What are the npm tarball, application bundle, JavaScript glue, Wasm module, compressed transfer, import/initialization latency, and peak-memory costs? How do eager and lazy Wasm loading differ?

### Contributions that would be credible if supported by measurements

- A layered TypeScript/Bun dataframe architecture with an explicit semantic/Wasm boundary.
- A reproducible pandas differential-conformance method that separates API-name coverage from behavioral parity.
- A controlled evaluation of when Wasm kernels help or hurt dataframe operations, including boundary and initialization costs.
- An interoperability and deployment study spanning Arrow, Parquet, package/bundle size, and cold-start behavior.
- A versioned artifact containing code, datasets/generators, raw samples, analysis scripts, environment manifests, and one-command reproduction.

Do not claim “the first pandas-compatible TypeScript dataframe,” “near-native performance,” “zero-copy,” “Q1,” or “full pandas compatibility” without evidence scoped to a specific search protocol, benchmark, data path, ranking database, and frozen pandas API version.

## 3. Primary-source evidence map

Each row says exactly what the source can support. Claims about `bun_panda` itself must come from its source, tests, generated parity report, package artifact, and newly collected experiments—not from these references.

| Source | Claims it supports | Claims it does **not** support |
|---|---|---|
| [pandas package overview, v3.0.5](https://pandas.pydata.org/docs/getting_started/overview.html) | pandas' main abstractions are one-dimensional `Series` and two-dimensional, heterogeneous, size-mutable `DataFrame`; pandas targets labeled/relational data. | Any claim that another library is behaviorally compatible. |
| [pandas public API reference, v3.0.5](https://pandas.pydata.org/docs/reference/) and [DataFrame](https://pandas.pydata.org/docs/reference/frame.html)/[Series](https://pandas.pydata.org/docs/reference/series.html) references | A reproducible, frozen census of documented public objects/methods and their signatures. | Behavioral parity merely from matching method names. |
| [pandas official citation page](https://pandas.pydata.org/about/citing.html) | Canonical pandas software and McKinney 2010 citations. | Current usage statistics or superiority. |
| [Petersohn et al., *Towards Scalable Dataframe Systems*, PVLDB 2020](https://www.vldb.org/pvldb/vol13/p2033-petersohn.pdf) | Dataframes have flexible semantics distinct from relational systems; pandas has a large, redundant API and eager operator-at-a-time execution; semantic and scalability issues are research problems. | Performance of `bun_panda` or current pandas releases. |
| [Petersohn et al., Modin, PVLDB 2021](https://doi.org/10.14778/3494124.3494152) | A pandas-compatible system can translate rich APIs into a smaller operator core; decomposition and metadata handling are key architecture concerns. | That `bun_panda` uses or matches Modin's techniques. |
| [TypeScript Handbook](https://www.typescriptlang.org/docs/handbook/intro) and [type compatibility](https://www.typescriptlang.org/docs/handbook/type-compatibility.html) | TypeScript is a static type checker layered on JavaScript and uses structural typing; its type system intentionally permits some unsound behavior. | Runtime correctness or memory safety of application code. |
| [Bun runtime documentation](https://bun.com/docs/runtime) | Bun executes JavaScript/TypeScript using JavaScriptCore and transpiles TypeScript; establishes the runtime under test. | Bun performance superiority unless independently reproduced; do not reuse Bun's marketing benchmark as a paper result. |
| [Bun file types/loaders](https://bun.com/docs/runtime/file-types) and [bundler](https://bun.com/docs/bundler) | Bun recognizes `.wasm`; bundling treats Wasm as an asset; the build metafile reports input/output byte contributions and dependency edges. | That a particular Wasm package loads without glue or is embedded automatically. |
| [Bun executable documentation](https://bun.com/docs/bundler/executables) | Bun can instantiate embedded/read Wasm bytes and can build standalone executables; supports experiments on package versus executable deployment. | That standalone executables are smaller than packages. |
| [W3C WebAssembly Core Specification](https://www.w3.org/TR/wasm-core-2/) and [standards history](https://www.w3.org/TR/wasm-core/all/) | Wasm is a safe, portable, low-level code format with compact representation; defines binary encoding, validation, execution, linear memory, imports, and exports. The URL is rolling, so pin the dated snapshot used in the paper. | Automatic application-level safety, native-equivalent speed, host I/O, or zero-copy JS interop. |
| [Haas et al., PLDI 2017](https://doi.org/10.1145/3062341.3062363) | Original motivation, formal semantics, compact representation, validation/compilation design, and language/platform independence of Wasm. | That every Wasm workload is near-native or faster than JavaScript. |
| [Jangda et al., USENIX ATC 2019](https://www.usenix.org/conference/atc19/presentation/jangda) | Wasm/native performance gaps can remain; motivates measuring generated code and runtime behavior instead of repeating “near-native” as a result. | Current Bun/JavaScriptCore performance or Rust-specific performance. |
| [`wasm-bindgen` official repository](https://github.com/wasm-bindgen/wasm-bindgen) and [guide](https://wasm-bindgen.github.io/wasm-bindgen/) | `wasm-bindgen` generates JS/Wasm bindings for exports/imports and ESM integration; glue is generated only for used bindings. | Zero-cost interoperation or zero-copy of arbitrary JS objects. |
| [Rust and WebAssembly book: code size](https://rustwasm.github.io/book/reference/code-size.html) | LTO, size optimization, stripping debug/name information, and `wasm-opt` can change Wasm size; size/speed trade-offs must be measured. | A guaranteed percentage reduction for this project. |
| [RustBelt, POPL 2018](https://doi.org/10.1145/3158154) | Formal, machine-checked safety results for a realistic Rust subset and selected unsafe libraries; a rigorous source for Rust's safety foundations. | That all dependencies or unsafe blocks in `bun_panda` are verified. |
| [Apache Arrow columnar format specification, format 1.5](https://arrow.apache.org/docs/format/Columnar.html) | Arrow is a language-independent in-memory columnar format with explicit physical layouts, metadata/IPC, constant-time random access, alignment, and zero-copy-friendly relocation. | That a conversion in `bun_panda` is actually zero-copy; this requires allocation/copy evidence. |
| [Apache Parquet specification repository](https://github.com/apache/parquet-format) and [official docs](https://parquet.apache.org/docs/) | Parquet is a column-oriented file format with row groups, column chunks, pages, encodings, and compression intended for efficient storage/retrieval. | In-memory zero-copy behavior or any library's I/O speed. |
| [Dataframe interchange protocol requirements](https://data-apis.org/dataframe-protocol/latest/design_requirements.html) and [API](https://data-apis.org/dataframe-protocol/latest/API.html) | A dataframe can be modeled as ordered named columns with dtype/missingness; interchange should expose buffers, chunking, missingness, and copy control and should be zero-copy where possible. | Universal cross-language adoption or that Arrow alone guarantees semantic round trips. |
| [Arquero official documentation](https://idl.uw.edu/arquero/) and [expression model](https://idl.uw.edu/arquero/api/expressions.html) | Arquero is a JavaScript library for array-backed, column-oriented tables, with relational-algebra/dplyr-inspired verbs and generated expressions; supports Arrow columns. | Peer-reviewed performance superiority or pandas compatibility. |
| [Danfo.js official documentation](https://danfo.jsdata.org/) | Danfo.js is a JavaScript dataframe library intentionally inspired by pandas, with labeled structures, missing data, group-by, joins, I/O, and TensorFlow.js integration. | Current performance, bundle size, or complete pandas parity. |
| [DuckDB-Wasm, PVLDB 2022](https://duckdb.org/pdf/VLDB2022-kohn-duckdb-wasm.pdf) | A concrete prior Wasm analytics architecture: async Web Workers, JavaScript UDFs, paged browser filesystem, Arrow/Parquet support, and TPC-H evaluation against web data libraries. | Direct dataframe-API equivalence, Bun performance, or current package size. |
| [DuckDB-Wasm current docs](https://duckdb.org/docs/stable/clients/wasm/overview) | A current comparator's documented single-thread default and memory limitations; distinguishes SQL/Wasm scope from dataframe semantics. | Results for versions not pinned in the experiment. |
| [Mozzillo et al., EDBT 2025](https://doi.org/10.48786/EDBT.2025.27) | A recent peer-reviewed evaluation design for single-machine dataframe libraries and relevant workload dimensions/baselines. | JavaScript/Bun/Wasm results; its findings cannot be transplanted. |
| [H2O.ai db-benchmark repository](https://github.com/h2oai/db-benchmark) | A public, reproducible group-by/join workload family that varies row count, cardinality, sortedness, and missingness. Useful as one benchmark component. | A formal industry benchmark or a complete dataframe workload. |
| [TPC-H current specifications](https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp) and [TPC-H 3.0.1 PDF](https://www.tpc.org/TPC_Documents_Current_Versions/pdf/TPC-H_v3.0.1.pdf) | TPC-H 3.0.1 is the current official decision-support workload, with data generation, 22 queries, scale factors, and disclosure rules. | A compliant TPC-H result if queries or rules are changed. Any adaptation must be labeled “derived from TPC-H,” not an official TPC-H score. |
| [Kalibera and Jones, ISMM 2013](https://doi.org/10.1145/2464157.2464160) | Systems benchmarks contain multiple levels of non-determinism; repetition and confidence intervals should reflect the experimental hierarchy. | A universal fixed iteration count. |
| [Georges et al., OOPSLA 2007](https://doi.org/10.1145/1297027.1297033) | Dynamic/JIT runtime performance needs repeated VM invocations, steady-state awareness, uncertainty reporting, and statistically rigorous comparisons. | That a fixed warm-up count is always sufficient. |
| [Barrett et al., OOPSLA 2017](https://doi.org/10.1145/3133876) | JIT warm-up is not reliably a simple transition to peak steady state; changepoint behavior and process-to-process variation matter. | That all Bun workloads exhibit the same warm-up behavior as their evaluated VMs. |
| [ACM Artifact Review and Badging v1.1](https://www.acm.org/publications/policies/artifact-review-and-badging-current) | A functional artifact should be documented, consistent, complete, and exercisable; archival availability requires a persistent repository/identifier. | That a GitHub link alone constitutes independent validation. |
| [ACM SIGPLAN empirical-evaluation guidelines](https://www.sigplan.org/Resources/EmpiricalEvaluation/) | Benchmark choice must be principled; claims, baselines, threats, and empirical evidence should be aligned; standard suites are useful when appropriate. | A substitute for domain-specific experimental design. |
| [Bun bundler metafile docs](https://bun.com/docs/bundler#metafile) | The metafile contains per-input and per-output byte contributions and dependency edges, suitable for bundle decomposition. | Transfer size; raw bundle bytes must also be gzip/Brotli compressed explicitly. |
| [npm `pack` documentation](https://docs.npmjs.com/cli/v11/commands/npm-pack/) | `npm pack` creates the consumer tarball and reports its contents; use it to measure the shipped package rather than the repository checkout. | Application bundle size after tree shaking. |
| [RFC 1952](https://www.rfc-editor.org/info/rfc1952/) and [RFC 7932](https://www.rfc-editor.org/info/rfc7932/) | Authoritative definitions for gzip and Brotli compressed representations; supports reporting reproducible raw/gzip/Brotli byte counts. | Network latency or browser caching behavior. |

## 4. Evaluation protocol recommended for the manuscript

### 4.1 Freeze the semantic target

- Pin a pandas release and archive the exact HTML/API inventory used. The current site is pandas 3.0.5, but the target should match the project's declared parity contract, not silently drift to “latest.”
- Define whether parity means top-level functions, `DataFrame`, `Series`, accessors, I/O, GroupBy/window intermediates, signatures/defaults, exceptions, dtypes, or only method names.
- Report at least four metrics separately: documented API-name coverage; callable/signature coverage; differential behavioral agreement; and test/branch coverage. Never combine these into a single “100% compatible” number.
- Differential fixtures should compare values, index/column labels, column order, dtypes/null representation, row ordering, exceptions/warnings, and mutation/copy semantics. Use exact comparison where defined and documented tolerances for floating-point reductions.
- Categorize every mismatch as unsupported, intentional divergence, implementation defect, environment limitation, or ambiguous pandas behavior. Publish the full machine-readable ledger.

### 4.2 Systems and versions

At minimum compare:

1. `bun_panda` with Wasm enabled.
2. `bun_panda` with pure-TypeScript/fallback execution where available (essential for the Wasm ablation).
3. pandas in CPython as the semantic reference and a cross-runtime performance baseline.
4. Arquero in the supported JavaScript runtime.
5. Danfo.js in its supported runtime.
6. DuckDB-Wasm for semantically equivalent relational/analytical kernels, explicitly labeled SQL rather than dataframe API.

Run a competitor under Bun only after a compatibility smoke test. If a competitor officially targets Node/browser and fails on Bun, run it in its supported runtime and label that comparison cross-runtime. Pin package versions and lockfiles. Do not silently substitute a different API, omit conversion time for one system, or include it for another.

Record CPU model/microcode, cores, RAM, storage, OS/kernel, power mode, governor, thermal state, Bun and JavaScriptCore versions, Node/V8 if used, Python/pandas, Rust toolchain/target, `wasm-bindgen`, `wasm-opt`, build profile/flags, compression tool versions, and repository commit. Disable unrelated work and report whether hyperthreading/turbo, swap, and thread counts were controlled.

### 4.3 Workload matrix

Use deterministic generators with saved seeds and independently verified expected outputs.

| Dimension | Suggested levels |
|---|---|
| Rows | `10^3`, `10^4`, `10^5`, `10^6`, and the largest size all systems can complete without swapping; larger stress runs reported separately |
| Width | 8, 32, 128 columns |
| Types | homogeneous numeric; numeric + Boolean; mixed numeric/string; categorical; date/time where shared |
| Missingness | 0%, 1%, 10%, including clustered and uniform missing values |
| Group cardinality | low (~10), medium (~sqrt(N)), high (~0.1N) |
| Sortedness | presorted, reverse, random |
| Join shape | 1:1, 1:N, N:M; matched and unmatched keys |
| Selectivity | ~1%, 10%, 50%, 90% |

Kernel families:

- construction and typed-column ingestion;
- projection, label/position indexing, Boolean filtering;
- vector arithmetic and null-aware reductions;
- stable/unstable sort as semantically applicable;
- group-by with `sum`, `mean`, `min/max`, count, multi-key aggregation;
- inner/left/outer joins with controlled cardinality;
- concat, melt/pivot, duplicate handling;
- rolling/window operations;
- CSV and JSON parse/serialize;
- Arrow IPC and Parquet read/write/round-trip;
- multi-stage pipelines that force intermediate materialization.

Use a clearly labeled subset of H2O.ai db-benchmark group-by/join workloads. Add selected TPC-H-derived pipelines only where all systems can express equivalent semantics. Do not publish a `QphH` or call the result “TPC-H compliant” unless every TPC rule is followed.

### 4.4 Timing and statistics

- Separate cold import, Wasm fetch/read, validation/compilation, instantiation, first operation, and warmed operation time. End-to-end results should include all costs users pay; kernel-only results should be separately labeled.
- Use multiple fresh process invocations. Within each process collect an ordered time series; inspect warm-up/changepoints rather than discarding an arbitrary fixed prefix without evidence.
- Randomize or balanced-block workload/system execution order to reduce thermal and temporal bias.
- Use monotonic high-resolution clocks and prevent dead-code elimination by validating/consuming results outside the timed region.
- Calibrate repetitions so clock overhead is negligible, but keep each observation independent at the intended level.
- Report raw samples, medians/means as appropriate, dispersion, and 95% confidence intervals. For ratios, compute paired or hierarchical bootstrap intervals over process-level aggregates. Report effect sizes, not only p-values.
- Never average unrelated raw execution times. If a summary across benchmarks is essential, use a geometric mean of per-workload normalized ratios and retain every workload result.
- Correctness is a gate: omit or mark a timing if the system's result differs semantically. A fast wrong answer is not a benchmark result.

### 4.5 Memory, boundary, and Wasm ablations

Measure peak RSS and, where accessible, JS heap and Wasm linear-memory growth. Explain that RSS includes runtime and allocator state and is not interchangeable with retained dataframe bytes.

For each Wasm-eligible kernel, measure:

1. TypeScript implementation with already-native JS inputs.
2. Conversion/copy into Wasm memory.
3. Wasm kernel alone.
4. Conversion/copy back.
5. End-to-end dispatched operation.
6. Repeated operation reusing resident Wasm buffers, if supported.

Vary row count to estimate the acceleration break-even point. Use allocation instrumentation or controlled buffer identity checks before calling any Arrow or Wasm path “zero-copy.” Show which dtypes force conversion and whether strings, null bitmaps, categorical dictionaries, and timestamps round-trip.

### 4.6 Package and bundle-size study

Measure distinct artifacts; they answer different questions:

- repository checkout and build outputs (developer cost, not user transfer size);
- `npm pack` tarball and unpacked installed size;
- minimal consumer app bundled for `target=bun`, `target=node`, and `target=browser` where supported;
- full-library import versus targeted named imports to test tree shaking;
- unminified/minified JavaScript, JS glue, `.wasm`, source maps, and declaration files separately;
- raw, gzip, and Brotli bytes with pinned compression settings;
- Bun metafile contribution by module;
- eager versus lazy Wasm loading;
- cold import/instantiate and first-operation latency alongside byte size.

Compare with equivalent minimal consumers for Arquero, Danfo.js, and DuckDB-Wasm. State whether external assets are counted. Package tarball size, installed size, browser transfer size, and standalone executable size must never be presented as the same metric.

### 4.7 Reproducibility package

Archive a release-specific artifact (Zenodo or equivalent DOI) containing:

- source at the evaluated commit and complete lockfiles;
- build and benchmark scripts, compiler flags, environment capture, and seeded data generators;
- frozen input fixtures and expected-result hashes;
- raw per-iteration measurements—not only plots/tables;
- analysis notebooks/scripts that regenerate every table and figure;
- a short smoke path plus the full experiment path and expected duration/resources;
- licenses/provenance for every dataset and third-party artifact;
- machine-readable parity ledger and known divergences.

The target should be ACM's “documented, consistent, complete, exercisable” standard even if the Springer journal does not award ACM badges.

## 5. Figure and table plan

Diagrams should communicate measured or inspectable structure; visual abundance is not a substitute for evidence.

1. **System context:** TypeScript caller → pandas-like semantic layer → dispatcher → TypeScript kernels / Rust-Wasm kernels → I/O/interchange adapters.
2. **Concrete module architecture:** map actual repository modules and dependencies after code inspection; do not invent components.
3. **Operation lifecycle:** construction, dtype inference, label alignment, dispatch, boundary conversion, kernel, result reconstruction.
4. **Memory-layout comparison:** JS arrays/typed arrays versus Wasm linear memory versus Arrow buffers, with copy/alias arrows verified by instrumentation.
5. **Wasm boundary cost model:** fixed initialization cost + per-byte conversion + kernel + output reconstruction; annotate empirically estimated break-even points.
6. **Arrow/Parquet data paths:** in-memory Arrow IPC and on-disk Parquet paths, including which conversions are lossless/copying.
7. **Pandas conformance pipeline:** frozen docs inventory → signature audit → differential fixtures → normalized comparison → divergence ledger.
8. **Benchmark harness:** fresh-process controller, randomized workloads, validation gate, raw-event log, hierarchical analysis, figure generation.
9. **Parity heat map:** operation families × coverage dimensions (name/signature/value/dtype/index/error); use counts and confidence, not decorative percentages.
10. **Scaling plots:** time and throughput versus rows, faceted by workload; log axes where justified; confidence bands and timeout/OOM marks.
11. **Wasm ablation stacked bars:** marshal-in, kernel, marshal-out, wrapper overhead across sizes.
12. **Memory plots:** peak RSS/heap/linear-memory versus input size, with OOM boundaries.
13. **Bundle decomposition:** stacked raw/gzip/Brotli contributions from JS, Wasm, glue, dependencies, declarations, maps.
14. **Capability matrix:** pandas semantics, Arrow, Parquet, browser/Bun/Node, threading, lazy loading, unsupported dtypes—each cell linked to a test.
15. **Threats-to-validity table:** construct, internal, external, and conclusion validity with mitigation and residual risk.

## 6. BibTeX-ready core references

Metadata below is transcribed from official publisher/project pages or the cited paper PDFs. Recheck against the selected journal's BibTeX processor before submission.

```bibtex
@manual{springernature_template_2024,
  author       = {{Springer Nature}},
  title        = {Journal Article LaTeX Authoring Template: User Manual},
  organization = {Springer Nature},
  year         = {2024},
  month        = dec,
  note         = {Version 3.1; prepared by Straive TeX Support},
  url          = {https://cms-resources.apps.public.k8s.springernature.io/springer-cms/rest/v1/content/18782940/data/v12},
  urldate      = {2026-08-26}
}

@inproceedings{mckinney2010data,
  author    = {McKinney, Wes},
  title     = {Data Structures for Statistical Computing in Python},
  booktitle = {Proceedings of the 9th Python in Science Conference},
  editor    = {van der Walt, St\'{e}fan and Millman, Jarrod},
  pages     = {56--61},
  year      = {2010},
  doi       = {10.25080/Majora-92bf1922-00a}
}

@article{petersohn2020scalable,
  author  = {Petersohn, Devin and Macke, Stephen and Xin, Doris and Ma, William and Lee, Doris and Mo, Xiangxi and Gonzalez, Joseph E. and Hellerstein, Joseph M. and Joseph, Anthony D. and Parameswaran, Aditya G.},
  title   = {Towards Scalable Dataframe Systems},
  journal = {Proceedings of the VLDB Endowment},
  volume  = {13},
  number  = {11},
  pages   = {2033--2046},
  year    = {2020},
  doi     = {10.14778/3407790.3407807}
}

@article{petersohn2021modin,
  author  = {Petersohn, Devin and Tang, Dixin and Durrani, Rehan and Melik-Adamyan, Areg and Gonzalez, Joseph E. and Joseph, Anthony D. and Parameswaran, Aditya G.},
  title   = {Flexible Rule-Based Decomposition and Metadata Independence in Modin: A Parallel Dataframe System},
  journal = {Proceedings of the VLDB Endowment},
  volume  = {15},
  number  = {3},
  pages   = {739--751},
  year    = {2021},
  doi     = {10.14778/3494124.3494152}
}

@inproceedings{haas2017webassembly,
  author    = {Haas, Andreas and Rossberg, Andreas and Schuff, Derek L. and Titzer, Ben L. and Holman, Michael and Gohman, Dan and Wagner, Luke and Zakai, Alon and Bastien, JF},
  title     = {Bringing the Web up to Speed with WebAssembly},
  booktitle = {Proceedings of the 38th ACM SIGPLAN Conference on Programming Language Design and Implementation},
  pages     = {185--200},
  year      = {2017},
  publisher = {ACM},
  doi       = {10.1145/3062341.3062363}
}

@inproceedings{jangda2019notsofast,
  author    = {Jangda, Abhinav and Powers, Bobby and Berger, Emery D. and Guha, Arjun},
  title     = {Not So Fast: Analyzing the Performance of WebAssembly vs. Native Code},
  booktitle = {2019 USENIX Annual Technical Conference},
  pages     = {107--120},
  year      = {2019},
  publisher = {USENIX Association},
  url       = {https://www.usenix.org/conference/atc19/presentation/jangda}
}

@article{kohn2022duckdbwasm,
  author  = {Kohn, Andr\'{e} and Moritz, Dominik and Raasveldt, Mark and M\"{u}hleisen, Hannes and Neumann, Thomas},
  title   = {DuckDB-Wasm: Fast Analytical Processing for the Web},
  journal = {Proceedings of the VLDB Endowment},
  volume  = {15},
  number  = {12},
  pages   = {3574--3577},
  year    = {2022},
  doi     = {10.14778/3554821.3554847}
}

@article{jung2018rustbelt,
  author  = {Jung, Ralf and Jourdan, Jacques-Henri and Krebbers, Robbert and Dreyer, Derek},
  title   = {RustBelt: Securing the Foundations of the Rust Programming Language},
  journal = {Proceedings of the ACM on Programming Languages},
  volume  = {2},
  number  = {POPL},
  articleno = {66},
  pages   = {66:1--66:34},
  year    = {2018},
  doi     = {10.1145/3158154}
}

@inproceedings{kalibera2013rigorous,
  author    = {Kalibera, Tomas and Jones, Richard E.},
  title     = {Rigorous Benchmarking in Reasonable Time},
  booktitle = {Proceedings of the 2013 International Symposium on Memory Management},
  pages     = {63--74},
  year      = {2013},
  publisher = {ACM},
  doi       = {10.1145/2464157.2464160}
}

@inproceedings{georges2007statistically,
  author    = {Georges, Andy and Buytaert, Dries and Eeckhout, Lieven},
  title     = {Statistically Rigorous Java Performance Evaluation},
  booktitle = {Proceedings of the 22nd Annual ACM SIGPLAN Conference on Object-Oriented Programming Systems and Applications},
  pages     = {57--76},
  year      = {2007},
  publisher = {ACM},
  doi       = {10.1145/1297027.1297033}
}

@article{barrett2017warmup,
  author    = {Barrett, Edd and Bolz-Tereick, Carl Friedrich and Killick, Rebecca and Mount, Sarah and Tratt, Laurence},
  title     = {Virtual Machine Warmup Blows Hot and Cold},
  journal   = {Proceedings of the ACM on Programming Languages},
  volume    = {1},
  number    = {OOPSLA},
  articleno = {52},
  pages     = {1--27},
  year      = {2017},
  doi       = {10.1145/3133876}
}

@inproceedings{mozzillo2025dataframes,
  author    = {Mozzillo, Angelo and Zecchini, Luca and Gagliardelli, Luca and Aslam, Adeel and Bergamaschi, Sonia and Simonini, Giovanni},
  title     = {Evaluation of Dataframe Libraries for Data Preparation on a Single Machine},
  booktitle = {Proceedings of the 28th International Conference on Extending Database Technology},
  pages     = {337--349},
  year      = {2025},
  doi       = {10.48786/EDBT.2025.27}
}

@manual{tpch301,
  author       = {{Transaction Processing Performance Council}},
  title        = {TPC Benchmark H Standard Specification},
  organization = {Transaction Processing Performance Council},
  edition      = {Revision 3.0.1},
  year         = {2022},
  url          = {https://www.tpc.org/TPC_Documents_Current_Versions/pdf/TPC-H_v3.0.1.pdf},
  urldate      = {2026-08-26}
}

@techreport{rfc1952,
  author      = {Deutsch, Peter},
  title       = {GZIP File Format Specification Version 4.3},
  institution = {RFC Editor},
  number      = {RFC 1952},
  year        = {1996},
  month       = may,
  doi         = {10.17487/RFC1952},
  url         = {https://www.rfc-editor.org/info/rfc1952/}
}

@techreport{rfc7932,
  author      = {Alakuijala, Jyrki and Szabadka, Zolt\'{a}n},
  title       = {Brotli Compressed Data Format},
  institution = {RFC Editor},
  number      = {RFC 7932},
  year        = {2016},
  month       = jul,
  doi         = {10.17487/RFC7932},
  url         = {https://www.rfc-editor.org/info/rfc7932/}
}

@misc{apachearrow_format,
  author  = {{Apache Arrow Project}},
  title   = {Arrow Columnar Format},
  note    = {Format version 1.5},
  url     = {https://arrow.apache.org/docs/format/Columnar.html},
  urldate = {2026-08-26}
}

@misc{apacheparquet_format,
  author  = {{Apache Parquet Project}},
  title   = {Apache Parquet Format Specification},
  url     = {https://github.com/apache/parquet-format},
  urldate = {2026-08-26}
}

@misc{acm_artifacts_2020,
  author  = {{Association for Computing Machinery}},
  title   = {Artifact Review and Badging, Version 1.1},
  year    = {2020},
  month   = aug,
  url     = {https://www.acm.org/publications/policies/artifact-review-and-badging-current},
  urldate = {2026-08-26}
}
```

## 7. Submission-quality cautions

- Journal quartiles change by year, subject category, and database (JCR versus Scopus/SCImago). Select the target journal first, verify its current official author instructions, then document the exact ranking source/year separately. A generic Springer template does not make a paper “Q1.”
- A library-description paper is unlikely to meet a top journal's novelty threshold by feature count alone. The strongest research angle is a measured semantic-fidelity/portable-acceleration trade-off with an independently reproducible artifact.
- “A lot of diagrams” can harm clarity. Every figure should answer a research question, expose architecture, or present measured evidence. Capability grids must be backed by executable tests.
- Benchmark and comparison results must be generated from the evaluated commit. Until that happens, manuscript tables should contain `TBD`/automatically imported data, never plausible-looking placeholder numbers.
- Authors, affiliations, funding, conflicts, data/code availability, and author-contribution statements must come from the actual authors. They cannot be inferred from the repository.
