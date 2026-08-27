# Submission readiness

## Current assessment

The manuscript is now a substantially stronger data-management systems submission candidate. It has a semantic oracle, a fresh-process backend ablation, a correctness-gated five-system comparison, a real three-browser worker study, bundle analysis, a public workload manifest, cross-platform reproduction tooling, and versioned npm and GitHub releases. Independent reproduction and a persistent archival identifier remain before submission.

The intended venue is **The VLDB Journal**, a Springer Nature hybrid journal ranked **SJR 2025 Q1** in Hardware and Architecture and Information Systems. The intended subscription route has no APC. Journal rank does not predict acceptance, and the paper should not be described as "Q1 worthy."

## Status of the original eight gaps

| Gap | Status | Evidence or remaining action |
| --- | --- | --- |
| x86-64 Linux | Optional validation runner complete | The current manuscript reports macOS performance only. A digest-pinned `linux/amd64` route remains available for later native validation. Apple QEMU was rejected and is not reportable evidence. |
| Memory and capacity | Optional protocol complete | The cgroup-v2 runner can fix equal memory and memory-swap limits and record OOM state on native Linux. The present paper makes no controlled-capacity claim. |
| Comparator breadth | Complete for four matched operations | Arquero, Danfo.js, nodejs-polars, and DuckDB-Wasm adapters produced 400 fresh processes and 4,000 timed calls. Every output hash matches the common reference. |
| Browser evidence | Complete at kernel level | Chromium, Firefox, and WebKit produced 180 isolated worker contexts and 10,800 timed calls. All hashes match. This does not claim that the full DataFrame API runs in browsers. |
| Workload breadth | Substantially addressed | The synthetic factorial inputs remain deterministic. UCI Bank Marketing is checksum-pinned and adds 200 fresh processes with matched hashes. The paper does not yet contain a TPC-H study or largest-scale capacity result. |
| Semantic depth | Corpus clean under declared rules | pandas agreement is 2,500/2,500. Merge comparison preserves join-key group order and compares duplicate matches as a row multiset because pandas does not specify duplicate-pair order. All TypeScript, Wasm, and adaptive outputs agree in 2,500/2,500 cases. |
| Independent reproduction | One-file protocol complete, sign-off pending | `docs/CROSS-PLATFORM-REPRODUCTION.md` gives macOS, Windows, and Ubuntu commands that return one JSON report. A person who did not develop the artifact must execute and confirm it. |
| Release and venue | Package and source release complete; archive pending | The VLDB Journal is selected. Version 0.4.1 is published on npm and as an immutable GitHub release. An archive DOI and Software Heritage identifier remain pending. |

## Actual blockers and optional strengthening

1. **Independent reproduction.** The author cannot truthfully manufacture an independent reproducer record. A clean-clone tester must return the generated JSON report.
2. **Persistent archival identifier.** The npm and GitHub releases are public, but an archive DOI and Software Heritage identifier still require deposition.

Native x86-64 timing and a controlled cgroup capacity study would strengthen
external validity, especially for a selective systems journal, but they are now
optional follow-up studies rather than claims promised by the current paper.

## Freeze sequence

1. Finish local tests, regenerate summaries and checksums, compile the PDF, and create one clean release-candidate commit.
2. Ask an independent tester to run the full one-file protocol from the frozen commit and return the JSON report.
3. Review the report's environment, correctness gates, logs, and artifact digests. Keep non-macOS timing separate from the manuscript tables.
4. Archive the release, update the persistent identifiers, and perform the final venue-format check.
5. If native Linux resources later become available, run the x86 and cgroup protocols as a separate extension rather than silently mixing their values with the macOS study.
