# bun_panda agent handoff

Updated 27 August 2026 for the `v0.4.1` release candidate after the semantic, comparator, browser, public-workload, cross-platform-runner, manuscript, and full local-reproduction work.

## Repository

- Working directory: `/Users/seyam/Work/bun_panda`
- Public source: `https://github.com/Seyamalam/bun_panda`
- Package version: `0.4.1`
- npm: `https://www.npmjs.com/package/bun_panda`
- Immutable source release: `https://github.com/Seyamalam/bun_panda/releases/tag/v0.4.1`
- CI: `.github/workflows/ci.yml` is intentionally deleted. Depot and GitHub Actions are not used.

## Verified local state

- `bun run check`: clean
- Tests: 376 pass, 0 fail, 813 expectations across 27 files
- `bun run pack:smoke`: clean, 77-file consumer tarball
- API census: 505/505 tracked names. This is name coverage, not blanket compatibility.
- Differential oracle: 2,500/2,500 against pandas 3.0.5 under the declared merge-order rule. The comparator preserves join-key group order and treats duplicate pair order within one equal-key group as unspecified.
- Backend equality: TypeScript, eligible Wasm, and adaptive execution agree in 2,500/2,500 oracle cases.
- Current Wasm binary: 7,359 bytes.

## Paper and evidence

The target is **The VLDB Journal** using Springer's subscription route, which currently has no APC. Planning material should say **SJR 2025 Q1**, not JCR Q1 and not that the paper itself is Q1.

The manuscript is `paper/manuscript/main.tex`. The compiled and visually checked 20-page PDF is `paper/build/main.pdf`.

Paper Amigo project `ab09cc57-095b-4342-a8a8-48e6da9c60b8` contains the
current PDF. After every successful PDF build, run `bun run paper:amigo:sync`.
It replaces `main.pdf` only when the hash changed, verifies the remote result,
and updates `paper/paper-amigo-project.json`. Never create a duplicate project
for a new revision.

Accepted local evidence:

- Wasm ablation: 720 fresh processes and 14,400 timed calls with hierarchical intervals.
- Synthetic five-system study: 400 fresh processes and 4,000 timed calls across bun_panda, Arquero, Danfo.js, nodejs-polars, and DuckDB-Wasm.
- UCI Bank Marketing study: checksum-pinned 45,211-row input, 200 fresh processes, and 2,000 timed calls.
- Browser study: 180 isolated Chromium, Firefox, and WebKit worker contexts and 10,800 timed calls, with 30 contexts per engine and scale.
- Every accepted comparator and browser cell passes its canonical output hash.
- Distribution analysis: 130.7 KiB npm tarball, 546.1 KiB unpacked, 180.2 KiB minified core, 51.6 KiB core gzip, 2,683-byte browser entry, and 7,359-byte Wasm asset.
- Full author-run reproduction: `paper/data/reproduction-macos-arm64.json`, status `completed`, with 14 passed stages and no failed or skipped stage. It is local validation, not independent reproduction.

The public workload changes the synthetic ranking. bun_panda has no operation-only win on the four UCI cells and retains a large join deficit. The 30-context browser medians also reverse by engine: stable argsort is 1.78x in Chromium, 1.28x in WebKit, and 0.47x in Firefox. Do not smooth these results into one library-wide speedup.

## Reproduction commands

The supported clean-clone route on macOS, Windows, and Linux is:

```sh
bun run reproduce:platform --profile full --tester "Reproducer name"
```

It returns one dated JSON report and keeps generated study data outside the
checked-in `paper/data` directory. Platform-specific launchers and prerequisites
are documented in `docs/CROSS-PLATFORM-REPRODUCTION.md`.

The underlying commands remain:

```sh
bun install --frozen-lockfile
bun run check
bun run pack:smoke
bun run parity
bun run conformance
bun run bench:fresh
bun run bench:competitors
bun run workload:public
bun run bench:competitors:uci
bun run bench:browser
bun run paper/artifact/analyze-package-size.ts
bun run paper/artifact/summarize-study.ts
bun run paper/artifact/summarize-expanded-study.ts
bun run artifact:checksums
```

The LaTeX plugin's bundled Tectonic compiler succeeds. TeX Live is not installed locally.

## Remaining external tasks and optional validation

1. Obtain an independent clean-checkout reproduction using `docs/CROSS-PLATFORM-REPRODUCTION.md`. The runner supports macOS, Windows, and Linux and returns one JSON report.
2. Deposit an archival DOI and request a Software Heritage snapshot. Follow `paper/RELEASE-CHECKLIST.md`.
3. If resources become available, run the frozen candidate on native x86-64 Linux and execute the cgroup-v2 capacity protocol. These are useful validation studies, not current manuscript result requirements. Apple QEMU faulted because the emulated CPU exposed no AVX, so its timing remains rejected.

The manuscript now reports macOS performance only. Do not claim native x86 timing, controlled capacity, independent reproduction, or a DOI until those artifacts exist, and do not pool returned cross-platform timings into the macOS tables without a new multi-host protocol.

## Important files

- `paper/SUBMISSION-READINESS.md`: status of the original eight gaps
- `paper/CLAIM-EVIDENCE.md`: claim-to-artifact map
- `paper/HUMANIZER-AUDIT.md`: humanizer and unslop audit
- `paper/journal-target.md`: venue and publication-route evidence
- `paper/artifact/competitors/`: five-system adapters and runner
- `paper/artifact/browser/`: real-browser worker study
- `paper/artifact/linux/`: native x86 and cgroup runners
- `paper/workloads/`: public-workload manifest and preparation
- `paper/data/rejected-runs.json`: rejected contamination and emulation attempts

## Release discipline

The release candidate is `v0.4.1`. Keep later paper revisions synchronized to the existing Paper Amigo project with `bun run paper:amigo:sync`; do not create duplicate projects. Regenerate summaries, checksums, and the PDF before any later archival deposit.
