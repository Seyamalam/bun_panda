# Claim-to-evidence map

| Manuscript claim | Raw evidence | Regeneration or check |
| --- | --- | --- |
| 505 names in the frozen API census | `docs/PARITY.md`, `scripts/parity-audit.ts` | `bun run parity` |
| 2,500/2,500 tested pandas observations agree under the declared merge-order rule | `paper/data/conformance/{cases,pandas,adaptive,summary,mismatch-ledger}.json` | `bun run conformance` |
| 2,500/2,500 TypeScript, eligible-Wasm, and adaptive observations agree | `paper/data/conformance/{typescript,wasm,adaptive,summary}.json` | `bun run conformance` |
| 720 fresh processes and 14,400 measured calls | `paper/data/fresh-process-ablation.json` | `bun run bench:fresh` |
| Full sort and cached-column GroupBy win at every measured Bun scale | same as above | `bun run bench:fresh` |
| Large top-1,000 is unresolved and forced-Wasm mask filtering loses | same as above | `bun run bench:fresh` |
| Five-system study has 400 fresh processes, 4,000 calls, and matched hashes | `paper/data/competitor-study.json` | `bun run bench:competitors` |
| Browser study has 180 isolated contexts and 10,800 matched calls | `paper/data/browser-study.json` | `bun run bench:browser` |
| Chromium and WebKit favor Wasm sorting while Firefox reverses it | `paper/data/browser-study.json`, `paper/data/expanded-summary.json` | `bun run paper/artifact/summarize-expanded-study.ts` |
| UCI Bank Marketing source, license, schema, checksums, and 200-process validation | `paper/workloads/manifest.json`, `paper/data/workloads/uci-bank/`, `paper/data/competitor-uci-bank.json` | `bun run workload:public`, then `bun run bench:competitors:uci` |
| Browser entry, Wasm, bundle, and npm tarball sizes | `paper/data/package-size.json` | `bun run paper/artifact/analyze-package-size.ts` |
| x86 and cgroup claims are protocols, not measured results | `paper/artifact/linux/`, `paper/data/rejected-runs.json` | `bun run bench:linux`, then `bun run bench:cgroup` on native x86-64 |
| Authorship and declarations | `paper/manuscript/main.tex` | manual author verification |

The manuscript must not convert the 505-name census into behavioral compatibility. It must not describe macOS process RSS as a controlled capacity result. Browser execution refers to the typed numeric kernel entry, not the full DataFrame API. No x86-64 timing, DOI, or independent sign-off may be claimed until the corresponding external artifact exists.
