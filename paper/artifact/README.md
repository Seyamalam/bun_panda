# Artifact guide

## Correctness and package checks

```sh
bun install --frozen-lockfile
bun run check
bun run pack:smoke
bun run parity
bun run conformance
```

For an independent clean-clone run on macOS, Windows, or Linux, use
`bun run reproduce:platform`. It executes these checks and the host, public-data,
package-size, and browser studies, then returns one JSON report. See
`docs/CROSS-PLATFORM-REPRODUCTION.md`.

## Reported local studies

```sh
bun run bench:fresh
bun run bench:competitors
bun run bench:competitors:uci
bun run bench:browser
bun run workload:public
bun run paper/artifact/analyze-package-size.ts
bun run paper/artifact/summarize-study.ts
bun run paper/artifact/summarize-expanded-study.ts
bun run artifact:checksums
```

`bench:fresh` starts 720 processes. `bench:competitors` starts 400 processes. Smaller smoke settings are documented beside each runner, but smoke outputs must not replace the paper data.

Every accepted performance cell has a canonical-output SHA-256 gate. Raw files retain per-call values, versions, process or browser identity, memory diagnostics where available, and rejected-run reasons.

## Native Linux work

On a native x86-64 Docker host:

```sh
bun run bench:linux
bun run bench:cgroup
```

The runner refuses publishable output on a non-x86-64 kernel. The cgroup study fixes equal memory and memory-swap values and records `memory.current`, `memory.peak`, `memory.events`, exit state, and OOM status. Do not use Apple QEMU timing in the paper.

## External completion gates

- freeze a clean release-candidate commit;
- obtain an independent clean-checkout JSON report;
- publish npm and an immutable source release;
- archive the release with a DOI and Software Heritage identifier.

Native x86-64 and cgroup runs remain optional strengthening studies. The current
manuscript reports macOS performance only and does not make a controlled-capacity
claim.

See `paper/CLAIM-EVIDENCE.md`, `paper/INDEPENDENT-REPRODUCTION.md`, and `paper/RELEASE-CHECKLIST.md`.
