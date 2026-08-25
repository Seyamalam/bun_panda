# bun_panda — Agent Handoff

*Written 2026-08-25 after the parity sprint + cleanup session. Everything below is verified against the working tree at commit `b563969`.*

## Location

- **Working directory**: `/Users/seyam/Work/bun_panda` (macOS, user `seyam`)
- **Local path on the user's machine**: `~/Work/bun_panda`

## What this repo is

`bun_panda` — a pandas-parity TypeScript library for Bun, with a Rust core compiled to WASM (flat C ABI over linear memory, **no wasm-bindgen/wasm-pack**; loaded via Bun's native `WebAssembly`). Repo: `github.com/Seyamalam/bun_panda`, branch `master`.

## Current state (all pushed)

| Item | Value |
|---|---|
| Version | **0.4.0** (package.json + crates/core/Cargo.toml), tag `v0.4.0` pushed |
| Parity | **505/505 tracked APIs (100%)** per `docs/PARITY.md` — plotting now real (ASCII/SVG), only `style` remains an intentional `NotSupportedError` |
| Tests | 352 pass / 744 expects / 22 files; tsc clean; oxlint clean (`--deny-warnings`); coverage ~81% vs 70% gate |
| LOC | dataframe.ts 2415 · series.ts 1282 · groupby.ts 1353 — rest in `src/internal/**` pure modules |

## Gates (run before every "done")

```bash
bunx tsc --noEmit
./node_modules/.bin/oxlint src test --deny-warnings
bun test
bun run scripts/parity-audit.ts   # needs nothing external anymore
```

## Key architecture facts

- **Delegation pattern**: class files hold thin methods that call pure functions in `src/internal/{dataframe,series,groupby,shared}/`. Two delegate modules use a *structural host-view interface* pattern: `internal/dataframe/windowApi.ts` (HostView) and `internal/series/seriesApi.ts` (SeriesHost) receive snapshot accessors + method closures from a private `view()` adapter on the class. Follow this pattern for any future extraction.
- **WASM**: 7176 bytes at `src/wasm/bun_panda_core.wasm`; rebuild with `bun run build:wasm` (Rust 1.96, wasm32-unknown-unknown installed). Default-on for numeric groupby aggs/sorts; `BUN_PANDA_WASM=0` env opts out.
- **Parity audit is self-contained**: baseline committed at `scripts/pandas-api-baseline.txt` (397 scraped pandas methods). Refresh offline-free via `scripts/refresh-pandas-baseline.sh`. The old `/tmp/pd-methods.txt` dependency is gone.

## Recent history (this session)

1. Parity climbed 255 → 500 → 505 APIs across v0.3.x–v0.4.0 (commits `584d79e`, `0a4e4ba`, `3bbfe08`, `f8a4d44`, `711a224`, `453d95b`). Releases: `v0.3.0` (`cbfc202`) and `v0.4.0` (`fe390d1`).
2. Post-release cleanup: docs rewritten (`2041d00`), DataFrame/Series window+export blocks extracted back into internal modules (`4b47410`, `b3ccc60`), plotting stubs replaced with real renderers (`453d95b`).

## Known traps (do not rediscover these)

- **Subagent delegation dies on HTTP 429** for multi-file API batches on this repo. Do API implementation batches inline; subagents are fine for single-file or read-only tasks.
- **`patch`/`write_file` refuse JSONC files** (tsconfig.json) — edit via terminal python.
- **Hermes memory tool `new_text` alias silently drops content** — always use `content` field.
- **Bare `T` generics**: when moving Series methods out of the generic class, replace `T` with `CellValue` and cast returns (`as never` at the delegate call site is acceptable).
- **Private-field access from delegate modules**: route through snapshot accessors on the HostView/SeriesHost interface, not `df._rows`.
- **`.depot/` directory** appears occasionally as a CI-migration artifact from some external tool — delete it if it shows up untracked or committed (`git rm -r .depot`).
- **pandas semantics traps already encoded in tests**: `ewm(adjust=True)` seeds mean with first observation; `resample().count()` counts non-missing entries of any type; Interval `overlaps` respects closed sides (closed=right excludes left point); `combine` aligns both frames onto the index **union**.

## Benchmarks (verified this session)

`bun run bench/compare.js` vs Arquero at 25k rows: **bun_panda faster in 78/87 cases** (~90%), including all three groupby_mean variants (0.57–0.66x = meaningfully faster). Arquero wins only 9 niche cases. No regression from the API expansion.

## Open work (from docs/TODO.md, in priority order)

1. **npm publish prep** — `prepublishOnly` gates wired to `bun run check`, verify `files` array in package.json covers new dirs (`src/internal/shared/` etc.), run `npm pack` smoke test, decide scoped name (`@seyamalam/bun-panda`?).
2. **Coverage push past 85%** — current 81%; the delegate `view()` adapters and exotic-format IO bridges are the big uncovered areas.
3. **Real binary Arrow/Feather IPC** — current `to_feather`/`read_feather` are JSON-buffer bridges; an actual Arrow IPC layer is the last honest gap in the IO story.
4. **Property-based tests** for CSV/JSON parsers (docs/TODO.md Quality Backlog).
5. **Docs site with runnable examples** (mid-term).

## File map (where things live)

```
src/
  dataframe.ts          # DataFrame class, thin delegates
  series.ts             # Series class, thin delegates
  groupby.ts            # GroupBy + WASM fast path wiring
  top-level.ts          # options registry, Timestamp/Timedelta/Index types, range builders
  top-level-io.ts       # read_html/fwf/json_lines/xml/clipboard/pickle/sql family
  top-level-meta.ts     # show_versions, test
  categorical.ts        # Categorical/CategoricalDtype/CategoricalAccessor
  datetime.ts reshape.ts io.ts errors.ts index.ts
  internal/
    dataframe/          # stats arith selection ordering deltas shape missing join merge reshape rolling fill extended combine evalExpr explode frameOps windowApi io windowTime apply where valueCounts pivotTable keys core merge fastpath...
    series/             # stringMethods datetimeMethods stats cumulative rank compat seriesApi
    groupby/fastAgg.ts  # WASM fast path helpers
    shared/             # time windows plotting index  ← cross-cutting engines
scripts/
  parity-audit.ts               # bun run parity
  pandas-api-baseline.txt       # committed scrape
  refresh-pandas-baseline.sh    # curl re-scrape
test/                           # 22 files, mirrors src families
bench/                          # compare.js (Arquero), pandas_compare.py, results/
crates/core/                    # Rust source + Cargo.toml (version-synced with package.json)
```

## Release checklist (for v0.4.1+)

1. Bump version in **both** package.json and crates/core/Cargo.toml (keep in sync).
2. Add CHANGELOG.md entry dated today.
3. Update README status line (parity % + version).
4. Run full gates above + `bun run build:wasm`.
5. Commit `chore(release): x.y.z …`, tag annotated `vx.y.z`, `git push origin master vX.Y.Z`.
