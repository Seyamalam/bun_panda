# Wasm Analytics Studio

This example is a browser-native performance laboratory for `bun_panda`'s
Rust/WebAssembly kernels. It does more than time a single function: it builds a
deterministic typed-column dataset, executes four analytics workloads through
equivalent TypeScript and Wasm paths, validates every output with SHA-256, and
explains the library's calibrated dispatch choice.

## Run it

From the repository root:

```bash
bun install --frozen-lockfile
bun run example:wasm
```

Open <http://localhost:4173>. To choose another port:

```bash
PORT=8080 bun run example:wasm
```

Build or test without starting the server:

```bash
bun run example:wasm:build
bun run example:wasm:test
```

## What it demonstrates

- browser-safe loading of `bun_panda/browser` with explicit Wasm bytes;
- stable numeric argsort, mask compaction, fused grouped sum, and a composed
  filter-to-group pipeline;
- Web Worker isolation, so expensive runs do not freeze the interface;
- deterministic uniform, skewed, and nearly sorted input distributions;
- warmups and interleaved TypeScript/Wasm timing to reduce order bias;
- median and p95 samples, output digests, Wasm memory, startup, input, and
  transfer-size telemetry;
- SIMD, threads, bulk-memory, exception, multi-value, reference-type, and
  tail-call capability detection;
- COOP/COEP response headers for a cross-origin-isolated local environment;
- downloadable JSON reports and a six-run local browser logbook.

## Architecture

```text
controls + report UI (main thread)
              |
              | structured-clone config/progress/report
              v
      analytics.worker.ts
       /              \
TypeScript baselines   bun_panda/browser
       \              /
      SHA-256 parity + sample statistics
              |
              v
 adaptive dispatch explanation + downloadable JSON
```

The build copies the repository's real `src/wasm/bun_panda_core.wasm` asset;
there is no mock kernel and no remote service. Generated `dist/` files are
disposable and excluded from version control.

## Reading the numbers

The faster implementation is intentionally allowed to vary by workload and
input size. Wasm is not presented as automatically faster: boundary copying,
JavaScript's optimized built-ins, compilation cost, browser version, thermal
state, and CPU architecture all affect the result. The policy line on each card
shows what the package's current versioned calibration would select.

For publishable measurements, use the repository's controlled artifact runners
rather than treating a single interactive run as general evidence.

See [browser verification](QA.md) for tested workloads and recovery scenarios.
