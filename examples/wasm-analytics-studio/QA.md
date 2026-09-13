# Browser verification

Verified 13 September 2026 in Chrome 152 on macOS, using the local Bun server
with COOP/COEP headers. These checks cover the working-tree example, not the
frozen v0.4.1 paper measurements.

| Scenario | Result |
| --- | --- |
| Default, 100,000 uniform rows | All four TypeScript/Wasm output digests match |
| Stress, 250,000 nearly sorted rows | All four digests match |
| Heavy skew, 150,000 rows | All four digests match |
| Quick, 20,000 rows | All four digests match |
| Cancel immediately, then restart | Worker recovers and Quick completes |
| Download latest JSON | File parses and contains four equivalent workloads |
| Reload after a completed run | Worker becomes ready; saved history remains visible |
| Corrupted stored history (`{}`) | Invalid history is discarded; worker becomes ready |
| Storage write throws `QuotaExceededError` | Run completes with history-unavailable status; download and rerun remain enabled |

Storage failures were injected through browser debugging, then removed. The
normal run and reload checks were repeated after restoring storage behavior.
Desktop layout and result cards were visually inspected. Cross-origin isolation
was confirmed at runtime.

The downloaded Quick report had SHA-256
`cb332cf8660afd61d228da928a1e51325a66459d965d0f388f8a6de9489f560a`.
Its timings describe that browser run only; they are not paper evidence.

Automated validation: `bun run example:wasm:test` passes the example type check
and six deterministic engine tests; `bun run example:wasm:build` succeeds.
