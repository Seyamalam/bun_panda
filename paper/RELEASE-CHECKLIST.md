# Release and archival checklist

## Freeze

- [x] All tests and paper builds pass from one clean working tree.
- [x] Run `bun run paper:amigo:sync` and confirm project `ab09cc57-095b-4342-a8a8-48e6da9c60b8` has the final PDF hash.
- [x] `package.json`, `bun.lock`, Wasm bytes, workload manifest, raw data, generated summaries, and manuscript checksums are current.
- [x] Create an annotated version tag and immutable GitHub release from the evaluated commit.

## Package

- [x] Authenticate with npm under the intended publisher account.
- [x] Confirm the package name is available and the package metadata, exports, license, README, and files list are correct.
- [x] Run `bun run pack:smoke` against the exact tarball.
- [x] Publish, then verify `https://www.npmjs.com/package/bun_panda` through the unauthenticated public registry.

## Archive

- [ ] Deposit the immutable GitHub release in Zenodo or an equivalent repository.
- [ ] Add the resulting DOI to `CITATION.cff`, README, manuscript, and code-availability statement.
- [ ] Request a Software Heritage snapshot and record the SWHID.
- [ ] Verify every public link and checksum from a clean browser and checkout.

## External evidence

- [x] Keep performance claims explicitly scoped to the measured macOS host.
- [x] Review available JSON reports separately and do not pool cross-platform timings into the macOS tables without a multi-host protocol. No external Windows or Linux report has been returned yet.
- [ ] Add the independent reproduction record.
- [x] Regenerate all tables, figures, claims, and the PDF from the frozen evidence.

## Submission

- [ ] Recheck The VLDB Journal's current author instructions and publishing options.
- [ ] Use the subscription route if the no-APC policy still applies.
- [ ] Say "SJR 2025 Q1" only in planning or cover-letter material, not as a quality claim inside the manuscript.
- [ ] Ensure all declarations, author details, ORCID, GitHub, npm, DOI, and archive links resolve.
