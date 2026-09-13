# Independent reproduction handoff

Status: prepared for a tester who did not develop the artifact. No independent
report or sign-off has been received.

## Message for the tester

Please reproduce the public bun_panda v0.4.1 research artifact from a clean
checkout on your own machine. The full protocol runs correctness checks and
the benchmark studies, then writes one JSON report. It typically takes about
an hour on a recent desktop. Prerequisites and platform-specific setup are in
[CROSS-PLATFORM-REPRODUCTION.md](../docs/CROSS-PLATFORM-REPRODUCTION.md).

Use a new directory and run:

```sh
git clone --branch v0.4.1 --single-branch https://github.com/Seyamalam/bun_panda.git bun-panda-reproduction
cd bun-panda-reproduction
git rev-parse HEAD
bun run reproduce:platform --profile full --tester "Your name"
```

The commit must be `d35161cd74e6d8f6f54fd8d564470794184b56a7`.
Please return the generated `reproduction-*.json` report, even if a stage fails.
Confirm that you ran it yourself, describe any setup changes, and state whether
you participated in developing this artifact. The report includes machine
details, tool versions, logs, and benchmark results; inspect it before sharing.

## Acceptance review

- Verify the commit, initial clean working tree, tester identity, and full profile.
- Check that every required stage passed and none was skipped.
- Check the conformance observations, workload checksums, and backend output digests.
- Record any setup changes and failures without editing the submitted report.
- Obtain the tester's explicit independent-execution confirmation.
- Store the original report and its SHA-256 digest, and link the sign-off from
  the release checklist and submission-readiness document.

Keep timings from other machines separate from the manuscript's macOS tables.
The current manuscript does not claim multi-host performance results.
