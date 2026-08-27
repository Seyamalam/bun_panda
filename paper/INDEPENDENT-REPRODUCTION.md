# Independent reproduction record

This form must be completed by a person who did not develop the implementation or manuscript. The author must not sign it on another person's behalf.

## Clean-checkout protocol

1. Check out the immutable release tag on a clean machine.
2. Follow `docs/CROSS-PLATFORM-REPRODUCTION.md` for the operating system.
3. Run the full one-file protocol:

   ```sh
   ./scripts/reproduce-platform.sh --profile full --tester "Your name"
   ```

   Windows testers use `scripts\\reproduce-platform.cmd` or the PowerShell
   launcher documented in the cross-platform guide.
4. Send the single dated JSON report to the author, including reports that end
   with `completed_with_failures`.
5. Treat Windows and Linux timings as independent validation inputs. The
   current manuscript reports macOS performance only.
6. Compare the report with the release protocol and record every dependency,
   browser, timing, correctness, or environment deviation.

## Reproducer return form

- Reproducer name:
- Affiliation:
- Contact:
- Date:
- Release tag and commit:
- Machine and operating system:
- Returned JSON report name and SHA-256:
- Total elapsed time (from report):
- Correctness checks passed (from report):
- Performance studies completed (from report):
- Deviations or failures:
- Overall outcome: reproduced / reproduced with deviations / not reproduced
- Signature or verifiable confirmation:

The JSON report and this confirmation should be included in the archival
deposit, subject to the reproducer's consent. The author must not fill in or
sign the independent reproducer's record.
