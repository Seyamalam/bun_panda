# Security Policy

## Reporting a Vulnerability

If you discover a security issue, please do not open a public issue first.

Instead:

1. Email details to `security@your-org.example`.
2. Include reproduction steps, affected versions, and potential impact.
3. Allow time for triage before public disclosure.

## Response Targets

- Initial acknowledgment: within 72 hours.
- Triage decision: within 7 days.
- Patch target: as soon as practical based on severity.

## Scope

This policy covers vulnerabilities in:

- Runtime library code (`src/`)
- CSV parsing and data handling paths
- Public APIs that could expose unsafe behavior

## Best Practices for Users

1. Pin package versions in production.
2. Validate untrusted inputs before processing.
3. Do not treat parsed CSV content as trusted executable data.
