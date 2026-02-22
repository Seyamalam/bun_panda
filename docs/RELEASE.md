# Release Instructions

## Pre-Release Checklist

1. Update `CHANGELOG.md`.
2. Confirm version in `package.json`.
3. Run full checks:

```bash
bun run check
```

4. Validate API docs reflect current exports.

## Versioning

Use semantic versioning:

- `MAJOR`: breaking API changes.
- `MINOR`: backward-compatible features.
- `PATCH`: backward-compatible fixes.

## Publishing (when ready)

```bash
bun publish
```

Before running publish, ensure package metadata URLs are set to real repository locations.
