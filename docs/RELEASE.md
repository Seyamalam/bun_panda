# Release Instructions

## Pre-Release Checklist

1. Update `CHANGELOG.md`.
2. Confirm version in `package.json`.
3. Run full checks:

```bash
bun run check
bun run parity
bun run pack:smoke
```

4. Validate API docs reflect current exports.
5. Confirm `git status --short` is clean.

## Versioning

Use semantic versioning:

- `MAJOR`: breaking API changes.
- `MINOR`: backward-compatible features.
- `PATCH`: backward-compatible fixes.

## Publishing (when ready)

```bash
bun publish
```

The package publishes as the unscoped npm name `bun_panda`. The `prepublishOnly` script runs `bun run check` again before the registry upload.
