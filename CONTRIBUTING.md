# Contributing to bun_panda

Thanks for contributing.

## Setup

1. Install Bun (v1.3+ recommended).
2. Install dependencies:

```bash
bun install
```

3. Run checks before committing:

```bash
bun run check
```

## Development Guidelines

1. Keep API names pandas-aligned when practical.
2. Prefer small, focused PRs.
3. Add tests for behavioral changes.
4. Update docs when adding or changing public API.
5. Avoid introducing breaking changes without discussion.

## Pull Request Checklist

1. Tests added/updated.
2. `bun run check` passes.
3. README/docs updated if API changed.
4. Changelog entry added for notable changes.
5. Scope fits current roadmap (`docs/TODO.md`).

## Commit Message Style

Use concise, imperative messages:

- `feat(dataframe): add value_counts`
- `fix(csv): handle quoted separators`
- `docs: update API examples`

## Questions and Ideas

Open an issue with:

1. Problem statement.
2. Proposed API shape.
3. Example usage.
