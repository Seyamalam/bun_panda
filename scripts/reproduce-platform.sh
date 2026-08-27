#!/usr/bin/env sh
set -eu

if ! command -v bun >/dev/null 2>&1; then
  echo "Bun is required. Install it from https://bun.sh/docs/installation" >&2
  exit 1
fi

exec bun run scripts/reproduce-platform.ts "$@"
