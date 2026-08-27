$ErrorActionPreference = "Stop"

if (-not (Get-Command bun -ErrorAction SilentlyContinue)) {
  Write-Error "Bun is required. Install it from https://bun.sh/docs/installation"
}

& bun run scripts/reproduce-platform.ts @args
exit $LASTEXITCODE
