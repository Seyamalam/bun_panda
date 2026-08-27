@echo off
where bun >nul 2>nul
if errorlevel 1 (
  echo Bun is required. Install it from https://bun.sh/docs/installation 1>&2
  exit /b 1
)

bun run scripts/reproduce-platform.ts %*
exit /b %errorlevel%
