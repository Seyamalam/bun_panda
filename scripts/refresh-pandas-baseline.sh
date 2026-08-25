#!/usr/bin/env bash
# Refresh the committed pandas API baseline used by scripts/parity-audit.ts.
set -euo pipefail
cd "$(dirname "$0")/.."

curl -s "https://pandas.pydata.org/docs/reference/frame.html" -o /tmp/pd-frame.html
curl -s "https://pandas.pydata.org/docs/reference/series.html" -o /tmp/pd-series.html

grep -ohE 'pandas\.DataFrame\.[a-z_]+|pandas\.Series\.[a-z_]+' \
  /tmp/pd-frame.html /tmp/pd-series.html \
  | sed 's/.*://' | sort -u > scripts/pandas-api-baseline.txt

echo "scripts/pandas-api-baseline.txt: $(wc -l < scripts/pandas-api-baseline.txt) methods"
