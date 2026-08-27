#!/usr/bin/env bash
set -euo pipefail

result_dir=${BUN_PANDA_LINUX_RESULTS:-paper/data/linux}
mkdir -p "$result_dir"

bash paper/artifact/linux/capture-environment.sh "$result_dir/environment"
bun install --frozen-lockfile
if [[ ! -x bench/.venv/bin/python ]]; then
  python3 -m venv bench/.venv
fi
bench/.venv/bin/pip install -r paper/artifact/linux/requirements.lock

bun run check
bun run conformance

BUN_PANDA_FRESH_OUTPUT="$result_dir/fresh-process-ablation.json" \
  bun run bench:fresh

BUN_PANDA_COMPETITOR_OUTPUT="$result_dir/competitor-synthetic.json" \
  bun run bench:competitors

bun run workload:public
BUN_PANDA_COMPETITOR_DATASET=uci_bank \
BUN_PANDA_COMPETITOR_SIZES=45211 \
BUN_PANDA_COMPETITOR_OUTPUT="$result_dir/competitor-uci-bank.json" \
  bun run bench:competitors

cat /proc/stat > "$result_dir/environment/proc-stat-after.txt"
bun run artifact:checksums
echo "Linux suite completed in $result_dir"
