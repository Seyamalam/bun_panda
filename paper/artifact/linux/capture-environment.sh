#!/usr/bin/env bash
set -euo pipefail

capture_dir=${1:-paper/data/linux/environment}
mkdir -p "$capture_dir"

date --iso-8601=seconds > "$capture_dir/date.txt"
uname -a > "$capture_dir/uname.txt"
uname -m > "$capture_dir/architecture.txt"
lscpu --json > "$capture_dir/lscpu.json"
cat /etc/os-release > "$capture_dir/os-release.txt"
systemd-detect-virt > "$capture_dir/virtualization.txt" 2>&1 || true
bun --version > "$capture_dir/bun-version.txt"
python3 --version > "$capture_dir/python-version.txt"
bench/.venv/bin/python -c 'import pandas; print(pandas.__version__)' > "$capture_dir/pandas-version.txt"
git rev-parse HEAD > "$capture_dir/git-commit.txt"
git status --porcelain > "$capture_dir/git-status.txt"
cat /proc/meminfo > "$capture_dir/meminfo.txt"
cat /proc/stat > "$capture_dir/proc-stat-before.txt"
cat /proc/cpuinfo > "$capture_dir/cpuinfo.txt"
find /sys/devices/system/cpu -maxdepth 2 -name scaling_governor -print -exec cat {} \; > "$capture_dir/cpu-governors.txt" 2>/dev/null || true

if [[ $(cat "$capture_dir/architecture.txt") != "x86_64" ]]; then
  echo "refusing publication run because architecture is not x86_64" >&2
  exit 2
fi

echo "captured environment in $capture_dir"
