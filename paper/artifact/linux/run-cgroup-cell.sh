#!/usr/bin/env bash
set -euo pipefail

cell_name=$1
rows=$2
result_dir=/results
cgroup_relative=$(awk -F: '$1 == "0" { print $3 }' /proc/self/cgroup)
cgroup_path="/sys/fs/cgroup${cgroup_relative}"

mkdir -p "$result_dir"
cat "$cgroup_path/memory.max" > "$result_dir/${cell_name}-memory-max.txt"
cat "$cgroup_path/memory.swap.max" > "$result_dir/${cell_name}-memory-swap-max.txt"
cat "$cgroup_path/memory.events" > "$result_dir/${cell_name}-memory-events-before.txt"

BUN_PANDA_FRESH_SIZES="$rows" \
BUN_PANDA_FRESH_PROCESSES=${BUN_PANDA_CGROUP_PROCESSES:-3} \
BUN_PANDA_FRESH_ITERATIONS=${BUN_PANDA_CGROUP_ITERATIONS:-5} \
BUN_PANDA_FRESH_WARMUPS=${BUN_PANDA_CGROUP_WARMUPS:-2} \
BUN_PANDA_FRESH_OUTPUT="$result_dir/${cell_name}-study.json" \
  bun run bench:fresh

cat "$cgroup_path/memory.current" > "$result_dir/${cell_name}-memory-current.txt"
cat "$cgroup_path/memory.peak" > "$result_dir/${cell_name}-memory-peak.txt"
cat "$cgroup_path/memory.events" > "$result_dir/${cell_name}-memory-events-after.txt"
