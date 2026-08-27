#!/usr/bin/env bash
set -euo pipefail

image_name=bun-panda-x86-study:1.4.0
result_dir=${BUN_PANDA_CGROUP_RESULTS:-paper/data/linux/cgroup}
limits=${BUN_PANDA_CGROUP_LIMITS:-256m,512m,1024m}
row_sizes=${BUN_PANDA_CGROUP_ROWS:-100000,250000}
mkdir -p "$result_dir"
result_abs=$(cd "$result_dir" && pwd)

docker build --platform linux/amd64 -f paper/artifact/linux/Dockerfile.x86 -t "$image_name" .

IFS=',' read -r -a limit_values <<< "$limits"
IFS=',' read -r -a row_values <<< "$row_sizes"
for limit in "${limit_values[@]}"; do
  for rows in "${row_values[@]}"; do
    cell_name="limit-${limit}-rows-${rows}"
    container_name="bun-panda-${limit}-${rows}"
    docker create \
      --name "$container_name" \
      --platform linux/amd64 \
      --cpus 2 \
      --memory "$limit" \
      --memory-swap "$limit" \
      --volume "$result_abs:/results" \
      "$image_name" \
      bash paper/artifact/linux/run-cgroup-cell.sh "$cell_name" "$rows" >/dev/null
    docker start --attach "$container_name" || true
    docker inspect "$container_name" > "$result_dir/${cell_name}-container-inspect.json"
    docker rm "$container_name" >/dev/null
    echo "completed $cell_name"
  done
done

echo "cgroup study completed in $result_dir"
