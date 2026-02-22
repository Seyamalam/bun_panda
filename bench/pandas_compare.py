#!/usr/bin/env python3
import argparse
import json
import os
import time
from typing import Any, Callable, Dict, List

import pandas as pd


def lcg(seed: int) -> Callable[[], float]:
    state = seed & 0xFFFFFFFF

    def rnd() -> float:
        nonlocal state
        state = (1664525 * state + 1013904223) & 0xFFFFFFFF
        return state / 2**32

    return rnd


def build_dataset(row_count: int, skew: bool = False, wide: bool = False, high_cardinality: bool = False, include_missing: bool = False):
    rnd = lcg(42)
    groups = ["A", "B", "C", "D", "E", "F"]
    cities = ["Austin", "Seattle", "Boston", "Denver", "Miami"]
    segments = ["consumer", "enterprise", "startup"]

    rows: List[Dict[str, Any]] = []
    for idx in range(row_count):
        value = int(rnd() * 1000)
        weight = round(rnd() * 5 + 0.5, 2)
        row: Dict[str, Any] = {
            "id": idx,
            "group": "A" if skew and idx % 100 < 70 else groups[idx % len(groups)],
            "city": None if include_missing and idx % 23 == 0 else cities[idx % len(cities)],
            "segment": None if include_missing and idx % 31 == 0 else segments[idx % len(segments)],
            "value": value,
            "weight": weight,
            "revenue": round(value * weight, 2),
            "active": idx % 3 == 0,
            "bucket": "high" if value > 700 else "mid" if value > 350 else "low",
            "user_key": f"u_{idx}" if high_cardinality else f"u_{idx % 120}",
            "session_key": f"s_{idx * 7}" if high_cardinality else f"s_{idx % 300}",
        }
        if wide:
            for i in range(10):
                row[f"extra_{i}"] = (value + i) % (50 + i)
        rows.append(row)

    return pd.DataFrame(rows)


def run_case(name: str, fn: Callable[[], int], iterations: int) -> float:
    for _ in range(3):
        fn()

    times = []
    for _ in range(iterations):
        start = time.perf_counter()
        out = fn()
        if out is None:
            raise RuntimeError(f"case '{name}' returned None")
        times.append((time.perf_counter() - start) * 1000.0)
    return sum(times) / len(times)


def median(values: List[float]) -> float:
    if not values:
        return 0.0
    values = sorted(values)
    mid = len(values) // 2
    if len(values) % 2 == 1:
        return values[mid]
    return (values[mid - 1] + values[mid]) / 2.0


def main() -> None:
    parser = argparse.ArgumentParser(description="pandas benchmark companion for bun_panda")
    parser.add_argument("--rows", type=int, default=int(os.getenv("BUN_PANDA_BENCH_ROWS", "25000")))
    parser.add_argument("--iters", type=int, default=int(os.getenv("BUN_PANDA_BENCH_ITERS", "8")))
    parser.add_argument("--rounds", type=int, default=int(os.getenv("BUN_PANDA_BENCH_ROUNDS", "3")))
    parser.add_argument("--json-out", default=os.getenv("BUN_PANDA_PANDAS_JSON", "bench/results/pandas.json"))
    args = parser.parse_args()

    datasets = {
        "base": build_dataset(args.rows),
        "high_card": build_dataset(args.rows, high_cardinality=True),
        "missing": build_dataset(args.rows, include_missing=True),
    }

    cases = [
        {
            "name": "groupby_mean",
            "dataset": "base",
            "fn": lambda df: len(
                df.groupby("group", dropna=True, sort=False).agg({"value": "mean", "revenue": "sum"})
            ),
        },
        {
            "name": "filter_sort_top100",
            "dataset": "base",
            "fn": lambda df: len(df[(df["active"] == True) & (df["value"] > 500)].nlargest(100, columns="value")),
        },
        {
            "name": "sort_top1000",
            "dataset": "base",
            "fn": lambda df: len(df.nlargest(1000, columns="value")),
        },
        {
            "name": "sort_multicol_top800",
            "dataset": "base",
            "fn": lambda df: len(df.sort_values(["city", "value", "id"], ascending=[True, False, True]).head(800)),
        },
        {
            "name": "value_counts_city",
            "dataset": "base",
            "fn": lambda df: len(df.value_counts(subset=["city"], dropna=True)),
        },
        {
            "name": "value_counts_group_city_top10",
            "dataset": "base",
            "fn": lambda df: len(df.value_counts(subset=["group", "city"], dropna=True).head(10)),
        },
        {
            "name": "value_counts_missing_city_dropna_false",
            "dataset": "missing",
            "fn": lambda df: len(df.value_counts(subset=["city"], dropna=False)),
        },
        {
            "name": "groupby_missing_city_mean",
            "dataset": "missing",
            "fn": lambda df: len(df.groupby("city", dropna=False, sort=False).agg({"value": "mean"})),
        },
        {
            "name": "value_counts_high_card_city_top20",
            "dataset": "high_card",
            "fn": lambda df: len(df.value_counts(subset=["user_key", "city"], dropna=True).head(20)),
        },
        {
            "name": "value_counts_high_card_user_top100",
            "dataset": "high_card",
            "fn": lambda df: len(df.value_counts(subset=["user_key"], dropna=True).head(100)),
        },
    ]

    results = []
    for case in cases:
        df = datasets[case["dataset"]]
        round_averages = []
        for _ in range(args.rounds):
            round_averages.append(run_case(case["name"], lambda c=case, d=df: c["fn"](d), args.iters))
        avg_ms = median(round_averages)
        results.append(
            {
                "case": case["name"],
                "dataset": case["dataset"],
                "pandasAvgMs": avg_ms,
                "pandasRoundAverages": round_averages,
            }
        )

    payload = {
        "generatedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "rows": args.rows,
        "iterations": args.iters,
        "rounds": args.rounds,
        "cases": len(results),
        "results": results,
    }

    os.makedirs(os.path.dirname(args.json_out), exist_ok=True)
    with open(args.json_out, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
        f.write("\n")

    print("# bun_panda vs pandas benchmark")
    print(f"rows={args.rows}, iterations={args.iters}, rounds={args.rounds}, cases={len(results)}")
    print("")
    print("| case | dataset | pandas avg |")
    print("| --- | --- | ---: |")
    for entry in results:
        print(f"| {entry['case']} | {entry['dataset']} | {entry['pandasAvgMs']:.2f}ms |")


if __name__ == "__main__":
    main()
