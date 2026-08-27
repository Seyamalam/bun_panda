#!/usr/bin/env python3
"""Execute the language-neutral conformance corpus with pinned pandas."""

from __future__ import annotations

import argparse
import json
import math
import os
import platform
from datetime import date, datetime
from typing import Any

import numpy as np
import pandas as pd
from pandas.api import types as ptypes


def decode_cell(value: Any) -> Any:
    if isinstance(value, dict) and "$number" in value:
        tag = value["$number"]
        if tag == "NaN":
            return float("nan")
        if tag == "Infinity":
            return float("inf")
        return float("-inf")
    return value


def encode_cell(value: Any) -> Any:
    if value is pd.NA or value is pd.NaT or value is None:
        return None
    if isinstance(value, np.generic):
        value = value.item()
    if isinstance(value, float):
        if math.isnan(value):
            return {"$number": "NaN"}
        if math.isinf(value):
            return {"$number": "Infinity" if value > 0 else "-Infinity"}
        if value == 0:
            return 0
    if isinstance(value, (pd.Timestamp, datetime, date)):
        return value.isoformat()
    if isinstance(value, (bool, int, float, str)):
        return value
    return str(value)


def frame_from_spec(spec: dict[str, Any]) -> pd.DataFrame:
    rows = [
        {column: decode_cell(row.get(column)) for column in spec["columns"]}
        for row in spec["rows"]
    ]
    return pd.DataFrame(rows, columns=spec["columns"], index=spec["index"])


def dtype_family(series: pd.Series) -> str:
    dtype = series.dtype
    if ptypes.is_bool_dtype(dtype):
        return "boolean"
    if ptypes.is_numeric_dtype(dtype):
        return "number"
    if ptypes.is_datetime64_any_dtype(dtype):
        return "datetime"
    if ptypes.is_string_dtype(dtype) and not ptypes.is_object_dtype(dtype):
        return "string"
    present = [value for value in series.tolist() if not pd.isna(value)]
    if not present:
        return "unknown"
    kinds = {"boolean" if isinstance(value, (bool, np.bool_)) else
             "number" if isinstance(value, (int, float, np.number)) else
             "string" if isinstance(value, str) else
             "datetime" if isinstance(value, (pd.Timestamp, datetime, date)) else
             "mixed" for value in present}
    return next(iter(kinds)) if len(kinds) == 1 else "mixed"


def canonical_frame(frame: pd.DataFrame) -> dict[str, Any]:
    columns = [str(column) for column in frame.columns.tolist()]
    return {
        "kind": "frame",
        "columns": columns,
        "index": [encode_cell(value) for value in frame.index.tolist()],
        "data": [
            [encode_cell(value) for value in row]
            for row in frame.itertuples(index=False, name=None)
        ],
        "dtypeFamilies": [dtype_family(frame.iloc[:, position]) for position in range(len(columns))],
        "nativeDtypes": [str(dtype) for dtype in frame.dtypes.tolist()],
    }


def classify_error(error: Exception) -> str:
    message = str(error).lower()
    if isinstance(error, KeyError) and message.strip("'\"") == "invalid":
        return "invalid-argument"
    if isinstance(error, KeyError) or "not found" in message or "does not exist" in message:
        return "missing-label"
    if isinstance(error, NotImplementedError) or "not supported" in message:
        return "unsupported"
    if isinstance(error, (TypeError, ValueError)):
        return "invalid-argument"
    return "runtime-error"


def execute(frame: pd.DataFrame, right: pd.DataFrame | None, operation: dict[str, Any]) -> pd.DataFrame:
    name = operation["name"]
    args = operation["args"]
    if name == "sort_values":
        return frame.sort_values(
            by=args["by"],
            ascending=args.get("ascending", True),
            na_position=args.get("na_position", "last"),
            kind="stable",
        )
    if name == "dropna":
        return frame.dropna(subset=args.get("subset"))
    if name == "fillna":
        return frame.fillna(value=args["value"])
    if name == "drop_duplicates":
        return frame.drop_duplicates(
            subset=args.get("subset"),
            keep=args.get("keep", "first"),
            ignore_index=args.get("ignore_index", False),
        )
    if name == "shift":
        return frame.shift(periods=args.get("periods", 1))
    if name == "diff":
        return frame.diff(periods=args.get("periods", 1))
    if name == "rank":
        return frame.rank(
            method=args.get("method", "average"),
            ascending=args.get("ascending", True),
            na_option=args.get("na_option", "keep"),
            pct=args.get("pct", False),
        )
    if name == "groupby_agg":
        return frame.groupby(
            by=args["by"],
            dropna=args.get("dropna", True),
            sort=args.get("sort", True),
            as_index=False,
        ).agg(args["spec"])
    if name == "value_counts":
        normalize = args.get("normalize", False)
        value_name = "proportion" if normalize else "count"
        result = frame.value_counts(
            subset=args.get("subset"),
            normalize=normalize,
            dropna=args.get("dropna", True),
            sort=args.get("sort", True),
            ascending=args.get("ascending", False),
        )
        return result.rename(value_name).reset_index()
    if name == "merge":
        if right is None:
            raise ValueError("merge requires a right input")
        return frame.merge(
            right,
            on=args["on"],
            how=args.get("how", "inner"),
            suffixes=("_x", "_y"),
            sort=False,
        )
    raise NotImplementedError(f"unsupported operation '{name}'")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cases", default="paper/data/conformance/cases.json")
    parser.add_argument("--out", default="paper/data/conformance/pandas.json")
    args = parser.parse_args()

    with open(args.cases, "r", encoding="utf-8") as handle:
        corpus = json.load(handle)

    results: list[dict[str, Any]] = []
    for test_case in corpus["cases"]:
        frame = frame_from_spec(test_case["input"])
        right = frame_from_spec(test_case["right"]) if "right" in test_case else None
        before = json.dumps(canonical_frame(frame), sort_keys=True)
        right_before = json.dumps(canonical_frame(right), sort_keys=True) if right is not None else None
        common = {
            "id": test_case["id"],
            "family": test_case["family"],
            "classification": test_case["classification"],
        }
        try:
            output = execute(frame, right, test_case["operation"])
            results.append({
                **common,
                "status": "ok",
                "output": canonical_frame(output),
                "inputPreserved": (
                    before == json.dumps(canonical_frame(frame), sort_keys=True)
                    and right_before == (json.dumps(canonical_frame(right), sort_keys=True) if right is not None else None)
                ),
            })
        except Exception as error:  # the exception is part of the observation
            results.append({
                **common,
                "status": "error",
                "error": {
                    "category": classify_error(error),
                    "nativeType": type(error).__name__,
                    "message": str(error),
                },
                "inputPreserved": (
                    before == json.dumps(canonical_frame(frame), sort_keys=True)
                    and right_before == (json.dumps(canonical_frame(right), sort_keys=True) if right is not None else None)
                ),
            })

    payload = {
        "schemaVersion": "1.0.0",
        "implementation": "pandas",
        "version": pd.__version__,
        "runtime": f"CPython {platform.python_version()}",
        "cases": len(results),
        "results": results,
    }
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, allow_nan=False)
        handle.write("\n")
    print(f"wrote {len(results)} pandas {pd.__version__} observations to {args.out}")


if __name__ == "__main__":
    main()
