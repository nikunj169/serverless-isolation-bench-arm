"""Benchmark experiment metadata helpers."""

import os
from typing import Any

from bench_utils import iso_timestamp

PAYLOAD_SIZES_KB = [1, 10, 100, 1024]


def format_payload_size(payload_size_kb: int) -> str:
    if payload_size_kb >= 1024:
        return "1MB"
    return f"{payload_size_kb}KB"


def _parse_matrix_size() -> int | None:
    raw = os.environ.get("MATRIX_SIZE")
    if raw is None:
        return None
    return int(raw)


def _model_type_for_workload(workload: str) -> str | None:
    if workload == "ml":
        return "LogisticRegression"
    return None


def collect_benchmark_metadata() -> dict[str, Any]:
    """Build metadata describing the current benchmark experiment configuration."""
    workload = os.getenv("WORKLOAD", "sha256").lower()
    matrix_size = _parse_matrix_size() if workload == "matrix" else None
    model_type = _model_type_for_workload(workload)
    timestamp = iso_timestamp()

    runs = []
    for payload_size_kb in PAYLOAD_SIZES_KB:
        runs.append(
            {
                "workload": workload,
                "payload_size_kb": payload_size_kb,
                "payload_size": format_payload_size(payload_size_kb),
                "matrix_size": matrix_size,
                "model_type": model_type,
                "timestamp": timestamp,
            }
        )

    return {
        "experiment": {
            "workload": workload,
            "matrix_size": matrix_size,
            "model_type": model_type,
            "timestamp": timestamp,
            "payload_sizes_kb": PAYLOAD_SIZES_KB,
        },
        "runs": runs,
    }
