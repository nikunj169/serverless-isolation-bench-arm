"""Workload implementations for the serverless benchmark."""

import os

from workloads.base import Workload
from workloads.json_workload import JSONWorkload
from workloads.matrix import MatrixWorkload
from workloads.ml_inference import MLInferenceWorkload
from workloads.sha256 import SHA256Workload

WORKLOAD_REGISTRY: dict[str, type[Workload]] = {
    "sha256": SHA256Workload,
    "json": JSONWorkload,
    "matrix": MatrixWorkload,
    "ml": MLInferenceWorkload,
}


def get_workload() -> Workload:
    """Instantiate the workload selected by the WORKLOAD environment variable."""
    name = os.getenv("WORKLOAD", "sha256").lower()
    try:
        workload_cls = WORKLOAD_REGISTRY[name]
    except KeyError as exc:
        supported = ", ".join(sorted(WORKLOAD_REGISTRY))
        raise ValueError(
            f"Unknown WORKLOAD={name!r}. Supported values: {supported}"
        ) from exc
    return workload_cls()
