"""CPU-bound matrix multiplication workload."""

import os
from typing import Any

import numpy as np

from workloads.base import Workload

VALID_MATRIX_SIZES = (256, 512, 768, 1024)
MATRIX_SEED = 42
DEFAULT_MATRIX_SIZE = 256


def resolve_matrix_size() -> int:
    """Return the fixed matrix dimension for this experiment."""
    raw = os.environ.get("MATRIX_SIZE")
    if raw is None:
        return DEFAULT_MATRIX_SIZE

    try:
        size = int(raw)
    except ValueError as exc:
        raise ValueError(f"MATRIX_SIZE must be an integer, got {raw!r}") from exc

    if size not in VALID_MATRIX_SIZES:
        supported = ", ".join(str(value) for value in VALID_MATRIX_SIZES)
        raise ValueError(f"MATRIX_SIZE must be one of {supported}, got {size}")
    return size


class MatrixWorkload(Workload):
    def __init__(self) -> None:
        self._matrix_a: np.ndarray | None = None
        self._matrix_b: np.ndarray | None = None
        self._matrix_size = resolve_matrix_size()

    def startup(self) -> None:
        rng = np.random.RandomState(MATRIX_SEED)
        matrix_a = None
        matrix_b = None
        for dim in VALID_MATRIX_SIZES:
            candidate_a = rng.randn(dim, dim).astype(np.float64)
            candidate_b = rng.randn(dim, dim).astype(np.float64)
            if dim == self._matrix_size:
                matrix_a = candidate_a
                matrix_b = candidate_b
        if matrix_a is None or matrix_b is None:
            raise RuntimeError(f"Failed to initialize matrices for size {self._matrix_size}")
        self._matrix_a = matrix_a
        self._matrix_b = matrix_b

    def compute(self, body: bytes) -> dict[str, Any]:
        del body  # Payload size is independent of matrix computation size.
        if self._matrix_a is None or self._matrix_b is None:
            raise RuntimeError("Matrix workload is not initialized")

        result = self._matrix_a @ self._matrix_b
        checksum = float(np.sum(result))
        return {"checksum": checksum}
