"""Base workload interface."""

from abc import ABC, abstractmethod
from typing import Any


class Workload(ABC):
    """Common interface for all benchmark workloads."""

    def startup(self) -> None:
        """Optional one-time initialization before serving requests."""

    @abstractmethod
    def compute(self, body: bytes) -> dict[str, Any]:
        """Process a request body and return a JSON-serializable result."""
