"""SHA256 hashing workload (original benchmark)."""

import hashlib
from typing import Any

from workloads.base import Workload


class SHA256Workload(Workload):
    def compute(self, body: bytes) -> dict[str, Any]:
        digest = hashlib.sha256(body).hexdigest()
        return {
            "sha256": digest,
            "payload_bytes": len(body),
        }
