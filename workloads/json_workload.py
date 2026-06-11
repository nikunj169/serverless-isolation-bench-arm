"""JSON parsing and traversal workload."""

import json
from typing import Any

from workloads.base import Workload


class JSONWorkload(Workload):
    def compute(self, body: bytes) -> dict[str, Any]:
        obj = json.loads(body)
        keys, strings, numbers, depth = _traverse(obj, depth=1)
        return {
            "keys": keys,
            "strings": strings,
            "numbers": numbers,
            "depth": depth,
        }


def _traverse(obj: Any, depth: int) -> tuple[int, int, int, int]:
    keys = 0
    strings = 0
    numbers = 0
    max_depth = depth

    if isinstance(obj, dict):
        keys += len(obj)
        for value in obj.values():
            child_keys, child_strings, child_numbers, child_depth = _traverse(value, depth + 1)
            keys += child_keys
            strings += child_strings
            numbers += child_numbers
            max_depth = max(max_depth, child_depth)
    elif isinstance(obj, list):
        for item in obj:
            child_keys, child_strings, child_numbers, child_depth = _traverse(item, depth + 1)
            keys += child_keys
            strings += child_strings
            numbers += child_numbers
            max_depth = max(max_depth, child_depth)
    elif isinstance(obj, str):
        strings += 1
    elif isinstance(obj, bool):
        pass
    elif isinstance(obj, (int, float)):
        numbers += 1

    return keys, strings, numbers, max_depth
