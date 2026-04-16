"""
memory_tracker.py — Background memory sampler for both process and container modes.

Process mode:
  - uses psutil to read RSS of the uvicorn process tree

Container mode:
  - uses `docker stats --no-stream` to read memory reported for the container
  - this reflects memory inside the Linux guest/container boundary only
  - it does NOT include Docker Desktop's macOS VM overhead such as
    `com.docker.virtualization`

Samples every SAMPLE_INTERVAL_S seconds in a background thread.
Results written to results/memory_<mode>_<timestamp>.csv.
"""

import argparse
import csv
import subprocess
import threading
import time
from datetime import datetime
from pathlib import Path

try:
    import psutil
except ImportError:
    psutil = None  # type: ignore

SAMPLE_INTERVAL_S = 1.0
RESULTS_DIR = Path(__file__).parent / "results"

CSV_FIELDNAMES = [
    "timestamp",
    "mode",
    "elapsed_s",
    "memory_mb",
    "measurement_scope",
]


class MemoryTracker:
    """
    Thread-safe background memory sampler.
    Call start() to begin sampling, stop() to halt, save() to write CSV.
    """

    def __init__(
        self,
        mode: str,
        pid: int | None = None,
        container_name: str | None = None,
        label: str = "",
    ):
        if mode not in ("process", "container"):
            raise ValueError(f"mode must be 'process' or 'container', got: {mode}")
        if mode == "process" and pid is None:
            raise ValueError("pid is required for process mode")
        if mode == "container" and container_name is None:
            raise ValueError("container_name is required for container mode")

        self.mode = mode
        self.pid = pid
        self.container_name = container_name
        self.label = label

        self._samples: list[dict] = []
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._start_time: float | None = None

    def start(self) -> None:
        """Begin background sampling in a daemon thread."""
        self._start_time = time.monotonic()
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._sample_loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Signal the sampling thread to stop and wait for it to finish."""
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=SAMPLE_INTERVAL_S * 3)
        self._thread = None

    def save(self) -> Path:
        """
        Write collected samples to a timestamped CSV file.
        Returns the path of the written file.
        """
        RESULTS_DIR.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        suffix = f"_{self.label}" if self.label else ""
        out_path = RESULTS_DIR / f"memory_{self.mode}{suffix}_{ts}.csv"

        with open(out_path, "w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=CSV_FIELDNAMES)
            writer.writeheader()
            writer.writerows(self._samples)

        print(f"[memory_tracker] Saved {len(self._samples)} samples → {out_path}")
        return out_path

    @property
    def samples(self) -> list[dict]:
        return list(self._samples)

    def _sample_loop(self) -> None:
        """Called in the background thread. Samples memory periodically."""
        while not self._stop_event.is_set():
            memory_mb = self._read_memory()
            if memory_mb is not None and self._start_time is not None:
                elapsed = time.monotonic() - self._start_time
                self._samples.append(
                    {
                        "timestamp": datetime.now().isoformat(timespec="milliseconds"),
                        "mode": self.mode,
                        "elapsed_s": round(elapsed, 2),
                        "memory_mb": round(memory_mb, 2),
                        "measurement_scope": self._measurement_scope(),
                    }
                )
            self._stop_event.wait(timeout=SAMPLE_INTERVAL_S)

    def _measurement_scope(self) -> str:
        if self.mode == "process":
            return "host_process_tree_rss"
        return "docker_stats_container_only_excludes_host_vm"

    def _read_memory(self) -> float | None:
        if self.mode == "process":
            return self._read_process_memory()
        return self._read_container_memory()

    def _read_process_memory(self) -> float | None:
        """
        Read RSS of the target PID and all its children.
        Returns total RSS in MB. Returns None if the process is no longer alive.
        """
        if psutil is None:
            print("[memory_tracker] WARNING: psutil not installed — skipping process memory")
            return None
        try:
            proc = psutil.Process(self.pid)
            children = proc.children(recursive=True)
            total_rss = proc.memory_info().rss
            for child in children:
                try:
                    total_rss += child.memory_info().rss
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
            return total_rss / (1024 * 1024)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            return None

    def _read_container_memory(self) -> float | None:
        """
        Read memory reported by `docker stats` for the named container.
        This intentionally captures the in-container view only; host-side VM
        overhead must be accounted for separately in analysis.
        """
        try:
            result = subprocess.run(
                [
                    "docker",
                    "stats",
                    "--no-stream",
                    "--format",
                    "{{.MemUsage}}",
                    self.container_name,
                ],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if result.returncode != 0 or not result.stdout.strip():
                return None

            usage_str = result.stdout.strip().split("/")[0].strip()
            return _parse_docker_memory(usage_str)
        except (subprocess.TimeoutExpired, FileNotFoundError):
            return None

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *_):
        self.stop()
        self.save()


def _parse_docker_memory(value: str) -> float:
    """
    Convert docker stats memory string to MB.
    Examples: "12.5MiB" → 12.5, "1.2GiB" → 1228.8, "512KiB" → 0.5
    """
    value = value.strip()
    if value.endswith("GiB"):
        return float(value[:-3]) * 1024
    if value.endswith("MiB"):
        return float(value[:-3])
    if value.endswith("KiB"):
        return float(value[:-3]) / 1024
    if value.endswith("GB"):
        return float(value[:-2]) * 1000
    if value.endswith("MB"):
        return float(value[:-2])
    if value.endswith("KB"):
        return float(value[:-2]) / 1000
    if value.endswith("B"):
        return float(value[:-1]) / (1024 * 1024)
    return float(value) / (1024 * 1024)


def main():
    parser = argparse.ArgumentParser(description="Background memory sampler")
    parser.add_argument("--mode", choices=["process", "container"], required=True)
    parser.add_argument("--pid", type=int, help="PID for process mode")
    parser.add_argument("--container", help="Container name for container mode")
    parser.add_argument("--duration", type=float, default=30, help="Seconds to sample")
    parser.add_argument("--label", default="", help="Label for output file")
    args = parser.parse_args()

    tracker = MemoryTracker(
        mode=args.mode,
        pid=args.pid,
        container_name=args.container,
        label=args.label,
    )

    print(f"[memory_tracker] Sampling {args.mode} memory for {args.duration}s …")
    tracker.start()
    time.sleep(args.duration)
    tracker.stop()
    path = tracker.save()
    print(f"[memory_tracker] Done. Results written to {path}")


if __name__ == "__main__":
    main()
