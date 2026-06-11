#!/usr/bin/env python3

import argparse
import csv
import statistics
import subprocess
import threading
import time
from pathlib import Path

import psutil

from bench_utils import (
    DEFAULT_MEMORY_DURATION_S,
    DEFAULT_MEMORY_INTERVAL_S,
    PAYLOAD_SIZES_KB,
    detect_platform,
    ensure_output_dir,
    make_payload,
    managed_server,
    parse_docker_memory_to_mb,
    post_compute,
    utc_timestamp,
    warm_server,
)

CSV_FIELDNAMES = [
    "timestamp",
    "platform",
    "mode",
    "payload_size_kb",
    "elapsed_s",
    "memory_mb",
    "measurement_scope",
]

MIN_REQUIRED_SAMPLES = 50
MAX_EXTRA_COLLECTION_S = 60


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect memory under warm benchmark load.")
    parser.add_argument("--mode", choices=["process", "container"], required=True)
    parser.add_argument("--payload-kb", type=int, choices=PAYLOAD_SIZES_KB, required=True)
    parser.add_argument(
        "--platform",
        choices=["m1_dockerdesktop", "oracle_arm64_linux"],
        help="Platform label. Auto-detected if omitted.",
    )
    parser.add_argument("--output-dir", default="results")
    return parser.parse_args()


def process_tree_rss_mb(pid: int) -> float:
    proc = psutil.Process(pid)
    total_rss = proc.memory_info().rss
    for child in proc.children(recursive=True):
        try:
            total_rss += child.memory_info().rss
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return total_rss / (1024 * 1024)


def read_container_memory_mb(container_id: str) -> float:
    result = subprocess.run(
        [
            "docker",
            "stats",
            "--no-stream",
            "--format",
            "{{.MemUsage}}",
            container_id,
        ],
        capture_output=True,
        text=True,
        timeout=3,
        check=False,
    )
    if result.returncode != 0 or not result.stdout.strip():
        raise RuntimeError(result.stderr.strip() or "docker stats returned no data")
    return parse_docker_memory_to_mb(result.stdout.strip())


def memory_sampler(
    rows: list[dict],
    stop_event: threading.Event,
    started_at: float,
    platform: str,
    mode: str,
    payload_kb: int,
    pid: int | None,
    container_id: str | None,
) -> None:
    scope = "host_process_tree_rss" if mode == "process" else "docker_stats_container_only"
    sample_index = 0
    while not stop_event.is_set():
        target_time = started_at + (sample_index * DEFAULT_MEMORY_INTERVAL_S)
        remaining = target_time - time.monotonic()
        if remaining > 0 and stop_event.wait(remaining):
            break
        try:
            memory_mb = (
                process_tree_rss_mb(pid) if mode == "process" else read_container_memory_mb(container_id)
            )
            rows.append(
                {
                    "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                    "platform": platform,
                    "mode": mode,
                    "payload_size_kb": payload_kb,
                    "elapsed_s": round(time.monotonic() - started_at, 2),
                    "memory_mb": round(memory_mb, 3),
                    "measurement_scope": scope,
                }
            )
        except Exception as exc:
            print(f"[memory] sample skipped: {exc}")
        sample_index += 1


def run_warm_requests(url: str, payload: bytes, total_requests: int = 100) -> None:
    completed = 0
    while completed < total_requests:
        post_compute(url, payload)
        completed += 1


def write_csv(output_dir: Path, platform: str, mode: str, payload_kb: int, rows: list[dict]) -> Path:
    output_path = output_dir / f"memory_{platform}_{mode}_{payload_kb}kb_{utc_timestamp()}.csv"
    with output_path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)
    return output_path


def print_summary(rows: list[dict]) -> None:
    values = [row["memory_mb"] for row in rows]
    print(
        "memory_mb summary: "
        f"mean={statistics.mean(values):.3f} "
        f"std={statistics.stdev(values) if len(values) > 1 else 0.0:.3f} "
        f"min={min(values):.3f} "
        f"max={max(values):.3f}"
    )


def main() -> None:
    args = parse_args()
    platform = args.platform or detect_platform()
    output_dir = ensure_output_dir(args.output_dir)
    payload = make_payload(args.payload_kb)
    rows: list[dict] = []

    with managed_server(args.mode) as server:
        warm_server(server.url, payload, request_count=20)
        stop_event = threading.Event()
        started_at = time.monotonic()
        sampler = threading.Thread(
            target=memory_sampler,
            args=(
                rows,
                stop_event,
                started_at,
                platform,
                args.mode,
                args.payload_kb,
                server.process.pid if server.process else None,
                server.container_id,
            ),
            daemon=True,
        )
        sampler.start()

        benchmark_thread = threading.Thread(
            target=run_warm_requests,
            args=(server.url, payload),
            daemon=True,
        )
        benchmark_thread.start()

        while time.monotonic() - started_at < DEFAULT_MEMORY_DURATION_S:
            time.sleep(0.2)

        extra_deadline = time.monotonic() + MAX_EXTRA_COLLECTION_S
        while len(rows) < MIN_REQUIRED_SAMPLES and time.monotonic() < extra_deadline:
            time.sleep(0.2)

        stop_event.set()
        sampler.join(timeout=5)
        benchmark_thread.join(timeout=5)

    if not rows:
        raise RuntimeError("No memory samples were collected.")

    output_path = write_csv(output_dir, platform, args.mode, args.payload_kb, rows)
    print(f"Wrote {len(rows)} memory samples to {output_path}")
    print_summary(rows)


if __name__ == "__main__":
    main()
