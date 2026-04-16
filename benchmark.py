"""
benchmark.py — Core measurement harness for container vs. process isolation overhead.

Experimental matrix:
  2 modes  (process, container)
× 4 payload sizes (1 KB, 10 KB, 100 KB, 1 MB)
× 2 request types (cold_start × 30 runs, warm × 100 requests)
× 3 throughput bursts (concurrency 10, 50, 100)

Outputs:
  - results/benchmark_results.csv
  - results/throughput_results.csv

Usage:
  python benchmark.py
  python benchmark.py --mode process
  python benchmark.py --mode container
  python benchmark.py --cold-runs 5 --warm-runs 20
"""

import argparse
import concurrent.futures
import csv
import json
import random
import string
import time
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from docker_runner import DockerRunner
from process_runner import ProcessRunner

# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────

PAYLOAD_SIZES_KB = [1, 10, 100, 1024]
DEFAULT_COLD_RUNS = 30
DEFAULT_WARM_RUNS = 100
CONCURRENCY_LEVELS = [10, 50, 100]

PROCESS_PORT = 8000
CONTAINER_PORT = 8001

RESULTS_DIR = Path(__file__).parent / "results"
RESULTS_FILE = RESULTS_DIR / "benchmark_results.csv"
THROUGHPUT_RESULTS_FILE = RESULTS_DIR / "throughput_results.csv"

CSV_FIELDNAMES = ["mode", "payload_size_kb", "request_type", "run_id", "latency_ms"]
THROUGHPUT_FIELDNAMES = [
    "mode",
    "payload_size_kb",
    "concurrency_level",
    "total_requests",
    "total_time_s",
    "throughput_req_per_sec",
]


# ─────────────────────────────────────────────────────────────────────────────
# Payload generation
# ─────────────────────────────────────────────────────────────────────────────


def make_payload(target_kb: int) -> bytes:
    """
    Build a JSON payload of approximately `target_kb` kilobytes.
    """
    target_bytes = target_kb * 1024
    data_len = max(1, target_bytes - 20)
    data_str = "".join(random.choices(string.ascii_letters + string.digits, k=data_len))
    payload = json.dumps({"data": data_str}).encode("utf-8")
    if len(payload) < target_bytes:
        padding = target_bytes - len(payload)
        data_str += "A" * padding
        payload = json.dumps({"data": data_str}).encode("utf-8")
    return payload[:target_bytes] if len(payload) > target_bytes else payload


# ─────────────────────────────────────────────────────────────────────────────
# HTTP helpers
# ─────────────────────────────────────────────────────────────────────────────


def make_session() -> requests.Session:
    """
    Create a requests Session with retry logic.
    """
    session = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=0.3,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["POST"],
    )
    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=max(CONCURRENCY_LEVELS) + 10,
        pool_maxsize=max(CONCURRENCY_LEVELS) + 10,
    )
    session.mount("http://", adapter)
    return session


def post_with_timing(session: requests.Session, url: str, payload: bytes) -> float:
    """
    POST `payload` to `url` and return round-trip latency in milliseconds.
    """
    t0 = time.perf_counter()
    response = session.post(
        url,
        data=payload,
        headers={"Content-Type": "application/json"},
        timeout=30,
    )
    t1 = time.perf_counter()
    response.raise_for_status()
    return (t1 - t0) * 1000


def measure_single_cold_start(runner_factory, url: str, payload: bytes) -> float:
    """
    Start a fresh runtime, issue one request, stop the runtime, and return
    startup-plus-first-request latency in milliseconds.
    """
    runner = runner_factory()
    t0 = time.perf_counter()
    runner.start()
    try:
        session = make_session()
        post_with_timing(session, f"{url}/compute", payload)
        return (time.perf_counter() - t0) * 1000
    finally:
        runner.stop()


# ─────────────────────────────────────────────────────────────────────────────
# Measurement routines
# ─────────────────────────────────────────────────────────────────────────────


def run_cold_starts(
    runner_factory,
    url: str,
    payload: bytes,
    n_runs: int,
    mode: str,
    payload_kb: int,
    writer: csv.DictWriter,
) -> None:
    """
    Cold-start benchmark with one discarded warm-up run.
    """
    print(f"  [cold_start] mode={mode} payload={payload_kb}KB runs={n_runs}")

    try:
        # Drop the first cold start because Docker Desktop's VM bridge can pay
        # an initialization penalty in the network proxy path (for example
        # vpnkit), which creates a one-off outlier unrelated to steady cold-start
        # behavior and would otherwise skew the 30 recorded trials.
        discarded_ms = measure_single_cold_start(runner_factory, url, payload)
        print(f"    warm-up (discarded): {discarded_ms:.1f} ms")
    except Exception as exc:
        print(f"    WARNING discarded warm-up cold start failed: {exc}")
    time.sleep(0.5)

    for run_id in range(n_runs):
        try:
            total_cold_ms = measure_single_cold_start(runner_factory, url, payload)
        except Exception as exc:
            print(f"    WARNING run {run_id} failed: {exc}")
            time.sleep(0.5)
            continue

        writer.writerow(
            {
                "mode": mode,
                "payload_size_kb": payload_kb,
                "request_type": "cold_start",
                "run_id": run_id,
                "latency_ms": round(total_cold_ms, 3),
            }
        )
        print(f"    run {run_id:02d}: {total_cold_ms:.1f} ms")
        time.sleep(0.5)


def run_warm_requests(
    runner,
    url: str,
    payload: bytes,
    n_requests: int,
    mode: str,
    payload_kb: int,
    writer: csv.DictWriter,
) -> None:
    """
    Warm-request benchmark:
    Server is already running. Send `n_requests` sequential POSTs and record
    each latency individually.
    """
    print(f"  [warm] mode={mode} payload={payload_kb}KB requests={n_requests}")

    session = make_session()
    failed = 0

    for run_id in range(n_requests):
        try:
            latency_ms = post_with_timing(session, f"{url}/compute", payload)
        except Exception as exc:
            print(f"    WARNING request {run_id} failed: {exc}")
            failed += 1
            continue

        writer.writerow(
            {
                "mode": mode,
                "payload_size_kb": payload_kb,
                "request_type": "warm",
                "run_id": run_id,
                "latency_ms": round(latency_ms, 3),
            }
        )

    if failed:
        print(f"    {failed} / {n_requests} requests failed.")
    else:
        print(f"    All {n_requests} requests completed.")


def _concurrent_request(url: str, payload: bytes) -> float:
    """
    Worker used by the concurrent throughput experiment.
    Each thread owns its own Session to avoid cross-thread sharing issues.
    """
    session = make_session()
    return post_with_timing(session, f"{url}/compute", payload)


def measure_concurrent_throughput(
    mode: str,
    payload_size: int,
    concurrency_level: int,
    url: str,
    payload: bytes,
    writer: csv.DictWriter,
) -> None:
    """
    Measure burst throughput for a steady-state warm runtime.
    One request is issued per worker, and throughput is computed from the total
    wall-clock time for the batch.
    """
    print(
        "  [throughput] "
        f"mode={mode} payload={payload_size}KB concurrency={concurrency_level}"
    )

    started_at = time.perf_counter()
    latencies: list[float] = []
    failed = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency_level) as executor:
        futures = [
            executor.submit(_concurrent_request, url, payload)
            for _ in range(concurrency_level)
        ]
        for future in concurrent.futures.as_completed(futures):
            try:
                latencies.append(future.result())
            except Exception as exc:
                failed += 1
                print(f"    WARNING concurrent request failed: {exc}")

    total_time_s = time.perf_counter() - started_at
    successful_requests = len(latencies)
    throughput_req_per_sec = (
        successful_requests / total_time_s if total_time_s > 0 else 0.0
    )

    writer.writerow(
        {
            "mode": mode,
            "payload_size_kb": payload_size,
            "concurrency_level": concurrency_level,
            "total_requests": successful_requests,
            "total_time_s": round(total_time_s, 6),
            "throughput_req_per_sec": round(throughput_req_per_sec, 3),
        }
    )

    print(
        f"    completed={successful_requests}/{concurrency_level} "
        f"failed={failed} total_time={total_time_s:.3f}s "
        f"throughput={throughput_req_per_sec:.2f} req/s"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Mode orchestrators
# ─────────────────────────────────────────────────────────────────────────────


def benchmark_process(
    cold_runs: int,
    warm_runs: int,
    writer: csv.DictWriter,
    throughput_writer: csv.DictWriter,
) -> None:
    """Run the full benchmark matrix for PROCESS mode."""
    print("\n=== PROCESS MODE ===")
    url = f"http://127.0.0.1:{PROCESS_PORT}"

    for kb in PAYLOAD_SIZES_KB:
        payload = make_payload(kb)

        def process_factory(port=PROCESS_PORT):
            return ProcessRunner(port=port)

        run_cold_starts(
            runner_factory=process_factory,
            url=url,
            payload=payload,
            n_runs=cold_runs,
            mode="process",
            payload_kb=kb,
            writer=writer,
        )

        with ProcessRunner(port=PROCESS_PORT) as runner:
            run_warm_requests(
                runner=runner,
                url=url,
                payload=payload,
                n_requests=warm_runs,
                mode="process",
                payload_kb=kb,
                writer=writer,
            )

            for concurrency_level in CONCURRENCY_LEVELS:
                measure_concurrent_throughput(
                    mode="process",
                    payload_size=kb,
                    concurrency_level=concurrency_level,
                    url=url,
                    payload=payload,
                    writer=throughput_writer,
                )


def benchmark_container(
    cold_runs: int,
    warm_runs: int,
    writer: csv.DictWriter,
    throughput_writer: csv.DictWriter,
) -> None:
    """Run the full benchmark matrix for CONTAINER mode."""
    print("\n=== CONTAINER MODE ===")
    url = f"http://127.0.0.1:{CONTAINER_PORT}"

    for kb in PAYLOAD_SIZES_KB:
        payload = make_payload(kb)

        def container_factory(port=CONTAINER_PORT):
            return DockerRunner(host_port=port)

        run_cold_starts(
            runner_factory=container_factory,
            url=url,
            payload=payload,
            n_runs=cold_runs,
            mode="container",
            payload_kb=kb,
            writer=writer,
        )

        with DockerRunner(host_port=CONTAINER_PORT) as runner:
            run_warm_requests(
                runner=runner,
                url=url,
                payload=payload,
                n_requests=warm_runs,
                mode="container",
                payload_kb=kb,
                writer=writer,
            )

            for concurrency_level in CONCURRENCY_LEVELS:
                measure_concurrent_throughput(
                    mode="container",
                    payload_size=kb,
                    concurrency_level=concurrency_level,
                    url=url,
                    payload=payload,
                    writer=throughput_writer,
                )


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────


def parse_args():
    parser = argparse.ArgumentParser(
        description="Container vs Process isolation benchmark for ARM64 Mac"
    )
    parser.add_argument(
        "--mode",
        choices=["process", "container", "both"],
        default="both",
        help="Which execution mode to benchmark (default: both)",
    )
    parser.add_argument(
        "--cold-runs",
        type=int,
        default=DEFAULT_COLD_RUNS,
        help=f"Number of cold-start runs per payload size (default: {DEFAULT_COLD_RUNS})",
    )
    parser.add_argument(
        "--warm-runs",
        type=int,
        default=DEFAULT_WARM_RUNS,
        help=f"Number of warm requests per payload size (default: {DEFAULT_WARM_RUNS})",
    )
    parser.add_argument(
        "--skip-build",
        action="store_true",
        help="Skip Docker image build (assumes image already exists)",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    if args.mode in ("container", "both") and not args.skip_build:
        project_dir = str(Path(__file__).parent)
        DockerRunner.build_image(project_dir=project_dir)

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    benchmark_file_exists = RESULTS_FILE.exists()
    throughput_file_exists = THROUGHPUT_RESULTS_FILE.exists()

    with open(RESULTS_FILE, "a", newline="") as benchmark_csvfile, open(
        THROUGHPUT_RESULTS_FILE, "a", newline=""
    ) as throughput_csvfile:
        writer = csv.DictWriter(benchmark_csvfile, fieldnames=CSV_FIELDNAMES)
        throughput_writer = csv.DictWriter(
            throughput_csvfile,
            fieldnames=THROUGHPUT_FIELDNAMES,
        )

        if not benchmark_file_exists:
            writer.writeheader()
        if not throughput_file_exists:
            throughput_writer.writeheader()

        start_time = time.monotonic()

        if args.mode in ("process", "both"):
            benchmark_process(
                cold_runs=args.cold_runs,
                warm_runs=args.warm_runs,
                writer=writer,
                throughput_writer=throughput_writer,
            )

        if args.mode in ("container", "both"):
            benchmark_container(
                cold_runs=args.cold_runs,
                warm_runs=args.warm_runs,
                writer=writer,
                throughput_writer=throughput_writer,
            )

        elapsed = time.monotonic() - start_time
        print(f"\n✓ Benchmark complete in {elapsed:.1f}s")
        print(f"  Latency results written to: {RESULTS_FILE}")
        print(f"  Throughput results written to: {THROUGHPUT_RESULTS_FILE}")


if __name__ == "__main__":
    main()
