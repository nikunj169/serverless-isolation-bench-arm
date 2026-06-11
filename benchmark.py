"""
benchmark.py — Core measurement harness for container vs. process isolation overhead.

This version keeps the original project structure but adds surgical fixes for:
  - loud failures instead of silent skips
  - explicit startup/reachability validation
  - debuggable process/container startup errors
  - warm-up requests and GC control for more stable measurements
  - post-run validation that both modes actually produced rows
"""

import argparse
import concurrent.futures
import csv
import gc
import json
import os
import random
import statistics
import string
import subprocess
import sys
import time
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from docker_runner import DockerRunner, FULL_IMAGE
from process_runner import HEALTH_POLL_INTERVAL, ProcessRunner, STARTUP_TIMEOUT

# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────

PAYLOAD_SIZES_KB = [1, 10, 100, 1024]
DEFAULT_COLD_RUNS = 30
DEFAULT_WARM_RUNS = 100
DEFAULT_WORKERS = 4
WARMUP_REQUESTS = 10
HEALTHCHECK_TIMEOUT_S = 10.0

PROCESS_PORT = 8000
CONTAINER_PORT = 8001

RESULTS_DIR = Path(__file__).parent / "results"
RESULTS_FILE = RESULTS_DIR / "benchmark_results.csv"
THROUGHPUT_RESULTS_FILE = RESULTS_DIR / "throughput_results.csv"
CURRENT_PLATFORM = "m1_dockerdesktop"

CSV_FIELDNAMES = ["platform", "mode", "payload_size_kb", "request_type", "run_id", "latency_ms"]
THROUGHPUT_FIELDNAMES = [
    "platform",
    "mode",
    "payload_size_kb",
    "concurrency_level",
    "total_requests",
    "total_time_s",
    "throughput_req_per_sec",
]
CONCURRENCY_LEVELS = [10, 50, 100]
TOTAL_THROUGHPUT_REQUESTS = 1000


# ─────────────────────────────────────────────────────────────────────────────
# Payload generation
# ─────────────────────────────────────────────────────────────────────────────


def make_payload(target_kb: int) -> bytes:
    """Build a JSON payload of exactly `target_kb` kilobytes."""
    target_bytes = target_kb * 1024
    data_len = max(1, target_bytes - 20)
    data_str = "".join(random.choices(string.ascii_letters + string.digits, k=data_len))
    payload = json.dumps({"data": data_str}).encode("utf-8")
    if len(payload) < target_bytes:
        data_str += "A" * (target_bytes - len(payload))
        payload = json.dumps({"data": data_str}).encode("utf-8")
    return payload[:target_bytes] if len(payload) > target_bytes else payload


# ─────────────────────────────────────────────────────────────────────────────
# HTTP helpers
# ─────────────────────────────────────────────────────────────────────────────


def make_session() -> requests.Session:
    """Shared Session config for all benchmark requests."""
    session = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=0.2,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["POST", "GET"],
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=32, pool_maxsize=32)
    session.mount("http://", adapter)
    return session


def post_with_timing(session: requests.Session, url: str, payload: bytes) -> float:
    """POST payload and return round-trip latency in milliseconds."""
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


def verify_compute_endpoint(url: str, mode: str) -> None:
    """
    Retry the /compute endpoint for up to 10 seconds before any measurements.
    This makes startup failures obvious instead of silently producing no rows.
    """
    print(f"  [healthcheck] verifying {mode} at {url}/compute")
    session = make_session()
    payload = b'{"data":"healthcheck"}'
    deadline = time.monotonic() + HEALTHCHECK_TIMEOUT_S
    last_error: Exception | None = None

    while time.monotonic() < deadline:
        try:
            response = session.post(
                f"{url}/compute",
                data=payload,
                headers={"Content-Type": "application/json"},
                timeout=2,
            )
            response.raise_for_status()
            print(f"  [healthcheck] {mode} reachable")
            return
        except Exception as exc:
            last_error = exc
            time.sleep(0.25)

    raise RuntimeError(
        f"{mode} server did not become reachable at {url}/compute within "
        f"{HEALTHCHECK_TIMEOUT_S:.0f}s: {last_error}"
    )


def _purge_page_cache() -> None:
    """
    Drop the macOS unified memory page cache between cold-start runs.
    Without this, the OS serves Python/uvicorn module reads from RAM
    by run 3-4, collapsing process cold-start latency from ~430ms to
    ~10ms and making the isolation delta meaningless.

    Requires passwordless sudo for 'purge'. To enable:
        sudo visudo
        Add line: <your_username> ALL=(ALL) NOPASSWD: /usr/bin/purge

    If purge is unavailable or fails, print a WARNING and continue.
    The benchmark will still run but cold-start results will be
    contaminated by page cache effects — note this in the paper.
    """
    try:
        result = subprocess.run(
            ["sudo", "purge"],
            capture_output=True,
            timeout=10,
        )
        if result.returncode != 0:
            print(
                "    WARNING: `sudo purge` failed (returncode="
                f"{result.returncode}). Cold-start results may be "
                "contaminated by page cache. Check sudo permissions."
            )
    except (FileNotFoundError, subprocess.TimeoutExpired) as exc:
        print(f"    WARNING: page cache purge unavailable: {exc}")


def _check_purge_available() -> bool:
    """Return True if passwordless sudo purge works."""
    try:
        result = subprocess.run(
            ["sudo", "-n", "purge"],
            capture_output=True,
            timeout=5,
        )
        return result.returncode == 0
    except Exception:
        return False


def _ensure_docker_desktop_ready(timeout_s: int = 60) -> None:
    """
    Confirm Docker Desktop VM is running and responsive before the
    container cold-start loop begins. This ensures consistent VM-warm
    state across all 30 cold-start runs.

    Polls `docker info` every 2 seconds until exit code 0.
    Raises RuntimeError if Docker Desktop is not ready within timeout_s.
    """
    print(f"  [docker_desktop_check] confirming Docker Desktop is ready...")
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        result = subprocess.run(
            ["docker", "info"],
            capture_output=True,
            timeout=5,
        )
        if result.returncode == 0:
            print(f"  [docker_desktop_check] Docker Desktop ready.")
            return
        time.sleep(2)
    raise RuntimeError(
        f"Docker Desktop did not become ready within {timeout_s}s. "
        f"Ensure Docker Desktop is running before starting the benchmark."
    )



def warm_up_runtime(url: str, mode: str, payload: bytes, count: int = WARMUP_REQUESTS) -> None:
    """
    Send unrecorded warm-up requests before recording latency.
    This reduces first-request jitter while keeping the recorded request count
    consistent across runs.
    """
    print(f"  [warmup] mode={mode} requests={count}")
    session = make_session()
    for run_id in range(count):
        try:
            post_with_timing(session, f"{url}/compute", payload)
        except Exception as exc:
            raise RuntimeError(
                f"Warm-up request {run_id + 1}/{count} failed for {mode}: {exc}"
            ) from exc


def _concurrent_request(url: str, payload: bytes) -> None:
    session = make_session()
    post_with_timing(session, f"{url}/compute", payload)


def measure_concurrent_throughput(
    mode: str,
    payload_size: int,
    concurrency_level: int,
    url: str,
    payload: bytes,
    writer: csv.DictWriter,
) -> None:
    """
    Preserve the existing throughput output file while making failures loud.
    """
    print(
        f"  [throughput] mode={mode} payload={payload_size}KB "
        f"concurrency={concurrency_level} total={TOTAL_THROUGHPUT_REQUESTS}"
    )

    batch_count = TOTAL_THROUGHPUT_REQUESTS // concurrency_level
    if batch_count * concurrency_level != TOTAL_THROUGHPUT_REQUESTS:
        raise RuntimeError(
            f"TOTAL_THROUGHPUT_REQUESTS={TOTAL_THROUGHPUT_REQUESTS} must be divisible "
            f"by concurrency_level={concurrency_level}"
        )

    started_at = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency_level) as executor:
        for batch_id in range(batch_count):
            futures = [
                executor.submit(_concurrent_request, url, payload)
                for _ in range(concurrency_level)
            ]
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as exc:
                    raise RuntimeError(
                        f"{mode} throughput request failed in batch {batch_id + 1}/{batch_count}: {exc}"
                    ) from exc

    total_time_s = time.perf_counter() - started_at
    throughput = TOTAL_THROUGHPUT_REQUESTS / total_time_s if total_time_s > 0 else 0.0
    writer.writerow(
        {
            "platform": CURRENT_PLATFORM,
            "mode": mode,
            "payload_size_kb": payload_size,
            "concurrency_level": concurrency_level,
            "total_requests": TOTAL_THROUGHPUT_REQUESTS,
            "total_time_s": round(total_time_s, 6),
            "throughput_req_per_sec": round(throughput, 3),
        }
    )
    print(f"    throughput={throughput:.2f} req/s total_time={total_time_s:.3f}s")


# ─────────────────────────────────────────────────────────────────────────────
# Robust runner wrappers
# ─────────────────────────────────────────────────────────────────────────────


class RobustProcessRunner(ProcessRunner):
    """
    Process runner with captured stdout/stderr and explicit crash detection.
    This keeps benchmark.py debuggable without changing the rest of the project.
    """

    def __init__(self, port: int = PROCESS_PORT, workers: int = DEFAULT_WORKERS):
        super().__init__(port=port)
        self.workers = workers
        self._captured_stdout = ""
        self._captured_stderr = ""

    def _read_captured_output(self) -> tuple[str, str]:
        if self.process is None:
            return self._captured_stdout, self._captured_stderr
        stdout, stderr = self.process.communicate(timeout=1)
        self._captured_stdout = stdout or ""
        self._captured_stderr = stderr or ""
        return self._captured_stdout, self._captured_stderr

    def start(self) -> None:
        if self.process is not None:
            raise RuntimeError("ProcessRunner already started. Call stop() first.")

        cmd = [
            sys.executable,
            "-m",
            "uvicorn",
            "app:app",
            "--host",
            "127.0.0.1",
            "--port",
            str(self.port),
            "--workers",
            str(self.workers),
            "--log-level",
            "error",
        ]

        print(f"  [process] starting: {' '.join(cmd)}")
        self.process = subprocess.Popen(
            cmd,
            cwd=os.path.dirname(os.path.abspath(__file__)),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            start_new_session=True,
        )

        self._wait_for_ready()

    def _wait_for_ready(self) -> None:
        deadline = time.monotonic() + STARTUP_TIMEOUT
        while time.monotonic() < deadline:
            if self.process is None:
                raise RuntimeError("Process exited before readiness check completed.")
            if self.process.poll() is not None:
                stdout, stderr = self._read_captured_output()
                raise RuntimeError(
                    "Process server crashed during startup.\n"
                    f"stdout:\n{stdout or '(empty)'}\n"
                    f"stderr:\n{stderr or '(empty)'}"
                )
            try:
                response = requests.get(f"{self.url}/health", timeout=1)
                if response.status_code == 200:
                    return
            except requests.exceptions.ConnectionError:
                pass
            time.sleep(HEALTH_POLL_INTERVAL)

        raise RuntimeError(
            f"Process server did not become ready within {STARTUP_TIMEOUT}s on {self.url}.\n"
            f"stderr:\n{self._captured_stderr or '(no stderr captured yet)'}"
        )


class RobustDockerRunner(DockerRunner):
    """
    Container runner with explicit docker run/inspect/log checks.
    Failures surface the container logs immediately instead of silently timing out.
    """

    def __init__(self, host_port: int = CONTAINER_PORT, workers: int = DEFAULT_WORKERS):
        super().__init__(host_port=host_port)
        self.workers = workers

    def _container_logs(self) -> str:
        if not self.container_name:
            return "(container name unavailable)"
        result = subprocess.run(
            ["docker", "logs", self.container_name],
            capture_output=True,
            text=True,
        )
        combined = "\n".join(part for part in [result.stdout, result.stderr] if part)
        return combined or "(no container logs)"

    def _ensure_container_running(self) -> None:
        if not self.container_name:
            raise RuntimeError("Container name is not set after docker run.")
        result = subprocess.run(
            ["docker", "inspect", "-f", "{{.State.Running}}", self.container_name],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0 or result.stdout.strip() != "true":
            raise RuntimeError(
                f"Container {self.container_name} is not running.\n"
                f"docker inspect stderr:\n{result.stderr or '(empty)'}\n"
                f"container logs:\n{self._container_logs()}"
            )

    def start(self) -> None:
        if self.container_name is not None:
            raise RuntimeError("DockerRunner already started. Call stop() first.")

        self.container_name = f"bench-{time.time_ns():x}"
        cmd = [
            "docker",
            "run",
            "--rm",
            "--detach",
            "--platform",
            "linux/arm64",
            "--name",
            self.container_name,
            "-p",
            f"{self.host_port}:{self.container_port}",
        ]
        for env_name in ("WORKLOAD", "MATRIX_SIZE", "MODEL_PATH"):
            env_value = os.environ.get(env_name)
            if env_value:
                cmd.extend(["-e", f"{env_name}={env_value}"])
        cmd.extend([
            FULL_IMAGE,
            "uvicorn",
            "app:app",
            "--host",
            "0.0.0.0",
            "--port",
            str(self.container_port),
            "--workers",
            str(self.workers),
            "--log-level",
            "error",
        ])
        print(f"  [container] starting: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(
                "docker run failed.\n"
                f"stdout:\n{result.stdout or '(empty)'}\n"
                f"stderr:\n{result.stderr or '(empty)'}"
            )

        self._ensure_container_running()
        self._wait_for_ready()

    def _wait_for_ready(self) -> None:
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            self._ensure_container_running()
            try:
                response = requests.get(f"{self.url}/health", timeout=1)
                if response.status_code == 200:
                    return
            except requests.exceptions.ConnectionError:
                pass
            time.sleep(0.3)

        raise RuntimeError(
            f"Container {self.container_name} did not become ready on {self.url}.\n"
            f"container logs:\n{self._container_logs()}"
        )


# ─────────────────────────────────────────────────────────────────────────────
# Measurement routines
# ─────────────────────────────────────────────────────────────────────────────


def measure_single_cold_start(runner_factory, url: str, payload: bytes, mode: str) -> float:
    """
    Cold-start measurement includes runtime startup plus the first measured request.
    The reachability validation remains /health-based inside runner.start() so we do
    not accidentally consume the first measured /compute request.
    """
    runner = runner_factory()
    t0 = time.perf_counter()
    runner.start()
    try:
        session = make_session()
        post_with_timing(session, f"{url}/compute", payload)
        return (time.perf_counter() - t0) * 1000
    except Exception as exc:
        raise RuntimeError(f"{mode} cold-start request failed: {exc}") from exc
    finally:
        runner.stop()


def run_cold_starts(
    runner_factory,
    url: str,
    payload: bytes,
    n_runs: int,
    mode: str,
    payload_kb: int,
    writer: csv.DictWriter,
    skip_purge: bool = False,
) -> None:
    """
    Cold-start benchmark with loud failures.
    We keep exactly n_runs recorded rows to avoid silently biased sample sizes.
    """
    print(f"  [cold_start] mode={mode} payload={payload_kb}KB runs={n_runs}")
    cold_latencies: list[float] = []

    # Diagnostic: verify cold-start baseline is physically plausible.
    # On M1 with sudo purge working, process should take >100ms.
    # If it is under 50ms, either purge is not clearing module pages
    # or the health poll interval is too coarse and we are missing
    # the actual startup time.
    try:
        diag_runner = runner_factory()
        t0 = time.perf_counter()
        diag_runner.start()
        diag_ms = (time.perf_counter() - t0) * 1000
        diag_runner.stop()
        time.sleep(0.5)
        print(f"  [diagnostic] cold-start baseline sample: {diag_ms:.1f}ms")
        if mode == "process" and diag_ms < 100:
            print(
                f"  WARNING: process cold-start baseline {diag_ms:.1f}ms is "
                f"suspiciously fast. Expected >100ms with sudo purge active. "
                f"Verify that `sudo purge` is running between each cold-start "
                f"run and that passwordless sudo is configured correctly. "
                f"DO NOT use cold-start results in the paper if this warning "
                f"persists across all payload sizes."
            )
    except Exception as exc:
        print(f"  [diagnostic] baseline check failed: {exc}")

    for run_id in range(n_runs):
        total_cold_ms = measure_single_cold_start(runner_factory, url, payload, mode)
        cold_latencies.append(total_cold_ms)
        writer.writerow(
            {
                "platform": CURRENT_PLATFORM,
                "mode": mode,
                "payload_size_kb": payload_kb,
                "request_type": "cold_start",
                "run_id": run_id,
                "latency_ms": round(total_cold_ms, 3),
            }
        )
        print(f"    run {run_id:02d}: {total_cold_ms:.1f} ms")
        if not skip_purge:
            _purge_page_cache()
        time.sleep(0.5)

    print(f"    median cold-start ({mode}): {statistics.median(cold_latencies):.1f} ms")


def run_warm_requests(
    url: str,
    payload: bytes,
    n_requests: int,
    mode: str,
    payload_kb: int,
    writer: csv.DictWriter,
) -> None:
    """
    Warm-request benchmark: exactly n_requests are recorded or the benchmark fails.
    """
    print(f"  [warm] mode={mode} payload={payload_kb}KB requests={n_requests}")

    for run_id in range(n_requests):
        try:
            # Fresh session per request forces a new TCP connection
            # each time, giving both modes identical connection
            # establishment costs and preventing Docker VM connection
            # pooling from distorting small-payload results.
            session = requests.Session()
            latency_ms = post_with_timing(session, f"{url}/compute", payload)
            session.close()
        except Exception as exc:
            raise RuntimeError(
                f"{mode} warm request {run_id + 1}/{n_requests} failed: {exc}"
            ) from exc

        writer.writerow(
            {
                "platform": CURRENT_PLATFORM,
                "mode": mode,
                "payload_size_kb": payload_kb,
                "request_type": "warm",
                "run_id": run_id,
                "latency_ms": round(latency_ms, 3),
            }
        )

    print(f"    All {n_requests} requests completed.")


def benchmark_process(
    cold_runs: int,
    warm_runs: int,
    writer: csv.DictWriter,
    throughput_writer: csv.DictWriter,
    workers: int,
    skip_purge: bool = False,
) -> None:
    """Run the process-mode benchmark with explicit startup validation."""
    print("\n=== PROCESS MODE ===")
    url = f"http://localhost:{PROCESS_PORT}"

    for kb in PAYLOAD_SIZES_KB:
        payload = make_payload(kb)

        def process_factory(port=PROCESS_PORT, worker_count=workers):
            return RobustProcessRunner(port=port, workers=worker_count)

        run_cold_starts(
            runner_factory=process_factory,
            url=url,
            payload=payload,
            n_runs=cold_runs,
            mode="process",
            payload_kb=kb,
            writer=writer,
            skip_purge=skip_purge,
        )

        with RobustProcessRunner(port=PROCESS_PORT, workers=workers):
            verify_compute_endpoint(url, mode="process")
            warm_up_runtime(url, mode="process", payload=payload)
            run_warm_requests(
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
    workers: int,
    skip_purge: bool = False,
) -> None:
    """Run the container-mode benchmark with explicit docker validation."""
    print("\n=== CONTAINER MODE ===")
    url = f"http://localhost:{CONTAINER_PORT}"
    _ensure_docker_desktop_ready()

    for kb in PAYLOAD_SIZES_KB:
        payload = make_payload(kb)

        def container_factory(port=CONTAINER_PORT, worker_count=workers):
            return RobustDockerRunner(host_port=port, workers=worker_count)

        run_cold_starts(
            runner_factory=container_factory,
            url=url,
            payload=payload,
            n_runs=cold_runs,
            mode="container",
            payload_kb=kb,
            writer=writer,
            skip_purge=skip_purge,
        )

        with RobustDockerRunner(host_port=CONTAINER_PORT, workers=workers):
            verify_compute_endpoint(url, mode="container")
            warm_up_runtime(url, mode="container", payload=payload)
            run_warm_requests(
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
# Validation
# ─────────────────────────────────────────────────────────────────────────────


def validate_results(path: Path, requested_mode: str) -> None:
    """
    Print rows-per-mode summary and fail if expected modes are missing.
    This catches the original failure mode where only container rows appeared.
    """
    with open(path, newline="") as handle:
        rows = list(csv.DictReader(handle))

    counts: dict[str, int] = {}
    for row in rows:
        counts[row["mode"]] = counts.get(row["mode"], 0) + 1

    print("\nResult row summary:")
    for mode, count in sorted(counts.items()):
        print(f"  {mode}: {count} rows")

    expected_modes = {"process", "container"} if requested_mode == "both" else {requested_mode}
    missing_modes = expected_modes - set(counts)
    if missing_modes:
        raise RuntimeError(
            f"Missing result rows for mode(s): {', '.join(sorted(missing_modes))}. "
            f"Observed modes: {', '.join(sorted(counts)) or '(none)'}"
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
    )
    parser.add_argument("--cold-runs", type=int, default=DEFAULT_COLD_RUNS)
    parser.add_argument("--warm-runs", type=int, default=DEFAULT_WARM_RUNS)
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    parser.add_argument(
        "--platform",
        choices=["m1_dockerdesktop", "oracle_arm64_linux"],
        default="m1_dockerdesktop",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=RESULTS_DIR,
    )
    parser.add_argument(
        "--skip-build",
        action="store_true",
        help="Skip Docker image build",
    )
    parser.add_argument(
        "--skip-purge",
        action="store_true",
        default=False,
        help="Skip sudo purge between cold-start runs. Use for fair "
             "cross-mode comparison where both modes have warm caches.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    global CURRENT_PLATFORM, RESULTS_FILE, THROUGHPUT_RESULTS_FILE
    CURRENT_PLATFORM = args.platform
    output_dir = args.output_dir
    RESULTS_FILE = output_dir / f"{args.platform}_benchmark_results.csv"
    THROUGHPUT_RESULTS_FILE = output_dir / "throughput_results.csv"

    if not _check_purge_available():
        print(
            "\n⚠️  WARNING: passwordless `sudo purge` is not available.\n"
            "   Cold-start measurements will be contaminated by OS page cache.\n"
            "   To fix: run `sudo visudo` and add:\n"
            "   <username> ALL=(ALL) NOPASSWD: /usr/bin/purge\n"
            "   Then re-run the benchmark.\n"
        )
        print("   Continuing anyway — but DO NOT use cold-start results in paper.\n")

    if args.mode in ("container", "both") and not args.skip_build:
        DockerRunner.build_image(project_dir=str(Path(__file__).parent))

    output_dir.mkdir(parents=True, exist_ok=True)

    gc_was_enabled = gc.isenabled()
    gc.disable()
    print("[benchmark] Disabled Python GC for measurement stability.")

    try:
        # run_all.sh invokes this script twice: first with --mode process, then
        # with --mode container. If we always use write mode here, the container
        # pass overwrites the process rows and the final CSV appears to contain
        # only container data. We therefore start fresh on "both" or "process",
        # and append on the subsequent container-only pass.
        benchmark_file_mode = "a" if args.mode == "container" and RESULTS_FILE.exists() else "w"
        throughput_file_mode = (
            "a" if args.mode == "container" and THROUGHPUT_RESULTS_FILE.exists() else "w"
        )

        with open(RESULTS_FILE, benchmark_file_mode, newline="") as benchmark_csvfile, open(
            THROUGHPUT_RESULTS_FILE, throughput_file_mode, newline=""
        ) as throughput_csvfile:
            writer = csv.DictWriter(benchmark_csvfile, fieldnames=CSV_FIELDNAMES)
            throughput_writer = csv.DictWriter(
                throughput_csvfile, fieldnames=THROUGHPUT_FIELDNAMES
            )
            if benchmark_file_mode == "w":
                writer.writeheader()
            if throughput_file_mode == "w":
                throughput_writer.writeheader()

            start_time = time.monotonic()

            if args.mode in ("process", "both"):
                benchmark_process(
                    cold_runs=args.cold_runs,
                    warm_runs=args.warm_runs,
                    writer=writer,
                    throughput_writer=throughput_writer,
                    workers=args.workers,
                    skip_purge=args.skip_purge,
                )

            if args.mode in ("container", "both"):
                benchmark_container(
                    cold_runs=args.cold_runs,
                    warm_runs=args.warm_runs,
                    writer=writer,
                    throughput_writer=throughput_writer,
                    workers=args.workers,
                    skip_purge=args.skip_purge,
                )

            elapsed = time.monotonic() - start_time
            print(f"\n✓ Benchmark complete in {elapsed:.1f}s")
            print(f"  Latency results written to: {RESULTS_FILE}")
            print(f"  Throughput results written to: {THROUGHPUT_RESULTS_FILE}")

        validate_results(RESULTS_FILE, requested_mode=args.mode)
    finally:
        if gc_was_enabled:
            gc.enable()
            print("[benchmark] Restored Python GC.")


if __name__ == "__main__":
    main()
