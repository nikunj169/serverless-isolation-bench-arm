import json
import os
import random
import signal
import string
import subprocess
import sys
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

IMAGE_NAME = "serverless-bench:latest"
PROCESS_PORT = 8000
CONTAINER_HOST_PORT = 8001
CONTAINER_INTERNAL_PORT = 8000
PAYLOAD_SIZES_KB = [1, 10, 100, 1024]
THROUGHPUT_CONCURRENCY_LEVELS = [10, 50, 100]
DEFAULT_WARMUP_REQUESTS = 20
DEFAULT_MEMORY_DURATION_S = 60
DEFAULT_MEMORY_INTERVAL_S = 1.0


def detect_platform() -> str:
    if sys.platform == "darwin":
        return "m1_dockerdesktop"

    os_release = Path("/etc/os-release")
    if os_release.exists():
        text = os_release.read_text(encoding="utf-8", errors="ignore").lower()
        if "oracle" in text or "ubuntu" in text:
            return "oracle_arm64_linux"

    raise RuntimeError(
        "Could not auto-detect platform. Pass --platform explicitly."
    )


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def iso_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def make_payload(target_kb: int) -> bytes:
    target_bytes = target_kb * 1024
    data_len = max(1, target_bytes - 20)
    data_str = "".join(random.choices(string.ascii_letters + string.digits, k=data_len))
    payload = json.dumps({"data": data_str}).encode("utf-8")
    if len(payload) < target_bytes:
        data_str += "A" * (target_bytes - len(payload))
        payload = json.dumps({"data": data_str}).encode("utf-8")
    return payload[:target_bytes] if len(payload) > target_bytes else payload


def make_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=0.2,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
    )
    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=64,
        pool_maxsize=64,
    )
    session.mount("http://", adapter)
    return session


def wait_for_server(url: str, timeout_s: float = 30.0) -> None:
    deadline = time.monotonic() + timeout_s
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            response = requests.get(f"{url}/health", timeout=1)
            if response.status_code == 200:
                return
        except Exception as exc:
            last_error = exc
        time.sleep(0.25)
    raise RuntimeError(f"Server at {url} did not become ready: {last_error}")


def post_compute(url: str, payload: bytes, session: requests.Session | None = None) -> requests.Response:
    active_session = session or make_session()
    created_session = session is None
    try:
        response = active_session.post(
            f"{url}/compute",
            data=payload,
            headers={"Content-Type": "application/json"},
            timeout=30,
        )
        response.raise_for_status()
        return response
    finally:
        if created_session:
            active_session.close()


def warm_server(url: str, payload: bytes, request_count: int = DEFAULT_WARMUP_REQUESTS) -> None:
    with make_session() as session:
        for _ in range(request_count):
            post_compute(url, payload, session=session)


@dataclass
class ServerHandle:
    mode: str
    url: str
    process: subprocess.Popen[str] | None = None
    container_id: str | None = None


def _start_process_server() -> ServerHandle:
    env = os.environ.copy()
    env["PORT"] = str(PROCESS_PORT)
    process = subprocess.Popen(
        [sys.executable, "app.py"],
        cwd=os.path.dirname(os.path.abspath(__file__)),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    handle = ServerHandle(mode="process", url=f"http://127.0.0.1:{PROCESS_PORT}", process=process)
    try:
        wait_for_server(handle.url)
    except Exception:
        stop_server(handle)
        stdout, stderr = process.communicate(timeout=2)
        raise RuntimeError(
            "Process server failed to start.\n"
            f"stdout:\n{stdout or '(empty)'}\n"
            f"stderr:\n{stderr or '(empty)'}"
        )
    return handle


def _start_container_server() -> ServerHandle:
    docker_cmd = [
        "docker",
        "run",
        "-d",
        "-p",
        f"{CONTAINER_HOST_PORT}:{CONTAINER_INTERNAL_PORT}",
    ]
    for env_name in ("WORKLOAD", "MATRIX_SIZE", "MODEL_PATH"):
        env_value = os.environ.get(env_name)
        if env_value:
            docker_cmd.extend(["-e", f"{env_name}={env_value}"])
    docker_cmd.append(IMAGE_NAME)

    result = subprocess.run(
        docker_cmd,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "docker run failed")

    container_id = result.stdout.strip()
    handle = ServerHandle(
        mode="container",
        url=f"http://127.0.0.1:{CONTAINER_HOST_PORT}",
        container_id=container_id,
    )
    try:
        wait_for_server(handle.url, timeout_s=60.0)
    except Exception:
        stop_server(handle)
        raise
    return handle


def start_server(mode: str) -> ServerHandle:
    if mode == "process":
        return _start_process_server()
    if mode == "container":
        return _start_container_server()
    raise ValueError(f"Unsupported mode: {mode}")


def stop_server(handle: ServerHandle) -> None:
    if handle.mode == "process" and handle.process is not None:
        try:
            os.killpg(os.getpgid(handle.process.pid), signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            handle.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(os.getpgid(handle.process.pid), signal.SIGKILL)
            except ProcessLookupError:
                pass
            handle.process.wait(timeout=5)
        handle.process = None
        return

    if handle.mode == "container" and handle.container_id:
        subprocess.run(["docker", "stop", handle.container_id], capture_output=True, text=True)
        subprocess.run(["docker", "rm", handle.container_id], capture_output=True, text=True)
        handle.container_id = None


@contextmanager
def managed_server(mode: str):
    handle = start_server(mode)
    try:
        yield handle
    finally:
        stop_server(handle)


def parse_docker_memory_to_mb(value: str) -> float:
    usage_str = value.strip().split("/")[0].strip()
    if usage_str.endswith("GiB"):
        return float(usage_str[:-3]) * 1024
    if usage_str.endswith("MiB"):
        return float(usage_str[:-3])
    if usage_str.endswith("KiB"):
        return float(usage_str[:-3]) / 1024
    if usage_str.endswith("GB"):
        return float(usage_str[:-2]) * 1000
    if usage_str.endswith("MB"):
        return float(usage_str[:-2])
    if usage_str.endswith("KB"):
        return float(usage_str[:-2]) / 1000
    if usage_str.endswith("B"):
        return float(usage_str[:-1]) / (1024 * 1024)
    return float(usage_str) / (1024 * 1024)


def ensure_output_dir(path: str | Path) -> Path:
    output_dir = Path(path)
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir
