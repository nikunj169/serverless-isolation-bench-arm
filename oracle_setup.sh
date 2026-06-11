#!/usr/bin/env bash

set -eEuo pipefail

SKIP_INSTALL=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-install)
      SKIP_INSTALL=1
      shift
      ;;
    *)
      echo "Unknown argument: $1"
      exit 1
      ;;
  esac
done

trap 'CODE=$?; echo "ERROR: command failed at line ${LINENO} with exit code ${CODE}"; exit 1' ERR

if [[ "$SKIP_INSTALL" -eq 0 ]]; then
  sudo apt-get update
  sudo apt-get install -y python3 python3-pip python3-venv docker.io curl git
  sudo usermod -aG docker "$USER"
  echo "Log out and back in for docker group to take effect, then re-run with --skip-install"
  pip3 install flask requests psutil pandas scipy numpy matplotlib fastapi "uvicorn[standard]"
fi

docker build --platform linux/arm64 -t serverless-bench:latest .

python3 - <<'PY'
import os
import signal
import subprocess
import sys
import time

import requests


def wait_for_ready(url: str, timeout_s: float = 30.0) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            response = requests.get(f"{url}/health", timeout=1)
            if response.status_code == 200:
                return
        except Exception:
            pass
        time.sleep(0.25)
    raise RuntimeError(f"Server did not become ready at {url}")


def smoke_post(url: str) -> None:
    payload = {"data": "A" * 1024}
    response = requests.post(f"{url}/compute", json=payload, timeout=5)
    if response.status_code != 200:
        raise RuntimeError(f"Smoke test failed for {url}: HTTP {response.status_code}")


process = subprocess.Popen(
    [sys.executable, "app.py"],
    env={**os.environ, "PORT": "8000"},
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    text=True,
    start_new_session=True,
)
try:
    wait_for_ready("http://127.0.0.1:8000")
    smoke_post("http://127.0.0.1:8000")
finally:
    try:
        os.killpg(os.getpgid(process.pid), signal.SIGTERM)
    except ProcessLookupError:
        pass
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        os.killpg(os.getpgid(process.pid), signal.SIGKILL)
        process.wait(timeout=5)

container = subprocess.run(
    ["docker", "run", "-d", "-p", "8001:8000", "serverless-bench:latest"],
    capture_output=True,
    text=True,
    check=True,
)
container_id = container.stdout.strip()
try:
    wait_for_ready("http://127.0.0.1:8001", timeout_s=60.0)
    smoke_post("http://127.0.0.1:8001")
finally:
    subprocess.run(["docker", "stop", container_id], check=False, capture_output=True, text=True)
    subprocess.run(["docker", "rm", container_id], check=False, capture_output=True, text=True)
PY

python3 write_platform_info.py --platform oracle_arm64_linux --output-dir .

echo "SETUP COMPLETE — ready to run benchmarks"
