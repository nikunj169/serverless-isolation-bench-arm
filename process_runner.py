"""
process_runner.py — Manages the lifecycle of the FastAPI app as a native OS process.
Used by benchmark.py to start/stop the server in "process" mode.
"""

import subprocess
import sys
import time
import os
import signal
import requests

# How long to wait (seconds) for the server to become ready before giving up
STARTUP_TIMEOUT = 15
HEALTH_POLL_INTERVAL = 0.2


class ProcessRunner:
    """
    Launches app.py as a subprocess using uvicorn.
    Provides start(), stop(), and a context-manager interface.
    """

    def __init__(self, port: int = 8000):
        self.port = port
        self.process: subprocess.Popen | None = None
        self.url = f"http://127.0.0.1:{port}"

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """
        Spawn uvicorn as a child process.
        Blocks until the /health endpoint responds or STARTUP_TIMEOUT is reached.
        """
        if self.process is not None:
            raise RuntimeError("ProcessRunner already started. Call stop() first.")

        app_path = os.path.join(os.path.dirname(__file__), "app.py")

        cmd = [
            sys.executable,   # same Python interpreter as the benchmark
            "-m", "uvicorn",
            "app:app",
            "--host", "127.0.0.1",
            "--port", str(self.port),
            "--log-level", "error",   # suppress request logs to avoid noise
        ]

        # Launch in the directory containing app.py so uvicorn can import it
        self.process = subprocess.Popen(
            cmd,
            cwd=os.path.dirname(os.path.abspath(__file__)),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            # Keep process group so we can kill the whole tree
            start_new_session=True,
        )

        self._wait_for_ready()

    def stop(self) -> None:
        """
        Terminate the server process and wait for it to exit cleanly.
        Falls back to SIGKILL if SIGTERM is not honoured within 5 seconds.
        """
        if self.process is None:
            return

        try:
            # Kill the entire process group (handles uvicorn worker children)
            os.killpg(os.getpgid(self.process.pid), signal.SIGTERM)
        except ProcessLookupError:
            pass  # already dead

        try:
            self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(os.getpgid(self.process.pid), signal.SIGKILL)
            except ProcessLookupError:
                pass
            self.process.wait()

        self.process = None

    # ------------------------------------------------------------------
    # Context manager support  (with ProcessRunner() as r: ...)
    # ------------------------------------------------------------------

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *_):
        self.stop()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _wait_for_ready(self) -> None:
        """
        Poll /health until the server responds 200 or we time out.
        Raises RuntimeError on timeout so the benchmark fails fast.
        """
        deadline = time.monotonic() + STARTUP_TIMEOUT
        while time.monotonic() < deadline:
            try:
                r = requests.get(f"{self.url}/health", timeout=1)
                if r.status_code == 200:
                    return
            except requests.exceptions.ConnectionError:
                pass
            time.sleep(HEALTH_POLL_INTERVAL)

        # Timeout — clean up before raising
        self.stop()
        raise RuntimeError(
            f"Process server did not become ready within {STARTUP_TIMEOUT}s "
            f"on port {self.port}"
        )

    @property
    def pid(self) -> int | None:
        return self.process.pid if self.process else None
