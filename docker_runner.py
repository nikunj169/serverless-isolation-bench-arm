"""
docker_runner.py — Manages Docker image build and container lifecycle.
Used by benchmark.py to start/stop the server in "container" mode.

All Docker operations use subprocess so we don't need the Docker SDK,
keeping the dependency list minimal.
"""

import subprocess
import time
import requests
import uuid
import sys

IMAGE_NAME = "serverless-bench"
IMAGE_TAG = "latest"
FULL_IMAGE = f"{IMAGE_NAME}:{IMAGE_TAG}"

STARTUP_TIMEOUT = 60   # containers take longer than bare processes
HEALTH_POLL_INTERVAL = 0.3


class DockerRunner:
    """
    Builds the ARM64 Docker image (once) and manages a single container instance.
    Each start() creates a fresh container; stop() removes it.
    """

    def __init__(self, host_port: int = 8001, container_port: int = 8000):
        self.host_port = host_port
        self.container_port = container_port
        self.container_name: str | None = None
        self.url = f"http://127.0.0.1:{host_port}"

    # ------------------------------------------------------------------
    # Image management
    # ------------------------------------------------------------------

    @staticmethod
    def build_image(project_dir: str = ".") -> None:
        """
        Build the Docker image with --platform linux/arm64.
        Safe to call multiple times; Docker caches layers.
        Prints build output so failures are visible.
        """
        print(f"[docker_runner] Building image {FULL_IMAGE} …", flush=True)
        result = subprocess.run(
            [
                "docker", "build",
                "--platform", "linux/arm64",
                "-t", FULL_IMAGE,
                project_dir,
            ],
            capture_output=False,   # let build output stream to the terminal
            check=True,             # raise CalledProcessError on failure
        )
        print(f"[docker_runner] Image {FULL_IMAGE} built successfully.", flush=True)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """
        Start a new container from the benchmark image.
        Assigns a unique name so parallel/sequential runs don't collide.
        Blocks until /health responds or STARTUP_TIMEOUT is reached.
        """
        if self.container_name is not None:
            raise RuntimeError("DockerRunner already started. Call stop() first.")

        # Unique container name prevents name conflicts across benchmark runs
        self.container_name = f"bench-{uuid.uuid4().hex[:8]}"

        cmd = [
            "docker", "run",
            "--rm",                                    # auto-remove on stop
            "--detach",                                # run in background
            "--platform", "linux/arm64",               # enforce ARM64 — no emulation
            "--name", self.container_name,
            "-p", f"{self.host_port}:{self.container_port}",
            FULL_IMAGE,
        ]

        subprocess.run(cmd, check=True, capture_output=True)
        self._wait_for_ready()

    def stop(self) -> None:
        """
        Stop (and auto-remove, because --rm) the running container.
        Graceful stop first; kill if it doesn't respond within 5 seconds.
        """
        if self.container_name is None:
            return

        subprocess.run(
            ["docker", "stop", "--time", "5", self.container_name],
            capture_output=True,   # suppress "container not found" noise
        )
        self.container_name = None

    # ------------------------------------------------------------------
    # Context manager support
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
        Poll /health until the container's server responds 200.
        Cleans up the container if startup times out to avoid orphaned containers.
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

        # Timeout — kill the container before raising
        self.stop()
        raise RuntimeError(
            f"Container '{self.container_name}' did not become ready within "
            f"{STARTUP_TIMEOUT}s on port {self.host_port}"
        )

    # ------------------------------------------------------------------
    # Utility — used by memory_tracker
    # ------------------------------------------------------------------

    def get_container_name(self) -> str | None:
        return self.container_name

    @staticmethod
    def image_exists() -> bool:
        """Return True if the benchmark image is already present locally."""
        result = subprocess.run(
            ["docker", "image", "inspect", FULL_IMAGE],
            capture_output=True,
        )
        return result.returncode == 0
