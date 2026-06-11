#!/usr/bin/env python3
"""
Execute the full experimental campaign without modifying benchmark methodology.
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PYTHON = ROOT / ".venv" / "bin" / "python"
CAMPAIGN_DIR = ROOT / "campaign"
LOG_DIR = CAMPAIGN_DIR / "logs"
RESULTS_ROOT = ROOT / "results"
PLATFORM = "m1_dockerdesktop"
IMAGE = "serverless-bench:latest"
PROCESS_PORT = 18080
CONTAINER_PORT = 18081

WORKLOAD_SCHEMAS: dict[str, set[str]] = {
    "sha256": {"sha256", "payload_bytes"},
    "json": {"keys", "strings", "numbers", "depth"},
    "matrix": {"checksum"},
    "ml": {"prediction", "confidence"},
}


@dataclass
class CampaignLog:
    started_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    phases: list[dict] = field(default_factory=list)
    anomalies: list[str] = field(default_factory=list)

    def save(self, path: Path) -> None:
        path.write_text(json.dumps(self.__dict__, indent=2) + "\n", encoding="utf-8")


def log_msg(msg: str, log_file: Path | None = None) -> None:
    line = f"[{datetime.now().strftime('%H:%M:%S')}] {msg}"
    print(line, flush=True)
    if log_file:
        with log_file.open("a", encoding="utf-8") as handle:
            handle.write(line + "\n")


def run_cmd(
    cmd: list[str],
    env: dict[str, str] | None = None,
    log_file: Path | None = None,
    cwd: Path | None = None,
) -> subprocess.CompletedProcess[str]:
    merged_env = os.environ.copy()
    if env:
        merged_env.update(env)
    log_msg(f"RUN {' '.join(cmd)}", log_file)
    result = subprocess.run(
        cmd,
        cwd=cwd or ROOT,
        env=merged_env,
        capture_output=True,
        text=True,
    )
    if log_file:
        with log_file.open("a", encoding="utf-8") as handle:
            handle.write(result.stdout)
            if result.stderr:
                handle.write("\n--- stderr ---\n")
                handle.write(result.stderr)
    if result.returncode != 0:
        raise RuntimeError(
            f"Command failed ({result.returncode}): {' '.join(cmd)}\n"
            f"{result.stderr or result.stdout}"
        )
    return result


def post_compute(url: str, payload: bytes) -> dict:
    req = urllib.request.Request(
        f"{url}/compute",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=120) as resp:
        return json.loads(resp.read().decode())


def wait_health(url: str, timeout_s: float = 120.0) -> None:
    deadline = time.monotonic() + timeout_s
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(f"{url}/health", timeout=2) as resp:
                if resp.status == 200:
                    return
        except Exception as exc:
            last_error = exc
        time.sleep(0.25)
    raise RuntimeError(f"Health check failed for {url}: {last_error}")


def stop_process(proc: subprocess.Popen[str]) -> None:
    if proc.poll() is not None:
        return
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
    except ProcessLookupError:
        pass
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        proc.wait(timeout=5)


def smoke_process(workload: str, matrix_size: str | None, log_file: Path) -> dict:
    env = os.environ.copy()
    env["WORKLOAD"] = workload
    env["PORT"] = str(PROCESS_PORT)
    if matrix_size:
        env["MATRIX_SIZE"] = matrix_size
    proc = subprocess.Popen(
        [str(PYTHON), "-m", "uvicorn", "app:app", "--host", "127.0.0.1",
         "--port", str(PROCESS_PORT), "--log-level", "error"],
        cwd=ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    url = f"http://127.0.0.1:{PROCESS_PORT}"
    try:
        wait_health(url)
        payload = json.dumps({"data": "smoke-test"}).encode()
        body = post_compute(url, payload)
        expected = WORKLOAD_SCHEMAS[workload]
        if set(body) != expected:
            raise RuntimeError(f"Schema mismatch for {workload}: got {body}")
        return {"mode": "process", "workload": workload, "status": "pass", "response": body}
    finally:
        stop_process(proc)


def smoke_container(workload: str, matrix_size: str | None, log_file: Path) -> dict:
    env_args: list[str] = ["-e", f"WORKLOAD={workload}"]
    if matrix_size:
        env_args.extend(["-e", f"MATRIX_SIZE={matrix_size}"])
    run_cmd(
        ["docker", "rm", "-f", f"smoke-{workload}"],
        log_file=log_file,
    )
    result = run_cmd(
        [
            "docker", "run", "-d", "--rm",
            "--platform", "linux/arm64",
            "--name", f"smoke-{workload}",
            "-p", f"{CONTAINER_PORT}:8000",
            *env_args,
            IMAGE,
        ],
        log_file=log_file,
    )
    container_id = result.stdout.strip()
    url = f"http://127.0.0.1:{CONTAINER_PORT}"
    try:
        wait_health(url, timeout_s=180.0)
        payload = json.dumps({"data": "smoke-test"}).encode()
        body = post_compute(url, payload)
        expected = WORKLOAD_SCHEMAS[workload]
        if set(body) != expected:
            raise RuntimeError(f"Schema mismatch for {workload}: got {body}")
        return {"mode": "container", "workload": workload, "status": "pass", "response": body}
    finally:
        run_cmd(["docker", "stop", f"smoke-{workload}"], log_file=log_file)


def phase1_smoke(campaign_log: CampaignLog, log_file: Path) -> list[dict]:
    log_msg("=== PHASE 1: VALIDATION SMOKE TESTS ===", log_file)
    results = []
    for workload in ["sha256", "json", "matrix", "ml"]:
        matrix_size = "256" if workload == "matrix" else None
        for fn in (smoke_process, smoke_container):
            entry = fn(workload, matrix_size, log_file)
            results.append(entry)
            log_msg(f"PASS {entry['mode']} {workload}", log_file)
    campaign_log.phases.append({"phase": 1, "name": "smoke_tests", "results": results})
    return results


def phase2_cold_sanity(campaign_log: CampaignLog, log_file: Path) -> Path:
    log_msg("=== PHASE 2: COLD START SANITY (10 trials) ===", log_file)
    out_dir = RESULTS_ROOT / "phase2_cold_sanity"
    out_dir.mkdir(parents=True, exist_ok=True)
    summary_path = CAMPAIGN_DIR / "phase2_cold_summary.csv"
    rows = ["workload,mode,mean_cold_start_ms,median_cold_start_ms"]

    for workload in ["sha256", "json", "matrix", "ml"]:
        env = {"WORKLOAD": workload}
        if workload == "matrix":
            env["MATRIX_SIZE"] = "256"
        wdir = out_dir / workload
        wdir.mkdir(parents=True, exist_ok=True)
        run_cmd(
            [str(PYTHON), "write_platform_info.py", "--platform", PLATFORM, "--output-dir", str(wdir)],
            env=env,
            log_file=log_file,
        )
        run_cmd(
            [
                str(PYTHON), "benchmark.py",
                "--platform", PLATFORM,
                "--output-dir", str(wdir),
                "--cold-runs", "10",
                "--warm-runs", "0",
                "--skip-build",
                "--skip-purge",
            ],
            env=env,
            log_file=log_file,
        )
        import pandas as pd

        bench = pd.read_csv(wdir / f"{PLATFORM}_benchmark_results.csv")
        cold = bench[bench["request_type"] == "cold_start"]
        for mode in ["process", "container"]:
            subset = cold[cold["mode"] == mode]["latency_ms"]
            rows.append(
                f"{workload},{mode},{subset.mean():.3f},{subset.median():.3f}"
            )
    summary_path.write_text("\n".join(rows) + "\n", encoding="utf-8")
    campaign_log.phases.append({"phase": 2, "name": "cold_sanity", "summary": str(summary_path)})
    return summary_path


def run_full_suite(
    output_dir: Path,
    env: dict[str, str],
    log_file: Path,
    skip_build: bool = True,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    run_cmd(
        [str(PYTHON), "write_platform_info.py", "--platform", PLATFORM, "--output-dir", str(output_dir)],
        env=env,
        log_file=log_file,
    )
    build_flag = ["--skip-build"] if skip_build else []
    run_cmd(
        [str(PYTHON), "benchmark.py", "--platform", PLATFORM, "--output-dir", str(output_dir), *build_flag],
        env=env,
        log_file=log_file,
    )
    for mode in ["process", "container"]:
        for payload in [1, 10, 100, 1024]:
            run_cmd(
                [
                    str(PYTHON), "fix_memory.py",
                    "--mode", mode,
                    "--payload-kb", str(payload),
                    "--platform", PLATFORM,
                    "--output-dir", str(output_dir),
                ],
                env=env,
                log_file=log_file,
            )
    run_cmd(
        [str(PYTHON), "fix_throughput.py", "--platform", PLATFORM, "--output-dir", str(output_dir)],
        env=env,
        log_file=log_file,
    )
    run_cmd(
        [str(PYTHON), "verify_results.py", "--dir", str(output_dir)],
        env=env,
        log_file=log_file,
    )


def phase3_json(campaign_log: CampaignLog, log_file: Path) -> None:
    log_msg("=== PHASE 3: FULL JSON EXPERIMENTS ===", log_file)
    out = RESULTS_ROOT / "json"
    run_full_suite(out, {"WORKLOAD": "json"}, log_file)
    campaign_log.phases.append({"phase": 3, "name": "json_full", "output_dir": str(out)})


def phase4_ml(campaign_log: CampaignLog, log_file: Path) -> None:
    log_msg("=== PHASE 4: FULL ML EXPERIMENTS ===", log_file)
    out = RESULTS_ROOT / "ml"
    run_full_suite(out, {"WORKLOAD": "ml"}, log_file)
    campaign_log.phases.append({"phase": 4, "name": "ml_full", "output_dir": str(out)})


def phase5_matrix(campaign_log: CampaignLog, log_file: Path) -> None:
    log_msg("=== PHASE 5: FULL MATRIX EXPERIMENTS ===", log_file)
    for size in ["256", "512", "768", "1024"]:
        out = RESULTS_ROOT / f"matrix_{size}"
        run_full_suite(out, {"WORKLOAD": "matrix", "MATRIX_SIZE": size}, log_file)
        campaign_log.phases.append({"phase": 5, "name": f"matrix_{size}", "output_dir": str(out)})


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Run experimental campaign")
    parser.add_argument("--from-phase", type=int, default=1, choices=[1, 2, 3, 4, 5])
    parser.add_argument("--skip-docker-build", action="store_true")
    args = parser.parse_args()

    if not PYTHON.exists():
        print(f"Missing venv python at {PYTHON}", file=sys.stderr)
        return 1

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = LOG_DIR / f"campaign_{ts}.log"
    campaign = CampaignLog()

    try:
        if not args.skip_docker_build:
            run_cmd(["docker", "build", "--platform", "linux/arm64", "-t", IMAGE, "."], log_file=log_file)
        if args.from_phase <= 1:
            phase1_smoke(campaign, log_file)
        if args.from_phase <= 2:
            phase2_cold_sanity(campaign, log_file)
        if args.from_phase <= 3:
            phase3_json(campaign, log_file)
        if args.from_phase <= 4:
            phase4_ml(campaign, log_file)
        if args.from_phase <= 5:
            phase5_matrix(campaign, log_file)
    except Exception as exc:
        campaign.anomalies.append(str(exc))
        log_msg(f"CAMPAIGN ERROR: {exc}", log_file)
        campaign.save(CAMPAIGN_DIR / "execution_log.json")
        raise

    campaign.save(CAMPAIGN_DIR / "execution_log.json")
    log_msg("Campaign complete.", log_file)
    return 0


if __name__ == "__main__":
    sys.exit(main())
