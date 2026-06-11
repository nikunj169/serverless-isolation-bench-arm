#!/usr/bin/env python3

import argparse
import csv
import json
import platform as py_platform
import subprocess
from pathlib import Path

from bench_utils import detect_platform, ensure_output_dir, iso_timestamp
from workloads.metadata import collect_benchmark_metadata


def run_text(cmd: list[str]) -> str:
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        return ""
    return result.stdout.strip()


def first_line(text: str) -> str:
    return text.splitlines()[0].strip() if text else ""


def cpu_model() -> str:
    if py_platform.system() == "Darwin":
        return first_line(run_text(["sysctl", "-n", "machdep.cpu.brand_string"]))
    return first_line(run_text(["sh", "-lc", "lscpu | grep 'Model name' | awk -F: '{print $2}' | xargs"]))


def ram_gb() -> str:
    if py_platform.system() == "Darwin":
        mem_bytes = run_text(["sysctl", "-n", "hw.memsize"])
        return str(round(int(mem_bytes) / (1024 ** 3))) if mem_bytes else ""
    return first_line(run_text(["sh", "-lc", "free -g | awk '/Mem:/{print $2}'"]))


def cpu_cores() -> str:
    if py_platform.system() == "Darwin":
        return first_line(run_text(["sysctl", "-n", "hw.ncpu"]))
    return first_line(run_text(["nproc"]))


def write_benchmark_metadata(output_dir: Path) -> Path:
    metadata = collect_benchmark_metadata()
    json_path = output_dir / "benchmark_metadata.json"
    json_path.write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")

    csv_path = output_dir / "benchmark_metadata.csv"
    fieldnames = [
        "workload",
        "payload_size_kb",
        "payload_size",
        "matrix_size",
        "model_type",
        "timestamp",
    ]
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(metadata["runs"])

    return json_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Write platform metadata JSON.")
    parser.add_argument("--platform", choices=["m1_dockerdesktop", "oracle_arm64_linux"])
    parser.add_argument("--output-dir", default="results")
    args = parser.parse_args()

    platform_name = args.platform or detect_platform()
    output_dir = ensure_output_dir(args.output_dir)
    payload = {
        "platform": platform_name,
        "cpu_model": cpu_model(),
        "kernel": py_platform.release(),
        "docker_version": first_line(run_text(["docker", "--version"])),
        "python_version": first_line(run_text(["python3", "--version"])),
        "ram_gb": ram_gb(),
        "cpu_cores": cpu_cores(),
        "timestamp": iso_timestamp(),
    }
    output_path = output_dir / "platform_info.json"
    output_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote platform info to {output_path}")

    metadata_path = write_benchmark_metadata(output_dir)
    print(f"Wrote benchmark metadata to {metadata_path}")


if __name__ == "__main__":
    main()
