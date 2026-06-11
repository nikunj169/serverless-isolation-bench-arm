#!/usr/bin/env python3

import argparse
import math
import sys
from pathlib import Path

import pandas as pd

PLATFORMS = ["m1_dockerdesktop", "oracle_arm64_linux"]
MODES = ["process", "container"]
PAYLOADS = [1, 10, 100, 1024]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify benchmark result completeness and sanity.")
    parser.add_argument("--dir", required=True, type=Path)
    return parser.parse_args()


def print_check(passed: bool, label: str, actual, expected) -> bool:
    status = "PASS" if passed else "FAIL"
    print(f"[{status}] {label}: actual={actual} expected={expected}")
    return passed


def detect_platform(result_dir: Path) -> str | None:
    for platform in PLATFORMS:
        if (result_dir / f"{platform}_benchmark_results.csv").exists():
            return platform
        if list(result_dir.glob(f"memory_{platform}_*_*.csv")):
            return platform
        if (result_dir / f"throughput_summary_{platform}.csv").exists():
            return platform
    return None


def load_benchmark_file(result_dir: Path, platform: str | None) -> Path | None:
    candidates = [result_dir / "benchmark_results.csv"]
    if platform:
        candidates.insert(0, result_dir / f"{platform}_benchmark_results.csv")
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def has_nan(series: pd.Series) -> bool:
    return series.isna().any() or any(math.isnan(value) for value in series.astype(float))


def main() -> int:
    args = parse_args()
    result_dir = args.dir
    overall_pass = True
    platform = detect_platform(result_dir)

    benchmark_path = load_benchmark_file(result_dir, platform)
    throughput_path = result_dir / "throughput_results.csv"
    summary_path = result_dir / "throughput_summary.csv"
    platform_info_path = result_dir / "platform_info.json"

    overall_pass &= print_check(benchmark_path is not None, "benchmark results file exists", benchmark_path, "present")
    overall_pass &= print_check(throughput_path.exists(), "throughput_results.csv exists", throughput_path.exists(), True)
    overall_pass &= print_check(summary_path.exists(), "throughput_summary.csv exists", summary_path.exists(), True)
    overall_pass &= print_check(platform_info_path.exists(), "platform_info.json exists", platform_info_path.exists(), True)

    memory_files: list[Path] = []
    memory_expected = 8
    if platform:
        for mode in MODES:
            for payload in PAYLOADS:
                matches = sorted(result_dir.glob(f"memory_{platform}_{mode}_{payload}kb_*.csv"))
                if matches:
                    memory_files.append(matches[-1])
    overall_pass &= print_check(len(memory_files) == memory_expected, "memory CSV count", len(memory_files), memory_expected)

    if benchmark_path and benchmark_path.exists():
        bench_df = pd.read_csv(benchmark_path)
        cold_rows = len(bench_df[bench_df["request_type"] == "cold_start"])
        warm_rows = len(bench_df[bench_df["request_type"] == "warm"])
        overall_pass &= print_check(cold_rows == 240, "cold-start row count", cold_rows, 240)
        overall_pass &= print_check(warm_rows == 800, "warm row count", warm_rows, 800)
        if "latency_ms" in bench_df:
            overall_pass &= print_check(not has_nan(bench_df["latency_ms"]), "latency_ms has no NaN", not has_nan(bench_df["latency_ms"]), True)
            overall_pass &= print_check((bench_df["latency_ms"] > 0).all(), "latency_ms all > 0", bool((bench_df["latency_ms"] > 0).all()), True)

    if throughput_path.exists():
        throughput_df = pd.read_csv(throughput_path)
        overall_pass &= print_check(len(throughput_df) == 240, "throughput row count", len(throughput_df), 240)
        if "throughput_req_per_sec" in throughput_df:
            overall_pass &= print_check(
                not has_nan(throughput_df["throughput_req_per_sec"]),
                "throughput_req_per_sec has no NaN",
                not has_nan(throughput_df["throughput_req_per_sec"]),
                True,
            )
            overall_pass &= print_check(
                (throughput_df["throughput_req_per_sec"] > 0).all(),
                "throughput_req_per_sec all > 0",
                bool((throughput_df["throughput_req_per_sec"] > 0).all()),
                True,
            )

    for memory_file in memory_files:
        memory_df = pd.read_csv(memory_file)
        overall_pass &= print_check(len(memory_df) >= 50, f"{memory_file.name} row count", len(memory_df), ">= 50")
        overall_pass &= print_check(
            not has_nan(memory_df["memory_mb"]),
            f"{memory_file.name} memory_mb has no NaN",
            not has_nan(memory_df["memory_mb"]),
            True,
        )
        overall_pass &= print_check(
            ((memory_df["memory_mb"] > 0) & (memory_df["memory_mb"] < 4096)).all(),
            f"{memory_file.name} memory_mb sanity bound",
            bool(((memory_df["memory_mb"] > 0) & (memory_df["memory_mb"] < 4096)).all()),
            True,
        )

    return 0 if overall_pass else 1


if __name__ == "__main__":
    sys.exit(main())
