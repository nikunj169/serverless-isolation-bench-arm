#!/usr/bin/env python3

import argparse
import concurrent.futures
import csv
import statistics
import time
from pathlib import Path

import numpy as np
import requests

from bench_utils import (
    PAYLOAD_SIZES_KB,
    THROUGHPUT_CONCURRENCY_LEVELS,
    detect_platform,
    ensure_output_dir,
    make_payload,
    managed_server,
    make_session,
    post_compute,
    warm_server,
)

RESULTS_FIELDNAMES = [
    "platform",
    "mode",
    "payload_size_kb",
    "concurrency_level",
    "run_id",
    "total_requests",
    "total_time_s",
    "throughput_req_per_sec",
]

SUMMARY_FIELDNAMES = [
    "platform",
    "mode",
    "payload_size_kb",
    "concurrency_level",
    "mean_throughput",
    "std_throughput",
    "p5_throughput",
    "p95_throughput",
]

TOTAL_REQUESTS = 1000
RUNS_PER_CONFIG = 10
MAX_ATTEMPTS_PER_PAYLOAD = 2


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect repeated throughput measurements.")
    parser.add_argument(
        "--platform",
        choices=["m1_dockerdesktop", "oracle_arm64_linux"],
        help="Platform label. Auto-detected if omitted.",
    )
    parser.add_argument("--output-dir", default="results")
    return parser.parse_args()


def run_one_benchmark(url: str, payload: bytes, concurrency_level: int) -> float:
    started_at = time.perf_counter()
    batch_count = TOTAL_REQUESTS // concurrency_level
    if batch_count * concurrency_level != TOTAL_REQUESTS:
        raise RuntimeError(
            f"TOTAL_REQUESTS={TOTAL_REQUESTS} must be divisible by concurrency_level={concurrency_level}"
        )

    def do_request() -> None:
        with make_session() as session:
            post_compute(url, payload, session=session)

    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency_level) as executor:
        for _ in range(batch_count):
            futures = [executor.submit(do_request) for _ in range(concurrency_level)]
            for future in concurrent.futures.as_completed(futures):
                future.result()
    return time.perf_counter() - started_at


def write_csv(path: Path, fieldnames: list[str], rows: list[dict]) -> None:
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def build_summary(rows: list[dict]) -> list[dict]:
    grouped: dict[tuple[str, str, int, int], list[float]] = {}
    for row in rows:
        key = (
            row["platform"],
            row["mode"],
            row["payload_size_kb"],
            row["concurrency_level"],
        )
        grouped.setdefault(key, []).append(row["throughput_req_per_sec"])

    summary_rows: list[dict] = []
    for key in sorted(grouped):
        values = grouped[key]
        platform, mode, payload_kb, concurrency_level = key
        summary_rows.append(
            {
                "platform": platform,
                "mode": mode,
                "payload_size_kb": payload_kb,
                "concurrency_level": concurrency_level,
                "mean_throughput": round(statistics.mean(values), 6),
                "std_throughput": round(statistics.stdev(values) if len(values) > 1 else 0.0, 6),
                "p5_throughput": round(float(np.percentile(values, 5)), 6),
                "p95_throughput": round(float(np.percentile(values, 95)), 6),
            }
        )
    return summary_rows


def run_payload_suite(
    platform: str,
    mode: str,
    payload_kb: int,
) -> list[dict]:
    payload = make_payload(payload_kb)

    for attempt in range(1, MAX_ATTEMPTS_PER_PAYLOAD + 1):
        rows: list[dict] = []
        try:
            with managed_server(mode) as server:
                warm_server(server.url, payload, request_count=20)
                for concurrency_level in THROUGHPUT_CONCURRENCY_LEVELS:
                    for run_id in range(1, RUNS_PER_CONFIG + 1):
                        total_time_s = run_one_benchmark(server.url, payload, concurrency_level)
                        throughput = TOTAL_REQUESTS / total_time_s if total_time_s > 0 else 0.0
                        print(
                            f"[{mode}] [{payload_kb}]KB concurrency=[{concurrency_level}] "
                            f"run [{run_id}/10]: {throughput:.2f} req/s"
                        )
                        rows.append(
                            {
                                "platform": platform,
                                "mode": mode,
                                "payload_size_kb": payload_kb,
                                "concurrency_level": concurrency_level,
                                "run_id": run_id,
                                "total_requests": TOTAL_REQUESTS,
                                "total_time_s": round(total_time_s, 6),
                                "throughput_req_per_sec": round(throughput, 6),
                            }
                        )
                        time.sleep(3)
            return rows
        except requests.RequestException as exc:
            if attempt >= MAX_ATTEMPTS_PER_PAYLOAD:
                raise RuntimeError(
                    f"{mode} payload={payload_kb}KB failed after {attempt} attempts: {exc}"
                ) from exc
            print(
                f"[{mode}] [{payload_kb}]KB attempt {attempt} failed with dropped connection; retrying on a fresh server"
            )
            time.sleep(2)
    raise RuntimeError("Unreachable retry state")


def main() -> None:
    args = parse_args()
    platform = args.platform or detect_platform()
    output_dir = ensure_output_dir(args.output_dir)
    result_rows: list[dict] = []

    for mode in ("process", "container"):
        for payload_kb in PAYLOAD_SIZES_KB:
            result_rows.extend(run_payload_suite(platform, mode, payload_kb))

    summary_rows = build_summary(result_rows)
    write_csv(output_dir / "throughput_results.csv", RESULTS_FIELDNAMES, result_rows)
    write_csv(output_dir / f"throughput_results_{platform}.csv", RESULTS_FIELDNAMES, result_rows)
    write_csv(output_dir / "throughput_summary.csv", SUMMARY_FIELDNAMES, summary_rows)
    write_csv(output_dir / f"throughput_summary_{platform}.csv", SUMMARY_FIELDNAMES, summary_rows)


if __name__ == "__main__":
    main()
