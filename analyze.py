"""
analyze.py — Post-benchmark analysis and paper-ready summary tables.

Reads:
  - results/benchmark_results.csv
  - results/throughput_results.csv
  - results/memory_*.csv

Prints markdown tables with:
  - Mean latency, stddev, p95
  - 95% confidence intervals for mean latency
  - Welch's t-test p-values (process vs. container)
  - Concurrent throughput and throughput Isolation Delta
  - Memory mean/peak and memory Isolation Delta
"""

import argparse
import csv
import math
import statistics
from collections import defaultdict
from pathlib import Path

from scipy import stats

RESULTS_DIR = Path(__file__).parent / "results"
DEFAULT_RESULTS = RESULTS_DIR / "benchmark_results.csv"
DEFAULT_THROUGHPUT_RESULTS = RESULTS_DIR / "throughput_results.csv"
HOST_VM_CORRECTION_FACTOR_MB = 1536.0


# ─────────────────────────────────────────────────────────────────────────────
# Data loading
# ─────────────────────────────────────────────────────────────────────────────


def load_csv(path: Path) -> list[dict]:
    """Load a CSV file into a list of row dicts."""
    if not path.exists():
        raise FileNotFoundError(f"Results file not found: {path}")
    with open(path, newline="") as handle:
        return list(csv.DictReader(handle))


def load_results(path: Path) -> list[dict]:
    return load_csv(path)


def load_throughput_results(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return load_csv(path)


def group_latencies(rows: list[dict]) -> dict[tuple[str, int, str], list[float]]:
    groups: dict[tuple[str, int, str], list[float]] = defaultdict(list)
    for row in rows:
        key = (row["mode"], int(row["payload_size_kb"]), row["request_type"])
        groups[key].append(float(row["latency_ms"]))
    return dict(groups)


def group_throughput(rows: list[dict]) -> dict[tuple[str, int, int], list[dict]]:
    groups: dict[tuple[str, int, int], list[dict]] = defaultdict(list)
    for row in rows:
        key = (
            row["mode"],
            int(row["payload_size_kb"]),
            int(row["concurrency_level"]),
        )
        groups[key].append(row)
    return dict(groups)


def latest_memory_files() -> dict[str, Path]:
    latest: dict[str, Path] = {}
    for mode in ("process", "container"):
        candidates = sorted(
            RESULTS_DIR.glob(f"memory_{mode}*.csv"),
            key=lambda path: path.stat().st_mtime,
        )
        if candidates:
            latest[mode] = candidates[-1]
    return latest


def load_memory_groups() -> dict[str, list[float]]:
    groups: dict[str, list[float]] = {}
    for mode, path in latest_memory_files().items():
        rows = load_csv(path)
        groups[mode] = [float(row["memory_mb"]) for row in rows if row.get("memory_mb")]
    return groups


# ─────────────────────────────────────────────────────────────────────────────
# Statistics
# ─────────────────────────────────────────────────────────────────────────────


def percentile(sorted_values: list[float], p: float) -> float:
    if not sorted_values:
        return 0.0
    index = max(0, math.ceil(p * len(sorted_values)) - 1)
    return sorted_values[index]


def mean_confidence_interval(values: list[float], confidence: float = 0.95) -> tuple[float, float]:
    """Return the lower/upper CI bounds for the mean using Student's t."""
    if not values:
        return (0.0, 0.0)
    if len(values) == 1:
        return (values[0], values[0])
    mean_value = statistics.mean(values)
    sem = stats.sem(values)
    interval = stats.t.interval(confidence, df=len(values) - 1, loc=mean_value, scale=sem)
    return (float(interval[0]), float(interval[1]))


def compute_stats(values: list[float]) -> dict:
    if not values:
        return {}
    sorted_values = sorted(values)
    ci_low, ci_high = mean_confidence_interval(sorted_values)
    return {
        "n": len(sorted_values),
        "mean_ms": statistics.mean(sorted_values),
        "median_ms": statistics.median(sorted_values),
        "std_ms": statistics.stdev(sorted_values) if len(sorted_values) > 1 else 0.0,
        "p95_ms": percentile(sorted_values, 0.95),
        "min_ms": sorted_values[0],
        "max_ms": sorted_values[-1],
        "ci_low_ms": ci_low,
        "ci_high_ms": ci_high,
    }


def compute_memory_stats(values: list[float]) -> dict:
    if not values:
        return {}
    ci_low, ci_high = mean_confidence_interval(values)
    return {
        "n": len(values),
        "mean_mb": statistics.mean(values),
        "peak_mb": max(values),
        "std_mb": statistics.stdev(values) if len(values) > 1 else 0.0,
        "ci_low_mb": ci_low,
        "ci_high_mb": ci_high,
    }


def welch_p_value(process_values: list[float], container_values: list[float]) -> float | None:
    if len(process_values) < 2 or len(container_values) < 2:
        return None
    test_result = stats.ttest_ind(process_values, container_values, equal_var=False)
    if math.isnan(test_result.pvalue):
        return None
    return float(test_result.pvalue)


def isolation_delta(numerator: float, denominator: float) -> float | None:
    if denominator == 0:
        return None
    return numerator / denominator


def fmt_float(value: float | None, digits: int = 2) -> str:
    if value is None:
        return "N/A"
    return f"{value:.{digits}f}"


def fmt_ci(low: float, high: float, digits: int = 2, unit: str = "") -> str:
    return f"[{low:.{digits}f}, {high:.{digits}f}]{unit}"


def fmt_p_value(value: float | None) -> str:
    if value is None:
        return "N/A"
    if value < 0.001:
        return "<0.001"
    return f"{value:.4f}"


def fmt_ratio(value: float | None) -> str:
    if value is None:
        return "N/A"
    return f"{value:.2f}x"


# ─────────────────────────────────────────────────────────────────────────────
# Markdown rendering
# ─────────────────────────────────────────────────────────────────────────────


def print_markdown_heading(title: str) -> None:
    print(f"\n## {title}")


def print_latency_tables(groups: dict[tuple[str, int, str], list[float]]) -> None:
    print_markdown_heading("Latency Summary")
    print(
        "| Payload (KB) | Request Type | Process Mean (ms) | Process 95% CI | "
        "Container Mean (ms) | Container 95% CI | p95 Process | p95 Container | "
        "Welch p-value | Latency ID (Container/Process) |"
    )
    print("|---:|---|---:|---|---:|---|---:|---:|---:|---:|")

    payload_sizes = sorted({payload_kb for (_, payload_kb, _) in groups})
    for request_type in ("cold_start", "warm"):
        for payload_kb in payload_sizes:
            process_values = groups.get(("process", payload_kb, request_type), [])
            container_values = groups.get(("container", payload_kb, request_type), [])
            if not process_values or not container_values:
                continue

            process_stats = compute_stats(process_values)
            container_stats = compute_stats(container_values)
            p_value = welch_p_value(process_values, container_values)
            latency_id = isolation_delta(
                container_stats["mean_ms"],
                process_stats["mean_ms"],
            )

            print(
                f"| {payload_kb} | {request_type} | "
                f"{process_stats['mean_ms']:.2f} | "
                f"{fmt_ci(process_stats['ci_low_ms'], process_stats['ci_high_ms'])} | "
                f"{container_stats['mean_ms']:.2f} | "
                f"{fmt_ci(container_stats['ci_low_ms'], container_stats['ci_high_ms'])} | "
                f"{process_stats['p95_ms']:.2f} | "
                f"{container_stats['p95_ms']:.2f} | "
                f"{fmt_p_value(p_value)} | "
                f"{fmt_ratio(latency_id)} |"
            )


def print_throughput_table(groups: dict[tuple[str, int, int], list[dict]]) -> None:
    if not groups:
        print_markdown_heading("Concurrent Throughput Summary")
        print("_No throughput_results.csv data found._")
        return

    print_markdown_heading("Concurrent Throughput Summary")
    print(
        "| Payload (KB) | Concurrency | Process Total Time (s) | "
        "Process Throughput (req/s) | Container Total Time (s) | "
        "Container Throughput (req/s) | Throughput ID (Process/Container) |"
    )
    print("|---:|---:|---:|---:|---:|---:|---:|")

    payload_sizes = sorted({payload_kb for (_, payload_kb, _) in groups})
    concurrency_levels = sorted({level for (_, _, level) in groups})

    for payload_kb in payload_sizes:
        for concurrency_level in concurrency_levels:
            process_rows = groups.get(("process", payload_kb, concurrency_level), [])
            container_rows = groups.get(("container", payload_kb, concurrency_level), [])
            if not process_rows or not container_rows:
                continue

            process_total_time = statistics.mean(
                float(row["total_time_s"]) for row in process_rows
            )
            process_tput = statistics.mean(
                float(row["throughput_req_per_sec"]) for row in process_rows
            )
            container_total_time = statistics.mean(
                float(row["total_time_s"]) for row in container_rows
            )
            container_tput = statistics.mean(
                float(row["throughput_req_per_sec"]) for row in container_rows
            )
            throughput_id = isolation_delta(process_tput, container_tput)

            print(
                f"| {payload_kb} | {concurrency_level} | "
                f"{process_total_time:.3f} | {process_tput:.2f} | "
                f"{container_total_time:.3f} | {container_tput:.2f} | "
                f"{fmt_ratio(throughput_id)} |"
            )


def print_memory_table(memory_groups: dict[str, list[float]]) -> None:
    print_markdown_heading("Memory Summary")

    process_values = memory_groups.get("process", [])
    container_values = memory_groups.get("container", [])
    if not process_values or not container_values:
        print("_No complete process/container memory sample pair found in results/._")
        return

    process_stats = compute_memory_stats(process_values)
    container_stats = compute_memory_stats(container_values)
    mean_id = isolation_delta(container_stats["mean_mb"], process_stats["mean_mb"])
    peak_id = isolation_delta(container_stats["peak_mb"], process_stats["peak_mb"])

    print(
        "| Mode | Mean Memory (MB) | Mean 95% CI | Peak Memory (MB) | Stddev (MB) |"
    )
    print("|---|---:|---|---:|---:|")
    print(
        f"| Process | {process_stats['mean_mb']:.2f} | "
        f"{fmt_ci(process_stats['ci_low_mb'], process_stats['ci_high_mb'])} | "
        f"{process_stats['peak_mb']:.2f} | {process_stats['std_mb']:.2f} |"
    )
    print(
        f"| Container (docker stats only) | {container_stats['mean_mb']:.2f} | "
        f"{fmt_ci(container_stats['ci_low_mb'], container_stats['ci_high_mb'])} | "
        f"{container_stats['peak_mb']:.2f} | {container_stats['std_mb']:.2f} |"
    )

    print("\n| Memory Metric | Isolation Delta |")
    print("|---|---:|")
    print(f"| Mean Memory ID (Container/Process) | {fmt_ratio(mean_id)} |")
    print(f"| Peak Memory ID (Container/Process) | {fmt_ratio(peak_id)} |")
    print(
        f"| Host VM Correction Factor | ~{HOST_VM_CORRECTION_FACTOR_MB:.0f} MB additional host overhead |"
    )
    print(
        f"| Note for paper | Container memory measured via docker stats "
        f"({container_stats['mean_mb']:.2f} MB) excludes macOS "
        f"com.docker.virtualization overhead (typically ~1.5GB). |"
    )


def print_plaintext_findings(groups: dict[tuple[str, int, str], list[float]]) -> None:
    print("\n### Statistical Findings")
    for request_type in ("cold_start", "warm"):
        print(f"\n{request_type}:")
        payload_sizes = sorted({payload_kb for (_, payload_kb, rt) in groups if rt == request_type})
        for payload_kb in payload_sizes:
            process_values = groups.get(("process", payload_kb, request_type), [])
            container_values = groups.get(("container", payload_kb, request_type), [])
            if not process_values or not container_values:
                continue

            process_stats = compute_stats(process_values)
            container_stats = compute_stats(container_values)
            p_value = welch_p_value(process_values, container_values)

            print(
                f"- {payload_kb} KB: process mean {process_stats['mean_ms']:.2f} ms "
                f"(95% CI {fmt_ci(process_stats['ci_low_ms'], process_stats['ci_high_ms'])}), "
                f"container mean {container_stats['mean_ms']:.2f} ms "
                f"(95% CI {fmt_ci(container_stats['ci_low_ms'], container_stats['ci_high_ms'])}), "
                f"Welch p-value {fmt_p_value(p_value)}."
            )


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────


def parse_args():
    parser = argparse.ArgumentParser(description="Analyze benchmark results")
    parser.add_argument(
        "--results",
        type=Path,
        default=DEFAULT_RESULTS,
        help=f"Path to benchmark CSV (default: {DEFAULT_RESULTS})",
    )
    parser.add_argument(
        "--throughput-results",
        type=Path,
        default=DEFAULT_THROUGHPUT_RESULTS,
        help=f"Path to throughput CSV (default: {DEFAULT_THROUGHPUT_RESULTS})",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    latency_rows = load_results(args.results)
    throughput_rows = load_throughput_results(args.throughput_results)
    memory_groups = load_memory_groups()

    print(f"Loaded {len(latency_rows)} latency rows from {args.results}")
    if throughput_rows:
        print(f"Loaded {len(throughput_rows)} throughput rows from {args.throughput_results}")
    else:
        print(f"No throughput rows found at {args.throughput_results}")

    latest_memory = latest_memory_files()
    if latest_memory:
        print(
            "Using latest memory files: "
            + ", ".join(f"{mode}={path.name}" for mode, path in latest_memory.items())
        )

    latency_groups = group_latencies(latency_rows)
    throughput_groups = group_throughput(throughput_rows)

    print_latency_tables(latency_groups)
    print_throughput_table(throughput_groups)
    print_memory_table(memory_groups)
    print_plaintext_findings(latency_groups)


if __name__ == "__main__":
    main()
