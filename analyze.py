"""
analyze.py — Post-benchmark analysis and paper-ready summary tables.

Reads:
  - results/benchmark_results.csv
  - results/throughput_results.csv  (only rows where total_requests == 1000)
  - results/memory_*.csv

Prints markdown-formatted tables with:
  - Mean latency, std, p95, 95% CI
  - Welch's t-test p-values (process vs. container)
  - Latency Isolation Delta (container / process)
  - Concurrent throughput and throughput Isolation Delta (process / container)
  - Separate process/container memory metrics with instrumentation warning

Usage:
  python analyze.py
  python analyze.py --results path/to/benchmark_results.csv
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

# Only accept throughput rows with exactly this many total requests.
# Filters out stale N=10/50/100 rows from earlier benchmark versions.
VALID_THROUGHPUT_N = 1000


# ─────────────────────────────────────────────────────────────────────────────
# Data loading
# ─────────────────────────────────────────────────────────────────────────────


def load_csv(path: Path) -> list[dict]:
    if not path.exists():
        raise FileNotFoundError(f"Results file not found: {path}")
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def load_throughput_results(path: Path) -> list[dict]:
    """
    Load throughput CSV and filter to VALID_THROUGHPUT_N rows only.
    Earlier benchmark versions wrote N=10/50/100 rows via append mode.
    Those rows are invalid for throughput analysis and must be excluded.
    """
    if not path.exists():
        return []
    rows = load_csv(path)
    valid = [r for r in rows if int(r["total_requests"]) == VALID_THROUGHPUT_N]
    skipped = len(rows) - len(valid)
    if skipped:
        print(
            f"  [analyze] Skipped {skipped} stale throughput rows "
            f"(total_requests != {VALID_THROUGHPUT_N})"
        )
    return valid


def group_latencies(rows: list[dict]) -> dict:
    groups = defaultdict(list)
    for row in rows:
        key = (row["mode"], int(row["payload_size_kb"]), row["request_type"])
        groups[key].append(float(row["latency_ms"]))
    return dict(groups)


def group_throughput(rows: list[dict]) -> dict:
    groups = defaultdict(list)
    for row in rows:
        key = (row["mode"], int(row["payload_size_kb"]), int(row["concurrency_level"]))
        groups[key].append(row)
    return dict(groups)


def latest_memory_files() -> dict[str, Path]:
    latest = {}
    for mode in ("process", "container"):
        candidates = sorted(
            RESULTS_DIR.glob(f"memory_{mode}*.csv"),
            key=lambda p: p.stat().st_mtime,
        )
        if candidates:
            latest[mode] = candidates[-1]
    return latest


def load_memory_groups() -> dict[str, list[float]]:
    groups = {}
    for mode, path in latest_memory_files().items():
        rows = load_csv(path)
        groups[mode] = [float(r["memory_mb"]) for r in rows if r.get("memory_mb")]
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
    """95% CI for the mean using Student's t-distribution."""
    if len(values) < 2:
        v = values[0] if values else 0.0
        return (v, v)
    mean_val = statistics.mean(values)
    sem = stats.sem(values)
    interval = stats.t.interval(confidence, df=len(values) - 1, loc=mean_val, scale=sem)
    return (float(interval[0]), float(interval[1]))


def compute_stats(values: list[float]) -> dict:
    if not values:
        return {}
    sv = sorted(values)
    ci_low, ci_high = mean_confidence_interval(sv)
    return {
        "n": len(sv),
        "mean_ms": statistics.mean(sv),
        "median_ms": statistics.median(sv),
        "std_ms": statistics.stdev(sv) if len(sv) > 1 else 0.0,
        "p95_ms": percentile(sv, 0.95),
        "min_ms": sv[0],
        "max_ms": sv[-1],
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


def welch_p_value(process_vals: list[float], container_vals: list[float]) -> float | None:
    if len(process_vals) < 2 or len(container_vals) < 2:
        return None
    result = stats.ttest_ind(process_vals, container_vals, equal_var=False)
    return None if math.isnan(result.pvalue) else float(result.pvalue)


# ─────────────────────────────────────────────────────────────────────────────
# Formatters
# ─────────────────────────────────────────────────────────────────────────────


def fmt_p(p: float | None) -> str:
    if p is None:
        return "N/A"
    if p < 0.001:
        return "<0.001"
    return f"{p:.4f}"


def fmt_ci(low: float, high: float) -> str:
    return f"[{low:.2f}, {high:.2f}]"


def fmt_ratio(num: float, den: float) -> str:
    if den == 0:
        return "N/A"
    return f"{num / den:.2f}×"


# ─────────────────────────────────────────────────────────────────────────────
# Markdown table printers
# ─────────────────────────────────────────────────────────────────────────────


def print_latency_tables(groups: dict) -> None:
    print("\n## Latency Summary\n")
    print(
        "| Payload | Type | Process Mean (ms) | Process 95% CI | "
        "Container Mean (ms) | Container 95% CI | "
        "P95 Process | P95 Container | Welch p | Latency ID |"
    )
    print("|---:|---|---:|---|---:|---|---:|---:|---:|---:|")

    payload_sizes = sorted({kb for (_, kb, _) in groups})
    for rt in ("cold_start", "warm"):
        for kb in payload_sizes:
            pv = groups.get(("process", kb, rt), [])
            cv = groups.get(("container", kb, rt), [])
            if not pv or not cv:
                continue
            ps = compute_stats(pv)
            cs = compute_stats(cv)
            p = welch_p_value(pv, cv)
            print(
                f"| {kb} KB | {rt} | "
                f"{ps['mean_ms']:.2f} | {fmt_ci(ps['ci_low_ms'], ps['ci_high_ms'])} | "
                f"{cs['mean_ms']:.2f} | {fmt_ci(cs['ci_low_ms'], cs['ci_high_ms'])} | "
                f"{ps['p95_ms']:.2f} | {cs['p95_ms']:.2f} | "
                f"{fmt_p(p)} | {fmt_ratio(cs['mean_ms'], ps['mean_ms'])} |"
            )


def print_throughput_table(groups: dict) -> None:
    print("\n## Concurrent Throughput Summary (N=1000 requests each cell)\n")
    if not groups:
        print("_No valid throughput data (N=1000) found._")
        return

    print(
        "| Payload | Concurrency | Process Time (s) | Process (req/s) | "
        "Container Time (s) | Container (req/s) | Throughput ID (P/C) |"
    )
    print("|---:|---:|---:|---:|---:|---:|---:|")

    payload_sizes = sorted({kb for (_, kb, _) in groups})
    concurrency_levels = sorted({c for (_, _, c) in groups})

    for kb in payload_sizes:
        for c in concurrency_levels:
            p_rows = groups.get(("process", kb, c), [])
            c_rows = groups.get(("container", kb, c), [])
            if not p_rows or not c_rows:
                continue
            p_time = statistics.mean(float(r["total_time_s"]) for r in p_rows)
            p_tput = statistics.mean(float(r["throughput_req_per_sec"]) for r in p_rows)
            c_time = statistics.mean(float(r["total_time_s"]) for r in c_rows)
            c_tput = statistics.mean(float(r["throughput_req_per_sec"]) for r in c_rows)
            print(
                f"| {kb} KB | {c} | "
                f"{p_time:.3f} | {p_tput:.2f} | "
                f"{c_time:.3f} | {c_tput:.2f} | "
                f"{fmt_ratio(p_tput, c_tput)} |"
            )


def print_memory_section(memory_groups: dict) -> None:
    print("\n## Memory\n")

    p_vals = memory_groups.get("process", [])
    c_vals = memory_groups.get("container", [])

    if not p_vals and not c_vals:
        print("_No memory CSV files found in results/._")
        return

    if p_vals:
        ps = compute_memory_stats(p_vals)
        print(
            f"**Process memory** (host RSS via psutil):  "
            f"peak = {ps['peak_mb']:.2f} MB, mean = {ps['mean_mb']:.2f} MB  "
            f"(n={ps['n']} samples)"
        )

    if c_vals:
        cs = compute_memory_stats(c_vals)
        print(
            f"**Container memory** (docker stats, container-only):  "
            f"peak = {cs['peak_mb']:.2f} MB, mean = {cs['mean_mb']:.2f} MB  "
            f"(n={cs['n']} samples)"
        )

    # NO Isolation Delta for memory — the two instruments are incomparable.
    # Process RSS is measured by psutil on the macOS host.
    # Container memory is reported by docker stats inside the Linux VM cgroup.
    # These measure different things at different abstraction layers.
    print(
        "\n> **WARNING FOR PAPER:** These two values are NOT comparable and no "
        "Isolation Delta should be reported for memory. Process memory is measured "
        "via psutil RSS on the macOS host. Container memory is measured via "
        "`docker stats` inside the Linux VM cgroup. The `com.docker.virtualization` "
        "process on the macOS host consumes an additional ~1.5 GB that is entirely "
        "excluded from the container figure. Reporting a ratio would be misleading."
    )


def print_plaintext_findings(groups: dict) -> None:
    print("\n## Statistical Findings (inline text for paper)\n")
    for rt in ("cold_start", "warm"):
        print(f"**{rt}:**\n")
        payload_sizes = sorted({kb for (_, kb, r) in groups if r == rt})
        for kb in payload_sizes:
            pv = groups.get(("process", kb, rt), [])
            cv = groups.get(("container", kb, rt), [])
            if not pv or not cv:
                continue
            ps = compute_stats(pv)
            cs = compute_stats(cv)
            p = welch_p_value(pv, cv)
            sig = "p < 0.05 — significant" if (p is not None and p < 0.05) else "not significant"
            print(
                f"- **{kb} KB**: process mean {ps['mean_ms']:.2f} ms "
                f"(95% CI {fmt_ci(ps['ci_low_ms'], ps['ci_high_ms'])}), "
                f"container mean {cs['mean_ms']:.2f} ms "
                f"(95% CI {fmt_ci(cs['ci_low_ms'], cs['ci_high_ms'])}), "
                f"Welch p={fmt_p(p)} ({sig}), "
                f"Isolation Delta = {fmt_ratio(cs['mean_ms'], ps['mean_ms'])}"
            )
        print()


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────


def parse_args():
    parser = argparse.ArgumentParser(description="Analyze benchmark results")
    parser.add_argument("--results", type=Path, default=DEFAULT_RESULTS)
    parser.add_argument("--throughput-results", type=Path, default=DEFAULT_THROUGHPUT_RESULTS)
    return parser.parse_args()


def main():
    args = parse_args()

    latency_rows = load_csv(args.results)
    throughput_rows = load_throughput_results(args.throughput_results)
    memory_groups = load_memory_groups()

    print(f"Loaded {len(latency_rows)} latency rows from {args.results}")
    if throughput_rows:
        print(f"Loaded {len(throughput_rows)} throughput rows (N=1000) from {args.throughput_results}")

    mem_files = latest_memory_files()
    if mem_files:
        for mode, path in mem_files.items():
            print(f"  memory [{mode}]: {path.name}")

    latency_groups = group_latencies(latency_rows)
    throughput_groups = group_throughput(throughput_rows)

    print_latency_tables(latency_groups)
    print_throughput_table(throughput_groups)
    print_memory_section(memory_groups)
    print_plaintext_findings(latency_groups)


if __name__ == "__main__":
    main()