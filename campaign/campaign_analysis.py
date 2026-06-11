#!/usr/bin/env python3
"""Validate campaign outputs, compute isolation deltas, and generate figures."""

from __future__ import annotations

import json
import math
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
CAMPAIGN_DIR = ROOT / "campaign"
FIGURES_DIR = CAMPAIGN_DIR / "figures"
PLATFORM = "m1_dockerdesktop"
PAYLOADS = [1, 10, 100, 1024]


def load_benchmark(result_dir: Path) -> pd.DataFrame:
    path = result_dir / f"{PLATFORM}_benchmark_results.csv"
    if not path.exists():
        path = result_dir / "benchmark_results.csv"
    return pd.read_csv(path)


def load_throughput_summary(result_dir: Path) -> pd.DataFrame:
    path = result_dir / "throughput_summary.csv"
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


def memory_mean(result_dir: Path, mode: str, payload_kb: int) -> float:
    matches = sorted(result_dir.glob(f"memory_{PLATFORM}_{mode}_{payload_kb}kb_*.csv"))
    if not matches:
        return float("nan")
    df = pd.read_csv(matches[-1])
    return float(df["memory_mb"].mean())


def isolation_delta(container: float, process: float, higher_is_better: bool = False) -> float:
    if process == 0 or math.isnan(process) or math.isnan(container):
        return float("nan")
    ratio = container / process
    if not higher_is_better:
        return ratio
    return process / container if container else float("nan")


def summarize_workload(result_dir: Path, label: str) -> pd.DataFrame:
    bench = load_benchmark(result_dir)
    throughput = load_throughput_summary(result_dir)
    rows = []
    for payload in PAYLOADS:
        cold_p = bench[(bench["request_type"] == "cold_start") & (bench["mode"] == "process") & (bench["payload_size_kb"] == payload)]["latency_ms"].mean()
        cold_c = bench[(bench["request_type"] == "cold_start") & (bench["mode"] == "container") & (bench["payload_size_kb"] == payload)]["latency_ms"].mean()
        warm_p = bench[(bench["request_type"] == "warm") & (bench["mode"] == "process") & (bench["payload_size_kb"] == payload)]["latency_ms"].mean()
        warm_c = bench[(bench["request_type"] == "warm") & (bench["mode"] == "container") & (bench["payload_size_kb"] == payload)]["latency_ms"].mean()
        mem_p = memory_mean(result_dir, "process", payload)
        mem_c = memory_mean(result_dir, "container", payload)
        tp_p = tp_c = float("nan")
        if not throughput.empty:
            tp_p = throughput[(throughput["mode"] == "process") & (throughput["payload_size_kb"] == payload)]["mean_throughput"].mean()
            tp_c = throughput[(throughput["mode"] == "container") & (throughput["payload_size_kb"] == payload)]["mean_throughput"].mean()
        rows.append(
            {
                "workload": label,
                "payload_size_kb": payload,
                "cold_process_ms": cold_p,
                "cold_container_ms": cold_c,
                "cold_isolation_delta": isolation_delta(cold_c, cold_p),
                "warm_process_ms": warm_p,
                "warm_container_ms": warm_c,
                "warm_isolation_delta": isolation_delta(warm_c, warm_p),
                "throughput_process_rps": tp_p,
                "throughput_container_rps": tp_c,
                "throughput_isolation_delta": isolation_delta(tp_c, tp_p, higher_is_better=True),
                "memory_process_mb": mem_p,
                "memory_container_mb": mem_c,
                "memory_isolation_delta": isolation_delta(mem_c, mem_p),
            }
        )
    return pd.DataFrame(rows)


def validate_result_dir(result_dir: Path) -> list[str]:
    anomalies: list[str] = []
    required = [
        result_dir / f"{PLATFORM}_benchmark_results.csv",
        result_dir / "throughput_results.csv",
        result_dir / "throughput_summary.csv",
        result_dir / "platform_info.json",
        result_dir / "benchmark_metadata.json",
    ]
    for path in required:
        if not path.exists():
            anomalies.append(f"Missing file: {path}")
        elif path.stat().st_size == 0:
            anomalies.append(f"Empty file: {path}")

    bench_path = result_dir / f"{PLATFORM}_benchmark_results.csv"
    if bench_path.exists():
        bench = pd.read_csv(bench_path)
        if bench["latency_ms"].isna().any():
            anomalies.append(f"NaN latency in {bench_path}")
        if (bench["latency_ms"] <= 0).any():
            anomalies.append(f"Non-positive latency in {bench_path}")

    tp_path = result_dir / "throughput_results.csv"
    if tp_path.exists():
        tp = pd.read_csv(tp_path)
        if "throughput_req_per_sec" in tp.columns:
            if tp["throughput_req_per_sec"].isna().any():
                anomalies.append(f"NaN throughput in {tp_path}")
            if (tp["throughput_req_per_sec"] <= 0).any():
                anomalies.append(f"Zero/negative throughput in {tp_path}")

    mem_expected = 8
    mem_files = list(result_dir.glob(f"memory_{PLATFORM}_*_*.csv"))
    if len(mem_files) < mem_expected:
        anomalies.append(f"{result_dir}: expected >= {mem_expected} memory files, found {len(mem_files)}")
    return anomalies


def plot_latency_comparison(df: pd.DataFrame, prefix: str, title: str, path: Path) -> None:
    fig, ax = plt.subplots(figsize=(8, 5))
    x = np.arange(len(PAYLOADS))
    width = 0.35
    ax.bar(x - width / 2, df[f"{prefix}_process_ms"], width, label="Process")
    ax.bar(x + width / 2, df[f"{prefix}_container_ms"], width, label="Container")
    ax.set_xticks(x, [f"{p}KB" if p < 1024 else "1MB" for p in PAYLOADS])
    ax.set_title(title)
    ax.set_ylabel("Latency (ms)")
    ax.legend()
    fig.tight_layout()
    fig.savefig(path.with_suffix(".png"), dpi=300)
    fig.savefig(path.with_suffix(".pdf"))
    plt.close(fig)


def main() -> None:
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)
    all_anomalies: list[str] = []

    suites = {
        "json": ROOT / "results" / "json",
        "ml": ROOT / "results" / "ml",
    }
    for size in ["256", "512", "768", "1024"]:
        suites[f"matrix_{size}"] = ROOT / "results" / f"matrix_{size}"

    summaries = []
    for label, path in suites.items():
        if not path.exists():
            all_anomalies.append(f"Missing result directory: {path}")
            continue
        all_anomalies.extend(validate_result_dir(path))
        summaries.append(summarize_workload(path, label))

    if summaries:
        combined = pd.concat(summaries, ignore_index=True)
        combined.to_csv(CAMPAIGN_DIR / "isolation_delta_summary.csv", index=False)

    json_df = summarize_workload(ROOT / "results" / "json", "json") if (ROOT / "results" / "json").exists() else pd.DataFrame()
    if not json_df.empty:
        plot_latency_comparison(json_df, "cold", "JSON Cold Start: Process vs Container", FIGURES_DIR / "json_cold_start")
        plot_latency_comparison(json_df, "warm", "JSON Warm Latency: Process vs Container", FIGURES_DIR / "json_warm_latency")

    if (ROOT / "results" / "ml").exists():
        ml_df = summarize_workload(ROOT / "results" / "ml", "ml")
        plot_latency_comparison(ml_df, "cold", "ML Cold Start: Process vs Container", FIGURES_DIR / "ml_cold_start")
        plot_latency_comparison(ml_df, "warm", "ML Warm Latency: Process vs Container", FIGURES_DIR / "ml_warm_latency")

    matrix_rows = []
    for size in ["256", "512", "768", "1024"]:
        p = ROOT / "results" / f"matrix_{size}"
        if p.exists():
            matrix_rows.append(summarize_workload(p, size).assign(matrix_size=int(size)))
    if matrix_rows:
        matrix_df = pd.concat(matrix_rows, ignore_index=True)
        matrix_df.to_csv(CAMPAIGN_DIR / "matrix_isolation_summary.csv", index=False)
        for metric, title, fname in [
            ("cold_isolation_delta", "Matrix Cold Start Isolation Delta vs Size", "matrix_cold_delta_vs_size"),
            ("warm_isolation_delta", "Matrix Warm Latency Isolation Delta vs Size", "matrix_warm_delta_vs_size"),
            ("throughput_isolation_delta", "Matrix Throughput Isolation Delta vs Size", "matrix_throughput_delta_vs_size"),
            ("memory_isolation_delta", "Matrix Memory Isolation Delta vs Size", "matrix_memory_delta_vs_size"),
        ]:
            fig, ax = plt.subplots(figsize=(8, 5))
            for payload in PAYLOADS:
                subset = matrix_df[matrix_df["payload_size_kb"] == payload]
                ax.plot(subset["matrix_size"], subset[metric], marker="o", label=f"{payload}KB" if payload < 1024 else "1MB")
            ax.set_xlabel("Matrix Size")
            ax.set_ylabel("Isolation Delta (Container / Process)")
            ax.set_title(title)
            ax.legend()
            fig.tight_layout()
            fig.savefig(FIGURES_DIR / f"{fname}.png", dpi=300)
            fig.savefig(FIGURES_DIR / f"{fname}.pdf")
            plt.close(fig)

    workload_compare = []
    for label in ["json", "ml"]:
        p = ROOT / "results" / label
        if p.exists():
            df = summarize_workload(p, label)
            workload_compare.append(
                {
                    "workload": label,
                    "cold_isolation_delta": df["cold_isolation_delta"].mean(),
                    "warm_isolation_delta": df["warm_isolation_delta"].mean(),
                    "throughput_isolation_delta": df["throughput_isolation_delta"].mean(),
                    "memory_isolation_delta": df["memory_isolation_delta"].mean(),
                }
            )
    if workload_compare:
        wc = pd.DataFrame(workload_compare)
        wc.to_csv(CAMPAIGN_DIR / "workload_isolation_comparison.csv", index=False)
        fig, ax = plt.subplots(figsize=(8, 5))
        metrics = ["cold_isolation_delta", "warm_isolation_delta", "throughput_isolation_delta", "memory_isolation_delta"]
        x = np.arange(len(metrics))
        width = 0.35
        for idx, row in wc.iterrows():
            ax.bar(x + (idx - 0.5) * width, [row[m] for m in metrics], width, label=row["workload"])
        ax.set_xticks(x, ["Cold", "Warm", "Throughput", "Memory"])
        ax.set_title("Isolation Delta: Workload Comparison")
        ax.legend()
        fig.tight_layout()
        fig.savefig(FIGURES_DIR / "workload_isolation_comparison.png", dpi=300)
        fig.savefig(FIGURES_DIR / "workload_isolation_comparison.pdf")
        plt.close(fig)

    report = {
        "anomalies": all_anomalies,
        "summaries_written": [
            str(CAMPAIGN_DIR / "isolation_delta_summary.csv"),
            str(CAMPAIGN_DIR / "matrix_isolation_summary.csv"),
        ],
        "figures_dir": str(FIGURES_DIR),
    }
    (CAMPAIGN_DIR / "validation_report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
