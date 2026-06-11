#!/usr/bin/env python3
"""
merge_cold_start.py — Replace ALL cold_start rows (both modes) in the main
benchmark CSV with rows from a fresh re-run.

Usage:
  python3 merge_cold_start.py \
    --main results/m1_dockerdesktop/m1_dockerdesktop_benchmark_results_backup.csv \
    --new-cold results/m1_dockerdesktop/cold_start_nopurge.csv \
    --output results/m1_dockerdesktop/m1_dockerdesktop_benchmark_results.csv
"""

import argparse
import pandas as pd
from pathlib import Path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--main", type=Path, required=True,
                        help="Existing main benchmark CSV")
    parser.add_argument("--new-cold", type=Path, required=True,
                        help="New cold start CSV from re-run")
    parser.add_argument("--output", type=Path, required=True,
                        help="Output path (can be same as --main to overwrite)")
    args = parser.parse_args()

    main_df = pd.read_csv(args.main)
    new_df = pd.read_csv(args.new_cold)

    print(f"Loaded main CSV: {len(main_df)} rows")
    print(f"  cold_start rows to DROP (all modes): "
          f"{len(main_df[main_df['request_type'] == 'cold_start'])}")

    # NEW — drops ALL cold start rows (both modes)
    kept = main_df[main_df["request_type"] != "cold_start"]

    # Drop duplicate warm/throughput rows if they were appended during the rerun
    kept = kept.drop_duplicates(
        subset=["mode", "request_type", "payload_size_kb", "run_id"],
        keep="first"
    )

    # NEW — accept cold_start rows from both modes
    new_cold = new_df[new_df["request_type"] == "cold_start"]

    # Keep only the latest runs if duplicates exist (per mode+payload+run_id)
    new_cold = new_cold.drop_duplicates(
        subset=["mode", "payload_size_kb", "run_id"],
        keep="last"
    )

    print(f"  new cold_start rows to ADD (both modes): {len(new_cold)}")

    if len(new_cold) == 0:
        raise RuntimeError(
            "No cold_start rows found in new CSV. "
            "Check that the re-run completed successfully."
        )

    merged = pd.concat([kept, new_cold], ignore_index=True)

    # Sort for readability
    merged = merged.sort_values(
        ["mode", "request_type", "payload_size_kb", "run_id"]
    ).reset_index(drop=True)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(args.output, index=False)

    print(f"Merged CSV written to: {args.output}")
    print(f"Final row count: {len(merged)}")

    # Sanity check
    final_cold = merged[merged["request_type"] == "cold_start"]
    expected = 4 * 30 * 2  # 4 payloads × 30 runs × 2 modes
    if len(final_cold) != expected:
        print(f"  WARNING: expected {expected} cold_start rows, "
              f"got {len(final_cold)}")
        print("  Breakdown by mode:")
        print(final_cold.groupby(["mode", "payload_size_kb"]).size().to_string())
    else:
        print(f"  ✓ cold_start rows: {len(final_cold)} (correct)")


if __name__ == "__main__":
    main()
