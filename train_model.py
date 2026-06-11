#!/usr/bin/env python3
"""Offline training script for the ML inference workload."""

import argparse
import os
from pathlib import Path

import joblib

from workloads.ml_training import MODEL_TYPE, train_logistic_regression


def default_model_path() -> Path:
    return Path(os.environ.get("MODEL_PATH", "model.pkl"))


def main() -> None:
    parser = argparse.ArgumentParser(description="Train and save the benchmark ML model.")
    parser.add_argument(
        "--output",
        default=str(default_model_path()),
        help="Output path for the serialized model (default: model.pkl)",
    )
    args = parser.parse_args()

    output_path = Path(args.output)
    model = train_logistic_regression()
    joblib.dump(model, output_path)
    print(f"Wrote {MODEL_TYPE} model to {output_path.resolve()}")


if __name__ == "__main__":
    main()
