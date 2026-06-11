"""Scikit-learn inference workload."""

import hashlib
import os
from pathlib import Path
from typing import Any

import joblib
import numpy as np
from sklearn.linear_model import LogisticRegression

from workloads.base import Workload
from workloads.ml_training import N_FEATURES

DEFAULT_MODEL_PATH = Path(__file__).resolve().parent.parent / "model.pkl"


def resolve_model_path() -> Path:
    return Path(os.environ.get("MODEL_PATH", DEFAULT_MODEL_PATH))


class MLInferenceWorkload(Workload):
    def __init__(self) -> None:
        self._model: LogisticRegression | None = None

    def startup(self) -> None:
        model_path = resolve_model_path()
        if not model_path.is_file():
            raise FileNotFoundError(
                f"ML model not found at {model_path}. "
                "Run `python train_model.py` before starting the server."
            )
        self._model = joblib.load(model_path)

    def compute(self, body: bytes) -> dict[str, Any]:
        if self._model is None:
            raise RuntimeError("ML model is not initialized")

        feature_vector = _features_from_body(body, N_FEATURES)
        prediction = int(self._model.predict(feature_vector)[0])
        probabilities = self._model.predict_proba(feature_vector)[0]
        confidence = float(np.max(probabilities))
        return {
            "prediction": prediction,
            "confidence": confidence,
        }


def _features_from_body(body: bytes, n_features: int) -> np.ndarray:
    """Derive a deterministic feature vector from the request body."""
    seed_bytes = hashlib.sha256(body).digest()[:8]
    seed = int.from_bytes(seed_bytes, byteorder="big", signed=False) % (2**31)
    rng = np.random.RandomState(seed)
    return rng.randn(1, n_features)
