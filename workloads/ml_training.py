"""Shared ML training configuration and helpers."""

from sklearn.datasets import make_classification
from sklearn.linear_model import LogisticRegression

N_FEATURES = 20
TRAIN_SEED = 42
MODEL_TYPE = "LogisticRegression"


def train_logistic_regression() -> LogisticRegression:
    """Train the benchmark model on a deterministic synthetic dataset."""
    features, labels = make_classification(
        n_samples=1000,
        n_features=N_FEATURES,
        n_informative=15,
        n_redundant=2,
        random_state=TRAIN_SEED,
    )
    model = LogisticRegression(random_state=TRAIN_SEED, max_iter=1000)
    model.fit(features, labels)
    return model
