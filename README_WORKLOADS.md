# Workload Benchmarking Guide

This project compares **native FastAPI process execution** vs **Docker Desktop ARM64 container execution** on Apple M1 using identical benchmark methodology across four workloads.

The benchmark harness (`benchmark.py`, `fix_memory.py`, `fix_throughput.py`, `run_all.sh`) is unchanged. Select a workload with environment variables before starting the server or running the full pipeline.

## Workloads

| `WORKLOAD` | Description | Request work | Startup work |
|------------|-------------|--------------|--------------|
| `sha256` (default) | Original benchmark | SHA256 hash of raw body | None |
| `json` | API / JSON parsing | Parse JSON, traverse fields, count keys/strings/numbers/depth | None |
| `matrix` | CPU-bound compute | NumPy matrix multiply + checksum | Load fixed `MATRIX_SIZE` matrices (seed 42) |
| `ml` | ML inference | LogisticRegression `predict` + confidence | Load pre-trained `model.pkl` |

All workloads expose:

- `POST /compute` — accepts the same JSON payloads as the SHA256 benchmark
- `GET /health` — liveness probe

Payload sizes used by the harness: **1 KB, 10 KB, 100 KB, 1024 KB (1 MB)**.

### Matrix workload (`WORKLOAD=matrix`)

Matrix computation size is controlled independently from network payload size:

```bash
export WORKLOAD=matrix
export MATRIX_SIZE=512   # 256, 512, 768, or 1024
```

The harness still sends 1/10/100/1024 KB payloads, but matrix dimensions stay fixed for the entire experiment. Run separate benchmark sessions per `MATRIX_SIZE` to isolate compute cost from transfer cost.

### ML workload (`WORKLOAD=ml`)

Training happens **offline** so cold-start latency measures model loading, not training:

```bash
python3 train_model.py    # creates model.pkl
export WORKLOAD=ml
python3 app.py
```

The Docker image runs `train_model.py` during `docker build`, so `model.pkl` is baked into the container.

### ML feature vectors

The harness still sends `{"data": "<synthetic string>"}` payloads. The ML workload derives a deterministic 20-feature vector from the request body (SHA256-seeded) so existing clients work without modification.

## Setup

Install host dependencies:

```bash
pip install -r requirements.txt
```

For ML benchmarks in process mode, train the model first:

```bash
python3 train_model.py
```

Build the ARM64 container image:

```bash
docker build --platform linux/arm64 -t serverless-bench:latest .
```

## Benchmark metadata

`run_all.sh` calls `write_platform_info.py`, which writes:

- `platform_info.json` — hardware / runtime metadata
- `benchmark_metadata.json` — experiment config (`workload`, `matrix_size`, `model_type`, timestamp)
- `benchmark_metadata.csv` — one row per payload size for table generation

Example matrix experiment metadata:

```json
{
  "workload": "matrix",
  "matrix_size": 512,
  "payload_size": "100KB"
}
```

Set `WORKLOAD` and `MATRIX_SIZE` in your shell before `run_all.sh` so metadata matches the run.

## Running benchmarks

Use a separate output directory per workload (and per matrix size):

```bash
export WORKLOAD=matrix
export MATRIX_SIZE=512
export PLATFORM=m1_dockerdesktop
export OUTPUT_DIR="./results/${PLATFORM}_${WORKLOAD}_${MATRIX_SIZE}"

bash run_all.sh --platform "$PLATFORM" --output-dir "$OUTPUT_DIR"
```

ML example:

```bash
python3 train_model.py
export WORKLOAD=ml
bash run_all.sh --platform m1_dockerdesktop --output-dir "./results/m1_dockerdesktop_ml"
```

## Process mode (manual)

```bash
export WORKLOAD=json
python3 app.py
```

```bash
python3 train_model.py
WORKLOAD=ml python3 app.py
```

```bash
WORKLOAD=matrix MATRIX_SIZE=768 python3 app.py
```

## Container mode (manual)

```bash
docker build --platform linux/arm64 -t serverless-bench:latest .

docker run --rm -p 8000:8000 -e WORKLOAD=json serverless-bench:latest
docker run --rm -p 8000:8000 -e WORKLOAD=matrix -e MATRIX_SIZE=512 serverless-bench:latest
docker run --rm -p 8000:8000 -e WORKLOAD=ml serverless-bench:latest
```

Export `WORKLOAD`, `MATRIX_SIZE`, and `MODEL_PATH` in your shell before `benchmark.py`; container runners forward them automatically.

## Research notes

- **Methodology preserved**: cold-start, warm latency, throughput, and memory collection are identical to the SHA256 experiments.
- **Only the server-side compute path changes** via `WORKLOAD` / `MATRIX_SIZE`.
- **ML cold-start** reflects `joblib.load()` overhead, not `fit()` overhead.
- **Matrix experiments** decouple payload transfer size from compute intensity.
