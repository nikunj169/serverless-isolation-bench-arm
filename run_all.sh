#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# run_all.sh — One-shot orchestration script for the full benchmark pipeline.
#
# Steps:
#   1. Verify prerequisites (Docker, Python 3.10+)
#   2. Install Python dependencies into a venv
#   3. Build the ARM64 Docker image
#   4. Run the full benchmark (process + container, all payload sizes)
#   5. Run the analysis and print the summary table
#
# Usage:
#   chmod +x run_all.sh
#   ./run_all.sh
#
# Optional: quick smoke test (fewer runs)
#   COLD_RUNS=3 WARM_RUNS=10 ./run_all.sh
# ─────────────────────────────────────────────────────────────────────────────

set -euo pipefail

# ── Configurable parameters ───────────────────────────────────────────────
COLD_RUNS="${COLD_RUNS:-30}"      # cold-start runs per payload size
WARM_RUNS="${WARM_RUNS:-100}"     # warm requests per payload size
VENV_DIR=".venv"
RESULTS_DIR="results"
PYTHON_BIN="${PYTHON_BIN:-}"

# ── Color helpers ─────────────────────────────────────────────────────────
GREEN="\033[0;32m"
YELLOW="\033[1;33m"
RED="\033[0;31m"
RESET="\033[0m"

log()  { echo -e "${GREEN}[run_all]${RESET} $*"; }
warn() { echo -e "${YELLOW}[run_all] WARNING:${RESET} $*"; }
die()  { echo -e "${RED}[run_all] ERROR:${RESET} $*" >&2; exit 1; }

pick_python() {
    if [[ -n "$PYTHON_BIN" ]]; then
        echo "$PYTHON_BIN"
        return
    fi

    for candidate in python3.12 python3.11 python3.10; do
        if command -v "$candidate" &>/dev/null; then
            echo "$candidate"
            return
        fi
    done

    if command -v python3 &>/dev/null; then
        echo "python3"
        return
    fi

    return 1
}

# ─────────────────────────────────────────────────────────────────────────────
# Step 0: Verify prerequisites
# ─────────────────────────────────────────────────────────────────────────────
log "Step 0: Checking prerequisites …"

PYTHON_BIN="$(pick_python)" || die "No usable Python found. Install Python 3.10-3.12 and retry."
PY_VERSION=$("$PYTHON_BIN" -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
PY_MAJOR=$(echo "$PY_VERSION" | cut -d. -f1)
PY_MINOR=$(echo "$PY_VERSION" | cut -d. -f2)

if [[ "$PY_MAJOR" -lt 3 ]] || { [[ "$PY_MAJOR" -eq 3 ]] && [[ "$PY_MINOR" -lt 10 ]]; }; then
    die "Python 3.10+ required. Found: $PY_VERSION"
fi
if [[ "$PY_MAJOR" -eq 3 ]] && [[ "$PY_MINOR" -ge 13 ]]; then
    die "Python $PY_VERSION is too new for this benchmark environment. Use Python 3.10, 3.11, or 3.12 so scipy installs from wheels instead of failing a source build."
fi
log "  Python $PY_VERSION ✓ ($PYTHON_BIN)"

# Docker
if ! command -v docker &>/dev/null; then
    die "docker not found. Install Docker Desktop for Mac and retry."
fi

if ! docker info &>/dev/null; then
    die "Docker daemon is not running. Start Docker Desktop and retry."
fi
log "  Docker ✓"

# Confirm ARM64 (Apple Silicon)
ARCH=$(uname -m)
if [[ "$ARCH" != "arm64" ]]; then
    warn "Expected arm64, got $ARCH. Benchmark is designed for Apple Silicon."
    warn "Continuing anyway — Docker will use --platform linux/arm64."
fi
log "  Architecture: $ARCH"

# ─────────────────────────────────────────────────────────────────────────────
# Step 1: Create virtual environment and install dependencies
# ─────────────────────────────────────────────────────────────────────────────
log "Step 1: Setting up Python virtual environment …"

if [[ ! -d "$VENV_DIR" ]]; then
    "$PYTHON_BIN" -m venv "$VENV_DIR"
    log "  Created venv at $VENV_DIR"
else
    log "  Venv already exists, skipping creation."
fi

# Activate
# shellcheck disable=SC1090
source "$VENV_DIR/bin/activate"

python -m pip install --upgrade pip
python -m pip install -r requirements.txt
log "  Dependencies installed ✓"

# ─────────────────────────────────────────────────────────────────────────────
# Step 2: Build Docker image
# ─────────────────────────────────────────────────────────────────────────────
log "Step 2: Building Docker image (platform: linux/arm64) …"
docker build --platform linux/arm64 -t serverless-bench:latest .
log "  Image serverless-bench:latest built ✓"

# ─────────────────────────────────────────────────────────────────────────────
# Step 3: Clean up any leftover benchmark containers/ports from previous runs
# ─────────────────────────────────────────────────────────────────────────────
log "Step 3: Cleaning up any leftover containers …"
# Kill any bench-* containers that might be left over from a previous crashed run
docker ps --filter "name=bench-" --format "{{.Names}}" | xargs -r docker stop 2>/dev/null || true
log "  Cleanup done ✓"

# Ensure results directory exists
mkdir -p "$RESULTS_DIR"

# ─────────────────────────────────────────────────────────────────────────────
# Step 4: Run benchmark — process mode
# ─────────────────────────────────────────────────────────────────────────────
log "Step 4: Running PROCESS mode benchmark (cold=${COLD_RUNS}, warm=${WARM_RUNS}) …"
python benchmark.py \
    --mode process \
    --cold-runs "$COLD_RUNS" \
    --warm-runs "$WARM_RUNS" \
    --skip-build
log "  Process benchmark complete ✓"

# ─────────────────────────────────────────────────────────────────────────────
# Step 5: Run benchmark — container mode
# ─────────────────────────────────────────────────────────────────────────────
log "Step 5: Running CONTAINER mode benchmark (cold=${COLD_RUNS}, warm=${WARM_RUNS}) …"
python benchmark.py \
    --mode container \
    --cold-runs "$COLD_RUNS" \
    --warm-runs "$WARM_RUNS" \
    --skip-build
log "  Container benchmark complete ✓"

# ─────────────────────────────────────────────────────────────────────────────
# Step 6: Run memory tracker (background, during a short warm run)
# ─────────────────────────────────────────────────────────────────────────────
log "Step 6: Running memory sampling (30s each mode) …"

# Process memory: start the server, sample for 30s, stop
python -c "
import time, sys
sys.path.insert(0, '.')
from process_runner import ProcessRunner
from memory_tracker import MemoryTracker

with ProcessRunner(port=8000) as runner:
    tracker = MemoryTracker(mode='process', pid=runner.pid, label='idle')
    tracker.start()
    time.sleep(30)
    tracker.stop()
    tracker.save()
print('[run_all] Process memory sampling done.')
"

# Container memory: start container, sample for 30s, stop
python -c "
import time, sys
sys.path.insert(0, '.')
from docker_runner import DockerRunner
from memory_tracker import MemoryTracker

with DockerRunner(host_port=8001) as runner:
    tracker = MemoryTracker(
        mode='container',
        container_name=runner.get_container_name(),
        label='idle'
    )
    tracker.start()
    time.sleep(30)
    tracker.stop()
    tracker.save()
print('[run_all] Container memory sampling done.')
"

log "  Memory tracking complete ✓"

# ─────────────────────────────────────────────────────────────────────────────
# Step 7: Print analysis summary
# ─────────────────────────────────────────────────────────────────────────────
log "Step 7: Analyzing results …"
python analyze.py

# ─────────────────────────────────────────────────────────────────────────────
# Done
# ─────────────────────────────────────────────────────────────────────────────
echo ""
log "════════════════════════════════════════════════════════════"
log "  All done! Results are in ./$RESULTS_DIR/"
log "  benchmark_results.csv  — latency data"
log "  memory_*.csv           — memory samples"
log "════════════════════════════════════════════════════════════"
