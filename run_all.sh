#!/usr/bin/env bash

set -euo pipefail

PLATFORM=""
OUTPUT_DIR=""
START_TS="$(date +%s)"

usage() {
  echo "Usage: bash run_all.sh --platform <m1_dockerdesktop|oracle_arm64_linux> [--output-dir <path>]"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --platform)
      PLATFORM="$2"
      shift 2
      ;;
    --output-dir)
      OUTPUT_DIR="$2"
      shift 2
      ;;
    *)
      usage
      exit 1
      ;;
  esac
done

if [[ -z "$PLATFORM" ]]; then
  usage
  exit 1
fi

if [[ -z "$OUTPUT_DIR" ]]; then
  OUTPUT_DIR="./results/${PLATFORM}/"
fi

mkdir -p "$OUTPUT_DIR"

echo "[run_all] Writing results to $OUTPUT_DIR"
python3 write_platform_info.py --platform "$PLATFORM" --output-dir "$OUTPUT_DIR"

echo "[run_all] Running benchmark.py for cold start and warm latency"
python3 benchmark.py --platform "$PLATFORM" --output-dir "$OUTPUT_DIR"

echo "[run_all] Running fixed memory collection"
for MODE in process container; do
  for PAYLOAD in 1 10 100 1024; do
    python3 fix_memory.py \
      --mode "$MODE" \
      --payload-kb "$PAYLOAD" \
      --platform "$PLATFORM" \
      --output-dir "$OUTPUT_DIR"
  done
done

echo "[run_all] Running fixed throughput collection"
python3 fix_throughput.py --platform "$PLATFORM" --output-dir "$OUTPUT_DIR"

echo "[run_all] Verifying outputs"
if ! python3 verify_results.py --dir "$OUTPUT_DIR"; then
  echo "VERIFICATION FAILED — do not use these results"
  exit 1
fi

END_TS="$(date +%s)"
ELAPSED="$((END_TS - START_TS))"
echo "[run_all] Total elapsed time: ${ELAPSED}s"
