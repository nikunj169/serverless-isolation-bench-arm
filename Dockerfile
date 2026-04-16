# ─────────────────────────────────────────────────────────────────────────────
# Dockerfile — ARM64-native image for the serverless benchmark function.
#
# Fix 1: Use requirements-docker.txt (no psutil — avoids gcc build failure
#         in python:3.10-slim which ships without a C compiler).
# Fix 2: No --platform on FROM. Docker Desktop on M1 resolves the image
#         natively. The --platform flag on `docker run` / `docker build`
#         is what enforces ARM64 — not the FROM line.
#
# Build:
#   docker build --platform linux/arm64 -t serverless-bench:latest .
# Run:
#   docker run --rm -p 8000:8000 serverless-bench:latest
# ─────────────────────────────────────────────────────────────────────────────

FROM python:3.10-slim

ENV PYTHONUNBUFFERED=1
ENV PORT=8000

WORKDIR /app

# Install only what the container needs to serve the function.
# psutil is a host-side tool — it does not belong in this image.
COPY requirements-docker.txt .
RUN pip install --no-cache-dir -r requirements-docker.txt

COPY app.py .

EXPOSE 8000

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000", "--log-level", "error"]