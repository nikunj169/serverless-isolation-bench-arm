"""
app.py — Minimal FastAPI function simulating a serverless handler.
Receives a JSON payload, computes SHA256 hash, returns the result.
"""

import hashlib
import json
import os

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

app = FastAPI()


@app.post("/compute")
async def compute(request: Request):
    """
    Core FaaS-style handler.
    Accepts arbitrary JSON payload, returns SHA256 hash of the raw body.
    """
    body = await request.body()
    digest = hashlib.sha256(body).hexdigest()
    payload_size = len(body)
    return JSONResponse(
        content={
            "sha256": digest,
            "payload_bytes": payload_size,
        }
    )


@app.get("/health")
async def health():
    """Simple liveness probe used by runners to confirm server is ready."""
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("app:app", host="0.0.0.0", port=port, log_level="error")
