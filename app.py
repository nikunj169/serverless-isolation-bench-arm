"""
app.py — Minimal FastAPI function simulating a serverless handler.

Dispatches POST /compute to the workload selected by the WORKLOAD environment
variable (sha256, json, matrix, ml).
"""

import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from workloads import get_workload
from workloads.base import Workload

_workload: Workload | None = None


@asynccontextmanager
async def lifespan(_: FastAPI):
    global _workload
    _workload = get_workload()
    _workload.startup()
    yield


app = FastAPI(lifespan=lifespan)


@app.post("/compute")
async def compute(request: Request):
    """Core FaaS-style handler."""
    body = await request.body()
    result = _workload.compute(body)
    return JSONResponse(content=result)


@app.get("/health")
async def health():
    """Simple liveness probe used by runners to confirm server is ready."""
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("app:app", host="0.0.0.0", port=port, log_level="error")
