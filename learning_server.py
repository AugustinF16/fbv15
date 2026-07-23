#!/usr/bin/env python3
"""Authenticated remote learning API for Apex Tool v15.

Recommended production configuration:

  APEX_LEARNING_API_KEY=<strong random secret>
  APEX_LEARNING_DATABASE_URL=postgresql://...

SQLite remains supported when APEX_LEARNING_DB points to a mounted persistent
disk. Set APEX_REQUIRE_PERSISTENT_DB=true to fail fast on an unverified Render
filesystem.
"""
from __future__ import annotations

import hmac
import logging
import os
import threading
import time
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from typing import Any

from fastapi import BackgroundTasks, FastAPI, Header, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

import learning_store


SERVICE_VERSION = "15.0"
API_KEY = os.environ.get("APEX_LEARNING_API_KEY", "").strip()
MAX_REQUEST_BYTES = max(
    1_000_000,
    int(os.environ.get("APEX_LEARNING_MAX_REQUEST_BYTES", "25000000")),
)
RATE_LIMIT_PER_MINUTE = max(
    10,
    int(os.environ.get("APEX_LEARNING_RATE_LIMIT_PER_MINUTE", "180")),
)
ASYNC_WRITES = os.environ.get("APEX_LEARNING_ASYNC_WRITES", "true").strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
LOG_LEVEL = os.environ.get("APEX_LEARNING_LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
LOGGER = logging.getLogger("apex.learning.v15")
_RATE_LOCK = threading.Lock()
_RATE_BUCKETS: dict[str, deque[float]] = defaultdict(deque)

if not API_KEY:
    raise RuntimeError("APEX_LEARNING_API_KEY must be set on the remote learning server")
if len(API_KEY) < 32 or API_KEY.lower() in {
    "ta_cle_api_render",
    "a-long-secret-key",
    "your-api-key",
    "changeme",
}:
    raise RuntimeError(
        "APEX_LEARNING_API_KEY must be a non-placeholder secret of at least 32 characters"
    )


class ProcessRequest(BaseModel):
    payload: dict[str, Any]
    source: str = Field(default="visible_run", max_length=120)
    mode: str | None = Field(default=None, max_length=80)
    store: bool = True
    idempotency_key: str | None = Field(default=None, max_length=240)


@asynccontextmanager
async def lifespan(_: FastAPI):
    learning_store.init_db()
    storage = learning_store.storage_diagnostics()
    integrity = learning_store.database_integrity_check()
    if not integrity["ok"]:
        raise RuntimeError(
            f"Learning database integrity check failed: {integrity['message']}"
        )
    if storage.get("warning"):
        LOGGER.warning("%s", storage["warning"])
    LOGGER.info(
        "Apex learning v%s ready: backend=%s persistent=%s",
        SERVICE_VERSION,
        storage["backend"],
        storage["persistent"],
    )
    yield


app = FastAPI(
    title="Apex Remote Learning API",
    version=SERVICE_VERSION,
    lifespan=lifespan,
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)

cors_origins = [
    origin.strip()
    for origin in os.environ.get("APEX_LEARNING_CORS_ORIGINS", "").split(",")
    if origin.strip()
]
if cors_origins:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=cors_origins,
        allow_credentials=False,
        allow_methods=["GET", "POST"],
        allow_headers=[
            "X-API-Key",
            "X-Request-ID",
            "Idempotency-Key",
            "Content-Type",
        ],
    )


def require_key(x_api_key: str | None) -> None:
    supplied = (x_api_key or "").encode("utf-8")
    expected = API_KEY.encode("utf-8")
    if not supplied or not hmac.compare_digest(supplied, expected):
        raise HTTPException(status_code=401, detail="Invalid API key")


def _client_identifier(request: Request) -> str:
    forwarded = request.headers.get("x-forwarded-for", "").split(",", 1)[0].strip()
    if forwarded:
        return forwarded
    return request.client.host if request.client else "unknown"


def _rate_allowed(identifier: str) -> bool:
    now = time.monotonic()
    cutoff = now - 60.0
    with _RATE_LOCK:
        bucket = _RATE_BUCKETS[identifier]
        while bucket and bucket[0] < cutoff:
            bucket.popleft()
        if len(bucket) >= RATE_LIMIT_PER_MINUTE:
            return False
        bucket.append(now)
        if len(_RATE_BUCKETS) > 5000:
            empty = [key for key, values in _RATE_BUCKETS.items() if not values]
            for key in empty[:1000]:
                _RATE_BUCKETS.pop(key, None)
    return True


@app.middleware("http")
async def request_guard(request: Request, call_next):
    request_id = request.headers.get("x-request-id", "").strip()[:120] or "server-generated"
    content_length = request.headers.get("content-length")
    if content_length:
        try:
            if int(content_length) > MAX_REQUEST_BYTES:
                return JSONResponse(
                    status_code=413,
                    content={
                        "ok": False,
                        "error": "Request payload is too large",
                        "max_request_bytes": MAX_REQUEST_BYTES,
                        "request_id": request_id,
                    },
                )
        except ValueError:
            return JSONResponse(
                status_code=400,
                content={"ok": False, "error": "Invalid Content-Length", "request_id": request_id},
            )
    if not _rate_allowed(_client_identifier(request)):
        return JSONResponse(
            status_code=429,
            content={
                "ok": False,
                "error": "Rate limit exceeded",
                "limit_per_minute": RATE_LIMIT_PER_MINUTE,
                "request_id": request_id,
            },
            headers={"Retry-After": "60"},
        )
    started = time.perf_counter()
    try:
        response = await call_next(request)
    except Exception:
        LOGGER.exception("Unhandled request failure request_id=%s path=%s", request_id, request.url.path)
        raise
    response.headers["X-Request-ID"] = request_id
    response.headers["X-Apex-Learning-Version"] = SERVICE_VERSION
    response.headers["Server-Timing"] = (
        f"app;dur={(time.perf_counter() - started) * 1000.0:.3f}"
    )
    return response


def _background_record(
    req: ProcessRequest,
    calibrated_payload: dict[str, Any],
    idempotency_key: str,
    request_id: str,
) -> None:
    try:
        result = learning_store.record_payload(
            req.payload,
            calibrated_payload,
            source=req.source,
            mode=req.mode,
            store=req.store,
            idempotency_key=idempotency_key,
        )
        LOGGER.info(
            "Background learning write complete request_id=%s run_id=%s evaluated=%s inserted=%s",
            request_id,
            result.get("run_id"),
            result.get("evaluated_exact_targets"),
            result.get("inserted_predictions"),
        )
    except Exception:
        LOGGER.exception(
            "Background learning write failed request_id=%s idempotency_key=%s",
            request_id,
            idempotency_key,
        )


@app.get("/v1/health")
def health(
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> dict[str, Any]:
    require_key(x_api_key)
    storage = learning_store.storage_diagnostics()
    integrity = learning_store.database_integrity_check()
    snapshot = learning_store.stats()
    healthy = not bool(storage.get("warning")) and bool(integrity["ok"])
    return {
        "ok": healthy,
        "status": "healthy" if healthy else "degraded",
        "service": "apex-remote-learning",
        "version": SERVICE_VERSION,
        "schema_version": learning_store.SCHEMA_VERSION,
        "storage": storage,
        "database_integrity": integrity,
        "async_writes": ASYNC_WRITES,
        "stats": {
            "runs": snapshot["runs"],
            "predictions": snapshot["predictions"],
            "evaluated": snapshot["evaluated"],
            "approved_profiles": snapshot["approved_profiles"],
            "last_successful_write": snapshot["last_successful_write"],
        },
    }


@app.get("/v1/stats")
def stats(
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> dict[str, Any]:
    require_key(x_api_key)
    output = learning_store.stats()
    output.update(
        {
            "ok": True,
            "service": "apex-remote-learning",
            "version": SERVICE_VERSION,
        }
    )
    return output


@app.get("/v1/diagnostics")
def diagnostics(
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> dict[str, Any]:
    require_key(x_api_key)
    output = learning_store.diagnostics()
    output["service"] = "apex-remote-learning"
    output["version"] = SERVICE_VERSION
    output["async_writes"] = ASYNC_WRITES
    return output


@app.get("/v1/calibration/{label}")
def calibration(
    label: str,
    horizon_day: int | None = Query(default=None, ge=1, le=3650),
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> dict[str, Any]:
    require_key(x_api_key)
    return learning_store.calibration_detail(label, horizon_day)


@app.get("/v1/reliability/{label}")
def reliability(
    label: str,
    horizon_day: int | None = Query(default=None, ge=1, le=3650),
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> dict[str, Any]:
    require_key(x_api_key)
    return learning_store.reliability_curve(label, horizon_day)


@app.post("/v1/process-payload")
def process_payload(
    req: ProcessRequest,
    background_tasks: BackgroundTasks,
    request: Request,
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
    idempotency_header: str | None = Header(default=None, alias="Idempotency-Key"),
) -> dict[str, Any]:
    require_key(x_api_key)
    request_id = request.headers.get("x-request-id", "").strip()[:120] or "server-generated"
    supplied_key = req.idempotency_key or idempotency_header
    key = supplied_key or learning_store.make_idempotency_key(
        req.payload,
        req.source,
        req.mode,
        None,
    )
    if req.idempotency_key and idempotency_header and req.idempotency_key != idempotency_header:
        raise HTTPException(
            status_code=409,
            detail="Body and header idempotency keys do not match",
        )

    if ASYNC_WRITES and req.store:
        calibrated = learning_store.preview_payload(req.payload)
        calibrated.setdefault("server_learning", {})
        calibrated["server_learning"].update(
            {
                "version": SERVICE_VERSION,
                "write_queued": True,
                "preview_only": False,
                "idempotency_key": key,
                "request_id": request_id,
            }
        )
        calibrated["remote_learning"] = {
            "enabled": True,
            "storage": "server-only",
            "version": SERVICE_VERSION,
            "write_queued": True,
            "request_id": request_id,
        }
        background_tasks.add_task(
            _background_record,
            req,
            calibrated,
            key,
            request_id,
        )
        return calibrated

    output = learning_store.process_payload(
        req.payload,
        source=req.source,
        mode=req.mode,
        store=req.store,
        idempotency_key=key,
    )
    output["remote_learning"] = {
        "enabled": True,
        "storage": "server-only",
        "version": SERVICE_VERSION,
        "write_queued": False,
        "request_id": request_id,
    }
    return output


@app.get("/")
def root() -> dict[str, Any]:
    return {
        "service": "apex-remote-learning",
        "version": SERVICE_VERSION,
        "status_endpoint": "/v1/health",
    }
