#!/usr/bin/env python3
"""Resilient remote learning client for Apex Tool v15.

The dashboard remains operational when the learning service is unavailable,
but every failure is exposed through last_status(), health() and diagnostics().
POST requests are idempotent and can be retried without duplicating forecasts.
"""
from __future__ import annotations

import hashlib
import json
import os
import random
import ssl
import threading
import time
import uuid
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, Callable, Mapping
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlparse
from urllib.request import Request, urlopen


CLIENT_VERSION = "15.0"
DEFAULT_TIMEOUT = max(1.0, float(os.environ.get("APEX_LEARNING_TIMEOUT", "12")))
PROCESS_TIMEOUT = max(
    DEFAULT_TIMEOUT,
    float(os.environ.get("APEX_LEARNING_PROCESS_TIMEOUT", "18")),
)
MAX_ATTEMPTS = max(1, min(5, int(os.environ.get("APEX_LEARNING_MAX_ATTEMPTS", "3"))))
CIRCUIT_FAILURE_THRESHOLD = max(
    1,
    int(os.environ.get("APEX_LEARNING_CIRCUIT_FAILURES", "3")),
)
CIRCUIT_COOLDOWN_SEC = max(
    5.0,
    float(os.environ.get("APEX_LEARNING_CIRCUIT_COOLDOWN_SEC", "30")),
)
CACHE_TTL_SEC = max(1.0, float(os.environ.get("APEX_LEARNING_STATUS_CACHE_SEC", "10")))
_RETRYABLE_HTTP_CODES = {408, 425, 429, 500, 502, 503, 504}
_STATE_LOCK = threading.RLock()
_BACKGROUND_EXECUTOR = ThreadPoolExecutor(max_workers=2, thread_name_prefix="apex-learning")
_STATE: dict[str, Any] = {
    "consecutive_failures": 0,
    "circuit_open_until": 0.0,
    "last_success_at": None,
    "last_error_at": None,
    "last_error": None,
    "last_request_ms": None,
    "last_request_id": None,
    "last_endpoint": None,
    "background_pending": 0,
}
_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}


class RemoteLearningError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        retryable: bool = False,
        request_id: str | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.retryable = retryable
        self.request_id = request_id


def _base_url() -> str:
    return os.environ.get("APEX_LEARNING_API_URL", "").strip().rstrip("/")


def _api_key() -> str:
    return os.environ.get("APEX_LEARNING_API_KEY", "").strip()


def configured() -> bool:
    return bool(_base_url() and _api_key())


def configuration_diagnostics() -> dict[str, Any]:
    base_url = _base_url()
    parsed = urlparse(base_url) if base_url else None
    key = _api_key()
    placeholder_tokens = {
        "ta_cle_api_render",
        "a-long-secret-key",
        "your-api-key",
        "changeme",
    }
    key_placeholder = key.lower() in placeholder_tokens or "ton_" in key.lower()
    scheme_ok = bool(
        parsed
        and (
            parsed.scheme == "https"
            or parsed.hostname in {"127.0.0.1", "localhost", "::1"}
            or os.environ.get("APEX_LEARNING_ALLOW_HTTP", "").lower() in {"1", "true", "yes"}
        )
    )
    return {
        "configured": bool(base_url and key),
        "api_url": base_url,
        "scheme_ok": scheme_ok,
        "api_key_present": bool(key),
        "api_key_length": len(key),
        "api_key_placeholder": key_placeholder,
        "api_key_strength_ok": len(key) >= 32 and not key_placeholder,
        "client_version": CLIENT_VERSION,
    }


def _validate_configuration() -> None:
    diagnostics = configuration_diagnostics()
    if not diagnostics["configured"]:
        raise RemoteLearningError(
            "Remote learning is not configured. Set APEX_LEARNING_API_URL and "
            "APEX_LEARNING_API_KEY.",
            retryable=False,
        )
    if diagnostics["api_key_placeholder"]:
        raise RemoteLearningError(
            "APEX_LEARNING_API_KEY still contains a placeholder value.",
            retryable=False,
        )
    if not diagnostics["api_key_strength_ok"]:
        raise RemoteLearningError(
            "APEX_LEARNING_API_KEY must contain at least 32 characters.",
            retryable=False,
        )
    if not diagnostics["scheme_ok"]:
        raise RemoteLearningError(
            "APEX_LEARNING_API_URL must use HTTPS except for localhost. "
            "Set APEX_LEARNING_ALLOW_HTTP=true only for a trusted test network.",
            retryable=False,
        )


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def payload_idempotency_key(
    payload: Mapping[str, Any],
    source: str,
    mode: str | None,
) -> str:
    material = {
        "source": source,
        "mode": mode,
        "generated_at": payload.get("generated_at"),
        "payload_hash": hashlib.sha256(
            _canonical_json(payload).encode("utf-8")
        ).hexdigest(),
    }
    return hashlib.sha256(_canonical_json(material).encode("utf-8")).hexdigest()


def _ssl_context() -> ssl.SSLContext:
    return ssl.create_default_context()


def _circuit_check() -> None:
    now = time.monotonic()
    with _STATE_LOCK:
        open_until = float(_STATE["circuit_open_until"] or 0.0)
    if open_until > now:
        remaining = open_until - now
        raise RemoteLearningError(
            f"Remote learning circuit is open for another {remaining:.1f}s after repeated failures.",
            retryable=True,
        )


def _mark_success(
    request_id: str,
    endpoint: str,
    elapsed_ms: float,
) -> None:
    with _STATE_LOCK:
        _STATE.update(
            {
                "consecutive_failures": 0,
                "circuit_open_until": 0.0,
                "last_success_at": time.time(),
                "last_error": None,
                "last_request_ms": round(elapsed_ms, 3),
                "last_request_id": request_id,
                "last_endpoint": endpoint,
            }
        )


def _mark_failure(
    request_id: str,
    endpoint: str,
    elapsed_ms: float,
    error: Exception,
) -> None:
    with _STATE_LOCK:
        failures = int(_STATE["consecutive_failures"] or 0) + 1
        _STATE.update(
            {
                "consecutive_failures": failures,
                "last_error_at": time.time(),
                "last_error": str(error),
                "last_request_ms": round(elapsed_ms, 3),
                "last_request_id": request_id,
                "last_endpoint": endpoint,
            }
        )
        if failures >= CIRCUIT_FAILURE_THRESHOLD:
            _STATE["circuit_open_until"] = time.monotonic() + CIRCUIT_COOLDOWN_SEC


def _error_detail(exc: HTTPError) -> str:
    try:
        payload = exc.read().decode("utf-8", errors="replace")[:4000]
    except Exception:
        return str(exc)
    try:
        parsed = json.loads(payload)
        if isinstance(parsed, dict):
            return str(parsed.get("detail") or parsed.get("error") or parsed)
    except json.JSONDecodeError:
        pass
    return payload or str(exc)


def _request(
    method: str,
    path: str,
    payload: Mapping[str, Any] | None = None,
    *,
    timeout: float = DEFAULT_TIMEOUT,
    idempotency_key: str | None = None,
    attempts: int | None = None,
) -> dict[str, Any]:
    _validate_configuration()
    _circuit_check()
    request_id = uuid.uuid4().hex
    body = (
        None
        if payload is None
        else _canonical_json(payload).encode("utf-8")
    )
    url = _base_url() + path
    max_attempts = attempts if attempts is not None else MAX_ATTEMPTS
    max_attempts = max(1, min(5, int(max_attempts)))
    last_error: RemoteLearningError | None = None

    for attempt in range(1, max_attempts + 1):
        started = time.perf_counter()
        request = Request(url, data=body, method=method.upper())
        request.add_header("Accept", "application/json")
        request.add_header("X-API-Key", _api_key())
        request.add_header("X-Request-ID", request_id)
        request.add_header("User-Agent", f"ApexToolLearningClient/{CLIENT_VERSION}")
        if idempotency_key:
            request.add_header("Idempotency-Key", idempotency_key)
        if body is not None:
            request.add_header("Content-Type", "application/json")
        try:
            kwargs: dict[str, Any] = {"timeout": timeout}
            if urlparse(url).scheme == "https":
                kwargs["context"] = _ssl_context()
            with urlopen(request, **kwargs) as response:
                raw = response.read().decode("utf-8")
                output = json.loads(raw or "{}")
                if not isinstance(output, dict):
                    raise RemoteLearningError(
                        "Remote learning returned a non-object JSON response.",
                        retryable=False,
                        request_id=request_id,
                    )
                elapsed_ms = (time.perf_counter() - started) * 1000.0
                _mark_success(request_id, path, elapsed_ms)
                output.setdefault("request_id", request_id)
                output.setdefault("client_version", CLIENT_VERSION)
                return output
        except HTTPError as exc:
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            retryable = exc.code in _RETRYABLE_HTTP_CODES
            last_error = RemoteLearningError(
                f"Remote learning HTTP {exc.code}: {_error_detail(exc)}",
                status_code=exc.code,
                retryable=retryable,
                request_id=request_id,
            )
            _mark_failure(request_id, path, elapsed_ms, last_error)
        except (URLError, TimeoutError, OSError) as exc:
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            last_error = RemoteLearningError(
                f"Remote learning network error: {exc}",
                retryable=True,
                request_id=request_id,
            )
            _mark_failure(request_id, path, elapsed_ms, last_error)
        except RemoteLearningError as exc:
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            last_error = exc
            _mark_failure(request_id, path, elapsed_ms, exc)
        except Exception as exc:
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            last_error = RemoteLearningError(
                f"Remote learning protocol error: {exc}",
                retryable=False,
                request_id=request_id,
            )
            _mark_failure(request_id, path, elapsed_ms, last_error)

        if (
            last_error is None
            or not last_error.retryable
            or attempt >= max_attempts
        ):
            break
        delay = min(4.0, 0.30 * (2 ** (attempt - 1))) + random.uniform(0.0, 0.18)
        time.sleep(delay)
        _circuit_check()

    assert last_error is not None
    raise last_error


def _cached_request(cache_key: str, path: str, timeout: float) -> dict[str, Any]:
    now = time.monotonic()
    with _STATE_LOCK:
        cached = _CACHE.get(cache_key)
    if cached and now - cached[0] <= CACHE_TTL_SEC:
        output = dict(cached[1])
        output["cached"] = True
        return output
    output = _request("GET", path, timeout=timeout, attempts=2)
    with _STATE_LOCK:
        _CACHE[cache_key] = (now, dict(output))
    return output


def _failure_payload(error: Exception) -> dict[str, Any]:
    diagnostics = configuration_diagnostics()
    return {
        "ok": False,
        "configured": diagnostics["configured"],
        "mode": "remote-only",
        "api_url": diagnostics["api_url"],
        "client_version": CLIENT_VERSION,
        "error": str(error),
        "retryable": bool(getattr(error, "retryable", False)),
        "status_code": getattr(error, "status_code", None),
        "request_id": getattr(error, "request_id", None),
        "client_status": last_status(),
    }


def health(force: bool = False) -> dict[str, Any]:
    if not configured():
        return {
            "ok": False,
            "configured": False,
            "mode": "remote-only",
            "message": "APEX_LEARNING_API_URL/APEX_LEARNING_API_KEY missing",
            "configuration": configuration_diagnostics(),
            "client_status": last_status(),
        }
    try:
        if force:
            with _STATE_LOCK:
                _CACHE.pop("health", None)
        output = _cached_request("health", "/v1/health", timeout=min(DEFAULT_TIMEOUT, 8.0))
        output.update(
            {
                "configured": True,
                "mode": "remote-only",
                "api_url": _base_url(),
                "client_status": last_status(),
            }
        )
        return output
    except Exception as exc:
        return _failure_payload(exc)


def stats(force: bool = False) -> dict[str, Any]:
    if not configured():
        return {
            "ok": False,
            "configured": False,
            "mode": "remote-only",
            "predictions": 0,
            "evaluated": 0,
            "tickers": 0,
            "message": "Remote learning not configured",
            "configuration": configuration_diagnostics(),
            "client_status": last_status(),
        }
    try:
        if force:
            with _STATE_LOCK:
                _CACHE.pop("stats", None)
        output = _cached_request("stats", "/v1/stats", timeout=DEFAULT_TIMEOUT)
        output.update(
            {
                "configured": True,
                "mode": "remote-only",
                "api_url": _base_url(),
                "client_status": last_status(),
            }
        )
        return output
    except Exception as exc:
        return _failure_payload(exc)


def diagnostics(force: bool = False) -> dict[str, Any]:
    if not configured():
        return _failure_payload(
            RemoteLearningError(
                "Remote learning is not configured.",
                retryable=False,
            )
        )
    try:
        if force:
            with _STATE_LOCK:
                _CACHE.pop("diagnostics", None)
        output = _cached_request(
            "diagnostics",
            "/v1/diagnostics",
            timeout=DEFAULT_TIMEOUT,
        )
        output.update(
            {
                "configured": True,
                "mode": "remote-only",
                "api_url": _base_url(),
                "client_status": last_status(),
            }
        )
        return output
    except Exception as exc:
        return _failure_payload(exc)


def calibration(label: str, horizon_day: int | None = None) -> dict[str, Any]:
    path = f"/v1/calibration/{quote(str(label).upper())}"
    if horizon_day is not None:
        path += f"?horizon_day={int(horizon_day)}"
    return _request("GET", path, timeout=DEFAULT_TIMEOUT, attempts=2)


def reliability_curve(label: str, horizon_day: int | None = None) -> dict[str, Any]:
    path = f"/v1/reliability/{quote(str(label).upper())}"
    if horizon_day is not None:
        path += f"?horizon_day={int(horizon_day)}"
    return _request("GET", path, timeout=DEFAULT_TIMEOUT, attempts=2)


def process_payload(
    payload: Mapping[str, Any],
    source: str = "visible_run",
    mode: str | None = None,
    store: bool = True,
    *,
    idempotency_key: str | None = None,
) -> dict[str, Any]:
    key = idempotency_key or payload_idempotency_key(payload, source, mode)
    request_payload = {
        "payload": payload,
        "source": source,
        "mode": mode,
        "store": bool(store),
        "idempotency_key": key,
    }
    output = _request(
        "POST",
        "/v1/process-payload",
        request_payload,
        timeout=PROCESS_TIMEOUT,
        idempotency_key=key,
    )
    with _STATE_LOCK:
        _CACHE.pop("stats", None)
        _CACHE.pop("diagnostics", None)
    return output


def submit_payload_async(
    payload: Mapping[str, Any],
    source: str = "background_run",
    mode: str | None = None,
    store: bool = True,
    *,
    idempotency_key: str | None = None,
    callback: Callable[[dict[str, Any] | None, Exception | None], None] | None = None,
) -> Future[dict[str, Any]]:
    payload_copy = json.loads(json.dumps(payload, ensure_ascii=False, default=str))
    with _STATE_LOCK:
        _STATE["background_pending"] = int(_STATE["background_pending"] or 0) + 1

    def worker() -> dict[str, Any]:
        output: dict[str, Any] | None = None
        error: Exception | None = None
        try:
            output = process_payload(
                payload_copy,
                source=source,
                mode=mode,
                store=store,
                idempotency_key=idempotency_key,
            )
            return output
        except Exception as exc:
            error = exc
            raise
        finally:
            with _STATE_LOCK:
                _STATE["background_pending"] = max(
                    0,
                    int(_STATE["background_pending"] or 0) - 1,
                )
            if callback:
                try:
                    callback(output, error)
                except Exception:
                    pass

    return _BACKGROUND_EXECUTOR.submit(worker)


def last_status() -> dict[str, Any]:
    with _STATE_LOCK:
        state = dict(_STATE)
    open_until = float(state.pop("circuit_open_until") or 0.0)
    remaining = max(0.0, open_until - time.monotonic())
    state["circuit_open"] = remaining > 0
    state["circuit_remaining_sec"] = round(remaining, 3)
    state["configured"] = configured()
    state["client_version"] = CLIENT_VERSION
    return state


__all__ = [
    "CLIENT_VERSION",
    "RemoteLearningError",
    "calibration",
    "configuration_diagnostics",
    "configured",
    "diagnostics",
    "health",
    "last_status",
    "payload_idempotency_key",
    "process_payload",
    "reliability_curve",
    "stats",
    "submit_payload_async",
]
