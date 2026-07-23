#!/usr/bin/env python3
"""Apex Tool v15 durable forecast evaluation and calibration store.

The store keeps raw forecasts, calibrated forecasts and realised outcomes as
separate records. A forecast is evaluated only when the exact target-session
close is available. Calibration profiles are segmented by ticker, horizon,
market regime and model version, and are activated only after temporal
out-of-sample validation shows a measurable improvement.

Storage backends:
  - SQLite: APEX_LEARNING_DB=/persistent/path/learning.sqlite3
  - PostgreSQL: APEX_LEARNING_DATABASE_URL=postgresql://...

PostgreSQL support requires psycopg>=3. SQLite uses only the standard library.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
import sqlite3
import statistics
import threading
import time
import uuid
from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


SERVICE_VERSION = "15.0"
SCHEMA_VERSION = 15
BASE_DIR = Path(__file__).resolve().parent
DATABASE_URL = (
    os.environ.get("APEX_LEARNING_DATABASE_URL", "").strip()
    or os.environ.get("DATABASE_URL", "").strip()
)
DB_KIND = "postgresql" if DATABASE_URL.startswith(("postgresql://", "postgres://")) else "sqlite"
DB_PATH = Path(
    os.environ.get(
        "APEX_LEARNING_DB",
        BASE_DIR / ".apex_server_learning" / "learning-v15.sqlite3",
    )
).expanduser()

PROFILE_SOURCE_LIMIT = max(2_000, int(os.environ.get("APEX_LEARNING_PROFILE_SOURCE_LIMIT", "100000")))
PROFILE_MAX_SAMPLES = max(60, int(os.environ.get("APEX_LEARNING_PROFILE_MAX_SAMPLES", "600")))
PROFILE_MIN_EXACT = max(12, int(os.environ.get("APEX_LEARNING_MIN_EXACT_SAMPLES", "18")))
PROFILE_MIN_FALLBACK = max(24, int(os.environ.get("APEX_LEARNING_MIN_FALLBACK_SAMPLES", "40")))
PROFILE_HALF_LIFE_DAYS = max(30.0, float(os.environ.get("APEX_LEARNING_HALF_LIFE_DAYS", "180")))
PROBABILITY_BIN_COUNT = 10
_DB_LOCK = threading.RLock()
_INITIALISED = False


PREDICTION_COLUMNS: dict[str, str] = {
    "raw_predicted_price": "DOUBLE PRECISION",
    "raw_predicted_pct": "DOUBLE PRECISION",
    "calibrated_predicted_price": "DOUBLE PRECISION",
    "calibrated_predicted_pct": "DOUBLE PRECISION",
    "raw_low": "DOUBLE PRECISION",
    "raw_high": "DOUBLE PRECISION",
    "calibrated_low": "DOUBLE PRECISION",
    "calibrated_high": "DOUBLE PRECISION",
    "raw_confidence": "DOUBLE PRECISION",
    "calibrated_confidence": "DOUBLE PRECISION",
    "raw_probability_up": "DOUBLE PRECISION",
    "calibrated_probability_up": "DOUBLE PRECISION",
    "engine_version": "TEXT",
    "feature_version": "TEXT",
    "model_version": "TEXT",
    "regime": "TEXT",
    "data_snapshot_hash": "TEXT",
    "target_match_exact": "INTEGER DEFAULT 0",
    "raw_error_pct": "DOUBLE PRECISION",
    "calibrated_error_pct": "DOUBLE PRECISION",
    "raw_abs_error_pct": "DOUBLE PRECISION",
    "calibrated_abs_error_pct": "DOUBLE PRECISION",
    "raw_direction_ok": "INTEGER",
    "calibrated_direction_ok": "INTEGER",
    "raw_brier_score": "DOUBLE PRECISION",
    "calibrated_brier_score": "DOUBLE PRECISION",
    "actual_direction_up": "INTEGER",
    "realised_source": "TEXT",
}

RUN_COLUMNS: dict[str, str] = {
    "idempotency_key": "TEXT",
    "engine_version": "TEXT",
    "feature_version": "TEXT",
    "model_version": "TEXT",
    "raw_payload_json": "TEXT",
    "calibrated_payload_json": "TEXT",
    "completed_at": "TEXT",
}


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _safe_float(value: Any, default: float | None = 0.0) -> float | None:
    try:
        number = float(value)
        if math.isfinite(number):
            return number
    except (TypeError, ValueError):
        pass
    return default


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _json_copy(value: Any) -> Any:
    return json.loads(json.dumps(value, ensure_ascii=False, default=str))


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def _payload_hash(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()


def _normalise_label(value: Any) -> str:
    return str(value or "").strip().upper()


def _normalise_token(value: Any, fallback: str = "unknown") -> str:
    token = str(value or "").strip()
    return token if token else fallback


def _normalise_date(value: Any) -> str | None:
    text = str(value or "").strip()[:10]
    if not text:
        return None
    try:
        return date.fromisoformat(text).isoformat()
    except ValueError:
        return None


def _as_dict(row: Any) -> dict[str, Any]:
    if row is None:
        return {}
    if isinstance(row, dict):
        return dict(row)
    if isinstance(row, sqlite3.Row):
        return {key: row[key] for key in row.keys()}
    if isinstance(row, Mapping):
        return dict(row)
    raise TypeError(f"Unsupported database row type: {type(row)!r}")


def _sql(statement: str) -> str:
    return statement.replace("?", "%s") if DB_KIND == "postgresql" else statement


def _execute(conn: Any, statement: str, params: Sequence[Any] = ()) -> Any:
    return conn.execute(_sql(statement), tuple(params))


class _ClosingSQLiteConnection(sqlite3.Connection):
    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> bool:
        try:
            return bool(super().__exit__(exc_type, exc_value, traceback))
        finally:
            self.close()


def connect() -> Any:
    if DB_KIND == "postgresql":
        try:
            import psycopg
            from psycopg.rows import dict_row
        except ImportError as exc:
            raise RuntimeError(
                "APEX_LEARNING_DATABASE_URL is PostgreSQL but psycopg is missing. "
                "Install psycopg[binary]>=3.1."
            ) from exc
        url = DATABASE_URL
        if url.startswith("postgres://"):
            url = "postgresql://" + url[len("postgres://") :]
        return psycopg.connect(url, row_factory=dict_row, connect_timeout=10)

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(
        str(DB_PATH),
        timeout=30,
        isolation_level="DEFERRED",
        factory=_ClosingSQLiteConnection,
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def _table_columns(conn: Any, table: str) -> set[str]:
    if DB_KIND == "postgresql":
        rows = _execute(
            conn,
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema='public' AND table_name=?
            """,
            (table,),
        ).fetchall()
        return {str(_as_dict(row).get("column_name")) for row in rows}
    rows = _execute(conn, f"PRAGMA table_info({table})").fetchall()
    return {str(_as_dict(row).get("name")) for row in rows}


def _ensure_columns(conn: Any, table: str, columns: Mapping[str, str]) -> None:
    existing = _table_columns(conn, table)
    for name, definition in columns.items():
        if name in existing:
            continue
        if DB_KIND == "postgresql":
            _execute(conn, f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {name} {definition}")
        else:
            _execute(conn, f"ALTER TABLE {table} ADD COLUMN {name} {definition}")


def _schema_statements() -> list[str]:
    prediction_id = "BIGSERIAL PRIMARY KEY" if DB_KIND == "postgresql" else "INTEGER PRIMARY KEY AUTOINCREMENT"
    event_id = "BIGSERIAL PRIMARY KEY" if DB_KIND == "postgresql" else "INTEGER PRIMARY KEY AUTOINCREMENT"
    return [
        """
        CREATE TABLE IF NOT EXISTS meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS prediction_runs (
            run_id TEXT PRIMARY KEY,
            generated_at TEXT,
            days INTEGER,
            mode TEXT,
            source TEXT,
            success_count INTEGER,
            count INTEGER,
            payload_json TEXT,
            created_at TEXT NOT NULL
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS predictions (
            id {prediction_id},
            run_id TEXT NOT NULL,
            label TEXT NOT NULL,
            name TEXT,
            horizon_day INTEGER NOT NULL,
            run_date TEXT,
            target_date TEXT NOT NULL,
            start_price DOUBLE PRECISION,
            predicted_price DOUBLE PRECISION,
            predicted_pct DOUBLE PRECISION,
            confidence DOUBLE PRECISION,
            risk TEXT,
            signal TEXT,
            features_json TEXT,
            evaluated INTEGER DEFAULT 0,
            actual_price DOUBLE PRECISION,
            actual_pct DOUBLE PRECISION,
            error_pct DOUBLE PRECISION,
            abs_error_pct DOUBLE PRECISION,
            direction_ok INTEGER,
            evaluated_at TEXT,
            UNIQUE(run_id, label, horizon_day)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS realised_prices (
            label TEXT NOT NULL,
            price_date TEXT NOT NULL,
            close_price DOUBLE PRECISION NOT NULL,
            source TEXT,
            source_timestamp TEXT,
            data_snapshot_hash TEXT,
            ingested_at TEXT NOT NULL,
            PRIMARY KEY(label, price_date)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS calibration_profiles (
            label TEXT NOT NULL,
            horizon_day INTEGER NOT NULL,
            regime TEXT NOT NULL,
            model_version TEXT NOT NULL,
            scope TEXT NOT NULL,
            sample_count INTEGER NOT NULL,
            effective_sample_count DOUBLE PRECISION NOT NULL,
            validation_count INTEGER NOT NULL,
            raw_mae_pct DOUBLE PRECISION,
            calibrated_mae_pct DOUBLE PRECISION,
            mae_improvement_pct DOUBLE PRECISION,
            bias_pct DOUBLE PRECISION,
            directional_accuracy DOUBLE PRECISION,
            calibrated_directional_accuracy DOUBLE PRECISION,
            raw_brier_score DOUBLE PRECISION,
            calibrated_brier_score DOUBLE PRECISION,
            reliability DOUBLE PRECISION NOT NULL,
            approved INTEGER NOT NULL,
            intercept DOUBLE PRECISION NOT NULL,
            slope DOUBLE PRECISION NOT NULL,
            residual_q05 DOUBLE PRECISION,
            residual_q95 DOUBLE PRECISION,
            probability_bins_json TEXT,
            last_updated TEXT NOT NULL,
            PRIMARY KEY(label, horizon_day, regime, model_version)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS learning_events (
            id {event_id},
            created_at TEXT NOT NULL,
            level TEXT NOT NULL,
            event_type TEXT NOT NULL,
            run_id TEXT,
            message TEXT NOT NULL,
            details_json TEXT
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_predictions_eval ON predictions(evaluated, label, target_date)",
        "CREATE INDEX IF NOT EXISTS idx_realised_label_date ON realised_prices(label, price_date)",
        "CREATE INDEX IF NOT EXISTS idx_profiles_lookup ON calibration_profiles(label, horizon_day, regime, model_version, approved)",
        "CREATE INDEX IF NOT EXISTS idx_learning_events_created ON learning_events(created_at)",
    ]


def init_db(force: bool = False) -> None:
    global _INITIALISED
    if _INITIALISED and not force:
        return
    with _DB_LOCK:
        if _INITIALISED and not force:
            return
        persistence = storage_diagnostics()
        if persistence["required"] and not persistence["persistent"]:
            raise RuntimeError(str(persistence["warning"]))
        with connect() as conn:
            for statement in _schema_statements():
                _execute(conn, statement)
            _ensure_columns(conn, "prediction_runs", RUN_COLUMNS)
            _ensure_columns(conn, "predictions", PREDICTION_COLUMNS)
            _execute(
                conn,
                "CREATE INDEX IF NOT EXISTS idx_predictions_segment "
                "ON predictions(label, horizon_day, regime, model_version, evaluated)",
            )
            _execute(
                conn,
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_prediction_runs_idempotency "
                "ON prediction_runs(idempotency_key)",
            )
            _execute(
                conn,
                """
                UPDATE predictions
                SET raw_predicted_price=COALESCE(raw_predicted_price,predicted_price),
                    raw_predicted_pct=COALESCE(raw_predicted_pct,predicted_pct),
                    calibrated_predicted_price=COALESCE(calibrated_predicted_price,predicted_price),
                    calibrated_predicted_pct=COALESCE(calibrated_predicted_pct,predicted_pct),
                    raw_confidence=COALESCE(raw_confidence,confidence),
                    calibrated_confidence=COALESCE(calibrated_confidence,confidence),
                    raw_error_pct=COALESCE(raw_error_pct,error_pct),
                    raw_abs_error_pct=COALESCE(raw_abs_error_pct,abs_error_pct),
                    raw_direction_ok=COALESCE(raw_direction_ok,direction_ok)
                """,
            )
            _execute(
                conn,
                """
                INSERT INTO meta(key,value) VALUES('schema_version',?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value
                """,
                (str(SCHEMA_VERSION),),
            )
            _execute(
                conn,
                """
                INSERT INTO meta(key,value) VALUES('service_version',?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value
                """,
                (SERVICE_VERSION,),
            )
            conn.commit()
        _INITIALISED = True


def storage_diagnostics() -> dict[str, Any]:
    render = bool(os.environ.get("RENDER") or os.environ.get("RENDER_SERVICE_ID"))
    required = os.environ.get("APEX_REQUIRE_PERSISTENT_DB", "").strip().lower() in {"1", "true", "yes", "on"}
    explicit = os.environ.get("APEX_LEARNING_STORAGE_PERSISTENT", "").strip().lower()
    persistent_root = os.environ.get("APEX_LEARNING_PERSISTENT_ROOT", "").strip()

    if DB_KIND == "postgresql":
        persistent = True
        location = "postgresql"
    else:
        resolved = DB_PATH.resolve()
        location = str(resolved)
        if explicit in {"1", "true", "yes", "on"}:
            persistent = True
        elif explicit in {"0", "false", "no", "off"}:
            persistent = False
        elif persistent_root:
            try:
                resolved.relative_to(Path(persistent_root).expanduser().resolve())
                persistent = True
            except ValueError:
                persistent = False
        else:
            persistent = not render

    warning = None
    if not persistent:
        warning = (
            "Learning storage is not confirmed persistent. On Render, point "
            "APEX_LEARNING_DB to a mounted persistent disk and set "
            "APEX_LEARNING_PERSISTENT_ROOT, or use APEX_LEARNING_DATABASE_URL "
            "with PostgreSQL."
        )
    return {
        "backend": DB_KIND,
        "location": location,
        "persistent": persistent,
        "required": required,
        "render_detected": render,
        "warning": warning,
    }


def _record_event(
    level: str,
    event_type: str,
    message: str,
    run_id: str | None = None,
    details: Mapping[str, Any] | None = None,
) -> None:
    try:
        init_db()
        with connect() as conn:
            _execute(
                conn,
                """
                INSERT INTO learning_events(created_at,level,event_type,run_id,message,details_json)
                VALUES(?,?,?,?,?,?)
                """,
                (
                    utc_now(),
                    level,
                    event_type,
                    run_id,
                    str(message)[:4000],
                    _canonical_json(details or {}),
                ),
            )
            conn.commit()
    except Exception:
        # Event logging must never hide the original processing error.
        return


def _result_metadata(result: Mapping[str, Any], payload: Mapping[str, Any]) -> dict[str, str]:
    audit = result.get("audit_trail") if isinstance(result.get("audit_trail"), Mapping) else {}
    model = result.get("model") if isinstance(result.get("model"), Mapping) else {}
    professional = (
        result.get("professional_decision")
        if isinstance(result.get("professional_decision"), Mapping)
        else {}
    )
    engine_version = _normalise_token(
        audit.get("engine_version")
        or professional.get("model_version")
        or payload.get("engine")
        or "setup-stats-unknown"
    )
    feature_version = _normalise_token(
        audit.get("feature_version")
        or professional.get("feature_version")
        or "features-unknown"
    )
    model_version = _normalise_token(
        professional.get("model_version")
        or model.get("name")
        or engine_version
    )
    regime = _normalise_token(
        result.get("regime")
        or professional.get("regime")
        or result.get("risk")
        or "unknown"
    ).lower()
    snapshot_hash = _normalise_token(
        audit.get("data_snapshot_hash")
        or (result.get("data_quality") or {}).get("snapshot_hash")
        if isinstance(result.get("data_quality"), Mapping)
        else audit.get("data_snapshot_hash"),
        fallback="",
    )
    return {
        "engine_version": engine_version,
        "feature_version": feature_version,
        "model_version": model_version,
        "regime": regime,
        "data_snapshot_hash": snapshot_hash,
    }


def _extract_probability_up(result: Mapping[str, Any]) -> float | None:
    candidates: list[Any] = [
        result.get("probability_up"),
        (result.get("professional_decision") or {}).get("probability_up")
        if isinstance(result.get("professional_decision"), Mapping)
        else None,
        ((result.get("analysis") or {}).get("scenario") or {}).get("probability_up")
        if isinstance(result.get("analysis"), Mapping)
        else None,
    ]
    for candidate in candidates:
        number = _safe_float(candidate, None)
        if number is None:
            continue
        if number > 1.0 and number <= 100.0:
            number /= 100.0
        if 0.0 <= number <= 1.0:
            return float(number)
    return None


def _is_finalised_close(
    price_date: str,
    generated_date: str | None,
    row: Mapping[str, Any] | None = None,
    result: Mapping[str, Any] | None = None,
) -> bool:
    """Reject an in-progress daily bar unless finality is explicit."""
    row = row or {}
    result = result or {}
    explicit_flags = (
        row.get("is_final"),
        row.get("final"),
        row.get("session_complete"),
        result.get("close_is_final"),
    )
    if any(flag is True or str(flag).strip().lower() in {"1", "true", "yes"} for flag in explicit_flags):
        return True
    if generated_date is None:
        return price_date < date.today().isoformat()
    return price_date < generated_date


def ingest_realised_prices(payload: Mapping[str, Any]) -> int:
    """Ingest final daily closes carried by the current engine payload."""
    init_db()
    generated_date = _normalise_date(payload.get("generated_at")) or date.today().isoformat()
    rows_to_store: dict[tuple[str, str], tuple[float, str, str | None, str]] = {}
    for result in payload.get("results", []) or []:
        if not isinstance(result, Mapping) or result.get("error"):
            continue
        label = _normalise_label(result.get("label"))
        if not label:
            continue
        metadata = _result_metadata(result, payload)
        source = _normalise_token(
            (result.get("data_quality") or {}).get("source")
            if isinstance(result.get("data_quality"), Mapping)
            else result.get("source"),
            fallback="engine_history",
        )
        history = result.get("history") if isinstance(result.get("history"), list) else []
        for history_row in history:
            if not isinstance(history_row, Mapping):
                continue
            price_date = _normalise_date(history_row.get("date"))
            close = _safe_float(history_row.get("close"), None)
            if not price_date or close is None or close <= 0:
                continue
            if not _is_finalised_close(price_date, generated_date, history_row, result):
                continue
            rows_to_store[(label, price_date)] = (
                close,
                source,
                str(history_row.get("timestamp") or "") or None,
                metadata["data_snapshot_hash"],
            )
        last_date = _normalise_date(result.get("last_date"))
        last_price = _safe_float(result.get("last"), None)
        if (
            last_date
            and last_price is not None
            and last_price > 0
            and _is_finalised_close(last_date, generated_date, None, result)
        ):
            rows_to_store[(label, last_date)] = (
                last_price,
                source,
                str(result.get("source_timestamp") or "") or None,
                metadata["data_snapshot_hash"],
            )

    if not rows_to_store:
        return 0

    with _DB_LOCK, connect() as conn:
        for (label, price_date), values in rows_to_store.items():
            close, source, source_timestamp, snapshot_hash = values
            _execute(
                conn,
                """
                INSERT INTO realised_prices(
                    label,price_date,close_price,source,source_timestamp,
                    data_snapshot_hash,ingested_at
                ) VALUES(?,?,?,?,?,?,?)
                ON CONFLICT(label,price_date) DO UPDATE SET
                    close_price=excluded.close_price,
                    source=excluded.source,
                    source_timestamp=excluded.source_timestamp,
                    data_snapshot_hash=excluded.data_snapshot_hash,
                    ingested_at=excluded.ingested_at
                """,
                (
                    label,
                    price_date,
                    close,
                    source,
                    source_timestamp,
                    snapshot_hash,
                    utc_now(),
                ),
            )
        conn.commit()
    return len(rows_to_store)


def _direction_ok(predicted_pct: float, actual_pct: float) -> int:
    epsilon = 0.03
    if abs(predicted_pct) < epsilon and abs(actual_pct) < epsilon:
        return 1
    if abs(predicted_pct) < epsilon or abs(actual_pct) < epsilon:
        return 0
    return int((predicted_pct > 0) == (actual_pct > 0))


def evaluate_exact_targets() -> int:
    """Evaluate pending forecasts only against an exact target-date close."""
    init_db()
    with _DB_LOCK, connect() as conn:
        rows = _execute(
            conn,
            """
            SELECT p.*, rp.close_price AS realised_close, rp.source AS realised_price_source
            FROM predictions p
            JOIN realised_prices rp
              ON rp.label=p.label AND rp.price_date=p.target_date
            WHERE p.evaluated=0
            ORDER BY p.target_date ASC, p.id ASC
            """,
        ).fetchall()
        evaluated = 0
        now = utc_now()
        for raw_row in rows:
            row = _as_dict(raw_row)
            start_price = _safe_float(row.get("start_price"), None)
            actual_price = _safe_float(row.get("realised_close"), None)
            raw_pct = _safe_float(
                row.get("raw_predicted_pct"),
                _safe_float(row.get("predicted_pct"), 0.0),
            )
            calibrated_pct = _safe_float(
                row.get("calibrated_predicted_pct"),
                raw_pct,
            )
            if (
                start_price is None
                or actual_price is None
                or raw_pct is None
                or calibrated_pct is None
                or start_price <= 0
                or actual_price <= 0
            ):
                continue
            actual_pct = (actual_price / start_price - 1.0) * 100.0
            raw_error = actual_pct - raw_pct
            calibrated_error = actual_pct - calibrated_pct
            outcome_up = int(actual_pct > 0.0)
            raw_probability = _safe_float(row.get("raw_probability_up"), None)
            calibrated_probability = _safe_float(row.get("calibrated_probability_up"), None)
            raw_brier = (
                (raw_probability - outcome_up) ** 2
                if raw_probability is not None and 0.0 <= raw_probability <= 1.0
                else None
            )
            calibrated_brier = (
                (calibrated_probability - outcome_up) ** 2
                if calibrated_probability is not None and 0.0 <= calibrated_probability <= 1.0
                else None
            )
            raw_direction = _direction_ok(raw_pct, actual_pct)
            calibrated_direction = _direction_ok(calibrated_pct, actual_pct)
            _execute(
                conn,
                """
                UPDATE predictions
                SET evaluated=1,
                    target_match_exact=1,
                    actual_price=?,
                    actual_pct=?,
                    raw_error_pct=?,
                    calibrated_error_pct=?,
                    raw_abs_error_pct=?,
                    calibrated_abs_error_pct=?,
                    raw_direction_ok=?,
                    calibrated_direction_ok=?,
                    raw_brier_score=?,
                    calibrated_brier_score=?,
                    actual_direction_up=?,
                    realised_source=?,
                    error_pct=?,
                    abs_error_pct=?,
                    direction_ok=?,
                    evaluated_at=?
                WHERE id=?
                """,
                (
                    actual_price,
                    actual_pct,
                    raw_error,
                    calibrated_error,
                    abs(raw_error),
                    abs(calibrated_error),
                    raw_direction,
                    calibrated_direction,
                    raw_brier,
                    calibrated_brier,
                    outcome_up,
                    row.get("realised_price_source"),
                    raw_error,
                    abs(raw_error),
                    raw_direction,
                    now,
                    row["id"],
                ),
            )
            evaluated += 1
        conn.commit()
    return evaluated


def _weighted_mean(values: Sequence[float], weights: Sequence[float]) -> float:
    total = sum(weights)
    if total <= 0:
        return statistics.fmean(values) if values else 0.0
    return sum(value * weight for value, weight in zip(values, weights)) / total


def _weighted_quantile(values: Sequence[float], weights: Sequence[float], probability: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(zip(values, weights), key=lambda item: item[0])
    total = sum(max(0.0, weight) for _, weight in ordered)
    if total <= 0:
        index = min(len(ordered) - 1, max(0, round((len(ordered) - 1) * probability)))
        return float(ordered[index][0])
    target = _clamp(probability, 0.0, 1.0) * total
    cumulative = 0.0
    for value, weight in ordered:
        cumulative += max(0.0, weight)
        if cumulative >= target:
            return float(value)
    return float(ordered[-1][0])


def _effective_sample_size(weights: Sequence[float]) -> float:
    total = sum(weights)
    denominator = sum(weight * weight for weight in weights)
    return total * total / denominator if denominator > 0 else 0.0


def _row_weights(rows: Sequence[Mapping[str, Any]]) -> list[float]:
    parsed_dates = [
        date.fromisoformat(str(row.get("target_date")))
        for row in rows
        if _normalise_date(row.get("target_date"))
    ]
    latest = max(parsed_dates) if parsed_dates else date.today()
    weights: list[float] = []
    for row in rows:
        target = _normalise_date(row.get("target_date"))
        age_days = max(0, (latest - date.fromisoformat(target)).days) if target else 0
        weights.append(0.5 ** (age_days / PROFILE_HALF_LIFE_DAYS))
    return weights


def _fit_linear_calibration(
    rows: Sequence[Mapping[str, Any]],
    weights: Sequence[float],
) -> tuple[float, float]:
    raw = [float(row["raw_predicted_pct"]) for row in rows]
    actual = [float(row["actual_pct"]) for row in rows]
    raw_mean = _weighted_mean(raw, weights)
    actual_mean = _weighted_mean(actual, weights)
    covariance = sum(
        weight * (x - raw_mean) * (y - actual_mean)
        for x, y, weight in zip(raw, actual, weights)
    )
    variance = sum(weight * (x - raw_mean) ** 2 for x, weight in zip(raw, weights))
    if variance <= 1e-12:
        slope = 0.0
    else:
        ridge = variance * 0.08
        slope = covariance / (variance + ridge)
    slope = _clamp(slope, -0.25, 1.50)
    actual_scale = max(
        0.35,
        _weighted_quantile([abs(value) for value in actual], weights, 0.90),
    )
    intercept = _clamp(actual_mean - slope * raw_mean, -actual_scale, actual_scale)
    return intercept, slope


def _probability_bins(
    rows: Sequence[Mapping[str, Any]],
    weights: Sequence[float],
) -> list[dict[str, Any]]:
    buckets: list[list[tuple[float, int, float]]] = [[] for _ in range(PROBABILITY_BIN_COUNT)]
    for row, weight in zip(rows, weights):
        probability = _safe_float(row.get("raw_probability_up"), None)
        outcome = row.get("actual_direction_up")
        if probability is None or outcome is None or not 0.0 <= probability <= 1.0:
            continue
        index = min(PROBABILITY_BIN_COUNT - 1, int(probability * PROBABILITY_BIN_COUNT))
        buckets[index].append((probability, int(outcome), weight))

    all_observations = [item for bucket in buckets for item in bucket]
    if all_observations:
        global_weight = sum(item[2] for item in all_observations)
        global_up = sum(item[1] * item[2] for item in all_observations)
        global_rate = (global_up + 2.0) / (global_weight + 4.0)
    else:
        global_rate = 0.5

    output: list[dict[str, Any]] = []
    for index, bucket in enumerate(buckets):
        low = index / PROBABILITY_BIN_COUNT
        high = (index + 1) / PROBABILITY_BIN_COUNT
        if not bucket:
            output.append(
                {
                    "bin": index,
                    "low": low,
                    "high": high,
                    "count": 0,
                    "weight": 0.0,
                    "mean_predicted": (low + high) / 2.0,
                    "observed_up": None,
                    "calibrated_probability": global_rate,
                }
            )
            continue
        bucket_weight = sum(item[2] for item in bucket)
        predicted = sum(item[0] * item[2] for item in bucket) / bucket_weight
        successes = sum(item[1] * item[2] for item in bucket)
        observed = successes / bucket_weight
        prior_strength = 6.0
        calibrated = (
            successes + global_rate * prior_strength
        ) / (bucket_weight + prior_strength)
        output.append(
            {
                "bin": index,
                "low": low,
                "high": high,
                "count": len(bucket),
                "weight": round(bucket_weight, 6),
                "mean_predicted": round(predicted, 6),
                "observed_up": round(observed, 6),
                "calibrated_probability": round(_clamp(calibrated, 0.01, 0.99), 6),
            }
        )
    return output


def _map_probability(probability: float | None, bins: Sequence[Mapping[str, Any]]) -> float | None:
    if probability is None or not bins:
        return probability
    index = min(PROBABILITY_BIN_COUNT - 1, max(0, int(probability * PROBABILITY_BIN_COUNT)))
    selected = bins[index]
    calibrated = _safe_float(selected.get("calibrated_probability"), probability)
    return _clamp(float(calibrated), 0.01, 0.99) if calibrated is not None else probability


def _metric_mae(
    rows: Sequence[Mapping[str, Any]],
    predicted: Sequence[float],
    weights: Sequence[float],
) -> float:
    errors = [
        abs(float(row["actual_pct"]) - prediction)
        for row, prediction in zip(rows, predicted)
    ]
    return _weighted_mean(errors, weights)


def _metric_direction(
    rows: Sequence[Mapping[str, Any]],
    predicted: Sequence[float],
    weights: Sequence[float],
) -> float:
    outcomes = [
        float(_direction_ok(prediction, float(row["actual_pct"])))
        for row, prediction in zip(rows, predicted)
    ]
    return _weighted_mean(outcomes, weights)


def _metric_brier(
    rows: Sequence[Mapping[str, Any]],
    probabilities: Sequence[float | None],
    weights: Sequence[float],
) -> float | None:
    values: list[float] = []
    value_weights: list[float] = []
    for row, probability, weight in zip(rows, probabilities, weights):
        if probability is None or row.get("actual_direction_up") is None:
            continue
        values.append((probability - int(row["actual_direction_up"])) ** 2)
        value_weights.append(weight)
    return _weighted_mean(values, value_weights) if values else None


def _fit_profile(
    key: tuple[str, int, str, str],
    source_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    label, horizon_day, regime, model_version = key
    rows = sorted(
        source_rows[-PROFILE_MAX_SAMPLES:],
        key=lambda row: (
            str(row.get("target_date") or ""),
            str(row.get("evaluated_at") or ""),
        ),
    )
    sample_count = len(rows)
    weights = _row_weights(rows)
    effective_count = _effective_sample_size(weights)
    validation_size = max(5, int(round(sample_count * 0.30)))
    split_index = max(1, sample_count - validation_size)
    train_rows = rows[:split_index]
    validation_rows = rows[split_index:]
    train_weights = weights[:split_index]
    validation_weights = weights[split_index:]

    intercept_train, slope_train = _fit_linear_calibration(train_rows, train_weights)
    bins_train = _probability_bins(train_rows, train_weights)
    raw_validation = [float(row["raw_predicted_pct"]) for row in validation_rows]
    calibrated_validation = [
        intercept_train + slope_train * raw_value for raw_value in raw_validation
    ]
    raw_mae = _metric_mae(validation_rows, raw_validation, validation_weights)
    calibrated_mae = _metric_mae(
        validation_rows,
        calibrated_validation,
        validation_weights,
    )
    improvement = (
        (raw_mae - calibrated_mae) / raw_mae * 100.0
        if raw_mae > 1e-12
        else 0.0
    )
    raw_direction = _metric_direction(validation_rows, raw_validation, validation_weights)
    calibrated_direction = _metric_direction(
        validation_rows,
        calibrated_validation,
        validation_weights,
    )
    raw_probabilities = [
        _safe_float(row.get("raw_probability_up"), None) for row in validation_rows
    ]
    calibrated_probabilities = [
        _map_probability(probability, bins_train)
        for probability in raw_probabilities
    ]
    raw_brier = _metric_brier(validation_rows, raw_probabilities, validation_weights)
    calibrated_brier = _metric_brier(
        validation_rows,
        calibrated_probabilities,
        validation_weights,
    )

    scope = (
        "ticker_horizon_regime_model"
        if label != "*" and regime != "*"
        else "ticker_horizon_model"
        if label != "*"
        else "global_horizon_regime_model"
        if regime != "*"
        else "global_horizon_model"
    )
    required_samples = PROFILE_MIN_EXACT if scope == "ticker_horizon_regime_model" else PROFILE_MIN_FALLBACK
    probability_improved = (
        raw_brier is not None
        and calibrated_brier is not None
        and calibrated_brier <= raw_brier - 0.002
    )
    mae_improved = improvement >= 0.50
    mae_not_materially_worse = calibrated_mae <= raw_mae * 1.01 + 1e-9
    direction_not_materially_worse = calibrated_direction + 0.03 >= raw_direction
    approved = bool(
        sample_count >= required_samples
        and len(validation_rows) >= 5
        and direction_not_materially_worse
        and (
            mae_improved
            or (probability_improved and mae_not_materially_worse)
        )
    )

    intercept, slope = _fit_linear_calibration(rows, weights)
    bins = _probability_bins(rows, weights)
    fitted_all = [
        intercept + slope * float(row["raw_predicted_pct"])
        for row in rows
    ]
    residuals = [
        float(row["actual_pct"]) - prediction
        for row, prediction in zip(rows, fitted_all)
    ]
    residual_q05 = _weighted_quantile(residuals, weights, 0.05)
    residual_q95 = _weighted_quantile(residuals, weights, 0.95)
    bias = _weighted_mean(
        [
            float(row["actual_pct"]) - float(row["raw_predicted_pct"])
            for row in rows
        ],
        weights,
    )

    evidence = 1.0 - math.exp(-effective_count / 32.0)
    direction_skill = _clamp((raw_direction - 0.50) / 0.20, 0.0, 1.0)
    mae_skill = _clamp(improvement / 12.0, 0.0, 1.0)
    brier_skill = (
        _clamp(1.0 - raw_brier / 0.25, 0.0, 1.0)
        if raw_brier is not None
        else 0.35
    )
    stability = _clamp(1.0 - abs(slope - 1.0) / 1.5, 0.0, 1.0)
    reliability = evidence * (
        0.30 * direction_skill
        + 0.35 * mae_skill
        + 0.25 * brier_skill
        + 0.10 * stability
    )
    if not approved:
        reliability *= 0.25
    reliability = _clamp(reliability, 0.0, 0.97)

    return {
        "label": label,
        "horizon_day": horizon_day,
        "regime": regime,
        "model_version": model_version,
        "scope": scope,
        "sample_count": sample_count,
        "effective_sample_count": effective_count,
        "validation_count": len(validation_rows),
        "raw_mae_pct": raw_mae,
        "calibrated_mae_pct": calibrated_mae,
        "mae_improvement_pct": improvement,
        "bias_pct": bias,
        "directional_accuracy": raw_direction,
        "calibrated_directional_accuracy": calibrated_direction,
        "raw_brier_score": raw_brier,
        "calibrated_brier_score": calibrated_brier,
        "reliability": reliability,
        "approved": int(approved),
        "intercept": intercept,
        "slope": slope,
        "residual_q05": residual_q05,
        "residual_q95": residual_q95,
        "probability_bins_json": _canonical_json(bins),
        "last_updated": utc_now(),
    }


def recompute_calibration_profiles() -> int:
    """Build validated profiles with ticker and global fallback hierarchies."""
    init_db()
    with _DB_LOCK, connect() as conn:
        raw_rows = _execute(
            conn,
            """
            SELECT label,horizon_day,target_date,raw_predicted_pct,
                   raw_probability_up,actual_pct,actual_direction_up,
                   regime,model_version,evaluated_at
            FROM predictions
            WHERE evaluated=1
              AND target_match_exact=1
              AND raw_predicted_pct IS NOT NULL
              AND actual_pct IS NOT NULL
            ORDER BY target_date DESC, id DESC
            LIMIT ?
            """,
            (PROFILE_SOURCE_LIMIT,),
        ).fetchall()
        rows = [_as_dict(row) for row in reversed(raw_rows)]
        groups: dict[tuple[str, int, str, str], list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            label = _normalise_label(row.get("label"))
            horizon = int(row.get("horizon_day") or 0)
            regime = _normalise_token(row.get("regime"), "unknown").lower()
            model_version = _normalise_token(row.get("model_version"), "unknown")
            if not label or horizon <= 0:
                continue
            keys = {
                (label, horizon, regime, model_version),
                (label, horizon, "*", model_version),
                ("*", horizon, regime, model_version),
                ("*", horizon, "*", model_version),
            }
            for key in keys:
                groups[key].append(row)

        profiles = [
            _fit_profile(key, group_rows)
            for key, group_rows in groups.items()
            if len(group_rows) >= 6
        ]
        _execute(conn, "DELETE FROM calibration_profiles")
        for profile in profiles:
            _execute(
                conn,
                """
                INSERT INTO calibration_profiles(
                    label,horizon_day,regime,model_version,scope,
                    sample_count,effective_sample_count,validation_count,
                    raw_mae_pct,calibrated_mae_pct,mae_improvement_pct,bias_pct,
                    directional_accuracy,calibrated_directional_accuracy,
                    raw_brier_score,calibrated_brier_score,reliability,approved,
                    intercept,slope,residual_q05,residual_q95,
                    probability_bins_json,last_updated
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    profile["label"],
                    profile["horizon_day"],
                    profile["regime"],
                    profile["model_version"],
                    profile["scope"],
                    profile["sample_count"],
                    profile["effective_sample_count"],
                    profile["validation_count"],
                    profile["raw_mae_pct"],
                    profile["calibrated_mae_pct"],
                    profile["mae_improvement_pct"],
                    profile["bias_pct"],
                    profile["directional_accuracy"],
                    profile["calibrated_directional_accuracy"],
                    profile["raw_brier_score"],
                    profile["calibrated_brier_score"],
                    profile["reliability"],
                    profile["approved"],
                    profile["intercept"],
                    profile["slope"],
                    profile["residual_q05"],
                    profile["residual_q95"],
                    profile["probability_bins_json"],
                    profile["last_updated"],
                ),
            )
        conn.commit()
    return len(profiles)


def _load_profiles() -> dict[tuple[str, int, str, str], dict[str, Any]]:
    init_db()
    with connect() as conn:
        rows = _execute(conn, "SELECT * FROM calibration_profiles").fetchall()
    return {
        (
            str(profile["label"]),
            int(profile["horizon_day"]),
            str(profile["regime"]),
            str(profile["model_version"]),
        ): profile
        for profile in (_as_dict(row) for row in rows)
    }


def _select_profile(
    profiles: Mapping[tuple[str, int, str, str], dict[str, Any]],
    label: str,
    horizon_day: int,
    regime: str,
    model_version: str,
    approved_only: bool,
) -> tuple[dict[str, Any] | None, float]:
    candidates = [
        ((label, horizon_day, regime, model_version), 1.00),
        ((label, horizon_day, "*", model_version), 0.88),
        (("*", horizon_day, regime, model_version), 0.72),
        (("*", horizon_day, "*", model_version), 0.62),
    ]
    best_unapproved: tuple[dict[str, Any] | None, float] = (None, 0.0)
    for key, scope_factor in candidates:
        profile = profiles.get(key)
        if not profile:
            continue
        if int(profile.get("approved") or 0):
            return profile, scope_factor
        if best_unapproved[0] is None:
            best_unapproved = (profile, scope_factor)
    return (None, 0.0) if approved_only else best_unapproved


def _apply_profile_to_forecast(
    forecast: dict[str, Any],
    start_price: float,
    profile: Mapping[str, Any] | None,
    scope_factor: float,
    raw_probability: float | None,
) -> tuple[float, float | None]:
    raw_pct = float(_safe_float(forecast.get("pct"), 0.0) or 0.0)
    raw_price = float(
        _safe_float(
            forecast.get("price"),
            start_price * (1.0 + raw_pct / 100.0) if start_price > 0 else 0.0,
        )
        or 0.0
    )
    raw_low = float(_safe_float(forecast.get("low"), raw_price) or raw_price)
    raw_high = float(_safe_float(forecast.get("high"), raw_price) or raw_price)
    corrected_pct = raw_pct
    calibrated_probability = raw_probability
    low_price = raw_low
    high_price = raw_high

    if profile and int(profile.get("approved") or 0):
        reliability = _clamp(
            float(_safe_float(profile.get("reliability"), 0.0) or 0.0) * scope_factor,
            0.0,
            0.97,
        )
        intercept = float(_safe_float(profile.get("intercept"), 0.0) or 0.0)
        slope = float(_safe_float(profile.get("slope"), 1.0) or 1.0)
        target_pct = intercept + slope * raw_pct
        raw_mae = float(_safe_float(profile.get("raw_mae_pct"), 0.5) or 0.5)
        correction_limit = max(0.50, raw_mae * 1.50)
        correction = _clamp(target_pct - raw_pct, -correction_limit, correction_limit)
        corrected_pct = raw_pct + reliability * correction
        try:
            bins = json.loads(str(profile.get("probability_bins_json") or "[]"))
        except json.JSONDecodeError:
            bins = []
        mapped_probability = _map_probability(raw_probability, bins)
        if mapped_probability is not None and raw_probability is not None:
            calibrated_probability = _clamp(
                raw_probability + reliability * (mapped_probability - raw_probability),
                0.01,
                0.99,
            )
        corrected_price = (
            start_price * (1.0 + corrected_pct / 100.0)
            if start_price > 0
            else raw_price
        )
        if start_price > 0:
            raw_low_pct = (raw_low / start_price - 1.0) * 100.0
            raw_high_pct = (raw_high / start_price - 1.0) * 100.0
            residual_q05 = float(_safe_float(profile.get("residual_q05"), -raw_mae) or -raw_mae)
            residual_q95 = float(_safe_float(profile.get("residual_q95"), raw_mae) or raw_mae)
            profile_low_pct = corrected_pct + residual_q05
            profile_high_pct = corrected_pct + residual_q95
            conservative_low_pct = min(raw_low_pct, profile_low_pct, corrected_pct)
            conservative_high_pct = max(raw_high_pct, profile_high_pct, corrected_pct)
            low_price = max(0.000001, start_price * (1.0 + conservative_low_pct / 100.0))
            high_price = max(low_price, start_price * (1.0 + conservative_high_pct / 100.0))
        forecast["price"] = corrected_price
    else:
        forecast["price"] = raw_price

    forecast["raw_price_before_learning"] = raw_price
    forecast["raw_pct_before_learning"] = raw_pct
    forecast["raw_low_before_learning"] = raw_low
    forecast["raw_high_before_learning"] = raw_high
    forecast["pct"] = corrected_pct
    forecast["low"] = low_price
    forecast["high"] = high_price
    if raw_probability is not None:
        forecast["raw_probability_up_before_learning"] = raw_probability
        forecast["probability_up"] = calibrated_probability
    return corrected_pct, calibrated_probability


def apply_calibration_to_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Apply only validated profiles and expose the complete audit delta."""
    init_db()
    out = _json_copy(payload)
    profiles = _load_profiles()
    applied_count = 0
    for result in out.get("results", []) or []:
        if not isinstance(result, dict) or result.get("error"):
            continue
        label = _normalise_label(result.get("label"))
        metadata = _result_metadata(result, out)
        regime = metadata["regime"]
        model_version = metadata["model_version"]
        start_price = float(_safe_float(result.get("last"), 0.0) or 0.0)
        raw_confidence = float(_safe_float(result.get("confidence"), 50.0) or 50.0)
        raw_probability = _extract_probability_up(result)
        forecasts = result.get("forecast") if isinstance(result.get("forecast"), list) else []
        profile_audit: list[dict[str, Any]] = []
        final_profile: dict[str, Any] | None = None
        final_scope_factor = 0.0
        final_probability = raw_probability
        final_raw_pct = 0.0
        final_calibrated_pct = 0.0

        for index, forecast in enumerate(forecasts, 1):
            if not isinstance(forecast, dict):
                continue
            horizon_day = int(forecast.get("horizon_day") or index)
            profile, scope_factor = _select_profile(
                profiles,
                label,
                horizon_day,
                regime,
                model_version,
                approved_only=True,
            )
            evidence_profile, evidence_scope = _select_profile(
                profiles,
                label,
                horizon_day,
                regime,
                model_version,
                approved_only=False,
            )
            raw_pct = float(_safe_float(forecast.get("pct"), 0.0) or 0.0)
            calibrated_pct, calibrated_probability = _apply_profile_to_forecast(
                forecast,
                start_price,
                profile,
                scope_factor,
                raw_probability,
            )
            audit_profile = profile or evidence_profile
            profile_audit.append(
                {
                    "horizon_day": horizon_day,
                    "target_date": forecast.get("date"),
                    "applied": bool(profile),
                    "scope": audit_profile.get("scope") if audit_profile else None,
                    "sample_count": int(audit_profile.get("sample_count") or 0)
                    if audit_profile
                    else 0,
                    "reliability": round(
                        float(_safe_float(audit_profile.get("reliability"), 0.0) or 0.0)
                        * (scope_factor if profile else evidence_scope),
                        4,
                    )
                    if audit_profile
                    else 0.0,
                    "raw_pct": round(raw_pct, 6),
                    "calibrated_pct": round(calibrated_pct, 6),
                    "validation_mae_improvement_pct": round(
                        float(_safe_float(audit_profile.get("mae_improvement_pct"), 0.0) or 0.0),
                        4,
                    )
                    if audit_profile
                    else None,
                }
            )
            if profile:
                applied_count += 1
            final_profile = profile or evidence_profile
            final_scope_factor = scope_factor if profile else evidence_scope
            final_probability = calibrated_probability
            final_raw_pct = raw_pct
            final_calibrated_pct = calibrated_pct

        if final_profile:
            sample_count = int(final_profile.get("sample_count") or 0)
            effective_count = float(
                _safe_float(final_profile.get("effective_sample_count"), sample_count) or sample_count
            )
            evidence = 1.0 - math.exp(-effective_count / 35.0)
            reliability = float(_safe_float(final_profile.get("reliability"), 0.0) or 0.0)
            reliability *= final_scope_factor
            direction = float(
                _safe_float(final_profile.get("directional_accuracy"), 0.5) or 0.5
            )
            improvement = float(
                _safe_float(final_profile.get("mae_improvement_pct"), 0.0) or 0.0
            )
            confidence = 50.0 + (raw_confidence - 50.0) * (0.60 + 0.40 * evidence)
            confidence += reliability * ((direction - 0.50) * 26.0 + max(0.0, improvement) * 0.12)
        else:
            sample_count = 0
            effective_count = 0.0
            reliability = 0.0
            direction = 0.5
            improvement = 0.0
            confidence = 50.0 + (raw_confidence - 50.0) * 0.60
        confidence = _clamp(confidence, 5.0, 95.0)

        result["raw_confidence_before_learning"] = raw_confidence
        result["confidence"] = round(confidence, 2)
        if raw_probability is not None:
            result["raw_probability_up_before_learning"] = raw_probability
            result["probability_up"] = final_probability
            if isinstance(result.get("professional_decision"), dict):
                result["professional_decision"]["probability_up"] = final_probability
        if forecasts:
            result["change_5d_pct"] = final_calibrated_pct
            result["change_horizon_pct"] = final_calibrated_pct

        was_applied = any(item["applied"] for item in profile_audit)
        reason = (
            "validated temporal holdout improvement"
            if was_applied
            else "no profile passed the minimum evidence and holdout-improvement gates"
        )
        result["server_learning"] = {
            "version": SERVICE_VERSION,
            "applied": was_applied,
            "reason": reason,
            "sample_count": sample_count,
            "effective_sample_count": round(effective_count, 3),
            "reliability": round(reliability, 4),
            "directional_accuracy": round(direction, 4) if sample_count else None,
            "mae_improvement_pct": round(improvement, 4) if sample_count else None,
            "raw_forecast_pct": round(final_raw_pct, 6),
            "calibrated_forecast_pct": round(final_calibrated_pct, 6),
            "forecast_delta_pct": round(final_calibrated_pct - final_raw_pct, 6),
            "raw_confidence": round(raw_confidence, 2),
            "calibrated_confidence": round(confidence, 2),
            "confidence_delta": round(confidence - raw_confidence, 2),
            "profile_scope": final_profile.get("scope") if final_profile else None,
            "horizons": profile_audit,
        }
        result["learning"] = {
            "mae_pct": round(
                float(_safe_float(final_profile.get("raw_mae_pct"), 0.0) or 0.0),
                4,
            )
            if final_profile
            else None,
            "directional_accuracy": round(direction, 4) if sample_count else None,
            "evaluated_this_run": 0,
            "count": sample_count,
            "reliability": round(reliability, 4),
            "bias_correction_pct": round(final_calibrated_pct - final_raw_pct, 6),
            "source": "server v15 exact-target validated calibration",
        }

    storage = storage_diagnostics()
    out["learning_db"] = {
        "enabled": True,
        "version": SERVICE_VERSION,
        "schema_version": SCHEMA_VERSION,
        "backend": storage["backend"],
        "persistent": storage["persistent"],
        "updated_at": utc_now(),
    }
    out["server_learning"] = {
        "version": SERVICE_VERSION,
        "calibration_applied_to_forecasts": applied_count,
        "profile_policy": "ticker+horizon+regime+model with validated fallbacks",
        "confidence_policy": "evidence decay plus holdout skill",
    }
    return out


def _idempotency_key(
    payload: Mapping[str, Any],
    source: str,
    mode: str | None,
    supplied: str | None,
) -> str:
    if supplied:
        return str(supplied).strip()[:240]
    material = {
        "source": source,
        "mode": mode,
        "generated_at": payload.get("generated_at"),
        "payload_hash": _payload_hash(payload),
    }
    return hashlib.sha256(_canonical_json(material).encode("utf-8")).hexdigest()


def make_idempotency_key(
    payload: Mapping[str, Any],
    source: str = "visible_run",
    mode: str | None = None,
    supplied: str | None = None,
) -> str:
    return _idempotency_key(payload, source, mode, supplied)


def _run_id_from_key(key: str) -> str:
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()[:28]
    return f"run_{digest}"


def store_predictions(
    raw_payload: Mapping[str, Any],
    calibrated_payload: Mapping[str, Any],
    source: str = "visible_run",
    mode: str | None = None,
    idempotency_key: str | None = None,
) -> dict[str, Any]:
    """Persist raw and calibrated forecasts without recursive self-training."""
    init_db()
    key = _idempotency_key(raw_payload, source, mode, idempotency_key)
    run_id = _run_id_from_key(key)
    raw_json = _canonical_json(raw_payload)
    calibrated_json = _canonical_json(calibrated_payload)
    raw_results = {
        _normalise_label(result.get("label")): result
        for result in raw_payload.get("results", []) or []
        if isinstance(result, Mapping) and not result.get("error")
    }
    calibrated_results = {
        _normalise_label(result.get("label")): result
        for result in calibrated_payload.get("results", []) or []
        if isinstance(result, Mapping) and not result.get("error")
    }
    inserted_predictions = 0

    with _DB_LOCK, connect() as conn:
        existing = _execute(
            conn,
            "SELECT run_id FROM prediction_runs WHERE idempotency_key=?",
            (key,),
        ).fetchone()
        if existing:
            return {
                "run_id": _as_dict(existing)["run_id"],
                "idempotency_key": key,
                "duplicate": True,
                "inserted_predictions": 0,
            }

        first_result = next(iter(raw_results.values()), {})
        metadata = _result_metadata(first_result, raw_payload) if first_result else {
            "engine_version": _normalise_token(raw_payload.get("engine")),
            "feature_version": "unknown",
            "model_version": _normalise_token(raw_payload.get("engine")),
        }
        run_insert = _execute(
            conn,
            """
            INSERT INTO prediction_runs(
                run_id,idempotency_key,generated_at,days,mode,source,
                success_count,count,payload_json,raw_payload_json,
                calibrated_payload_json,engine_version,feature_version,
                model_version,created_at,completed_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(idempotency_key) DO NOTHING
            """,
            (
                run_id,
                key,
                str(raw_payload.get("generated_at") or utc_now()),
                int(raw_payload.get("days") or 0),
                mode,
                source,
                int(raw_payload.get("success_count") or 0),
                int(raw_payload.get("count") or 0),
                raw_json,
                raw_json,
                calibrated_json,
                metadata["engine_version"],
                metadata["feature_version"],
                metadata["model_version"],
                utc_now(),
                utc_now(),
            ),
        )
        if int(getattr(run_insert, "rowcount", 0) or 0) == 0:
            existing = _execute(
                conn,
                "SELECT run_id FROM prediction_runs WHERE idempotency_key=?",
                (key,),
            ).fetchone()
            if existing:
                return {
                    "run_id": _as_dict(existing)["run_id"],
                    "idempotency_key": key,
                    "duplicate": True,
                    "inserted_predictions": 0,
                }
            raise RuntimeError(
                "Idempotent run insert was skipped but the existing run could not be read"
            )

        for label, raw_result in raw_results.items():
            calibrated_result = calibrated_results.get(label, {})
            result_metadata = _result_metadata(raw_result, raw_payload)
            start_price = float(_safe_float(raw_result.get("last"), 0.0) or 0.0)
            raw_confidence = float(_safe_float(raw_result.get("confidence"), 50.0) or 50.0)
            calibrated_confidence = float(
                _safe_float(calibrated_result.get("confidence"), raw_confidence) or raw_confidence
            )
            raw_probability = _extract_probability_up(raw_result)
            calibrated_probability = _extract_probability_up(calibrated_result)
            calibrated_by_date = {
                str(item.get("date") or ""): item
                for item in calibrated_result.get("forecast", []) or []
                if isinstance(item, Mapping)
            }
            features = {
                "audit_trail": raw_result.get("audit_trail"),
                "data_quality": raw_result.get("data_quality"),
                "model": raw_result.get("model"),
                "professional_decision": raw_result.get("professional_decision"),
                "analysis": raw_result.get("analysis"),
                "news_score": (raw_result.get("news") or {}).get("score")
                if isinstance(raw_result.get("news"), Mapping)
                else None,
                "investment_score": raw_result.get("investment_score"),
                "volatility_ann_pct": raw_result.get("volatility_ann_pct"),
            }
            for index, raw_forecast in enumerate(raw_result.get("forecast", []) or [], 1):
                if not isinstance(raw_forecast, Mapping):
                    continue
                target_date = _normalise_date(raw_forecast.get("date"))
                if not target_date:
                    continue
                horizon_day = int(raw_forecast.get("horizon_day") or index)
                calibrated_forecast = calibrated_by_date.get(target_date, {})
                raw_pct = float(_safe_float(raw_forecast.get("pct"), 0.0) or 0.0)
                raw_price = float(_safe_float(raw_forecast.get("price"), 0.0) or 0.0)
                calibrated_pct = float(
                    _safe_float(calibrated_forecast.get("pct"), raw_pct) or raw_pct
                )
                calibrated_price = float(
                    _safe_float(calibrated_forecast.get("price"), raw_price) or raw_price
                )
                cursor = _execute(
                    conn,
                    """
                    INSERT INTO predictions(
                        run_id,label,name,horizon_day,run_date,target_date,
                        start_price,predicted_price,predicted_pct,confidence,
                        risk,signal,features_json,evaluated,
                        raw_predicted_price,raw_predicted_pct,
                        calibrated_predicted_price,calibrated_predicted_pct,
                        raw_low,raw_high,calibrated_low,calibrated_high,
                        raw_confidence,calibrated_confidence,
                        raw_probability_up,calibrated_probability_up,
                        engine_version,feature_version,model_version,regime,
                        data_snapshot_hash,target_match_exact
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,0,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0)
                    ON CONFLICT(run_id,label,horizon_day) DO NOTHING
                    """,
                    (
                        run_id,
                        label,
                        raw_result.get("name"),
                        horizon_day,
                        _normalise_date(raw_result.get("last_date")),
                        target_date,
                        start_price,
                        raw_price,
                        raw_pct,
                        raw_confidence,
                        raw_result.get("risk"),
                        raw_result.get("signal"),
                        _canonical_json(features),
                        raw_price,
                        raw_pct,
                        calibrated_price,
                        calibrated_pct,
                        _safe_float(raw_forecast.get("low"), raw_price),
                        _safe_float(raw_forecast.get("high"), raw_price),
                        _safe_float(calibrated_forecast.get("low"), calibrated_price),
                        _safe_float(calibrated_forecast.get("high"), calibrated_price),
                        raw_confidence,
                        calibrated_confidence,
                        raw_probability,
                        calibrated_probability,
                        result_metadata["engine_version"],
                        result_metadata["feature_version"],
                        result_metadata["model_version"],
                        result_metadata["regime"],
                        result_metadata["data_snapshot_hash"],
                    ),
                )
                if getattr(cursor, "rowcount", 0) and cursor.rowcount > 0:
                    inserted_predictions += int(cursor.rowcount)
        conn.commit()

    _record_event(
        "info",
        "run_stored",
        f"Stored {inserted_predictions} raw/calibrated forecast rows",
        run_id=run_id,
        details={"source": source, "mode": mode, "idempotency_key": key},
    )
    return {
        "run_id": run_id,
        "idempotency_key": key,
        "duplicate": False,
        "inserted_predictions": inserted_predictions,
    }


def preview_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Return a fast, read-only calibration preview using existing profiles."""
    started = time.perf_counter()
    calibrated = apply_calibration_to_payload(payload)
    calibrated.setdefault("server_learning", {})
    calibrated["server_learning"].update(
        {
            "version": SERVICE_VERSION,
            "write_queued": False,
            "preview_only": True,
            "processing_ms": round((time.perf_counter() - started) * 1000.0, 3),
        }
    )
    return calibrated


def record_payload(
    raw_payload: Mapping[str, Any],
    calibrated_payload: Mapping[str, Any] | None = None,
    source: str = "visible_run",
    mode: str | None = None,
    store: bool = True,
    idempotency_key: str | None = None,
) -> dict[str, Any]:
    """Ingest exact outcomes, evaluate, rebuild profiles and persist a run."""
    started = time.perf_counter()
    run_id = _run_id_from_key(_idempotency_key(raw_payload, source, mode, idempotency_key))
    try:
        ingested = ingest_realised_prices(raw_payload)
        evaluated = evaluate_exact_targets()
        profile_count = recompute_calibration_profiles() if evaluated else _profile_count()
        calibrated = (
            _json_copy(calibrated_payload)
            if calibrated_payload is not None
            else apply_calibration_to_payload(raw_payload)
        )
        stored = {
            "run_id": run_id,
            "duplicate": False,
            "inserted_predictions": 0,
        }
        if store:
            stored = store_predictions(
                raw_payload,
                calibrated,
                source=source,
                mode=mode,
                idempotency_key=idempotency_key,
            )
            run_id = str(stored["run_id"])
        result = {
            "ok": True,
            "version": SERVICE_VERSION,
            "run_id": run_id,
            "ingested_exact_closes": ingested,
            "evaluated_exact_targets": evaluated,
            "calibration_profiles": profile_count,
            "stored": bool(store),
            "duplicate": bool(stored.get("duplicate")),
            "inserted_predictions": int(stored.get("inserted_predictions") or 0),
            "processing_ms": round((time.perf_counter() - started) * 1000.0, 3),
        }
        return result
    except Exception as exc:
        _record_event(
            "error",
            "record_failed",
            str(exc),
            run_id=run_id,
            details={"source": source, "mode": mode},
        )
        raise


def process_payload(
    payload: Mapping[str, Any],
    source: str = "visible_run",
    mode: str | None = None,
    store: bool = True,
    idempotency_key: str | None = None,
) -> dict[str, Any]:
    """Synchronous reference path used by tests and non-background servers."""
    started = time.perf_counter()
    ingest_realised_prices(payload)
    evaluated = evaluate_exact_targets()
    if evaluated:
        recompute_calibration_profiles()
    calibrated = apply_calibration_to_payload(payload)
    stored = {
        "run_id": _run_id_from_key(_idempotency_key(payload, source, mode, idempotency_key)),
        "duplicate": False,
        "inserted_predictions": 0,
    }
    if store:
        stored = store_predictions(
            payload,
            calibrated,
            source=source,
            mode=mode,
            idempotency_key=idempotency_key,
        )
    calibrated.setdefault("server_learning", {})
    calibrated["server_learning"].update(
        {
            "version": SERVICE_VERSION,
            "evaluated_this_run": evaluated,
            "write_queued": False,
            "stored": bool(store),
            "run_id": stored["run_id"],
            "duplicate": bool(stored.get("duplicate")),
            "processing_ms": round((time.perf_counter() - started) * 1000.0, 3),
        }
    )
    calibrated["run_id"] = stored["run_id"]
    return calibrated


def _scalar(conn: Any, statement: str, params: Sequence[Any] = ()) -> int:
    row = _execute(conn, statement, params).fetchone()
    if row is None:
        return 0
    values = list(_as_dict(row).values())
    return int(values[0] or 0) if values else 0


def _profile_count() -> int:
    init_db()
    with connect() as conn:
        return _scalar(conn, "SELECT COUNT(*) AS count FROM calibration_profiles")


def database_integrity_check() -> dict[str, Any]:
    """Run a cheap read-only integrity probe suitable for health diagnostics."""
    init_db()
    started = time.perf_counter()
    try:
        with connect() as conn:
            if DB_KIND == "sqlite":
                row = _execute(conn, "PRAGMA quick_check").fetchone()
                values = list(_as_dict(row).values()) if row is not None else []
                message = str(values[0] if values else "no result")
                ok = message.lower() == "ok"
            else:
                row = _execute(conn, "SELECT 1 AS database_ok").fetchone()
                ok = int(_as_dict(row).get("database_ok") or 0) == 1
                message = "ok" if ok else "database probe failed"
    except Exception as exc:
        ok = False
        message = str(exc)
    return {
        "ok": ok,
        "message": message[:1000],
        "checked_at": utc_now(),
        "latency_ms": round((time.perf_counter() - started) * 1000.0, 3),
    }


def stats() -> dict[str, Any]:
    init_db()
    with connect() as conn:
        prediction_count = _scalar(conn, "SELECT COUNT(*) AS count FROM predictions")
        evaluated_count = _scalar(
            conn,
            "SELECT COUNT(*) AS count FROM predictions WHERE evaluated=1 AND target_match_exact=1",
        )
        pending_count = _scalar(
            conn,
            "SELECT COUNT(*) AS count FROM predictions WHERE evaluated=0",
        )
        ticker_count = _scalar(
            conn,
            "SELECT COUNT(DISTINCT label) AS count FROM predictions",
        )
        run_count = _scalar(conn, "SELECT COUNT(*) AS count FROM prediction_runs")
        profile_count = _scalar(conn, "SELECT COUNT(*) AS count FROM calibration_profiles")
        approved_count = _scalar(
            conn,
            "SELECT COUNT(*) AS count FROM calibration_profiles WHERE approved=1",
        )
        exact_prices = _scalar(conn, "SELECT COUNT(*) AS count FROM realised_prices")
        last_run = _execute(
            conn,
            """
            SELECT run_id,source,mode,created_at,completed_at,engine_version,
                   feature_version,model_version
            FROM prediction_runs
            ORDER BY created_at DESC
            LIMIT 1
            """,
        ).fetchone()
        top_rows = _execute(
            conn,
            """
            SELECT label,horizon_day,regime,model_version,scope,sample_count,
                   validation_count,raw_mae_pct,calibrated_mae_pct,
                   mae_improvement_pct,directional_accuracy,raw_brier_score,
                   calibrated_brier_score,reliability,approved,last_updated
            FROM calibration_profiles
            ORDER BY approved DESC,reliability DESC,sample_count DESC
            LIMIT 30
            """,
        ).fetchall()
        outcome_summary = _execute(
            conn,
            """
            SELECT AVG(raw_abs_error_pct) AS raw_mae,
                   AVG(calibrated_abs_error_pct) AS calibrated_mae,
                   AVG(raw_brier_score) AS raw_brier,
                   AVG(calibrated_brier_score) AS calibrated_brier,
                   AVG(raw_direction_ok) AS raw_direction,
                   AVG(calibrated_direction_ok) AS calibrated_direction
            FROM predictions
            WHERE evaluated=1 AND target_match_exact=1
            """,
        ).fetchone()

    outcome_metrics = _as_dict(outcome_summary) if outcome_summary else {}
    leaderboard = [
        {
            "model": "raw_engine",
            "evaluation_set": "exact target-session closes",
            "sample_count": evaluated_count,
            "mae_pct": outcome_metrics.get("raw_mae"),
            "brier_score": outcome_metrics.get("raw_brier"),
            "directional_accuracy": outcome_metrics.get("raw_direction"),
        },
        {
            "model": "calibrated_v15",
            "evaluation_set": "exact target-session closes",
            "sample_count": evaluated_count,
            "mae_pct": outcome_metrics.get("calibrated_mae"),
            "brier_score": outcome_metrics.get("calibrated_brier"),
            "directional_accuracy": outcome_metrics.get("calibrated_direction"),
        },
    ]
    leaderboard.sort(
        key=lambda item: (
            item["mae_pct"] is None,
            float(item["mae_pct"]) if item["mae_pct"] is not None else math.inf,
        )
    )
    for rank, row in enumerate(leaderboard, 1):
        row["rank_by_mae"] = rank if row["mae_pct"] is not None else None

    storage = storage_diagnostics()
    db_size = None
    if DB_KIND == "sqlite":
        try:
            db_size = DB_PATH.stat().st_size
        except OSError:
            db_size = 0
    return {
        "ok": True,
        "service_version": SERVICE_VERSION,
        "schema_version": SCHEMA_VERSION,
        "storage": storage,
        "database_size_bytes": db_size,
        "runs": run_count,
        "predictions": prediction_count,
        "evaluated": evaluated_count,
        "pending": pending_count,
        "tickers": ticker_count,
        "exact_realised_closes": exact_prices,
        "calibration_profiles": profile_count,
        "approved_profiles": approved_count,
        "evaluation_coverage": round(evaluated_count / prediction_count, 4)
        if prediction_count
        else 0.0,
        "last_successful_write": _as_dict(last_run) if last_run else None,
        "out_of_sample_summary": outcome_metrics,
        "model_leaderboard": leaderboard,
        "top_calibrations": [_as_dict(row) for row in top_rows],
        "updated_at": utc_now(),
    }


def diagnostics() -> dict[str, Any]:
    init_db()
    with connect() as conn:
        missing_exact = _scalar(
            conn,
            """
            SELECT COUNT(*) AS count
            FROM predictions p
            WHERE p.evaluated=0
              AND EXISTS(
                  SELECT 1 FROM realised_prices latest
                  WHERE latest.label=p.label AND latest.price_date>=p.target_date
              )
              AND NOT EXISTS(
                  SELECT 1 FROM realised_prices exact_price
                  WHERE exact_price.label=p.label
                    AND exact_price.price_date=p.target_date
              )
            """,
        )
        recent_events = _execute(
            conn,
            """
            SELECT created_at,level,event_type,run_id,message,details_json
            FROM learning_events
            ORDER BY id DESC
            LIMIT 25
            """,
        ).fetchall()
    snapshot = stats()
    integrity = database_integrity_check()
    storage = snapshot["storage"]
    recent_errors = [
        _as_dict(row) for row in recent_events if _as_dict(row).get("level") == "error"
    ]
    status = "healthy"
    if storage.get("warning") or recent_errors or not integrity["ok"]:
        status = "degraded"
    return {
        "ok": status == "healthy",
        "status": status,
        "service_version": SERVICE_VERSION,
        "schema_version": SCHEMA_VERSION,
        "storage": storage,
        "database_integrity": integrity,
        "missing_exact_target_closes": missing_exact,
        "recent_errors": recent_errors,
        "recent_events": [_as_dict(row) for row in recent_events],
        "stats": snapshot,
        "checks": {
            "exact_target_only": True,
            "raw_and_calibrated_separated": True,
            "temporal_holdout_required": True,
            "profile_dimensions": ["ticker", "horizon", "regime", "model_version"],
            "probability_calibration": "reliability bins with Brier validation",
            "confidence_decay": True,
            "idempotent_writes": True,
            "database_integrity": integrity["ok"],
        },
    }


def calibration_detail(label: str, horizon_day: int | None = None) -> dict[str, Any]:
    init_db()
    normalised = _normalise_label(label)
    with connect() as conn:
        if horizon_day is None:
            rows = _execute(
                conn,
                """
                SELECT * FROM calibration_profiles
                WHERE label IN (?, '*')
                ORDER BY horizon_day,approved DESC,reliability DESC
                """,
                (normalised,),
            ).fetchall()
        else:
            rows = _execute(
                conn,
                """
                SELECT * FROM calibration_profiles
                WHERE label IN (?, '*') AND horizon_day=?
                ORDER BY approved DESC,reliability DESC
                """,
                (normalised, int(horizon_day)),
            ).fetchall()
    profiles = []
    for row in rows:
        profile = _as_dict(row)
        try:
            profile["probability_bins"] = json.loads(
                str(profile.pop("probability_bins_json") or "[]")
            )
        except json.JSONDecodeError:
            profile["probability_bins"] = []
        profiles.append(profile)
    return {
        "ok": True,
        "label": normalised,
        "horizon_day": horizon_day,
        "profiles": profiles,
        "updated_at": utc_now(),
    }


def reliability_curve(label: str, horizon_day: int | None = None) -> dict[str, Any]:
    detail = calibration_detail(label, horizon_day)
    curves = [
        {
            "horizon_day": profile["horizon_day"],
            "regime": profile["regime"],
            "model_version": profile["model_version"],
            "scope": profile["scope"],
            "approved": bool(profile["approved"]),
            "sample_count": profile["sample_count"],
            "raw_brier_score": profile["raw_brier_score"],
            "calibrated_brier_score": profile["calibrated_brier_score"],
            "bins": profile["probability_bins"],
        }
        for profile in detail["profiles"]
    ]
    return {
        "ok": True,
        "label": detail["label"],
        "horizon_day": horizon_day,
        "curves": curves,
        "updated_at": utc_now(),
    }


def purge_test_database() -> None:
    """Test helper. Refuses to delete a non-test SQLite database."""
    if DB_KIND != "sqlite":
        raise RuntimeError("purge_test_database is available only for SQLite tests")
    resolved = DB_PATH.resolve()
    if "test" not in resolved.name.lower() and "/tmp/" not in str(resolved):
        raise RuntimeError(f"Refusing to delete a non-test database: {resolved}")
    for suffix in ("", "-wal", "-shm"):
        try:
            Path(str(resolved) + suffix).unlink()
        except FileNotFoundError:
            pass


__all__ = [
    "DB_KIND",
    "DB_PATH",
    "SCHEMA_VERSION",
    "SERVICE_VERSION",
    "apply_calibration_to_payload",
    "calibration_detail",
    "connect",
    "diagnostics",
    "evaluate_exact_targets",
    "ingest_realised_prices",
    "init_db",
    "make_idempotency_key",
    "preview_payload",
    "process_payload",
    "record_payload",
    "recompute_calibration_profiles",
    "reliability_curve",
    "stats",
    "storage_diagnostics",
    "store_predictions",
]
