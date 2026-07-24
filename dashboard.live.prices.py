#!/usr/bin/env python3
"""Chrome finance dashboard for setup.stats.py.

Run: python tv_dashboard.py
Then open http://127.0.0.1:8844
"""
from __future__ import annotations

import argparse
import ast
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shutil
import socket
import subprocess
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import webbrowser
import learning_remote_client as learning_client
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse, parse_qs, quote, urlencode
from urllib.request import Request, urlopen

try:
    import websocket as websocket_client
except Exception:
    websocket_client = None

APP_NAME = "Apex Market Predictor v15"
DEFAULT_PORT = 8845
BASE_DIR = Path(__file__).resolve().parent
PREDICT_SCRIPT = BASE_DIR / "setup.stats.py"
JOBS_DIR = BASE_DIR / ".tv_dashboard_runs"
LOGO_PATH = BASE_DIR / "apex_tool_logo.png"

FALLBACK_WATCHLIST = [{'label': 'CAC40', 'symbol': 'PX1', 'exchange': 'INDEXEURO', 'name': 'CAC 40'}, {'label': 'VXN', 'symbol': 'VXN', 'exchange': 'INDEXCBOE', 'name': 'CBOE NASDAQ Volatility'}, {'label': 'DJI', 'symbol': '.DJI', 'exchange': 'INDEXDJX', 'name': 'Dow Jones Industrial'}, {'label': 'FTSE', 'symbol': 'UKX', 'exchange': 'INDEXFTSE', 'name': 'FTSE 100'}, {'label': 'HSI', 'symbol': 'HSI', 'exchange': 'INDEXHANGSENG', 'name': 'Hang Seng Index'}, {'label': 'IXIC', 'symbol': '.IXIC', 'exchange': 'INDEXNASDAQ', 'name': 'Nasdaq Composite'}, {'label': 'NYA', 'symbol': 'NYA', 'exchange': 'INDEXNYSEGIS', 'name': 'NYSE Composite'}, {'label': 'NYFANG', 'symbol': 'NYFANG', 'exchange': 'INDEXNYSEGIS', 'name': 'NYSE FANG+ Index'}, {'label': 'SPX', 'symbol': '.INX', 'exchange': 'INDEXSP', 'name': 'S&P 500'}, {'label': 'VIX', 'symbol': 'VIX', 'exchange': 'INDEXCBOE', 'name': 'VIX'}, {'label': '0700', 'symbol': '0700', 'exchange': 'HKG', 'name': 'Tencent'}, {'label': '2222', 'symbol': '2222', 'exchange': 'TADAWUL', 'name': 'Saudi Aramco'}, {'label': '2317', 'symbol': '2317', 'exchange': 'TPE', 'name': 'Foxconn'}, {'label': '2330', 'symbol': '2330', 'exchange': 'TPE', 'name': 'TSMC (TPE)'}, {'label': '2357', 'symbol': '2357', 'exchange': 'TPE', 'name': 'ASUS'}, {'label': 'ASML', 'symbol': 'ASML', 'exchange': 'AMS', 'name': 'ASML'}, {'label': 'BARC', 'symbol': 'BARC', 'exchange': 'LON', 'name': 'Barclays'}, {'label': 'GLE', 'symbol': 'GLE', 'exchange': 'EPA', 'name': 'Société Générale'}, {'label': 'HSBA', 'symbol': 'HSBA', 'exchange': 'LON', 'name': 'HSBC (London)'}, {'label': 'RMS', 'symbol': 'RMS', 'exchange': 'EPA', 'name': 'Hermès'}, {'label': 'SAF', 'symbol': 'SAF', 'exchange': 'EPA', 'name': 'Safran'}, {'label': 'AAPL', 'symbol': 'AAPL', 'exchange': 'NASDAQ', 'name': 'Apple'}, {'label': 'ABBV', 'symbol': 'ABBV', 'exchange': 'NYSE', 'name': 'AbbVie'}, {'label': 'AMD', 'symbol': 'AMD', 'exchange': 'NASDAQ', 'name': 'AMD'}, {'label': 'AMZN', 'symbol': 'AMZN', 'exchange': 'NASDAQ', 'name': 'Amazon'}, {'label': 'AON', 'symbol': 'AON', 'exchange': 'NYSE', 'name': 'Aon'}, {'label': 'ARM', 'symbol': 'ARM', 'exchange': 'NASDAQ', 'name': 'ARM Holdings'}, {'label': 'AVGO', 'symbol': 'AVGO', 'exchange': 'NASDAQ', 'name': 'Broadcom'}, {'label': 'AXP', 'symbol': 'AXP', 'exchange': 'NYSE', 'name': 'American Express'}, {'label': 'BAC', 'symbol': 'BAC', 'exchange': 'NYSE', 'name': 'Bank of America'}, {'label': 'BLK', 'symbol': 'BLK', 'exchange': 'NYSE', 'name': 'BlackRock'}, {'label': 'BRKA', 'symbol': 'BRK.A', 'exchange': 'NYSE', 'name': 'Berkshire Hathaway A'}, {'label': 'BRKB', 'symbol': 'BRK.B', 'exchange': 'NYSE', 'name': 'Berkshire Hathaway B'}, {'label': 'BX', 'symbol': 'BX', 'exchange': 'NYSE', 'name': 'Blackstone'}, {'label': 'C', 'symbol': 'C', 'exchange': 'NYSE', 'name': 'Citigroup'}, {'label': 'CME', 'symbol': 'CME', 'exchange': 'NASDAQ', 'name': 'CME Group'}, {'label': 'COST', 'symbol': 'COST', 'exchange': 'NASDAQ', 'name': 'Costco'}, {'label': 'GE', 'symbol': 'GE', 'exchange': 'NYSE', 'name': 'General Electric'}, {'label': 'GOOG', 'symbol': 'GOOG', 'exchange': 'NASDAQ', 'name': 'Alphabet (GOOG)'}, {'label': 'GOOGL', 'symbol': 'GOOGL', 'exchange': 'NASDAQ', 'name': 'Alphabet (GOOGL)'}, {'label': 'GS', 'symbol': 'GS', 'exchange': 'NYSE', 'name': 'Goldman Sachs'}, {'label': 'HSBC', 'symbol': 'HSBC', 'exchange': 'NYSE', 'name': 'HSBC (NYSE)'}, {'label': 'IBIT', 'symbol': 'IBIT', 'exchange': 'NASDAQ', 'name': 'iShares Bitcoin ETF'}, {'label': 'IBM', 'symbol': 'IBM', 'exchange': 'NYSE', 'name': 'IBM'}, {'label': 'INTC', 'symbol': 'INTC', 'exchange': 'NASDAQ', 'name': 'Intel'}, {'label': 'ISVAF', 'symbol': 'ISVAF', 'exchange': 'OTCMKTS', 'name': 'iShares NQ100 UCITS'}, {'label': 'JPM', 'symbol': 'JPM', 'exchange': 'NYSE', 'name': 'JPMorgan Chase'}, {'label': 'KKR', 'symbol': 'KKR', 'exchange': 'NYSE', 'name': 'KKR & Co'}, {'label': 'KO', 'symbol': 'KO', 'exchange': 'NYSE', 'name': 'Coca-Cola'}, {'label': 'LLY', 'symbol': 'LLY', 'exchange': 'NYSE', 'name': 'Eli Lilly'}, {'label': 'LMT', 'symbol': 'LMT', 'exchange': 'NYSE', 'name': 'Lockheed Martin'}, {'label': 'MA', 'symbol': 'MA', 'exchange': 'NYSE', 'name': 'Mastercard'}, {'label': 'META', 'symbol': 'META', 'exchange': 'NASDAQ', 'name': 'Meta'}, {'label': 'MPWR', 'symbol': 'MPWR', 'exchange': 'NASDAQ', 'name': 'Monolithic Power Systems Inc'}, {'label': 'MS', 'symbol': 'MS', 'exchange': 'NYSE', 'name': 'Morgan Stanley'}, {'label': 'MSFT', 'symbol': 'MSFT', 'exchange': 'NASDAQ', 'name': 'Microsoft'}, {'label': 'MU', 'symbol': 'MU', 'exchange': 'NASDAQ', 'name': 'Micron Technology'}, {'label': 'NDAQ', 'symbol': 'NDAQ', 'exchange': 'NASDAQ', 'name': 'Nasdaq Inc'}, {'label': 'NFLX', 'symbol': 'NFLX', 'exchange': 'NASDAQ', 'name': 'Netflix'}, {'label': 'NVDA', 'symbol': 'NVDA', 'exchange': 'NASDAQ', 'name': 'Nvidia'}, {'label': 'ORCL', 'symbol': 'ORCL', 'exchange': 'NYSE', 'name': 'Oracle'}, {'label': 'PFE', 'symbol': 'PFE', 'exchange': 'NYSE', 'name': 'Pfizer'}, {'label': 'PLTR', 'symbol': 'PLTR', 'exchange': 'NYSE', 'name': 'Palantir'}, {'label': 'QCOM', 'symbol': 'QCOM', 'exchange': 'NASDAQ', 'name': 'Qualcomm'}, {'label': 'RTX', 'symbol': 'RTX', 'exchange': 'NYSE', 'name': 'RTX Corporation'}, {'label': 'RY', 'symbol': 'RY', 'exchange': 'NYSE', 'name': 'Royal Bank of Canada'}, {'label': 'SMCI', 'symbol': 'SMCI', 'exchange': 'NASDAQ', 'name': 'Supermicro'}, {'label': 'SONY', 'symbol': 'SONY', 'exchange': 'NYSE', 'name': 'Sony'}, {'label': 'SPOT', 'symbol': 'SPOT', 'exchange': 'NYSE', 'name': 'Spotify'}, {'label': 'SPY', 'symbol': 'SPY', 'exchange': 'NYSEARCA', 'name': 'SPDR S&P 500 ETF'}, {'label': 'T', 'symbol': 'T', 'exchange': 'NYSE', 'name': 'AT&T'}, {'label': 'TSLA', 'symbol': 'TSLA', 'exchange': 'NASDAQ', 'name': 'Tesla'}, {'label': 'TSM', 'symbol': 'TSM', 'exchange': 'NYSE', 'name': 'TSMC (ADR)'}, {'label': 'TTWO', 'symbol': 'TTWO', 'exchange': 'NASDAQ', 'name': 'Take-Two Interactive'}, {'label': 'UAL', 'symbol': 'UAL', 'exchange': 'NASDAQ', 'name': 'United Airlines'}, {'label': 'UBS', 'symbol': 'UBS', 'exchange': 'NYSE', 'name': 'UBS'}, {'label': 'UNH', 'symbol': 'UNH', 'exchange': 'NYSE', 'name': 'UnitedHealth'}, {'label': 'V', 'symbol': 'V', 'exchange': 'NYSE', 'name': 'Visa'}, {'label': 'WFC', 'symbol': 'WFC', 'exchange': 'NYSE', 'name': 'Wells Fargo'}, {'label': 'WMT', 'symbol': 'WMT', 'exchange': 'NYSE', 'name': 'Walmart'}, {'label': 'IUVL', 'symbol': 'IUVL', 'exchange': 'LON', 'name': 'iShares Edge MSCI USA Value Factor UCITS ETF USD A'}, {'label': 'VGT', 'symbol': 'VGT', 'exchange': 'NYSEARCA', 'name': 'Vanguard Information Technology ETF'}, {'label': 'GSOX', 'symbol': 'GSOX', 'exchange': 'NASDAQ', 'name': 'Nasdaq Global Semiconductor Index'}, {'label': 'GSOXNR', 'symbol': 'GSOXNR', 'exchange': 'NASDAQ', 'name': 'Nasdaq Global Semiconductor Net Total Return Index'}, {'label': 'GSOXTR', 'symbol': 'GSOXTR', 'exchange': 'NASDAQ', 'name': 'Nasdaq Global Semiconductor Total Return Index'}, {'label': 'SEMIEW5T', 'symbol': 'SEMIEW5T', 'exchange': 'ICE', 'name': 'NYSE Semiconductor Top 5 Equal Weight Index TR'}, {'label': 'SMH', 'symbol': 'SMH', 'exchange': 'NASDAQ', 'name': 'VanEck Semiconductor ETF'}, {'label': 'SMH_EPA', 'symbol': 'SMH', 'exchange': 'EPA', 'name': 'VanEck Semiconductor UCITS ETF USD A'}]
FALLBACK_WATCHLIST.append({'label': 'BTCUSD', 'symbol': 'BTCUSD', 'exchange': 'COINBASE', 'name': 'Bitcoin / U.S. Dollar'})
FALLBACK_WATCHLIST.append({'label': 'SPCX', 'symbol': 'SPCX', 'exchange': 'NASDAQ', 'name': 'Space Exploration Technologies Corp'})
FALLBACK_WATCHLIST.append({'label': 'LVMH', 'symbol': 'MC', 'exchange': 'EPA', 'name': 'LVMH'})
FALLBACK_WATCHLIST.extend([
    {'label': '005930', 'symbol': '005930', 'exchange': 'KRX', 'name': 'Samsung Electronics Co., Ltd.'},
    {'label': 'ASML_NYC', 'symbol': 'ASML', 'exchange': 'NASDAQ', 'name': 'ASML Holding N.V. - New York Registry Shares'},
    {'label': 'AMAT', 'symbol': 'AMAT', 'exchange': 'NASDAQ', 'name': 'Applied Materials, Inc.'},
    {'label': 'APH', 'symbol': 'APH', 'exchange': 'NYSE', 'name': 'Amphenol Corporation'},
    {'label': 'CAT', 'symbol': 'CAT', 'exchange': 'NYSE', 'name': 'Caterpillar, Inc.'},
    {'label': 'CSCO', 'symbol': 'CSCO', 'exchange': 'NASDAQ', 'name': 'Cisco Systems, Inc.'},
    {'label': 'DELL', 'symbol': 'DELL', 'exchange': 'NYSE', 'name': 'Dell Technologies Inc.'},
    {'label': 'GEV', 'symbol': 'GEV', 'exchange': 'NYSE', 'name': 'General Electric Vernova'},
    {'label': 'LRCX', 'symbol': 'LRCX', 'exchange': 'NASDAQ', 'name': 'Lam Research Corporation'},
    {'label': 'PANW', 'symbol': 'PANW', 'exchange': 'NASDAQ', 'name': 'Palo Alto Networks, Inc.'},
    {'label': 'SNDK', 'symbol': 'SNDK', 'exchange': 'NASDAQ', 'name': 'Sandisk Corporation'},
    {'label': 'STX', 'symbol': 'STX', 'exchange': 'NASDAQ', 'name': 'Seagate Technology Holdings PLC'},
    {'label': 'TD', 'symbol': 'TD', 'exchange': 'NYSE', 'name': 'Toronto Dominion Bank (The)'},
    {'label': 'TXN', 'symbol': 'TXN', 'exchange': 'NASDAQ', 'name': 'Texas Instruments Incorporated'},
    {'label': 'WDC', 'symbol': 'WDC', 'exchange': 'NASDAQ', 'name': 'Western Digital Corporation'},
])


def _maybe_reexec_from_local_venv() -> None:
    if os.environ.get("TVDASH_NO_VENV_REEXEC") == "1":
        return
    if getattr(sys, "base_prefix", sys.prefix) != sys.prefix:
        return
    for py in [BASE_DIR / "venv" / "bin" / "python", BASE_DIR / ".venv" / "bin" / "python"]:
        if py.exists() and py.resolve() != Path(sys.executable).resolve():
            os.environ["TVDASH_NO_VENV_REEXEC"] = "1"
            os.execv(str(py), [str(py), *sys.argv])


_maybe_reexec_from_local_venv()


@dataclass
class Job:
    id: str
    command: list[str]
    result_path: Path
    progress_path: Path
    created_at: float = field(default_factory=time.time)
    started_at: float | None = None
    ended_at: float | None = None
    returncode: int | None = None
    process: subprocess.Popen[str] | None = None
    error: str | None = None

    @property
    def running(self) -> bool:
        return self.process is not None and self.process.poll() is None


JOBS: dict[str, Job] = {}
LOCK = threading.Lock()
AUTO_LOCK = threading.Lock()
AUTO_RUNNING = False
AUTO_STARTED_AT: float | None = None
LEARNING_RUNTIME_LOCK = threading.Lock()
LEARNING_RUNTIME_STATUS: dict[str, Any] = {
    "last_success_at": None,
    "last_failure_at": None,
    "last_error": None,
    "last_source": None,
    "successful_requests": 0,
    "failed_requests": 0,
}
# Remote-only learning: no local ML database is initialised here.
# v14.6: chart-only full-history cache. This is in RAM only, so it does not slow
# the 88-instrument forecast run and does not create a local ML store.
FULL_HISTORY_CACHE: dict[str, dict[str, Any]] = {}
FULL_HISTORY_LOCK = threading.Lock()
FULL_HISTORY_TTL_SEC = int(os.environ.get("APEX_FULL_HISTORY_TTL_SEC", "900"))
FULL_HISTORY_BARS = int(os.environ.get("APEX_FULL_CHART_BARS", "20000"))
FULL_HISTORY_TIMEOUT = float(os.environ.get("APEX_FULL_HISTORY_TIMEOUT", "35"))
FULL_HISTORY_YAHOO_TIMEOUT = float(os.environ.get("APEX_FULL_HISTORY_YAHOO_TIMEOUT", "8"))
FULL_HISTORY_CACHE_MAX_ENTRIES = int(os.environ.get("APEX_FULL_HISTORY_CACHE_MAX_ENTRIES", "8"))
LIVE_PRICE_TIMEOUT = float(os.environ.get("APEX_LIVE_PRICE_TIMEOUT", "0.95"))
LIVE_PRICE_TTL_SEC = float(os.environ.get("APEX_LIVE_PRICE_TTL_SEC", "0.22"))
LIVE_PRICE_CHUNK = int(os.environ.get("APEX_LIVE_PRICE_CHUNK", "70"))
LIVE_DIRECT_MAX_SYMBOLS = int(os.environ.get("APEX_LIVE_DIRECT_MAX_SYMBOLS", "36"))
LIVE_PRICE_LOCK = threading.Lock()
LIVE_PRICE_CACHE: dict[str, Any] = {"key": None, "ts": 0.0, "payload": None}
LAST_GOOD_PRICE_CACHE: dict[str, dict[str, Any]] = {}
LAST_GOOD_PRICE_MAX_AGE_SEC = float(os.environ.get("APEX_LAST_GOOD_PRICE_MAX_AGE_SEC", str(14 * 86400)))
LIVE_STREAM_STALE_MS = int(os.environ.get("APEX_LIVE_STREAM_STALE_MS", "180000"))
LIVE_STREAM_CONNECT_TIMEOUT = float(os.environ.get("APEX_LIVE_STREAM_CONNECT_TIMEOUT", "8"))
LIVE_STREAM_RECV_TIMEOUT = float(os.environ.get("APEX_LIVE_STREAM_RECV_TIMEOUT", "1"))
LIVE_STREAM_RECONNECT_MAX = float(os.environ.get("APEX_LIVE_STREAM_RECONNECT_MAX", "20"))
PROVIDER_AUTH_PROBE_TIMEOUT = float(os.environ.get("APEX_PROVIDER_AUTH_PROBE_TIMEOUT", "4"))
PROVIDER_AUTH_PROBE_TTL_SEC = float(os.environ.get("APEX_PROVIDER_AUTH_PROBE_TTL_SEC", "120"))
LIVE_FINNHUB_REST_MAX_SYMBOLS = int(os.environ.get("APEX_FINNHUB_REST_MAX_SYMBOLS", "1"))
INTRADAY_LOCK = threading.Lock()
INTRADAY_CACHE: dict[str, dict[str, Any]] = {}
INTRADAY_TTL_SEC = float(os.environ.get("APEX_INTRADAY_TTL_SEC", "8"))
INTRADAY_LONG_TTL_SEC = float(os.environ.get("APEX_INTRADAY_LONG_TTL_SEC", "240"))
INTRADAY_ERROR_TTL_SEC = float(os.environ.get("APEX_INTRADAY_ERROR_TTL_SEC", "60"))
INTRADAY_TIMEOUT = float(os.environ.get("APEX_INTRADAY_TIMEOUT", "2.2"))
INTRADAY_MAX_BARS = int(os.environ.get("APEX_INTRADAY_MAX_BARS", "12000"))
INTRADAY_CACHE_MAX_ENTRIES = int(os.environ.get("APEX_INTRADAY_CACHE_MAX_ENTRIES", "24"))
INTRADAY_ARCHIVE_LOCK = threading.Lock()
INTRADAY_ARCHIVE_CACHE: dict[str, dict[str, Any]] = {}
INTRADAY_ARCHIVE_TTL_SEC = float(os.environ.get("APEX_INTRADAY_ARCHIVE_TTL_SEC", "900"))
INTRADAY_ARCHIVE_ERROR_TTL_SEC = float(os.environ.get("APEX_INTRADAY_ARCHIVE_ERROR_TTL_SEC", "180"))
INTRADAY_ARCHIVE_CACHE_MAX_ENTRIES = int(os.environ.get("APEX_INTRADAY_ARCHIVE_CACHE_MAX_ENTRIES", "32"))
INTRADAY_ARCHIVE_MAX_WINDOW_DAYS = 5
INTRADAY_ARCHIVE_MAX_BARS_PER_PAGE = int(os.environ.get("APEX_INTRADAY_ARCHIVE_MAX_BARS_PER_PAGE", "8000"))
INTRADAY_ARCHIVE_LOOKBACK_DAYS = 370
INTRADAY_TV_LOCK = threading.Lock()
INTRADAY_TV_CLIENT: Any = None
POSITION_ADVICE_LOCK = threading.Lock()
POSITION_ADVICE_CACHE: dict[str, dict[str, Any]] = {}
POSITION_ADVICE_TTL_SEC = float(os.environ.get("APEX_POSITION_ADVICE_TTL_SEC", "1.25"))
POSITION_ENGINE_CONTEXT_CACHE: dict[str, dict[str, Any]] = {}
POSITION_ENGINE_CONTEXT_TTL_SEC = float(os.environ.get("APEX_POSITION_ENGINE_CONTEXT_TTL_SEC", "12"))
POSITION_INTRADAY_LOCK = threading.Lock()
POSITION_INTRADAY_PENDING: dict[str, dict[str, Any]] = {}
POSITION_INTRADAY_LAST_GOOD: dict[str, dict[str, Any]] = {}
POSITION_INTRADAY_BUDGET_SEC = float(os.environ.get("APEX_POSITION_INTRADAY_BUDGET_SEC", "3.8"))
POSITION_LONG_LOCK = threading.Lock()
POSITION_LONG_HISTORY_CACHE: dict[str, dict[str, Any]] = {}
POSITION_LONG_PENDING: dict[str, dict[str, Any]] = {}
POSITION_LONG_HISTORY_TTL_SEC = float(os.environ.get("APEX_POSITION_LONG_HISTORY_TTL_SEC", "900"))
POSITION_LONG_BUDGET_SEC = float(os.environ.get("APEX_POSITION_LONG_BUDGET_SEC", "3.8"))
POSITION_LONG_MAX_BARS = int(os.environ.get("APEX_POSITION_LONG_MAX_BARS", "30000"))
POSITION_LONG_MAX_PAGES = max(4, min(64, int(os.environ.get("APEX_POSITION_LONG_MAX_PAGES", "16"))))
POSITION_LONG_START = datetime(2016, 1, 1, tzinfo=timezone.utc)
CHART_SERIES_SPECS = {
    ("1m", "5d"),
    ("1m", "30d"),
    ("5m", "1mo"),
    ("15m", "3mo"),
    ("1h", "3mo"),
    ("1h", "6mo"),
    ("1h", "1y"),
}


def _chunks(seq: list[str], n: int) -> list[list[str]]:
    return [seq[i:i+n] for i in range(0, len(seq), n)]


def _learning_success(source: str, payload: dict[str, Any] | None = None) -> None:
    with LEARNING_RUNTIME_LOCK:
        LEARNING_RUNTIME_STATUS.update({
            "last_success_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
            "last_error": None,
            "last_source": source,
            "successful_requests": int(LEARNING_RUNTIME_STATUS["successful_requests"] or 0) + 1,
            "last_request_id": (payload or {}).get("request_id"),
            "write_queued": bool(((payload or {}).get("remote_learning") or {}).get("write_queued")),
        })


def _learning_failure(source: str, exc: Exception) -> None:
    message = str(exc)
    with LEARNING_RUNTIME_LOCK:
        LEARNING_RUNTIME_STATUS.update({
            "last_failure_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
            "last_error": message,
            "last_source": source,
            "failed_requests": int(LEARNING_RUNTIME_STATUS["failed_requests"] or 0) + 1,
        })
    print(f"[learning-v15] {source} failed: {message}", file=sys.stderr, flush=True)


def learning_status_payload(force: bool = False) -> dict[str, Any]:
    remote = learning_client.stats(force=force)
    with LEARNING_RUNTIME_LOCK:
        runtime = dict(LEARNING_RUNTIME_STATUS)
    remote["dashboard_runtime"] = runtime
    remote["client_status"] = learning_client.last_status()
    return remote


def start_silent_auto_runs() -> bool:
    """Start low-load durable learning runs when the Chrome page opens.

    The visible UI stays quiet, but each compact batch is evaluated against
    previous predictions and stored in the server-side learning database. This
    is what makes the bot improve over time on a VPS or dedicated server.
    """
    global AUTO_RUNNING, AUTO_STARTED_AT
    if str(os.environ.get("APEX_DISABLE_SILENT_LEARNING", "")).strip().lower() in {"1", "true", "yes", "on"}:
        return False
    # No local ML storage is allowed. If the remote learning server is not
    # configured, silent learning runs are skipped instead of writing locally.
    if not learning_client.configured():
        return False
    with AUTO_LOCK:
        if AUTO_RUNNING:
            return False
        AUTO_RUNNING = True
        AUTO_STARTED_AT = time.time()

    def loop() -> None:
        global AUTO_RUNNING
        try:
            while True:
                labels = [x["label"] for x in load_watchlist()]
                # Never compete with a visible user run. The visible dashboard analysis
                # has priority; silent learning resumes only when it is idle.
                if any(j.running for j in JOBS.values()):
                    time.sleep(30)
                    continue
                for batch in _chunks(labels, 4):
                    if not batch:
                        continue
                    tmp_dir = JOBS_DIR / ("_silent_auto_" + uuid.uuid4().hex[:8])
                    try:
                        tmp_dir.mkdir(parents=True, exist_ok=True)
                        result_path = tmp_dir / "auto.json"
                        cmd = [
                            sys.executable, str(PREDICT_SCRIPT),
                            "--mode", "fast",
                            "--days", "10",
                            "--bars", "900",
                            "--news-limit", "1",
                            "--json-out", str(result_path),
                            "--quiet",
                            "--no-store-learning",
                            "--only", *batch,
                        ]
                        cp = subprocess.run(cmd, cwd=str(BASE_DIR), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=360)
                        if cp.returncode == 0 and result_path.exists():
                            raw = json.loads(result_path.read_text(encoding="utf-8"))
                            learned = learning_client.process_payload(raw, source="silent_auto_run", mode="fast", store=True)
                            _learning_success("silent_auto_run", learned)
                    except Exception as exc:
                        _learning_failure("silent_auto_run", exc)
                    finally:
                        try:
                            shutil.rmtree(tmp_dir, ignore_errors=True)
                        except Exception:
                            pass
                    time.sleep(45)
                # Full-watchlist silent pass complete. Wait before the next pass to avoid heavy CPU/network usage.
                time.sleep(35 * 60)
        finally:
            with AUTO_LOCK:
                AUTO_RUNNING = False

    threading.Thread(target=loop, daemon=True).start()
    return True


def load_watchlist() -> list[dict[str, str]]:
    """Return the complete canonical 88-instrument universe.

    The dashboard no longer trusts only AST parsing. If the user accidentally
    keeps an older tv_predict2.py or a partial upload, the missing canonical
    instruments are merged back so the UI and Run analysis stay at 88/88.
    """
    parsed: list[dict[str, str]] = []
    try:
        src = PREDICT_SCRIPT.read_text(encoding="utf-8")
        tree = ast.parse(src)
        for node in tree.body:
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name) and target.id == "WATCHLIST":
                        wl = ast.literal_eval(node.value)
                        parsed = [{"label": a, "symbol": b, "exchange": c, "name": d} for a, b, c, d in wl]
                        break
    except Exception:
        parsed = []
    merged: dict[str, dict[str, str]] = {}
    # Fallback guarantees 88 instruments if parsing fails. Parsed tv_predict2.py rows
    # are applied after fallback so corrected exchanges such as WMT/NASDAQ or
    # PLTR/NASDAQ override stale dashboard fallback metadata.
    for row in FALLBACK_WATCHLIST:
        lab = str(row.get("label", "")).upper()
        if lab:
            merged[lab] = {"label": lab, "symbol": str(row.get("symbol", lab)), "exchange": str(row.get("exchange", "")), "name": str(row.get("name", lab))}
    for row in parsed:
        lab = str(row.get("label", "")).upper()
        if lab:
            merged[lab] = {"label": lab, "symbol": str(row.get("symbol", lab)), "exchange": str(row.get("exchange", "")), "name": str(row.get("name", lab))}
    # Google-Finance-style tracker order: symbol/label ascending A-Z, stable for all 88.
    return sorted(merged.values(), key=lambda x: (str(x.get("label", "")).upper(), str(x.get("name", "")).upper()))


TV_SCANNER_COLUMNS = [
    "name",
    "description",
    "close",
    "change",
    "change_abs",
    "currency",
    "update_mode",
    "type",
    "subtype",
    "exchange",
]

TV_DOMAIN_BY_EXCHANGE = {
    "KRX": "korea",
    "NASDAQ": "america",
    "NYSE": "america",
    "AMEX": "america",
    "NYSEARCA": "america",
    "OTC": "america",
    "OTCMKTS": "america",
    "SP": "america",
    "DJ": "america",
    "CBOE": "america",
    "INDEXSP": "america",
    "INDEXNASDAQ": "america",
    "INDEXDJX": "america",
    "INDEXCBOE": "america",
    "INDEXNYSEGIS": "america",
    "LON": "uk",
    "LSE": "uk",
    "FTSE": "uk",
    "INDEXFTSE": "uk",
    "EPA": "france",
    "EURONEXT": "france",
    "INDEXEURO": "france",
    "AMS": "netherlands",
    "HKG": "hongkong",
    "HKEX": "hongkong",
    "HSI": "hongkong",
    "INDEXHANGSENG": "hongkong",
    "TPE": "taiwan",
    "TWSE": "taiwan",
    "TADAWUL": "saudiarabia",
    "COINBASE": "crypto",
    "BITSTAMP": "crypto",
    "ICE": "america",
    "TVC": "america",
}

TV_EXCHANGE_ALIASES = {
    "INDEXSP": ["SP", "TVC"],
    "INDEXNASDAQ": ["NASDAQ", "TVC"],
    "INDEXDJX": ["DJ", "TVC"],
    "INDEXCBOE": ["CBOE", "TVC"],
    "INDEXEURO": ["EURONEXT", "TVC"],
    "INDEXFTSE": ["FTSE", "TVC"],
    "INDEXHANGSENG": ["HSI", "TVC"],
    "INDEXNYSEGIS": ["NYSE", "TVC"],
    "OTCMKTS": ["OTC"],
}

TV_LIVE_SYMBOL_ALIASES = {
    "BTCUSD": ["COINBASE:BTCUSD", "BITSTAMP:BTCUSD"],
    "CAC40": ["EURONEXT:PX1", "TVC:CAC40", "INDEXEURO:PX1"],
    "DJI": ["DJ:DJI", "TVC:DJI", "INDEXDJX:.DJI"],
    "FTSE": ["FTSE:UKX", "TVC:UKX", "INDEXFTSE:UKX"],
    "GSOX": ["NASDAQ:GSOX"],
    "GSOXNR": ["NASDAQ:GSOXNR"],
    "GSOXTR": ["NASDAQ:GSOXTR"],
    "HSI": ["HSI:HSI", "TVC:HSI", "INDEXHANGSENG:HSI"],
    "IXIC": ["NASDAQ:IXIC", "TVC:IXIC", "INDEXNASDAQ:.IXIC"],
    "NYA": ["NYSE:NYA", "INDEXNYSEGIS:NYA"],
    "NYFANG": ["NYSE:NYFANG", "INDEXNYSEGIS:NYFANG"],
    "SEMIEW5T": ["ICE:SEMIEW5T"],
    "SPX": ["SP:SPX", "TVC:SPX", "INDEXSP:.INX"],
    "VIX": ["CBOE:VIX", "TVC:VIX", "INDEXCBOE:VIX"],
    "VXN": ["CBOE:VXN", "INDEXCBOE:VXN"],
}

YAHOO_LIVE_SYMBOL_ALIASES = {
    "BTCUSD": ["BTC-USD"],
    "CAC40": ["^FCHI"],
    "DJI": ["^DJI"],
    "FTSE": ["^FTSE"],
    "HSI": ["^HSI"],
    "IXIC": ["^IXIC"],
    "NYA": ["^NYA"],
    "SPX": ["^GSPC"],
    "VIX": ["^VIX"],
    "VXN": ["^VXN"],
}

YAHOO_SUFFIX_BY_EXCHANGE = {
    "KRX": ".KS",
    "EPA": ".PA",
    "EURONEXT": ".PA",
    "AMS": ".AS",
    "LON": ".L",
    "LSE": ".L",
    "HKG": ".HK",
    "HKEX": ".HK",
    "TPE": ".TW",
    "TWSE": ".TW",
    "TADAWUL": ".SR",
}

US_EXCHANGES = {"NASDAQ", "NYSE", "AMEX", "NYSEARCA", "OTC", "OTCMKTS"}


def _tv_clean_token(value: Any) -> str:
    return str(value or "").strip().upper()


def _tv_pair(exchange: Any, symbol: Any) -> str:
    ex = _tv_clean_token(exchange)
    sym = _tv_clean_token(symbol)
    if not ex or not sym:
        return ""
    return f"{ex}:{sym}"


def _tv_candidate_pairs(item: dict[str, Any], watch_by_label: dict[str, dict[str, str]]) -> list[str]:
    lab = _tv_clean_token(item.get("label") or item.get("ticker"))
    watch = watch_by_label.get(lab, {})
    pairs: list[str] = []

    def add_pair(pair: str) -> None:
        pair = pair.strip().upper()
        if ":" in pair and pair not in pairs:
            pairs.append(pair)

    for pair in TV_LIVE_SYMBOL_ALIASES.get(lab, []):
        add_pair(pair)

    raw_pairs = [
        (item.get("tv_exchange"), item.get("tv_symbol")),
        (item.get("original_exchange"), item.get("original_symbol")),
        (item.get("exchange"), item.get("symbol")),
        (watch.get("exchange"), watch.get("symbol")),
    ]
    for exchange, symbol in raw_pairs:
        ex = _tv_clean_token(exchange)
        sym = _tv_clean_token(symbol)
        if not sym:
            continue
        exchanges = [ex] + TV_EXCHANGE_ALIASES.get(ex, [])
        for ex2 in exchanges:
            add_pair(_tv_pair(ex2, sym))
            if sym.startswith("."):
                add_pair(_tv_pair(ex2, sym[1:]))
            if lab and lab != sym:
                add_pair(_tv_pair(ex2, lab))
    return pairs[:8]


def _tv_domain_for_pair(pair: str) -> str:
    exchange = _tv_clean_token(str(pair).split(":", 1)[0])
    return TV_DOMAIN_BY_EXCHANGE.get(exchange, "global")


def _num_or_none(value: Any) -> float | None:
    try:
        if isinstance(value, str):
            value = value.replace("$", "").replace(",", "").replace("%", "").strip()
        n = float(value)
    except Exception:
        return None
    if n == n and abs(n) != float("inf"):
        return n
    return None


def _label_from_symbol_map(symbol_map: dict[str, str], symbol: Any) -> str:
    return symbol_map.get(str(symbol or "").upper(), "")


def _change_pct_from_abs(price: float | None, change_abs: float | None) -> float | None:
    if price is None or change_abs is None:
        return None
    prev = price - change_abs
    if prev <= 0:
        return None
    return (price / prev - 1.0) * 100.0


def _is_delayed_update_mode(update_mode: str) -> bool:
    mode = str(update_mode or "").lower()
    return "delay" in mode or "endofday" in mode or mode in {"eod", "end_of_day"}


def _quote_score(q: dict[str, Any], now_ms: int) -> float:
    price = _num_or_none(q.get("price"))
    if price is None or price <= 0:
        return -1_000_000.0
    delayed = bool(q.get("delayed"))
    source = str(q.get("provider") or q.get("source") or "")
    age_ms = q.get("age_ms")
    try:
        age = float(age_ms)
    except Exception:
        ts = _num_or_none(q.get("updated_at"))
        age = max(0.0, now_ms - ts) if ts else 120_000.0
    score = 1000.0
    if bool(q.get("streaming")):
        score += 620.0
    if delayed:
        score -= 340.0
    score -= min(320.0, age / 1000.0 * 2.4)
    if "Alpaca" in source:
        score += 175.0
    elif "Finnhub" in source:
        score += 145.0
    elif "Yahoo" in source:
        score += 90.0
    elif "Nasdaq" in source:
        score += 35.0
    elif "TradingView" in source:
        score += 20.0
    if bool(q.get("market_open")):
        score += 18.0
    if q.get("real_time_hint"):
        score += 70.0
    try:
        score -= float(q.get("_rank", 0)) * 4.0
    except Exception:
        pass
    return score


def _merge_quote(prices: dict[str, dict[str, Any]], label: str, quote_payload: dict[str, Any], now_ms: int) -> None:
    lab = _tv_clean_token(label)
    if not lab:
        return
    quote_payload["label"] = lab
    quote_payload.setdefault("updated_at", now_ms)
    existing = prices.get(lab)
    if existing is None or _quote_score(quote_payload, now_ms) > _quote_score(existing, now_ms):
        prices[lab] = quote_payload


STREAM_INDEX_LABELS = {
    "CAC40", "DJI", "FTSE", "GSOX", "GSOXNR", "GSOXTR", "HSI", "IXIC",
    "NYA", "NYFANG", "SEMIEW5T", "SPX", "VIX", "VXN",
}


def _stream_timestamp_ms(value: Any, default: int | None = None) -> int:
    """Normalize provider seconds/ms/us/ns or RFC3339 timestamps to Unix ms."""
    fallback = int(default if default is not None else time.time() * 1000)
    if value in (None, ""):
        return fallback
    if isinstance(value, str):
        raw = value.strip()
        try:
            value = float(raw)
        except Exception:
            try:
                return int(datetime.fromisoformat(raw.replace("Z", "+00:00")).timestamp() * 1000)
            except Exception:
                return fallback
    try:
        ts = float(value)
    except Exception:
        return fallback
    magnitude = abs(ts)
    if magnitude >= 1e17:
        ts /= 1_000_000.0
    elif magnitude >= 1e14:
        ts /= 1_000.0
    elif magnitude < 1e11:
        ts *= 1_000.0
    return int(ts)


def _normalize_api_credential(value: Any, env_names: tuple[str, ...], *, bearer: bool = False) -> str:
    raw = str(value or "").replace("\ufeff", "").replace("\u200b", "").strip()
    while len(raw) >= 2 and raw[0] == raw[-1] and raw[0] in {"'", '"'}:
        raw = raw[1:-1].strip()
    upper = raw.upper()
    for name in env_names:
        for prefix in (f"{name}=", f"EXPORT {name}="):
            if upper.startswith(prefix):
                raw = raw[len(prefix):].strip()
                while len(raw) >= 2 and raw[0] == raw[-1] and raw[0] in {"'", '"'}:
                    raw = raw[1:-1].strip()
                upper = raw.upper()
                break
    if bearer and raw.lower().startswith("bearer "):
        raw = raw[7:].strip()
    if bearer and raw.lower().startswith("token="):
        raw = raw[6:].strip()
    return re.sub(r"\s+", "", raw)


def _read_api_credential(*env_names: str, bearer: bool = False) -> tuple[str, str]:
    names = tuple(env_names)
    for name in names:
        value = os.environ.get(name)
        if value not in (None, ""):
            return name, _normalize_api_credential(value, names, bearer=bearer)
    return "", ""


def _credential_issue(value: str, label: str) -> str | None:
    if not value:
        return f"missing {label}"
    lower = value.lower()
    placeholders = ("ta_cle", "ton_secret", "your_key", "your_secret", "api_key_here", "replace_me", "<", ">", "***")
    if any(token in lower for token in placeholders):
        return f"{label} still contains a placeholder"
    if len(value) < 12:
        return f"{label} is unusually short"
    return None


def _credential_fingerprint(value: str) -> str | None:
    if not value:
        return None
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:10]


def _http_error_detail(exc: Any) -> str:
    code = getattr(exc, "code", None)
    body = ""
    try:
        body = exc.read().decode("utf-8", "replace")
    except Exception:
        body = str(exc or "")
    body = re.sub(r"[\r\n]+", " ", body).strip()
    return f"HTTP {code}: {body[:150]}" if code else body[:180]


def _quote_plausible_for_item(quote_payload: dict[str, Any], item: dict[str, Any]) -> bool:
    price = _num_or_none(quote_payload.get("price"))
    if price is None or price <= 0:
        return False
    reference = _num_or_none(item.get("last")) or _num_or_none(item.get("previous_close"))
    if reference is None or reference <= 0:
        return True
    label = _tv_clean_token(item.get("label") or item.get("ticker"))
    ratio = price / reference
    if label == "BTCUSD":
        return 0.35 <= ratio <= 2.85
    if label in STREAM_INDEX_LABELS:
        return 0.65 <= ratio <= 1.45
    return 0.45 <= ratio <= 2.20


def _remember_last_good_quote(label: str, quote_payload: dict[str, Any]) -> None:
    label = _tv_clean_token(label)
    if not label or _num_or_none(quote_payload.get("price")) is None:
        return
    now_ms = int(time.time() * 1000)
    candidate = dict(quote_payload)
    candidate["_cached_at_ms"] = now_ms
    with LIVE_PRICE_LOCK:
        existing = LAST_GOOD_PRICE_CACHE.get(label)
        candidate_ts = int(_num_or_none(candidate.get("updated_at")) or now_ms)
        existing_ts = int(_num_or_none((existing or {}).get("updated_at")) or 0)
        if existing is None or candidate_ts >= existing_ts - 1_000:
            LAST_GOOD_PRICE_CACHE[label] = candidate


def _last_good_quote(label: str, item: dict[str, Any]) -> dict[str, Any] | None:
    now_ms = int(time.time() * 1000)
    with LIVE_PRICE_LOCK:
        cached = dict(LAST_GOOD_PRICE_CACHE.get(_tv_clean_token(label)) or {})
    cached_at = int(_num_or_none(cached.get("_cached_at_ms")) or 0)
    if not cached or now_ms - cached_at > LAST_GOOD_PRICE_MAX_AGE_SEC * 1000:
        return None
    if not _quote_plausible_for_item(cached, item):
        return None
    cached.pop("_cached_at_ms", None)
    cached.update({
        "source": f"{cached.get('source') or cached.get('provider') or 'Market source'} · cached last good",
        "provider": f"{cached.get('provider') or 'Market source'} cached",
        "provider_id": "cache",
        "update_mode": "cached last good quote",
        "streaming": False,
        "real_time_hint": False,
        "delayed": True,
        "market_open": False,
        "age_ms": max(0, now_ms - int(_num_or_none(cached.get("updated_at")) or cached_at)),
        "server_received_at": now_ms,
    })
    cached["delay_minutes"] = round(float(cached["age_ms"]) / 60000.0, 2)
    return cached


def _ensure_price_fallbacks(payload: dict[str, Any], items: list[dict[str, Any]]) -> dict[str, Any]:
    """Guarantee a truthful last-known value whenever the run supplied one."""
    out = dict(payload)
    prices = {str(label): dict(row) for label, row in (payload.get("prices") or {}).items()}
    errors = dict(payload.get("errors") or {})
    now_ms = int(time.time() * 1000)
    fallback_count = 0
    for item in items:
        label = _tv_clean_token(item.get("label") or item.get("ticker"))
        if not label:
            continue
        current = prices.get(label)
        if current is not None and _quote_plausible_for_item(current, item):
            _remember_last_good_quote(label, current)
            continue
        if current is not None:
            errors[f"validation:{label}"] = "implausible quote rejected"
            prices.pop(label, None)
        fallback = _last_good_quote(label, item)
        if fallback is None:
            last = _num_or_none(item.get("last")) or _num_or_none(item.get("previous_close"))
            if last is None or last <= 0:
                continue
            previous = _num_or_none(item.get("previous_close"))
            change_abs = last - previous if previous and previous > 0 else None
            change_pct = (last / previous - 1.0) * 100.0 if previous and previous > 0 else None
            raw_timestamp = item.get("last_updated_at") or item.get("last_date")
            quote_ts = _stream_timestamp_ms(raw_timestamp, now_ms)
            age_ms = max(0, now_ms - quote_ts)
            fallback = {
                "label": label,
                "price": last,
                "change_pct": change_pct,
                "change_abs": change_abs,
                "currency": item.get("currency"),
                "exchange": item.get("exchange") or item.get("original_exchange"),
                "source_symbol": item.get("symbol") or item.get("original_symbol") or label,
                "source": "Last validated analysis close",
                "provider": "Historical fallback",
                "provider_id": "history",
                "feed": "last-close",
                "update_mode": "last validated close",
                "market_open": False,
                "delayed": True,
                "delay_minutes": round(age_ms / 60000.0, 2),
                "age_ms": age_ms,
                "transport_latency_ms": 0,
                "server_received_at": now_ms,
                "updated_at": quote_ts,
                "real_time_hint": False,
                "streaming": False,
            }
        prices[label] = fallback
        fallback_count += 1
    out["prices"] = prices
    out["errors"] = errors
    out["fallback_count"] = fallback_count
    return out


class LiveStreamHub:
    """Server-side market WebSocket fan-in with an in-memory latest-tick cache."""

    PROVIDERS = ("massive", "alpaca", "finnhub", "twelvedata", "coinbase")
    DISPLAY_NAMES = {
        "massive": "Massive",
        "alpaca": "Alpaca",
        "finnhub": "Finnhub",
        "twelvedata": "Twelve Data",
        "coinbase": "Coinbase",
    }

    def __init__(self) -> None:
        self._condition = threading.Condition(threading.RLock())
        self._items: dict[str, dict[str, Any]] = {}
        self._subscription_groups: dict[str, dict[str, Any]] = {}
        self._fingerprint = ""
        self._subscription_version = 0
        self._sequence = 0
        self._quotes: dict[str, dict[str, dict[str, Any]]] = {}
        self._minute_bars: dict[tuple[str, str], dict[str, Any]] = {}
        self._threads: dict[str, threading.Thread] = {}
        self._stop = threading.Event()
        self._auth_probe_lock = threading.Lock()
        self._auth_probes: dict[str, dict[str, Any]] = {}
        self._massive_key = os.environ.get("APEX_MASSIVE_API_KEY") or os.environ.get("MASSIVE_API_KEY") or os.environ.get("POLYGON_API_KEY") or ""
        self._massive_ws_url = str(os.environ.get("APEX_MASSIVE_WS_URL") or "wss://delayed.massive.com/stocks").strip()
        self._massive_feed = "delayed_sip" if "delayed." in self._massive_ws_url.lower() else "SIP"
        self._alpaca_key_env, self._alpaca_key = _read_api_credential(
            "APCA_API_KEY_ID", "ALPACA_API_KEY_ID", "ALPACA_API_KEY"
        )
        self._alpaca_secret_env, self._alpaca_secret = _read_api_credential(
            "APCA_API_SECRET_KEY", "ALPACA_API_SECRET_KEY", "ALPACA_API_SECRET"
        )
        self._alpaca_feed = str(os.environ.get("APEX_ALPACA_FEED", "iex") or "iex").lower()
        if self._alpaca_feed not in {"iex", "sip", "delayed_sip"}:
            self._alpaca_feed = "iex"
        try:
            self._alpaca_symbol_limit = max(1, min(1_000, int(os.environ.get("APEX_ALPACA_SYMBOL_LIMIT", "30"))))
        except Exception:
            self._alpaca_symbol_limit = 30
        self._finnhub_key_env, self._finnhub_key = _read_api_credential(
            "FINNHUB_API_KEY", "FINNHUB_TOKEN", "FINNHUB_API_TOKEN", "APEX_FINNHUB_API_KEY", bearer=True
        )
        self._finnhub_btc_symbol = str(os.environ.get("APEX_FINNHUB_BTC_SYMBOL", "BINANCE:BTCUSDT") or "BINANCE:BTCUSDT").upper()
        try:
            self._finnhub_symbol_limit = max(1, min(1_000, int(os.environ.get("APEX_FINNHUB_SYMBOL_LIMIT", "50"))))
        except Exception:
            self._finnhub_symbol_limit = 50
        self._twelve_key = os.environ.get("TWELVE_DATA_API_KEY") or os.environ.get("APEX_TWELVE_DATA_API_KEY") or ""
        dependency = websocket_client is not None
        alpaca_issues = [
            issue for issue in (
                _credential_issue(self._alpaca_key, "Alpaca key ID"),
                _credential_issue(self._alpaca_secret, "Alpaca secret"),
            ) if issue
        ]
        finnhub_issues = [issue for issue in (_credential_issue(self._finnhub_key, "Finnhub token"),) if issue]
        self._credential_issues = {
            "alpaca": "; ".join(alpaca_issues),
            "finnhub": "; ".join(finnhub_issues),
        }
        configured = {
            "massive": bool(self._massive_key and dependency),
            "alpaca": bool(self._alpaca_key and self._alpaca_secret and not alpaca_issues and dependency),
            "finnhub": bool(self._finnhub_key and not finnhub_issues and dependency),
            "twelvedata": bool(self._twelve_key and dependency),
            "coinbase": bool(dependency),
        }
        self._status: dict[str, dict[str, Any]] = {
            provider: {
                "configured": configured[provider],
                "connected": False,
                "state": "idle" if configured[provider] else (
                    "missing websocket-client" if not dependency else self._credential_issues.get(provider) or "not configured"
                ),
                "symbols": 0,
                "last_event_ms": None,
                "last_error": None,
                "auth_probe": None,
                "credential_source": (
                    f"{self._alpaca_key_env} + {self._alpaca_secret_env}" if provider == "alpaca" and self._alpaca_key_env and self._alpaca_secret_env
                    else self._finnhub_key_env if provider == "finnhub" and self._finnhub_key_env
                    else None
                ),
                "credential_fingerprint": (
                    f"{_credential_fingerprint(self._alpaca_key)}/{_credential_fingerprint(self._alpaca_secret)}" if provider == "alpaca" and self._alpaca_key and self._alpaca_secret
                    else _credential_fingerprint(self._finnhub_key) if provider == "finnhub"
                    else None
                ),
                "feed": self._alpaca_feed if provider == "alpaca" else (self._massive_feed if provider == "massive" else ("public" if provider == "coinbase" else "real-time")),
            }
            for provider in self.PROVIDERS
        }

    def _probe_provider_auth(self, provider: str, force: bool = False) -> dict[str, Any]:
        if provider not in {"alpaca", "finnhub"}:
            return {"state": "not applicable", "checked_at": int(time.time() * 1000)}
        now = time.time()
        with self._auth_probe_lock:
            cached = dict(self._auth_probes.get(provider) or {})
            if cached and not force and now - float(cached.get("_checked_monotonic") or 0.0) < PROVIDER_AUTH_PROBE_TTL_SEC:
                cached.pop("_checked_monotonic", None)
                return cached
        checked_at = int(now * 1000)
        try:
            if provider == "alpaca":
                req = Request(
                    "https://data.alpaca.markets/v2/stocks/AAPL/trades/latest?feed=iex",
                    headers={
                        "Accept": "application/json",
                        "APCA-API-KEY-ID": self._alpaca_key,
                        "APCA-API-SECRET-KEY": self._alpaca_secret,
                        "User-Agent": "ApexMarketPredictor/15.0 auth-probe",
                    },
                    method="GET",
                )
            else:
                req = Request(
                    "https://finnhub.io/api/v1/quote?symbol=AAPL",
                    headers={
                        "Accept": "application/json",
                        "X-Finnhub-Token": self._finnhub_key,
                        "User-Agent": "ApexMarketPredictor/15.0 auth-probe",
                    },
                    method="GET",
                )
            with urlopen(req, timeout=PROVIDER_AUTH_PROBE_TIMEOUT) as response:
                payload = json.loads(response.read().decode("utf-8", "replace") or "{}")
            if provider == "finnhub" and isinstance(payload, dict) and str(payload.get("error") or "").strip():
                result = {"state": "rejected", "checked_at": checked_at, "detail": str(payload.get("error"))[:160]}
            else:
                result = {"state": "verified", "checked_at": checked_at, "detail": "REST authentication accepted"}
        except HTTPError as exc:
            result = {
                "state": "rejected" if int(getattr(exc, "code", 0) or 0) in {401, 402, 403} else "unavailable",
                "checked_at": checked_at,
                "detail": _http_error_detail(exc),
            }
        except Exception as exc:
            result = {"state": "unavailable", "checked_at": checked_at, "detail": _http_error_detail(exc)}
        stored = {**result, "_checked_monotonic": now}
        with self._auth_probe_lock:
            self._auth_probes[provider] = stored
        self._set_status(provider, auth_probe=dict(result))
        return result

    def http_credentials(self) -> dict[str, Any]:
        return {
            "alpaca": {
                "key": self._alpaca_key,
                "secret": self._alpaca_secret,
                "feed": "iex",
                "usable": not bool(self._credential_issues.get("alpaca")),
            },
            "finnhub": {
                "token": self._finnhub_key,
                "usable": not bool(self._credential_issues.get("finnhub")),
            },
        }

    def subscribe(self, items: Any, client_id: str = "dashboard") -> None:
        if not isinstance(items, list):
            return
        client_id = re.sub(r"[^a-z0-9_.:-]+", "-", str(client_id or "dashboard").strip().lower())[:80] or "dashboard"
        watch = {_tv_clean_token(row.get("label")): row for row in load_watchlist()}
        with self._condition:
            current_items = {label: dict(row) for label, row in self._items.items()}
        incoming: dict[str, dict[str, Any]] = {}
        for raw in items[:160]:
            if not isinstance(raw, dict):
                continue
            label = _tv_clean_token(raw.get("label") or raw.get("ticker"))
            if not label:
                continue
            base = dict(current_items.get(label, {}))
            base.update(watch.get(label, {}))
            base.update({k: v for k, v in raw.items() if v not in (None, "")})
            base["label"] = label
            base["exchange"] = _tv_clean_token(
                raw.get("tv_exchange") or raw.get("original_exchange") or raw.get("exchange") or base.get("exchange")
            )
            base["symbol"] = str(
                raw.get("tv_symbol") or raw.get("original_symbol") or raw.get("symbol") or base.get("symbol") or label
            ).strip()
            incoming[label] = base
        now = time.monotonic()
        with self._condition:
            self._subscription_groups[client_id] = {"expires": now + 30.0, "items": incoming}
            self._subscription_groups = {
                key: group for key, group in self._subscription_groups.items()
                if float(group.get("expires") or 0.0) >= now
            }
            merged: dict[str, dict[str, Any]] = {}
            for key in sorted(self._subscription_groups):
                for label, row in (self._subscription_groups[key].get("items") or {}).items():
                    merged[label] = {**merged.get(label, {}), **row}
            ranked = sorted(merged.items(), key=lambda pair: (-float(_num_or_none(pair[1].get("live_priority")) or 0.0), pair[0]))[:160]
            normalized = dict(ranked)
            fingerprint = json.dumps(
                sorted((lab, row.get("exchange"), row.get("symbol"), row.get("previous_close"), row.get("live_priority")) for lab, row in normalized.items()),
                separators=(",", ":"),
                default=str,
            )
            if fingerprint != self._fingerprint:
                self._items = normalized
                self._fingerprint = fingerprint
                self._subscription_version += 1
                self._sequence += 1
                self._condition.notify_all()
        self._start_threads()

    def _start_threads(self) -> None:
        if websocket_client is None:
            return
        with self._condition:
            for provider in self.PROVIDERS:
                if not self._status[provider].get("configured") or provider in self._threads:
                    continue
                thread = threading.Thread(
                    target=self._provider_runner,
                    args=(provider,),
                    daemon=True,
                    name=f"apex-live-{provider}",
                )
                self._threads[provider] = thread
                thread.start()

    def _provider_symbols(self, provider: str) -> tuple[list[str], dict[str, str], int]:
        with self._condition:
            items = {k: dict(v) for k, v in self._items.items()}
            version = self._subscription_version
        buckets: dict[str, list[str]] = {}
        priorities: dict[str, float] = {}
        for label, item in items.items():
            exchange = _tv_clean_token(item.get("exchange"))
            symbol = str(item.get("symbol") or label).strip().upper()
            if provider == "coinbase":
                stream_symbol = "BTC-USD" if label == "BTCUSD" else ""
            elif provider == "finnhub" and label == "BTCUSD":
                stream_symbol = self._finnhub_btc_symbol
            elif provider in {"massive", "alpaca", "finnhub"}:
                stream_symbol = symbol if exchange in US_EXCHANGES and label not in STREAM_INDEX_LABELS and not symbol.startswith(".") else ""
            elif provider == "twelvedata":
                if label in STREAM_INDEX_LABELS:
                    stream_symbol = ""
                elif label == "BTCUSD":
                    stream_symbol = "BTC/USD"
                else:
                    stream_symbol = symbol
            else:
                stream_symbol = ""
            stream_symbol = stream_symbol.strip().upper()
            if stream_symbol:
                buckets.setdefault(stream_symbol, []).append(label)
                priorities[stream_symbol] = max(priorities.get(stream_symbol, 0.0), float(_num_or_none(item.get("live_priority")) or 0.0))
        mapping: dict[str, str] = {}
        for symbol, labels in buckets.items():
            exact = next((lab for lab in labels if lab == symbol), None)
            if exact:
                mapping[symbol] = exact
            elif len(labels) == 1:
                mapping[symbol] = labels[0]
        symbols = sorted(mapping, key=lambda value: (-priorities.get(value, 0.0), value))
        if provider == "alpaca" and len(symbols) > self._alpaca_symbol_limit:
            symbols = symbols[:self._alpaca_symbol_limit]
            mapping = {symbol: mapping[symbol] for symbol in symbols}
        elif provider == "finnhub" and len(symbols) > self._finnhub_symbol_limit:
            symbols = symbols[:self._finnhub_symbol_limit]
            mapping = {symbol: mapping[symbol] for symbol in symbols}
        return symbols, mapping, version

    def _version_changed(self, version: int) -> bool:
        with self._condition:
            return version != self._subscription_version

    def _set_status(self, provider: str, **updates: Any) -> None:
        with self._condition:
            status = self._status.setdefault(provider, {})
            changed = any(status.get(k) != v for k, v in updates.items())
            status.update(updates)
            if changed:
                self._sequence += 1
                self._condition.notify_all()

    def _connect(self, url: str, headers: list[str] | None = None):
        if websocket_client is None:
            raise RuntimeError("websocket-client is not installed")
        options: dict[str, Any] = {"timeout": LIVE_STREAM_CONNECT_TIMEOUT, "enable_multithread": True}
        if headers:
            options["header"] = headers
        ws = websocket_client.create_connection(url, **options)
        ws.settimeout(LIVE_STREAM_RECV_TIMEOUT)
        return ws

    @staticmethod
    def _recv(ws: Any) -> str | bytes | None:
        try:
            return ws.recv()
        except Exception as exc:
            if websocket_client is not None and isinstance(exc, websocket_client.WebSocketTimeoutException):
                return None
            raise

    @staticmethod
    def _failure_state(provider: str, message: str) -> tuple[str, float]:
        lower = str(message or "").lower()
        if "connection limit" in lower or "code 406" in lower or " 406:" in lower:
            return "connection limit", 60.0
        if "symbol limit" in lower or "code 405" in lower or " 405:" in lower:
            return "symbol limit", 15.0
        auth_markers = (
            "auth failed", "authentication failed", "unauthorized", "invalid api key", "invalid token",
            "handshake status 401", "code 401", " 401:", "code 402", " 402:",
        )
        if any(marker in lower for marker in auth_markers):
            return "authentication failed", 300.0
        if provider in {"alpaca", "finnhub"} and ("handshake status 403" in lower or "403 forbidden" in lower):
            return "authentication failed", 300.0
        if "maximum number of connections" in lower or "too many connections" in lower or "code 429" in lower or " 429" in lower:
            return "connection limit", 60.0
        if "insufficient subscription" in lower or "not authorized" in lower or "code 409" in lower or " 409:" in lower:
            return "plan unavailable", 45.0
        return "reconnecting", 0.0

    def _redact_provider_error(self, message: Any) -> str:
        clean = re.sub(r"[\r\n]+", " ", str(message or ""))
        for secret in (self._alpaca_key, self._alpaca_secret, self._finnhub_key, self._massive_key, self._twelve_key):
            if secret and len(secret) >= 8:
                clean = clean.replace(secret, "[redacted]")
                clean = clean.replace(quote(secret, safe=""), "[redacted]")
        clean = re.sub(r"(?i)(token=)[^&\s'\"]+", r"\1[redacted]", clean)
        return clean[:220]

    def _provider_runner(self, provider: str) -> None:
        handler = getattr(self, f"_run_{provider}")
        backoff = 1.0
        while not self._stop.is_set():
            symbols, mapping, version = self._provider_symbols(provider)
            self._set_status(provider, symbols=len(symbols))
            if not symbols:
                self._set_status(provider, connected=False, state="waiting for compatible ticker", last_error=None)
                self._stop.wait(0.75)
                continue
            if provider in {"alpaca", "finnhub"}:
                probe = self._probe_provider_auth(provider)
                if probe.get("state") == "rejected":
                    detail = self._redact_provider_error(probe.get("detail") or "REST credentials rejected")
                    self._set_status(
                        provider,
                        connected=False,
                        state="authentication failed",
                        last_error=f"REST authentication rejected: {detail}",
                        retry_in_seconds=300.0,
                    )
                    self._stop.wait(300.0)
                    continue
            self._set_status(provider, connected=False, state="connecting", last_error=None)
            try:
                handler(symbols, mapping, version)
                backoff = 1.0
            except Exception as exc:
                message = self._redact_provider_error(exc)
                state, minimum_wait = self._failure_state(provider, message)
                if state == "authentication failed" and provider in {"alpaca", "finnhub"}:
                    probe = self._probe_provider_auth(provider, force=True)
                    if probe.get("state") == "verified":
                        state = "websocket rejected"
                        minimum_wait = 30.0
                        message = f"REST credentials verified; WebSocket rejected the session: {message}"
                wait_seconds = max(backoff, minimum_wait)
                self._set_status(provider, connected=False, state=state, last_error=message, retry_in_seconds=round(wait_seconds, 1))
                self._stop.wait(wait_seconds)
                backoff = min(LIVE_STREAM_RECONNECT_MAX, backoff * 1.8)

    def _emit(
        self,
        provider: str,
        label: str,
        price: Any,
        event_timestamp: Any,
        source_symbol: str,
        *,
        volume: Any = None,
        feed: str = "real-time",
        open_price: Any = None,
        high_price: Any = None,
        low_price: Any = None,
        bar_start: Any = None,
    ) -> None:
        numeric_price = _num_or_none(price)
        if numeric_price is None or numeric_price <= 0:
            return
        received_ms = int(time.time() * 1000)
        event_ms = _stream_timestamp_ms(event_timestamp, received_ms)
        if event_ms > received_ms + 5_000 or event_ms < received_ms - 86_400_000:
            event_ms = received_ms
        transport_latency = max(0, received_ms - event_ms)
        label = _tv_clean_token(label)
        if not label:
            return
        with self._condition:
            item = self._items.get(label, {})
            previous_close = _num_or_none(item.get("previous_close"))
            change_abs = numeric_price - previous_close if previous_close and previous_close > 0 else None
            change_pct = (numeric_price / previous_close - 1.0) * 100.0 if previous_close and previous_close > 0 else None
            start_ms = _stream_timestamp_ms(bar_start, event_ms) if bar_start not in (None, "") else (event_ms // 60_000) * 60_000
            start_ms = (start_ms // 60_000) * 60_000
            bar_key = (provider, label)
            old_bar = self._minute_bars.get(bar_key)
            if old_bar is None or int(old_bar.get("bar_start") or 0) != start_ms:
                old_bar = {
                    "bar_start": start_ms,
                    "open": numeric_price,
                    "high": numeric_price,
                    "low": numeric_price,
                    "close": numeric_price,
                    "bar_volume": 0.0,
                }
            supplied_open = _num_or_none(open_price)
            supplied_high = _num_or_none(high_price)
            supplied_low = _num_or_none(low_price)
            numeric_volume = _num_or_none(volume) or 0.0
            if supplied_open and supplied_high and supplied_low:
                old_bar.update({
                    "open": supplied_open,
                    "high": max(supplied_open, supplied_high, supplied_low, numeric_price),
                    "low": min(supplied_open, supplied_high, supplied_low, numeric_price),
                    "close": numeric_price,
                    "bar_volume": max(float(old_bar.get("bar_volume") or 0.0), numeric_volume),
                })
            else:
                old_bar["high"] = max(float(old_bar.get("high") or numeric_price), numeric_price)
                old_bar["low"] = min(float(old_bar.get("low") or numeric_price), numeric_price)
                old_bar["close"] = numeric_price
                old_bar["bar_volume"] = float(old_bar.get("bar_volume") or 0.0) + max(0.0, numeric_volume)
            self._minute_bars[bar_key] = old_bar
            existing = self._quotes.get(label, {}).get(provider)
            if existing and event_ms < int(existing.get("updated_at") or 0) - 1_000:
                return
            self._sequence += 1
            provider_name = self.DISPLAY_NAMES.get(provider, provider.title())
            display_feed = feed
            if provider == "alpaca":
                display_feed = self._alpaca_feed.upper()
                provider_name = f"Alpaca {display_feed}"
            payload = {
                "label": label,
                "price": numeric_price,
                "change_pct": change_pct,
                "change_abs": change_abs,
                "currency": item.get("currency") or ("USD" if provider != "twelvedata" else None),
                "exchange": item.get("exchange"),
                "source_symbol": source_symbol,
                "source": f"{provider_name} WebSocket",
                "provider": provider_name,
                "provider_id": provider,
                "feed": display_feed,
                "update_mode": f"{display_feed} streaming trade",
                "market_open": True,
                "delayed": feed == "delayed_sip",
                "delay_minutes": 15 if feed == "delayed_sip" else 0,
                "age_ms": transport_latency,
                "transport_latency_ms": transport_latency,
                "server_received_at": received_ms,
                "updated_at": event_ms,
                "real_time_hint": feed != "delayed_sip",
                "streaming": True,
                "trade_volume": max(0.0, numeric_volume),
                "bar_start": old_bar["bar_start"],
                "open": old_bar["open"],
                "high": old_bar["high"],
                "low": old_bar["low"],
                "close": old_bar["close"],
                "bar_volume": old_bar["bar_volume"],
                "_stream_seq": self._sequence,
            }
            self._quotes.setdefault(label, {})[provider] = payload
            status = self._status.setdefault(provider, {})
            status.update({"connected": True, "state": "streaming", "last_event_ms": received_ms, "last_error": None})
            self._condition.notify_all()

    def _candidate_score(self, quote_payload: dict[str, Any], now_ms: int) -> float:
        provider = str(quote_payload.get("provider_id") or "")
        base = {
            "coinbase": 720.0,
            "alpaca": 660.0 if self._alpaca_feed == "sip" else 610.0,
            "finnhub": 510.0,
            "massive": 360.0,
            "twelvedata": 320.0,
        }.get(provider, 0.0)
        if quote_payload.get("delayed"):
            base -= 500.0
        received = _num_or_none(quote_payload.get("server_received_at")) or now_ms
        base -= min(300.0, max(0.0, now_ms - received) / 1000.0 * 3.0)
        base -= min(180.0, float(quote_payload.get("transport_latency_ms") or 0.0) / 20.0)
        return base

    def snapshot(self, labels: Any = None, preferred: str = "auto", since_sequence: int = 0) -> dict[str, Any]:
        wanted = {_tv_clean_token(x) for x in (labels or []) if _tv_clean_token(x)}
        preferred = str(preferred or "auto").lower()
        now_ms = int(time.time() * 1000)
        with self._condition:
            prices: dict[str, dict[str, Any]] = {}
            for label, providers in self._quotes.items():
                if wanted and label not in wanted:
                    continue
                candidates = [
                    dict(q) for q in providers.values()
                    if now_ms - int(q.get("server_received_at") or 0) <= LIVE_STREAM_STALE_MS
                ]
                if not candidates:
                    continue
                preferred_rows = [q for q in candidates if q.get("provider_id") == preferred]
                selected = max(preferred_rows or candidates, key=lambda q: self._candidate_score(q, now_ms))
                if int(selected.get("_stream_seq") or 0) <= int(since_sequence or 0):
                    continue
                selected["stream_age_ms"] = max(0, now_ms - int(selected.get("server_received_at") or now_ms))
                selected["age_ms"] = max(0, now_ms - int(selected.get("updated_at") or now_ms))
                prices[label] = selected
            statuses = {k: dict(v) for k, v in self._status.items()}
            return {
                "ok": True,
                "streaming": any(bool(v.get("connected")) for v in statuses.values()),
                "sequence": self._sequence,
                "prices": prices,
                "stream_status": statuses,
                "preferred_provider": preferred,
                "server_time_ms": now_ms,
            }

    def wait_snapshot(self, labels: Any, preferred: str, last_sequence: int, timeout: float) -> dict[str, Any]:
        with self._condition:
            self._condition.wait_for(lambda: self._sequence > last_sequence or self._stop.is_set(), timeout=max(0.05, timeout))
        return self.snapshot(labels, preferred, since_sequence=last_sequence)

    def status_payload(self) -> dict[str, Any]:
        payload = self.snapshot([], "auto", since_sequence=10**18)
        payload.pop("prices", None)
        payload["websocket_dependency"] = websocket_client is not None
        payload["environment_keys"] = {
            "massive": "APEX_MASSIVE_API_KEY",
            "alpaca": "APCA_API_KEY_ID + APCA_API_SECRET_KEY",
            "finnhub": "FINNHUB_API_KEY",
            "twelvedata": "TWELVE_DATA_API_KEY",
            "coinbase": "public/no key",
        }
        payload["credential_diagnostics"] = {
            "alpaca": {
                "key_environment": self._alpaca_key_env or None,
                "secret_environment": self._alpaca_secret_env or None,
                "key_length": len(self._alpaca_key),
                "secret_length": len(self._alpaca_secret),
                "key_fingerprint": _credential_fingerprint(self._alpaca_key),
                "secret_fingerprint": _credential_fingerprint(self._alpaca_secret),
                "issue": self._credential_issues.get("alpaca") or None,
                "feed": self._alpaca_feed,
                "auth_probe": dict(self._auth_probes.get("alpaca") or {}, _checked_monotonic=None),
            },
            "finnhub": {
                "token_environment": self._finnhub_key_env or None,
                "token_length": len(self._finnhub_key),
                "token_fingerprint": _credential_fingerprint(self._finnhub_key),
                "issue": self._credential_issues.get("finnhub") or None,
                "auth_probe": dict(self._auth_probes.get("finnhub") or {}, _checked_monotonic=None),
            },
        }
        for row in payload["credential_diagnostics"].values():
            probe = row.get("auth_probe") or {}
            probe.pop("_checked_monotonic", None)
        return payload

    def _run_massive(self, symbols: list[str], mapping: dict[str, str], version: int) -> None:
        url = self._massive_ws_url
        feed = self._massive_feed
        ws = self._connect(url)
        subscribed = False
        try:
            ws.send(json.dumps({"action": "auth", "params": self._massive_key}))
            while not self._stop.is_set() and not self._version_changed(version):
                raw = self._recv(ws)
                if raw is None:
                    continue
                events = json.loads(raw)
                if isinstance(events, dict):
                    events = [events]
                for event in events or []:
                    if not isinstance(event, dict):
                        continue
                    if event.get("ev") == "status":
                        status = str(event.get("status") or "")
                        if status in {"auth_success", "success"} and not subscribed:
                            # One channel per symbol avoids free-plan subscription pressure.
                            # Minute aggregates are sufficient because the client maintains the live candle.
                            channels = [f"AM.{s}" for s in symbols] if feed == "delayed_sip" else [f"T.{s}" for s in symbols]
                            ws.send(json.dumps({"action": "subscribe", "params": ",".join(channels)}))
                            subscribed = True
                            self._set_status("massive", connected=True, state="subscribed", last_error=None, feed=feed, retry_in_seconds=None)
                        elif "auth_failed" in status or status == "error":
                            raise RuntimeError(f"Massive {status}: {event.get('message') or status}")
                        continue
                    symbol = str(event.get("sym") or "").upper()
                    label = mapping.get(symbol)
                    if not label:
                        continue
                    if event.get("ev") == "T":
                        self._emit("massive", label, event.get("p"), event.get("t") or event.get("pt"), symbol, volume=event.get("s"), feed=feed)
                    elif event.get("ev") == "AM":
                        self._emit(
                            "massive", label, event.get("c"), event.get("e"), symbol,
                            volume=event.get("v"), feed=feed, open_price=event.get("o"),
                            high_price=event.get("h"), low_price=event.get("l"), bar_start=event.get("s"),
                        )
        finally:
            try:
                ws.close()
            except Exception:
                pass

    def _run_alpaca(self, symbols: list[str], mapping: dict[str, str], version: int) -> None:
        url = f"wss://stream.data.alpaca.markets/v2/{self._alpaca_feed}"
        ws = self._connect(
            url,
            headers=[
                f"APCA-API-KEY-ID: {self._alpaca_key}",
                f"APCA-API-SECRET-KEY: {self._alpaca_secret}",
            ],
        )
        subscribed = False
        message_auth_sent = False
        connected_at = time.monotonic()
        try:
            while not self._stop.is_set() and not self._version_changed(version):
                raw = self._recv(ws)
                if raw is None:
                    if not subscribed and not message_auth_sent and time.monotonic() - connected_at >= 2.5:
                        ws.send(json.dumps({"action": "auth", "key": self._alpaca_key, "secret": self._alpaca_secret}))
                        message_auth_sent = True
                    continue
                events = json.loads(raw)
                if isinstance(events, dict):
                    events = [events]
                for event in events or []:
                    if not isinstance(event, dict):
                        continue
                    event_type = str(event.get("T") or "")
                    if event_type == "success" and str(event.get("msg") or "") == "authenticated" and not subscribed:
                        # IEX trades provide the lowest-latency price and are aggregated
                        # into minute candles locally. A single channel also stays within
                        # Alpaca Basic's 30-symbol WebSocket allowance.
                        ws.send(json.dumps({"action": "subscribe", "trades": symbols}))
                        subscribed = True
                        self._set_status("alpaca", connected=True, state="subscribed", last_error=None, retry_in_seconds=None)
                        continue
                    if event_type == "error":
                        if int(event.get("code") or 0) == 403 and not subscribed:
                            ws.send(json.dumps({"action": "subscribe", "trades": symbols}))
                            subscribed = True
                            self._set_status("alpaca", connected=True, state="subscribed", last_error=None, retry_in_seconds=None)
                            continue
                        raise RuntimeError(f"Alpaca {event.get('code') or 'error'}: {event.get('msg') or 'stream error'}")
                    symbol = str(event.get("S") or "").upper()
                    label = mapping.get(symbol)
                    if not label:
                        continue
                    if event_type == "t":
                        self._emit("alpaca", label, event.get("p"), event.get("t"), symbol, volume=event.get("s"), feed=self._alpaca_feed)
                    elif event_type in {"b", "u"}:
                        self._emit(
                            "alpaca", label, event.get("c"), event.get("t"), symbol,
                            volume=event.get("v"), feed=self._alpaca_feed, open_price=event.get("o"),
                            high_price=event.get("h"), low_price=event.get("l"), bar_start=event.get("t"),
                        )
        finally:
            try:
                ws.close()
            except Exception:
                pass

    def _run_finnhub(self, symbols: list[str], mapping: dict[str, str], version: int) -> None:
        ws = self._connect(
            f"wss://ws.finnhub.io?token={quote(self._finnhub_key, safe='')}",
            headers=[f"X-Finnhub-Token: {self._finnhub_key}"],
        )
        try:
            for symbol in symbols:
                ws.send(json.dumps({"type": "subscribe", "symbol": symbol}))
            self._set_status("finnhub", connected=True, state="subscribed", last_error=None)
            while not self._stop.is_set() and not self._version_changed(version):
                raw = self._recv(ws)
                if raw is None:
                    continue
                message = json.loads(raw)
                if message.get("type") == "error":
                    raise RuntimeError(str(message.get("msg") or "Finnhub stream error"))
                if message.get("type") != "trade":
                    continue
                for event in message.get("data") or []:
                    symbol = str(event.get("s") or "").upper()
                    label = mapping.get(symbol)
                    if label:
                        self._emit("finnhub", label, event.get("p"), event.get("t"), symbol, volume=event.get("v"), feed="real-time")
        finally:
            try:
                ws.close()
            except Exception:
                pass

    def _run_twelvedata(self, symbols: list[str], mapping: dict[str, str], version: int) -> None:
        ws = self._connect(f"wss://ws.twelvedata.com/v1/quotes/price?apikey={quote(self._twelve_key, safe='')}")
        last_heartbeat = time.monotonic()
        try:
            ws.send(json.dumps({"action": "subscribe", "params": {"symbols": ",".join(symbols)}}))
            self._set_status("twelvedata", connected=True, state="subscribed", last_error=None)
            while not self._stop.is_set() and not self._version_changed(version):
                raw = self._recv(ws)
                if time.monotonic() - last_heartbeat >= 10:
                    ws.send(json.dumps({"action": "heartbeat"}))
                    last_heartbeat = time.monotonic()
                if raw is None:
                    continue
                event = json.loads(raw)
                if event.get("event") == "subscribe-status" and event.get("status") == "error":
                    raise RuntimeError(str(event.get("message") or "Twelve Data subscription error"))
                if event.get("event") != "price":
                    continue
                symbol = str(event.get("symbol") or "").upper()
                label = mapping.get(symbol)
                if label:
                    self._emit("twelvedata", label, event.get("price"), event.get("timestamp"), symbol, feed="real-time")
        finally:
            try:
                ws.close()
            except Exception:
                pass

    def _run_coinbase(self, symbols: list[str], mapping: dict[str, str], version: int) -> None:
        ws = self._connect("wss://advanced-trade-ws.coinbase.com")
        try:
            ws.send(json.dumps({"type": "subscribe", "product_ids": symbols, "channel": "market_trades"}))
            ws.send(json.dumps({"type": "subscribe", "product_ids": symbols, "channel": "ticker"}))
            ws.send(json.dumps({"type": "subscribe", "channel": "heartbeats"}))
            self._set_status("coinbase", connected=True, state="subscribed", last_error=None)
            while not self._stop.is_set() and not self._version_changed(version):
                raw = self._recv(ws)
                if raw is None:
                    continue
                message = json.loads(raw)
                message_ts = message.get("timestamp")
                for event in message.get("events") or []:
                    for trade in event.get("trades") or []:
                        symbol = str(trade.get("product_id") or "").upper()
                        label = mapping.get(symbol)
                        if label:
                            self._emit("coinbase", label, trade.get("price"), trade.get("time") or message_ts, symbol, volume=trade.get("size"), feed="public")
                    for ticker in event.get("tickers") or []:
                        symbol = str(ticker.get("product_id") or "").upper()
                        label = mapping.get(symbol)
                        if label:
                            self._emit("coinbase", label, ticker.get("price"), message_ts, symbol, feed="public")
        finally:
            try:
                ws.close()
            except Exception:
                pass


LIVE_STREAM_HUB = LiveStreamHub()


def _overlay_stream_prices(base_payload: dict[str, Any], labels: list[str], preferred_provider: str) -> dict[str, Any]:
    """Overlay fresh WebSocket ticks without mutating the HTTP fallback cache."""
    preferred = str(preferred_provider or "auto").strip().lower()
    out = dict(base_payload)
    out["prices"] = {label: dict(row) for label, row in (base_payload.get("prices") or {}).items()}
    snapshot = LIVE_STREAM_HUB.snapshot(labels, preferred)
    if preferred != "http":
        now_ms = int(time.time() * 1000)
        for label, quote_payload in (snapshot.get("prices") or {}).items():
            _merge_quote(out["prices"], label, dict(quote_payload), now_ms)
    out["streaming"] = bool(snapshot.get("streaming") and preferred != "http")
    out["sequence"] = snapshot.get("sequence", 0)
    out["stream_status"] = snapshot.get("stream_status") or {}
    out["preferred_provider"] = preferred
    out["server_time_ms"] = snapshot.get("server_time_ms", int(time.time() * 1000))
    return out


def _yahoo_symbol_candidates(item: dict[str, Any], watch_by_label: dict[str, dict[str, str]]) -> list[str]:
    lab = _tv_clean_token(item.get("label") or item.get("ticker"))
    watch = watch_by_label.get(lab, {})
    out: list[str] = []

    def add(sym: str) -> None:
        sym = str(sym or "").strip()
        if sym and sym.upper() not in [x.upper() for x in out]:
            out.append(sym)

    for sym in YAHOO_LIVE_SYMBOL_ALIASES.get(lab, []):
        add(sym)

    symbol = str(item.get("tv_symbol") or item.get("original_symbol") or item.get("symbol") or watch.get("symbol") or lab).strip()
    exchange = _tv_clean_token(item.get("tv_exchange") or item.get("original_exchange") or item.get("exchange") or watch.get("exchange"))
    if not symbol:
        return out[:4]
    yahoo_symbol = symbol.replace(".", "-")
    if exchange in US_EXCHANGES:
        add(yahoo_symbol)
    suffix = YAHOO_SUFFIX_BY_EXCHANGE.get(exchange)
    if suffix:
        sym = yahoo_symbol
        if suffix == ".HK" and sym.isdigit():
            sym = sym.zfill(4)
        add(sym + suffix)
    if not out and lab:
        add(lab.replace(".", "-"))
    return out[:4]


def _nasdaq_symbol_candidates(item: dict[str, Any], watch_by_label: dict[str, dict[str, str]]) -> list[str]:
    lab = _tv_clean_token(item.get("label") or item.get("ticker"))
    watch = watch_by_label.get(lab, {})
    exchange = _tv_clean_token(item.get("tv_exchange") or item.get("original_exchange") or item.get("exchange") or watch.get("exchange"))
    if exchange not in US_EXCHANGES:
        return []
    raw = str(item.get("tv_symbol") or item.get("original_symbol") or item.get("symbol") or watch.get("symbol") or lab).strip()
    if not raw:
        return []
    candidates = [raw.replace(".", "-"), lab.replace(".", "-")]
    out: list[str] = []
    for sym in candidates:
        sym = sym.upper()
        if sym and sym not in out:
            out.append(sym)
    return out[:2]


def _yahoo_quote_batch(symbols: list[str]) -> dict[str, Any]:
    encoded = quote(",".join(symbols), safe=",^.-")
    req = Request(
        f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={encoded}",
        headers={
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0 ApexMarketPredictor/15.0 yahoo-low-latency-quote",
        },
        method="GET",
    )
    with urlopen(req, timeout=LIVE_PRICE_TIMEOUT) as response:
        return json.loads(response.read().decode("utf-8", "replace"))


def _nasdaq_quote(symbol: str) -> dict[str, Any]:
    req = Request(
        f"https://api.nasdaq.com/api/quote/{quote(symbol, safe='.-')}/info?assetclass=stocks",
        headers={
            "Accept": "application/json, text/plain, */*",
            "Origin": "https://www.nasdaq.com",
            "Referer": f"https://www.nasdaq.com/market-activity/stocks/{quote(symbol.lower())}",
            "User-Agent": "Mozilla/5.0 ApexMarketPredictor/15.0 nasdaq-direct-quote",
        },
        method="GET",
    )
    with urlopen(req, timeout=LIVE_PRICE_TIMEOUT) as response:
        return json.loads(response.read().decode("utf-8", "replace"))


def _alpaca_latest_trade_batch(symbols: list[str], key: str, secret: str, feed: str = "iex") -> dict[str, Any]:
    encoded = quote(",".join(symbols), safe=",.-")
    req = Request(
        f"https://data.alpaca.markets/v2/stocks/trades/latest?symbols={encoded}&feed={quote(feed, safe='')}",
        headers={
            "Accept": "application/json",
            "APCA-API-KEY-ID": key,
            "APCA-API-SECRET-KEY": secret,
            "User-Agent": "ApexMarketPredictor/15.0 alpaca-rest-low-latency",
        },
        method="GET",
    )
    with urlopen(req, timeout=max(LIVE_PRICE_TIMEOUT, 1.8)) as response:
        return json.loads(response.read().decode("utf-8", "replace"))


def _finnhub_latest_quote(symbol: str, token: str) -> dict[str, Any]:
    req = Request(
        f"https://finnhub.io/api/v1/quote?symbol={quote(symbol, safe='.-')}",
        headers={
            "Accept": "application/json",
            "X-Finnhub-Token": token,
            "User-Agent": "ApexMarketPredictor/15.0 finnhub-rest-fallback",
        },
        method="GET",
    )
    with urlopen(req, timeout=max(LIVE_PRICE_TIMEOUT, 1.8)) as response:
        return json.loads(response.read().decode("utf-8", "replace"))


def _tv_scan_domain(domain: str, symbols: list[str]) -> dict[str, Any]:
    body = json.dumps(
        {
            "symbols": {"tickers": symbols, "query": {"types": []}},
            "columns": TV_SCANNER_COLUMNS,
        }
    ).encode("utf-8")
    req = Request(
        f"https://scanner.tradingview.com/{domain}/scan",
        data=body,
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Origin": "https://www.tradingview.com",
            "User-Agent": "Mozilla/5.0 ApexMarketPredictor/14.7 live-price-scanner",
        },
        method="POST",
    )
    with urlopen(req, timeout=LIVE_PRICE_TIMEOUT) as response:
        return json.loads(response.read().decode("utf-8", "replace"))


def fetch_live_prices(items: Any, preferred_provider: str = "auto") -> dict[str, Any]:
    """Fetch live streams and independent HTTP fallbacks after a run.

    The visible model run never waits for this endpoint. The browser polls it
    independently so current prices can move without slowing forecast creation.
    """
    if not isinstance(items, list):
        items = []
    items = [x for x in items if isinstance(x, dict)][:160]
    LIVE_STREAM_HUB.subscribe(items)
    labels = [
        label for label in (_tv_clean_token(x.get("label") or x.get("ticker")) for x in items)
        if label
    ]
    now = time.time()
    key = json.dumps(
        sorted(
            [
                [
                    _tv_clean_token(x.get("label") or x.get("ticker")),
                    _tv_clean_token(x.get("tv_exchange") or x.get("original_exchange") or x.get("exchange")),
                    _tv_clean_token(x.get("tv_symbol") or x.get("original_symbol") or x.get("symbol")),
                ]
                for x in items
            ]
        ),
        separators=(",", ":"),
    )
    cached_payload: dict[str, Any] | None = None
    with LIVE_PRICE_LOCK:
        if LIVE_PRICE_CACHE.get("key") == key and now - float(LIVE_PRICE_CACHE.get("ts") or 0.0) < LIVE_PRICE_TTL_SEC:
            cached_payload = dict(LIVE_PRICE_CACHE.get("payload") or {})
            cached_payload["cached"] = True
    if cached_payload is not None:
        overlaid = _overlay_stream_prices(cached_payload, labels, preferred_provider)
        return _ensure_price_fallbacks(overlaid, items)

    watch_by_label = {_tv_clean_token(x.get("label")): x for x in load_watchlist()}
    item_by_label = {_tv_clean_token(x.get("label") or x.get("ticker")): x for x in items}
    provider_credentials = LIVE_STREAM_HUB.http_credentials()
    symbols_by_domain: dict[str, list[str]] = {}
    symbol_rank: dict[str, tuple[str, int]] = {}
    yahoo_symbol_label: dict[str, str] = {}
    nasdaq_symbol_label: dict[str, str] = {}
    alpaca_symbol_label: dict[str, str] = {}
    finnhub_symbol_label: dict[str, str] = {}
    for item in items:
        label = _tv_clean_token(item.get("label") or item.get("ticker"))
        if not label:
            continue
        for rank, pair in enumerate(_tv_candidate_pairs(item, watch_by_label)):
            domain = _tv_domain_for_pair(pair)
            symbols_by_domain.setdefault(domain, [])
            if pair not in symbols_by_domain[domain]:
                symbols_by_domain[domain].append(pair)
            symbol_rank.setdefault(pair, (label, rank))
        for sym in _yahoo_symbol_candidates(item, watch_by_label):
            yahoo_symbol_label.setdefault(sym.upper(), label)
        for sym in _nasdaq_symbol_candidates(item, watch_by_label):
            if len(nasdaq_symbol_label) < LIVE_DIRECT_MAX_SYMBOLS:
                nasdaq_symbol_label.setdefault(sym.upper(), label)
        exchange = _tv_clean_token(item.get("tv_exchange") or item.get("original_exchange") or item.get("exchange") or watch_by_label.get(label, {}).get("exchange"))
        stream_symbol = str(item.get("tv_symbol") or item.get("original_symbol") or item.get("symbol") or watch_by_label.get(label, {}).get("symbol") or label).strip().upper()
        if exchange in US_EXCHANGES and label not in STREAM_INDEX_LABELS and stream_symbol and not stream_symbol.startswith("."):
            if provider_credentials["alpaca"].get("usable"):
                alpaca_symbol_label.setdefault(stream_symbol, label)
            if provider_credentials["finnhub"].get("usable") and len(finnhub_symbol_label) < max(0, LIVE_FINNHUB_REST_MAX_SYMBOLS):
                finnhub_symbol_label.setdefault(stream_symbol, label)

    prices: dict[str, dict[str, Any]] = {}
    errors: dict[str, str] = {}
    now_ms = int(now * 1000)
    if symbols_by_domain or yahoo_symbol_label or nasdaq_symbol_label or alpaca_symbol_label or finnhub_symbol_label:
        worker_count = len(symbols_by_domain) + ((len(yahoo_symbol_label) + 49) // 50) + len(nasdaq_symbol_label) + ((len(alpaca_symbol_label) + 99) // 100) + len(finnhub_symbol_label)
        max_workers = min(10, max(1, worker_count))
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {}
            for domain, symbols in symbols_by_domain.items():
                for chunk in _chunks(symbols, max(1, LIVE_PRICE_CHUNK)):
                    futures[pool.submit(_tv_scan_domain, domain, chunk)] = ("tv", domain, chunk)
            yahoo_symbols = list(yahoo_symbol_label.keys())
            for chunk in _chunks(yahoo_symbols, 50):
                futures[pool.submit(_yahoo_quote_batch, chunk)] = ("yahoo", "quote", chunk)
            for sym in list(nasdaq_symbol_label.keys())[:LIVE_DIRECT_MAX_SYMBOLS]:
                futures[pool.submit(_nasdaq_quote, sym)] = ("nasdaq", sym, [sym])
            alpaca_credentials = provider_credentials["alpaca"]
            for chunk in _chunks(list(alpaca_symbol_label.keys()), 100):
                futures[pool.submit(
                    _alpaca_latest_trade_batch,
                    chunk,
                    str(alpaca_credentials.get("key") or ""),
                    str(alpaca_credentials.get("secret") or ""),
                    str(alpaca_credentials.get("feed") or "iex"),
                )] = ("alpaca_rest", str(alpaca_credentials.get("feed") or "iex"), chunk)
            finnhub_token = str(provider_credentials["finnhub"].get("token") or "")
            for sym in finnhub_symbol_label:
                futures[pool.submit(_finnhub_latest_quote, sym, finnhub_token)] = ("finnhub_rest", sym, [sym])
            for fut in as_completed(futures):
                kind, domain, chunk = futures[fut]
                try:
                    payload = fut.result()
                except (HTTPError, URLError, TimeoutError, Exception) as exc:
                    errors[f"{kind}:{domain}"] = str(exc)
                    continue
                if kind == "alpaca_rest":
                    for row_symbol, row in (payload.get("trades") or {}).items():
                        row_symbol = str(row_symbol or "").upper()
                        label = _label_from_symbol_map(alpaca_symbol_label, row_symbol)
                        price = _num_or_none((row or {}).get("p"))
                        if not label or price is None or price <= 0:
                            continue
                        quote_ts = _stream_timestamp_ms((row or {}).get("t"), now_ms)
                        age_ms = max(0, now_ms - quote_ts)
                        previous = _num_or_none((item_by_label.get(label) or {}).get("previous_close"))
                        change_abs = price - previous if previous and previous > 0 else None
                        change_pct = (price / previous - 1.0) * 100.0 if previous and previous > 0 else None
                        _merge_quote(prices, label, {
                            "price": price,
                            "change_pct": change_pct,
                            "change_abs": change_abs,
                            "source_symbol": row_symbol,
                            "source": f"Alpaca {domain.upper()} latest trade",
                            "provider": "Alpaca",
                            "provider_id": "alpaca_rest",
                            "feed": domain,
                            "update_mode": "authenticated latest trade",
                            "market_open": age_ms < 10 * 60_000,
                            "delayed": False,
                            "delay_minutes": round(age_ms / 60_000.0, 2),
                            "age_ms": age_ms,
                            "transport_latency_ms": 0,
                            "server_received_at": now_ms,
                            "real_time_hint": age_ms < 60_000,
                            "updated_at": quote_ts,
                            "streaming": False,
                        }, now_ms)
                    continue
                if kind == "finnhub_rest":
                    label = _label_from_symbol_map(finnhub_symbol_label, str(domain).upper())
                    price = _num_or_none(payload.get("c"))
                    if label and price is not None and price > 0:
                        quote_ts = _stream_timestamp_ms(payload.get("t"), now_ms)
                        age_ms = max(0, now_ms - quote_ts)
                        _merge_quote(prices, label, {
                            "price": price,
                            "change_pct": _num_or_none(payload.get("dp")),
                            "change_abs": _num_or_none(payload.get("d")),
                            "source_symbol": str(domain).upper(),
                            "source": "Finnhub authenticated quote",
                            "provider": "Finnhub",
                            "provider_id": "finnhub_rest",
                            "feed": "REST quote",
                            "update_mode": "authenticated quote fallback",
                            "market_open": age_ms < 10 * 60_000,
                            "delayed": age_ms >= 3 * 60_000,
                            "delay_minutes": round(age_ms / 60_000.0, 2),
                            "age_ms": age_ms,
                            "transport_latency_ms": 0,
                            "server_received_at": now_ms,
                            "real_time_hint": age_ms < 60_000,
                            "updated_at": quote_ts,
                            "streaming": False,
                        }, now_ms)
                    continue
                if kind == "yahoo":
                    for row in ((payload.get("quoteResponse") or {}).get("result") or []):
                        row_symbol = str(row.get("symbol") or "").upper()
                        label = _label_from_symbol_map(yahoo_symbol_label, row_symbol)
                        if not label:
                            continue
                        close = _num_or_none(row.get("regularMarketPrice"))
                        if close is None or close <= 0:
                            continue
                        change_abs = _num_or_none(row.get("regularMarketChange"))
                        change_pct = _num_or_none(row.get("regularMarketChangePercent"))
                        ts = _num_or_none(row.get("regularMarketTime"))
                        quote_ts = int(ts * 1000) if ts else now_ms
                        age_ms = max(0, now_ms - quote_ts)
                        delayed_by = _num_or_none(row.get("exchangeDataDelayedBy")) or 0
                        market_state = str(row.get("marketState") or "")
                        delayed = bool(delayed_by > 0 or age_ms > 180_000)
                        _merge_quote(
                            prices,
                            label,
                            {
                                "price": close,
                                "change_pct": change_pct,
                                "change_abs": change_abs,
                                "currency": row.get("currency"),
                                "description": row.get("shortName") or row.get("longName"),
                                "exchange": row.get("fullExchangeName") or row.get("exchange"),
                                "source_symbol": row_symbol,
                                "source": "Yahoo Finance quote",
                                "provider": "Yahoo Finance",
                                "update_mode": market_state or "quote",
                                "market_open": market_state in {"REGULAR", "PRE", "POST"},
                                "delayed": delayed,
                                "delay_minutes": delayed_by,
                                "age_ms": age_ms,
                                "real_time_hint": (not delayed and age_ms < 45_000),
                                "updated_at": quote_ts,
                            },
                            now_ms,
                        )
                    continue
                if kind == "nasdaq":
                    label = _label_from_symbol_map(nasdaq_symbol_label, str(domain).upper())
                    data = payload.get("data") or {}
                    primary = data.get("primaryData") or {}
                    close = _num_or_none(primary.get("lastSalePrice"))
                    if label and close is not None and close > 0:
                        change_abs = _num_or_none(primary.get("netChange"))
                        change_pct = _num_or_none(primary.get("percentageChange"))
                        if change_pct is None:
                            change_pct = _change_pct_from_abs(close, change_abs)
                        status = str((data.get("marketStatus") or "").strip())
                        _merge_quote(
                            prices,
                            label,
                            {
                                "price": close,
                                "change_pct": change_pct,
                                "change_abs": change_abs,
                                "currency": "USD",
                                "description": data.get("companyName"),
                                "exchange": data.get("exchange"),
                                "source_symbol": str(domain).upper(),
                                "source": "Nasdaq public quote",
                                "provider": "Nasdaq",
                                "update_mode": status or "direct quote",
                                "market_open": "closed" not in status.lower(),
                                "delayed": False,
                                "delay_minutes": None,
                                "age_ms": 30_000,
                                "real_time_hint": False,
                                "updated_at": now_ms,
                            },
                            now_ms,
                        )
                    continue
                for row in payload.get("data") or []:
                    row_symbol = _tv_clean_token(row.get("s"))
                    label, rank = symbol_rank.get(row_symbol, ("", 999))
                    if not label:
                        continue
                    values = row.get("d") or []
                    col = {k: values[i] if i < len(values) else None for i, k in enumerate(TV_SCANNER_COLUMNS)}
                    close = _num_or_none(col.get("close"))
                    if close is None or close <= 0:
                        continue
                    update_mode = str(col.get("update_mode") or "")
                    _merge_quote(prices, label, {
                        "_rank": rank,
                        "price": close,
                        "change_pct": _num_or_none(col.get("change")),
                        "change_abs": _num_or_none(col.get("change_abs")),
                        "currency": col.get("currency"),
                        "description": col.get("description"),
                        "exchange": col.get("exchange"),
                        "source_symbol": row_symbol,
                        "source": f"TradingView screener/{domain}",
                        "provider": "TradingView",
                        "update_mode": update_mode,
                        "market_open": not _is_delayed_update_mode(update_mode),
                        "delayed": _is_delayed_update_mode(update_mode),
                        "delay_minutes": None,
                        "age_ms": 30_000 if not _is_delayed_update_mode(update_mode) else 900_000,
                        "real_time_hint": not _is_delayed_update_mode(update_mode),
                        "updated_at": now_ms,
                    }, now_ms)
    for payload in prices.values():
        payload.pop("_rank", None)
    out = {
        "ok": True,
        "provider": "Alpaca IEX/Coinbase live + Finnhub/Yahoo/Nasdaq/TradingView fallback",
        "updated_at": now_ms,
        "ttl_ms": int(LIVE_PRICE_TTL_SEC * 1000),
        "prices": prices,
        "errors": errors,
    }
    with LIVE_PRICE_LOCK:
        LIVE_PRICE_CACHE.update({"key": key, "ts": now, "payload": out})
    overlaid = _overlay_stream_prices(out, labels, preferred_provider)
    return _ensure_price_fallbacks(overlaid, items)


def _alpaca_intraday_payload(
    lab: str,
    item: dict[str, Any],
    interval: str,
    range_name: str,
    *,
    start_at: datetime | None = None,
    end_at: datetime | None = None,
    max_bars: int | None = None,
) -> tuple[dict[str, Any] | None, list[str]]:
    """Fetch free-plan US bars from Alpaca IEX before public web fallbacks."""
    key = os.environ.get("APCA_API_KEY_ID") or os.environ.get("ALPACA_API_KEY") or ""
    secret = os.environ.get("APCA_API_SECRET_KEY") or os.environ.get("ALPACA_API_SECRET") or ""
    if not key or not secret:
        return None, []
    exchange = _tv_clean_token(
        item.get("tv_exchange") or item.get("original_exchange") or item.get("exchange")
    )
    symbol = str(
        item.get("tv_symbol") or item.get("original_symbol") or item.get("symbol") or lab
    ).strip().upper()
    if exchange not in US_EXCHANGES or lab in STREAM_INDEX_LABELS or not symbol or symbol.startswith("."):
        return None, []
    timeframe = {"1m": "1Min", "5m": "5Min", "15m": "15Min", "1h": "1Hour"}.get(interval)
    days = {"5d": 8, "30d": 35, "1mo": 35, "3mo": 100, "6mo": 190, "1y": 370}.get(range_name)
    if not timeframe or (start_at is None and not days):
        return None, []
    feed = str(os.environ.get("APEX_ALPACA_HISTORY_FEED") or os.environ.get("APEX_ALPACA_FEED") or "iex").lower()
    if feed not in {"iex", "sip", "delayed_sip"}:
        feed = "iex"
    end = end_at or datetime.now(timezone.utc)
    start = start_at or end - timedelta(days=int(days or 1))
    if end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)
    else:
        end = end.astimezone(timezone.utc)
    if start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    else:
        start = start.astimezone(timezone.utc)
    if end <= start:
        return None, [f"Alpaca {symbol} {interval}: invalid history window"]
    base_params = {
        "timeframe": timeframe,
        "limit": "10000",
        "adjustment": "all",
        "feed": feed,
        "sort": "asc",
    }
    rows_by_ts: dict[int, dict[str, Any]] = {}
    rejected = 0
    errors: list[str] = []
    # Keep every request below Alpaca's page ceiling. The former single-range
    # request stopped after four pages and could leave an old block separated
    # from the recent candles by several months.
    window_days = {"1m": 5, "5m": 25, "15m": 75, "1h": 120}[interval]
    window_start = start
    while window_start < end:
        window_end = min(end, window_start + timedelta(days=window_days))
        params = {
            **base_params,
            "start": window_start.isoformat(timespec="seconds").replace("+00:00", "Z"),
            "end": window_end.isoformat(timespec="seconds").replace("+00:00", "Z"),
        }
        next_page = ""
        seen_page_tokens: set[str] = set()
        for _ in range(8):
            if next_page:
                params["page_token"] = next_page
            req = Request(
                f"https://data.alpaca.markets/v2/stocks/{quote(symbol, safe='.-')}/bars?{urlencode(params)}",
                headers={
                    "Accept": "application/json",
                    "APCA-API-KEY-ID": key,
                    "APCA-API-SECRET-KEY": secret,
                    "User-Agent": "ApexMarketPredictor/15.0 alpaca-iex-history",
                },
                method="GET",
            )
            try:
                with urlopen(req, timeout=INTRADAY_TIMEOUT) as response:
                    raw = json.loads(response.read().decode("utf-8", "replace"))
            except Exception as exc:
                errors.append(
                    f"Alpaca {symbol} {interval} window "
                    f"{window_start.date()}-{window_end.date()}: {exc}"
                )
                return None, errors
            previous_close = rows_by_ts[max(rows_by_ts)]["close"] if rows_by_ts else None
            for row in raw.get("bars") or []:
                ts_ms = _stream_timestamp_ms(row.get("t"), 0)
                o = _num_or_none(row.get("o"))
                h = _num_or_none(row.get("h"))
                low = _num_or_none(row.get("l"))
                c = _num_or_none(row.get("c"))
                if ts_ms <= 0 or any(value is None or value <= 0 for value in (o, h, low, c)):
                    rejected += 1
                    continue
                assert o is not None and h is not None and low is not None and c is not None
                bar_high, bar_low = max(o, h, low, c), min(o, h, low, c)
                if bar_low <= 0 or bar_high / bar_low > 1.60:
                    rejected += 1
                    continue
                if previous_close is not None and not (0.45 <= c / previous_close <= 2.20):
                    rejected += 1
                    continue
                rows_by_ts[ts_ms] = {
                    "date": datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
                    "timestamp": ts_ms,
                    "open": o,
                    "high": bar_high,
                    "low": bar_low,
                    "close": c,
                    "volume": max(0, int(_num_or_none(row.get("v")) or 0)),
                }
                previous_close = c
            next_page = str(raw.get("next_page_token") or "")
            if not next_page:
                break
            if next_page in seen_page_tokens:
                errors.append(f"Alpaca {symbol} {interval}: repeated pagination token")
                return None, errors
            seen_page_tokens.add(next_page)
        else:
            errors.append(
                f"Alpaca {symbol} {interval}: pagination limit reached for "
                f"{window_start.date()}-{window_end.date()}"
            )
            return None, errors
        window_start = window_end
    bar_limit = max(100, int(max_bars or INTRADAY_MAX_BARS))
    bars = [rows_by_ts[value] for value in sorted(rows_by_ts)][-bar_limit:]
    if not bars:
        if not errors:
            errors.append(f"Alpaca {symbol}: empty {interval} IEX series")
        return None, errors
    last_bar_ms = int(bars[-1]["timestamp"])
    if int(end.timestamp() * 1000) - last_bar_ms > 10 * 86400 * 1000:
        errors.append(
            f"Alpaca {symbol} {interval}: incomplete history ends at "
            f"{datetime.fromtimestamp(last_bar_ms / 1000.0, tz=timezone.utc).date()}"
        )
        return None, errors
    for previous, current in zip(bars, bars[1:]):
        if int(current["timestamp"]) - int(previous["timestamp"]) > 10 * 86400 * 1000:
            errors.append(
                f"Alpaca {symbol} {interval}: incomplete history gap between "
                f"{str(previous['date'])[:10]} and {str(current['date'])[:10]}"
            )
            return None, errors
    age_seconds = max(0.0, time.time() - float(bars[-1]["timestamp"]) / 1000.0)
    return {
        "ok": True,
        "label": lab,
        "symbol": symbol,
        "interval": interval,
        "range": range_name,
        "source": f"Alpaca {feed.upper()} {interval} bars",
        "exchange": exchange,
        "timezone": "America/New_York",
        "currency": item.get("currency") or "USD",
        "market_open": age_seconds < 180,
        "delayed": age_seconds > 120,
        "delay_seconds": int(age_seconds),
        "updated_at": int(bars[-1]["timestamp"]),
        "bars": bars,
        "bars_returned": len(bars),
        "rejected_bars": rejected,
        "cached": False,
    }, errors


def _yahoo_chart_request(symbol: str, query_string: str, timeout: float | None = None) -> dict[str, Any]:
    encoded = quote(str(symbol or "").strip(), safe="^.-=")
    errors: list[str] = []
    for host in ("query2.finance.yahoo.com", "query1.finance.yahoo.com"):
        req = Request(
            f"https://{host}/v8/finance/chart/{encoded}?{query_string}",
            headers={
                "Accept": "application/json",
                "User-Agent": "Mozilla/5.0 ApexMarketPredictor/15.0 adaptive-chart-series",
            },
            method="GET",
        )
        try:
            with urlopen(req, timeout=float(timeout or INTRADAY_TIMEOUT)) as response:
                return json.loads(response.read().decode("utf-8", "replace"))
        except Exception as exc:
            errors.append(f"{host}: {exc}")
    raise URLError("; ".join(errors))


def _yahoo_intraday_chart(symbol: str, interval: str = "1m", range_name: str = "5d") -> dict[str, Any]:
    if not (interval == "1m" and range_name == "30d"):
        query_string = (
            f"interval={quote(interval)}&range={quote(range_name)}"
            "&includePrePost=false&events=div%2Csplits"
        )
        return _yahoo_chart_request(symbol, query_string)

    # Yahoo limits one-minute responses to short windows. Stitch six-day
    # windows on demand so the chart can expose roughly one month of genuine
    # one-minute OHLC without persisting a local dataset.
    end_s = int(time.time()) + 60
    start_s = end_s - 30 * 86400
    rows_by_ts: dict[int, dict[str, Any]] = {}
    latest_meta: dict[str, Any] = {}
    errors: list[str] = []
    cursor = start_s
    while cursor < end_s:
        chunk_end = min(end_s, cursor + 6 * 86400)
        query_string = (
            f"interval=1m&period1={cursor}&period2={chunk_end}"
            "&includePrePost=false&events=div%2Csplits"
        )
        try:
            raw = _yahoo_chart_request(symbol, query_string)
            chart = raw.get("chart") or {}
            results = chart.get("result") or []
            if chart.get("error") or not results:
                errors.append(str(chart.get("error") or f"empty {cursor}-{chunk_end}"))
                cursor = chunk_end
                continue
            result = results[0]
            latest_meta = result.get("meta") or latest_meta
            timestamps = result.get("timestamp") or []
            quotes = ((result.get("indicators") or {}).get("quote") or [{}])[0]
            for idx, raw_ts in enumerate(timestamps):
                try:
                    ts = int(raw_ts)
                except Exception:
                    continue
                rows_by_ts[ts] = {
                    key: (quotes.get(key) or [None] * len(timestamps))[idx]
                    if idx < len(quotes.get(key) or []) else None
                    for key in ("open", "high", "low", "close", "volume")
                }
        except Exception as exc:
            errors.append(str(exc))
        cursor = chunk_end
    if not rows_by_ts:
        raise URLError("Yahoo 30-day one-minute history unavailable: " + "; ".join(errors[-3:]))
    timestamps = sorted(rows_by_ts)
    quote_rows = {key: [rows_by_ts[ts].get(key) for ts in timestamps] for key in ("open", "high", "low", "close", "volume")}
    return {
        "chart": {
            "error": None,
            "result": [{"timestamp": timestamps, "indicators": {"quote": [quote_rows]}, "meta": latest_meta}],
        }
    }


def _yahoo_intraday_window_chart(symbol: str, start_at: datetime, end_at: datetime) -> dict[str, Any]:
    """Fetch one small one-minute page; callers keep old unsupported windows off Yahoo."""
    start_s = int(start_at.astimezone(timezone.utc).timestamp())
    end_s = int(end_at.astimezone(timezone.utc).timestamp())
    if end_s <= start_s:
        raise ValueError("invalid Yahoo one-minute window")
    query_string = (
        f"interval=1m&period1={start_s}&period2={end_s}"
        "&includePrePost=false&events=div%2Csplits"
    )
    return _yahoo_chart_request(symbol, query_string)


def _clean_intraday_result(result: dict[str, Any]) -> tuple[list[dict[str, Any]], int]:
    timestamps = result.get("timestamp") or []
    indicators = result.get("indicators") or {}
    quote_rows = indicators.get("quote") or []
    values = quote_rows[0] if quote_rows and isinstance(quote_rows[0], dict) else {}
    rows_by_ts: dict[int, dict[str, Any]] = {}
    rejected = 0
    previous_close: float | None = None
    for idx, raw_ts in enumerate(timestamps):
        try:
            ts = int(raw_ts)
        except Exception:
            rejected += 1
            continue
        o = _num_or_none((values.get("open") or [None] * len(timestamps))[idx] if idx < len(values.get("open") or []) else None)
        h = _num_or_none((values.get("high") or [None] * len(timestamps))[idx] if idx < len(values.get("high") or []) else None)
        low = _num_or_none((values.get("low") or [None] * len(timestamps))[idx] if idx < len(values.get("low") or []) else None)
        c = _num_or_none((values.get("close") or [None] * len(timestamps))[idx] if idx < len(values.get("close") or []) else None)
        if any(v is None or v <= 0 for v in (o, h, low, c)):
            rejected += 1
            continue
        assert o is not None and h is not None and low is not None and c is not None
        bar_high = max(o, h, low, c)
        bar_low = min(o, h, low, c)
        # Reject isolated feed spikes before they can destroy the chart scale.
        if bar_low <= 0 or bar_high / bar_low > 1.60:
            rejected += 1
            continue
        if previous_close is not None:
            ratio = c / previous_close
            if ratio < 0.45 or ratio > 2.20:
                rejected += 1
                continue
        volume_values = values.get("volume") or []
        raw_volume = _num_or_none(volume_values[idx] if idx < len(volume_values) else None)
        rows_by_ts[ts] = {
            "date": datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
            "timestamp": ts * 1000,
            "open": o,
            "high": bar_high,
            "low": bar_low,
            "close": c,
            "volume": max(0, int(raw_volume or 0)),
        }
        previous_close = c
    rows = [rows_by_ts[k] for k in sorted(rows_by_ts)]
    return rows[-max(100, INTRADAY_MAX_BARS):], rejected


def _tradingview_intraday_fallback(
    lab: str,
    item: dict[str, Any],
    watch_by_label: dict[str, dict[str, str]],
    interval_name: str = "1m",
) -> tuple[dict[str, Any] | None, list[str]]:
    """Use the project's TradingView websocket client when Yahoo lacks a chart series."""
    global INTRADAY_TV_CLIENT
    errors: list[str] = []
    try:
        from tvDatafeed import Interval, TvDatafeed
    except Exception as exc:
        return None, [f"TradingView chart client unavailable: {exc}"]
    interval_attr = {"1m": "in_1_minute", "5m": "in_5_minute", "15m": "in_15_minute", "1h": "in_1_hour"}.get(interval_name)
    tv_interval = getattr(Interval, interval_attr, None) if interval_attr else None
    if tv_interval is None:
        return None, [f"TradingView interval unsupported: {interval_name}"]
    pairs = _tv_candidate_pairs(item, watch_by_label)
    if not pairs:
        return None, ["TradingView 1m symbol mapping unavailable"]
    with INTRADAY_TV_LOCK:
        if INTRADAY_TV_CLIENT is None:
            try:
                INTRADAY_TV_CLIENT = TvDatafeed()
            except Exception as exc:
                return None, [f"TradingView chart client startup failed: {exc}"]
        local_tz = datetime.now().astimezone().tzinfo or timezone.utc
        for pair in pairs[:3]:
            exchange, symbol = pair.split(":", 1)
            try:
                frame = INTRADAY_TV_CLIENT.get_hist(
                    symbol=symbol,
                    exchange=exchange,
                    interval=tv_interval,
                    n_bars=min(10000, max(100, INTRADAY_MAX_BARS)),
                    extended_session=False,
                )
            except Exception as exc:
                errors.append(f"{pair}: {exc}")
                continue
            if frame is None or getattr(frame, "empty", True):
                errors.append(f"{pair}: empty TradingView {interval_name} series")
                continue
            rows_by_ts: dict[int, dict[str, Any]] = {}
            rejected = 0
            previous_close: float | None = None
            for idx, values in frame.iterrows():
                o = _num_or_none(values.get("open"))
                h = _num_or_none(values.get("high"))
                low = _num_or_none(values.get("low"))
                c = _num_or_none(values.get("close"))
                if any(v is None or v <= 0 for v in (o, h, low, c)):
                    rejected += 1
                    continue
                assert o is not None and h is not None and low is not None and c is not None
                bar_high, bar_low = max(o, h, low, c), min(o, h, low, c)
                if bar_low <= 0 or bar_high / bar_low > 1.60:
                    rejected += 1
                    continue
                if previous_close is not None and not (0.45 <= c / previous_close <= 2.20):
                    rejected += 1
                    continue
                try:
                    dt = idx.to_pydatetime() if hasattr(idx, "to_pydatetime") else idx
                    if not isinstance(dt, datetime):
                        dt = datetime.fromisoformat(str(dt))
                    if dt.tzinfo is None:
                        # tvDatafeed creates this naive value with
                        # datetime.fromtimestamp(epoch). Reversing it with
                        # timestamp() preserves the server's DST rules instead
                        # of applying today's fixed Paris offset to every bar.
                        ts_ms = int(dt.timestamp() * 1000)
                        dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
                    else:
                        dt = dt.astimezone(timezone.utc)
                        ts_ms = int(dt.timestamp() * 1000)
                except Exception:
                    rejected += 1
                    continue
                rows_by_ts[ts_ms] = {
                    "date": dt.isoformat(timespec="seconds").replace("+00:00", "Z"),
                    "timestamp": ts_ms,
                    "open": o,
                    "high": bar_high,
                    "low": bar_low,
                    "close": c,
                    "volume": max(0, int(_num_or_none(values.get("volume")) or 0)),
                }
                previous_close = c
            bars = [rows_by_ts[k] for k in sorted(rows_by_ts)][-max(100, INTRADAY_MAX_BARS):]
            if not bars:
                errors.append(f"{pair}: no validated TradingView {interval_name} bars")
                continue
            age_seconds = max(0.0, time.time() - float(bars[-1]["timestamp"]) / 1000.0)
            return {
                "ok": True,
                "label": lab,
                "symbol": pair,
                "interval": interval_name,
                "source": f"TradingView {interval_name} websocket bars",
                "exchange": exchange,
                "timezone": str(local_tz),
                "currency": None,
                "market_open": age_seconds < 180,
                "delayed": age_seconds > 120,
                "delay_seconds": int(age_seconds),
                "updated_at": int(bars[-1]["timestamp"]),
                "bars": bars,
                "bars_returned": len(bars),
                "rejected_bars": rejected,
                "cached": False,
            }, errors
    return None, errors


def fetch_intraday_payload(
    label: str,
    item: dict[str, Any] | None = None,
    interval: str = "1m",
    range_name: str = "5d",
) -> dict[str, Any]:
    """Return a validated, chart-only OHLC series without touching forecast runs."""
    lab = _tv_clean_token(label)
    if not lab:
        return {"ok": False, "error": "missing label", "bars": []}
    interval = str(interval or "1m").lower().strip()
    range_name = str(range_name or "5d").lower().strip()
    if (interval, range_name) not in CHART_SERIES_SPECS:
        return {"ok": False, "label": lab, "error": "unsupported chart interval/range", "bars": []}
    watch_by_label = {_tv_clean_token(x.get("label")): x for x in load_watchlist()}
    watch = watch_by_label.get(lab, {})
    request_item = {"label": lab, **watch, **(item or {})}
    candidates = _yahoo_symbol_candidates(request_item, watch_by_label)
    cache_key = lab + "|" + interval + "|" + range_name + "|" + ("|".join(candidates) or "direct")
    now = time.time()
    stale_cached_payload: dict[str, Any] | None = None
    with INTRADAY_LOCK:
        cached = INTRADAY_CACHE.get(cache_key)
        cached_payload = cached.get("payload") if cached else None
        if (cached_payload or {}).get("ok") and (cached_payload or {}).get("bars"):
            stale_cached_payload = dict(cached_payload)
        cache_ttl = (
            INTRADAY_LONG_TTL_SEC
            if (cached_payload or {}).get("ok") and range_name == "30d"
            else INTRADAY_TTL_SEC if (cached_payload or {}).get("ok")
            else INTRADAY_ERROR_TTL_SEC
        )
        if cached and now - float(cached.get("ts") or 0.0) < cache_ttl:
            payload = dict(cached.get("payload") or {})
            payload["cached"] = True
            return payload

    errors: list[str] = []
    payload, alpaca_errors = _alpaca_intraday_payload(lab, request_item, interval, range_name)
    errors.extend(alpaca_errors)
    for symbol in candidates[:3] if payload is None else []:
        try:
            raw = _yahoo_intraday_chart(symbol, interval, range_name)
            chart = raw.get("chart") or {}
            if chart.get("error"):
                errors.append(f"{symbol}: {chart.get('error')}")
                continue
            results = chart.get("result") or []
            if not results:
                errors.append(f"{symbol}: empty chart")
                continue
            result = results[0]
            bars, rejected = _clean_intraday_result(result)
            if not bars:
                errors.append(f"{symbol}: no valid {interval} bars")
                continue
            meta = result.get("meta") or {}
            period = (meta.get("currentTradingPeriod") or {}).get("regular") or {}
            now_s = int(now)
            market_open = bool(
                _num_or_none(period.get("start")) is not None
                and _num_or_none(period.get("end")) is not None
                and int(float(period.get("start"))) <= now_s <= int(float(period.get("end")))
            )
            delayed_by = int(_num_or_none(meta.get("exchangeDataDelayedBy")) or 0)
            payload = {
                "ok": True,
                "label": lab,
                "symbol": symbol,
                "interval": interval,
                "range": range_name,
                "source": f"Yahoo Finance {interval} chart",
                "exchange": meta.get("fullExchangeName") or meta.get("exchangeName") or watch.get("exchange"),
                "timezone": meta.get("exchangeTimezoneName") or "UTC",
                "currency": meta.get("currency"),
                "market_open": market_open,
                "delayed": delayed_by > 0,
                "delay_seconds": delayed_by,
                "updated_at": int(bars[-1]["timestamp"]),
                "bars": bars,
                "bars_returned": len(bars),
                "rejected_bars": rejected,
                "cached": False,
            }
            break
        except (HTTPError, URLError, TimeoutError, Exception) as exc:
            errors.append(f"{symbol}: {exc}")
    if payload is None:
        tv_payload, tv_errors = _tradingview_intraday_fallback(lab, request_item, watch_by_label, interval)
        errors.extend(tv_errors)
        if tv_payload is not None:
            tv_payload["range"] = range_name
            payload = tv_payload
    if payload is None and stale_cached_payload is not None:
        payload = stale_cached_payload
        updated_at = int(_num_or_none(payload.get("updated_at")) or 0)
        age_seconds = max(0, int(time.time() - updated_at / 1000.0)) if updated_at else 0
        payload.update({
            "source": f"{payload.get('source') or 'Intraday source'} · cached fallback",
            "market_open": False,
            "delayed": True,
            "delay_seconds": age_seconds,
            "cached": True,
            "stale": True,
            "fallback_errors": errors,
        })
    if payload is None:
        payload = {
            "ok": False,
            "label": lab,
            "interval": interval,
            "range": range_name,
            "error": "intraday feed unavailable" if candidates else "no intraday symbol mapping",
            "errors": errors,
            "bars": [],
            "cached": False,
        }
    with INTRADAY_LOCK:
        INTRADAY_CACHE[cache_key] = {"ts": now, "payload": payload}
        while len(INTRADAY_CACHE) > max(4, INTRADAY_CACHE_MAX_ENTRIES):
            oldest_key = min(INTRADAY_CACHE, key=lambda key: float(INTRADAY_CACHE[key].get("ts") or 0.0))
            INTRADAY_CACHE.pop(oldest_key, None)
    return payload


def read_json(path: Path, default: Any) -> Any:
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default
    return default


def _position_clip(value: float, low: float, high: float) -> float:
    return max(low, min(high, float(value)))


def _position_mean(values: list[float]) -> float:
    clean = [float(value) for value in values if math.isfinite(float(value))]
    return sum(clean) / len(clean) if clean else 0.0


def _position_std(values: list[float]) -> float:
    clean = [float(value) for value in values if math.isfinite(float(value))]
    if len(clean) < 2:
        return 0.0
    mean = sum(clean) / len(clean)
    return math.sqrt(sum((value - mean) ** 2 for value in clean) / (len(clean) - 1))


def _position_quantile(values: list[float], quantile_value: float) -> float:
    clean = sorted(float(value) for value in values if math.isfinite(float(value)))
    if not clean:
        return 0.0
    position = _position_clip(quantile_value, 0.0, 1.0) * (len(clean) - 1)
    lower = int(math.floor(position))
    upper = int(math.ceil(position))
    if lower == upper:
        return clean[lower]
    weight = position - lower
    return clean[lower] * (1.0 - weight) + clean[upper] * weight


def _position_ema(values: list[float], period: int) -> float:
    clean = [float(value) for value in values if math.isfinite(float(value))]
    if not clean:
        return 0.0
    alpha = 2.0 / (max(1, int(period)) + 1.0)
    ema = clean[0]
    for value in clean[1:]:
        ema = alpha * value + (1.0 - alpha) * ema
    return ema


def _position_rsi(values: list[float], period: int = 14) -> float:
    clean = [float(value) for value in values if math.isfinite(float(value))]
    if len(clean) < 2:
        return 50.0
    changes = [clean[index] - clean[index - 1] for index in range(1, len(clean))][-max(2, period):]
    gains = _position_mean([max(0.0, value) for value in changes])
    losses = _position_mean([max(0.0, -value) for value in changes])
    if losses <= 1e-12:
        return 100.0 if gains > 0 else 50.0
    return 100.0 - 100.0 / (1.0 + gains / losses)


def _position_slope_pct(values: list[float], period: int) -> float:
    sample = [float(value) for value in values[-max(2, period):] if math.isfinite(float(value))]
    if len(sample) < 2:
        return 0.0
    x_mean = (len(sample) - 1) / 2.0
    y_mean = _position_mean(sample)
    denominator = sum((index - x_mean) ** 2 for index in range(len(sample)))
    if denominator <= 0 or y_mean <= 0:
        return 0.0
    slope = sum((index - x_mean) * (value - y_mean) for index, value in enumerate(sample)) / denominator
    return slope / y_mean * 100.0


def _position_correlation(left: list[float], right: list[float]) -> float:
    size = min(len(left), len(right))
    if size < 3:
        return 0.0
    x = [float(value) for value in left[-size:]]
    y = [float(value) for value in right[-size:]]
    x_mean, y_mean = _position_mean(x), _position_mean(y)
    numerator = sum((a - x_mean) * (b - y_mean) for a, b in zip(x, y))
    denominator = math.sqrt(sum((a - x_mean) ** 2 for a in x) * sum((b - y_mean) ** 2 for b in y))
    return numerator / denominator if denominator > 1e-12 else 0.0


def _position_variance_ratio(closes: list[float], period: int = 5) -> float:
    if len(closes) <= period + 2:
        return 1.0
    one_step = [math.log(closes[index] / closes[index - 1]) for index in range(1, len(closes)) if closes[index] > 0 and closes[index - 1] > 0]
    multi_step = [math.log(closes[index] / closes[index - period]) for index in range(period, len(closes)) if closes[index] > 0 and closes[index - period] > 0]
    one_variance = _position_std(one_step) ** 2
    multi_variance = _position_std(multi_step) ** 2
    if one_variance <= 1e-14:
        return 1.0
    return _position_clip(multi_variance / (period * one_variance), 0.15, 4.0)


def _position_clean_bars(raw_bars: Any, max_rows: int = 2500) -> list[dict[str, Any]]:
    rows_by_time: dict[int, dict[str, Any]] = {}
    for raw in raw_bars if isinstance(raw_bars, list) else []:
        if not isinstance(raw, dict):
            continue
        timestamp = int(_num_or_none(raw.get("timestamp")) or 0)
        if 0 < timestamp < 10_000_000_000:
            timestamp *= 1000
        open_price = _num_or_none(raw.get("open"))
        high_price = _num_or_none(raw.get("high"))
        low_price = _num_or_none(raw.get("low"))
        close_price = _num_or_none(raw.get("close"))
        if timestamp <= 0 or any(value is None or value <= 0 for value in (open_price, high_price, low_price, close_price)):
            continue
        assert open_price is not None and high_price is not None and low_price is not None and close_price is not None
        high_price = max(open_price, high_price, low_price, close_price)
        low_price = min(open_price, high_price, low_price, close_price)
        if high_price / low_price > 1.60:
            continue
        rows_by_time[timestamp] = {
            "timestamp": timestamp,
            "date": datetime.fromtimestamp(timestamp / 1000.0, tz=timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
            "open": float(open_price),
            "high": float(high_price),
            "low": float(low_price),
            "close": float(close_price),
            "volume": max(0.0, float(_num_or_none(raw.get("volume")) or 0.0)),
            "live": bool(raw.get("live") or raw.get("_live")),
        }
    return [rows_by_time[key] for key in sorted(rows_by_time)][-max(1, int(max_rows)):]


def _position_merge_live_quote(bars: list[dict[str, Any]], quote_payload: dict[str, Any] | None) -> list[dict[str, Any]]:
    quote_payload = quote_payload or {}
    price = _num_or_none(quote_payload.get("price") or quote_payload.get("close"))
    if price is None or price <= 0:
        return bars
    timestamp = int(_num_or_none(quote_payload.get("bar_start") or quote_payload.get("updated_at")) or 0)
    if 0 < timestamp < 10_000_000_000:
        timestamp *= 1000
    if timestamp <= 0:
        timestamp = int(time.time() * 1000)
    timestamp = timestamp // 60_000 * 60_000
    existing = next((dict(row) for row in reversed(bars) if int(row.get("timestamp") or 0) == timestamp), None)
    open_price = _num_or_none(quote_payload.get("open")) or _num_or_none((existing or {}).get("open")) or price
    high_price = max(price, _num_or_none(quote_payload.get("high")) or price, _num_or_none((existing or {}).get("high")) or price)
    low_price = min(price, _num_or_none(quote_payload.get("low")) or price, _num_or_none((existing or {}).get("low")) or price)
    row = {
        "timestamp": timestamp,
        "date": datetime.fromtimestamp(timestamp / 1000.0, tz=timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "open": float(open_price),
        "high": float(high_price),
        "low": float(low_price),
        "close": float(price),
        "volume": max(float(_num_or_none((existing or {}).get("volume")) or 0.0), float(_num_or_none(quote_payload.get("bar_volume")) or 0.0)),
        "live": True,
    }
    merged = {int(item["timestamp"]): dict(item) for item in bars}
    merged[timestamp] = row
    return [merged[key] for key in sorted(merged)][-2500:]


def _position_session_rows(bars: list[dict[str, Any]], timezone_name: str) -> list[dict[str, Any]]:
    if not bars:
        return []
    try:
        local_timezone = ZoneInfo(str(timezone_name or "UTC"))
    except Exception:
        local_timezone = timezone.utc
    latest_timestamp = int(bars[-1].get("timestamp") or 0)
    latest_day = datetime.fromtimestamp(latest_timestamp / 1000.0, tz=timezone.utc).astimezone(local_timezone).date()
    return [
        row for row in bars
        if datetime.fromtimestamp(int(row.get("timestamp") or 0) / 1000.0, tz=timezone.utc).astimezone(local_timezone).date() == latest_day
    ]


def _position_latest_engine_row(label: str) -> dict[str, Any] | None:
    lab = _tv_clean_token(label)
    now = time.time()
    with POSITION_ADVICE_LOCK:
        cached = POSITION_ENGINE_CONTEXT_CACHE.get(lab)
        if cached and now - float(cached.get("ts") or 0.0) < POSITION_ENGINE_CONTEXT_TTL_SEC:
            context = cached.get("context")
            return dict(context) if isinstance(context, dict) else None
    paths: list[Path] = []
    with LOCK:
        paths.extend(job.result_path for job in sorted(JOBS.values(), key=lambda item: item.created_at, reverse=True) if job.result_path.exists())
    try:
        paths.extend(sorted(JOBS_DIR.glob("*/results.json"), key=lambda path: path.stat().st_mtime, reverse=True)[:24])
    except Exception:
        pass
    seen: set[str] = set()
    context: dict[str, Any] | None = None
    for path in paths:
        key = str(path)
        if key in seen:
            continue
        seen.add(key)
        payload = read_json(path, {})
        rows = payload.get("results") if isinstance(payload, dict) else []
        row = next((candidate for candidate in rows or [] if _tv_clean_token((candidate or {}).get("label")) == lab and not (candidate or {}).get("error")), None)
        if row:
            try:
                age_seconds = max(0.0, now - path.stat().st_mtime)
            except Exception:
                age_seconds = None
            context = {
                "result": dict(row),
                "engine": payload.get("engine"),
                "generated_at": (row.get("audit_trail") or {}).get("generated_at") or payload.get("generated_at"),
                "age_seconds": age_seconds,
            }
            break
    with POSITION_ADVICE_LOCK:
        POSITION_ENGINE_CONTEXT_CACHE[lab] = {"ts": now, "context": context}
    return dict(context) if context else None


def _position_engine_metrics(context: dict[str, Any] | None) -> dict[str, Any]:
    row = (context or {}).get("result") or {}
    decision = row.get("professional_decision") or {}
    evidence = row.get("alpha_evidence") or {}
    summary = evidence.get("evidence_summary") or {}
    probability_up = _num_or_none(row.get("probability_up"))
    if probability_up is not None and probability_up > 1.0:
        probability_up /= 100.0
    probability_up = _position_clip(probability_up if probability_up is not None else 0.5, 0.01, 0.99)
    return {
        "available": bool(row),
        "probability_up": probability_up,
        "probability_down": 1.0 - probability_up,
        "expected_return_pct": float(_num_or_none(decision.get("expected_return_pct")) or _num_or_none(row.get("change_horizon_pct")) or 0.0),
        "calibrated_confidence": float(_num_or_none(decision.get("calibrated_confidence")) or _num_or_none(row.get("confidence")) or 0.0),
        "signal_strength": float(_num_or_none(decision.get("signal_strength")) or 0.0),
        "volatility_ann_pct": float(_num_or_none(decision.get("expected_volatility_ann_pct")) or _num_or_none(row.get("volatility_ann_pct")) or 0.0),
        "data_quality": float(_num_or_none((row.get("data_quality") or {}).get("score")) or 0.0),
        "news_score": float(_num_or_none((row.get("news") or {}).get("score")) or 0.0),
        "model_agreement": bool((row.get("model") or {}).get("agreement")),
        "directional_accuracy": _num_or_none(summary.get("selected_directional_accuracy")),
        "hit_rate": _num_or_none(summary.get("selected_hit_rate")),
        "brier_score": _num_or_none(summary.get("selected_brier_score")),
        "sample_count": _num_or_none(summary.get("samples")),
        "regime": (decision.get("regime") or {}).get("name") or row.get("risk") or "unknown",
        "source_age_seconds": (context or {}).get("age_seconds"),
        "generated_at": (context or {}).get("generated_at"),
    }


def _position_return_pct(closes: list[float], periods: int) -> float:
    if len(closes) <= periods or closes[-1 - periods] <= 0:
        return 0.0
    return (closes[-1] / closes[-1 - periods] - 1.0) * 100.0


def _position_intraday_with_budget(label: str, item: dict[str, Any]) -> dict[str, Any]:
    """Fetch one-minute bars without allowing a slow upstream to block the UI."""
    lab = _tv_clean_token(label)
    with POSITION_INTRADAY_LOCK:
        pending = POSITION_INTRADAY_PENDING.get(lab)
        if pending is None:
            pending = {"event": threading.Event(), "payload": None}
            POSITION_INTRADAY_PENDING[lab] = pending

            def load() -> None:
                try:
                    payload = fetch_intraday_payload(lab, item, "1m", "5d")
                except Exception as exc:
                    payload = {
                        "ok": False,
                        "label": lab,
                        "error": f"intraday fetch failed: {exc}",
                        "bars": [],
                        "retryable": True,
                    }
                with POSITION_INTRADAY_LOCK:
                    pending["payload"] = payload
                    if payload.get("ok") and payload.get("bars"):
                        POSITION_INTRADAY_LAST_GOOD[lab] = {"ts": time.time(), "payload": dict(payload)}
                        while len(POSITION_INTRADAY_LAST_GOOD) > 32:
                            oldest = min(
                                POSITION_INTRADAY_LAST_GOOD,
                                key=lambda key: float(POSITION_INTRADAY_LAST_GOOD[key].get("ts") or 0.0),
                            )
                            POSITION_INTRADAY_LAST_GOOD.pop(oldest, None)
                    POSITION_INTRADAY_PENDING.pop(lab, None)
                    pending["event"].set()

            thread = threading.Thread(target=load, name=f"position-bars-{lab}", daemon=True)
            thread.start()
        event = pending["event"]
    event.wait(max(0.25, min(4.25, POSITION_INTRADAY_BUDGET_SEC)))
    payload = pending.get("payload")
    if isinstance(payload, dict):
        return dict(payload)
    with POSITION_INTRADAY_LOCK:
        fallback = POSITION_INTRADAY_LAST_GOOD.get(lab)
        fallback_payload = dict((fallback or {}).get("payload") or {})
    if fallback_payload.get("bars"):
        fallback_payload.update({
            "ok": True,
            "cached": True,
            "stale": True,
            "delayed": True,
            "source": f"{fallback_payload.get('source') or 'Intraday'} · bounded cache fallback",
            "budget_timeout": True,
        })
        return fallback_payload
    return {
        "ok": False,
        "label": lab,
        "error": "The upstream intraday feed exceeded the 3.8 second UI budget; cache warming continues in the background.",
        "bars": [],
        "retryable": True,
        "budget_timeout": True,
    }


def _position_regular_session_profile(item: dict[str, Any]) -> dict[str, Any]:
    exchange = _tv_clean_token(
        item.get("tv_exchange") or item.get("original_exchange") or item.get("exchange")
    )
    label = _tv_clean_token(item.get("label"))
    if label == "BTCUSD" or exchange in {"COINBASE", "BINANCE", "KRAKEN"}:
        return {"timezone": "UTC", "windows": [(0, 1440)], "weekdays": set(range(7)), "name": "continuous 24/7"}
    if exchange in US_EXCHANGES or exchange in {
        "INDEXCBOE", "INDEXDJX", "INDEXNASDAQ", "INDEXNYSEGIS", "INDEXSP", "ICE"
    }:
        # Hour bars may be stamped at 09:00 for the 09:30-09:59 opening bucket.
        return {"timezone": "America/New_York", "windows": [(9 * 60, 16 * 60)], "weekdays": {0, 1, 2, 3, 4}, "name": "US regular session"}
    if exchange in {"EPA", "EURONEXT", "AMS", "INDEXEURO"}:
        return {"timezone": "Europe/Paris", "windows": [(9 * 60, 17 * 60 + 30)], "weekdays": {0, 1, 2, 3, 4}, "name": "Euronext regular session"}
    if exchange in {"LON", "LSE", "INDEXFTSE"}:
        return {"timezone": "Europe/London", "windows": [(8 * 60, 16 * 60 + 30)], "weekdays": {0, 1, 2, 3, 4}, "name": "London regular session"}
    if exchange in {"HKG", "HKEX", "INDEXHANGSENG"}:
        return {"timezone": "Asia/Hong_Kong", "windows": [(9 * 60, 12 * 60), (13 * 60, 16 * 60)], "weekdays": {0, 1, 2, 3, 4}, "name": "Hong Kong regular sessions"}
    if exchange in {"TPE", "TWSE"}:
        return {"timezone": "Asia/Taipei", "windows": [(9 * 60, 13 * 60 + 30)], "weekdays": {0, 1, 2, 3, 4}, "name": "Taiwan regular session"}
    if exchange in {"KRX", "KOSPI"}:
        return {"timezone": "Asia/Seoul", "windows": [(9 * 60, 15 * 60 + 30)], "weekdays": {0, 1, 2, 3, 4}, "name": "Korea regular session"}
    if exchange in {"TADAWUL", "SAUDI"}:
        return {"timezone": "Asia/Riyadh", "windows": [(10 * 60, 15 * 60 + 30)], "weekdays": {6, 0, 1, 2, 3}, "name": "Tadawul regular session"}
    return {"timezone": "UTC", "windows": None, "weekdays": {0, 1, 2, 3, 4}, "name": "provider regular session"}


def _position_filter_regular_hourly(
    bars: list[dict[str, Any]], item: dict[str, Any]
) -> tuple[list[dict[str, Any]], dict[str, Any], int]:
    profile = _position_regular_session_profile(item)
    try:
        local_timezone = ZoneInfo(profile["timezone"])
    except Exception:
        local_timezone = timezone.utc
    filtered: list[dict[str, Any]] = []
    removed = 0
    for row in bars:
        timestamp = int(_num_or_none(row.get("timestamp")) or 0)
        if timestamp <= 0:
            removed += 1
            continue
        local_dt = datetime.fromtimestamp(timestamp / 1000.0, tz=timezone.utc).astimezone(local_timezone)
        if local_dt.date() < POSITION_LONG_START.date() or local_dt.weekday() not in profile["weekdays"]:
            removed += 1
            continue
        windows = profile.get("windows")
        minute = local_dt.hour * 60 + local_dt.minute
        if windows and not any(start <= minute < end for start, end in windows):
            removed += 1
            continue
        cleaned = dict(row)
        cleaned["session_date"] = local_dt.date().isoformat()
        cleaned["session_time"] = local_dt.strftime("%H:%M")
        filtered.append(cleaned)
    return filtered, profile, removed


def _filter_regular_intraday_rows(
    bars: list[dict[str, Any]], item: dict[str, Any]
) -> tuple[list[dict[str, Any]], dict[str, Any], int]:
    """Keep only exchange sessions so archive pages stay compact and gap-safe."""
    profile = _position_regular_session_profile(item)
    try:
        local_timezone = ZoneInfo(profile["timezone"])
    except Exception:
        local_timezone = timezone.utc
    windows = profile.get("windows")
    if profile.get("timezone") == "America/New_York" and windows:
        windows = [(9 * 60 + 30, 16 * 60)]
    filtered: list[dict[str, Any]] = []
    removed = 0
    for row in bars:
        timestamp = int(_num_or_none(row.get("timestamp")) or 0)
        if timestamp <= 0:
            removed += 1
            continue
        local_dt = datetime.fromtimestamp(timestamp / 1000.0, tz=timezone.utc).astimezone(local_timezone)
        minute = local_dt.hour * 60 + local_dt.minute
        if local_dt.weekday() not in profile["weekdays"] or (
            windows and not any(start <= minute < end for start, end in windows)
        ):
            removed += 1
            continue
        cleaned = dict(row)
        cleaned["session_date"] = local_dt.date().isoformat()
        cleaned["session_time"] = local_dt.strftime("%H:%M")
        filtered.append(cleaned)
    return filtered, {**profile, "windows": windows}, removed


def fetch_intraday_archive_payload(
    label: str,
    item: dict[str, Any] | None,
    start_ms: Any,
    end_ms: Any,
) -> dict[str, Any]:
    """Return one bounded 1m archive page without coupling it to forecast runs."""
    lab = _tv_clean_token(label)
    if not lab:
        return {"ok": False, "error": "missing label", "bars": []}
    now_ms = int(time.time() * 1000)
    minute_ms = 60_000
    try:
        requested_start = int(float(start_ms)) // minute_ms * minute_ms
        requested_end = int(float(end_ms)) // minute_ms * minute_ms
    except Exception:
        return {"ok": False, "label": lab, "error": "invalid archive window", "bars": []}
    earliest = now_ms - INTRADAY_ARCHIVE_LOOKBACK_DAYS * 86_400_000
    requested_start = max(earliest, requested_start)
    requested_end = min(now_ms + minute_ms, requested_end)
    max_window_ms = INTRADAY_ARCHIVE_MAX_WINDOW_DAYS * 86_400_000
    if requested_end <= requested_start or requested_end - requested_start > max_window_ms:
        return {
            "ok": False,
            "label": lab,
            "error": f"archive pages must cover at most {INTRADAY_ARCHIVE_MAX_WINDOW_DAYS} days",
            "bars": [],
        }

    watch_by_label = {_tv_clean_token(row.get("label")): row for row in load_watchlist()}
    watch = watch_by_label.get(lab, {})
    request_item = {"label": lab, **watch, **(item or {})}
    candidates = _yahoo_symbol_candidates(request_item, watch_by_label)
    cache_key = f"{lab}|1m|{requested_start}|{requested_end}|{'|'.join(candidates) or 'direct'}"
    now = time.time()
    stale_payload: dict[str, Any] | None = None
    with INTRADAY_ARCHIVE_LOCK:
        cached = INTRADAY_ARCHIVE_CACHE.get(cache_key)
        cached_payload = cached.get("payload") if cached else None
        if (cached_payload or {}).get("ok") and (cached_payload or {}).get("bars"):
            stale_payload = dict(cached_payload)
        ttl = INTRADAY_ARCHIVE_TTL_SEC if (cached_payload or {}).get("ok") else INTRADAY_ARCHIVE_ERROR_TTL_SEC
        if cached and now - float(cached.get("ts") or 0.0) < ttl:
            payload = dict(cached_payload or {})
            payload["cached"] = True
            return payload

    start_at = datetime.fromtimestamp(requested_start / 1000.0, tz=timezone.utc)
    end_at = datetime.fromtimestamp(requested_end / 1000.0, tz=timezone.utc)
    errors: list[str] = []
    payload, alpaca_errors = _alpaca_intraday_payload(
        lab,
        request_item,
        "1m",
        "archive",
        start_at=start_at,
        end_at=end_at,
        max_bars=INTRADAY_ARCHIVE_MAX_BARS_PER_PAGE,
    )
    errors.extend(alpaca_errors)

    if payload is None and requested_end >= now_ms - 31 * 86_400_000:
        for symbol in candidates[:3]:
            try:
                raw = _yahoo_intraday_window_chart(symbol, start_at, end_at)
                chart = raw.get("chart") or {}
                results = chart.get("result") or []
                if chart.get("error") or not results:
                    errors.append(f"{symbol}: {chart.get('error') or 'empty archive page'}")
                    continue
                result = results[0]
                bars, rejected = _clean_intraday_result(result)
                if not bars:
                    errors.append(f"{symbol}: no valid one-minute archive bars")
                    continue
                meta = result.get("meta") or {}
                payload = {
                    "ok": True,
                    "label": lab,
                    "symbol": symbol,
                    "interval": "1m",
                    "range": "archive",
                    "source": "Yahoo Finance 1m archive page",
                    "exchange": meta.get("fullExchangeName") or meta.get("exchangeName") or watch.get("exchange"),
                    "timezone": meta.get("exchangeTimezoneName") or "UTC",
                    "currency": meta.get("currency"),
                    "market_open": False,
                    "delayed": True,
                    "updated_at": int(bars[-1]["timestamp"]),
                    "bars": bars,
                    "bars_returned": len(bars),
                    "rejected_bars": rejected,
                    "cached": False,
                }
                break
            except Exception as exc:
                errors.append(f"{symbol}: {exc}")

    if payload is not None:
        regular_rows, profile, removed = _filter_regular_intraday_rows(payload.get("bars") or [], request_item)
        if regular_rows:
            payload = {
                **payload,
                "bars": regular_rows[-INTRADAY_ARCHIVE_MAX_BARS_PER_PAGE:],
                "bars_returned": min(len(regular_rows), INTRADAY_ARCHIVE_MAX_BARS_PER_PAGE),
                "archive": True,
                "requested_start": requested_start,
                "requested_end": requested_end,
                "coverage_start": int(regular_rows[0]["timestamp"]),
                "coverage_end": int(regular_rows[-1]["timestamp"]),
                "session": profile.get("name"),
                "off_session_bars_removed": removed,
                "fallback_errors": errors,
            }
        else:
            errors.append("archive source returned no regular-session bars")
            payload = None

    if payload is None and stale_payload is not None:
        payload = {
            **stale_payload,
            "cached": True,
            "stale": True,
            "fallback_errors": errors,
        }
    if payload is None:
        payload = {
            "ok": False,
            "label": lab,
            "interval": "1m",
            "range": "archive",
            "archive": True,
            "lod_only": True,
            "requested_start": requested_start,
            "requested_end": requested_end,
            "error": "exact one-minute archive page unavailable; hourly overview retained",
            "errors": errors,
            "bars": [],
            "cached": False,
        }

    with INTRADAY_ARCHIVE_LOCK:
        INTRADAY_ARCHIVE_CACHE[cache_key] = {"ts": now, "payload": payload}
        while len(INTRADAY_ARCHIVE_CACHE) > max(4, INTRADAY_ARCHIVE_CACHE_MAX_ENTRIES):
            oldest_key = min(INTRADAY_ARCHIVE_CACHE, key=lambda key: float(INTRADAY_ARCHIVE_CACHE[key].get("ts") or 0.0))
            INTRADAY_ARCHIVE_CACHE.pop(oldest_key, None)
    return payload


def _position_alpaca_long_hourly(
    label: str, item: dict[str, Any]
) -> tuple[dict[str, Any] | None, list[str]]:
    key = os.environ.get("APCA_API_KEY_ID") or os.environ.get("ALPACA_API_KEY") or ""
    secret = os.environ.get("APCA_API_SECRET_KEY") or os.environ.get("ALPACA_API_SECRET") or ""
    if not key or not secret or _credential_issue(key, "Alpaca key") or _credential_issue(secret, "Alpaca secret"):
        return None, []
    exchange = _tv_clean_token(
        item.get("tv_exchange") or item.get("original_exchange") or item.get("exchange")
    )
    symbol = str(
        item.get("tv_symbol") or item.get("original_symbol") or item.get("symbol") or label
    ).strip().upper()
    if exchange not in US_EXCHANGES or label in STREAM_INDEX_LABELS or not symbol or symbol.startswith("."):
        return None, []
    feed = str(os.environ.get("APEX_ALPACA_HISTORY_FEED") or os.environ.get("APEX_ALPACA_FEED") or "iex").lower()
    if feed not in {"iex", "sip", "delayed_sip"}:
        feed = "iex"
    params = {
        "timeframe": "1Hour",
        "start": POSITION_LONG_START.isoformat(timespec="seconds").replace("+00:00", "Z"),
        "end": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "limit": "10000",
        "adjustment": "all",
        "feed": feed,
        # Newest-first keeps the graph current even if an upstream page fails.
        # Pagination still continues to the requested 2016 boundary below.
        "sort": "desc",
    }
    rows_by_timestamp: dict[int, dict[str, Any]] = {}
    page_token = ""
    seen_tokens: set[str] = set()
    errors: list[str] = []
    pages_fetched = 0
    raw_bars_seen = 0
    off_session_bars_removed = 0
    pagination_complete = False
    for _ in range(POSITION_LONG_MAX_PAGES):
        request_params = dict(params)
        if page_token:
            request_params["page_token"] = page_token
        request = Request(
            f"https://data.alpaca.markets/v2/stocks/{quote(symbol, safe='.-')}/bars?{urlencode(request_params)}",
            headers={
                "Accept": "application/json",
                "APCA-API-KEY-ID": key,
                "APCA-API-SECRET-KEY": secret,
                "User-Agent": "ApexMarketPredictor/15.0 long-edge-history",
            },
            method="GET",
        )
        try:
            with urlopen(request, timeout=min(2.0, max(0.8, INTRADAY_TIMEOUT))) as response:
                raw = json.loads(response.read().decode("utf-8", "replace") or "{}")
        except Exception as exc:
            message = f"Alpaca {symbol} long history: {exc}"
            if not rows_by_timestamp:
                return None, [message]
            errors.append(message)
            break
        page_rows: list[dict[str, Any]] = []
        for source_row in raw.get("bars") or []:
            timestamp = _stream_timestamp_ms(source_row.get("t"), 0)
            page_rows.append({
                "timestamp": timestamp,
                "open": source_row.get("o"),
                "high": source_row.get("h"),
                "low": source_row.get("l"),
                "close": source_row.get("c"),
                "volume": source_row.get("v"),
            })
        pages_fetched += 1
        raw_bars_seen += len(page_rows)
        page_cleaned = _position_clean_bars(page_rows, max(1, len(page_rows)))
        page_regular, _, page_removed = _position_filter_regular_hourly(
            page_cleaned, {**item, "label": label}
        )
        off_session_bars_removed += page_removed
        for row in page_regular:
            rows_by_timestamp[int(row["timestamp"])] = row
        next_page_token = str(raw.get("next_page_token") or "")
        if not next_page_token:
            page_token = ""
            pagination_complete = True
            break
        if next_page_token in seen_tokens:
            errors.append("Alpaca returned a repeated pagination token")
            page_token = next_page_token
            break
        seen_tokens.add(next_page_token)
        page_token = next_page_token
    if page_token:
        errors.append(
            f"Alpaca hourly history did not complete inside the guarded "
            f"{POSITION_LONG_MAX_PAGES * 10000:,}-bar pagination ceiling"
        )
    cleaned = [rows_by_timestamp[key] for key in sorted(rows_by_timestamp)]
    regular_history_truncated = len(cleaned) > POSITION_LONG_MAX_BARS
    if regular_history_truncated:
        cleaned = cleaned[-POSITION_LONG_MAX_BARS:]
        errors.append(
            f"Regular-session history exceeded APEX_POSITION_LONG_MAX_BARS={POSITION_LONG_MAX_BARS}"
        )
        pagination_complete = False
    if not cleaned:
        return None, errors or [f"Alpaca {symbol}: no validated hourly history"]
    return {
        "ok": True,
        "label": label,
        "symbol": symbol,
        "interval": "1h",
        "source": f"Alpaca {feed.upper()} adjusted 1h bars",
        "exchange": exchange,
        "timezone": "America/New_York",
        "currency": item.get("currency") or "USD",
        "market_open": False,
        "delayed": feed == "delayed_sip",
        "updated_at": int(cleaned[-1]["timestamp"]),
        "bars": cleaned,
        "bars_returned": len(cleaned),
        "coverage_authority": (
            "Alpaca pagination completed from the requested 2016 boundary"
            if pagination_complete
            else "Alpaca partial hourly history; pagination did not reach the requested boundary"
        ),
        "coverage_pagination_complete": pagination_complete,
        "pagination_pages": pages_fetched,
        "raw_bars_seen": raw_bars_seen,
        "off_session_bars_removed": off_session_bars_removed,
        "source_errors": errors,
    }, errors


def _position_fetch_long_hourly(label: str, item: dict[str, Any]) -> dict[str, Any]:
    lab = _tv_clean_token(label)
    watch_by_label = {_tv_clean_token(row.get("label")): row for row in load_watchlist()}
    errors: list[str] = []
    payload, alpaca_errors = _position_alpaca_long_hourly(lab, item)
    errors.extend(alpaca_errors)
    if payload is None:
        payload, tv_errors = _tradingview_intraday_fallback(lab, item, watch_by_label, "1h")
        errors.extend(tv_errors)
    if payload is None:
        for symbol in _yahoo_symbol_candidates(item, watch_by_label)[:2]:
            try:
                raw = _yahoo_intraday_chart(symbol, "1h", "1y")
                results = (raw.get("chart") or {}).get("result") or []
                if not results:
                    continue
                result = results[0]
                bars, rejected = _clean_intraday_result(result)
                meta = result.get("meta") or {}
                payload = {
                    "ok": True,
                    "label": lab,
                    "symbol": symbol,
                    "interval": "1h",
                    "source": "Yahoo Finance 1h bounded fallback",
                    "exchange": meta.get("fullExchangeName") or item.get("exchange"),
                    "timezone": meta.get("exchangeTimezoneName") or "UTC",
                    "currency": meta.get("currency"),
                    "market_open": False,
                    "delayed": True,
                    "updated_at": int(bars[-1]["timestamp"]) if bars else 0,
                    "bars": bars,
                    "bars_returned": len(bars),
                    "rejected_bars": rejected,
                }
                break
            except Exception as exc:
                errors.append(f"Yahoo {symbol} long fallback: {exc}")
    if payload is None:
        return {
            "ok": False,
            "label": lab,
            "error": "No validated regular-session hourly history is currently available.",
            "errors": errors,
            "bars": [],
            "retryable": True,
        }
    source_is_alpaca = str(payload.get("source") or "").startswith("Alpaca")
    source_rows = payload.get("bars") or []
    clean_limit = max(POSITION_LONG_MAX_BARS, len(source_rows)) if source_is_alpaca else POSITION_LONG_MAX_BARS
    cleaned = _position_clean_bars(source_rows, clean_limit)
    regular_bars, profile, removed = _position_filter_regular_hourly(cleaned, {**item, "label": lab})
    regular_history_truncated = len(regular_bars) > POSITION_LONG_MAX_BARS
    if regular_history_truncated:
        regular_bars = regular_bars[-POSITION_LONG_MAX_BARS:]
        errors.append(
            f"Regular-session history exceeded APEX_POSITION_LONG_MAX_BARS={POSITION_LONG_MAX_BARS}"
        )
    if len(regular_bars) < 250:
        return {
            "ok": False,
            "label": lab,
            "error": "Fewer than 250 validated regular-session hourly bars are available for Long edge.",
            "errors": errors,
            "bars": [],
            "retryable": True,
        }
    first_date = str(regular_bars[0].get("session_date") or regular_bars[0]["date"][:10])
    last_date = str(regular_bars[-1].get("session_date") or regular_bars[-1]["date"][:10])
    authoritative_since_2016 = source_is_alpaca and bool(payload.get("coverage_pagination_complete"))
    coverage_complete = (
        authoritative_since_2016 or (not source_is_alpaca and first_date <= "2016-01-15")
    ) and not regular_history_truncated
    return {
        **payload,
        "ok": True,
        "mode": "long",
        "interval": "1h",
        "range": "2016-to-present",
        "timezone": profile["timezone"],
        "session_profile": profile["name"],
        "bars": regular_bars,
        "bars_returned": len(regular_bars),
        "closed_period_bars_removed": removed,
        "coverage": {
            "requested_start": "2016-01-01",
            "start": first_date,
            "end": last_date,
            "complete_from_2016_or_first_available": coverage_complete,
            "basis": payload.get("coverage_authority") or "first regular-session bar returned by the best available source",
            "pagination_pages": int(payload.get("pagination_pages") or 0),
        },
        "source_errors": errors,
    }


def _position_long_history_with_budget(label: str, item: dict[str, Any]) -> dict[str, Any]:
    lab = _tv_clean_token(label)
    now = time.time()
    with POSITION_LONG_LOCK:
        cached = POSITION_LONG_HISTORY_CACHE.get(lab)
        if cached and now - float(cached.get("ts") or 0.0) < POSITION_LONG_HISTORY_TTL_SEC:
            payload = dict(cached.get("payload") or {})
            payload["cached"] = True
            return payload
        pending = POSITION_LONG_PENDING.get(lab)
        if pending is None:
            pending = {"event": threading.Event(), "payload": None}
            POSITION_LONG_PENDING[lab] = pending

            def load() -> None:
                try:
                    loaded = _position_fetch_long_hourly(lab, item)
                except Exception as exc:
                    loaded = {
                        "ok": False,
                        "label": lab,
                        "error": f"Long-edge hourly history failed: {exc}",
                        "bars": [],
                        "retryable": True,
                    }
                with POSITION_LONG_LOCK:
                    pending["payload"] = loaded
                    if loaded.get("ok") and loaded.get("bars"):
                        POSITION_LONG_HISTORY_CACHE[lab] = {"ts": time.time(), "payload": dict(loaded)}
                        while len(POSITION_LONG_HISTORY_CACHE) > 8:
                            oldest = min(
                                POSITION_LONG_HISTORY_CACHE,
                                key=lambda key: float(POSITION_LONG_HISTORY_CACHE[key].get("ts") or 0.0),
                            )
                            POSITION_LONG_HISTORY_CACHE.pop(oldest, None)
                    POSITION_LONG_PENDING.pop(lab, None)
                    pending["event"].set()

            threading.Thread(target=load, name=f"position-long-{lab}", daemon=True).start()
        event = pending["event"]
        stale_payload = dict((cached or {}).get("payload") or {})
    if stale_payload.get("bars"):
        stale_payload.update({"cached": True, "stale": True, "refreshing": True})
        return stale_payload
    event.wait(max(0.25, min(4.25, POSITION_LONG_BUDGET_SEC)))
    payload = pending.get("payload")
    if isinstance(payload, dict):
        return dict(payload)
    return {
        "ok": False,
        "label": lab,
        "error": "The first Long-edge history load exceeded the 3.8 second UI budget; loading continues in the background.",
        "bars": [],
        "retryable": True,
        "budget_timeout": True,
    }


def _position_sma(values: list[float], period: int) -> float:
    sample = [float(value) for value in values[-max(1, int(period)):] if math.isfinite(float(value))]
    return _position_mean(sample)


def _position_wilson_interval(successes: int, samples: int, z_score: float = 1.645) -> tuple[float, float]:
    if samples <= 0:
        return 0.0, 1.0
    probability = _position_clip(successes / samples, 0.0, 1.0)
    z_squared = z_score * z_score
    denominator = 1.0 + z_squared / samples
    center = (probability + z_squared / (2.0 * samples)) / denominator
    radius = z_score * math.sqrt(
        probability * (1.0 - probability) / samples + z_squared / (4.0 * samples * samples)
    ) / denominator
    return _position_clip(center - radius, 0.0, 1.0), _position_clip(center + radius, 0.0, 1.0)


def _position_daily_from_hourly(
    bars: list[dict[str, Any]], timezone_name: str
) -> list[dict[str, Any]]:
    try:
        local_timezone = ZoneInfo(str(timezone_name or "UTC"))
    except Exception:
        local_timezone = timezone.utc
    sessions: dict[str, dict[str, Any]] = {}
    for row in bars:
        timestamp = int(_num_or_none(row.get("timestamp")) or 0)
        if timestamp <= 0:
            continue
        session_date = str(row.get("session_date") or "")
        if not session_date:
            session_date = datetime.fromtimestamp(
                timestamp / 1000.0, tz=timezone.utc
            ).astimezone(local_timezone).date().isoformat()
        open_price = float(row["open"])
        high_price = float(row["high"])
        low_price = float(row["low"])
        close_price = float(row["close"])
        volume = max(0.0, float(_num_or_none(row.get("volume")) or 0.0))
        session = sessions.get(session_date)
        if session is None:
            sessions[session_date] = {
                "date": session_date,
                "timestamp": timestamp,
                "open": open_price,
                "high": high_price,
                "low": low_price,
                "close": close_price,
                "volume": volume,
                "hourly_bars": 1,
            }
            continue
        session["high"] = max(float(session["high"]), high_price)
        session["low"] = min(float(session["low"]), low_price)
        session["close"] = close_price
        session["volume"] = float(session["volume"]) + volume
        session["hourly_bars"] = int(session["hourly_bars"]) + 1
    return [sessions[key] for key in sorted(sessions)]


def _position_merge_long_quote(
    bars: list[dict[str, Any]], quote_payload: dict[str, Any] | None, item: dict[str, Any]
) -> list[dict[str, Any]]:
    quote_payload = quote_payload or {}
    price = _num_or_none(quote_payload.get("price") or quote_payload.get("close"))
    if price is None or price <= 0 or quote_payload.get("market_open") is False:
        return [dict(row) for row in bars]
    timestamp = int(_num_or_none(quote_payload.get("bar_start") or quote_payload.get("updated_at")) or 0)
    if 0 < timestamp < 10_000_000_000:
        timestamp *= 1000
    if timestamp <= 0:
        timestamp = int(time.time() * 1000)
    profile = _position_regular_session_profile(item)
    try:
        local_timezone = ZoneInfo(profile["timezone"])
    except Exception:
        local_timezone = timezone.utc
    local_dt = datetime.fromtimestamp(timestamp / 1000.0, tz=timezone.utc).astimezone(local_timezone)
    local_hour = local_dt.replace(minute=0, second=0, microsecond=0)
    minute = local_dt.hour * 60 + local_dt.minute
    windows = profile.get("windows")
    if local_dt.weekday() not in profile["weekdays"] or (
        windows and not any(start <= minute < end for start, end in windows)
    ):
        return [dict(row) for row in bars]
    bucket_timestamp = int(local_hour.astimezone(timezone.utc).timestamp() * 1000)
    existing = next(
        (dict(row) for row in reversed(bars) if int(row.get("timestamp") or 0) == bucket_timestamp),
        None,
    )
    open_price = _num_or_none(quote_payload.get("open")) or _num_or_none((existing or {}).get("open")) or price
    high_price = max(
        price,
        _num_or_none(quote_payload.get("high")) or price,
        _num_or_none((existing or {}).get("high")) or price,
    )
    low_price = min(
        price,
        _num_or_none(quote_payload.get("low")) or price,
        _num_or_none((existing or {}).get("low")) or price,
    )
    row = {
        "timestamp": bucket_timestamp,
        "date": datetime.fromtimestamp(bucket_timestamp / 1000.0, tz=timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "open": float(open_price),
        "high": float(high_price),
        "low": float(low_price),
        "close": float(price),
        "volume": max(
            float(_num_or_none((existing or {}).get("volume")) or 0.0),
            float(_num_or_none(quote_payload.get("bar_volume")) or 0.0),
        ),
        "live": True,
        "session_date": local_dt.date().isoformat(),
        "session_time": local_hour.strftime("%H:%M"),
    }
    merged = {int(source["timestamp"]): dict(source) for source in bars}
    merged[bucket_timestamp] = row
    return [merged[key] for key in sorted(merged)][-max(1, POSITION_LONG_MAX_BARS):]


def _position_long_state(daily: list[dict[str, Any]], index: int) -> dict[str, float]:
    closes = [float(row["close"]) for row in daily[:index + 1]]
    volumes = [float(row.get("volume") or 0.0) for row in daily[:index + 1]]
    close = closes[-1]

    def change(period: int) -> float:
        return close / closes[-1 - period] - 1.0 if len(closes) > period and closes[-1 - period] > 0 else 0.0

    log_returns = [
        math.log(closes[position] / closes[position - 1])
        for position in range(max(1, len(closes) - 60), len(closes))
        if closes[position] > 0 and closes[position - 1] > 0
    ]
    high_watermark = max(closes[-252:]) if closes else close
    median_volume = _position_quantile(volumes[-21:-1], 0.5) if len(volumes) > 1 else 0.0
    return {
        "momentum_20": change(20),
        "momentum_60": change(60),
        "momentum_126": change(126),
        "momentum_252": change(252),
        "trend_50": close / max(_position_sma(closes, 50), 1e-12) - 1.0,
        "trend_200": close / max(_position_sma(closes, 200), 1e-12) - 1.0,
        "volatility_60": _position_std(log_returns) * math.sqrt(252.0),
        "drawdown_252": close / max(high_watermark, 1e-12) - 1.0,
        "volume_ratio": volumes[-1] / median_volume if median_volume > 0 else 1.0,
        "rsi_14": _position_rsi(closes, 14),
        "variance_ratio_5": _position_variance_ratio(closes[-160:], 5),
    }


def _position_long_analog_evidence(daily: list[dict[str, Any]]) -> dict[str, Any]:
    if len(daily) < 320:
        return {"available": False, "sample_count": 0, "horizons": [], "reason": "fewer than 320 daily sessions"}
    current = _position_long_state(daily, len(daily) - 1)
    scales = {
        "momentum_20": 0.10,
        "momentum_60": 0.18,
        "momentum_126": 0.28,
        "momentum_252": 0.42,
        "trend_50": 0.12,
        "trend_200": 0.25,
        "volatility_60": 0.20,
        "drawdown_252": 0.22,
        "rsi_14": 22.0,
        "variance_ratio_5": 0.75,
    }
    candidates: list[tuple[float, int]] = []
    for index in range(252, len(daily) - 20):
        state = _position_long_state(daily, index)
        distance = 0.0
        for name, scale in scales.items():
            delta = (state[name] - current[name]) / max(scale, 1e-9)
            distance += delta * delta
        candidates.append((math.sqrt(distance / len(scales)), index))
    nearest = sorted(candidates)[:160]
    horizon_results: list[dict[str, Any]] = []
    internal_samples: dict[int, dict[str, list[float]]] = {}
    annual_horizons = list(range(252, max(253, len(daily) - 275), 252))
    for horizon in sorted({20, 60, 126, *annual_horizons}):
        returns: list[float] = []
        favorable: list[float] = []
        adverse: list[float] = []
        for _, index in nearest:
            if index + horizon >= len(daily) - 1:
                continue
            entry = float(daily[index]["close"])
            future = daily[index + 1:index + horizon + 1]
            if entry <= 0 or len(future) < horizon:
                continue
            returns.append(float(future[-1]["close"]) / entry - 1.0)
            favorable.append(max(float(row["high"]) for row in future) / entry - 1.0)
            adverse.append(min(float(row["low"]) for row in future) / entry - 1.0)
        if len(returns) < 24:
            continue
        hit_rate = sum(value > 0 for value in returns) / len(returns)
        median_return = _position_quantile(returns, 0.50)
        downside = _position_quantile(returns, 0.20)
        annual_factor = 252.0 / max(1, horizon)
        annualized_median = (max(0.001, 1.0 + median_return) ** annual_factor) - 1.0
        annualized_downside = (max(0.001, 1.0 + downside) ** annual_factor) - 1.0
        robust_utility = annualized_median - 0.45 * abs(min(0.0, annualized_downside)) + (hit_rate - 0.50) * 0.08
        result = {
            "horizon_days": horizon,
            "sample_count": len(returns),
            "terminal_hit_rate": hit_rate,
            "median_return": median_return,
            "return_q20": downside,
            "return_q80": _position_quantile(returns, 0.80),
            "annualized_median_return": annualized_median,
            "mfe_q40": max(0.0, _position_quantile(favorable, 0.40)),
            "mfe_q70": max(0.0, _position_quantile(favorable, 0.70)),
            "mae_q20": min(0.0, _position_quantile(adverse, 0.20)),
            "robust_utility": robust_utility,
        }
        horizon_results.append(result)
        internal_samples[horizon] = {"returns": returns, "favorable": favorable, "adverse": adverse}
    if not horizon_results:
        return {"available": False, "sample_count": 0, "horizons": [], "reason": "insufficient completed analog outcomes"}
    selected = max(
        horizon_results,
        key=lambda row: (float(row["robust_utility"]), -int(row["horizon_days"])),
    )
    samples = internal_samples[int(selected["horizon_days"])]
    return {
        "available": True,
        "current_state": current,
        "nearest_states": len(nearest),
        "sample_count": int(selected["sample_count"]),
        "selected": selected,
        "horizons": horizon_results,
        "_returns": samples["returns"],
        "_favorable": samples["favorable"],
        "_adverse": samples["adverse"],
    }


def _position_build_long_advice(
    label: str,
    item: dict[str, Any],
    hourly: dict[str, Any],
    engine_context: dict[str, Any] | None,
    quote_payload: dict[str, Any] | None,
    account_value: float,
    risk_percent: float,
) -> dict[str, Any]:
    started = time.perf_counter()
    if not hourly.get("ok"):
        return {
            "ok": False,
            "label": label,
            "edge": "long",
            "error": hourly.get("error") or "Long-edge hourly history is unavailable.",
            "retryable": bool(hourly.get("retryable", True)),
            "budget_timeout": bool(hourly.get("budget_timeout")),
        }
    bars = _position_clean_bars(hourly.get("bars") or [], POSITION_LONG_MAX_BARS)
    bars, profile, _ = _position_filter_regular_hourly(bars, {**item, "label": label})
    bars = _position_merge_long_quote(bars, quote_payload, {**item, "label": label})
    daily = _position_daily_from_hourly(bars, profile["timezone"])
    if len(bars) < 250 or len(daily) < 80:
        return {
            "ok": False,
            "label": label,
            "edge": "long",
            "error": "Long edge needs at least 250 regular-session hourly bars and 80 completed sessions.",
            "retryable": True,
        }
    closes = [float(row["close"]) for row in daily]
    highs = [float(row["high"]) for row in daily]
    lows = [float(row["low"]) for row in daily]
    volumes = [float(row.get("volume") or 0.0) for row in daily]
    quote_price = _num_or_none((quote_payload or {}).get("price"))
    current_price = float(quote_price if quote_price is not None and quote_price > 0 else closes[-1])
    closes[-1] = current_price
    sma20 = _position_sma(closes, 20)
    sma50 = _position_sma(closes, 50)
    sma200 = _position_sma(closes, 200)
    ema50 = _position_ema(closes, 50)
    rsi = _position_rsi(closes, 14)
    true_ranges: list[float] = []
    for index, row in enumerate(daily):
        previous_close = float(daily[index - 1]["close"]) if index else float(row["open"])
        true_ranges.append(max(
            float(row["high"]) - float(row["low"]),
            abs(float(row["high"]) - previous_close),
            abs(float(row["low"]) - previous_close),
        ))
    atr = max(_position_mean(true_ranges[-14:]), current_price * 0.001)
    atr_pct = atr / current_price * 100.0
    log_returns = [
        math.log(closes[index] / closes[index - 1])
        for index in range(1, len(closes))
        if closes[index] > 0 and closes[index - 1] > 0
    ]
    vol20 = _position_std(log_returns[-20:]) * math.sqrt(252.0) * 100.0
    vol60 = _position_std(log_returns[-60:]) * math.sqrt(252.0) * 100.0
    vol252 = _position_std(log_returns[-252:]) * math.sqrt(252.0) * 100.0
    peak = max(closes)
    max_drawdown = 0.0
    rolling_peak = closes[0]
    for close in closes:
        rolling_peak = max(rolling_peak, close)
        max_drawdown = min(max_drawdown, close / rolling_peak - 1.0)
    drawdown_252 = current_price / max(closes[-252:]) - 1.0
    momentum_20 = _position_return_pct(closes, 20)
    momentum_60 = _position_return_pct(closes, 60)
    momentum_126 = _position_return_pct(closes, 126)
    momentum_252 = _position_return_pct(closes, 252)
    variance_ratio = _position_variance_ratio(closes[-260:], 5)
    median_volume = _position_quantile(volumes[-21:-1], 0.5)
    relative_volume = volumes[-1] / median_volume if median_volume > 0 else 1.0
    analog = _position_long_analog_evidence(daily)
    selected_evidence = analog.get("selected") or {}
    evidence_available = bool(analog.get("available"))
    engine = _position_engine_metrics(engine_context)

    contributions: list[dict[str, Any]] = []

    def add_signal(name: str, value: float, weight: float) -> None:
        normalized = _position_clip(value, -1.0, 1.0)
        contributions.append({"name": name, "value": normalized, "weight": weight, "contribution": normalized * weight})

    add_signal("Price versus SMA 200", (current_price / max(sma200, 1e-9) - 1.0) / 0.18, 0.16)
    add_signal("SMA 50 / 200 structure", (sma50 / max(sma200, 1e-9) - 1.0) / 0.10, 0.16)
    add_signal("One-month momentum", momentum_20 / 12.0, 0.09)
    add_signal("Three-month momentum", momentum_60 / 22.0, 0.12)
    add_signal("Six-month momentum", momentum_126 / 34.0, 0.10)
    add_signal("Twelve-month momentum", momentum_252 / 52.0, 0.08)
    add_signal("RSI quality", 1.0 - abs(rsi - 56.0) / 30.0, 0.06)
    add_signal("Pullback from 252-day high", (abs(drawdown_252) - 0.05) / 0.16 if drawdown_252 < 0 else -0.2, 0.05)
    add_signal("Volume participation", (relative_volume - 1.0) / 1.5, 0.04)
    if evidence_available:
        add_signal("Historical analog terminal hit rate", (float(selected_evidence.get("terminal_hit_rate") or 0.5) - 0.5) * 3.0, 0.16)
    if engine["available"]:
        add_signal("setup.stats.py probability up", (engine["probability_up"] - 0.5) * 2.0, 0.10)
    total_weight = sum(float(row["weight"]) for row in contributions) or 1.0
    long_edge = _position_clip(sum(float(row["contribution"]) for row in contributions) / total_weight, -1.0, 1.0)

    support_candidates = [value for value in (sma20, sma50, ema50) if 0 < value <= current_price]
    dynamic_support = max(support_candidates) if support_candidates else current_price - 0.60 * atr
    entry_center = _position_clip(dynamic_support + 0.25 * atr, current_price - 1.60 * atr, current_price + 0.05 * atr)
    entry_half_width = max(0.18 * atr, current_price * 0.001)
    entry_low = max(0.000001, entry_center - entry_half_width)
    entry_high = entry_center + entry_half_width
    recent_low = min(lows[-min(20, len(lows)):])
    raw_stop = min(entry_low - 0.85 * atr, recent_low - 0.12 * atr)
    stop_distance = _position_clip(
        entry_center - raw_stop,
        max(1.35 * atr, current_price * 0.012),
        max(4.0 * atr, current_price * 0.12),
    )
    stop_loss = max(0.000001, entry_center - stop_distance)
    slippage_bps = _position_clip(
        2.0 + (5.0 if hourly.get("delayed") else 0.0) + (5.0 if relative_volume < 0.65 else 0.0),
        1.5,
        25.0,
    )
    slippage_per_unit = entry_center * slippage_bps / 10_000.0
    net_risk = stop_distance + 2.0 * slippage_per_unit
    analog_mfe_40 = float(selected_evidence.get("mfe_q40") or 0.0)
    analog_mfe_70 = float(selected_evidence.get("mfe_q70") or 0.0)
    minimum_target = 2.0 * net_risk + 2.0 * slippage_per_unit
    target_distance_1 = max(minimum_target, entry_center * analog_mfe_40)
    target_distance_1 = min(target_distance_1, max(minimum_target, entry_center * 0.75))
    target_distance_2 = max(3.25 * net_risk + 2.0 * slippage_per_unit, entry_center * analog_mfe_70, target_distance_1 + atr)
    target_distance_2 = min(target_distance_2, max(target_distance_1 + atr, entry_center * 1.25))
    take_profit_1 = entry_center + target_distance_1
    take_profit_2 = entry_center + target_distance_2
    net_reward = max(0.0, take_profit_1 - entry_center - 2.0 * slippage_per_unit)
    reward_risk = net_reward / max(net_risk, 1e-9)
    target_return = target_distance_1 / max(entry_center, 1e-9)
    favorable_samples = [float(value) for value in analog.get("_favorable") or []]
    target_hits = sum(value >= target_return for value in favorable_samples)
    target_samples = len(favorable_samples)
    empirical_probability = (target_hits + 8.0) / (target_samples + 16.0) if target_samples else 0.50
    confidence_low, confidence_high = _position_wilson_interval(target_hits, target_samples)
    coverage = dict(hourly.get("coverage") or {})
    coverage_complete = bool(coverage.get("complete_from_2016_or_first_available"))
    sample_quality = _position_clip(math.sqrt(target_samples / 120.0), 0.20, 1.0)
    coverage_quality = 1.0 if coverage_complete else 0.68
    feed_quality = 0.80 if hourly.get("stale") else 0.88 if hourly.get("delayed") else 1.0
    probability_quality = sample_quality * coverage_quality * feed_quality
    engine_weight = 0.0
    calibration_quality = 0.0
    combined_probability = empirical_probability
    if engine["available"]:
        evidence_samples = float(engine["sample_count"] or 0.0)
        evidence_decay = _position_clip(math.sqrt(evidence_samples / 180.0), 0.0, 1.0)
        brier = float(engine["brier_score"] if engine["brier_score"] is not None else 0.25)
        calibration_quality = _position_clip(1.0 - brier / 0.35, 0.0, 1.0) * evidence_decay
        engine_weight = 0.08 + 0.10 * calibration_quality
        combined_probability = empirical_probability * (1.0 - engine_weight) + float(engine["probability_up"]) * engine_weight
    gain_probability = 0.50 + (combined_probability - 0.50) * probability_quality
    gain_probability = _position_clip(gain_probability, 0.20, 0.79)
    expected_value_r = gain_probability * reward_risk - (1.0 - gain_probability)
    interval_width = confidence_high - confidence_low
    confidence_score = _position_clip(
        18.0 + 42.0 * probability_quality + 22.0 * abs(long_edge) + 18.0 * (1.0 - interval_width),
        5.0,
        95.0,
    )
    risk_budget = account_value * risk_percent / 100.0
    units_by_risk = risk_budget / max(stop_distance, 1e-9)
    units_by_notional = account_value * (0.35 if gain_probability < 0.62 else 0.50) / max(entry_center, 1e-9)
    suggested_units = min(units_by_risk, units_by_notional)
    if label != "BTCUSD":
        suggested_units = float(max(0, math.floor(suggested_units)))
    else:
        suggested_units = round(max(0.0, suggested_units), 6)
    estimated_loss = suggested_units * stop_distance
    estimated_gain_1 = suggested_units * target_distance_1
    market_open = bool((quote_payload or {}).get("market_open") or hourly.get("market_open"))
    latest_timestamp = int(bars[-1]["timestamp"])
    source_timestamp = int(_num_or_none((quote_payload or {}).get("updated_at")) or latest_timestamp)
    source_age_ms = max(0, int(time.time() * 1000) - source_timestamp)
    stale_for_open_market = market_open and source_age_ms > 20 * 60_000
    bullish_regime = current_price >= sma200 and sma50 >= sma200 * 0.98
    target_supported = target_samples >= 24 and empirical_probability >= 0.52
    if stale_for_open_market:
        action, action_code = "NO TRADE - STALE DATA", "avoid"
    elif not market_open:
        action, action_code = "PLAN ONLY - MARKET CLOSED", "plan"
    elif not coverage_complete:
        action, action_code = "WATCH LONG - PARTIAL HISTORY", "watch"
    elif not bullish_regime:
        action, action_code = "NO TRADE - LONG REGIME ABSENT", "avoid"
    elif current_price > entry_high + 0.55 * atr:
        action, action_code = "WAIT FOR PULLBACK", "wait"
    elif not target_supported or expected_value_r <= 0:
        action, action_code = "NO TRADE - TARGET NOT SUPPORTED", "avoid"
    elif long_edge >= 0.12 and gain_probability >= 0.56 and reward_risk >= 2.0:
        action, action_code = "LONG SETUP", "long"
    elif long_edge >= 0.03 and gain_probability >= 0.51:
        action, action_code = "WATCH LONG", "watch"
    else:
        action, action_code = "NO TRADE", "avoid"
    if current_price > entry_high:
        entry_state = "Wait for a pullback into the entry zone; do not chase above it."
    elif current_price < entry_low:
        entry_state = "Require a close back inside the entry zone before considering the long setup."
    else:
        entry_state = "Price is inside the statistical entry zone; require bullish confirmation before execution."
    selected_horizon = int(selected_evidence.get("horizon_days") or 0)
    drivers = [
        f"Price is {(current_price / max(sma200, 1e-9) - 1.0) * 100.0:+.2f}% versus SMA200; SMA50/SMA200 are {sma50:.4f}/{sma200:.4f}.",
        f"Momentum is {momentum_20:+.2f}% over 1m, {momentum_60:+.2f}% over 3m, {momentum_126:+.2f}% over 6m and {momentum_252:+.2f}% over 12m.",
        f"Historical analogs: {target_hits}/{target_samples} reached TP1 inside the {selected_horizon or 'selected'}-session evidence window; the 90% Wilson interval is {confidence_low * 100.0:.1f}-{confidence_high * 100.0:.1f}%.",
        f"ATR(14) is {atr:.4f}, realised volatility is {vol60:.1f}% annualised and current 252-session drawdown is {drawdown_252 * 100.0:.1f}%.",
    ]
    if engine["available"]:
        drivers.append(
            f"setup.stats.py context: probability up {engine['probability_up'] * 100.0:.1f}%, calibrated confidence {engine['calibrated_confidence']:.1f}/100, regime {engine['regime']}."
        )
    else:
        drivers.append("No completed setup.stats.py run was found; model context is absent and confidence is decayed.")
    risk_checks = [
        "Hard invalidation: a regular-session close below the stop, with no averaging down after invalidation.",
        "No fixed expiry: exit at a target, at the stop, or when the bullish regime and setup conditions invalidate; reassess at every completed session.",
        f"TP1 requires at least 2.0R after a {slippage_bps:.1f} bps round-trip allowance and is checked against historical maximum favorable excursion.",
    ]
    if not coverage_complete:
        risk_checks.append(
            f"Coverage warning: the best source starts on {coverage.get('start') or 'an unknown date'}; confidence is reduced because full post-2016 or post-listing coverage is not proven."
        )
    strongest = sorted(contributions, key=lambda row: abs(float(row["contribution"])), reverse=True)
    last_session_rows = [row for row in bars if row.get("session_date") == bars[-1].get("session_date")]
    session_open = float(last_session_rows[0]["open"] if last_session_rows else daily[-1]["open"])
    session_high = max(float(row["high"]) for row in (last_session_rows or [daily[-1]]))
    session_low = min(float(row["low"]) for row in (last_session_rows or [daily[-1]]))
    session_delta = (current_price / session_open - 1.0) * 100.0 if session_open > 0 else 0.0
    public_analog = {
        key: value for key, value in analog.items() if not str(key).startswith("_")
    }
    return {
        "ok": True,
        "edge": "long",
        "analysis_version": "position-long-v1",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "label": label,
        "name": item.get("name") or label,
        "symbol": item.get("symbol") or label,
        "exchange": item.get("exchange"),
        "currency": hourly.get("currency") or (quote_payload or {}).get("currency") or "",
        "market_open": market_open,
        "source": hourly.get("source") or "validated hourly history",
        "source_age_ms": source_age_ms,
        "delayed": bool(hourly.get("delayed") or (quote_payload or {}).get("delayed")),
        "current_price": round(current_price, 6),
        "coverage": coverage,
        "session": {
            "open": round(session_open, 6),
            "high": round(session_high, 6),
            "low": round(session_low, 6),
            "delta_pct": round(session_delta, 4),
            "bars": len(last_session_rows),
            "timezone": profile["timezone"],
        },
        "advice": {
            "side": "LONG",
            "action": action,
            "action_code": action_code,
            "setup": "Regime-aligned long entry with analog-calibrated exits",
            "entry_zone": {"low": round(entry_low, 6), "high": round(entry_high, 6), "mid": round(entry_center, 6)},
            "entry_state": entry_state,
            "stop_loss": round(stop_loss, 6),
            "take_profit_1": round(take_profit_1, 6),
            "take_profit_2": round(take_profit_2, 6),
            "reward_risk_tp1": round(reward_risk, 3),
            "expected_value_r": round(expected_value_r, 3),
            "gain_probability": round(gain_probability, 4),
            "confidence_score": round(confidence_score, 2),
            "confidence_interval": {"level": 0.90, "low": round(confidence_low, 4), "high": round(confidence_high, 4)},
            "holding_period": "No fixed expiry; exit on target, stop, or regime invalidation.",
            "evidence_horizon_days": selected_horizon,
            "invalidation": risk_checks[0],
        },
        "position_sizing": {
            "account_value": round(account_value, 2),
            "risk_percent": round(risk_percent, 3),
            "risk_budget": round(risk_budget, 2),
            "suggested_units_ceiling": suggested_units,
            "estimated_notional": round(suggested_units * entry_center, 2),
            "estimated_loss_at_stop": round(estimated_loss, 2),
            "estimated_gain_at_tp1": round(estimated_gain_1, 2),
        },
        "technical": {
            "sma_20": round(sma20, 6),
            "sma_50": round(sma50, 6),
            "sma_200": round(sma200, 6),
            "ema_50": round(ema50, 6),
            "atr_14": round(atr, 6),
            "atr_pct": round(atr_pct, 4),
            "rsi_14": round(rsi, 2),
            "momentum_1m_pct": round(momentum_20, 4),
            "momentum_3m_pct": round(momentum_60, 4),
            "momentum_6m_pct": round(momentum_126, 4),
            "momentum_12m_pct": round(momentum_252, 4),
            "variance_ratio_5": round(variance_ratio, 4),
            "volatility_20_ann_pct": round(vol20, 2),
            "volatility_60_ann_pct": round(vol60, 2),
            "volatility_252_ann_pct": round(vol252, 2),
            "drawdown_252_pct": round(drawdown_252 * 100.0, 3),
            "max_drawdown_since_2016_pct": round(max_drawdown * 100.0, 3),
            "relative_volume": round(relative_volume, 3),
            "long_edge": round(long_edge, 4),
            "target_hit_rate": round(target_hits / target_samples, 4) if target_samples else None,
            "target_samples": target_samples,
        },
        "model_context": {
            **engine,
            "weight_in_probability": round(engine_weight, 4),
            "calibration_quality": round(calibration_quality, 4),
            "empirical_target_probability": round(empirical_probability, 4),
            "data_quality_factor": round(probability_quality, 4),
            "analog_evidence": public_analog,
        },
        "feature_contributions": strongest,
        "drivers": drivers,
        "risk_checks": risk_checks,
        "chart": {
            "bars": bars,
            "timezone": profile["timezone"],
            "interval": "1h",
            "regular_sessions_only": True,
            "closed_periods_compressed": True,
        },
        "calculation_ms": round((time.perf_counter() - started) * 1000.0, 2),
        "disclaimer": "Statistical Long-edge research only. Targets, stops and probabilities are conditional estimates, not guaranteed outcomes or personalised financial advice.",
    }


def _position_build_advice(
    label: str,
    item: dict[str, Any],
    intraday: dict[str, Any],
    engine_context: dict[str, Any] | None,
    quote_payload: dict[str, Any] | None,
    account_value: float,
    risk_percent: float,
) -> dict[str, Any]:
    started = time.perf_counter()
    bars = _position_merge_live_quote(_position_clean_bars(intraday.get("bars") or []), quote_payload)
    session = _position_session_rows(bars, str(intraday.get("timezone") or "UTC"))
    if not session:
        return {
            "ok": False,
            "label": label,
            "name": item.get("name") or label,
            "error": intraday.get("error") or "No validated one-minute session is available yet.",
            "source": intraday.get("source"),
            "retryable": True,
            "calculation_ms": round((time.perf_counter() - started) * 1000.0, 2),
        }
    closes = [float(row["close"]) for row in session]
    highs = [float(row["high"]) for row in session]
    lows = [float(row["low"]) for row in session]
    opens = [float(row["open"]) for row in session]
    volumes = [max(0.0, float(row.get("volume") or 0.0)) for row in session]
    current_price = closes[-1]
    session_open = opens[0]
    log_returns = [math.log(closes[index] / closes[index - 1]) for index in range(1, len(closes)) if closes[index] > 0 and closes[index - 1] > 0]
    true_ranges = []
    for index, row in enumerate(session):
        previous_close = closes[index - 1] if index else float(row["open"])
        true_ranges.append(max(float(row["high"]) - float(row["low"]), abs(float(row["high"]) - previous_close), abs(float(row["low"]) - previous_close)))
    atr = max(_position_mean(true_ranges[-14:]), current_price * 0.00025)
    atr_pct = atr / current_price * 100.0
    ema8 = _position_ema(closes, 8)
    ema21 = _position_ema(closes, 21)
    typical_prices = [(high + low + close) / 3.0 for high, low, close in zip(highs, lows, closes)]
    volume_total = sum(volumes)
    vwap = sum(price * volume for price, volume in zip(typical_prices, volumes)) / volume_total if volume_total > 0 else _position_mean(typical_prices)
    rsi = _position_rsi(closes)
    momentum_5 = _position_return_pct(closes, 5)
    momentum_15 = _position_return_pct(closes, 15)
    momentum_30 = _position_return_pct(closes, 30)
    slope_15 = _position_slope_pct(closes, 15)
    variance_ratio = _position_variance_ratio(closes[-120:], 5)
    minute_sigma = _position_std(log_returns[-90:])
    bars_per_day = 1440 if label == "BTCUSD" else 390
    annualisation_days = 365 if label == "BTCUSD" else 252
    realised_volatility = minute_sigma * math.sqrt(bars_per_day * annualisation_days) * 100.0
    expected_move_30m = current_price * minute_sigma * math.sqrt(30.0)
    opening_count = min(15, len(session))
    opening_high = max(highs[:opening_count])
    opening_low = min(lows[:opening_count])
    recent_reference = session[-21:-1] or session[:-1] or session
    recent_high = max(float(row["high"]) for row in recent_reference)
    recent_low = min(float(row["low"]) for row in recent_reference)
    recent_volumes = volumes[-21:-1] or volumes[:-1]
    median_volume = _position_quantile(recent_volumes, 0.5)
    volume_std = _position_std(recent_volumes)
    relative_volume = volumes[-1] / median_volume if median_volume > 0 else 1.0
    volume_zscore = (volumes[-1] - _position_mean(recent_volumes)) / volume_std if volume_std > 0 else 0.0
    signed_volume = []
    matching_returns = []
    for index in range(max(1, len(session) - 30), len(session)):
        change = closes[index] / closes[index - 1] - 1.0 if closes[index - 1] > 0 else 0.0
        matching_returns.append(change)
        signed_volume.append(volumes[index] * (1.0 if change > 0 else -1.0 if change < 0 else 0.0))
    recent_volume_total = sum(volumes[-30:])
    volume_imbalance = sum(signed_volume) / recent_volume_total if recent_volume_total > 0 else 0.0
    return_volume_correlation = _position_correlation(matching_returns, volumes[-len(matching_returns):])
    last_range = max(highs[-1] - lows[-1], current_price * 1e-8)
    upper_wick_ratio = (highs[-1] - max(opens[-1], closes[-1])) / last_range
    close_location = (closes[-1] - lows[-1]) / last_range
    bearish_rejection = closes[-1] < opens[-1] and upper_wick_ratio >= 0.35 and close_location <= 0.55
    lower_highs = sum(1 for index in range(max(1, len(highs) - 6), len(highs)) if highs[index] < highs[index - 1])
    delta_from_open = (current_price / session_open - 1.0) * 100.0 if session_open > 0 else 0.0
    gap_count = sum(1 for left, right in zip(session, session[1:]) if int(right["timestamp"]) - int(left["timestamp"]) > 150_000)
    zero_volume_ratio = sum(1 for value in volumes if value <= 0) / max(1, len(volumes))
    volume_available = volume_total > 0 and zero_volume_ratio < 0.90
    latest_timestamp = int(session[-1]["timestamp"])
    source_age_ms = max(0, int(time.time() * 1000) - int(_num_or_none((quote_payload or {}).get("updated_at")) or latest_timestamp))
    engine = _position_engine_metrics(engine_context)

    contributions: list[dict[str, Any]] = []

    def add_signal(name: str, value: float, weight: float) -> None:
        normalized = _position_clip(value, -1.0, 1.0)
        contributions.append({"name": name, "value": normalized, "weight": weight, "contribution": normalized * weight})

    scale = max(atr_pct, 0.04)
    add_signal("EMA 8/21 structure", (ema21 - ema8) / atr, 0.13)
    add_signal("VWAP displacement", (vwap - current_price) / atr, 0.12)
    add_signal("5-minute momentum", -momentum_5 / (scale * math.sqrt(5.0)), 0.10)
    add_signal("15-minute momentum", -momentum_15 / (scale * math.sqrt(15.0)), 0.11)
    add_signal("30-minute momentum", -momentum_30 / (scale * math.sqrt(30.0)), 0.07)
    add_signal("15-minute regression slope", -slope_15 / max(scale / 3.0, 0.015), 0.08)
    add_signal("Session delta", -delta_from_open / max(scale * math.sqrt(max(1, len(session))), 0.10), 0.06)
    opening_signal = 1.0 if current_price < opening_low else -1.0 if current_price > opening_high else (opening_high + opening_low - 2.0 * current_price) / max(opening_high - opening_low, atr)
    add_signal("Opening range location", opening_signal, 0.08)
    add_signal("RSI pressure", (50.0 - rsi) / 22.0, 0.06)
    variance_direction = 1.0 if momentum_15 < 0 else -1.0
    add_signal("Variance-ratio regime", (variance_ratio - 1.0) * 1.8 * variance_direction, 0.06)
    add_signal("Signed volume imbalance", -volume_imbalance * 3.0 if volume_available else 0.0, 0.08 if volume_available else 0.02)
    add_signal("Volume confirmation", (relative_volume - 1.0) * (1.0 if momentum_5 < 0 else -1.0), 0.05 if volume_available else 0.01)
    add_signal("Bearish candle rejection", 1.0 if bearish_rejection else -0.2, 0.04)
    add_signal("Lower-high structure", (lower_highs - 2.0) / 3.0, 0.04)
    if engine["available"]:
        add_signal("Daily ensemble probability", (engine["probability_down"] - 0.5) * 2.0, 0.10)
        add_signal("Daily expected return", -engine["expected_return_pct"] / max(engine["volatility_ann_pct"] / 8.0, 0.5), 0.05)
        add_signal("News catalyst", -engine["news_score"] / 1.5, 0.03)
    total_weight = sum(float(row["weight"]) for row in contributions) or 1.0
    short_edge = sum(float(row["contribution"]) for row in contributions) / total_weight
    extension_atr = (vwap - current_price) / atr
    overextended = extension_atr > 2.25 or rsi < 23.0
    if overextended:
        short_edge -= min(0.18, max(0.0, extension_atr - 1.75) * 0.06 + max(0.0, 25.0 - rsi) * 0.008)
    short_edge = _position_clip(short_edge, -1.0, 1.0)
    intraday_probability = 1.0 / (1.0 + math.exp(-3.4 * short_edge))
    if engine["available"]:
        evidence_samples = float(engine["sample_count"] or 0.0)
        evidence_decay = _position_clip(math.sqrt(evidence_samples / 180.0), 0.0, 1.0)
        brier = float(engine["brier_score"] if engine["brier_score"] is not None else 0.25)
        calibration_quality = _position_clip(1.0 - brier / 0.35, 0.0, 1.0) * evidence_decay
        engine_weight = 0.12 + 0.18 * calibration_quality
        combined_probability = intraday_probability * (1.0 - engine_weight) + float(engine["probability_down"]) * engine_weight
    else:
        calibration_quality = 0.0
        engine_weight = 0.0
        combined_probability = intraday_probability
    coverage_quality = _position_clip(math.sqrt(len(session) / 90.0), 0.35, 1.0)
    continuity_quality = _position_clip(1.0 - gap_count / max(1.0, len(session) / 25.0), 0.35, 1.0)
    feed_quality = 0.78 if intraday.get("stale") else 0.88 if intraday.get("delayed") else 1.0
    data_quality = coverage_quality * continuity_quality * feed_quality
    gain_probability = 0.5 + (combined_probability - 0.5) * data_quality
    gain_probability = _position_clip(gain_probability, 0.20, 0.79)
    confidence_score = _position_clip((data_quality * 62.0) + abs(short_edge) * 26.0 + calibration_quality * 12.0, 5.0, 95.0)

    resistance_candidates = [vwap, ema8, ema21, recent_low, opening_low]
    usable_resistance = [value for value in resistance_candidates if value >= current_price - 0.20 * atr]
    resistance = min(usable_resistance, key=lambda value: abs(value - current_price)) if usable_resistance else current_price
    if current_price < min(vwap, ema21) and short_edge > 0:
        entry_center = _position_clip(resistance, current_price + 0.10 * atr, current_price + 1.45 * atr)
        setup_name = "Bearish pullback into dynamic resistance"
    elif current_price < min(recent_low, opening_low) and relative_volume >= 1.05:
        entry_center = _position_clip(max(recent_low, current_price), current_price - 0.05 * atr, current_price + 0.55 * atr)
        setup_name = "Opening-range breakdown retest"
    elif bearish_rejection:
        entry_center = current_price
        setup_name = "Failed rebound with bearish rejection"
    else:
        entry_center = _position_clip(max(current_price, ema8), current_price - 0.10 * atr, current_price + 1.00 * atr)
        setup_name = "Conditional short at resistance"
    entry_half_width = max(0.10 * atr, current_price * 0.00025)
    entry_low = max(0.000001, entry_center - entry_half_width)
    entry_high = entry_center + entry_half_width
    swing_high = max(highs[-min(10, len(highs)):])
    structural_stop = max(entry_high + 0.82 * atr, min(swing_high + 0.12 * atr, entry_high + 2.20 * atr), ema21 + 0.12 * atr if ema21 > entry_high else entry_high)
    stop_distance = _position_clip(structural_stop - entry_center, max(0.90 * atr, current_price * 0.0010), max(2.40 * atr, current_price * 0.0060))
    stop_loss = entry_center + stop_distance
    holding_minutes = int(round(_position_clip(28.0 + max(0.0, variance_ratio - 0.7) * 24.0 + abs(short_edge) * 22.0, 20.0, 90.0)))
    expected_holding_move = max(expected_move_30m * math.sqrt(holding_minutes / 30.0), atr * 1.10)
    slippage_bps = _position_clip(2.0 + (0.0 if volume_available else 8.0) + max(0.0, 1.0 - relative_volume) * 5.0 + (4.0 if intraday.get("delayed") else 0.0), 1.5, 25.0)
    slippage_per_unit = entry_center * slippage_bps / 10_000.0
    minimum_net_target = 1.25 * (stop_distance + 2.0 * slippage_per_unit) + 2.0 * slippage_per_unit
    target_distance_1 = max(minimum_net_target, 1.45 * stop_distance, min(2.05 * stop_distance, expected_holding_move))
    target_distance_2 = max(2.20 * stop_distance, target_distance_1 + 0.65 * atr)
    take_profit_1 = max(0.000001, entry_center - target_distance_1)
    take_profit_2 = max(0.000001, entry_center - target_distance_2)
    net_reward = max(0.0, entry_center - take_profit_1 - 2.0 * slippage_per_unit)
    net_risk = max(1e-9, stop_loss - entry_center + 2.0 * slippage_per_unit)
    reward_risk = net_reward / net_risk
    expected_value_r = gain_probability * reward_risk - (1.0 - gain_probability)
    risk_budget = account_value * risk_percent / 100.0
    units_by_risk = risk_budget / max(1e-9, stop_loss - entry_center)
    max_notional_fraction = 0.35 if gain_probability < 0.64 else 0.50
    units_by_notional = account_value * max_notional_fraction / max(entry_center, 1e-9)
    suggested_units = min(units_by_risk, units_by_notional)
    if label != "BTCUSD":
        suggested_units = float(max(0, math.floor(suggested_units)))
    else:
        suggested_units = round(max(0.0, suggested_units), 6)
    estimated_loss = suggested_units * max(0.0, stop_loss - entry_center)
    estimated_gain_1 = suggested_units * max(0.0, entry_center - take_profit_1)
    market_open = bool((quote_payload or {}).get("market_open") or intraday.get("market_open"))
    stale_for_open_market = market_open and source_age_ms > 20 * 60_000
    engine_blocks_short = engine["available"] and engine["probability_up"] >= 0.70 and short_edge < 0.38
    if len(session) < 15:
        action = "WAIT - OPENING RANGE"
        action_code = "wait"
    elif stale_for_open_market:
        action = "NO TRADE - STALE DATA"
        action_code = "avoid"
    elif not market_open:
        action = "PLAN ONLY - MARKET CLOSED"
        action_code = "plan"
    elif overextended:
        action = "WAIT FOR PULLBACK"
        action_code = "wait"
    elif engine_blocks_short:
        action = "NO TRADE - DAILY MODEL CONFLICT"
        action_code = "avoid"
    elif short_edge >= 0.18 and gain_probability >= 0.60 and reward_risk >= 1.25 and expected_value_r > 0:
        action = "SHORT SETUP"
        action_code = "short"
    elif short_edge >= 0.07 and gain_probability >= 0.53:
        action = "WATCH SHORT"
        action_code = "watch"
    else:
        action = "NO TRADE"
        action_code = "avoid"
    if current_price < entry_low:
        entry_state = "Wait for a rebound into the entry zone; do not chase below it."
    elif current_price > entry_high:
        entry_state = "Wait for rejection back inside the entry zone before considering a short."
    else:
        entry_state = "Price is inside the statistical entry zone; confirmation still requires a bearish one-minute close."

    strongest = sorted(contributions, key=lambda row: abs(float(row["contribution"])), reverse=True)
    drivers = [
        f"Price is {(current_price / vwap - 1.0) * 100.0:+.2f}% versus session VWAP; EMA8/EMA21 are {ema8:.4f}/{ema21:.4f}.",
        f"Momentum is {momentum_5:+.2f}% over 5m, {momentum_15:+.2f}% over 15m and {momentum_30:+.2f}% over 30m.",
        f"Variance ratio is {variance_ratio:.2f}; realised volatility is {realised_volatility:.1f}% annualised and ATR(14) is {atr:.4f}.",
        f"Relative one-minute volume is {relative_volume:.2f}x with signed imbalance {volume_imbalance:+.2f} and price/volume correlation {return_volume_correlation:+.2f}.",
    ]
    if engine["available"]:
        drivers.append(
            f"setup.stats.py context: probability down {engine['probability_down'] * 100.0:.1f}%, calibrated confidence {engine['calibrated_confidence']:.1f}/100, regime {engine['regime']}."
        )
    else:
        drivers.append("No completed setup.stats.py run was found for this ticker; probability is therefore intraday-only and confidence is decayed.")
    risk_checks = [
        "Hard invalidation: two consecutive one-minute closes above the stop, or one close above it on abnormal volume.",
        f"Time stop: reassess after {holding_minutes} minutes if TP1 has not been reached; do not carry this day-trade plan overnight.",
        f"Estimated round-trip slippage allowance: {slippage_bps:.1f} bps. Reward/risk is shown after this allowance.",
    ]
    if gap_count:
        risk_checks.append(f"Data warning: {gap_count} intraday gap(s) above 2.5 minutes were detected in the displayed session.")
    if not volume_available:
        risk_checks.append("Volume is unavailable or structurally zero for this instrument; volume confirmation has been down-weighted.")
    return {
        "ok": True,
        "analysis_version": "position-short-v1",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "label": label,
        "name": item.get("name") or label,
        "symbol": item.get("symbol") or label,
        "exchange": item.get("exchange"),
        "currency": intraday.get("currency") or (quote_payload or {}).get("currency") or "",
        "market_open": market_open,
        "source": intraday.get("source") or "validated intraday fallback",
        "source_age_ms": source_age_ms,
        "delayed": bool(intraday.get("delayed") or (quote_payload or {}).get("delayed")),
        "current_price": round(current_price, 6),
        "session": {
            "open": round(session_open, 6),
            "high": round(max(highs), 6),
            "low": round(min(lows), 6),
            "delta_pct": round(delta_from_open, 4),
            "bars": len(session),
            "timezone": intraday.get("timezone") or "UTC",
        },
        "advice": {
            "side": "SHORT",
            "action": action,
            "action_code": action_code,
            "setup": setup_name,
            "entry_zone": {"low": round(entry_low, 6), "high": round(entry_high, 6), "mid": round(entry_center, 6)},
            "entry_state": entry_state,
            "stop_loss": round(stop_loss, 6),
            "take_profit_1": round(take_profit_1, 6),
            "take_profit_2": round(take_profit_2, 6),
            "reward_risk_tp1": round(reward_risk, 3),
            "expected_value_r": round(expected_value_r, 3),
            "gain_probability": round(gain_probability, 4),
            "confidence_score": round(confidence_score, 2),
            "holding_minutes": holding_minutes,
            "invalidation": risk_checks[0],
        },
        "position_sizing": {
            "account_value": round(account_value, 2),
            "risk_percent": round(risk_percent, 3),
            "risk_budget": round(risk_budget, 2),
            "suggested_units_ceiling": suggested_units,
            "estimated_notional": round(suggested_units * entry_center, 2),
            "estimated_loss_at_stop": round(estimated_loss, 2),
            "estimated_gain_at_tp1": round(estimated_gain_1, 2),
        },
        "technical": {
            "vwap": round(vwap, 6),
            "ema_8": round(ema8, 6),
            "ema_21": round(ema21, 6),
            "atr_14": round(atr, 6),
            "atr_pct": round(atr_pct, 4),
            "rsi_14": round(rsi, 2),
            "momentum_5m_pct": round(momentum_5, 4),
            "momentum_15m_pct": round(momentum_15, 4),
            "momentum_30m_pct": round(momentum_30, 4),
            "slope_15m_pct_per_bar": round(slope_15, 5),
            "variance_ratio_5": round(variance_ratio, 4),
            "realised_volatility_ann_pct": round(realised_volatility, 2),
            "expected_move_30m": round(expected_move_30m, 6),
            "opening_range_high": round(opening_high, 6),
            "opening_range_low": round(opening_low, 6),
            "relative_volume": round(relative_volume, 3),
            "volume_zscore": round(volume_zscore, 3),
            "signed_volume_imbalance": round(volume_imbalance, 4),
            "return_volume_correlation": round(return_volume_correlation, 4),
            "short_edge": round(short_edge, 4),
            "overextended": overextended,
        },
        "model_context": {
            **engine,
            "weight_in_probability": round(engine_weight, 4),
            "calibration_quality": round(calibration_quality, 4),
            "intraday_probability_down": round(intraday_probability, 4),
            "data_quality_factor": round(data_quality, 4),
        },
        "feature_contributions": strongest,
        "drivers": drivers,
        "risk_checks": risk_checks,
        "chart": {"bars": session, "timezone": intraday.get("timezone") or "UTC"},
        "calculation_ms": round((time.perf_counter() - started) * 1000.0, 2),
        "disclaimer": "Statistical short-setup research only. Levels are conditional, not guaranteed outcomes or personalised financial advice.",
    }


def _position_build_intraday_long_advice(
    label: str,
    item: dict[str, Any],
    intraday: dict[str, Any],
    engine_context: dict[str, Any] | None,
    quote_payload: dict[str, Any] | None,
    account_value: float,
    risk_percent: float,
) -> dict[str, Any]:
    """Build a bullish intraday plan without changing the existing Short engine."""
    started = time.perf_counter()
    bars = _position_merge_live_quote(_position_clean_bars(intraday.get("bars") or []), quote_payload)
    session = _position_session_rows(bars, str(intraday.get("timezone") or "UTC"))
    if not session:
        return {
            "ok": False,
            "label": label,
            "name": item.get("name") or label,
            "error": intraday.get("error") or "No validated one-minute session is available yet.",
            "source": intraday.get("source"),
            "retryable": True,
            "calculation_ms": round((time.perf_counter() - started) * 1000.0, 2),
        }

    closes = [float(row["close"]) for row in session]
    highs = [float(row["high"]) for row in session]
    lows = [float(row["low"]) for row in session]
    opens = [float(row["open"]) for row in session]
    volumes = [max(0.0, float(row.get("volume") or 0.0)) for row in session]
    current_price = closes[-1]
    session_open = opens[0]
    log_returns = [math.log(closes[index] / closes[index - 1]) for index in range(1, len(closes)) if closes[index] > 0 and closes[index - 1] > 0]
    true_ranges = []
    for index, row in enumerate(session):
        previous_close = closes[index - 1] if index else float(row["open"])
        true_ranges.append(max(float(row["high"]) - float(row["low"]), abs(float(row["high"]) - previous_close), abs(float(row["low"]) - previous_close)))
    atr = max(_position_mean(true_ranges[-14:]), current_price * 0.00025)
    atr_pct = atr / current_price * 100.0
    ema8 = _position_ema(closes, 8)
    ema21 = _position_ema(closes, 21)
    typical_prices = [(high + low + close) / 3.0 for high, low, close in zip(highs, lows, closes)]
    volume_total = sum(volumes)
    vwap = sum(value * volume for value, volume in zip(typical_prices, volumes)) / volume_total if volume_total > 0 else _position_mean(typical_prices)
    rsi = _position_rsi(closes)
    momentum_5 = _position_return_pct(closes, 5)
    momentum_15 = _position_return_pct(closes, 15)
    momentum_30 = _position_return_pct(closes, 30)
    slope_15 = _position_slope_pct(closes, 15)
    variance_ratio = _position_variance_ratio(closes[-120:], 5)
    minute_sigma = _position_std(log_returns[-90:])
    bars_per_day = 1440 if label == "BTCUSD" else 390
    annualisation_days = 365 if label == "BTCUSD" else 252
    realised_volatility = minute_sigma * math.sqrt(bars_per_day * annualisation_days) * 100.0
    expected_move_30m = current_price * minute_sigma * math.sqrt(30.0)
    opening_count = min(15, len(session))
    opening_high = max(highs[:opening_count])
    opening_low = min(lows[:opening_count])
    recent_reference = session[-21:-1] or session[:-1] or session
    recent_high = max(float(row["high"]) for row in recent_reference)
    recent_low = min(float(row["low"]) for row in recent_reference)
    recent_volumes = volumes[-21:-1] or volumes[:-1]
    median_volume = _position_quantile(recent_volumes, 0.5)
    volume_std = _position_std(recent_volumes)
    relative_volume = volumes[-1] / median_volume if median_volume > 0 else 1.0
    volume_zscore = (volumes[-1] - _position_mean(recent_volumes)) / volume_std if volume_std > 0 else 0.0
    signed_volume = []
    matching_returns = []
    for index in range(max(1, len(session) - 30), len(session)):
        change = closes[index] / closes[index - 1] - 1.0 if closes[index - 1] > 0 else 0.0
        matching_returns.append(change)
        signed_volume.append(volumes[index] * (1.0 if change > 0 else -1.0 if change < 0 else 0.0))
    recent_volume_total = sum(volumes[-30:])
    volume_imbalance = sum(signed_volume) / recent_volume_total if recent_volume_total > 0 else 0.0
    return_volume_correlation = _position_correlation(matching_returns, volumes[-len(matching_returns):])
    last_range = max(highs[-1] - lows[-1], current_price * 1e-8)
    lower_wick_ratio = (min(opens[-1], closes[-1]) - lows[-1]) / last_range
    close_location = (closes[-1] - lows[-1]) / last_range
    bullish_rejection = closes[-1] > opens[-1] and lower_wick_ratio >= 0.35 and close_location >= 0.45
    higher_lows = sum(1 for index in range(max(1, len(lows) - 6), len(lows)) if lows[index] > lows[index - 1])
    delta_from_open = (current_price / session_open - 1.0) * 100.0 if session_open > 0 else 0.0
    gap_count = sum(1 for left, right in zip(session, session[1:]) if int(right["timestamp"]) - int(left["timestamp"]) > 150_000)
    zero_volume_ratio = sum(1 for value in volumes if value <= 0) / max(1, len(volumes))
    volume_available = volume_total > 0 and zero_volume_ratio < 0.90
    latest_timestamp = int(session[-1]["timestamp"])
    source_age_ms = max(0, int(time.time() * 1000) - int(_num_or_none((quote_payload or {}).get("updated_at")) or latest_timestamp))
    engine = _position_engine_metrics(engine_context)

    contributions: list[dict[str, Any]] = []

    def add_signal(name: str, value: float, weight: float) -> None:
        normalized = _position_clip(value, -1.0, 1.0)
        contributions.append({"name": name, "value": normalized, "weight": weight, "contribution": normalized * weight})

    scale = max(atr_pct, 0.04)
    add_signal("EMA 8/21 structure", (ema8 - ema21) / atr, 0.13)
    add_signal("VWAP displacement", (current_price - vwap) / atr, 0.12)
    add_signal("5-minute momentum", momentum_5 / (scale * math.sqrt(5.0)), 0.10)
    add_signal("15-minute momentum", momentum_15 / (scale * math.sqrt(15.0)), 0.11)
    add_signal("30-minute momentum", momentum_30 / (scale * math.sqrt(30.0)), 0.07)
    add_signal("15-minute regression slope", slope_15 / max(scale / 3.0, 0.015), 0.08)
    add_signal("Session delta", delta_from_open / max(scale * math.sqrt(max(1, len(session))), 0.10), 0.06)
    opening_signal = 1.0 if current_price > opening_high else -1.0 if current_price < opening_low else (2.0 * current_price - opening_high - opening_low) / max(opening_high - opening_low, atr)
    add_signal("Opening range location", opening_signal, 0.08)
    add_signal("RSI pressure", (rsi - 50.0) / 22.0, 0.06)
    variance_direction = 1.0 if momentum_15 > 0 else -1.0
    add_signal("Variance-ratio regime", (variance_ratio - 1.0) * 1.8 * variance_direction, 0.06)
    add_signal("Signed volume imbalance", volume_imbalance * 3.0 if volume_available else 0.0, 0.08 if volume_available else 0.02)
    add_signal("Volume confirmation", (relative_volume - 1.0) * (1.0 if momentum_5 > 0 else -1.0), 0.05 if volume_available else 0.01)
    add_signal("Bullish candle rejection", 1.0 if bullish_rejection else -0.2, 0.04)
    add_signal("Higher-low structure", (higher_lows - 2.0) / 3.0, 0.04)
    if engine["available"]:
        add_signal("Daily ensemble probability", (engine["probability_up"] - 0.5) * 2.0, 0.10)
        add_signal("Daily expected return", engine["expected_return_pct"] / max(engine["volatility_ann_pct"] / 8.0, 0.5), 0.05)
        add_signal("News catalyst", engine["news_score"] / 1.5, 0.03)
    total_weight = sum(float(row["weight"]) for row in contributions) or 1.0
    long_edge = sum(float(row["contribution"]) for row in contributions) / total_weight
    extension_atr = (current_price - vwap) / atr
    overextended = extension_atr > 2.25 or rsi > 77.0
    if overextended:
        long_edge -= min(0.18, max(0.0, extension_atr - 1.75) * 0.06 + max(0.0, rsi - 75.0) * 0.008)
    long_edge = _position_clip(long_edge, -1.0, 1.0)
    intraday_probability = 1.0 / (1.0 + math.exp(-3.4 * long_edge))
    if engine["available"]:
        evidence_samples = float(engine["sample_count"] or 0.0)
        evidence_decay = _position_clip(math.sqrt(evidence_samples / 180.0), 0.0, 1.0)
        brier = float(engine["brier_score"] if engine["brier_score"] is not None else 0.25)
        calibration_quality = _position_clip(1.0 - brier / 0.35, 0.0, 1.0) * evidence_decay
        engine_weight = 0.12 + 0.18 * calibration_quality
        combined_probability = intraday_probability * (1.0 - engine_weight) + float(engine["probability_up"]) * engine_weight
    else:
        calibration_quality = 0.0
        engine_weight = 0.0
        combined_probability = intraday_probability
    coverage_quality = _position_clip(math.sqrt(len(session) / 90.0), 0.35, 1.0)
    continuity_quality = _position_clip(1.0 - gap_count / max(1.0, len(session) / 25.0), 0.35, 1.0)
    feed_quality = 0.78 if intraday.get("stale") else 0.88 if intraday.get("delayed") else 1.0
    data_quality = coverage_quality * continuity_quality * feed_quality
    gain_probability = 0.5 + (combined_probability - 0.5) * data_quality
    gain_probability = _position_clip(gain_probability, 0.20, 0.79)
    confidence_score = _position_clip((data_quality * 62.0) + abs(long_edge) * 26.0 + calibration_quality * 12.0, 5.0, 95.0)

    support_candidates = [vwap, ema8, ema21, recent_high, opening_high]
    usable_support = [value for value in support_candidates if value <= current_price + 0.20 * atr]
    support = min(usable_support, key=lambda value: abs(value - current_price)) if usable_support else current_price
    if current_price > max(vwap, ema21) and long_edge > 0:
        entry_center = _position_clip(support, current_price - 1.45 * atr, current_price - 0.10 * atr)
        setup_name = "Bullish pullback into dynamic support"
    elif current_price > max(recent_high, opening_high) and relative_volume >= 1.05:
        entry_center = _position_clip(min(recent_high, current_price), current_price - 0.55 * atr, current_price + 0.05 * atr)
        setup_name = "Opening-range breakout retest"
    elif bullish_rejection:
        entry_center = current_price
        setup_name = "Failed selloff with bullish rejection"
    else:
        entry_center = _position_clip(min(current_price, ema8), current_price - 1.00 * atr, current_price + 0.10 * atr)
        setup_name = "Conditional long at support"
    entry_half_width = max(0.10 * atr, current_price * 0.00025)
    entry_low = max(0.000001, entry_center - entry_half_width)
    entry_high = entry_center + entry_half_width
    swing_low = min(lows[-min(10, len(lows)):])
    structural_stop = min(entry_low - 0.82 * atr, max(swing_low - 0.12 * atr, entry_low - 2.20 * atr), ema21 - 0.12 * atr if ema21 < entry_low else entry_low)
    stop_distance = _position_clip(entry_center - structural_stop, max(0.90 * atr, current_price * 0.0010), max(2.40 * atr, current_price * 0.0060))
    stop_loss = max(0.000001, entry_center - stop_distance)
    holding_minutes = int(round(_position_clip(28.0 + max(0.0, variance_ratio - 0.7) * 24.0 + abs(long_edge) * 22.0, 20.0, 90.0)))
    expected_holding_move = max(expected_move_30m * math.sqrt(holding_minutes / 30.0), atr * 1.10)
    slippage_bps = _position_clip(2.0 + (0.0 if volume_available else 8.0) + max(0.0, 1.0 - relative_volume) * 5.0 + (4.0 if intraday.get("delayed") else 0.0), 1.5, 25.0)
    slippage_per_unit = entry_center * slippage_bps / 10_000.0
    minimum_net_target = 1.25 * (stop_distance + 2.0 * slippage_per_unit) + 2.0 * slippage_per_unit
    target_distance_1 = max(minimum_net_target, 1.45 * stop_distance, min(2.05 * stop_distance, expected_holding_move))
    target_distance_2 = max(2.20 * stop_distance, target_distance_1 + 0.65 * atr)
    take_profit_1 = entry_center + target_distance_1
    take_profit_2 = entry_center + target_distance_2
    net_reward = max(0.0, take_profit_1 - entry_center - 2.0 * slippage_per_unit)
    net_risk = max(1e-9, entry_center - stop_loss + 2.0 * slippage_per_unit)
    reward_risk = net_reward / net_risk
    expected_value_r = gain_probability * reward_risk - (1.0 - gain_probability)
    risk_budget = account_value * risk_percent / 100.0
    units_by_risk = risk_budget / max(1e-9, entry_center - stop_loss)
    max_notional_fraction = 0.35 if gain_probability < 0.64 else 0.50
    units_by_notional = account_value * max_notional_fraction / max(entry_center, 1e-9)
    suggested_units = min(units_by_risk, units_by_notional)
    if label != "BTCUSD":
        suggested_units = float(max(0, math.floor(suggested_units)))
    else:
        suggested_units = round(max(0.0, suggested_units), 6)
    estimated_loss = suggested_units * max(0.0, entry_center - stop_loss)
    estimated_gain_1 = suggested_units * max(0.0, take_profit_1 - entry_center)
    market_open = bool((quote_payload or {}).get("market_open") or intraday.get("market_open"))
    stale_for_open_market = market_open and source_age_ms > 20 * 60_000
    engine_blocks_long = engine["available"] and engine["probability_down"] >= 0.70 and long_edge < 0.38
    if len(session) < 15:
        action, action_code = "WAIT - OPENING RANGE", "wait"
    elif stale_for_open_market:
        action, action_code = "NO TRADE - STALE DATA", "avoid"
    elif not market_open:
        action, action_code = "PLAN ONLY - MARKET CLOSED", "plan"
    elif overextended:
        action, action_code = "WAIT FOR PULLBACK", "wait"
    elif engine_blocks_long:
        action, action_code = "NO TRADE - DAILY MODEL CONFLICT", "avoid"
    elif long_edge >= 0.18 and gain_probability >= 0.60 and reward_risk >= 1.25 and expected_value_r > 0:
        action, action_code = "LONG SETUP", "long"
    elif long_edge >= 0.07 and gain_probability >= 0.53:
        action, action_code = "WATCH LONG", "watch"
    else:
        action, action_code = "NO TRADE", "avoid"
    if current_price > entry_high:
        entry_state = "Wait for a pullback into the entry zone; do not chase above it."
    elif current_price < entry_low:
        entry_state = "Wait for a reclaim into the entry zone and bullish confirmation before considering a long."
    else:
        entry_state = "Price is inside the statistical entry zone; confirmation still requires a bullish one-minute close."

    strongest = sorted(contributions, key=lambda row: abs(float(row["contribution"])), reverse=True)
    drivers = [
        f"Price is {(current_price / vwap - 1.0) * 100.0:+.2f}% versus session VWAP; EMA8/EMA21 are {ema8:.4f}/{ema21:.4f}.",
        f"Momentum is {momentum_5:+.2f}% over 5m, {momentum_15:+.2f}% over 15m and {momentum_30:+.2f}% over 30m.",
        f"Variance ratio is {variance_ratio:.2f}; realised volatility is {realised_volatility:.1f}% annualised and ATR(14) is {atr:.4f}.",
        f"Relative one-minute volume is {relative_volume:.2f}x with signed imbalance {volume_imbalance:+.2f} and price/volume correlation {return_volume_correlation:+.2f}.",
    ]
    if engine["available"]:
        drivers.append(f"setup.stats.py context: probability up {engine['probability_up'] * 100.0:.1f}%, calibrated confidence {engine['calibrated_confidence']:.1f}/100, regime {engine['regime']}.")
    else:
        drivers.append("No completed setup.stats.py run was found for this ticker; probability is therefore intraday-only and confidence is decayed.")
    risk_checks = [
        "Hard invalidation: two consecutive one-minute closes below the stop, or one close below it on abnormal volume.",
        f"Time stop: reassess after {holding_minutes} minutes if TP1 has not been reached; do not carry this day-trade plan overnight.",
        f"Estimated round-trip slippage allowance: {slippage_bps:.1f} bps. Reward/risk is shown after this allowance.",
    ]
    if gap_count:
        risk_checks.append(f"Data warning: {gap_count} intraday gap(s) above 2.5 minutes were detected in the displayed session.")
    if not volume_available:
        risk_checks.append("Volume is unavailable or structurally zero for this instrument; volume confirmation has been down-weighted.")
    return {
        "ok": True,
        "analysis_version": "position-intraday-long-v1",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "label": label,
        "name": item.get("name") or label,
        "symbol": item.get("symbol") or label,
        "exchange": item.get("exchange"),
        "currency": intraday.get("currency") or (quote_payload or {}).get("currency") or "",
        "market_open": market_open,
        "source": intraday.get("source") or "validated intraday fallback",
        "source_age_ms": source_age_ms,
        "delayed": bool(intraday.get("delayed") or (quote_payload or {}).get("delayed")),
        "current_price": round(current_price, 6),
        "session": {
            "open": round(session_open, 6),
            "high": round(max(highs), 6),
            "low": round(min(lows), 6),
            "delta_pct": round(delta_from_open, 4),
            "bars": len(session),
            "timezone": intraday.get("timezone") or "UTC",
        },
        "advice": {
            "side": "LONG",
            "action": action,
            "action_code": action_code,
            "setup": setup_name,
            "entry_zone": {"low": round(entry_low, 6), "high": round(entry_high, 6), "mid": round(entry_center, 6)},
            "entry_state": entry_state,
            "stop_loss": round(stop_loss, 6),
            "take_profit_1": round(take_profit_1, 6),
            "take_profit_2": round(take_profit_2, 6),
            "reward_risk_tp1": round(reward_risk, 3),
            "expected_value_r": round(expected_value_r, 3),
            "gain_probability": round(gain_probability, 4),
            "confidence_score": round(confidence_score, 2),
            "holding_minutes": holding_minutes,
            "invalidation": risk_checks[0],
        },
        "position_sizing": {
            "account_value": round(account_value, 2),
            "risk_percent": round(risk_percent, 3),
            "risk_budget": round(risk_budget, 2),
            "suggested_units_ceiling": suggested_units,
            "estimated_notional": round(suggested_units * entry_center, 2),
            "estimated_loss_at_stop": round(estimated_loss, 2),
            "estimated_gain_at_tp1": round(estimated_gain_1, 2),
        },
        "technical": {
            "vwap": round(vwap, 6),
            "ema_8": round(ema8, 6),
            "ema_21": round(ema21, 6),
            "atr_14": round(atr, 6),
            "atr_pct": round(atr_pct, 4),
            "rsi_14": round(rsi, 2),
            "momentum_5m_pct": round(momentum_5, 4),
            "momentum_15m_pct": round(momentum_15, 4),
            "momentum_30m_pct": round(momentum_30, 4),
            "slope_15m_pct_per_bar": round(slope_15, 5),
            "variance_ratio_5": round(variance_ratio, 4),
            "realised_volatility_ann_pct": round(realised_volatility, 2),
            "expected_move_30m": round(expected_move_30m, 6),
            "opening_range_high": round(opening_high, 6),
            "opening_range_low": round(opening_low, 6),
            "relative_volume": round(relative_volume, 3),
            "volume_zscore": round(volume_zscore, 3),
            "signed_volume_imbalance": round(volume_imbalance, 4),
            "return_volume_correlation": round(return_volume_correlation, 4),
            "long_edge": round(long_edge, 4),
            "overextended": overextended,
        },
        "model_context": {
            **engine,
            "weight_in_probability": round(engine_weight, 4),
            "calibration_quality": round(calibration_quality, 4),
            "intraday_probability_up": round(intraday_probability, 4),
            "data_quality_factor": round(data_quality, 4),
        },
        "feature_contributions": strongest,
        "drivers": drivers,
        "risk_checks": risk_checks,
        "chart": {"bars": session, "timezone": intraday.get("timezone") or "UTC"},
        "calculation_ms": round((time.perf_counter() - started) * 1000.0, 2),
        "disclaimer": "Statistical intraday Long setup research only. Levels are conditional, not guaranteed outcomes or personalised financial advice.",
    }


def fetch_position_advice(
    label: str,
    account_value: Any = 100_000,
    risk_percent: Any = 0.50,
    edge: str = "short",
    include_chart: bool = True,
) -> dict[str, Any]:
    request_started = time.perf_counter()
    lab = _tv_clean_token(label)
    requested_edge = str(edge or "short").strip().lower()
    edge_mode = requested_edge if requested_edge in {"short", "long", "long_intraday"} else "short"
    watch = {_tv_clean_token(row.get("label")): row for row in load_watchlist()}
    item = watch.get(lab)
    if not item:
        return {"ok": False, "label": lab, "error": "Ticker is not present in the setup.stats.py watchlist/TV mapping.", "retryable": False}
    parsed_capital = _num_or_none(account_value)
    capital = _position_clip(float(100_000.0 if parsed_capital is None else parsed_capital), 0.0, 1_000_000_000.0)
    risk = _position_clip(float(_num_or_none(risk_percent) or 0.50), 0.05, 5.0)
    LIVE_STREAM_HUB.subscribe([{**item, "live_priority": 10_000_000}], client_id="position-advice")
    snapshot = LIVE_STREAM_HUB.snapshot([lab], "auto")
    quote_payload = (snapshot.get("prices") or {}).get(lab)
    quote_sequence = int((quote_payload or {}).get("_stream_seq") or snapshot.get("sequence") or 0)
    cache_key = f"{edge_mode}|{lab}|{capital:.2f}|{risk:.4f}|{quote_sequence}"
    now = time.monotonic()
    with POSITION_ADVICE_LOCK:
        cached = POSITION_ADVICE_CACHE.get(cache_key)
        if cached and now - float(cached.get("ts") or 0.0) < POSITION_ADVICE_TTL_SEC:
            payload = dict(cached.get("payload") or {})
            payload["cached"] = True
            payload["response_ms"] = round((time.perf_counter() - request_started) * 1000.0, 2)
            if not include_chart and payload.get("ok"):
                payload["chart"] = {**dict(payload.get("chart") or {}), "bars": [], "reuse": True}
            return payload
    engine_context = _position_latest_engine_row(lab)
    if edge_mode == "long":
        hourly = _position_long_history_with_budget(lab, item)
        payload = _position_build_long_advice(lab, item, hourly, engine_context, quote_payload, capital, risk)
    else:
        intraday = _position_intraday_with_budget(lab, item)
        if edge_mode == "long_intraday":
            payload = _position_build_intraday_long_advice(lab, item, intraday, engine_context, quote_payload, capital, risk)
        else:
            payload = _position_build_advice(lab, item, intraday, engine_context, quote_payload, capital, risk)
    payload["edge"] = edge_mode
    payload["cached"] = False
    payload["response_ms"] = round((time.perf_counter() - request_started) * 1000.0, 2)
    with POSITION_ADVICE_LOCK:
        POSITION_ADVICE_CACHE[cache_key] = {"ts": now, "payload": payload}
        while len(POSITION_ADVICE_CACHE) > 24:
            oldest = min(
                POSITION_ADVICE_CACHE,
                key=lambda key: float(POSITION_ADVICE_CACHE[key].get("ts") or 0.0),
            )
            POSITION_ADVICE_CACHE.pop(oldest, None)
    if not include_chart and payload.get("ok"):
        response_payload = dict(payload)
        response_payload["chart"] = {**dict(payload.get("chart") or {}), "bars": [], "reuse": True}
        return response_payload
    return payload


def start_job(payload: dict[str, Any]) -> Job:
    JOBS_DIR.mkdir(exist_ok=True)
    job_id = uuid.uuid4().hex[:10]
    job_dir = JOBS_DIR / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    result_path = job_dir / "results.json"
    progress_path = job_dir / "progress.json"
    mode = str(payload.get("mode") or "deep")
    days = int(payload.get("days") or 10)
    bars = payload.get("bars")
    news = bool(payload.get("news", True))
    news_limit = int(payload.get("newsLimit") or 8)
    tickers = payload.get("tickers") or []
    cmd = [
        sys.executable, str(PREDICT_SCRIPT),
        "--mode", mode,
        "--days", str(days),
        "--news-limit", str(news_limit),
        "--json-out", str(result_path),
        "--progress-file", str(progress_path),
        "--quiet",
    ]
    if bars:
        cmd += ["--bars", str(int(bars))]
    if not news:
        cmd.append("--no-news")
    if tickers:
        cmd += ["--only", *[str(x).upper() for x in tickers]]
    job = Job(id=job_id, command=cmd, result_path=result_path, progress_path=progress_path)

    def runner() -> None:
        job.started_at = time.time()
        try:
            with open(job_dir / "engine.log", "w", encoding="utf-8") as log:
                job.process = subprocess.Popen(
                    cmd,
                    cwd=str(BASE_DIR),
                    text=True,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                )
                job.returncode = job.process.wait()
                if job.returncode != 0:
                    job.error = "Le moteur de prédiction v15 a échoué. Vérifie que setup.stats.py est bien dans le même dossier et consulte engine.log."
                elif job.result_path.exists():
                    try:
                        raw = json.loads(job.result_path.read_text(encoding="utf-8"))
                        learned = learning_client.process_payload(raw, source="visible_run", mode=mode, store=True)
                        _learning_success("visible_run", learned)
                        job.result_path.write_text(json.dumps(learned, ensure_ascii=False, indent=2), encoding="utf-8")
                    except Exception as learn_exc:
                        # Forecast stays visible even if the learning database has an issue.
                        _learning_failure("visible_run", learn_exc)
                        try:
                            raw = json.loads(job.result_path.read_text(encoding="utf-8"))
                            raw["server_learning_error"] = str(learn_exc)
                            raw["server_learning"] = {
                                "version": "15.0",
                                "applied": False,
                                "status": "remote_error",
                                "error": str(learn_exc),
                            }
                            job.result_path.write_text(json.dumps(raw, ensure_ascii=False, indent=2), encoding="utf-8")
                        except Exception:
                            pass
        except Exception as exc:
            job.error = str(exc)
        finally:
            job.ended_at = time.time()
            if job.returncode == 0 and job.result_path.exists():
                threading.Thread(target=_background_auto_run, args=(payload,), daemon=True).start()

    with LOCK:
        JOBS[job_id] = job
    threading.Thread(target=runner, daemon=True).start()
    return job


def _background_auto_run(payload: dict[str, Any]) -> None:
    """Silent durable shadow run after a visible run.

    It updates the server learning database but never writes to the UI. It is
    intentionally compact so it does not overload the machine.
    """
    try:
        if not learning_client.configured():
            return
        tickers = payload.get("tickers") or []
        if not tickers:
            tickers = ["SPY", "IXIC", "SMH", "AAPL", "MSFT", "NVDA"]
        tickers = [str(x).upper() for x in tickers[:8]]
        tmp_dir = JOBS_DIR / ("_auto_" + uuid.uuid4().hex[:8])
        tmp_dir.mkdir(parents=True, exist_ok=True)
        result_path = tmp_dir / "shadow.json"
        cmd = [
            sys.executable, str(PREDICT_SCRIPT),
            "--mode", "fast",
            "--days", str(int(payload.get("days") or 10)),
            "--bars", "900",
            "--news-limit", str(max(6, min(int(payload.get("newsLimit") or 8), 16))),
            "--json-out", str(result_path),
            "--quiet",
            "--no-store-learning",
            "--only", *tickers,
        ]
        if not bool(payload.get("news", True)):
            cmd.append("--no-news")
        cp = subprocess.run(cmd, cwd=str(BASE_DIR), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=240)
        if cp.returncode == 0 and result_path.exists():
            raw = json.loads(result_path.read_text(encoding="utf-8"))
            learned = learning_client.process_payload(raw, source="post_visible_shadow", mode="fast", store=True)
            _learning_success("post_visible_shadow", learned)
    except Exception as exc:
        _learning_failure("post_visible_shadow", exc)
    finally:
        try:
            shutil.rmtree(tmp_dir, ignore_errors=True)  # type: ignore[name-defined]
        except Exception:
            pass


def stop_job(job_id: str) -> bool:
    with LOCK:
        job = JOBS.get(job_id)
    if not job or not job.running:
        return False
    try:
        job.process.terminate()
        return True
    except Exception:
        return False


def job_payload(job: Job) -> dict[str, Any]:
    progress = read_json(job.progress_path, {"percent": 0, "message": "En attente", "current": 0, "total": 0})
    result = read_json(job.result_path, None)
    return {
        "id": job.id,
        "running": job.running,
        "returncode": job.returncode,
        "error": job.error,
        "createdAt": job.created_at,
        "startedAt": job.started_at,
        "endedAt": job.ended_at,
        "progress": progress,
        "result": result,
    }


HTML = r"""<!doctype html>
<html lang="en-US">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<link rel="icon" type="image/png" sizes="400x400" href="/assets/apex-tool-logo.png?v=15" />
<link rel="apple-touch-icon" href="/assets/apex-tool-logo.png?v=15" />
<title>Apex Market Predictor v15</title>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
<style>
:root{--bg:#070a10;--surface:#0e1422;--surface2:#121b2c;--card:#131d31;--line:#253047;--muted:#94a3b8;--soft:#c7d2fe;--text:#edf4ff;--blue:#3b82f6;--cyan:#22d3ee;--green:#22c55e;--red:#ef4444;--amber:#f59e0b;--purple:#a78bfa;--shadow:0 24px 70px rgba(0,0,0,.38)}
*{box-sizing:border-box}html{min-height:100%;overflow-y:scroll;scrollbar-color:#334155 #070a10;scrollbar-width:thin}body{margin:0;min-height:100vh;overflow-y:auto;overflow-x:hidden;background:radial-gradient(circle at 15% -8%,rgba(59,130,246,.20),transparent 32%),radial-gradient(circle at 95% 0%,rgba(34,211,238,.12),transparent 30%),linear-gradient(180deg,#080b12,#070a10 45%,#05070c);color:var(--text);font-family:Inter,ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"SF Pro Display","Segoe UI",sans-serif}body::-webkit-scrollbar,.side::-webkit-scrollbar,.tableWrap::-webkit-scrollbar,.details::-webkit-scrollbar,.tickerList::-webkit-scrollbar{width:11px;height:11px}body::-webkit-scrollbar-track,.side::-webkit-scrollbar-track,.tableWrap::-webkit-scrollbar-track,.details::-webkit-scrollbar-track,.tickerList::-webkit-scrollbar-track{background:#070a10}body::-webkit-scrollbar-thumb,.side::-webkit-scrollbar-thumb,.tableWrap::-webkit-scrollbar-thumb,.details::-webkit-scrollbar-thumb,.tickerList::-webkit-scrollbar-thumb{background:linear-gradient(180deg,#334155,#1d4ed8);border:3px solid #070a10;border-radius:999px}button,input,select{font:inherit}button{cursor:pointer}.app{min-height:100vh;display:grid;grid-template-columns:320px minmax(0,1fr);align-items:start}.side{position:sticky;top:0;height:100vh;overflow-y:auto;background:linear-gradient(180deg,rgba(15,23,42,.98),rgba(7,10,16,.98));border-right:1px solid var(--line);padding:22px}.brand{display:flex;align-items:center;gap:12px;margin-bottom:18px}.logo{width:44px;height:44px;border-radius:15px;background:linear-gradient(135deg,var(--blue),var(--cyan));display:grid;place-items:center;box-shadow:0 0 35px rgba(34,211,238,.28);font-weight:950}.brand h1{font-size:18px;margin:0;line-height:1}.brand p{margin:4px 0 0;color:var(--muted);font-size:12px}.sideHint{color:#8ea2bd;font-size:12px;line-height:1.45;margin:0 0 18px}.section{padding:16px;border:1px solid var(--line);background:rgba(15,23,42,.76);border-radius:20px;margin-bottom:16px;box-shadow:0 14px 35px rgba(0,0,0,.18)}.section h2{font-size:12px;text-transform:uppercase;letter-spacing:.12em;color:#c7d2fe;margin:0 0 12px}.grid2{display:grid;grid-template-columns:1fr 1fr;gap:10px}.field{margin-bottom:12px}.field label{display:block;color:var(--muted);font-size:12px;margin-bottom:6px}.field input,.field select{width:100%;background:#090e19;color:var(--text);border:1px solid #253149;border-radius:13px;padding:11px 12px;outline:none}.field input:focus,.field select:focus{border-color:var(--blue);box-shadow:0 0 0 3px rgba(59,130,246,.16)}.actions{display:grid;grid-template-columns:1fr 1fr;gap:10px}.primary{background:linear-gradient(135deg,var(--blue),#06b6d4);border:0;color:#fff;border-radius:15px;padding:13px;font-weight:850;box-shadow:0 18px 36px rgba(59,130,246,.25)}.primary:disabled{opacity:.5;cursor:not-allowed}.danger{background:rgba(239,68,68,.12);color:#fecaca;border:1px solid rgba(239,68,68,.45);border-radius:15px;padding:13px;font-weight:800}.tickerTools{display:flex;gap:8px;margin-bottom:10px}.tickerTools button{flex:1;background:#0b1220;border:1px solid #253149;color:#dbeafe;border-radius:12px;padding:8px;font-size:12px}.tickerList{max-height:40vh;overflow:auto;display:grid;gap:8px;padding-right:4px}.ticker{display:flex;align-items:center;gap:9px;padding:10px;border:1px solid #22304a;border-radius:13px;background:#0a1020;transition:.15s}.ticker:hover{border-color:#3b82f6;background:#0c1528}.ticker input{accent-color:var(--blue)}.ticker b{font-size:13px}.ticker span{font-size:11px;color:var(--muted);display:block;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:190px}.progress{height:9px;border-radius:999px;background:#101827;overflow:hidden;border:1px solid #263044}.bar{height:100%;width:0%;background:linear-gradient(90deg,var(--blue),var(--cyan));transition:.25s}.main{min-width:0;min-height:100vh;display:flex;flex-direction:column}.top{position:sticky;top:0;z-index:30;display:flex;align-items:center;justify-content:space-between;border-bottom:1px solid rgba(37,48,71,.9);padding:16px 26px;background:rgba(7,10,16,.78);backdrop-filter:blur(20px)}.top h2{margin:0;font-size:19px}.dashTitle{display:flex;align-items:center;gap:10px;letter-spacing:-.02em}.dashTitle:before{content:"";width:11px;height:28px;border-radius:999px;background:linear-gradient(180deg,var(--cyan),var(--blue));box-shadow:0 0 26px rgba(34,211,238,.55)}.versionLine{display:inline-flex;margin:6px 0 0;color:#8fb5ff;font-size:12px;border:1px solid rgba(59,130,246,.28);background:rgba(59,130,246,.08);padding:3px 9px;border-radius:999px}.top p{margin:4px 0 0;color:var(--muted);font-size:12px}.status{display:flex;align-items:center;gap:12px;background:rgba(15,23,42,.85);border:1px solid var(--line);padding:10px 13px;border-radius:999px}.dot{width:10px;height:10px;border-radius:50%;background:var(--muted);box-shadow:0 0 18px currentColor}.dot.run{background:var(--green)}.dot.err{background:var(--red)}.cards{display:grid;grid-template-columns:repeat(5,minmax(140px,1fr));gap:16px;padding:18px 26px}.card{background:linear-gradient(180deg,rgba(18,26,42,.96),rgba(13,17,28,.96));border:1px solid var(--line);border-radius:22px;padding:17px;box-shadow:var(--shadow)}.card .k{color:var(--muted);font-size:12px;text-transform:uppercase;letter-spacing:.1em}.card .v{font-size:28px;font-weight:950;margin-top:8px;letter-spacing:-.02em}.card .s{font-size:12px;color:var(--muted);margin-top:4px}.work{display:grid;grid-template-columns:minmax(0,1.55fr) minmax(400px,.65fr);gap:18px;padding:0 26px 42px;align-items:start}.chartPanel{grid-column:1;grid-row:1}.profilePanel{grid-column:1;grid-row:2;margin-top:0}.right{grid-column:2;grid-row:1 / span 2;display:grid;gap:18px;position:sticky;top:92px;align-self:start}.panel{background:rgba(13,17,28,.94);border:1px solid var(--line);border-radius:24px;box-shadow:var(--shadow);overflow:hidden}.panelHead{min-height:60px;display:flex;align-items:center;justify-content:space-between;gap:14px;padding:0 20px;border-bottom:1px solid var(--line)}.panelHead h3{margin:0;font-size:15px}.panelHead span{color:var(--muted);font-size:12px}.chartPanel{min-height:700px}.chart{height:635px;min-height:520px}.tablePanel{min-height:430px}.rankHead{min-height:86px;align-items:center;gap:12px}.rankTitle{display:flex;flex-direction:column;gap:3px;min-width:112px}.rankTitle h3{line-height:1.05}.rankTitle span{font-size:11px;color:#8fa1ba}.rankControls{display:flex;align-items:flex-end;justify-content:flex-end;gap:8px;flex:1;min-width:0}.rankControls label{display:flex;flex-direction:column;gap:4px;color:#8fa1ba;font-size:9px;text-transform:uppercase;letter-spacing:.1em;white-space:nowrap}.rankControls select{height:36px;max-width:144px;min-width:120px;background:#090e19;color:#e5eefc;border:1px solid #253149;border-radius:13px;padding:0 9px;outline:none}.rankControls select:focus{border-color:var(--blue);box-shadow:0 0 0 3px rgba(59,130,246,.14)}@media(max-width:1450px){.rankHead{align-items:flex-start;flex-direction:column;height:auto;padding:14px 16px}.rankControls{width:100%;justify-content:space-between}.rankControls label{flex:1}.rankControls select{max-width:none;width:100%}}.rankSearch{padding:12px 16px;border-bottom:1px solid var(--line);background:rgba(11,18,32,.62)}.rankSearch input{width:100%;background:#090e19;color:var(--text);border:1px solid #253149;border-radius:13px;padding:11px 12px;outline:none}.rankSearch input:focus{border-color:var(--blue);box-shadow:0 0 0 3px rgba(59,130,246,.16)}.tableWrap{max-height:382px;overflow:auto}.resultTable{width:100%;border-collapse:collapse}.resultTable th{position:sticky;top:0;background:#101827;color:#93a4bd;text-align:left;font-size:11px;text-transform:uppercase;letter-spacing:.09em;padding:13px;border-bottom:1px solid var(--line);z-index:2}.resultTable td{padding:13px;border-bottom:1px solid rgba(38,48,68,.65);font-size:13px;vertical-align:top}.resultTable tr{cursor:pointer}.resultTable tr:hover,.resultTable tr.active{background:rgba(59,130,246,.10)}.sym{font-weight:950}.name{color:var(--muted);font-size:11px}.pos{color:#4ade80}.neg{color:#fb7185}.neu{color:#fbbf24}.badge{display:inline-flex;align-items:center;border-radius:999px;padding:5px 9px;font-size:11px;font-weight:850;border:1px solid #31405e;background:#0b1220;white-space:nowrap}.badge.green{color:#bbf7d0;border-color:rgba(34,197,94,.45);background:rgba(34,197,94,.1)}.badge.red{color:#fecaca;border-color:rgba(239,68,68,.45);background:rgba(239,68,68,.1)}.badge.amber{color:#fde68a;border-color:rgba(245,158,11,.45);background:rgba(245,158,11,.1)}.details{max-height:760px;overflow:auto;padding:18px}.bigTicker{display:flex;justify-content:space-between;gap:12px;align-items:flex-start}.bigTicker h3{margin:0;font-size:30px;letter-spacing:-.03em}.bigTicker p{margin:4px 0 0;color:var(--muted)}.priceBox{text-align:right}.priceBox .price{font-size:27px;font-weight:950}.forecastGrid{display:grid;grid-template-columns:repeat(auto-fit,minmax(82px,1fr));gap:9px;margin:18px 0}.day{border:1px solid #263044;background:#0a1020;border-radius:15px;padding:11px;text-align:center}.day .d{color:var(--muted);font-size:11px}.day .p{font-weight:850;margin:5px 0}.metrics{display:grid;grid-template-columns:repeat(2,1fr);gap:10px}.metric{background:#0a1020;border:1px solid #263044;border-radius:15px;padding:12px}.metric span{display:block;color:var(--muted);font-size:11px}.metric b{display:block;font-size:18px;margin-top:4px}.news{margin-top:16px}.news h4{margin:0 0 10px}.newsItem{padding:11px 0;border-bottom:1px solid rgba(38,48,68,.65);font-size:12px;color:#cbd5e1;line-height:1.4}.newsItem small{display:block;color:#7dd3fc;margin-top:4px}.newsLink{display:block;text-decoration:none;color:#cbd5e1;border-radius:12px;padding:11px 10px;margin:0 -10px}.newsLink:hover{background:rgba(59,130,246,.11);border-bottom-color:rgba(59,130,246,.25)}.newsLink strong{display:block;font-weight:750;color:#e5eefc}.newsLink em{font-style:normal;color:#94a3b8;word-break:break-all}.profilePanel{margin-top:18px}.profileBody{padding:18px}.profileTop{display:flex;align-items:flex-start;justify-content:space-between;gap:18px;margin-bottom:16px}.profileTitle h3{margin:0 0 6px;font-size:19px}.profileTitle p{margin:0;color:#b7c4d8;line-height:1.55;max-width:760px}.pillRow{display:flex;flex-wrap:wrap;gap:8px;margin-top:12px}.pill{display:inline-flex;align-items:center;border-radius:999px;padding:7px 10px;background:#0a1020;border:1px solid #263044;color:#cbd5e1;font-size:12px}.profileStats{display:grid;grid-template-columns:repeat(4,minmax(150px,1fr));gap:10px}.statBox{background:#0a1020;border:1px solid #263044;border-radius:15px;padding:12px}.statBox span{display:block;color:#8fa1ba;font-size:10px;text-transform:uppercase;letter-spacing:.09em}.statBox b{display:block;margin-top:6px;font-size:15px}.investBox{display:flex;align-items:center;gap:16px;background:linear-gradient(135deg,rgba(56,189,248,.08),rgba(59,130,246,.05));border:1px solid #263044;border-radius:18px;padding:14px;min-width:260px}.scoreRing{width:76px;height:76px;border-radius:50%;display:grid;place-items:center;border:1px solid rgba(56,189,248,.65);box-shadow:inset 0 0 26px rgba(56,189,248,.08);font-size:23px;font-weight:950}.scoreRing small{font-size:11px;color:#94a3b8;font-weight:700}.investBox h4{margin:0 0 4px;font-size:15px}.investBox p{margin:0;color:#94a3b8;font-size:12px;line-height:1.45}.driverList{display:grid;gap:9px;margin-top:14px}.driver{padding:11px 12px;border:1px solid #263044;background:#0a1020;border-radius:14px;color:#cbd5e1;font-size:12px;line-height:1.45}.methodGrid{display:grid;grid-template-columns:repeat(2,1fr);gap:10px;margin-top:14px}.methodCard{background:#0a1020;border:1px solid #263044;border-radius:15px;padding:12px}.methodCard span{display:block;color:#8fa1ba;font-size:11px}.methodCard b{display:block;font-size:16px;margin:4px 0}.methodCard p{margin:0;color:#94a3b8;font-size:11px;line-height:1.35}.analysisBox{margin-top:14px;padding:14px;border:1px solid #263044;background:#0a1020;border-radius:15px}.analysisBox h4{margin:0 0 8px;font-size:14px}.analysisBox p{margin:0;color:#cbd5e1;font-size:12px;line-height:1.55}.analysisList{margin:10px 0 0;padding-left:18px;color:#cbd5e1;font-size:12px;line-height:1.55}.smallMuted{color:#94a3b8;font-size:11px}
.menuToggle{width:38px;height:38px;min-width:38px;border:1px solid #253149;background:linear-gradient(180deg,#111b2e,#0a1020);border-radius:13px;display:grid;place-items:center;gap:0;padding:8px;box-shadow:0 12px 28px rgba(0,0,0,.22)}.menuToggle span{display:block;width:17px;height:2px;border-radius:999px;background:linear-gradient(90deg,var(--cyan),var(--blue));box-shadow:0 0 12px rgba(34,211,238,.35)}.menuToggle:hover{border-color:#38bdf8;box-shadow:0 0 0 3px rgba(56,189,248,.12)}.brandText{min-width:0}.app.side-collapsed{grid-template-columns:76px minmax(0,1fr)}.app.side-collapsed .side{padding:16px 10px;overflow:hidden}.app.side-collapsed .brand{flex-direction:column;align-items:center;gap:12px}.app.side-collapsed .brandText,.app.side-collapsed .section{display:none}.app.side-collapsed .logo{width:42px;height:42px;border-radius:14px}.app.side-collapsed .main{min-width:0}.rankHead{min-height:86px;align-items:center;gap:12px;padding:0 16px}.rankHead h3{padding-top:0}.rankTitle{display:flex;flex-direction:column;gap:3px;min-width:112px}.rankControls{display:flex;align-items:flex-end;justify-content:flex-end;gap:8px;flex-wrap:nowrap;flex:1}.rankControls label{display:flex;flex-direction:column;gap:4px;color:#8fa1ba;font-size:9px;text-transform:uppercase;letter-spacing:.1em;white-space:nowrap}.rankControls select{height:36px;min-width:120px;max-width:144px;background:#090e19;color:var(--text);border:1px solid #253149;border-radius:13px;padding:0 9px;outline:none;text-transform:none;letter-spacing:0;font-size:12px}.rankControls select:focus{border-color:var(--blue);box-shadow:0 0 0 3px rgba(59,130,246,.16)}.resultTable th,.resultTable td{white-space:nowrap}.resultTable td:first-child,.resultTable th:first-child{white-space:normal;min-width:95px}.resultTable td:nth-child(5){white-space:normal}.dayCell b{display:block}.dayCell .name{margin-top:2px}@media(max-width:1280px){.app.side-collapsed{grid-template-columns:1fr}.app.side-collapsed .side{height:auto;width:auto}.app.side-collapsed .brand{flex-direction:row;justify-content:flex-start}.app.side-collapsed .logo{display:grid}.rankControls{justify-content:flex-start}.rankHead{display:grid;gap:10px}}
@media(max-width:960px){.profileTop{flex-direction:column}.profileStats{grid-template-columns:repeat(2,1fr)}.methodGrid{grid-template-columns:1fr}}.empty{min-height:100%;display:grid;place-items:center;text-align:center;color:var(--muted);padding:30px}.hidden{display:none!important}@media(max-width:1280px){.app{grid-template-columns:1fr}.side{position:relative;height:auto;max-height:none}.work{grid-template-columns:1fr}.chartPanel,.profilePanel,.right{grid-column:1;grid-row:auto;position:relative;top:auto}.cards{grid-template-columns:repeat(2,1fr)}.chartPanel{min-height:640px}.chart{height:580px}.tickerList{max-height:280px}}@media(max-width:760px){.cards{grid-template-columns:1fr}.forecastGrid{grid-template-columns:1fr 1fr}.grid2,.actions{grid-template-columns:1fr}.top{align-items:flex-start;flex-direction:column}.work{padding:0 14px 32px}.cards{padding:14px}.side{padding:14px}}
/* v13.7: Ranked forecasts sort controls layout fix. Keep title readable with sidebar open/collapsed. */
.tablePanel .rankHead{
  min-height:auto!important;
  height:auto!important;
  display:flex!important;
  flex-direction:column!important;
  align-items:stretch!important;
  justify-content:flex-start!important;
  gap:12px!important;
  padding:16px 16px 14px!important;
}
.tablePanel .rankTitle{
  width:100%!important;
  min-width:0!important;
  display:flex!important;
  flex-direction:row!important;
  align-items:flex-end!important;
  justify-content:space-between!important;
  gap:10px!important;
}
.tablePanel .rankTitle h3{
  display:block!important;
  margin:0!important;
  padding:0!important;
  line-height:1.12!important;
  white-space:normal!important;
  overflow:visible!important;
  text-overflow:clip!important;
  max-width:none!important;
}
.tablePanel .rankTitle span{
  display:block!important;
  flex:0 0 auto!important;
  font-size:11px!important;
  white-space:nowrap!important;
}
.tablePanel .rankControls{
  width:100%!important;
  display:grid!important;
  grid-template-columns:minmax(0,1fr) minmax(0,1fr)!important;
  gap:10px!important;
  align-items:end!important;
  justify-content:stretch!important;
  flex:none!important;
  margin-top:2px!important;
}
.tablePanel .rankControls label{
  min-width:0!important;
  width:100%!important;
  display:flex!important;
  flex-direction:column!important;
  gap:5px!important;
  color:#8fa1ba!important;
  font-size:9px!important;
  line-height:1!important;
  text-transform:uppercase!important;
  letter-spacing:.105em!important;
}
.tablePanel .rankControls select{
  width:100%!important;
  min-width:0!important;
  max-width:none!important;
  height:38px!important;
  border-radius:13px!important;
  background:linear-gradient(180deg,#0b1220,#080d18)!important;
  border:1px solid #253149!important;
  color:#e5eefc!important;
  padding:0 10px!important;
  font-size:12px!important;
  letter-spacing:0!important;
  text-transform:none!important;
}
.tablePanel .rankSearch{
  padding-top:10px!important;
}
@media(max-width:520px){
  .tablePanel .rankControls{grid-template-columns:1fr!important;}
  .tablePanel .rankTitle{align-items:flex-start!important;flex-direction:column!important;}
}

/* v14.6.2 targeted patch: Ticker Tracker full-height sidebar list + A-Z tracker display. */
.side{display:flex;flex-direction:column;min-height:100vh;}
.tickerTrackerSection{flex:1;min-height:0;display:flex;flex-direction:column;margin-bottom:0;}
.tickerTrackerSection .field,.tickerTrackerSection .tickerTools{flex:0 0 auto;}
.tickerTrackerSection .tickerList{flex:1;min-height:180px;max-height:none!important;overflow:auto;padding-bottom:18px;}
.app.side-collapsed .tickerTrackerSection{display:none!important;}
@media(max-width:1280px){
  .side{display:block;min-height:auto;}
  .tickerTrackerSection{display:block;min-height:0;}
  .tickerTrackerSection .tickerList{max-height:280px!important;min-height:0;}
}

/* v14.7: return prevision, fullscreen chart and default collapsed sidebar. */
.side{gap:0}.returnConfigSection{flex:0 0 auto;max-height:46vh;overflow:auto}.returnDates{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:10px}.returnConfigTable{width:100%;border-collapse:separate;border-spacing:0 7px}.returnConfigTable th{color:#8fa1ba;font-size:9px;text-transform:uppercase;letter-spacing:.1em;text-align:left;font-weight:800}.returnConfigTable td{vertical-align:middle}.returnConfigTable select,.returnConfigTable input{width:100%;height:34px;background:#090e19;color:var(--text);border:1px solid #253149;border-radius:10px;padding:0 8px;outline:none;font-size:12px}.returnConfigTable input{text-align:right}.returnConfigTable select:focus,.returnConfigTable input:focus{border-color:var(--blue);box-shadow:0 0 0 3px rgba(59,130,246,.14)}.returnHelp{font-size:11px;line-height:1.35;color:#8fa1ba;margin:8px 0 10px}.returnRun{width:100%;height:40px;border:0;border-radius:14px;background:linear-gradient(135deg,#22c55e,#06b6d4);color:white;font-weight:900;box-shadow:0 14px 28px rgba(34,197,94,.18)}.returnRun:disabled{opacity:.55;cursor:not-allowed}.returnPanel{grid-column:1;grid-row:3;margin-top:0}.right{grid-row:1 / span 3}.returnPanel .panelHead{min-height:66px}.returnTableWrap{max-height:560px;overflow:auto;padding:0 0 12px}.returnTable{width:100%;border-collapse:collapse;min-width:980px}.returnTable th{position:sticky;top:0;background:#101827;color:#dbeafe;text-align:right;font-size:11px;text-transform:uppercase;letter-spacing:.06em;padding:12px;border-bottom:1px solid var(--line);z-index:2}.returnTable th:first-child,.returnTable td:first-child{text-align:left;position:sticky;left:0;background:#101827;z-index:3}.returnTable td{padding:11px 12px;border-bottom:1px solid rgba(38,48,68,.65);font-size:12px;text-align:right;background:#0a1020}.returnTable tr:nth-child(even) td{background:#0c1323}.returnTable tr.totalRow td{background:linear-gradient(180deg,rgba(59,130,246,.18),rgba(14,20,34,.98));font-weight:950;color:#e0f2fe}.returnHeadCell{display:flex;flex-direction:column;align-items:flex-end;gap:3px}.returnHeadCell b{font-size:12px;color:#fff}.returnHeadCell span{font-size:10px;color:#93c5fd;text-transform:none;letter-spacing:0}.returnStatus{color:#94a3b8;font-size:12px}.miniBtn{height:34px;border:1px solid #253149;background:#0b1220;color:#dbeafe;border-radius:12px;padding:0 12px;font-size:12px;font-weight:850}.miniBtn:hover{border-color:#38bdf8;box-shadow:0 0 0 3px rgba(56,189,248,.12)}.chartPanel:fullscreen{background:#070a10;padding:18px}.chartPanel:fullscreen .chart{height:calc(100vh - 98px)!important;min-height:calc(100vh - 98px)}.chartPanel.chartFull{position:fixed;inset:12px;z-index:999;background:#070a10}.chartPanel.chartFull .chart{height:calc(100vh - 110px)}.chartInstruction{display:grid;place-items:center;min-height:100%;color:#8fb5ff;font-weight:850;letter-spacing:.01em;text-align:center;padding:30px}.chartInstruction small{display:block;color:#94a3b8;font-weight:600;margin-top:7px}.app.side-collapsed .returnConfigSection{display:none!important}@media(max-width:1280px){.returnPanel{grid-column:1;grid-row:auto}.right{grid-row:auto}.returnConfigSection{max-height:none}.returnTable{min-width:820px}}


/* v14.7.2 targeted patch: sidebar bottom-space allocation for Ticker Tracker + Total Return Config. */
.side{
  height:100vh!important;
  overflow:hidden!important;
  display:flex!important;
  flex-direction:column!important;
  gap:12px!important;
  padding:18px!important;
}
.side>.brand{flex:0 0 auto!important;margin-bottom:0!important;}
.side>.section{margin-bottom:0!important;}
.side>.section:not(.tickerTrackerSection):not(.returnConfigSection){flex:0 0 auto!important;}
.tickerTrackerSection,
.returnConfigSection{
  display:flex!important;
  flex-direction:column!important;
  overflow:hidden!important;
  min-height:0!important;
  max-height:none!important;
}
.tickerTrackerSection{flex:1.05 1 0!important;}
.returnConfigSection{flex:.95 1 0!important;}
.tickerTrackerSection h2,
.returnConfigSection h2{flex:0 0 auto!important;margin-bottom:10px!important;}
.tickerTrackerSection .field,
.tickerTrackerSection .tickerTools,
.returnConfigSection .returnDates,
.returnConfigSection .returnHelp,
.returnConfigSection .returnRun{flex:0 0 auto!important;}
.tickerTrackerSection .tickerList{
  flex:1 1 auto!important;
  min-height:0!important;
  max-height:none!important;
  overflow:auto!important;
  padding-bottom:24px!important;
}
.returnRowsWrap{
  flex:1 1 auto!important;
  min-height:0!important;
  overflow:auto!important;
  padding-right:4px!important;
  margin:0 0 10px!important;
}
.returnConfigTable{margin:0!important;}
.returnConfigTable thead th{position:sticky;top:0;z-index:2;background:#0f172a;padding-bottom:6px!important;}
.returnConfigTable select,.returnConfigTable input{height:32px!important;}
.returnHelp{margin:4px 0 8px!important;}
.returnRun{margin-top:0!important;}
.app.side-collapsed .side{
  height:100vh!important;
  overflow:hidden!important;
  padding:16px 10px!important;
}
.app.side-collapsed .section{display:none!important;}
.app.side-collapsed .brand{margin-bottom:0!important;}
@media(max-height:820px){
  .side{padding:14px!important;gap:9px!important;}
  .section{padding:12px!important;border-radius:17px!important;}
  .brand{gap:9px!important;}
  .field{margin-bottom:8px!important;}
  .actions{gap:8px!important;}
  .primary,.danger{padding:10px!important;}
  .tickerTools{gap:6px!important;margin-bottom:7px!important;}
  .ticker{padding:8px!important;}
  .returnHelp{font-size:10px!important;line-height:1.25!important;}
  .returnConfigTable{border-spacing:0 5px!important;}
  .returnConfigTable select,.returnConfigTable input{height:30px!important;font-size:11px!important;}
  .returnRun{height:36px!important;border-radius:12px!important;}
}
@media(max-width:1280px){
  .side{height:auto!important;min-height:auto!important;overflow:visible!important;display:block!important;}
  .tickerTrackerSection,.returnConfigSection{display:block!important;overflow:visible!important;}
  .tickerTrackerSection .tickerList{max-height:300px!important;overflow:auto!important;}
  .returnRowsWrap{max-height:340px!important;overflow:auto!important;}
  .app.side-collapsed .side{height:auto!important;overflow:hidden!important;}
}


/* v14.7.3 final sidebar usability patch.
   Fixed full-height Apex Predictor panel + large usable dataframes. */
.app{
  display:block!important;
  min-height:100vh!important;
}
.side{
  position:fixed!important;
  left:0!important;
  top:0!important;
  bottom:0!important;
  width:390px!important;
  height:100vh!important;
  max-height:100vh!important;
  overflow-y:auto!important;
  overflow-x:hidden!important;
  display:block!important;
  padding:18px 18px 34px!important;
  background:linear-gradient(180deg,rgba(15,23,42,.98),rgba(7,10,16,.985))!important;
  border-right:1px solid var(--line)!important;
  z-index:60!important;
  scrollbar-gutter:stable!important;
}
.main{
  margin-left:390px!important;
  min-width:0!important;
  width:auto!important;
}
.side>.brand{
  position:sticky!important;
  top:-18px!important;
  z-index:5!important;
  margin:-18px -18px 16px!important;
  padding:18px 18px 13px!important;
  background:linear-gradient(180deg,rgba(15,23,42,.99),rgba(15,23,42,.92))!important;
  border-bottom:1px solid rgba(37,48,71,.72)!important;
  backdrop-filter:blur(18px)!important;
}
.side>.section{
  margin:0 0 18px!important;
  overflow:visible!important;
}
.side>.section:not(.tickerTrackerSection):not(.returnConfigSection){
  height:auto!important;
  min-height:0!important;
  max-height:none!important;
  flex:none!important;
}
.tickerTrackerSection{
  display:flex!important;
  flex-direction:column!important;
  min-height:680px!important;
  height:680px!important;
  max-height:none!important;
  overflow:hidden!important;
  padding:16px!important;
}
.tickerTrackerSection h2,
.returnConfigSection h2{
  flex:0 0 auto!important;
  margin:0 0 12px!important;
}
.tickerTrackerSection .field,
.tickerTrackerSection .tickerTools{
  flex:0 0 auto!important;
}
.tickerTrackerSection .tickerList{
  flex:1 1 auto!important;
  min-height:500px!important;
  height:auto!important;
  max-height:none!important;
  overflow-y:auto!important;
  overflow-x:auto!important;
  display:grid!important;
  gap:8px!important;
  padding:0 7px 18px 0!important;
  scrollbar-gutter:stable!important;
}
.tickerTrackerSection .ticker{
  min-height:49px!important;
}
.tickerTrackerSection .ticker span{
  max-width:280px!important;
}
.returnConfigSection{
  display:flex!important;
  flex-direction:column!important;
  min-height:760px!important;
  height:760px!important;
  max-height:none!important;
  overflow:hidden!important;
  padding:16px!important;
}
.returnDates{
  flex:0 0 auto!important;
  grid-template-columns:1fr 1fr!important;
  gap:10px!important;
}
.returnConfigSection .field{
  margin-bottom:10px!important;
}
.returnHelp{
  flex:0 0 auto!important;
  margin:6px 0 10px!important;
  font-size:11px!important;
  line-height:1.35!important;
}
.returnRowsWrap{
  flex:1 1 auto!important;
  min-height:545px!important;
  height:auto!important;
  max-height:none!important;
  overflow-y:auto!important;
  overflow-x:auto!important;
  margin:0 0 12px!important;
  padding:0 7px 16px 0!important;
  scrollbar-gutter:stable!important;
}
.returnConfigTable{
  width:100%!important;
  min-width:322px!important;
  border-spacing:0 8px!important;
}
.returnConfigTable thead th{
  position:sticky!important;
  top:0!important;
  z-index:4!important;
  background:#0f172a!important;
  padding:4px 0 8px!important;
}
.returnConfigTable select,
.returnConfigTable input{
  height:38px!important;
  min-height:38px!important;
  font-size:12px!important;
}
.returnRun{
  flex:0 0 auto!important;
  position:sticky!important;
  bottom:0!important;
  z-index:5!important;
  height:44px!important;
  min-height:44px!important;
  margin:0!important;
  border-radius:15px!important;
  box-shadow:0 -12px 28px rgba(7,10,16,.72),0 14px 28px rgba(34,197,94,.16)!important;
}
.app.side-collapsed .side{
  width:76px!important;
  padding:16px 10px!important;
  overflow:hidden!important;
}
.app.side-collapsed .main{
  margin-left:76px!important;
}
.app.side-collapsed .brand{
  position:static!important;
  margin:0!important;
  padding:0!important;
  border-bottom:0!important;
  background:transparent!important;
  flex-direction:column!important;
  align-items:center!important;
}
.app.side-collapsed .brandText,
.app.side-collapsed .section{
  display:none!important;
}
.app.side-collapsed .logo{
  width:42px!important;
  height:42px!important;
}
@media(max-height:820px){
  .side{padding:14px 14px 30px!important;}
  .side>.brand{
    top:-14px!important;
    margin:-14px -14px 14px!important;
    padding:14px 14px 11px!important;
  }
  .tickerTrackerSection{
    min-height:610px!important;
    height:610px!important;
  }
  .tickerTrackerSection .tickerList{
    min-height:440px!important;
  }
  .returnConfigSection{
    min-height:700px!important;
    height:700px!important;
  }
  .returnRowsWrap{
    min-height:490px!important;
  }
}
@media(max-width:980px){
  .side{
    position:relative!important;
    width:auto!important;
    height:auto!important;
    max-height:none!important;
  }
  .main{margin-left:0!important;}
  .app.side-collapsed .side{
    width:auto!important;
    height:auto!important;
  }
  .app.side-collapsed .main{margin-left:0!important;}
}


/* v14.7.5 targeted return-config label/alignment patch. */
.returnConfigSection h2{
  text-transform:uppercase!important;
  letter-spacing:.12em!important;
}
.returnConfigSection,
.returnConfigSection *{
  box-sizing:border-box!important;
}
.returnDates{
  display:grid!important;
  grid-template-columns:minmax(0,1fr) minmax(0,1fr)!important;
  gap:8px!important;
  width:100%!important;
  max-width:100%!important;
  padding-right:5px!important;
  margin-left:-2px!important;
  overflow:visible!important;
}
.returnDates .field{
  min-width:0!important;
  max-width:100%!important;
  overflow:hidden!important;
}
.returnDates .field label{
  padding-left:1px!important;
}
.returnDates input[type="date"]{
  width:100%!important;
  max-width:100%!important;
  min-width:0!important;
  display:block!important;
  padding-left:9px!important;
  padding-right:7px!important;
  transform:translateX(-1px)!important;
}
.returnRowsWrap{
  max-width:100%!important;
  overflow-x:auto!important;
}

.analysisClickable{
  cursor:pointer;
  transition:border-color .15s, background .15s;
}
.analysisClickable:hover{
  background:rgba(59,130,246,.08);
  border-color:#3b82f6;
}
.analysisClickable h3:after{
  content:"Open sheet";
  display:inline-flex;
  margin-left:9px;
  vertical-align:middle;
  font-size:9px;
  line-height:1;
  border:1px solid rgba(56,189,248,.45);
  border-radius:999px;
  padding:4px 7px;
  color:#bae6fd;
  background:rgba(56,189,248,.08);
  letter-spacing:.04em;
  text-transform:uppercase;
}
.chartTools{
  display:flex;
  align-items:center;
  justify-content:flex-end;
  gap:8px;
  position:relative;
  flex:0 0 auto;
}
.chartClockWrap{
  position:relative;
  display:flex;
  align-items:center;
  gap:6px;
}
.chartLatencyWrap{
  position:relative;
  flex:0 0 auto;
}
.chartLatencyBtn{
  width:132px;
  height:34px;
  border:1px solid #253149;
  border-radius:8px;
  background:#0b1220;
  color:#dbeafe;
  padding:0 9px;
  display:grid;
  grid-template-columns:7px 46px minmax(0,1fr);
  align-items:center;
  gap:6px;
  font-size:10px;
  font-weight:800;
  white-space:nowrap;
}
.chartLatencyBtn:hover,.chartLatencyBtn[aria-expanded="true"]{border-color:#38bdf8;background:#121b2a;}
.chartStreamDot{width:7px;height:7px;border-radius:50%;background:#64748b;box-shadow:0 0 0 3px rgba(100,116,139,.12);}
.chartLatencyBtn.streaming .chartStreamDot{background:#22c55e;box-shadow:0 0 0 3px rgba(34,197,94,.12);}
.chartLatencyBtn.warn .chartStreamDot{background:#f59e0b;box-shadow:0 0 0 3px rgba(245,158,11,.12);}
.chartLatencyValue{font-variant-numeric:tabular-nums;text-align:right;}
.chartLatencyProvider{overflow:hidden;text-overflow:ellipsis;color:#8fb5ff;text-align:left;}
.chartLatencyMenu{
  display:none;
  position:absolute;
  top:40px;
  right:0;
  z-index:1100;
  width:276px;
  padding:10px;
  border:1px solid #303a4d;
  border-radius:8px;
  background:#090e19;
  box-shadow:0 18px 44px rgba(0,0,0,.44);
}
.chartLatencyMenu.open{display:block;}
.chartLatencyMenu label{display:grid;grid-template-columns:76px minmax(0,1fr);align-items:center;gap:8px;color:#8996aa;font-size:10px;font-weight:800;text-transform:uppercase;margin-bottom:8px;}
.chartLatencyMenu select{width:100%;height:32px;border:1px solid #283349;border-radius:6px;background:#101725;color:#e5edf9;padding:0 7px;font-size:11px;outline:none;}
.chartLatencyMenu select:focus{border-color:#38bdf8;}
.chartStreamStatus{border-top:1px solid #202a3a;padding-top:7px;display:grid;gap:4px;max-height:132px;overflow:auto;}
.chartStreamStatusRow{display:grid;grid-template-columns:minmax(0,1fr) auto;gap:8px;align-items:center;color:#8f9bad;font-size:10px;}
.chartStreamStatusRow b{color:#d7e2f2;font-weight:750;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;}
.chartStreamStatusRow span{font-variant-numeric:tabular-nums;white-space:nowrap;}
.chartStreamStatusRow .connected{color:#4ade80;}.chartStreamStatusRow .waiting{color:#fbbf24;}.chartStreamStatusRow .offline{color:#7c8798;}
.chartClock{
  min-width:142px;
  height:34px;
  border:1px solid #253149;
  border-radius:12px;
  background:#0b1220;
  color:#dbeafe;
  display:flex;
  align-items:center;
  justify-content:center;
  gap:7px;
  padding:0 10px;
  font-weight:850;
  font-size:12px;
}
.chartClock:hover{
  border-color:#38bdf8;
  box-shadow:0 0 0 3px rgba(56,189,248,.12);
}
.chartClockTime{
  font-variant-numeric:tabular-nums;
  letter-spacing:.04em;
}
.chartClockZone{
  color:#8fb5ff;
  font-size:10px;
  text-transform:uppercase;
}
.chartTimezone{
  display:none;
  position:absolute;
  right:0;
  top:40px;
  width:210px;
  max-width:70vw;
  height:38px;
  z-index:90;
  background:#090e19;
  color:#edf4ff;
  border:1px solid #38bdf8;
  border-radius:12px;
  padding:0 9px;
  outline:none;
  box-shadow:0 18px 40px rgba(0,0,0,.34);
}
.chartTimezone.open{display:block;}
.chartPanel:fullscreen .chartTools,
.chartPanel.chartFull .chartTools{
  position:fixed;
  top:18px;
  right:24px;
  z-index:1000;
}
.chartPanel:fullscreen .panelHead,
.chartPanel.chartFull .panelHead{
  padding-right:482px;
}
@media(max-width:760px){
  .chartTools{gap:6px;}
  .chartLatencyBtn{width:76px;grid-template-columns:7px 46px;padding:0 7px;}.chartLatencyProvider{display:none;}.chartLatencyMenu{right:-118px;max-width:88vw;}
  .chartClock{min-width:112px;padding:0 8px;}
  .chartClockZone{display:none;}
  .chartPanel:fullscreen .panelHead,
  .chartPanel.chartFull .panelHead{padding-right:18px;padding-top:58px;}
}

/* v15.0: chart-only professional minute-candle workspace. */
.chartPanel{background:#0b0e14;}
.chartPanel>.panelHead{background:#10141c;border-bottom-color:#262b36;}
.chartStage{--chart-crosshair-width:.55;position:relative;height:710px;min-height:580px;overflow:hidden;background:#0b0e14;isolation:isolate;contain:layout style;overscroll-behavior:contain;touch-action:none;}
.chartStage>.chart{height:100%;min-height:100%;}
.chartRangeToolbar{position:absolute;top:8px;left:12px;right:12px;z-index:22;display:flex;align-items:center;gap:6px;height:32px;pointer-events:auto;}
.chartTimeframeControl,.chartStyleControl,.chartCrosshairWidthControl{position:relative;flex:0 0 auto;}
.chartTimeframeBtn,.chartStyleBtn{height:30px;border:1px solid transparent;background:#121720;color:#c4cbd5;border-radius:4px;font-size:10px;font-weight:750;padding:0 8px;white-space:nowrap;display:flex;align-items:center;justify-content:center;gap:6px;}
.chartTimeframeBtn:hover,.chartStyleBtn:hover{background:#1b202a;color:#f3f4f6;border-color:#303743;}
.chartTimeframeBtn[aria-expanded="true"],.chartStyleBtn[aria-expanded="true"]{background:#252b36;color:white;border-color:#414958;}
.chartTimeframeBtn svg{width:16px;height:16px;display:block;}
.chartTimeframeValue{min-width:24px;text-align:left;font-variant-numeric:tabular-nums;}
.chartTimeframeChevron{font-size:9px;color:#7f8a9b;}
.chartTimeframeMenu{display:none;position:absolute;top:35px;left:0;width:310px;max-width:calc(100vw - 42px);max-height:470px;overflow-y:auto;padding:7px;background:#171b24;border:1px solid #303743;border-radius:6px;box-shadow:0 14px 36px rgba(0,0,0,.48);z-index:125;}
.chartTimeframeMenu.open{display:block;}
.chartTimeframeGroup{padding:4px 2px 7px;}
.chartTimeframeGroup+.chartTimeframeGroup{border-top:1px solid #2a303a;padding-top:8px;}
.chartTimeframeGroupTitle{display:block;padding:0 5px 5px;color:#7f8a9b;font-size:9px;font-weight:750;text-transform:uppercase;}
.chartTimeframeOptions{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:3px;}
.chartTimeframeOption{height:30px;border:1px solid transparent;border-radius:4px;background:transparent;color:#d7dce4;font-size:11px;font-weight:700;font-variant-numeric:tabular-nums;}
.chartTimeframeOption:hover{background:#252b36;}
.chartTimeframeOption.active{background:#2962ff;color:white;border-color:#4b7dff;}
.chartStyleBtn{width:34px;padding:0;display:grid;place-items:center;}
.chartStyleBtn svg,.chartStyleOption svg{width:18px;height:18px;display:block;overflow:visible;}
.chartStyleMenu{display:none;position:absolute;top:35px;left:0;right:auto;width:236px;max-height:390px;overflow-y:auto;padding:5px;background:#171b24;border:1px solid #303743;border-radius:6px;box-shadow:0 14px 36px rgba(0,0,0,.48);z-index:120;}
.chartStyleMenu.open{display:block;}
.chartStyleOption{width:100%;height:34px;border:0;border-radius:4px;background:transparent;color:#d7dce4;display:flex;align-items:center;gap:10px;padding:0 9px;text-align:left;font-size:12px;}
.chartStyleOption:hover{background:#252b36;}
.chartStyleOption.active{background:#28344d;color:white;}
.chartStyleOption span:last-child{overflow:hidden;text-overflow:ellipsis;white-space:nowrap;}
.chartCrosshairWidthBtn{height:30px;min-width:54px;border:1px solid transparent;background:#121720;color:#c4cbd5;border-radius:4px;padding:0 7px;display:flex;align-items:center;justify-content:center;gap:5px;font-size:9px;font-weight:750;font-variant-numeric:tabular-nums;}
.chartCrosshairWidthBtn:hover,.chartCrosshairWidthBtn[aria-expanded="true"]{background:#252b36;color:#f3f4f6;border-color:#414958;}
.chartCrosshairWidthBtn svg{width:15px;height:15px;display:block;stroke:currentColor;}
.chartCrosshairWidthMenu{display:none;position:absolute;top:35px;left:0;width:210px;padding:10px;background:#171b24;border:1px solid #303743;border-radius:6px;box-shadow:0 14px 36px rgba(0,0,0,.48);z-index:126;}
.chartCrosshairWidthMenu.open{display:grid;gap:8px;}
.chartCrosshairWidthMenu label{display:flex;align-items:center;justify-content:space-between;gap:10px;color:#aeb6c2;font-size:10px;font-weight:700;}
.chartCrosshairWidthMenu output{color:#f3f4f6;font-variant-numeric:tabular-nums;}
.chartCrosshairWidthMenu input[type="range"]{width:100%;height:18px;margin:0;padding:0;border:0;background:transparent;box-shadow:none;accent-color:#4b7dff;cursor:pointer;}
.chartHistoryToolbar{position:absolute;left:8px;bottom:4px;z-index:46;display:flex;align-items:center;gap:1px;max-width:calc(100% - 88px);height:28px;padding:1px;background:#10141c;border:1px solid #2a303a;border-radius:4px;overflow-x:auto;overflow-y:hidden;pointer-events:auto;scrollbar-width:none;}
.chartHistoryToolbar::-webkit-scrollbar{display:none;}
.chartHistoryBtn{flex:0 0 auto;min-width:31px;height:24px;padding:0 7px;border:0;border-radius:3px;background:transparent;color:#9aa4b3;font-size:10px;font-weight:700;font-variant-numeric:tabular-nums;white-space:nowrap;}
.chartHistoryBtn:hover{background:#252b36;color:#f3f4f6;}
.chartHistoryBtn.active{background:#28344d;color:#fff;}
.chartForecastToggle{position:relative;z-index:1;flex:0 0 auto;height:34px;border:1px solid #303743;background:#171b24;color:#cbd5e1;border-radius:4px;padding:0 8px;display:flex;align-items:center;gap:6px;font-size:10px;font-weight:700;}
.chartForecastToggle:hover{background:#252b36;color:white;}
.chartForecastToggle svg{width:15px;height:15px;}
.chartForecastToggle.off{color:#707887;background:#12161d;}
.chartOhlcStrip{position:absolute;top:49px;left:66px;z-index:18;display:flex;align-items:center;gap:10px;max-width:calc(100% - 160px);color:#9aa4b3;font-size:11px;font-variant-numeric:tabular-nums;pointer-events:none;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;}
.chartOhlcStrip b{color:#e5e7eb;font-weight:750;}
.chartOhlcStrip .up{color:#26a69a;}
.chartOhlcStrip .down{color:#ef5350;}
.chartOhlcStrip .feed{color:#7f8a9b;overflow:hidden;text-overflow:ellipsis;}
.chartCrosshair{position:absolute;inset:0;z-index:17;display:none;pointer-events:none;}
.chartCrosshair.visible{display:block;}
.chartCrosshairV,.chartCrosshairH{position:absolute;border-color:rgba(194,202,214,.72);border-style:dashed;opacity:.92;}
.chartCrosshairV{width:0;border-left-width:1px;}
.chartCrosshairH{height:0;border-top-width:1px;}
.chartAxisLabel{position:absolute;background:#2d3440;color:#f3f4f6;border:0;border-radius:2px;padding:4px 7px;font-size:11px;line-height:1;font-weight:650;font-variant-numeric:tabular-nums;white-space:nowrap;box-shadow:none;}
.chartCrosshairX{transform:translateX(-50%);}
.chartCrosshairY{transform:translateY(-50%);}
.chartLastPrice{position:absolute;z-index:16;display:none;transform:translateY(-50%);min-width:62px;background:#2962ff;color:white;border-radius:2px;padding:4px 7px;font-size:11px;line-height:1;font-weight:750;font-variant-numeric:tabular-nums;pointer-events:none;white-space:nowrap;}
.chartLastPrice.up{background:#089981;}
.chartLastPrice.down{background:#f23645;}
.chartLastPrice.visible{display:block;}
.top{background:rgba(7,10,16,.96);-webkit-backdrop-filter:none;backdrop-filter:none;}
.resultTable [data-live-price],.resultTable [data-live-day-pct],.resultTable [data-live-day-abs],.resultTable [data-live-forecast],.priceBox .price,.forecastGrid .p{font-variant-numeric:tabular-nums;}
.liveState:empty{display:none;}
.chartCrosshair{display:block;visibility:hidden;opacity:0;contain:layout style;transform:translateZ(0);}
.chartCrosshair.visible{visibility:visible;opacity:1;}
.chartCrosshairV,.chartCrosshairH{border:0;opacity:.74;transform-origin:0 0;will-change:transform;backface-visibility:hidden;}
.chartCrosshairV{left:0;top:0;width:1px;background:repeating-linear-gradient(to bottom,rgba(207,214,224,.56) 0 2px,transparent 2px 6px);}
.chartCrosshairH{left:0;top:0;height:1px;background:repeating-linear-gradient(to right,rgba(207,214,224,.56) 0 2px,transparent 2px 6px);}
.chartCrosshairX,.chartCrosshairY{left:0;top:0;will-change:transform;backface-visibility:hidden;}
.chartLastPrice{will-change:transform;backface-visibility:hidden;}
.chartStage .modebar-container{overflow:visible!important;}
.chartStage .modebar{top:8px!important;right:12px!important;left:auto!important;display:flex!important;opacity:1!important;z-index:45!important;}
.chartStage .modebar-btn{position:relative!important;}
.chartStage .modebar-btn[data-title]:before{left:auto!important;right:0!important;transform:none!important;max-width:230px!important;white-space:nowrap!important;}
.chartStage .modebar-btn[data-title]:after{left:auto!important;right:9px!important;transform:none!important;}
.chartPanel:fullscreen .chartStage,.chartPanel.chartFull .chartStage{height:calc(100vh - 98px)!important;min-height:calc(100vh - 98px);}
.chartPanel:fullscreen,.chartPanel.chartFull{width:100vw;height:100vh;border-radius:0;}
.chartPanel.chartFull{inset:0;}
.chartPanel.chartFull .chartStage>.chart{height:100%!important;min-height:100%!important;}
@media(max-width:1280px){.chartStage{height:650px;min-height:560px;}}
@media(max-width:760px){.chartRangeToolbar{right:12px;}.chartHistoryToolbar{max-width:calc(100% - 24px);}.chartForecastToggle{width:34px;padding:0;justify-content:center;}.chartForecastToggle span{display:none;}.chartOhlcStrip{left:54px;top:49px;max-width:calc(100% - 112px);gap:6px;font-size:10px;}.chartAxisLabel,.chartLastPrice{font-size:10px;padding:4px 5px;}.chartStyleMenu{left:0;right:auto;width:220px;}}

/* Display settings: stable rail order, spectrum palette and language panel. */
:root{
  --theme-bg:#070a10;
  --theme-bg-top:#080b12;
  --theme-bg-deep:#05070c;
  --theme-side-top:#0f172a;
  --theme-panel:#0d111c;
  --theme-card-top:#121a2a;
  --theme-control:#090e19;
  --theme-section:#0f172a;
  --theme-line:#253047;
  --theme-bg-rgb:7,10,16;
  --theme-accent-rgb:59,130,246;
  --theme-text:#edf4ff;
  --theme-muted:#94a3b8;
  --theme-soft:#c7d2fe;
}
body{background:radial-gradient(circle at 15% -8%,rgba(var(--theme-accent-rgb),.18),transparent 32%),radial-gradient(circle at 95% 0%,rgba(34,211,238,.10),transparent 30%),linear-gradient(180deg,var(--theme-bg-top),var(--theme-bg) 45%,var(--theme-bg-deep))!important;color:var(--theme-text)!important;}
.main{background:linear-gradient(180deg,rgba(var(--theme-bg-rgb),.04),rgba(var(--theme-bg-rgb),.22))!important;}
.side{background:linear-gradient(180deg,var(--theme-side-top),var(--theme-bg))!important;border-color:var(--theme-line)!important;}
.side>.brand{background:linear-gradient(180deg,var(--theme-side-top),rgba(var(--theme-bg-rgb),.92))!important;border-color:var(--theme-line)!important;}
.top{background:rgba(var(--theme-bg-rgb),.82)!important;border-color:var(--theme-line)!important;}
.section{background:color-mix(in srgb,var(--theme-section) 88%,transparent)!important;border-color:var(--theme-line)!important;}
.panel{background:color-mix(in srgb,var(--theme-panel) 96%,transparent)!important;border-color:var(--theme-line)!important;}
.card{background:linear-gradient(180deg,var(--theme-card-top),var(--theme-panel))!important;border-color:var(--theme-line)!important;}
.field input,.field select,.rankControls select,.rankSearch input,.returnConfigTable select,.returnConfigTable input{background:var(--theme-control)!important;border-color:var(--theme-line)!important;}
.ticker,.day,.metric,.statBox,.methodCard,.analysisBox,.driver{background:color-mix(in srgb,var(--theme-control) 94%,transparent)!important;border-color:var(--theme-line)!important;}
.status,.tickerTools button,.miniBtn,.progress{background:var(--theme-control)!important;color:var(--theme-text)!important;border-color:var(--theme-line)!important;}
.rankSearch,.resultTable th,.returnTable th,.returnTable th:first-child,.returnTable td:first-child{background:var(--theme-section)!important;color:var(--theme-text)!important;border-color:var(--theme-line)!important;}
.resultTable td,.returnTable td{border-color:var(--theme-line)!important;}
.returnTable td{background:var(--theme-control)!important;color:var(--theme-text)!important;}
.returnTable tr:nth-child(even) td{background:var(--theme-panel)!important;}
.chartStage,.chartStage>.chart{background:var(--theme-panel)!important;}
.chartPanel>.panelHead{background:var(--theme-section)!important;border-color:var(--theme-line)!important;}
.chartPanel:fullscreen,.chartPanel.chartFull{background:var(--theme-bg)!important;}
.brand{align-items:flex-start!important;gap:14px!important;}
.brandRail{display:flex;flex:0 0 88px;flex-direction:column;align-items:center;gap:8px;}
.brandRail .logo{width:88px!important;height:88px!important;border-radius:10px!important;overflow:hidden;background:#07101d!important;box-shadow:0 12px 30px rgba(0,0,0,.28)!important;}
.brandRail .logo img{display:block;width:100%;height:100%;object-fit:cover;}
.brandRail .menuToggle,.railIconBtn{width:38px;height:38px;min-width:38px;border:1px solid var(--theme-line);background:linear-gradient(180deg,var(--theme-card-top),var(--theme-control));color:#dbeafe;border-radius:8px;display:grid;place-items:center;padding:0;box-shadow:0 10px 24px rgba(0,0,0,.2);}
.brandRail .menuToggle{padding:8px;}
.railIconBtn svg{width:18px;height:18px;stroke:currentColor;}
.brandRail .menuToggle:hover,.railIconBtn:hover,.railIconBtn.active{border-color:#38bdf8;color:#7dd3fc;box-shadow:0 0 0 3px rgba(56,189,248,.12);}
.app.side-collapsed{grid-template-columns:108px minmax(0,1fr);}
.app.side-collapsed .brandRail{width:88px;}
.app.side-collapsed .brandRail .logo{width:88px!important;height:88px!important;}
.side>.brand{align-items:center!important;}
.brandRail{width:100%;min-height:152px;flex:1 1 auto;display:grid;grid-template-columns:38px minmax(0,1fr) 38px;grid-template-rows:1fr 38px 8px 38px 1fr;align-items:center;gap:0;}
.brandRail .logo{grid-column:2;grid-row:1 / 6;align-self:center;justify-self:center;width:152px!important;height:152px!important;}
.brandRail .menuToggle{grid-column:1;grid-row:2;align-self:center;justify-self:start;}
.brandRail .railIconBtn{grid-column:1;grid-row:4;align-self:center;justify-self:start;}
.brandRail .logo,.brandRail .menuToggle,.brandRail .railIconBtn{flex:0 0 auto;}
.brandRail .menuToggle{display:flex!important;flex-direction:column;align-items:center;justify-content:center;gap:4px;}
.brandRail .menuToggle span{width:17px;height:2px;min-height:2px;flex:0 0 2px;}
.app.side-collapsed .side{scrollbar-gutter:auto!important;}
.app.side-collapsed .brand{width:100%!important;}
.app.side-collapsed .brandRail{width:100%;height:auto;min-height:0;flex:0 0 auto;display:flex;flex-direction:column;align-items:center;gap:8px;overflow:visible;}
.app.side-collapsed .brandRail .logo{width:52px!important;height:52px!important;min-height:52px;align-self:center;margin-inline:auto;}
.app.side-collapsed .brandRail .menuToggle,.app.side-collapsed .brandRail .railIconBtn{align-self:center;margin-inline:auto;}
.positionAdviceLaunch{width:100%;display:flex;align-items:center;gap:10px;margin:-4px 0 14px;padding:11px 12px;border:1px solid var(--theme-line);border-radius:8px;background:linear-gradient(180deg,color-mix(in srgb,var(--theme-card-top) 94%,#0ea5e9 6%),var(--theme-control));color:var(--text);text-align:left;box-shadow:0 12px 28px rgba(0,0,0,.18);}
.positionAdviceLaunch:hover{border-color:#38bdf8;background:color-mix(in srgb,var(--theme-card-top) 88%,#0ea5e9 12%);}
.positionAdviceLaunch svg{width:20px;height:20px;flex:0 0 20px;color:#7dd3fc;}
.positionAdviceLaunch span{min-width:0;display:grid;gap:2px;flex:1;}
.positionAdviceLaunch strong{font-size:13px;letter-spacing:0;}
.positionAdviceLaunch small{font-size:10px;color:var(--muted);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;}
.positionAdviceLaunch .launchArrow{width:15px;height:15px;flex:0 0 15px;color:#64748b;}
.app.side-collapsed .positionAdviceLaunch{display:none;}
.settingsBackdrop{position:fixed;inset:0;z-index:118;background:rgba(0,0,0,.18);opacity:0;pointer-events:none;transition:opacity .14s ease;}
.settingsBackdrop.open{opacity:1;pointer-events:auto;}
.settingsPanel{position:fixed;z-index:120;width:min(360px,calc(100vw - 28px));max-height:calc(100vh - 28px);overflow:auto;background:var(--theme-panel);border:1px solid var(--theme-line);border-radius:8px;box-shadow:0 28px 80px rgba(0,0,0,.52);opacity:0;visibility:hidden;pointer-events:none;transform:translate3d(-8px,0,0);transition:opacity .14s ease,transform .14s ease,visibility .14s;}
.settingsPanel.open{opacity:1;visibility:visible;pointer-events:auto;transform:translate3d(0,0,0);}
.settingsHead{height:54px;display:flex;align-items:center;justify-content:space-between;padding:0 14px;border-bottom:1px solid var(--theme-line);}
.settingsHead h2{margin:0;font-size:15px;letter-spacing:0;}
.settingsClose{width:32px;height:32px;border:0;background:transparent;color:var(--muted);display:grid;place-items:center;border-radius:6px;}
.settingsClose:hover{background:rgba(148,163,184,.12);color:var(--text);}
.settingsClose svg{width:17px;height:17px;stroke:currentColor;}
.settingsGroup{padding:14px;border-bottom:1px solid var(--theme-line);}
.settingsGroup:last-child{border-bottom:0;}
.settingsLabel{display:flex;align-items:center;justify-content:space-between;gap:12px;margin-bottom:12px;color:#dbeafe;font-size:12px;font-weight:800;}
.settingsLabel output{color:var(--muted);font-size:11px;font-weight:650;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;}
.settingsMode{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:4px;padding:4px;border:1px solid var(--theme-line);border-radius:7px;background:var(--theme-control);}
.settingsModeBtn{height:36px;min-width:0;border:0;border-radius:5px;background:transparent;color:var(--theme-muted);font-size:11px;font-weight:800;}
.settingsModeBtn:hover{color:var(--theme-text);background:color-mix(in srgb,var(--theme-card-top) 72%,transparent);}
.settingsModeBtn.active{color:var(--theme-text);background:var(--theme-card-top);box-shadow:inset 0 0 0 1px var(--theme-line);}
.settingsPalette{display:grid;grid-template-columns:repeat(6,30px);justify-content:space-between;gap:10px;}
.colorSwatch{width:30px;height:30px;border:1px solid rgba(255,255,255,.18);border-radius:50%;padding:0;background:var(--swatch);box-shadow:inset 0 0 0 4px rgba(0,0,0,.18);position:relative;}
.colorSwatch:hover{transform:scale(1.08);border-color:rgba(255,255,255,.72);}
.colorSwatch.active{border:2px solid #fff;box-shadow:0 0 0 3px rgba(var(--theme-accent-rgb),.42),inset 0 0 0 3px rgba(0,0,0,.16);}
.colorSwatch.active:after{content:"";position:absolute;inset:9px;border-radius:50%;background:#fff;}
.settingsSelect{display:grid;gap:7px;color:var(--muted);font-size:11px;font-weight:750;}
.settingsSelect select{width:100%;height:40px;background:var(--theme-control);color:var(--text);border:1px solid var(--theme-line);border-radius:7px;padding:0 10px;outline:none;}
.settingsSelect select:focus{border-color:var(--blue);box-shadow:0 0 0 3px rgba(59,130,246,.15);}
.learningMonitor{display:grid;gap:9px;}
.learningStatusLine{display:grid;grid-template-columns:minmax(0,1fr) auto;align-items:center;gap:12px;padding:6px 0;border-bottom:1px solid color-mix(in srgb,var(--theme-line) 72%,transparent);font-size:11px;}
.learningStatusLine:last-of-type{border-bottom:0;}
.learningStatusLine span{color:var(--theme-muted);}
.learningStatusLine b{max-width:200px;color:var(--theme-text);font-size:11px;text-align:right;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;}
.learningState{display:inline-flex;align-items:center;gap:7px;font-weight:850;}
.learningState:before{content:"";width:7px;height:7px;border-radius:50%;background:#64748b;box-shadow:0 0 12px currentColor;}
.learningState.healthy{color:#22c55e}.learningState.degraded{color:#f59e0b}.learningState.failed{color:#ef4444}.learningState.disabled{color:#94a3b8}
.learningError{display:none;padding:9px 10px;border-left:2px solid #ef4444;background:rgba(239,68,68,.08);color:#fecaca;font-size:10px;line-height:1.45;overflow-wrap:anywhere;}
.learningError.visible{display:block;}
.learningRefresh{width:34px;height:34px;justify-self:end;display:grid;place-items:center;border:1px solid var(--theme-line);border-radius:7px;background:var(--theme-control);color:var(--theme-text);}
.learningRefresh:hover{border-color:var(--blue);color:#7dd3fc;}
.learningRefresh svg{width:15px;height:15px;stroke:currentColor;}
.learningRefresh.loading svg{animation:learningSpin .8s linear infinite;}
@keyframes learningSpin{to{transform:rotate(360deg)}}
body[data-visual-mode="light"] .section h2,
body[data-visual-mode="light"] .tickerTools button,
body[data-visual-mode="light"] .rankControls select,
body[data-visual-mode="light"] .resultTable th,
body[data-visual-mode="light"] .newsLink,
body[data-visual-mode="light"] .newsLink strong,
body[data-visual-mode="light"] .profileTitle p,
body[data-visual-mode="light"] .pill,
body[data-visual-mode="light"] .driver,
body[data-visual-mode="light"] .methodCard p,
body[data-visual-mode="light"] .analysisBox p,
body[data-visual-mode="light"] .analysisList,
body[data-visual-mode="light"] .settingsLabel,
body[data-visual-mode="light"] .brandRail .menuToggle,
body[data-visual-mode="light"] .railIconBtn{color:var(--theme-text)!important;}
body[data-visual-mode="light"] .chartTimeframeBtn,
body[data-visual-mode="light"] .chartStyleBtn,
body[data-visual-mode="light"] .chartCrosshairWidthBtn,
body[data-visual-mode="light"] .chartForecastToggle,
body[data-visual-mode="light"] .chartLatencyBtn,
body[data-visual-mode="light"] .chartClock,
body[data-visual-mode="light"] .chartFullBtn{background:var(--theme-control)!important;color:var(--theme-text)!important;border-color:var(--theme-line)!important;}
body[data-visual-mode="light"] .chartTimeframeMenu,
body[data-visual-mode="light"] .chartStyleMenu,
body[data-visual-mode="light"] .chartCrosshairWidthMenu,
body[data-visual-mode="light"] .chartLatencyMenu,
body[data-visual-mode="light"] .chartTimezone{background:var(--theme-panel)!important;color:var(--theme-text)!important;border-color:var(--theme-line)!important;}
body[data-visual-mode="light"] .chartTimeframeOption,
body[data-visual-mode="light"] .chartStyleOption,
body[data-visual-mode="light"] .chartCrosshairWidthMenu label,
body[data-visual-mode="light"] .chartLatencyMenu label{color:var(--theme-text)!important;}
body[data-visual-mode="light"] .name,
body[data-visual-mode="light"] .smallMuted,
body[data-visual-mode="light"] .rankTitle span,
body[data-visual-mode="light"] .panelHead span,
body[data-visual-mode="light"] .methodCard span,
body[data-visual-mode="light"] .statBox span{color:var(--theme-muted)!important;}
body[data-visual-mode="light"] .settingsBackdrop{background:rgba(15,23,42,.10);}
#chart .bg{fill:var(--theme-panel)!important;}
#chart .gridlayer path,#chart .zerolinelayer path,#chart .xlines-below,#chart .ylines-below{stroke:var(--theme-line)!important;}
.legalFooter{margin:0 26px 26px;padding:15px 0 0;border-top:1px solid var(--theme-line,var(--line));color:var(--theme-muted,var(--muted));font-size:9.5px;line-height:1.55}
.legalFooter strong,.legalCopyright{color:var(--theme-text,var(--text));font-weight:750}
.legalFooter p{max-width:1540px;margin:5px 0 0}
body[data-visual-mode="light"] .legalFooter{color:var(--theme-muted)!important;border-color:var(--theme-line)!important}
body[data-visual-mode="light"] .legalFooter strong,body[data-visual-mode="light"] .legalCopyright{color:var(--theme-text)!important}
@media(max-width:1280px){.app.side-collapsed{grid-template-columns:1fr;}}
@media(max-width:620px){.settingsPalette{grid-template-columns:repeat(5,30px);}.settingsPanel{width:calc(100vw - 24px);}.legalFooter{margin:0 14px 20px}}

</style>
</head>
<body>
<div class="app side-collapsed">
  <aside class="side">
    <div class="brand"><div class="brandRail"><div class="logo" aria-label="Apex Tool"><img src="/assets/apex-tool-logo.png" alt="Apex Tool"></div><button class="menuToggle" id="sideToggle" type="button" title="Show or hide menus" aria-label="Show or hide menus"><span></span><span></span><span></span></button><button class="railIconBtn" id="settingsToggle" type="button" title="Settings" aria-label="Settings" aria-expanded="false" aria-controls="settingsPanel"><svg class="lucide lucide-settings" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M12.22 2h-.44a2 2 0 0 0-2 2v.18a2 2 0 0 1-1 1.73l-.43.25a2 2 0 0 1-2 0l-.15-.08a2 2 0 0 0-2.73.73l-.22.38a2 2 0 0 0 .73 2.73l.15.09a2 2 0 0 1 1 1.74v.5a2 2 0 0 1-1 1.74l-.15.09a2 2 0 0 0-.73 2.73l.22.38a2 2 0 0 0 2.73.73l.15-.08a2 2 0 0 1 2 0l.43.25a2 2 0 0 1 1 1.73V20a2 2 0 0 0 2 2h.44a2 2 0 0 0 2-2v-.18a2 2 0 0 1 1-1.73l.43-.25a2 2 0 0 1 2 0l.15.08a2 2 0 0 0 2.73-.73l.22-.38a2 2 0 0 0-.73-2.73l-.15-.09a2 2 0 0 1-1-1.74v-.5a2 2 0 0 1 1-1.74l.15-.09a2 2 0 0 0 .73-2.73l-.22-.38a2 2 0 0 0-2.73-.73l-.15.08a2 2 0 0 1-2 0l-.43-.25a2 2 0 0 1-1-1.73V4a2 2 0 0 0-2-2z"/><circle cx="12" cy="12" r="3"/></svg></button></div></div>
  <button class="positionAdviceLaunch" id="positionAdviceLaunch" type="button" title="Open Position advice in a new tab"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M3 3v18h18"/><path d="m7 8 4 4 4-6 4 3"/><path d="M15 9h4V5"/></svg><span><strong>Position advice</strong><small>Real-time Long and Short edge setups</small></span><svg class="launchArrow" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M9 18l6-6-6-6"/></svg></button>
    <div class="section"><h2>Forecast run configuration</h2>
      <div class="field"><label>Mode</label><select id="mode"><option value="fast">Fast scan</option><option value="balanced">Balanced</option><option value="deep" selected>Deep history</option></select></div>
      <div class="grid2"><div class="field"><label>Days</label><input id="days" type="number" value="10" min="1" max="15"></div><div class="field"><label>Bars</label><input id="bars" type="number" placeholder="Turbo auto"></div></div>
      <div class="grid2"><div class="field"><label>News</label><select id="news"><option value="1" selected>Enabled</option><option value="0">Disabled</option></select></div><div class="field"><label>Headlines</label><input id="newsLimit" type="number" value="32" min="1" max="60"></div></div>
      <div class="actions"><button class="primary" id="runBtn">Run analysis</button><button class="danger" id="stopBtn">Stop</button></div>
      <div style="margin-top:14px"><div class="progress"><div class="bar" id="progressBar"></div></div><p id="progressText" style="color:var(--muted);font-size:12px;margin:9px 0 0">Ready</p></div>
    </div>
    <div class="section tickerTrackerSection"><h2>Ticker Tracker</h2>
      <div class="field"><input id="tickerSearch" placeholder="Search ticker or company"></div>
      <div class="tickerTools"><button id="selectAll">Select all</button><button id="clearAll">Clear</button><button id="megaCaps">Mega caps</button></div>
      <div class="tickerList" id="tickerList"></div>
    </div>
    <div class="section returnConfigSection"><h2>RETURN PREVISION CONFIG</h2>
      <div class="returnDates"><div class="field"><label>Start</label><input id="returnStart" type="date" lang="en-US" min="2015-01-01" aria-label="Start date"></div><div class="field"><label>End</label><input id="returnEnd" type="date" lang="en-US" min="2015-01-01" aria-label="End date"></div></div>
      <div class="returnHelp">Maximum 15 tickers · allocation required per selected ticker · $0 to $1B per ticker.</div>
      <div class="returnRowsWrap"><table class="returnConfigTable"><thead><tr><th>Tickers</th><th>Allocated funds</th></tr></thead><tbody id="returnConfigRows"></tbody></table></div>
      <button class="returnRun" id="returnRunBtn">Run return prevision</button>
    </div>
  </aside>
  <div class="settingsBackdrop" id="settingsBackdrop"></div>
  <section class="settingsPanel" id="settingsPanel" role="dialog" aria-modal="false" aria-labelledby="settingsTitle" aria-hidden="true">
    <div class="settingsHead"><h2 id="settingsTitle">Settings</h2><button class="settingsClose" id="settingsClose" type="button" title="Close settings" aria-label="Close settings"><svg class="lucide lucide-x" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M18 6 6 18M6 6l12 12"/></svg></button></div>
    <div class="settingsGroup"><div class="settingsLabel"><span id="settingsModeLabel">Display mode</span></div><div class="settingsMode" id="settingsMode" role="radiogroup" aria-label="Display mode"><button class="settingsModeBtn" type="button" data-mode="dark" role="radio" aria-checked="true">Dark</button><button class="settingsModeBtn" type="button" data-mode="balanced" role="radio" aria-checked="false">Balanced</button><button class="settingsModeBtn" type="button" data-mode="light" role="radio" aria-checked="false">Light</button></div></div>
    <div class="settingsGroup"><div class="settingsLabel"><span id="settingsColorLabel">Background color</span><output id="settingsColorName">Current default</output></div><div class="settingsPalette" id="settingsPalette" role="list" aria-label="Background color palette"></div></div>
    <div class="settingsGroup"><label class="settingsSelect" for="settingsLanguage"><span id="settingsLanguageLabel">Interface language</span><select id="settingsLanguage"><option value="en">English</option><option value="fr">Français</option></select></label></div>
    <div class="settingsGroup"><div class="settingsLabel"><span id="learningMonitorLabel">Remote learning v15</span><output class="learningState disabled" id="learningMonitorState">Checking</output></div><div class="learningMonitor" aria-live="polite"><div class="learningStatusLine"><span id="learningStorageLabel">Storage</span><b id="learningStorage">—</b></div><div class="learningStatusLine"><span id="learningEvaluatedLabel">Exact evaluations</span><b id="learningEvaluated">0 / 0</b></div><div class="learningStatusLine"><span id="learningProfilesLabel">Validated profiles</span><b id="learningProfiles">0</b></div><div class="learningStatusLine"><span id="learningWriteLabel">Last successful write</span><b id="learningLastWrite">—</b></div><div class="learningStatusLine"><span id="learningImpactLabel">Selected forecast impact</span><b id="learningSelectedImpact">—</b></div><div class="learningError" id="learningError"></div><button class="learningRefresh" id="learningRefresh" type="button" title="Refresh remote learning diagnostics" aria-label="Refresh remote learning diagnostics"><svg viewBox="0 0 24 24" fill="none" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M20 11a8 8 0 1 0 2 5"/><path d="M20 4v7h-7"/></svg></button></div></div>
  </section>
  <main class="main">
    <header class="top"><div><h2 class="dashTitle">Dashboard</h2><p class="versionLine">v15</p></div><div class="status"><span class="dot" id="stateDot"></span><span id="stateText">Idle</span></div></header>
    <section class="cards">
      <div class="card"><div class="k">Coverage</div><div class="v" id="mCoverage">—</div><div class="s">successful tickers</div></div>
      <div class="card"><div class="k">Average 10D</div><div class="v" id="mAvg">—</div><div class="s">watchlist forecast</div></div>
      <div class="card"><div class="k">Constructive</div><div class="v" id="mBull">—</div><div class="s">positive setups</div></div>
      <div class="card"><div class="k">Confidence</div><div class="v" id="mConf">—</div><div class="s">mean score</div></div>
      <div class="card"><div class="k">Risk</div><div class="v" id="mRisk">—</div><div class="s">dominant regime</div></div>
    </section>
    <section class="work">
      <div class="panel chartPanel" id="chartPanel"><div class="panelHead"><div><h3 id="chartTitle">Interactive price path</h3><span id="chartSub">Select a ticker after a run</span></div><div class="chartTools"><button class="chartForecastToggle" id="chartForecastToggle" type="button" aria-pressed="true" title="Hide forecast"><svg viewBox="0 0 24 24" aria-hidden="true"><path d="M2 12s3.5-6 10-6 10 6 10 6-3.5 6-10 6S2 12 2 12Z" fill="none" stroke="currentColor" stroke-width="1.7"/><circle cx="12" cy="12" r="2.7" fill="none" stroke="currentColor" stroke-width="1.7"/></svg><span>Forecast</span></button><div class="chartLatencyWrap"><button class="chartLatencyBtn" id="chartLatencyBtn" type="button" aria-haspopup="menu" aria-expanded="false" title="Live market connection"><span class="chartStreamDot"></span><span class="chartLatencyValue" id="chartLatencyValue">-- ms</span><span class="chartLatencyProvider" id="chartLatencyProvider">Fallback</span></button><div class="chartLatencyMenu" id="chartLatencyMenu" role="menu"><label>Source<select id="chartLiveProvider"><option value="auto">Exchange auto</option><option value="massive">Massive</option><option value="alpaca">Alpaca</option><option value="finnhub">Finnhub</option><option value="twelvedata">Twelve Data</option><option value="coinbase">Coinbase</option><option value="http">HTTP fallback</option></select></label><label>Cadence<select id="chartLiveCadence"><option value="100">100 ms</option><option value="250" selected>250 ms</option><option value="500">500 ms</option><option value="1000">1000 ms</option></select></label><div class="chartStreamStatus" id="chartStreamStatus"><div class="chartStreamStatusRow"><b>HTTP fallback</b><span class="waiting">ready</span></div></div></div></div><div class="chartClockWrap"><button class="chartClock" id="chartClockBtn" title="Choose chart clock timezone"><span class="chartClockTime" id="chartClockTime">--:--:--</span><span class="chartClockZone" id="chartClockZone">Paris</span></button><select class="chartTimezone" id="chartTimezone" aria-label="Chart clock timezone"></select></div><button class="miniBtn" id="chartFullBtn">Fullscreen</button></div></div><div class="chartStage" id="chartStage"><div id="chart" class="chart"><div class="chartInstruction"><div><small>Open the three-bar menu to configure the run.</small></div></div></div><div class="chartRangeToolbar" id="chartRangeToolbar"><div class="chartTimeframeControl"><button class="chartTimeframeBtn" id="chartTimeframeBtn" type="button" aria-haspopup="menu" aria-expanded="false" title="Chart interval: 1 minute"><svg viewBox="0 0 20 20" aria-hidden="true"><path d="M4 4v12M10 2v16M16 6v8M2 8h4M8 6h4M14 9h4" fill="none" stroke="currentColor" stroke-width="1.4" stroke-linecap="round"/></svg><span class="chartTimeframeValue" id="chartTimeframeValue">1m</span><span class="chartTimeframeChevron">▼</span></button><div class="chartTimeframeMenu" id="chartTimeframeMenu" role="menu"></div></div><div class="chartStyleControl"><button class="chartStyleBtn" id="chartStyleBtn" type="button" aria-haspopup="menu" aria-expanded="false" title="Chart display: Candles"></button><div class="chartStyleMenu" id="chartStyleMenu" role="menu"></div></div></div><div class="chartOhlcStrip" id="chartOhlcStrip" aria-live="polite"></div><div class="chartCrosshair" id="chartCrosshair"><div class="chartCrosshairV" id="chartCrosshairV"></div><div class="chartCrosshairH" id="chartCrosshairH"></div><div class="chartAxisLabel chartCrosshairX" id="chartCrosshairX"></div><div class="chartAxisLabel chartCrosshairY" id="chartCrosshairY"></div></div><div class="chartLastPrice" id="chartLastPrice"></div></div></div>
      <div class="panel profilePanel"><div class="panelHead"><h3>Company profile & investor view</h3><span id="profileSub">Select a ticker</span></div><div id="profilePanel" class="profileBody"><div class="empty">Company profile appears here after selecting a forecast.</div></div></div>
      <div class="panel returnPanel"><div class="panelHead"><div><h3>Return prevision</h3><span id="returnSub">Configure dates, tickers and allocated funds in Return prevision config</span></div><span class="returnStatus" id="returnStatus">Idle</span></div><div id="returnPanel" class="returnTableWrap"><div class="empty">Run a return prevision from Return prevision config.</div></div></div>
      <div class="right">
        <div class="panel tablePanel"><div class="panelHead rankHead"><div class="rankTitle"><h3>Ranked forecasts</h3><span id="tableCount">0 rows</span></div><div class="rankControls"><label>Sort by<select id="rankSort"><option value="name">Name</option><option value="symbol" selected>Symbol</option><option value="price">Price</option><option value="day_abs">Day change</option><option value="day_pct">Day change %</option></select></label><label>Sort order<select id="rankOrder"><option value="asc" selected>Ascending (A-Z)</option><option value="desc">Descending (Z-A)</option></select></label></div></div><div class="rankSearch"><input id="rankSearch" type="search" placeholder="Search ranked forecasts by ticker, company, signal or risk"></div><div class="tableWrap"><table class="resultTable"><thead><tr><th>Ticker</th><th>Price</th><th>Day</th><th id="horizonHead">10D</th><th>Signal</th><th>Conf.</th><th>Risk</th></tr></thead><tbody id="resultsBody"></tbody></table></div></div>
	        <div class="panel"><div class="panelHead analysisClickable" id="selectedAnalysisHead" title="Open measurable market analysis sheet in a new tab" role="button" tabindex="0"><h3>Selected analysis</h3><span id="selectedSub">—</span></div><div id="details" class="details"><div class="empty">Select a row to inspect forecast, confidence, volatility and news.</div></div></div>
	      </div>
	    </section>
	    <footer class="legalFooter" aria-label="Legal notice">
	      <div class="legalCopyright">© 2026 Apex Tool. All rights reserved.</div>
	      <p><strong>Professional analytics notice.</strong> Apex Tool provides data analysis, research, analytical consulting and decision-support services for informational and professional use only. Outputs, scores, forecasts, scenarios, alerts and position levels are model-generated estimates that may rely on delayed, incomplete, estimated or third-party data and may change without notice. They do not constitute personalised investment advice, regulated investment research, an offer, solicitation, guarantee, official or contractual investment document, or any obligation to buy, hold or sell a financial instrument. The platform does not transmit or execute orders.</p>
	      <p>Investing involves risk, including possible loss of capital. Past, simulated and forecast performance is not a reliable indicator of future results. Users remain responsible for independent verification, suitability, compliance and execution decisions and should consult appropriately authorised financial, legal and tax professionals. To the fullest extent permitted by applicable law, Apex Tool disclaims liability for losses or damages arising from use of, interruption of, or reliance on the platform or its data.</p>
	    </footer>
	  </main>
</div>
<script>
let tickers=[], jobId=null, pollTimer=null, results=[], allResults=[], selected=null;
let tickerSelection=new Set(), activeRunTickerSelection=new Set();
let liveTimer=null, liveInflight=false, liveLastPaint=0, liveLastChartPaint=0, liveEventSource=null;
let liveSseOpen=false, liveStreamConnected=false, livePreferredProvider='auto', liveStreamCadenceMs=250, liveStreamStatus={}, lastStreamStatusSignature='';
let chartRenderedLabel='', chartUserRangeChangedAt=0, chartRenderSeq=0, chartViewportRevision=0, chartApplyingRange=false, chartManualViewport=null;
let chartTimezone='Europe/Paris', chartClockTimer=null;
let chartDisplayMode='candles', chartForecastVisible=true, chartTimeframeKey='1m', chartHistoryRangeKey='auto', chartCrosshairWidth=.55;
const CROSSHAIR_WIDTH_STORAGE_KEY='apex-crosshair-width';
let intradayTimer=null, intradayInflightLabel='', liveCandleLastPaint=0, chartFollowLive=true;
let chartPointerActive=false, chartPointerLastMove=0, chartPointerIdleTimer=null, deferredIntradayPaint=null, deferredLiveChartUpdate=null;
let liveDomPaintFrame=0, dynamicRankOrderTimer=null, selectedSheetRefreshTimer=null, lastJobResultFingerprint='';
let chartResizeFrame=0, chartResizeObserver=null, chartResizeLastSize='', chartResizeTimer80=null, chartResizeTimer180=null, chartWindowResizeBound=false;
let chartFullRenderInFlight=false, chartQueuedRender=null;
let chartArchiveTimer=null, chartArchivePaintTimer=null, chartArchiveGeneration=0;
const LIVE_PRICE_INTERVAL_MS=650, LIVE_STREAM_HTTP_FALLBACK_MS=1200, LIVE_PRICE_PAINT_MS=100, LIVE_CHART_PAINT_MS=12000, LIVE_CANDLE_PAINT_MS=250, INTRADAY_REFRESH_MS=9000, CHART_USER_HOLD_MS=9000, CHART_MAX_BARS=3600;
const BTC_CHART_MAX_BARS=2200, BTC_CHART_RECENT_BARS=1500, BTC_CANDLE_PAINT_MS=1000;
const CHART_POINTER_IDLE_MS=90, DYNAMIC_RANK_ORDER_MS=900;
const CHART_ARCHIVE_PAGE_MS=4*86400000, CHART_ARCHIVE_LOOKBACK_MS=370*86400000, CHART_ARCHIVE_EXACT_MAX_SPAN_MS=32*86400000;
const CHART_ARCHIVE_MAX_REQUEST_PAGES=10, CHART_ARCHIVE_CACHE_PAGES=14, CHART_ARCHIVE_PAGE_MAX_BARS=8000, CHART_ARCHIVE_DEBOUNCE_MS=140;
const CHART_TIMEFRAMES=[
  {key:'1s',label:'1s',name:'1 seconde',group:'Secondes',kind:'seconds',bucketMs:1000,fetch:{interval:'1m',range:'30d'}},
  {key:'15s',label:'15s',name:'15 secondes',group:'Secondes',kind:'seconds',bucketMs:15000,fetch:{interval:'1m',range:'30d'}},
  {key:'30s',label:'30s',name:'30 secondes',group:'Secondes',kind:'seconds',bucketMs:30000,fetch:{interval:'1m',range:'30d'}},
  {key:'45s',label:'45s',name:'45 secondes',group:'Secondes',kind:'seconds',bucketMs:45000,fetch:{interval:'1m',range:'30d'}},
  {key:'1m',label:'1m',name:'1 minute',group:'Minutes',kind:'intraday',bucketMs:60000,fetch:{interval:'1m',range:'30d'}},
  {key:'2m',label:'2m',name:'2 minutes',group:'Minutes',kind:'intraday',bucketMs:120000,fetch:{interval:'1m',range:'30d'}},
  {key:'3m',label:'3m',name:'3 minutes',group:'Minutes',kind:'intraday',bucketMs:180000,fetch:{interval:'1m',range:'30d'}},
  {key:'5m',label:'5m',name:'5 minutes',group:'Minutes',kind:'intraday',bucketMs:300000,fetch:{interval:'5m',range:'1mo'}},
  {key:'10m',label:'10m',name:'10 minutes',group:'Minutes',kind:'intraday',bucketMs:600000,fetch:{interval:'5m',range:'1mo'}},
  {key:'15m',label:'15m',name:'15 minutes',group:'Minutes',kind:'intraday',bucketMs:900000,fetch:{interval:'5m',range:'1mo'}},
  {key:'30m',label:'30m',name:'30 minutes',group:'Minutes',kind:'intraday',bucketMs:1800000,fetch:{interval:'5m',range:'1mo'}},
  {key:'45m',label:'45m',name:'45 minutes',group:'Minutes',kind:'intraday',bucketMs:2700000,fetch:{interval:'5m',range:'1mo'}},
  {key:'1h',label:'1h',name:'1 heure',group:'Heures',kind:'intraday',bucketMs:3600000,fetch:{interval:'1h',range:'1y'}},
  {key:'2h',label:'2h',name:'2 heures',group:'Heures',kind:'intraday',bucketMs:7200000,fetch:{interval:'1h',range:'1y'}},
  {key:'3h',label:'3h',name:'3 heures',group:'Heures',kind:'intraday',bucketMs:10800000,fetch:{interval:'1h',range:'1y'}},
  {key:'4h',label:'4h',name:'4 heures',group:'Heures',kind:'intraday',bucketMs:14400000,fetch:{interval:'1h',range:'1y'}},
  {key:'1d',label:'1j',name:'1 jour',group:'Jours et mois',kind:'calendar',calendar:'day',months:0},
  {key:'1w',label:'1sem',name:'1 semaine',group:'Jours et mois',kind:'calendar',calendar:'week',months:0},
  {key:'1mo',label:'1mois',name:'1 mois',group:'Jours et mois',kind:'calendar',calendar:'month',months:1},
  {key:'3mo',label:'3mois',name:'3 mois',group:'Jours et mois',kind:'calendar',calendar:'month',months:3},
  {key:'6mo',label:'6mois',name:'6 mois',group:'Jours et mois',kind:'calendar',calendar:'month',months:6},
  {key:'12mo',label:'12mois',name:'12 mois',group:'Jours et mois',kind:'calendar',calendar:'month',months:12},
  {key:'1y',label:'1an',name:'Historique sur 1 an',group:'Historique',kind:'range',years:1},
  {key:'3y',label:'3ans',name:'Historique sur 3 ans',group:'Historique',kind:'range',years:3},
  {key:'5y',label:'5ans',name:'Historique sur 5 ans',group:'Historique',kind:'range',years:5},
  {key:'all',label:'All',name:'Tout l’historique disponible',group:'Historique',kind:'range',years:0},
];
const CHART_HISTORY_RANGES=[
  {key:'1d',label:'1D',sessions:1},
  {key:'5d',label:'5D',sessions:5},
  {key:'1m',label:'1M',months:1},
  {key:'3m',label:'3M',months:3},
  {key:'6m',label:'6M',months:6},
  {key:'ytd',label:'YTD',ytd:true},
  {key:'1y',label:'1Y',years:1},
  {key:'3y',label:'3Y',years:3},
  {key:'5y',label:'5Y',years:5},
  {key:'all',label:'All'},
];
const selectedAnalysisWindows=new Map();
const fullHistoryCache=new Map(), fullHistoryPending=new Set();
const intradayCache=new Map();
const adaptiveSeriesCache=new Map(), adaptiveSeriesPending=new Map();
const chartArchivePageCache=new Map(), chartArchivePending=new Map();
const rankRowCache=new Map(), rankDomRowCache=new Map(), pendingLiveLabels=new Set(), chartZoneOffsetCache=new Map();
const lastLiveQuoteByLabel=new Map();
let interfaceLanguage='en', visualThemeKey='default', visualModeKey='dark', settingsPositionFrame=0;
const VISUAL_MODES=[
  {key:'dark',nameEn:'Dark',nameFr:'Sombre'},
  {key:'balanced',nameEn:'Balanced',nameFr:'Intermédiaire'},
  {key:'light',nameEn:'Light',nameFr:'Clair'},
];
const VISUAL_THEMES=[
  {key:'default',nameEn:'Current default',nameFr:'Actuel (défaut)',base:'#070a10',accent:'#3b82f6'},
  {key:'graphite',nameEn:'Graphite',nameFr:'Graphite',base:'#101216',accent:'#94a3b8'},
  {key:'steel',nameEn:'Steel',nameFr:'Acier',base:'#101b25',accent:'#60a5fa'},
  {key:'midnight',nameEn:'Midnight',nameFr:'Minuit',base:'#071426',accent:'#3b82f6'},
  {key:'navy',nameEn:'Navy',nameFr:'Marine',base:'#071a33',accent:'#2563eb'},
  {key:'cobalt',nameEn:'Cobalt',nameFr:'Cobalt',base:'#081c3f',accent:'#3b82f6'},
  {key:'indigo',nameEn:'Indigo',nameFr:'Indigo',base:'#11113b',accent:'#6366f1'},
  {key:'violet',nameEn:'Violet',nameFr:'Violet',base:'#1b0d38',accent:'#8b5cf6'},
  {key:'purple',nameEn:'Purple',nameFr:'Pourpre',base:'#270c36',accent:'#a855f7'},
  {key:'orchid',nameEn:'Orchid',nameFr:'Orchidée',base:'#310b32',accent:'#d946ef'},
  {key:'magenta',nameEn:'Magenta',nameFr:'Magenta',base:'#330a29',accent:'#ec4899'},
  {key:'rose',nameEn:'Rose',nameFr:'Rose',base:'#330b20',accent:'#f43f5e'},
  {key:'crimson',nameEn:'Crimson',nameFr:'Cramoisi',base:'#320b16',accent:'#ef4444'},
  {key:'ruby',nameEn:'Ruby',nameFr:'Rubis',base:'#300c10',accent:'#f87171'},
  {key:'vermilion',nameEn:'Vermilion',nameFr:'Vermillon',base:'#321008',accent:'#fb7185'},
  {key:'orange',nameEn:'Orange',nameFr:'Orange',base:'#321706',accent:'#f97316'},
  {key:'amber',nameEn:'Amber',nameFr:'Ambre',base:'#302006',accent:'#f59e0b'},
  {key:'gold',nameEn:'Gold',nameFr:'Or',base:'#2b2406',accent:'#eab308'},
  {key:'yellow',nameEn:'Yellow',nameFr:'Jaune',base:'#292806',accent:'#facc15'},
  {key:'lime',nameEn:'Lime',nameFr:'Citron vert',base:'#1d2b07',accent:'#84cc16'},
  {key:'forest',nameEn:'Forest',nameFr:'Forêt',base:'#0b2812',accent:'#22c55e'},
  {key:'emerald',nameEn:'Emerald',nameFr:'Émeraude',base:'#07291c',accent:'#10b981'},
  {key:'jade',nameEn:'Jade',nameFr:'Jade',base:'#062c25',accent:'#14b8a6'},
  {key:'teal',nameEn:'Teal',nameFr:'Sarcelle',base:'#052d2d',accent:'#2dd4bf'},
  {key:'cyan',nameEn:'Cyan',nameFr:'Cyan',base:'#052b35',accent:'#22d3ee'},
  {key:'azure',nameEn:'Azure',nameFr:'Azur',base:'#06263d',accent:'#38bdf8'},
  {key:'sky',nameEn:'Sky',nameFr:'Ciel',base:'#071f3a',accent:'#0ea5e9'},
  {key:'slate',nameEn:'Slate',nameFr:'Ardoise',base:'#131b2a',accent:'#64748b'},
  {key:'warm',nameEn:'Warm',nameFr:'Chaud',base:'#231915',accent:'#fb923c'},
  {key:'black',nameEn:'Black',nameFr:'Noir',base:'#030406',accent:'#e2e8f0'},
  {key:'white',nameEn:'White',nameFr:'Blanc',base:'#f8fafc',darkBase:'#20252d',balancedBase:'#707986',lightBase:'#f8fafc',accent:'#2563eb'},
  {key:'neutral-gray',nameEn:'Neutral gray',nameFr:'Gris neutre',base:'#6b7280',darkBase:'#20242b',balancedBase:'#6b7280',lightBase:'#f3f4f6',accent:'#334155'},
  {key:'basic-blue',nameEn:'Basic blue',nameFr:'Bleu classique',base:'#2563eb',darkBase:'#081d4a',balancedBase:'#315b9c',lightBase:'#eff6ff',accent:'#2563eb'},
  {key:'basic-green',nameEn:'Basic green',nameFr:'Vert classique',base:'#16a34a',darkBase:'#082b17',balancedBase:'#2f7a49',lightBase:'#f0fdf4',accent:'#16a34a'},
  {key:'basic-red',nameEn:'Basic red',nameFr:'Rouge classique',base:'#dc2626',darkBase:'#320b0b',balancedBase:'#8f3939',lightBase:'#fef2f2',accent:'#dc2626'},
  {key:'basic-orange',nameEn:'Basic orange',nameFr:'Orange classique',base:'#ea580c',darkBase:'#351407',balancedBase:'#9c552e',lightBase:'#fff7ed',accent:'#ea580c'},
  {key:'basic-yellow',nameEn:'Basic yellow',nameFr:'Jaune classique',base:'#eab308',darkBase:'#302606',balancedBase:'#8b792c',lightBase:'#fefce8',accent:'#ca8a04'},
  {key:'basic-purple',nameEn:'Basic purple',nameFr:'Violet classique',base:'#9333ea',darkBase:'#26083d',balancedBase:'#704292',lightBase:'#faf5ff',accent:'#9333ea'},
  {key:'basic-pink',nameEn:'Basic pink',nameFr:'Rose classique',base:'#db2777',darkBase:'#35091f',balancedBase:'#8f3b63',lightBase:'#fdf2f8',accent:'#db2777'},
  {key:'neon-green',nameEn:'Neon green',nameFr:'Vert fluo',base:'#39ff14',darkBase:'#072b04',balancedBase:'#427f36',lightBase:'#efffec',accent:'#39ff14',lightAccent:'#15803d'},
  {key:'neon-cyan',nameEn:'Neon cyan',nameFr:'Cyan fluo',base:'#00f5ff',darkBase:'#00272b',balancedBase:'#26767a',lightBase:'#ecfeff',accent:'#00f5ff',lightAccent:'#0891b2'},
  {key:'neon-pink',nameEn:'Neon pink',nameFr:'Rose fluo',base:'#ff2bd6',darkBase:'#300529',balancedBase:'#8a3779',lightBase:'#fff0fc',accent:'#ff2bd6',lightAccent:'#c026a5'},
  {key:'electric-purple',nameEn:'Electric purple',nameFr:'Violet électrique',base:'#bf5af2',darkBase:'#21052f',balancedBase:'#6f3c85',lightBase:'#faf0ff',accent:'#bf5af2',lightAccent:'#9333ea'},
  {key:'neon-orange',nameEn:'Neon orange',nameFr:'Orange fluo',base:'#ff7a00',darkBase:'#321704',balancedBase:'#9a5a24',lightBase:'#fff7ed',accent:'#ff7a00',lightAccent:'#ea580c'},
  {key:'acid-yellow',nameEn:'Acid yellow',nameFr:'Jaune acide',base:'#e6ff00',darkBase:'#262b00',balancedBase:'#747d2f',lightBase:'#fdffe8',accent:'#e6ff00',lightAccent:'#a16207'},
  {key:'laser-blue',nameEn:'Laser blue',nameFr:'Bleu laser',base:'#2f6bff',darkBase:'#07194a',balancedBase:'#345da8',lightBase:'#eef4ff',accent:'#2f6bff'},
];
const DISPLAY_COPY={
  en:{settings:'Settings',close:'Close settings',appearance:'Appearance',background:'Background color',palette:'Background color palette',language:'Interface language',menu:'Show or hide menus',runConfig:'Forecast run configuration',mode:'Mode',fast:'Fast scan',balanced:'Balanced',deep:'Deep history',days:'Days',bars:'Bars',turbo:'Turbo auto',news:'News',enabled:'Enabled',disabled:'Disabled',headlines:'Headlines',run:'Run analysis',stop:'Stop',ready:'Ready',tracker:'Ticker Tracker',tickerSearch:'Search ticker or company',selectAll:'Select all',clear:'Clear',mega:'Mega caps',returnConfig:'RETURN PREVISION CONFIG',start:'Start',end:'End',startDate:'Start date',endDate:'End date',returnHelp:'Maximum 15 tickers · allocation required per selected ticker · $0 to $1B per ticker.',tickers:'Tickers',funds:'Allocated funds',runReturn:'Run return prevision',dashboard:'Market forecast dashboard',idle:'Idle',coverage:'Coverage',successful:'successful tickers',average:'Average 10D',watchlist:'watchlist forecast',constructive:'Constructive',positive:'positive setups',confidence:'Confidence',mean:'mean score',risk:'Risk',regime:'dominant regime',chartTitle:'Interactive price path',chartSub:'Select a ticker after a run',chartInstruction:'Open the three-bar menu to configure the run.',fullscreen:'Fullscreen',profile:'Company profile & investor view',selectTicker:'Select a ticker',profileEmpty:'Company profile appears here after selecting a forecast.',returnTitle:'Return prevision',returnSub:'Configure dates, tickers and allocated funds in Return prevision config',returnEmpty:'Run a return prevision from Return prevision config.',ranked:'Ranked forecasts',rows:'rows',sortBy:'Sort by',sortOrder:'Sort order',name:'Name',symbol:'Symbol',price:'Price',dayChange:'Day change',dayChangePct:'Day change %',ascending:'Ascending (A-Z)',descending:'Descending (Z-A)',rankSearch:'Search ranked forecasts by ticker, company, signal or risk',ticker:'Ticker',day:'Day',signal:'Signal',conf:'Conf.',selected:'Selected analysis',selectedTitle:'Open measurable market analysis sheet in a new tab',selectedEmpty:'Select a row to inspect forecast, confidence, volatility and news.',learningMonitor:'Remote learning v15',learningStorage:'Storage',learningEvaluated:'Exact evaluations',learningProfiles:'Validated profiles',learningWrite:'Last successful write',learningImpact:'Selected forecast impact',learningChecking:'Checking',learningHealthy:'Healthy',learningDegraded:'Degraded',learningDisabled:'Disabled',learningFailed:'Unavailable'},
  fr:{settings:'Paramètres',close:'Fermer les paramètres',appearance:"Mode d'affichage",background:'Couleur de fond',palette:'Palette de couleurs de fond',language:"Langue de l'interface",menu:'Afficher ou masquer les menus',runConfig:'Configuration du run de prévision',mode:'Mode',fast:'Analyse rapide',balanced:'Équilibré',deep:'Historique profond',days:'Jours',bars:'Barres',turbo:'Turbo auto',news:'Actualités',enabled:'Activées',disabled:'Désactivées',headlines:'Titres',run:"Lancer l'analyse",stop:'Arrêter',ready:'Prêt',tracker:'Suivi des tickers',tickerSearch:'Rechercher un ticker ou une entreprise',selectAll:'Tout sélectionner',clear:'Effacer',mega:'Mega caps',returnConfig:'CONFIGURATION DES RENDEMENTS',start:'Début',end:'Fin',startDate:'Date de début',endDate:'Date de fin',returnHelp:"15 tickers maximum · allocation requise par ticker sélectionné · 0 $ à 1 Md$ par ticker.",tickers:'Tickers',funds:'Fonds alloués',runReturn:'Lancer la prévision de rendement',dashboard:'Tableau de bord des prévisions',idle:'Inactif',coverage:'Couverture',successful:'tickers analysés',average:'Moyenne 10J',watchlist:'prévision watchlist',constructive:'Constructifs',positive:'setups positifs',confidence:'Confiance',mean:'score moyen',risk:'Risque',regime:'régime dominant',chartTitle:'Évolution interactive du prix',chartSub:'Sélectionnez un ticker après un run',chartInstruction:"Ouvrez le menu à trois barres pour configurer le run.",fullscreen:'Plein écran',profile:'Profil entreprise et vue investisseur',selectTicker:'Sélectionnez un ticker',profileEmpty:'Le profil entreprise apparaît ici après la sélection d\'une prévision.',returnTitle:'Prévision de rendement',returnSub:'Configurez les dates, tickers et fonds alloués dans la configuration des rendements',returnEmpty:'Lancez une prévision depuis la configuration des rendements.',ranked:'Prévisions classées',rows:'lignes',sortBy:'Trier par',sortOrder:'Ordre',name:'Nom',symbol:'Symbole',price:'Prix',dayChange:'Variation du jour',dayChangePct:'Variation du jour %',ascending:'Croissant (A-Z)',descending:'Décroissant (Z-A)',rankSearch:'Rechercher par ticker, entreprise, signal ou risque',ticker:'Ticker',day:'Jour',signal:'Signal',conf:'Conf.',selected:'Analyse sélectionnée',selectedTitle:"Ouvrir la fiche d'analyse de marché mesurable dans un nouvel onglet",selectedEmpty:'Sélectionnez une ligne pour examiner la prévision, la confiance, la volatilité et les actualités.',learningMonitor:'Apprentissage distant v15',learningStorage:'Stockage',learningEvaluated:'Évaluations exactes',learningProfiles:'Profils validés',learningWrite:'Dernière écriture réussie',learningImpact:'Impact sur la prévision sélectionnée',learningChecking:'Vérification',learningHealthy:'Opérationnel',learningDegraded:'Dégradé',learningDisabled:'Désactivé',learningFailed:'Indisponible'},
};
let chartParisPartsFormatter=null, chartClockParisFormatter=null, chartIntradayTimeFormatter=null, chartDateFormatter=null, chartPriceFormatter2=null, chartPriceFormatter4=null;
const $=id=>document.getElementById(id); const STABLE_PCT=0.12; function horizon(){return selected?.horizon_days || Number($('days')?.value||10) || 10;} function chg(r){return Number(r?.change_horizon_pct ?? r?.change_5d_pct ?? 0);}
try{
  const savedProvider=localStorage.getItem('apex-live-provider');
  const savedCadence=Number(localStorage.getItem('apex-live-cadence'));
  if(['auto','massive','alpaca','finnhub','twelvedata','coinbase','http'].includes(savedProvider))livePreferredProvider=savedProvider;
  if([100,250,500,1000].includes(savedCadence))liveStreamCadenceMs=savedCadence;
}catch(e){}
function esc(s){return String(s??'').replace(/[&<>"']/g,m=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#039;'}[m]));}
function fmtPct(x){ if(x===null||x===undefined||Number.isNaN(Number(x))) return '—'; x=Number(x); if(Math.abs(x)<STABLE_PCT) return 'Stable'; return (x>0?'+':'')+x.toFixed(2)+'%'; }
function pctClass(x){ x=Number(x||0); if(Math.abs(x)<STABLE_PCT) return 'neu'; return x>0?'pos':'neg'; }
function money(x){ if(x===null||x===undefined||Number.isNaN(Number(x)))return '—'; return Number(x).toLocaleString('en-US',{maximumFractionDigits:2}); }
function badge(signal){ const s=(signal||'').toLowerCase(); let c=s.includes('constructive')?'green':s.includes('cautious')?'red':'amber'; return `<span class="badge ${c}">${esc(signal||'Neutral')}</span>`; }
async function api(path, opts={}){ const r=await fetch(path,{headers:{'Content-Type':'application/json'},...opts}); return await r.json(); }
let learningStatusSnapshot=null,learningStatusTimer=null,learningRefreshInFlight=false;
function learningImpactText(r=selected){
  const learning=r?.server_learning||{};
  if(learning.status==='remote_error')return 'Remote error';
  const raw=Number(learning.raw_forecast_pct),calibrated=Number(learning.calibrated_forecast_pct),delta=Number(learning.forecast_delta_pct);
  if(Number.isFinite(raw)&&Number.isFinite(calibrated)){
    const scope=learning.profile_scope?` · ${learning.profile_scope}`:'';
    return `${raw>=0?'+':''}${raw.toFixed(2)}% → ${calibrated>=0?'+':''}${calibrated.toFixed(2)}% · Δ ${delta>=0?'+':''}${delta.toFixed(2)}%${scope}`;
  }
  return learning.applied===false?'No validated profile':'—';
}
function updateLearningSelectedImpact(){const element=$('learningSelectedImpact');if(element)element.textContent=learningImpactText();}
function learningWriteText(value){
  if(!value)return '—';
  const timestamp=value.completed_at||value.created_at||value;
  const parsed=new Date(timestamp);
  return Number.isNaN(parsed.getTime())?String(timestamp):parsed.toLocaleString(interfaceLanguage==='fr'?'fr-FR':'en-US',{dateStyle:'short',timeStyle:'medium'});
}
function renderLearningMonitor(payload){
  learningStatusSnapshot=payload||{};
  const copy=DISPLAY_COPY[interfaceLanguage]||DISPLAY_COPY.en;
  const runtime=payload?.dashboard_runtime||{},storage=payload?.storage||payload?.stats?.storage||{};
  const configured=payload?.configured!==false&&payload?.configuration?.configured!==false;
  const failed=Boolean(payload?.error),degraded=Boolean(storage?.warning)||payload?.status==='degraded'||(!payload?.ok&&configured&&!failed);
  const state=$('learningMonitorState');
  let stateClass='healthy',stateText=copy.learningHealthy;
  if(!configured){stateClass='disabled';stateText=copy.learningDisabled;}
  else if(failed){stateClass='failed';stateText=copy.learningFailed;}
  else if(degraded){stateClass='degraded';stateText=copy.learningDegraded;}
  if(state){state.className=`learningState ${stateClass}`;state.textContent=stateText;}
  const persistent=storage?.persistent===true?'persistent':storage?.persistent===false?'ephemeral':'unverified';
  setTextIfChanged($('learningStorage'),`${storage?.backend||'—'} · ${persistent}`);
  setTextIfChanged($('learningEvaluated'),`${Number(payload?.evaluated||0).toLocaleString()} / ${Number(payload?.predictions||0).toLocaleString()}`);
  setTextIfChanged($('learningProfiles'),`${Number(payload?.approved_profiles||0).toLocaleString()} / ${Number(payload?.calibration_profiles||0).toLocaleString()}`);
  setTextIfChanged($('learningLastWrite'),learningWriteText(payload?.last_successful_write));
  updateLearningSelectedImpact();
  const errorText=payload?.error||runtime?.last_error||storage?.warning||'';
  const error=$('learningError');if(error){error.textContent=errorText;error.classList.toggle('visible',Boolean(errorText));}
}
async function refreshLearningMonitor(force=false){
  if(learningRefreshInFlight)return;
  learningRefreshInFlight=true;$('learningRefresh')?.classList.add('loading');
  try{
    const payload=await api('/api/learning'+(force?'?force=1':''));
    renderLearningMonitor(payload);
  }catch(error){
    renderLearningMonitor({ok:false,configured:true,error:String(error?.message||error)});
  }finally{
    learningRefreshInFlight=false;$('learningRefresh')?.classList.remove('loading');
  }
}

function parseThemeHex(hex){
  const value=String(hex||'#000000').replace('#','');
  return [0,2,4].map(index=>parseInt(value.slice(index,index+2),16)||0);
}
function blendThemeHex(from,to,amount){
  const a=parseThemeHex(from),b=parseThemeHex(to),t=Math.max(0,Math.min(1,Number(amount)||0));
  return '#'+a.map((value,index)=>Math.round(value+(b[index]-value)*t).toString(16).padStart(2,'0')).join('');
}
function themeRgbTriplet(hex){return parseThemeHex(hex).join(',');}
function normalizedVisualMode(key){return VISUAL_MODES.some(mode=>mode.key===key)?key:'dark';}
function visualThemeTokens(theme,modeKey=visualModeKey){
  const mode=normalizedVisualMode(modeKey),darkSeed=theme.darkBase||theme.base,accent=mode==='light'?(theme.lightAccent||theme.accent):theme.accent;
  if(mode==='dark'&&theme.key==='default')return {mode,scheme:'dark',bg:'#070a10',top:'#080b12',deep:'#05070c',side:'#0f172a',panel:'#0d111c',card:'#121a2a',control:'#090e19',section:'#0f172a',line:'#253047',text:'#edf4ff',muted:'#94a3b8',soft:'#c7d2fe',accent:'#3b82f6',accent2:'#22d3ee'};
  if(mode==='dark')return {mode,scheme:'dark',bg:darkSeed,top:blendThemeHex(darkSeed,'#172033',.22),deep:blendThemeHex(darkSeed,'#000000',.30),side:blendThemeHex(darkSeed,'#1c2a43',.30),panel:blendThemeHex(darkSeed,'#151b29',.34),card:blendThemeHex(darkSeed,'#26334a',.31),control:blendThemeHex(darkSeed,'#02050a',.28),section:blendThemeHex(darkSeed,'#1b2740',.27),line:blendThemeHex(darkSeed,'#667085',.38),text:'#edf4ff',muted:'#94a3b8',soft:'#c7d2fe',accent,accent2:blendThemeHex(accent,'#67e8f9',.34)};
  if(mode==='balanced'){
    const base=theme.balancedBase||blendThemeHex(darkSeed,'#ffffff',.29);
    return {mode,scheme:'dark',bg:base,top:blendThemeHex(base,'#ffffff',.05),deep:blendThemeHex(base,'#000000',.12),side:blendThemeHex(base,'#111827',.10),panel:blendThemeHex(base,'#ffffff',.08),card:blendThemeHex(base,'#ffffff',.13),control:blendThemeHex(base,'#000000',.14),section:blendThemeHex(base,'#ffffff',.06),line:blendThemeHex(base,'#ffffff',.34),text:'#f8fafc',muted:'#d2d8e2',soft:'#e0e7ff',accent,accent2:blendThemeHex(accent,'#ffffff',.24)};
  }
  const base=theme.lightBase||blendThemeHex(theme.base,'#ffffff',.90);
  return {mode,scheme:'light',bg:base,top:blendThemeHex(base,'#ffffff',.46),deep:blendThemeHex(base,'#334155',.08),side:blendThemeHex(base,'#ffffff',.42),panel:blendThemeHex(base,'#ffffff',.72),card:blendThemeHex(base,'#ffffff',.84),control:blendThemeHex(base,'#0f172a',.06),section:blendThemeHex(base,'#0f172a',.035),line:blendThemeHex(base,'#334155',.22),text:'#172033',muted:'#5f6b7a',soft:'#334155',accent,accent2:blendThemeHex(accent,'#0891b2',.28)};
}
function activeTheme(){return VISUAL_THEMES.find(theme=>theme.key===visualThemeKey)||VISUAL_THEMES[0];}
function themeDisplayName(theme){return interfaceLanguage==='fr'?theme.nameFr:theme.nameEn;}
function modeDisplayName(mode){return interfaceLanguage==='fr'?mode.nameFr:mode.nameEn;}
function visualThemeProperties(theme,tokens=visualThemeTokens(theme)){
  return {'--theme-bg':tokens.bg,'--theme-bg-top':tokens.top,'--theme-bg-deep':tokens.deep,'--theme-side-top':tokens.side,'--theme-panel':tokens.panel,'--theme-card-top':tokens.card,'--theme-control':tokens.control,'--theme-section':tokens.section,'--theme-line':tokens.line,'--theme-bg-rgb':themeRgbTriplet(tokens.bg),'--theme-accent-rgb':themeRgbTriplet(tokens.accent),'--theme-text':tokens.text,'--theme-muted':tokens.muted,'--theme-soft':tokens.soft,'--bg':tokens.bg,'--surface':tokens.panel,'--surface2':tokens.card,'--card':tokens.card,'--line':tokens.line,'--text':tokens.text,'--muted':tokens.muted,'--soft':tokens.soft,'--blue':tokens.accent,'--cyan':tokens.accent2,colorScheme:tokens.scheme};
}
function sharedVisualThemePayload(theme,tokens=visualThemeTokens(theme)){
  return {key:theme.key,...tokens,bgRgb:themeRgbTriplet(tokens.bg),accentRgb:themeRgbTriplet(tokens.accent)};
}
function applyVisualThemeProperties(root,properties){if(root)Object.entries(properties).forEach(([name,value])=>{if(name==='colorScheme')root.style.colorScheme=value;else root.style.setProperty(name,value);});}
function applySelectedAnalysisTheme(win,theme=activeTheme(),tokens=visualThemeTokens(theme)){
  try{if(win&&!win.closed){
    applyVisualThemeProperties(win.document.documentElement,visualThemeProperties(theme,tokens));
    if(win.document.body)win.document.body.dataset.visualMode=visualModeKey;
    let override=win.document.getElementById('apexThemeOverrides');
    if(!override){override=win.document.createElement('style');override.id='apexThemeOverrides';win.document.head.appendChild(override);}
    override.textContent='body{color:var(--text)!important}.top p,.amountBox label,.amountBox small,.sheetMetric span,.whyGrid span,.sheetEmpty{color:var(--muted)!important}.amountBox input{color:var(--text)!important}.sheetBlock p,.sheetBlock li{color:var(--text)!important}';
  }}catch(e){}
}
function updateVisualThemeControls(){
  document.querySelectorAll('.colorSwatch').forEach(button=>{const active=button.dataset.theme===visualThemeKey;button.classList.toggle('active',active);button.setAttribute('aria-checked',String(active));});
  document.querySelectorAll('.settingsModeBtn').forEach(button=>{const active=button.dataset.mode===visualModeKey;button.classList.toggle('active',active);button.setAttribute('aria-checked',String(active));});
  const output=$('settingsColorName');if(output)output.textContent=themeDisplayName(activeTheme());
}
function syncDashboardPlotTheme(){
  const el=$('chart');if(!window.Plotly||!el?._fullLayout)return;
  const tokens=visualThemeTokens(activeTheme());
  Plotly.relayout(el,{paper_bgcolor:tokens.panel,plot_bgcolor:tokens.panel,'font.color':tokens.muted,'xaxis.gridcolor':tokens.line,'xaxis.linecolor':tokens.line,'xaxis.tickcolor':tokens.line,'xaxis.tickfont.color':tokens.muted,'yaxis.gridcolor':tokens.line,'yaxis.linecolor':tokens.line,'yaxis.tickcolor':tokens.line,'yaxis.tickfont.color':tokens.muted}).catch(()=>{});
}
function applyVisualTheme(key,persist=true){
  const theme=VISUAL_THEMES.find(item=>item.key===key)||VISUAL_THEMES[0],tokens=visualThemeTokens(theme),root=document.documentElement;
  visualThemeKey=theme.key;
  const properties=visualThemeProperties(theme,tokens);
  applyVisualThemeProperties(root,properties);
  document.body.dataset.visualTheme=theme.key;
  document.body.dataset.visualMode=visualModeKey;
  updateVisualThemeControls();
  try{localStorage.setItem('apex-visual-theme-tokens',JSON.stringify(sharedVisualThemePayload(theme,tokens)));if(persist)localStorage.setItem('apex-visual-theme',theme.key);}catch(e){}
  for(const [label,entry] of selectedAnalysisWindows){if(!entry?.win||entry.win.closed)selectedAnalysisWindows.delete(label);else applySelectedAnalysisTheme(entry.win,theme,tokens);}
  requestAnimationFrame(syncDashboardPlotTheme);
}
function applyVisualMode(key,persist=true){
  visualModeKey=normalizedVisualMode(key);
  applyVisualTheme(visualThemeKey,false);
  renderVisualThemePalette();updateVisualThemeControls();
  if(persist)try{localStorage.setItem('apex-visual-mode',visualModeKey);}catch(e){}
}
function setDisplayText(selector,value){const element=document.querySelector(selector);if(element)element.textContent=value;}
function setFieldLabel(controlId,value){const control=$(controlId),label=control?.closest('.field')?.querySelector('label');if(label)label.textContent=value;}
function setSelectOption(selectId,value,label){const option=$(selectId)?.querySelector(`option[value="${value}"]`);if(option)option.textContent=label;}
function setSelectOwnLabel(selectId,value){
  const select=$(selectId),label=select?.closest('label');if(!label)return;
  const textNode=[...label.childNodes].find(node=>node.nodeType===Node.TEXT_NODE);
  if(textNode)textNode.textContent=value;else label.insertBefore(document.createTextNode(value),select);
}
function applyInterfaceLanguage(language,persist=true){
  interfaceLanguage=language==='fr'?'fr':'en';
  const copy=DISPLAY_COPY[interfaceLanguage],isIdle=!jobId&&!results.length&&!selected;
  document.documentElement.lang=interfaceLanguage==='fr'?'fr-FR':'en-US';
  for(const input of [$('returnStart'),$('returnEnd')])if(input)input.lang=document.documentElement.lang;
  if($('returnStart'))$('returnStart').setAttribute('aria-label',copy.startDate);
  if($('returnEnd'))$('returnEnd').setAttribute('aria-label',copy.endDate);
  if($('settingsLanguage'))$('settingsLanguage').value=interfaceLanguage;
  setDisplayText('#settingsTitle',copy.settings);setDisplayText('#settingsModeLabel',copy.appearance);setDisplayText('#settingsColorLabel',copy.background);setDisplayText('#settingsLanguageLabel',copy.language);
  setDisplayText('#learningMonitorLabel',copy.learningMonitor);setDisplayText('#learningStorageLabel',copy.learningStorage);setDisplayText('#learningEvaluatedLabel',copy.learningEvaluated);setDisplayText('#learningProfilesLabel',copy.learningProfiles);setDisplayText('#learningWriteLabel',copy.learningWrite);setDisplayText('#learningImpactLabel',copy.learningImpact);
  if($('settingsMode'))$('settingsMode').setAttribute('aria-label',copy.appearance);
  for(const mode of VISUAL_MODES){const button=document.querySelector(`.settingsModeBtn[data-mode="${mode.key}"]`);if(button)button.textContent=modeDisplayName(mode);}
  if($('settingsPalette'))$('settingsPalette').setAttribute('aria-label',copy.palette);
  if($('settingsToggle')){ $('settingsToggle').title=copy.settings;$('settingsToggle').setAttribute('aria-label',copy.settings); }
  if($('settingsClose')){ $('settingsClose').title=copy.close;$('settingsClose').setAttribute('aria-label',copy.close); }
  if($('sideToggle')){ $('sideToggle').title=copy.menu;$('sideToggle').setAttribute('aria-label',copy.menu); }
  const sectionTitles=document.querySelectorAll('.side .section h2');
  if(sectionTitles[0])sectionTitles[0].textContent=copy.runConfig;if(sectionTitles[1])sectionTitles[1].textContent=copy.tracker;if(sectionTitles[2])sectionTitles[2].textContent=copy.returnConfig;
  setFieldLabel('mode',copy.mode);setFieldLabel('days',copy.days);setFieldLabel('bars',copy.bars);setFieldLabel('news',copy.news);setFieldLabel('newsLimit',copy.headlines);setFieldLabel('returnStart',copy.start);setFieldLabel('returnEnd',copy.end);
  if($('bars'))$('bars').placeholder=copy.turbo;
  setSelectOption('mode','fast',copy.fast);setSelectOption('mode','balanced',copy.balanced);setSelectOption('mode','deep',copy.deep);setSelectOption('news','1',copy.enabled);setSelectOption('news','0',copy.disabled);
  setDisplayText('#runBtn',copy.run);setDisplayText('#stopBtn',copy.stop);if(isIdle)setDisplayText('#progressText',copy.ready);
  if($('tickerSearch'))$('tickerSearch').placeholder=copy.tickerSearch;setDisplayText('#selectAll',copy.selectAll);setDisplayText('#clearAll',copy.clear);setDisplayText('#megaCaps',copy.mega);
  setDisplayText('.returnHelp',copy.returnHelp);const returnHeaders=document.querySelectorAll('.returnConfigTable th');if(returnHeaders[0])returnHeaders[0].textContent=copy.tickers;if(returnHeaders[1])returnHeaders[1].textContent=copy.funds;setDisplayText('#returnRunBtn',copy.runReturn);
  setDisplayText('.dashTitle','Dashboard');if(isIdle)setDisplayText('#stateText',copy.idle);
  const cards=document.querySelectorAll('.cards .card'),cardCopy=[[copy.coverage,copy.successful],[copy.average,copy.watchlist],[copy.constructive,copy.positive],[copy.confidence,copy.mean],[copy.risk,copy.regime]];
  cards.forEach((card,index)=>{const values=cardCopy[index];if(!values)return;const key=card.querySelector('.k'),sub=card.querySelector('.s');if(key)key.textContent=values[0];if(sub)sub.textContent=values[1];});
  if(!selected){setDisplayText('#chartTitle',copy.chartTitle);setDisplayText('#chartSub',copy.chartSub);setDisplayText('.chartInstruction small',copy.chartInstruction);setDisplayText('#profileSub',copy.selectTicker);setDisplayText('#selectedSub','—');}
  setDisplayText('#chartFullBtn',copy.fullscreen);setDisplayText('.profilePanel .panelHead h3',copy.profile);if(!selected)setDisplayText('#profilePanel .empty',copy.profileEmpty);
  setDisplayText('.returnPanel .panelHead h3',copy.returnTitle);if(!results.length){setDisplayText('#returnSub',copy.returnSub);setDisplayText('#returnStatus',copy.idle);setDisplayText('#returnPanel .empty',copy.returnEmpty);}
  setDisplayText('.rankTitle h3',copy.ranked);if(!results.length)setDisplayText('#tableCount',`0 ${copy.rows}`);setSelectOwnLabel('rankSort',copy.sortBy);setSelectOwnLabel('rankOrder',copy.sortOrder);
  setSelectOption('rankSort','name',copy.name);setSelectOption('rankSort','symbol',copy.symbol);setSelectOption('rankSort','price',copy.price);setSelectOption('rankSort','day_abs',copy.dayChange);setSelectOption('rankSort','day_pct',copy.dayChangePct);setSelectOption('rankOrder','asc',copy.ascending);setSelectOption('rankOrder','desc',copy.descending);
  if($('rankSearch'))$('rankSearch').placeholder=copy.rankSearch;const rankHeaders=document.querySelectorAll('.resultTable th');const rankCopy=[copy.ticker,copy.price,copy.day,null,copy.signal,copy.conf,copy.risk];rankHeaders.forEach((header,index)=>{if(rankCopy[index])header.textContent=rankCopy[index];});
  setDisplayText('#selectedAnalysisHead h3',copy.selected);if($('selectedAnalysisHead'))$('selectedAnalysisHead').title=copy.selectedTitle;if(!selected)setDisplayText('#details .empty',copy.selectedEmpty);
  if(learningStatusSnapshot)renderLearningMonitor(learningStatusSnapshot);
  updateVisualThemeControls();
  if(persist)try{localStorage.setItem('apex-interface-language',interfaceLanguage);}catch(e){}
}
function renderVisualThemePalette(){
  const palette=$('settingsPalette');if(!palette)return;palette.replaceChildren();
  for(const theme of VISUAL_THEMES){
    const tokens=visualThemeTokens(theme),button=document.createElement('button');button.type='button';button.className='colorSwatch';button.dataset.theme=theme.key;button.setAttribute('role','radio');button.style.setProperty('--swatch',`linear-gradient(135deg,${tokens.bg} 0 49%,${tokens.accent} 51% 100%)`);button.title=themeDisplayName(theme);button.setAttribute('aria-label',themeDisplayName(theme));palette.appendChild(button);
  }
}
function positionDisplaySettings(){
  const panel=$('settingsPanel'),button=$('settingsToggle');if(!panel||!button||!panel.classList.contains('open'))return;
  const anchor=button.getBoundingClientRect(),width=panel.offsetWidth||360,height=panel.offsetHeight||300,margin=14;
  const left=Math.min(Math.max(margin,anchor.right+10),Math.max(margin,window.innerWidth-width-margin));
  const top=Math.min(Math.max(margin,anchor.top),Math.max(margin,window.innerHeight-height-margin));
  panel.style.left=`${Math.round(left)}px`;panel.style.top=`${Math.round(top)}px`;
}
function closeDisplaySettings(returnFocus=false){
  const panel=$('settingsPanel'),backdrop=$('settingsBackdrop'),button=$('settingsToggle');if(!panel||!button)return;
  panel.classList.remove('open');backdrop?.classList.remove('open');button.classList.remove('active');button.setAttribute('aria-expanded','false');panel.setAttribute('aria-hidden','true');if(returnFocus)button.focus({preventScroll:true});
}
function openDisplaySettings(){
  const panel=$('settingsPanel'),backdrop=$('settingsBackdrop'),button=$('settingsToggle');if(!panel||!button)return;
  panel.classList.add('open');backdrop?.classList.add('open');button.classList.add('active');button.setAttribute('aria-expanded','true');panel.setAttribute('aria-hidden','false');positionDisplaySettings();$('settingsClose')?.focus({preventScroll:true});
}
function initDisplaySettings(){
  let savedTheme='default',savedLanguage='en',savedMode='dark';
  try{savedTheme=localStorage.getItem('apex-visual-theme')||'default';savedLanguage=localStorage.getItem('apex-interface-language')||'en';savedMode=localStorage.getItem('apex-visual-mode')||'dark';}catch(e){}
  interfaceLanguage=savedLanguage==='fr'?'fr':'en';visualModeKey=normalizedVisualMode(savedMode);renderVisualThemePalette();applyVisualTheme(savedTheme,false);applyInterfaceLanguage(interfaceLanguage,false);
  $('settingsToggle')?.addEventListener('click',()=>$('settingsPanel')?.classList.contains('open')?closeDisplaySettings():openDisplaySettings());
  $('settingsClose')?.addEventListener('click',()=>closeDisplaySettings(true));$('settingsBackdrop')?.addEventListener('click',()=>closeDisplaySettings());
  $('settingsMode')?.addEventListener('click',event=>{const button=event.target.closest('.settingsModeBtn');if(button)applyVisualMode(button.dataset.mode);});
  $('settingsPalette')?.addEventListener('click',event=>{const button=event.target.closest('.colorSwatch');if(button)applyVisualTheme(button.dataset.theme);});
  $('settingsLanguage')?.addEventListener('change',event=>{applyInterfaceLanguage(event.target.value);renderVisualThemePalette();updateVisualThemeControls();});
  $('learningRefresh')?.addEventListener('click',()=>refreshLearningMonitor(true));
  document.addEventListener('keydown',event=>{if(event.key==='Escape'&&$('settingsPanel')?.classList.contains('open'))closeDisplaySettings(true);});
  window.addEventListener('resize',()=>{if(!$('settingsPanel')?.classList.contains('open'))return;cancelAnimationFrame(settingsPositionFrame);settingsPositionFrame=requestAnimationFrame(positionDisplaySettings);},{passive:true});
  refreshLearningMonitor();if(learningStatusTimer)clearInterval(learningStatusTimer);learningStatusTimer=setInterval(()=>{if(!document.hidden)refreshLearningMonitor();},30000);
}

function utcOffsetLabel(minutes){
  if(minutes===0) return 'UTC';
  const sign=minutes>=0?'+':'-';
  const abs=Math.abs(minutes), h=String(Math.floor(abs/60)).padStart(2,'0'), m=String(abs%60).padStart(2,'0');
  return `UTC${sign}${h}:${m}`;
}
function chartTimezoneOptions(){
  const out=[{value:'Europe/Paris',label:'Paris'}];
  for(let m=-12*60;m<=14*60;m+=15) out.push({value:'UTC_OFFSET_'+m,label:utcOffsetLabel(m)});
  return out;
}
function chartZoneLabel(value){
  if(value==='Europe/Paris') return 'Paris';
  const m=String(value||'').match(/^UTC_OFFSET_(-?\d+)$/);
  return m?utcOffsetLabel(Number(m[1])):'Paris';
}
function chartZoneOffsetMinutes(ms,zone=chartTimezone){
  const fixed=String(zone||'').match(/^UTC_OFFSET_(-?\d+)$/);
  if(fixed)return Number(fixed[1]);
  const bucket=Math.floor(Number(ms)/3600000),cacheKey=`Europe/Paris|${bucket}`;
  if(chartZoneOffsetCache.has(cacheKey))return chartZoneOffsetCache.get(cacheKey);
  try{
    chartParisPartsFormatter=chartParisPartsFormatter||new Intl.DateTimeFormat('en-CA',{timeZone:'Europe/Paris',year:'numeric',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit',second:'2-digit',hourCycle:'h23'});
    const parts=chartParisPartsFormatter.formatToParts(new Date(ms));
    const values={};parts.forEach(p=>{if(p.type!=='literal')values[p.type]=Number(p.value);});
    const wallAsUtc=Date.UTC(values.year,values.month-1,values.day,values.hour,values.minute,values.second);
    const offset=Math.round((wallAsUtc-Math.floor(ms/1000)*1000)/60000);
    boundedCacheSet(chartZoneOffsetCache,cacheKey,offset,1800);
    return offset;
  }catch(e){return 120;}
}
function chartDisplayMinuteBars(rows){
  return chartDisplayTimedBars(cleanMinuteBars(rows));
}
function chartDisplayTimedBars(rows){
  return (rows||[]).filter(row=>Number.isFinite(Number(row?.timestamp))&&[row?.open,row?.high,row?.low,row?.close].every(v=>Number.isFinite(Number(v))&&Number(v)>0)).map(row=>{
    const sourceTs=Number(row.timestamp), wallTs=sourceTs+chartZoneOffsetMinutes(sourceTs)*60000;
    return {...row,_source_timestamp:sourceTs,timestamp:wallTs,date:new Date(wallTs).toISOString()};
  }).sort((a,b)=>a.timestamp-b.timestamp);
}
function formatChartClock(date=new Date()){
  if(chartTimezone==='Europe/Paris'){
    try{chartClockParisFormatter=chartClockParisFormatter||new Intl.DateTimeFormat('en-GB',{timeZone:'Europe/Paris',hour:'2-digit',minute:'2-digit',second:'2-digit',hour12:false});return chartClockParisFormatter.format(date);}
    catch(e){}
  }
  const m=String(chartTimezone||'').match(/^UTC_OFFSET_(-?\d+)$/);
  const offset=m?Number(m[1]):120;
  const d=new Date(date.getTime()+offset*60000);
  return [d.getUTCHours(),d.getUTCMinutes(),d.getUTCSeconds()].map(x=>String(x).padStart(2,'0')).join(':');
}
function updateChartClock(){
  const t=$('chartClockTime'), z=$('chartClockZone');
  const nextTime=formatChartClock(new Date()),nextZone=chartZoneLabel(chartTimezone);
  if(t&&t.textContent!==nextTime) t.textContent=nextTime;
  if(z&&z.textContent!==nextZone) z.textContent=nextZone;
}
function initChartClock(){
  const sel=$('chartTimezone'), btn=$('chartClockBtn');
  if(!sel||!btn||sel.dataset.ready==='1') return;
  sel.innerHTML=chartTimezoneOptions().map(o=>`<option value="${esc(o.value)}">${esc(o.label)}</option>`).join('');
  sel.value=chartTimezone;
  sel.dataset.ready='1';
  btn.onclick=()=>{ sel.classList.toggle('open'); if(sel.classList.contains('open')) sel.focus(); };
  sel.onchange=()=>{
    chartTimezone=sel.value||'Europe/Paris'; updateChartClock(); sel.classList.remove('open');
    stopChartArchiveRequests();
    chartManualViewport=null;
    if(selected){chartRenderedLabel='';renderChart(selected,{preserveViewport:false});}
  };
  sel.onblur=()=>setTimeout(()=>sel.classList.remove('open'),180);
  updateChartClock();
  if(chartClockTimer) clearInterval(chartClockTimer);
  chartClockTimer=setInterval(updateChartClock,1000);
}

function todayISO(){return new Date().toISOString().slice(0,10);} 
function addDaysISO(days){const d=new Date();d.setDate(d.getDate()+days);return d.toISOString().slice(0,10);} 
function fmtUSD(x){ if(x===null||x===undefined||Number.isNaN(Number(x))) return '—'; const v=Number(x); return (v<0?'-':'')+'$'+Math.abs(v).toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2}); }
function initReturnConfig(){
  const rs=$('returnStart'), re=$('returnEnd');
  if(rs && !rs.value) rs.value='2015-01-01';
  if(re && !re.value) re.value=todayISO();
  const body=$('returnConfigRows'); if(!body || body.dataset.ready==='1') return;
  const opts=['<option value="">—</option>',...(tickers||[]).slice().sort((a,b)=>String(a.label).localeCompare(String(b.label),undefined,{numeric:true,sensitivity:'base'})).map(t=>`<option value="${esc(t.label)}">${esc(t.label)} · ${esc(t.name)}</option>`)].join('');
  body.innerHTML=Array.from({length:15},(_,i)=>`<tr><td><select class="retTicker">${opts}</select></td><td><input class="retAlloc" type="number" min="0" max="1000000000" step="1000" placeholder="$ allocated"></td></tr>`).join('');
  body.dataset.ready='1';
}
function selectedReturnPositions(){
  const rows=[...document.querySelectorAll('#returnConfigRows tr')];
  const positions=[];
  for(const tr of rows){ const label=tr.querySelector('.retTicker')?.value||''; const allocated=Number(tr.querySelector('.retAlloc')?.value||0); if(label){ positions.push({label,allocated}); } }
  return positions.slice(0,15);
}
async function runReturnPrevision(){
  const btn=$('returnRunBtn'), status=$('returnStatus'), panel=$('returnPanel');
  const payload={start:$('returnStart')?.value,end:$('returnEnd')?.value,positions:selectedReturnPositions()};
  if(!payload.positions.length){ panel.innerHTML='<div class="empty">Select at least one ticker and enter allocated funds.</div>'; return; }
  if(btn) btn.disabled=true; if(status) status.textContent='Running'; panel.innerHTML='<div class="empty">Fetching validated multi-source market history and calculating daily returns…</div>';
  try{ const out=await api('/api/return-prevision',{method:'POST',body:JSON.stringify(payload)}); renderReturnPrevision(out); }
  catch(e){ panel.innerHTML='<div class="empty">Return prevision failed: '+esc(e.message||e)+'</div>'; }
  finally{ if(btn) btn.disabled=false; if(status) status.textContent='Idle'; }
}
function renderReturnPrevision(out){
  const panel=$('returnPanel'), sub=$('returnSub');
  if(!out || !out.ok){ panel.innerHTML=`<div class="empty">${esc(out?.error||'Return prevision failed.')}</div>`; return; }
  const pos=out.positions||[], rows=out.rows||[], totals=out.totals||{};
  if(sub) sub.textContent=`${esc(out.start)} → ${esc(out.end)} · ${rows.length} validated market close date(s)`;
  const head='<tr><th>Dates</th>'+pos.map(p=>`<th><div class="returnHeadCell"><b>${esc(p.label)}</b><span>${esc(fmtUSD(p.allocated))}</span></div></th>`).join('')+'<th>Total earning</th></tr>';
  const body=rows.map(r=>'<tr><td>'+esc(r.date)+'</td>'+pos.map(p=>{const c=r.values?.[p.label]; return '<td class="'+(c?.return>=0?'pos':'neg')+'">'+(c?esc(fmtUSD(c.return))+'<div class="name">'+esc(fmtPct(c.pct))+'</div>':'—')+'</td>';}).join('')+'<td></td></tr>').join('');
  const total='<tr class="totalRow"><td>Total return</td>'+pos.map(p=>`<td class="${Number(totals[p.label]||0)>=0?'pos':'neg'}">${esc(fmtUSD(totals[p.label]||0))}</td>`).join('')+`<td class="${Number(out.total_earning||0)>=0?'pos':'neg'}">${esc(fmtUSD(out.total_earning||0))}</td></tr>`;
  const err=out.errors&&Object.keys(out.errors).length?'<div class="returnStatus" style="padding:10px 14px">Skipped/missing data: '+esc(Object.entries(out.errors).map(([k,v])=>k+': '+v).join(' · '))+'</div>':'';
  panel.innerHTML=`<table class="returnTable"><thead>${head}</thead><tbody>${body}${total}</tbody></table>${err}`;
}
async function loadTickers(){ tickers=(await api('/api/tickers')).tickers||[]; renderTickers(); setAll(true); initReturnConfig(); $('mCoverage').textContent='0/'+tickers.length; }
function renderTickers(){
  const q=($('tickerSearch').value||'').toLowerCase();
  const list=[...(tickers||[])].filter(t=>(t.label+t.name).toLowerCase().includes(q)).sort((a,b)=>String(a.label||'').localeCompare(String(b.label||''),undefined,{numeric:true,sensitivity:'base'}));
  $('tickerList').innerHTML=list.map(t=>{
    const lab=String(t.label||'');
    return `<label class="ticker"><input type="checkbox" value="${esc(lab)}" ${tickerSelection.has(lab)?'checked':''}><div><b>${esc(lab)}</b><span>${esc(t.name)}</span></div></label>`;
  }).join('');
  document.querySelectorAll('.ticker input').forEach(x=>x.onchange=()=>{ if(x.checked) tickerSelection.add(x.value); else tickerSelection.delete(x.value); });
}
function checkedTickers(){ return [...tickerSelection]; }
function setAll(on){ tickerSelection=on?new Set((tickers||[]).map(t=>String(t.label||'')).filter(Boolean)):new Set(); renderTickers(); }
function mega(){ const m=['AAPL','MSFT','NVDA','AMZN','GOOGL','GOOG','META','TSLA','AVGO','AMD','MPWR','VGT','SMH']; tickerSelection=new Set(m); renderTickers(); }
function liveRows(){
  return ((allResults.length?allResults:results)||[]).filter(r=>r&&!r.error);
}
function livePriceItems(){
  const rows=liveRows();
  const selectedLabel=String(selected?.label||'').toUpperCase();
  const priorityLabels=new Set(['SPY','SMH','VGT','IBIT']);
  return rows.map((r,index)=>{
    const current=Number(r.last??r.quote_price??r.price),dayAbs=Number(r.live_change_abs??r.day_change??r.change_day);
    const inferredPrevious=Number.isFinite(current)&&Number.isFinite(dayAbs)?current-dayAbs:NaN;
    const label=String(r.label||'').toUpperCase();
    let livePriority=Math.max(0,Number(r.confidence||0))*100+Math.abs(Number(chg(r))||0)*1000+(rows.length-index);
    if(priorityLabels.has(label))livePriority+=200000;
    if(label===selectedLabel)livePriority+=1000000;
    return {
      label:r.label,
      tv_symbol:r.tv_symbol,
      tv_exchange:r.tv_exchange,
      original_symbol:r.original_symbol,
      original_exchange:r.original_exchange,
      symbol:r.symbol,
      exchange:r.exchange,
      currency:r.currency??r.profile?.currency,
      previous_close:r.previous_close??r.prev_close??(Number.isFinite(inferredPrevious)&&inferredPrevious>0?inferredPrevious:null),
      last:current,
      last_date:r.last_date,
      last_updated_at:r.last_updated_at??r.updated_at,
      live_priority:livePriority,
    };
  }).filter(x=>x.label).sort((a,b)=>b.live_priority-a.live_priority);
}
function minuteKey(value){
  const ms=Number(value?.timestamp ?? parseDateMs(value?.date));
  return Number.isFinite(ms)&&ms>0?Math.floor(ms/60000)*60000:NaN;
}
function cleanMinuteBars(rows,maxBars=12000){
  const map=new Map();
  for(const raw of (rows||[])){
    const ts=minuteKey(raw), o=Number(raw?.open), h=Number(raw?.high), l=Number(raw?.low), c=Number(raw?.close);
    if(!Number.isFinite(ts)||![o,h,l,c].every(v=>Number.isFinite(v)&&v>0)) continue;
    const high=Math.max(o,h,l,c), low=Math.min(o,h,l,c);
    if(low<=0||high/low>1.60) continue;
    map.set(ts,{...raw,date:new Date(ts).toISOString(),timestamp:ts,open:o,high,low,close:c,volume:Math.max(0,Number(raw?.volume||0))});
  }
  const out=[...map.values()].sort((a,b)=>a.timestamp-b.timestamp);
  const safe=[];
  for(const row of out){
    const prev=safe.at(-1)?.close;
    if(Number.isFinite(prev) && (row.close/prev<.45 || row.close/prev>2.20)) continue;
    safe.push(row);
  }
  const limit=Number(maxBars);return Number.isFinite(limit)&&limit>0?safe.slice(-Math.max(1,Math.floor(limit))):safe;
}
function mergeMinuteBars(serverRows,localRows){
  const map=new Map(cleanMinuteBars(serverRows).map(x=>[x.timestamp,x]));
  for(const local of cleanMinuteBars(localRows)){
    const existing=map.get(local.timestamp);
    if(local._live || !existing){
      map.set(local.timestamp,existing?{...existing,...local,open:existing.open,high:Math.max(existing.high,local.high),low:Math.min(existing.low,local.low),close:local.close}:local);
    }
  }
  return cleanMinuteBars([...map.values()]);
}
function clientQuoteTimestamp(q){
  let ts=Number(q?.updated_at||q?.server_received_at||0);
  if(Number.isFinite(ts)&&ts>0&&ts<1000000000000)ts*=1000;
  return Number.isFinite(ts)&&ts>0?ts:0;
}
function clientQuoteQuality(q){
  if(q?.streaming&&!q?.delayed)return 5;
  if(['alpaca_rest','finnhub_rest'].includes(String(q?.provider_id||'')))return q?.delayed?2.7:4;
  if(q?._from_intraday)return q?.market_open?(q?.delayed?2.8:3.7):1.5;
  if(q?.real_time_hint&&!q?.delayed)return 3.2;
  if(q?.delayed)return 1;
  return 2;
}
function shouldAcceptClientQuote(label,incoming){
  const existing=lastLiveQuoteByLabel.get(String(label||'').toUpperCase());
  if(!existing)return true;
  const now=Date.now(),existingReceived=Number(existing.client_received_at||0),existingFresh=existingReceived>0&&now-existingReceived<30000;
  if(existingFresh&&existing.streaming&&!existing.delayed&&!incoming?.streaming)return false;
  const incomingQuality=clientQuoteQuality(incoming),existingQuality=clientQuoteQuality(existing),incomingTs=clientQuoteTimestamp(incoming),existingTs=clientQuoteTimestamp(existing);
  if(existingFresh&&incomingQuality+.2<existingQuality)return false;
  if(incomingTs&&existingTs&&incomingTs<existingTs-120000&&incomingQuality<=existingQuality)return false;
  return true;
}
function intradayQuoteFromBars(r,payload,bars,meta){
  const latest=(bars||[]).at(-1),price=Number(latest?.close),updatedAt=Number(latest?.timestamp??parseDateMs(latest?.date)),now=Date.now();
  if(!r||!Number.isFinite(price)||price<=0||!Number.isFinite(updatedAt)||updatedAt<=0)return null;
  const previous=Number(r.previous_close??r.prev_close),changeAbs=Number.isFinite(previous)&&previous>0?price-previous:NaN,changePct=Number.isFinite(changeAbs)&&previous>0?(price/previous-1)*100:NaN,ageMs=Math.max(0,now-updatedAt),delayed=!!meta?.delayed;
  return {
    price,
    change_abs:Number.isFinite(changeAbs)?changeAbs:null,
    change_pct:Number.isFinite(changePct)?changePct:null,
    currency:meta?.currency||r.currency||r.profile?.currency,
    source_symbol:meta?.symbol||r.symbol||r.original_symbol||r.label,
    source:meta?.source||payload?.source||'validated intraday candle',
    provider:meta?.source||payload?.source||'Intraday market feed',
    provider_id:'intraday',
    feed:meta?.interval||'1m',
    update_mode:'latest validated minute close',
    market_open:!!meta?.market_open,
    delayed,
    delay_minutes:Math.round(ageMs/600)/100,
    age_ms:ageMs,
    transport_latency_ms:0,
    server_received_at:now,
    real_time_hint:!!meta?.market_open&&!delayed&&ageMs<120000,
    updated_at:updatedAt,
    streaming:false,
    _from_intraday:true,
  };
}
function boundedCacheSet(cache,key,value,maxEntries=18){
  if(cache.has(key))cache.delete(key);cache.set(key,value);
  while(cache.size>maxEntries)cache.delete(cache.keys().next().value);
}
function intradayUrl(r,spec={interval:'1m',range:'5d'}){
  const q=new URLSearchParams({label:String(r?.label||''),symbol:String(r?.symbol||r?.original_symbol||''),exchange:String(r?.exchange||r?.original_exchange||''),tv_symbol:String(r?.tv_symbol||''),tv_exchange:String(r?.tv_exchange||''),interval:String(spec.interval||'1m'),range:String(spec.range||'5d')});
  return '/api/intraday?'+q.toString();
}
function chartDisplayMsToSourceMs(value){
  const displayMs=Number(value);if(!Number.isFinite(displayMs))return NaN;
  let sourceMs=displayMs;
  for(let index=0;index<3;index++)sourceMs=displayMs-chartZoneOffsetMinutes(sourceMs)*60000;
  return sourceMs;
}
function chartSourceRangeFromDisplay(range){
  const first=chartDisplayMsToSourceMs(parseChartAxisMs(range?.[0])),last=chartDisplayMsToSourceMs(parseChartAxisMs(range?.[1]));
  return Number.isFinite(first)&&Number.isFinite(last)&&first!==last?[Math.min(first,last),Math.max(first,last)]:null;
}
function intradayArchiveUrl(r,start,end){
  const q=new URLSearchParams({label:String(r?.label||''),symbol:String(r?.symbol||r?.original_symbol||''),exchange:String(r?.exchange||r?.original_exchange||''),tv_symbol:String(r?.tv_symbol||''),tv_exchange:String(r?.tv_exchange||''),start:String(Math.floor(Number(start))),end:String(Math.floor(Number(end)))});
  return '/api/intraday-archive?'+q.toString();
}
function chartArchiveCacheKey(r,start,end){return `${String(r?.label||'').toUpperCase()}|1m|${Math.floor(Number(start))}|${Math.floor(Number(end))}`;}
function chartArchiveRecentCoverage(r){
  const rows=cleanMinuteBars(r?._intraday_rows||[],0);if(!rows.length)return null;
  return [Number(rows[0].timestamp),Number(rows.at(-1).timestamp)+60000];
}
function chartArchivePagesForRange(r,displayRange,now=Date.now()){
  if(chartTimeframeKey!=='1m'||!r)return [];
  const sourceRange=chartSourceRangeFromDisplay(displayRange);if(!sourceRange)return [];
  const span=sourceRange[1]-sourceRange[0];if(span<=0||span>CHART_ARCHIVE_EXACT_MAX_SPAN_MS)return [];
  const earliest=Number(now)-CHART_ARCHIVE_LOOKBACK_MS,latest=Number(now)+60000,low=Math.max(earliest,sourceRange[0]),high=Math.min(latest,sourceRange[1]);
  if(high<=low)return [];
  const first=Math.floor((low-CHART_ARCHIVE_PAGE_MS)/CHART_ARCHIVE_PAGE_MS)*CHART_ARCHIVE_PAGE_MS,last=Math.floor((high+CHART_ARCHIVE_PAGE_MS-1)/CHART_ARCHIVE_PAGE_MS)*CHART_ARCHIVE_PAGE_MS,center=(low+high)/2,recent=chartArchiveRecentCoverage(r),pages=[];
  for(let start=first;start<=last;start+=CHART_ARCHIVE_PAGE_MS){
    const pageStart=Math.max(earliest,start),pageEnd=Math.min(latest,start+CHART_ARCHIVE_PAGE_MS);
    if(pageEnd<=pageStart)continue;
    if(recent&&pageStart>=recent[0]-60000&&pageEnd<=recent[1]+60000)continue;
    const key=chartArchiveCacheKey(r,pageStart,pageEnd),cached=chartArchivePageCache.get(key);
    if(cached&&Number(cached.expiresAt||0)>Date.now())continue;
    pages.push({key,start:pageStart,end:pageEnd,distance:Math.abs((pageStart+pageEnd)/2-center)});
  }
  return pages.sort((a,b)=>a.distance-b.distance||a.start-b.start).slice(0,CHART_ARCHIVE_MAX_REQUEST_PAGES);
}
function chartArchiveIntervals(rows){
  const ordered=(rows||[]).filter(x=>Number.isFinite(Number(x?.start))&&Number.isFinite(Number(x?.end))&&Number(x.end)>Number(x.start)).map(x=>[Number(x.start),Number(x.end)]).sort((a,b)=>a[0]-b[0]),merged=[];
  for(const interval of ordered){const previous=merged.at(-1);if(previous&&interval[0]<=previous[1]+60000)previous[1]=Math.max(previous[1],interval[1]);else merged.push([...interval]);}
  return merged;
}
function chartArchiveSnapshot(r,displayRange=null){
  const label=String(r?.label||'').toUpperCase(),sourceRange=chartSourceRangeFromDisplay(displayRange);if(!label||!sourceRange||sourceRange[1]-sourceRange[0]>CHART_ARCHIVE_EXACT_MAX_SPAN_MS)return {rows:[],intervals:[]};
  const low=sourceRange[0]-CHART_ARCHIVE_PAGE_MS,high=sourceRange[1]+CHART_ARCHIVE_PAGE_MS,entries=[];
  for(const entry of chartArchivePageCache.values()){
    if(entry?.label!==label||!entry.ok||!entry.bars?.length||Number(entry.end)<low||Number(entry.start)>high)continue;
    entries.push(entry);
  }
  const intervals=chartArchiveIntervals(entries.map(entry=>({start:Number(entry.coverageStart||entry.start),end:Number(entry.coverageEnd||entry.end)})));
  return {rows:cleanMinuteBars(entries.flatMap(entry=>entry.bars||[]),0),intervals};
}
async function requestIntradayArchivePage(r,page){
  const cached=chartArchivePageCache.get(page.key);
  if(cached&&Number(cached.expiresAt||0)>Date.now()){chartArchivePageCache.delete(page.key);chartArchivePageCache.set(page.key,cached);return {entry:cached,fetched:false};}
  if(chartArchivePending.has(page.key))return await chartArchivePending.get(page.key);
  const label=String(r?.label||'').toUpperCase(),pending=(async()=>{
    try{
      const payload=await api(intradayArchiveUrl(r,page.start,page.end)),bars=cleanMinuteBars(payload?.bars||[],CHART_ARCHIVE_PAGE_MAX_BARS),ok=!!payload?.ok&&bars.length>0;
      const entry={label,start:page.start,end:page.end,coverageStart:Number(payload?.coverage_start||bars[0]?.timestamp||page.start),coverageEnd:Number(payload?.coverage_end||bars.at(-1)?.timestamp||page.end),bars,ok,lodOnly:!!payload?.lod_only,source:String(payload?.source||''),receivedAt:Date.now(),expiresAt:Date.now()+(ok?15*60000:60000)};
      boundedCacheSet(chartArchivePageCache,page.key,entry,CHART_ARCHIVE_CACHE_PAGES);return {entry,fetched:true};
    }catch(error){
      const entry={label,start:page.start,end:page.end,bars:[],ok:false,lodOnly:true,receivedAt:Date.now(),expiresAt:Date.now()+60000};
      boundedCacheSet(chartArchivePageCache,page.key,entry,CHART_ARCHIVE_CACHE_PAGES);return {entry,fetched:true};
    }finally{chartArchivePending.delete(page.key);}
  })();
  chartArchivePending.set(page.key,pending);return await pending;
}
function stopChartArchiveRequests(){
  chartArchiveGeneration++;if(chartArchiveTimer){clearTimeout(chartArchiveTimer);chartArchiveTimer=null;}if(chartArchivePaintTimer){clearTimeout(chartArchivePaintTimer);chartArchivePaintTimer=null;}
}
function queueChartArchivePaint(r,generation,delay=36){
  if(chartArchivePaintTimer)clearTimeout(chartArchivePaintTimer);
  chartArchivePaintTimer=setTimeout(()=>{
    chartArchivePaintTimer=null;
    if(generation!==chartArchiveGeneration||chartTimeframeKey!=='1m'||!selected||String(selected.label||'').toUpperCase()!==String(r?.label||'').toUpperCase())return;
    if(chartPointerActive||chartApplyingRange||chartFullRenderInFlight){queueChartArchivePaint(r,generation,100);return;}
    requestAnimationFrame(()=>paintIntradayTrace(selected,true,false,true));
  },Math.max(0,Number(delay)||0));
}
async function loadChartArchiveForViewport(r,displayRange,generation){
  const pages=chartArchivePagesForRange(r,displayRange);let loaded=0;
  for(const page of pages){
    if(generation!==chartArchiveGeneration||chartTimeframeKey!=='1m'||!selected||String(selected.label||'').toUpperCase()!==String(r?.label||'').toUpperCase())return;
    const result=await requestIntradayArchivePage(r,page);
    if(generation!==chartArchiveGeneration)return;
    if(result?.entry?.ok&&result.fetched){loaded++;if(loaded===1)queueChartArchivePaint(r,generation,0);}
  }
  if(loaded>1)queueChartArchivePaint(r,generation,24);
}
function scheduleChartArchiveForViewport(r,displayRange=null,delay=CHART_ARCHIVE_DEBOUNCE_MS){
  if(chartTimeframeKey!=='1m'||!r)return;
  if(chartArchiveTimer)clearTimeout(chartArchiveTimer);
  const generation=++chartArchiveGeneration,range=displayRange||$('chart')?._fullLayout?.xaxis?.range||null;
  chartArchiveTimer=setTimeout(()=>{chartArchiveTimer=null;loadChartArchiveForViewport(r,range,generation).catch(()=>{});},Math.max(0,Number(delay)||0));
}
function applyIntradayPayload(r,payload){
  const lab=String(r?.label||'').toUpperCase();
  if(!lab||!payload?.ok||!Array.isArray(payload.bars)||!payload.bars.length) return false;
  const previousEntry=intradayCache.get(lab)||{},previous=previousEntry.bars||r._intraday_rows||[];
  const bars=mergeMinuteBars(payload.bars,previous);
  const meta={source:payload.source||'one-minute market feed',symbol:payload.symbol||'',timezone:payload.timezone||'UTC',currency:payload.currency||'',market_open:!!payload.market_open,delayed:!!payload.delayed,updated_at:Number(payload.updated_at||0),interval:payload.interval||'1m',range:previousEntry.meta?.range==='30d'?'30d':payload.range||'30d'};
  boundedCacheSet(intradayCache,lab,{bars,meta,receivedAt:Date.now()},12);
  matchingResultObjects(lab).forEach(x=>{x._intraday_rows=bars;x._intraday_rows_validated=true;x._intraday_meta=meta;});
  const intradayQuote=intradayQuoteFromBars(r,payload,bars,meta);
  if(intradayQuote)applyLivePricePayload({prices:{[lab]:intradayQuote},streaming:liveStreamConnected,stream_status:liveStreamStatus,server_time_ms:Date.now(),transport:'intraday'});
  return true;
}
async function requestIntradayCandles(r,fullHistory=false){
  const lab=String(r?.label||'').toUpperCase();
  if(!lab||intradayInflightLabel===lab) return;
  intradayInflightLabel=lab;
  try{
    const payload=await api(intradayUrl(r,{interval:'1m',range:fullHistory?'30d':'5d'}));
    if(!selected||String(selected.label||'').toUpperCase()!==lab) return;
    const hadTrace=Array.isArray($('chart')?._apexMarketRoleIndices)&&$('chart')._apexMarketRoleIndices.length>0;
    if(applyIntradayPayload(selected,payload)){
      if(hadTrace) paintIntradayTrace(selected,true);
      else renderChart(selected);
    }
  }catch(e){} finally{ if(intradayInflightLabel===lab) intradayInflightLabel=''; }
}
function chartTimeframeSpec(key=chartTimeframeKey){return CHART_TIMEFRAMES.find(x=>x.key===key)||CHART_TIMEFRAMES.find(x=>x.key==='1m');}
function chartHistoryContextSpec(timeframe=chartTimeframeSpec()){
  return timeframe&&(timeframe.kind==='seconds'||(timeframe.kind==='intraday'&&Number(timeframe.bucketMs||0)<3600000))?{interval:'1h',range:'1y'}:null;
}
function adaptiveFetchKey(r,fetchSpec){return fetchSpec?`${String(r?.label||'').toUpperCase()}|${fetchSpec.interval}|${fetchSpec.range}`:'';}
function adaptiveSeriesKey(r,key=chartTimeframeKey){
  const fetchSpec=chartTimeframeSpec(key)?.fetch;
  return adaptiveFetchKey(r,fetchSpec);
}
function activateAdaptiveSeries(r,key=chartTimeframeKey){
  const cacheKey=adaptiveSeriesKey(r,key), cached=cacheKey?adaptiveSeriesCache.get(cacheKey):null;
  if(!cached||!r)return false;
  r._adaptive_rows=cached.bars;r._adaptive_meta=cached.meta;r._adaptive_timeframe=key;return true;
}
async function requestAdaptiveFetch(r,fetchSpec){
  const cacheKey=adaptiveFetchKey(r,fetchSpec);
  if(!cacheKey)return false;
  if(adaptiveSeriesCache.has(cacheKey))return false;
  if(adaptiveSeriesPending.has(cacheKey))return await adaptiveSeriesPending.get(cacheKey);
  const pending=(async()=>{
    try{
      const payload=await api(intradayUrl(r,fetchSpec)),bars=cleanMinuteBars(payload?.bars||[]);
      if(!payload?.ok||!bars.length)return false;
      const meta={source:payload.source||'adaptive chart feed',symbol:payload.symbol||'',timezone:payload.timezone||'UTC',currency:payload.currency||'',delayed:!!payload.delayed,updated_at:Number(payload.updated_at||0),interval:payload.interval||fetchSpec.interval,range:payload.range||fetchSpec.range};
      boundedCacheSet(adaptiveSeriesCache,cacheKey,{bars,meta,receivedAt:Date.now()},18);
      return true;
    }catch(e){return false;}
    finally{adaptiveSeriesPending.delete(cacheKey);}
  })();
  adaptiveSeriesPending.set(cacheKey,pending);
  return await pending;
}
async function requestChartTimeframeSeries(r,key=chartTimeframeKey){
  const timeframe=chartTimeframeSpec(key),fetchSpec=timeframe?.fetch,contextSpec=chartHistoryContextSpec(timeframe),lab=String(r?.label||'').toUpperCase(),requestRevision=chartViewportRevision;
  if(!fetchSpec||!lab)return;
  const requests=[];
  if(fetchSpec.interval==='1m'&&fetchSpec.range==='30d'){
    const cached=intradayCache.get(lab);
    if(cached?.meta?.range!=='30d')requestIntradayCandles(r,true);
  }else requests.push(requestAdaptiveFetch(r,fetchSpec));
  if(contextSpec&&(contextSpec.interval!==fetchSpec.interval||contextSpec.range!==fetchSpec.range))requests.push(requestAdaptiveFetch(r,contextSpec));
  const loaded=(await Promise.all(requests)).some(Boolean);
  if(!selected||String(selected.label||'').toUpperCase()!==lab||chartTimeframeKey!==key)return;
  activateAdaptiveSeries(selected,key);
  if(loaded)await Promise.resolve(renderChart(selected,{preserveViewport:chartViewportRevision!==requestRevision}));
}
function startIntradayForSelected(r){
  if(intradayTimer){clearInterval(intradayTimer);intradayTimer=null;}
  stopChartArchiveRequests();
  if(!r)return;
  const lab=String(r.label||'').toUpperCase(), cached=intradayCache.get(lab);
  if(cached){r._intraday_rows=cached.bars;r._intraday_rows_validated=true;r._intraday_meta=cached.meta;}
  requestIntradayCandles(r,true);
  intradayTimer=setInterval(()=>{if(selected)requestIntradayCandles(selected,false);},INTRADAY_REFRESH_MS);
}
function stopIntraday(){
  if(intradayTimer){clearInterval(intradayTimer);intradayTimer=null;}
  stopChartArchiveRequests();
  intradayInflightLabel='';
}
function ingestLiveMinuteQuote(r,q){
  const price=Number(q?.price), now=Date.now();
  if(!r||!Number.isFinite(price)||price<=0||!isLiveQuotePlausibleForChart(r,price)) return false;
  if(q?.market_open!==true && r?._intraday_meta?.market_open!==true) return false;
  let rawTs=Number(q?.updated_at||now);
  if(Number.isFinite(rawTs)&&rawTs>0&&rawTs<1000000000000)rawTs*=1000;
  if(!Number.isFinite(rawTs)||rawTs<now-6*3600000||rawTs>now+120000) rawTs=now;
  let ticks=Array.isArray(r._live_ticks)?r._live_ticks:[];
  const cutoff=now-24*3600000;
  if(ticks.length&&Number(ticks[0]?.timestamp)<cutoff){
    let lo=0,hi=ticks.length;while(lo<hi){const mid=(lo+hi)>>1;if(Number(ticks[mid]?.timestamp)<cutoff)lo=mid+1;else hi=mid;}ticks=ticks.slice(lo);
  }
  const previousTick=ticks.at(-1);
  const streamSeq=Number(q?._stream_seq||0);
  if(previousTick&&Number(previousTick.timestamp)===rawTs&&Number(previousTick.price)===price&&Number(previousTick._stream_seq||0)===streamSeq)return false;
  ticks.push({timestamp:rawTs,date:new Date(rawTs).toISOString(),price,_live:true,_stream_seq:streamSeq});
  if(ticks.length>40000)ticks=ticks.slice(-40000);
  r._live_ticks=ticks;
  let providerBarTs=Number(q?.bar_start);
  if(Number.isFinite(providerBarTs)&&providerBarTs>0&&providerBarTs<1000000000000)providerBarTs*=1000;
  const ts=Number.isFinite(providerBarTs)&&providerBarTs>=now-6*3600000&&providerBarTs<=now+120000?Math.floor(providerBarTs/60000)*60000:Math.floor(rawTs/60000)*60000;
  const qOpen=Number(q?.open),qHigh=Number(q?.high),qLow=Number(q?.low),qClose=Number(q?.close??price);
  const close=Number.isFinite(qClose)&&qClose>0?qClose:price;
  const suppliedOpen=Number.isFinite(qOpen)&&qOpen>0?qOpen:NaN,suppliedHigh=Number.isFinite(qHigh)&&qHigh>0?qHigh:close,suppliedLow=Number.isFinite(qLow)&&qLow>0?qLow:close;
  const barVolume=Number(q?.bar_volume),tradeVolume=Math.max(0,Number(q?.trade_volume||0));
  const rows=r._intraday_rows_validated&&Array.isArray(r._intraday_rows)?r._intraday_rows:cleanMinuteBars(r._intraday_rows||[]);
  const last=rows.at(-1);
  let idx=last?.timestamp===ts?rows.length-1:rows.findIndex(x=>x.timestamp===ts);
  if(last && ts<last.timestamp-60000) return false;
  if(idx<0){
    const open=Number.isFinite(suppliedOpen)?suppliedOpen:Number(last?.close||r._live_base_last||close);
    rows.push({date:new Date(ts).toISOString(),timestamp:ts,open,high:Math.max(open,suppliedHigh,close),low:Math.min(open,suppliedLow,close),close,volume:Number.isFinite(barVolume)?Math.max(0,barVolume):tradeVolume,_live:true,_quote_updated_at:rawTs});
    idx=rows.length-1;
  }else{
    const row=rows[idx];
    const volume=Number.isFinite(barVolume)?Math.max(Number(row.volume||0),Math.max(0,barVolume)):Number(row.volume||0)+tradeVolume;
    rows[idx]={...row,open:row._live&&Number.isFinite(suppliedOpen)?suppliedOpen:Number(row.open),high:Math.max(Number(row.high),suppliedHigh,close),low:Math.min(Number(row.low),suppliedLow,close),close,volume,_live:true,_quote_updated_at:rawTs};
  }
  const validateFrom=Math.max(0,idx-1),clean=rows.slice(0,validateFrom).concat(cleanMinuteBars(rows.slice(validateFrom))).slice(-12000);
  r._intraday_rows=clean;r._intraday_rows_validated=true;
  const lab=String(r.label||'').toUpperCase(), cached=intradayCache.get(lab)||{};
  boundedCacheSet(intradayCache,lab,{...cached,bars:clean,meta:{...(cached.meta||r._intraday_meta||{}),market_open:true,updated_at:rawTs},receivedAt:now},12);
  matchingResultObjects(lab).forEach(x=>{x._intraday_rows=clean;x._intraday_rows_validated=true;x._intraday_meta=intradayCache.get(lab).meta;x._live_ticks=r._live_ticks;});
  return true;
}
function ensureForecastBase(r){
  if(!r || r._liveBaseReady) return;
  const base=Number(r.last ?? r.quote_price ?? r.price ?? r.close ?? priceVal(r));
  r._live_base_last=Number.isFinite(base)&&base>0?base:NaN;
  r._live_base_change_horizon_pct=chg(r);
  r._live_base_forecast=(r.forecast||[]).map(x=>({
    ...x,
    _base_price:Number(x.price),
    _base_low:Number(x.low ?? x.price),
    _base_high:Number(x.high ?? x.price),
    _base_pct:Number(x.pct),
  }));
  r._liveBaseReady=true;
}
function restoreBaseForecast(r){
  if(!r || !r._liveBaseReady) return;
  r.forecast=(r._live_base_forecast||[]).map(x=>({
    ...x,
    price:Number(x._base_price),
    low:Number(x._base_low),
    high:Number(x._base_high),
    pct:Number(x._base_pct),
  }));
}
function liveReferencePrice(r){
  const refs=[];
  const base=Number(r?._live_base_last);
  if(Number.isFinite(base)&&base>0) refs.push(base);
  const h=(r?.history||[]).map(d=>Number(d.close)).filter(v=>Number.isFinite(v)&&v>0).slice(-8);
  refs.push(...h);
  const last=Number(r?.last ?? r?.price ?? r?.close);
  if(Number.isFinite(last)&&last>0) refs.push(last);
  return refs.length?median(refs):NaN;
}
function isLiveQuotePlausibleForChart(r,live){
  const ref=liveReferencePrice(r);
  if(!Number.isFinite(ref)||ref<=0) return true;
  const ratio=Number(live)/ref;
  if(!Number.isFinite(ratio)||ratio<=0) return false;
  const inst=inferInstrument(r);
  const maxRatio=inst==='Index'?1.28:1.42;
  const minRatio=inst==='Index'?0.72:0.58;
  return ratio>=minRatio && ratio<=maxRatio;
}
function rebaseForecastToLive(r,q){
  const live=Number(q?.price);
  if(!r || !Number.isFinite(live) || live<=0) return false;
  ensureForecastBase(r);
  const base=Number(r._live_base_last);
  const chartSafe=Number.isFinite(base)&&base>0 && isLiveQuotePlausibleForChart(r,live);
  r.live_price=live;
  r.quote_price=live;
  r.last=live;
  r.live_chart_safe=chartSafe;
  r.live_source=q.source||'TradingView screener';
  r.live_provider=q.provider||q.source||'TradingView';
  r.live_provider_id=q.provider_id||'http';
  r.live_feed=q.feed||'';
  r.live_streaming=!!q.streaming;
  r.live_delayed=!!q.delayed;
  r.live_delay_minutes=q.delay_minutes;
  r.live_age_ms=Number(q.age_ms||0);
  r.live_transport_latency_ms=Number(q.transport_latency_ms||0);
  r.live_latency_ms=Number(q.end_to_end_latency_ms??q.age_ms??0);
  r.live_server_received_at=Number(q.server_received_at||0);
  r.live_real_time_hint=!!q.real_time_hint;
  r.live_source_symbol=q.source_symbol||'';
  r.live_update_mode=q.update_mode||'';
  r.live_market_open=!!q.market_open;
  r.live_updated_at=Number(q.updated_at||Date.now());
  r.live_date=new Date(r.live_updated_at).toISOString().slice(0,10);
  const pct=Number(q.change_pct);
  const abs=Number(q.change_abs);
  if(Number.isFinite(pct)){ r.live_change_pct=pct; r.day_change_pct=pct; r.change_day_pct=pct; }
  if(Number.isFinite(abs)){
    r.live_change_abs=abs; r.day_change=abs; r.change_day=abs;
  }else if(Number.isFinite(pct) && pct!==-100){
    const prev=live/(1+pct/100);
    if(Number.isFinite(prev)&&prev>0){ r.live_change_abs=live-prev; r.day_change=live-prev; r.change_day=live-prev; }
  }
  if(!chartSafe){
    restoreBaseForecast(r);
    return true;
  }
  const ratio=live/base;
  r.forecast=(r._live_base_forecast||[]).map(x=>{
    const bp=Number(x._base_price), bl=Number(x._base_low), bh=Number(x._base_high);
    const price=Number.isFinite(bp)&&bp>0?bp*ratio:live;
    const low=Number.isFinite(bl)&&bl>0?bl*ratio:price;
    const high=Number.isFinite(bh)&&bh>0?bh*ratio:price;
    const pctBase=Number(x._base_pct);
    return {...x,price,low,high,pct:Number.isFinite(pctBase)?pctBase:((price/live)-1)*100};
  });
  return true;
}
function matchingResultObjects(label){
  const lab=String(label||'').toUpperCase();
  const seen=new Set(), out=[];
  [selected,...results,...allResults].forEach(r=>{
    if(r && String(r.label||'').toUpperCase()===lab && !seen.has(r)){ seen.add(r); out.push(r); }
  });
  return out;
}
function setTextIfChanged(el,value){
  if(!el)return;
  const next=String(value??'');
  if(el.textContent!==next)el.textContent=next;
}
function runUiIdle(task,timeout=400){
  if(typeof window.requestIdleCallback==='function')return window.requestIdleCallback(task,{timeout});
  return setTimeout(task,0);
}
function liveStateText(r){return r?.live_delayed?'delayed':(r?.live_streaming?'live':(r?.live_real_time_hint?'low latency':'delayed'));}
function resultForLiveLabel(label){
  const lab=String(label||'').toUpperCase();
  if(selected&&String(selected.label||'').toUpperCase()===lab)return selected;
  return results.find(r=>String(r?.label||'').toUpperCase()===lab)||allResults.find(r=>String(r?.label||'').toUpperCase()===lab)||null;
}
function cacheRankRows(){
  rankRowCache.clear();rankDomRowCache.clear();
  document.querySelectorAll('#resultsBody tr[data-rank-label]').forEach(row=>{
    const lab=String(row.dataset.rankLabel||'').toUpperCase();
    if(lab)rankDomRowCache.set(lab,row);
    if(!row.dataset.label)return;
    rankRowCache.set(lab,row);
    row._apexLiveRefs={
      price:row.querySelector('[data-live-price]'),state:row.querySelector('[data-live-state]'),day:row.querySelector('[data-live-day]'),dayPct:row.querySelector('[data-live-day-pct]'),dayAbs:row.querySelector('[data-live-day-abs]'),forecast:row.querySelector('[data-live-forecast]')
    };
    row.onclick=()=>selectTicker(row.dataset.label);
  });
}
function paintRankedLiveRow(r){
  const row=rankRowCache.get(String(r?.label||'').toUpperCase());
  if(!row)return;
  const refs=row._apexLiveRefs||{};
  const p=priceVal(r),da=dayAbsVal(r),dp=dayPctVal(r),dayGap=Number.isFinite(da)?((da>=0?'+':'')+money(da)):'—';
  setTextIfChanged(refs.price,money(p));
  setTextIfChanged(refs.state,r.live_price?liveStateText(r):'');
  setTextIfChanged(refs.dayPct,fmtPct(dp));
  setTextIfChanged(refs.dayAbs,dayGap);
  setTextIfChanged(refs.forecast,money(r.forecast?.at(-1)?.price));
  if(refs.day){const cls='dayCell '+pctClass(dp);if(refs.day.className!==cls)refs.day.className=cls;}
}
function paintSelectedLiveDetails(r){
  const root=$('details'),refs=root?._apexLiveRefs;
  if(!root||!refs||root._apexLiveLabel!==String(r?.label||''))return;
  const f=r.forecast||[],change=chg(r),values=f.flatMap(x=>[Number(x.low||x.price||0),Number(x.high||x.price||0)]).filter(v=>Number.isFinite(v)&&v>0);
  setTextIfChanged(refs.price,money(priceVal(r)));
  setTextIfChanged(refs.horizon,`${fmtPct(change)} · ${r.horizon_days||(f.length||horizon())}D`);
  if(refs.horizon){const cls=pctClass(change);if(refs.horizon.className!==cls)refs.horizon.className=cls;}
  setTextIfChanged(refs.central,fmtPct(change));
  if(refs.central){const cls=pctClass(change);if(refs.central.className!==cls)refs.central.className=cls;}
  setTextIfChanged(refs.range,values.length?`${money(Math.min(...values))} - ${money(Math.max(...values))}`:'—');
  setTextIfChanged(refs.liveSource,r.live_price?`${r.live_provider||r.live_source||'Live quote'} · ${liveStateText(r)} · ${r.live_update_mode||'quote'}`:'');
  refs.forecastPrices.forEach((el,i)=>setTextIfChanged(el,money(f[i]?.price)));
  refs.forecastPcts.forEach((el,i)=>{setTextIfChanged(el,fmtPct(f[i]?.pct));if(el){const cls=pctClass(f[i]?.pct);if(el.className!==cls)el.className=cls;}});
}
function syncDynamicRankOrder(){
  if(chartPointerActive){scheduleDynamicRankOrder();return;}
  const key=$('rankSort')?.value||'symbol';
  if(!['price','day_abs','day_pct'].includes(key))return;
  const body=$('resultsBody');if(!body)return;
  const q=resultSearchQuery(),ordered=rankSortRows(allResults.length?allResults:results).filter(r=>resultMatches(r,q)),fragment=document.createDocumentFragment();
  let count=0;
  ordered.forEach(r=>{const row=rankDomRowCache.get(String(r?.label||'').toUpperCase());if(row){fragment.appendChild(row);count++;}});
  if(count)body.appendChild(fragment);
}
function scheduleDynamicRankOrder(){
  const key=$('rankSort')?.value||'symbol';
  if(dynamicRankOrderTimer||!['price','day_abs','day_pct'].includes(key))return;
  dynamicRankOrderTimer=setTimeout(()=>{dynamicRankOrderTimer=null;runUiIdle(syncDynamicRankOrder,450);},DYNAMIC_RANK_ORDER_MS);
}
function flushLiveDomPaint(){
  liveDomPaintFrame=0;if(chartPointerActive)return;
  const labs=[...pendingLiveLabels];pendingLiveLabels.clear();
  labs.forEach(lab=>{const r=resultForLiveLabel(lab);if(r)paintRankedLiveRow(r);});
  if(selected&&labs.includes(String(selected.label||'').toUpperCase()))paintSelectedLiveDetails(selected);
  scheduleDynamicRankOrder();
}
function scheduleLiveDomPaint(labels){
  for(const lab of labels||[])pendingLiveLabels.add(String(lab||'').toUpperCase());
  if(chartPointerActive||liveDomPaintFrame)return;
  liveDomPaintFrame=requestAnimationFrame(flushLiveDomPaint);
}
function refreshSelectedAnalysisSheets(){
  for(const [lab,ref] of [...selectedAnalysisWindows.entries()]){
    if(!ref || !ref.win || ref.win.closed){ selectedAnalysisWindows.delete(lab); continue; }
    const amount=sheetBound(sheetNum(ref.input?.value,0),0,1000000000);
    ref.body.innerHTML=selectedAnalysisSheetBody(lab,amount);
  }
}
function scheduleSelectedAnalysisSheetsRefresh(){
  if(!selectedAnalysisWindows.size||selectedSheetRefreshTimer)return;
  selectedSheetRefreshTimer=setTimeout(()=>{
    selectedSheetRefreshTimer=null;
    if(chartPointerActive){scheduleSelectedAnalysisSheetsRefresh();return;}
    runUiIdle(()=>{if(chartPointerActive)scheduleSelectedAnalysisSheetsRefresh();else refreshSelectedAnalysisSheets();},600);
  },650);
}
function streamProviderLabel(value){
  return ({auto:'Exchange auto',massive:'Massive',alpaca:'Alpaca',finnhub:'Finnhub',twelvedata:'Twelve Data',coinbase:'Coinbase',http:'HTTP fallback'})[String(value||'').toLowerCase()]||String(value||'Fallback');
}
function renderChartStreamStatus(status=liveStreamStatus){
  const root=$('chartStreamStatus');if(!root)return;
  const signature=JSON.stringify(Object.entries(status||{}).map(([id,row])=>[id,!!row?.configured,!!row?.connected,row?.state,row?.last_event_ms,row?.last_error,row?.symbols,row?.feed,row?.auth_probe?.state,row?.auth_probe?.detail,row?.credential_source,row?.credential_fingerprint]));
  if(signature===lastStreamStatusSignature)return;
  lastStreamStatusSignature=signature;
  const rows=Object.entries(status||{}).map(([id,row])=>{
    const connected=!!row?.connected,configured=!!row?.configured,state=connected?'streaming':(configured?(row?.state||'waiting'):(row?.state||'not configured'));
    const cls=connected?'connected':(configured?'waiting':'offline');
    const error=String(row?.last_error||'').trim(),probe=row?.auth_probe||{},probeText=probe.state?`REST auth ${probe.state}${probe.detail?' · '+probe.detail:''}`:'',credentialText=row?.credential_source?`${row.credential_source}${row.credential_fingerprint?' · fingerprint '+row.credential_fingerprint:''}`:'',title=[state,error,probeText,credentialText].filter(Boolean).join(' · ');
    return `<div class="chartStreamStatusRow"><b>${esc(streamProviderLabel(id))}</b><span class="${cls}" title="${esc(title)}">${esc(state)}</span></div>`;
  });
  rows.push('<div class="chartStreamStatusRow"><b>HTTP fallback</b><span class="waiting">ready</span></div>');
  root.innerHTML=rows.join('');
}
function updateChartLatencyUi(q=null,payload=null){
  if(payload?.stream_status)liveStreamStatus=payload.stream_status;
  renderChartStreamStatus(liveStreamStatus);
  const button=$('chartLatencyBtn'),value=$('chartLatencyValue'),provider=$('chartLatencyProvider');if(!button)return;
  const latency=Number(q?.end_to_end_latency_ms??q?.age_ms),streaming=!!q?.streaming&&!q?.delayed;
  setTextIfChanged(value,Number.isFinite(latency)?`${Math.min(99999,Math.max(0,Math.round(latency)))} ms`:'-- ms');
  setTextIfChanged(provider,q?.provider||q?.source||(liveSseOpen?'SSE':'Fallback'));
  button.classList.toggle('streaming',streaming);
  button.classList.toggle('warn',!streaming&&(liveSseOpen||!!q));
  button.title=q?`${q.provider||q.source||'Market source'} · ${Number.isFinite(latency)?Math.round(latency)+' ms':'latency unavailable'}`:'Live market connection';
}
function initChartLatencyControl(){
  const button=$('chartLatencyBtn'),menu=$('chartLatencyMenu'),provider=$('chartLiveProvider'),cadence=$('chartLiveCadence');
  if(!button||!menu||!provider||!cadence)return;
  provider.value=livePreferredProvider;cadence.value=String(liveStreamCadenceMs);
  button.onclick=e=>{e.stopPropagation();const open=!menu.classList.contains('open');menu.classList.toggle('open',open);button.setAttribute('aria-expanded',String(open));};
  menu.onclick=e=>e.stopPropagation();
  document.addEventListener('click',()=>{menu.classList.remove('open');button.setAttribute('aria-expanded','false');});
  provider.onchange=()=>{
    livePreferredProvider=provider.value;
    try{localStorage.setItem('apex-live-provider',livePreferredProvider);}catch(e){}
    stopLivePrices();startLivePrices();updateChartLatencyUi(lastLiveQuoteByLabel.get(String(selected?.label||'').toUpperCase())||null);
  };
  cadence.onchange=()=>{
    liveStreamCadenceMs=Math.max(100,Math.min(1000,Number(cadence.value)||250));
    try{localStorage.setItem('apex-live-cadence',String(liveStreamCadenceMs));}catch(e){}
    stopLivePrices();startLivePrices();
  };
  updateChartLatencyUi();
}
function applyLivePricePayload(payload){
  const quotes=payload?.prices||{};
  let touched=false, selectedTouched=false, selectedQuote=null;
  const touchedLabels=new Set(),targetsByLabel=new Map();
  [selected,...results,...allResults].forEach(r=>{if(!r)return;const lab=String(r.label||'').toUpperCase(),list=targetsByLabel.get(lab)||[];if(!list.includes(r))list.push(r);targetsByLabel.set(lab,list);});
  const clientReceivedAt=Date.now(),serverTime=Number(payload?.server_time_ms||0);
  Object.entries(quotes).forEach(([lab,rawQuote])=>{
    const q={...(rawQuote||{})},label=String(lab||'').toUpperCase();
    if(!shouldAcceptClientQuote(label,q))return;
    const providerLatency=Math.max(0,Number(q.transport_latency_ms??q.age_ms??0));
    const browserLeg=Number.isFinite(serverTime)&&serverTime>0?clientReceivedAt-serverTime:0;
    q.end_to_end_latency_ms=Math.max(0,Math.min(99999,providerLatency+((browserLeg>=0&&browserLeg<30000)?browserLeg:0)));
    q.client_received_at=clientReceivedAt;
    lastLiveQuoteByLabel.set(label,q);
    (targetsByLabel.get(label)||[]).forEach(r=>{
      if(rebaseForecastToLive(r,q)){
        touched=true;touchedLabels.add(label);
        if(selected && String(selected.label||'').toUpperCase()===label){selectedTouched=true;selectedQuote=q;}
      }
    });
  });
  liveStreamConnected=!!payload?.streaming;
  updateChartLatencyUi(lastLiveQuoteByLabel.get(String(selected?.label||'').toUpperCase())||selectedQuote,payload);
  if(!touched)return;
  const now=performance.now();
  if(now-liveLastPaint<LIVE_PRICE_PAINT_MS) return;
  liveLastPaint=now;
  scheduleLiveDomPaint(touchedLabels);
  if(selectedTouched && selected){
    const fromIntraday=!!selectedQuote?._from_intraday,minuteTouched=selectedQuote&&!fromIntraday?ingestLiveMinuteQuote(selected,selectedQuote):false;
    if(fromIntraday){
      paintForecastTraces(selected);
    }else if(minuteTouched){
      paintIntradayTrace(selected,false);
    }else if(now-liveLastChartPaint>LIVE_CHART_PAINT_MS){
      liveLastChartPaint=now;
      updateLiveChart(selected);
    }
  }
  scheduleSelectedAnalysisSheetsRefresh();
}
async function pollLivePrices(){
  if(liveInflight || !results.length) return;
  const items=livePriceItems();
  if(!items.length) return;
  liveInflight=true;
  try{
    const out=await api('/api/live-prices',{method:'POST',body:JSON.stringify({items,preferred_provider:livePreferredProvider})});
    applyLivePricePayload(out);
  }catch(e){} finally{ liveInflight=false; }
}
function startLiveEventStream(){
  if(liveEventSource||livePreferredProvider==='http'||typeof EventSource==='undefined'||!results.length)return;
  const labels=livePriceItems().map(x=>String(x.label||'')).filter(Boolean);
  if(!labels.length)return;
  const query=new URLSearchParams({labels:labels.join(','),preferred:livePreferredProvider,cadence:String(liveStreamCadenceMs)});
  const source=new EventSource('/api/live-stream?'+query.toString());
  liveEventSource=source;
  source.onopen=()=>{if(liveEventSource!==source)return;liveSseOpen=true;updateChartLatencyUi(lastLiveQuoteByLabel.get(String(selected?.label||'').toUpperCase())||null);};
  source.onmessage=event=>{
    if(liveEventSource!==source)return;
    try{const payload=JSON.parse(event.data);liveSseOpen=true;applyLivePricePayload(payload);}catch(e){}
  };
  source.onerror=()=>{if(liveEventSource!==source)return;liveSseOpen=false;liveStreamConnected=false;updateChartLatencyUi(null);};
}
function scheduleLiveFallbackPoll(delay=null){
  if(liveTimer||!results.length)return;
  const wait=delay??(liveStreamConnected?LIVE_STREAM_HTTP_FALLBACK_MS:LIVE_PRICE_INTERVAL_MS);
  liveTimer=setTimeout(async()=>{
    liveTimer=null;
    await pollLivePrices();
    scheduleLiveFallbackPoll();
  },wait);
}
function startLivePrices(){
  if(liveTimer||liveEventSource||!results.length)return;
  pollLivePrices();
  startLiveEventStream();
  scheduleLiveFallbackPoll(LIVE_PRICE_INTERVAL_MS);
}
function stopLivePrices(){
  if(liveTimer){clearTimeout(liveTimer);liveTimer=null;}
  if(liveEventSource){liveEventSource.close();liveEventSource=null;}
  liveSseOpen=false;liveStreamConnected=false;
  if(liveDomPaintFrame){cancelAnimationFrame(liveDomPaintFrame);liveDomPaintFrame=0;}
  pendingLiveLabels.clear();
  liveInflight=false;
}
function jobResultFingerprint(payload){
  const rows=payload?.results||[];
  return `${payload?.count??rows.length}#${payload?.error||''}#${rows.length}#`+rows.map(r=>`${r.label||''}|${r.error||''}|${r.last_date||''}|${Number(r.last||0).toFixed(6)}|${Number(r.confidence||0).toFixed(3)}|${Number(r.forecast?.at(-1)?.price||0).toFixed(6)}`).join('~');
}
async function run(){
  stopLivePrices();stopIntraday();
  const ticks=checkedTickers();
  activeRunTickerSelection=new Set(ticks.map(label=>String(label||'').toUpperCase()).filter(Boolean));
  const payload={mode:$('mode').value,days:Number($('days').value||10),bars:$('bars').value?Number($('bars').value):null,news:$('news').value==='1',newsLimit:Number($('newsLimit').value||32),tickers:ticks};
  const r=await api('/api/run',{method:'POST',body:JSON.stringify(payload)});
  jobId=r.id;results=[];allResults=[];selected=null;lastJobResultFingerprint='';liveLastPaint=0;liveLastChartPaint=0;liveCandleLastPaint=0;chartRenderedLabel='';chartUserRangeChangedAt=0;chartFollowLive=true;chartManualViewport=null;chartRenderSeq++;deferredIntradayPaint=null;deferredLiveChartUpdate=null;
  $('stateDot').className='dot run';setTextIfChanged($('stateText'),'Running');$('runBtn').disabled=true;$('resultsBody').innerHTML='';rankRowCache.clear();rankDomRowCache.clear();
  $('chart').innerHTML='<div class="empty">Analysis running… results will appear automatically.</div>';$('chartOhlcStrip').innerHTML='';$('chartCrosshair').classList.remove('visible');$('chartLastPrice').classList.remove('visible');$('profilePanel').innerHTML='<div class="empty">Company profile will appear after the first forecast.</div>';setTextIfChanged($('profileSub'),'Waiting for results');
  poll();pollTimer=setInterval(poll,1000);
}
async function stop(){ stopLivePrices(); stopIntraday(); if(jobId) await api('/api/stop',{method:'POST',body:JSON.stringify({id:jobId})}); }
async function poll(){
  if(!jobId)return;
  const s=await api('/api/job?id='+encodeURIComponent(jobId)),p=s.progress||{},progressWidth=(p.percent||0)+'%',progressText=(p.message||'Running')+' · '+(p.percent||0)+'%';
  if($('progressBar').style.width!==progressWidth)$('progressBar').style.width=progressWidth;
  setTextIfChanged($('progressText'),progressText);
  if(s.result){
    const fingerprint=jobResultFingerprint(s.result);
    if(fingerprint!==lastJobResultFingerprint){
      lastJobResultFingerprint=fingerprint;allResults=(s.result.results||[]);results=allResults.filter(r=>!r.error);renderResults(s.result);
      if(results.length&&!selected)selectTicker(results[0].label);
    }
  }
  if(!s.running){
    clearInterval(pollTimer);$('runBtn').disabled=false;$('stateDot').className=s.error?'dot err':'dot';setTextIfChanged($('stateText'),s.error?'Engine issue':'Analysis complete');
    if(results.length)startLivePrices();
    setTimeout(()=>refreshLearningMonitor(true),1500);setTimeout(()=>refreshLearningMonitor(true),7000);
    if(s.error&&!s.result)$('details').innerHTML='<div class="empty">The v15 engine returned an error. Check setup.stats.py and the run engine.log, then restart the dashboard.</div>';
  }
}
function resultSearchQuery(){ return (($('rankSearch')?.value)||'').trim().toLowerCase(); }
function resultMatches(r,q){
  if(!q) return true;
  const hay=[r.label,r.name,r.signal,r.risk,r.tv_symbol,r.tv_exchange,r.original_symbol,r.original_exchange].map(x=>String(x||'').toLowerCase()).join(' ');
  return hay.includes(q);
}
function lastHistClose(r,idxFromEnd=1){
  const h=(r.history||[]).filter(d=>Number.isFinite(Number(d.close))).sort((a,b)=>String(a.date||'').localeCompare(String(b.date||'')));
  const row=h[h.length-idxFromEnd];
  return row?Number(row.close):NaN;
}
function priceVal(r){
  const direct=Number(r.live_price ?? r.quote_price ?? r.last ?? r.price ?? r.close);
  if(Number.isFinite(direct)&&direct>0) return direct;
  const h=lastHistClose(r,1);
  return Number.isFinite(h)?h:NaN;
}
function prevPriceVal(r){
  const p=Number(r.previous_close ?? r.prev_close ?? r.regularMarketPreviousClose);
  if(Number.isFinite(p)&&p>0) return p;
  const h=lastHistClose(r,2);
  return Number.isFinite(h)?h:NaN;
}
function dayAbsVal(r){
  const direct=Number(r.live_change_abs ?? r.day_change ?? r.change_day ?? r.regularMarketChange ?? r.price_change);
  if(Number.isFinite(direct)) return direct;
  const p=priceVal(r), prev=prevPriceVal(r);
  return Number.isFinite(p)&&Number.isFinite(prev)?p-prev:NaN;
}
function dayPctVal(r){
  const direct=Number(r.live_change_pct ?? r.day_change_pct ?? r.change_day_pct ?? r.regularMarketChangePercent ?? r.price_change_pct);
  if(Number.isFinite(direct)) return direct;
  const p=priceVal(r), prev=prevPriceVal(r);
  return Number.isFinite(p)&&Number.isFinite(prev)&&prev!==0?((p/prev)-1)*100:NaN;
}
function sortText(v){return String(v||'').toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g,'');}
function rankSortRows(rows){
  const key=$('rankSort')?.value||'symbol';
  const dir=($('rankOrder')?.value||'asc')==='desc'?-1:1;
  const arr=[...rows];
  arr.sort((a,b)=>{
    if(a.error&&!b.error) return 1;
    if(!a.error&&b.error) return -1;
    let av,bv,numeric=false;
    if(key==='name'){av=sortText(a.name||a.label); bv=sortText(b.name||b.label);}
    else if(key==='symbol'){av=sortText(a.label||a.symbol); bv=sortText(b.label||b.symbol);}
    else if(key==='price'){av=priceVal(a); bv=priceVal(b); numeric=true;}
    else if(key==='day_abs'){av=dayAbsVal(a); bv=dayAbsVal(b); numeric=true;}
    else if(key==='day_pct'){av=dayPctVal(a); bv=dayPctVal(b); numeric=true;}
    if(numeric){
      const af=Number.isFinite(av), bf=Number.isFinite(bv);
      if(!af&&!bf) return String(a.label||'').localeCompare(String(b.label||''));
      if(!af) return 1; if(!bf) return -1;
      return (av-bv)*dir || String(a.label||'').localeCompare(String(b.label||''));
    }
    return String(av).localeCompare(String(bv))*dir;
  });
  return arr;
}
function mergeMissingRows(rows){
  const requested=activeRunTickerSelection;
  const runRows=(rows||[]).filter(r=>requested.has(String(r.label||'').toUpperCase()));
  const map=new Map(runRows.map(r=>[String(r.label||'').toUpperCase(), r]));
  (tickers||[]).filter(t=>requested.has(String(t.label||'').toUpperCase())).forEach(t=>{
    const lab=String(t.label||'').toUpperCase();
    if(lab && !map.has(lab)){
      map.set(lab,{label:lab,name:t.name||lab,original_symbol:t.symbol||lab,original_exchange:t.exchange||'',error:'Not returned by engine',source_attempts:['The engine did not return this selected instrument in the last payload.']});
    }
  });
  return [...map.values()];
}
function renderResults(payload){
  const raw=(payload.results&&payload.results.length?payload.results:(allResults.length?allResults:results))||[];
  const all=mergeMissingRows(raw);
  allResults=all;
  const ok=all.filter(r=>!r.error);
  results=ok;
  const total=Math.max(Number(payload.count||0), all.length, tickers.length);
  $('mCoverage').textContent=ok.length+'/'+total;
  const hh=$('horizonHead'); if(hh) hh.textContent=horizon()+'D';
  if(!ok.length){
    $('mAvg').textContent='No data'; $('mAvg').className='v neu'; $('mBull').textContent='0'; $('mConf').textContent='—'; $('mRisk').textContent='—';
    const q=resultSearchQuery(); const errs=all.filter(r=>r.error&&resultMatches(r,q)); $('tableCount').textContent=q?`${errs.length}/${all.length} rows`:`${errs.length} error rows`;
    $('resultsBody').innerHTML=errs.slice(0,120).map(r=>`<tr class="errRow" data-rank-label="${esc(r.label||'?')}"><td><div class="sym">${esc(r.label||'?')}</div><div class="name">${esc(r.name||'')}</div></td><td class="neg" colspan="6"><b>${esc(r.error||'No data')}</b><div class="name">${esc((r.source_attempts||[]).slice(0,2).join(' · ')||r.hint||'No reliable market history returned.')}</div></td></tr>`).join('');
    $('chartTitle').textContent='No forecast generated'; $('chartSub').textContent='Engine returned 0 usable tickers';
    $('chart').innerHTML='<div class="empty">Aucune donnée exploitable n’a été générée. Vérifie la connexion aux sources de marché et les mappings du ticker.</div>';
    $('details').innerHTML='<div class="empty">Les erreurs détaillées sont listées dans le tableau de droite.</div>';
    $('profilePanel').innerHTML='<div class="empty">No usable profile because no forecast was generated.</div>'; $('profileSub').textContent='No forecast'; cacheRankRows(); return;
  }
  const avg=ok.reduce((a,r)=>a+chg(r),0)/(ok.length||1);
  $('mAvg').textContent=fmtPct(avg); $('mAvg').className='v '+pctClass(avg); $('mBull').textContent=ok.filter(r=>chg(r)>0.12).length;
  $('mConf').textContent=Math.round(ok.reduce((a,r)=>a+Number(r.confidence||0),0)/(ok.length||1));
  const risks={}; ok.forEach(r=>risks[r.risk]=(risks[r.risk]||0)+1); $('mRisk').textContent=Object.entries(risks).sort((a,b)=>b[1]-a[1])[0]?.[0]||'—';
  const displayRows=rankSortRows(all);
  const q=resultSearchQuery(); const shown=displayRows.filter(r=>resultMatches(r,q));
  $('tableCount').textContent=q ? `${shown.length}/${displayRows.length} rows` : displayRows.length+' rows';
  $('resultsBody').innerHTML=shown.map(r=>{
    if(r.error){
      return `<tr class="errRow" data-rank-label="${esc(r.label||'?')}"><td><div class="sym">${esc(r.label||'?')}</div><div class="name">${esc(r.name||'')}</div></td><td class="neg" colspan="6"><b>No reliable chart data</b><div class="name">${esc((r.source_attempts||[]).slice(0,2).join(' · ')||r.error||'TradingView did not return enough history.')}</div></td></tr>`;
    }
    const p=priceVal(r), da=dayAbsVal(r), dp=dayPctVal(r);
    const dayGap=Number.isFinite(da)?((da>=0?'+':'')+money(da)):'—';
    const liveState=liveStateText(r);
    return `<tr data-rank-label="${esc(r.label)}" data-label="${esc(r.label)}" class="${selected&&selected.label===r.label?'active':''}"><td><div class="sym">${esc(r.label)}</div><div class="name">${esc(r.name)}</div></td><td><b data-live-price>${money(p)}</b><div class="name liveState" data-live-state>${r.live_price?esc(liveState):''}</div></td><td data-live-day class="dayCell ${pctClass(dp)}"><b data-live-day-pct>${fmtPct(dp)}</b><div class="name" data-live-day-abs>${dayGap}</div></td><td class="${pctClass(chg(r))}"><b>${fmtPct(chg(r))}</b><div class="name">${money(r.last)} → <span data-live-forecast>${money(r.forecast?.at(-1)?.price)}</span></div></td><td>${badge(r.signal)}</td><td>${Number(r.confidence||0).toFixed(0)}</td><td>${esc(r.risk||'—')}</td></tr>`;
  }).join('') || `<tr><td colspan="7"><div class="empty" style="min-height:120px">No ranked forecast matches “${esc(q)}”.</div></td></tr>`;
  cacheRankRows();
}
function mergeFullHistoryRows(currentRows,historyRows){
  const byDate=new Map();
  for(const row of (historyRows||[])){const date=String(row?.date||'').slice(0,10);if(date)byDate.set(date,row);}
  for(const row of (currentRows||[])){const date=String(row?.date||'').slice(0,10);if(date)byDate.set(date,row);}
  return [...byDate.values()].sort((a,b)=>String(a.date).localeCompare(String(b.date)));
}
function chartAllHistoryRequested(){return chartHistoryRangeKey==='all'||chartTimeframeKey==='all';}
function selectTicker(label){
  selected=results.find(r=>r.label===label); if(!selected)return;
  chartRenderedLabel='';
  chartUserRangeChangedAt=0;
  chartFollowLive=true;
  chartManualViewport=null;
  chartViewportRevision++;
  chartRenderSeq++;
  const cached=fullHistoryCache.get(String(label).toUpperCase());
  if(cached && Array.isArray(cached) && cached.length){
    selected.history=mergeFullHistoryRows(selected.history||[],cached);
    selected.full_history_loaded=true;
  }
  const minuteCached=intradayCache.get(String(label).toUpperCase());
  if(minuteCached){selected._intraday_rows=minuteCached.bars;selected._intraday_rows_validated=true;selected._intraday_meta=minuteCached.meta;}
  activateAdaptiveSeries(selected,chartTimeframeKey);
  renderResults({count:allResults.length||results.length,results:allResults.length?allResults:results});
  renderDetails(selected); renderChart(selected); renderProfile(selected);
  updateLearningSelectedImpact();
  updateChartLatencyUi(lastLiveQuoteByLabel.get(String(label||'').toUpperCase())||null);
  if(chartAllHistoryRequested())requestFullChartHistory(selected);
  startIntradayForSelected(selected);
  requestChartTimeframeSeries(selected,chartTimeframeKey);
  setTimeout(()=>pollLivePrices(),0);
}
async function requestFullChartHistory(r){
  const lab=String(r?.label||'').toUpperCase();
  if(!lab || r?.full_history_loaded || fullHistoryPending.has(lab)) return;
  const requestRevision=chartViewportRevision;
  const cached=fullHistoryCache.get(lab);
  if(cached?.length){r.history=mergeFullHistoryRows(r.history||[],cached);r.full_history_loaded=true;return;}
  fullHistoryPending.add(lab);
  try{
    const payload=await api('/api/full-history?label='+encodeURIComponent(lab));
    const hist=(payload&&payload.history)||[];
    if(Array.isArray(hist) && hist.length){
      const mergedForCache=mergeFullHistoryRows(r.history||[],hist);
      boundedCacheSet(fullHistoryCache,lab,mergedForCache,8);
      const targets=[r, selected, ...results.filter(x=>String(x.label||'').toUpperCase()===lab), ...allResults.filter(x=>String(x.label||'').toUpperCase()===lab)];
      targets.forEach(x=>{if(x){x.history=mergeFullHistoryRows(x.history||[],hist);x.full_history_loaded=true;x.full_history_source=payload.source||payload.source_label||'Validated multi-source OHLCV';}});
      if(selected&&String(selected.label||'').toUpperCase()===lab&&chartAllHistoryRequested())renderChart(selected,{preserveViewport:chartViewportRevision!==requestRevision||!!activeManualChartViewport($('chart'))});
    }
  }catch(e){} finally{ fullHistoryPending.delete(lab); }
}

function val(x){
  if(x===undefined||x===null) return '';
  if(typeof x==='object') x = x.fmt ?? x.raw ?? x.longFmt ?? x.value ?? x.label ?? '';
  let s=String(x).trim();
  if(!s || ['nan','none','null','—','-','not published by tradingview','not published','public feed unavailable'].includes(s.toLowerCase())) return '';
  if(/not returned|not supplied|unavailable|fallback field|proxy unavailable/i.test(s) && s.length<95) return '';
  return s.replace(/\s+/g,' ');
}
function firstVal(...xs){ for(const x of xs){ const v=val(x); if(v) return v; } return ''; }
function inferInstrument(r){ const n=String(r.name||r.label||'').toLowerCase(), lab=String(r.label||'').toUpperCase(); if(n.includes('etf')||n.includes('ucits')||['SPY','IBIT','ISVAF','IUVL','VGT','SMH','SMH_EPA'].includes(lab)) return 'ETF'; if(n.includes('index')||['CAC40','VXN','DJI','FTSE','HSI','IXIC','NYA','NYFANG','SPX','VIX','GSOX','GSOXNR','GSOXTR','SEMIEW5T'].includes(lab)) return 'Index'; return 'Equity'; }
const sectorHints={AAPL:['Technology','Consumer electronics'],MSFT:['Technology','Software infrastructure'],NVDA:['Technology','Semiconductors'],AMD:['Technology','Semiconductors'],AVGO:['Technology','Semiconductors'],INTC:['Technology','Semiconductors'],ASML:['Technology','Semiconductor equipment'],QCOM:['Technology','Semiconductors'],MU:['Technology','Semiconductor memory'],MPWR:['Technology','Semiconductors'],SMCI:['Technology','Computer hardware'],ARM:['Technology','Semiconductor IP'],ORCL:['Technology','Enterprise software'],IBM:['Technology','IT services'],GOOG:['Communication services','Internet content & information'],GOOGL:['Communication services','Internet content & information'],META:['Communication services','Internet content & information'],NFLX:['Communication services','Streaming entertainment'],SPOT:['Communication services','Audio streaming'],TTWO:['Communication services','Video games'],TSLA:['Consumer discretionary','Electric vehicles'],AMZN:['Consumer discretionary','Internet retail & cloud'],COST:['Consumer staples','Discount stores'],WMT:['Consumer staples','Discount stores'],KO:['Consumer staples','Beverages'],JPM:['Financials','Banks'],BAC:['Financials','Banks'],GS:['Financials','Capital markets'],MS:['Financials','Capital markets'],C:['Financials','Banks'],BLK:['Financials','Asset management'],BX:['Financials','Asset management'],KKR:['Financials','Asset management'],CME:['Financials','Financial exchanges'],NDAQ:['Financials','Financial exchanges'],AXP:['Financials','Credit services'],MA:['Financials','Payment networks'],V:['Financials','Payment networks'],AON:['Financials','Insurance brokers'],HSBC:['Financials','Banks'],HSBA:['Financials','Banks'],UBS:['Financials','Banks'],RY:['Financials','Banks'],ABBV:['Healthcare','Drug manufacturers'],LLY:['Healthcare','Drug manufacturers'],PFE:['Healthcare','Drug manufacturers'],UNH:['Healthcare','Healthcare plans'],GE:['Industrials','Aerospace & industrials'],LMT:['Industrials','Aerospace & defense'],RTX:['Industrials','Aerospace & defense'],SAF:['Industrials','Aerospace & defense'],PLTR:['Technology','Software infrastructure'],TSM:['Technology','Semiconductors'],2330:['Technology','Semiconductors'],2317:['Technology','Electronics manufacturing'],2357:['Technology','Computer hardware'],SPY:['ETF','Broad US equity ETF'],IBIT:['ETF','Spot bitcoin ETF'],ISVAF:['ETF','Nasdaq 100 UCITS ETF'],IUVL:['ETF','USA value factor UCITS ETF'],VGT:['ETF','Information technology ETF'],SMH:['ETF','Semiconductor ETF'],SMH_EPA:['ETF','Semiconductor UCITS ETF'],GSOX:['Index','Semiconductor index'],GSOXNR:['Index','Semiconductor net return index'],GSOXTR:['Index','Semiconductor total return index'],SEMIEW5T:['Index','Semiconductor equal weight index'],CAC40:['Index','France large-cap index'],DJI:['Index','US blue-chip index'],FTSE:['Index','UK large-cap index'],HSI:['Index','Hong Kong equity index'],IXIC:['Index','Nasdaq Composite index'],NYA:['Index','NYSE Composite index'],NYFANG:['Index','NYSE FANG+ index'],SPX:['Index','S&P 500 index'],VIX:['Index','Volatility index'],VXN:['Index','Nasdaq volatility index']};
const countryByExchange={NASDAQ:'United States',NYSE:'United States',AMEX:'United States',NYSEARCA:'United States',LON:'United Kingdom',LSE:'United Kingdom',EPA:'France',EURONEXT:'France',AMS:'Netherlands',HKG:'Hong Kong',HKEX:'Hong Kong',TPE:'Taiwan',TWSE:'Taiwan',TADAWUL:'Saudi Arabia',ICE:'United States',TVC:'Global'};
const currencyByExchange={NASDAQ:'USD',NYSE:'USD',AMEX:'USD',NYSEARCA:'USD',LON:'GBX',LSE:'GBX',EPA:'EUR',EURONEXT:'EUR',AMS:'EUR',HKG:'HKD',HKEX:'HKD',TPE:'TWD',TWSE:'TWD',TADAWUL:'SAR',ICE:'USD',TVC:'Chart currency'};
function profileValue(p,k){ return val((p||{})[k]); }
function localRsi(r){ const h=(r.history||[]).map(d=>Number(d.close)).filter(v=>Number.isFinite(v)&&v>0); if(h.length<15) return ''; let gains=0,losses=0; for(let i=h.length-14;i<h.length;i++){ const d=h[i]-h[i-1]; if(d>=0) gains+=d; else losses-=d; } if(losses===0) return '100.0'; const rs=gains/losses; return (100-100/(1+rs)).toFixed(1); }
function histPerf(r,n){ const h=(r.history||[]).map(d=>Number(d.close)).filter(v=>Number.isFinite(v)&&v>0); if(h.length<=n) return 'Insufficient history'; const pct=(h.at(-1)/h[h.length-1-n]-1)*100; return (pct>=0?'+':'')+pct.toFixed(2)+'%'; }
function localRating(r){ const pct=chg(r), conf=Number(r.confidence||0); if(pct>0.12 && conf>=42) return 'Constructive (local model)'; if(pct<-0.12 && conf>=42) return 'Cautious (local model)'; return 'Neutral (local model)'; }
function localMaRating(r){ const h=(r.history||[]).map(d=>Number(d.close)).filter(v=>Number.isFinite(v)&&v>0); if(h.length<50) return 'Neutral (local MA)'; const avg=n=>h.slice(-n).reduce((a,b)=>a+b,0)/Math.min(n,h.length); const last=h.at(-1); let score=0; if(last>avg(20))score++; else score--; if(last>avg(50))score++; else score--; if(h.length>=200){ if(last>avg(200))score++; else score--; } return score>=2?'Buy (local MA)':score<=-2?'Sell (local MA)':'Neutral (local MA)'; }

function localAdtv(r){
  try{
    const h=(r.history||[]).slice(-70).filter(d=>Number.isFinite(Number(d.close))&&Number.isFinite(Number(d.volume))&&Number(d.volume)>0);
    if(!h.length) return '';
    const last=Number((r.history||[]).at(-1)?.close||r.last||0);
    const vols=h.slice(-60).map(d=>Number(d.volume)).filter(v=>Number.isFinite(v)&&v>0);
    if(!Number.isFinite(last)||last<=0||!vols.length) return '';
    const avgVol=vols.reduce((a,b)=>a+b,0)/vols.length;
    const cur=(r.profile&&r.profile.currency)||'';
    return money(last*avgVol)+' '+cur+' last close × 60D avg volume';
  }catch(e){return '';}
}
function probabilityValue(r){
  let p=r?.analysis?.scenario?.probability_up ?? r?.probability_up;
  if(p===undefined||p===null||Number.isNaN(Number(p))) return null;
  p=Number(p);
  if(p>1.0001) p=p/100;
  return Math.max(0,Math.min(1,p));
}
function fmtProb01(p){ return p===null?'Unavailable':p.toFixed(2); }
function completeProfile(r){
  const p={...(r.profile||{})};
  const lab=String(r.label||'').toUpperCase();
  const inst=firstVal(p.instrument,inferInstrument(r));
  const hint=sectorHints[lab]||[];
  const ex=firstVal(p.exchange,r.tv_exchange,r.original_exchange,'Chart exchange');
  const nonCorp=(inst==='ETF'||inst==='Index');
  const pubMissing=nonCorp?'N/A for ETF/index':'Field not published by current public feeds';
  const out={
    description:firstVal(p.description,`${r.name||r.label} is analysed with TradingView overview/screener, Superchart OHLCV history, public fundamentals, news sentiment and quantitative validation.`),
    instrument:firstVal(p.instrument,inst),
    sector:firstVal(p.sector,hint[0],nonCorp?inst:'Public company'),
    industry:firstVal(p.industry,hint[1],inst==='ETF'?'Diversified ETF':inst==='Index'?'Benchmark/index':'Public industry classification'),
    exchange:firstVal(p.exchange,ex),
    market_cap:firstVal(p.market_cap, pubMissing),
    adtv:firstVal(p.adtv, localAdtv(r), pubMissing),
    revenue:firstVal(p.revenue, pubMissing),
    net_income:firstVal(p.net_income, pubMissing),
    profit_margin:firstVal(p.profit_margin, pubMissing),
    gross_margin:firstVal(p.gross_margin, pubMissing),
    operating_margin:firstVal(p.operating_margin, pubMissing),
    pe_ratio:firstVal(p.pe_ratio, nonCorp?'N/A for ETF/index':pubMissing),
    price_sales:firstVal(p.price_sales, nonCorp?'N/A for ETF/index':pubMissing),
    price_book:firstVal(p.price_book, pubMissing),
    dividend_yield:firstVal(p.dividend_yield,'0.00% or not distributed'),
    beta:firstVal(p.beta, nonCorp?'N/A for ETF/index':pubMissing),
    employees:firstVal(p.employees, nonCorp?'N/A for ETF/index':pubMissing),
    country:firstVal(p.country,countryByExchange[String(ex).toUpperCase()],countryByExchange[String(r.original_exchange||'').toUpperCase()],'Global'),
    currency:firstVal(p.currency,currencyByExchange[String(ex).toUpperCase()],currencyByExchange[String(r.original_exchange||'').toUpperCase()],'Chart currency'),
    tradingview_rating:firstVal(p.tradingview_rating,localRating(r)),
    technical_rating:firstVal(p.technical_rating,localMaRating(r)),
    rsi:firstVal(p.rsi,localRsi(r),'50.0'),
    perf_1d:firstVal(p.perf_1d,histPerf(r,1)),
    perf_5d:firstVal(p.perf_5d,histPerf(r,5)),
    perf_1m:firstVal(p.perf_1m,histPerf(r,21)),
    perf_3m:firstVal(p.perf_3m,histPerf(r,63)),
    perf_6m:firstVal(p.perf_6m,histPerf(r,126)),
    perf_1y:firstVal(p.perf_1y,histPerf(r,252)),
    data_source:firstVal(p.data_source,'TradingView overview/screener + Superchart OHLCV + Yahoo fundamentals')
  };
  Object.keys(out).forEach(k=>{ if(!val(out[k])) out[k]=pubMissing; });
  return out;
}
function cleanDesc(s){ s=firstVal(s,'TradingView overview is partially unavailable; the dashboard displays remote fundamentals when available plus chart-derived fallback fields.'); s=s.replace(/followed in this watchlist for price action, risk, news and market-context analysis\.?/gi,''); s=s.replace(/The dashboard combines TradingView price history, volatility, market context and public headlines\.?/gi,''); return s.replace(/\s+/g,' ').trim(); }
function renderProfile(r){
  const p=completeProfile(r);
  $('profileSub').textContent=`${esc(r.label)} · ${esc(p.instrument)} · score ${Number(r.investment_score||0).toFixed(1)}/10`;
  const desc=cleanDesc(p.description);
  const stats=[['Instrument',p.instrument],['Sector',p.sector],['Industry',p.industry],['Exchange',p.exchange],['Market cap',p.market_cap],['ADTV',p.adtv],['Revenue / scale',p.revenue],['Net income',p.net_income],['Profit margin',p.profit_margin],['Gross margin',p.gross_margin],['Operating margin',p.operating_margin],['P/E',p.pe_ratio],['P/S',p.price_sales],['P/B',p.price_book],['Dividend yield',p.dividend_yield],['Beta',p.beta],['Employees',p.employees],['Country',p.country],['Currency',p.currency],['TV rating',p.tradingview_rating],['MA rating',p.technical_rating],['RSI',p.rsi],['Perf 1D',p.perf_1d],['Perf 5D',p.perf_5d],['Perf 1M',p.perf_1m],['Perf 3M',p.perf_3m],['Perf 6M',p.perf_6m],['Perf 1Y',p.perf_1y],['Data source',p.data_source]];
  $('profilePanel').innerHTML=`<div class="profileTop"><div class="profileTitle"><h3>${esc(r.name||r.label)}</h3><p>${esc(desc)}</p><div class="pillRow"><span class="pill">${esc(p.instrument)}</span><span class="pill">${esc(p.sector)}</span><span class="pill">${esc(p.industry)}</span><span class="pill">Latest close ${esc(r.last_date||'Unavailable')}</span></div></div><div class="investBox"><div class="scoreRing">${Number(r.investment_score||0).toFixed(1)}<small>/10</small></div><div><h4>${esc(r.investment_label||'Investment view')}</h4><p>Low-loss-tolerance score using forecast distribution, confidence, risk regime, public catalysts and validation quality.</p></div></div></div><div class="profileStats">${stats.map(([k,v])=>`<div class="statBox"><span>${esc(k)}</span><b>${esc(v)}</b></div>`).join('')}</div>`;
}
function articleCard(n){
  const title=esc(n.title||'Untitled article');
  const src=esc(n.source||n.source_group||'News');
  const score=Number(n.score||0).toFixed(2);
  const published=n.published?` · ${esc(n.published)}`:'';
  const url=String(n.link||n.url||'').trim();
  if(url){
    return `<a class="newsItem newsLink" href="${esc(url)}" target="_blank" rel="noopener noreferrer"><strong>${title}</strong><small>${src}${published} · score ${score}</small><small>Source: ${src} · Lien article: <em>${esc(url)}</em></small></a>`;
  }
  return `<div class="newsItem"><strong>${title}</strong><small>${src}${published} · score ${score}</small></div>`;
}
function renderDetails(r){
  $('selectedSub').textContent=`${r.tv_exchange||''}:${r.tv_symbol||r.label}`;
  const H=r.horizon_days || (r.forecast||[]).length || horizon();
  const headlines=(r.news?.headlines||[]).slice(0,12);
  const f=r.forecast||[];
  const low=Math.min(...f.map(x=>Number(x.low||x.price||0)).filter(Boolean));
  const high=Math.max(...f.map(x=>Number(x.high||x.price||0)).filter(Boolean));
  const drivers=(r.analysis?.drivers||[]).slice(0,7);
  const methods=(r.analysis?.method_summary||["Validated Alpaca/Yahoo/Stooq/TradingView OHLCV fallback chain.","Ridge/Huber validation on log-return features.","EWMA realised-volatility bands.","TradingView screener/overview profile and ratings.","Multi-source headline sentiment with clickable source links.","Remote realised-error learning calibration when configured."]).slice(0,7);
  const scenario=r.analysis?.scenario||{};
  const prob=probabilityValue(r);
  const learn=r.learning||{},srv=r.server_learning||{};
  const pm=r.premarket||{};
  const pmPct=Number(pm.change_pct||0);
  const pmTxt=pm.session?`${esc(pm.session)} · ${money(pm.price||r.last)} · <span class="${pctClass(pmPct)}">${fmtPct(pmPct)}</span>`:'Unavailable';
  const pmSource=pm.source?esc(pm.source):'No pre-market source';
  const liveState=liveStateText(r);
  const liveTxt=`<small class="smallMuted liveState" data-selected-live-source>${r.live_price?`${esc(r.live_provider||r.live_source||'Live quote')} · ${esc(liveState)} · ${esc(r.live_update_mode||'quote')}`:''}</small>`;
  const learnMae=(learn.mae_pct===null||learn.mae_pct===undefined)?'Cold start':Number(learn.mae_pct).toFixed(2)+'%';
  const learnDir=(learn.directional_accuracy===null||learn.directional_accuracy===undefined)?'Cold start':(Number(learn.directional_accuracy)*100).toFixed(0)+'%';
  const root=$('details');
  root.innerHTML=`<div class="bigTicker"><div><h3>${esc(r.label)}</h3><p>${esc(r.name)}</p></div><div class="priceBox"><div class="price" data-selected-price>${money(priceVal(r))}</div><div data-selected-horizon class="${pctClass(chg(r))}">${fmtPct(chg(r))} · ${H}D</div>${liveTxt}</div></div><div class="forecastGrid">${f.map((x,i)=>`<div class="day" data-forecast-index="${i}"><div class="d">${esc(x.date.slice(5))}</div><div class="p" data-selected-forecast-price>${money(x.price)}</div><div data-selected-forecast-pct class="${pctClass(x.pct)}">${fmtPct(x.pct)}</div></div>`).join('')}</div><div class="metrics"><div class="metric"><span>${H}D central case</span><b data-selected-central class="${pctClass(chg(r))}">${fmtPct(chg(r))}</b></div><div class="metric"><span>Forecast range</span><b data-selected-range>${money(low)} - ${money(high)}</b></div><div class="metric"><span>Probability up</span><b>${fmtProb01(prob)}</b><small class="smallMuted">0 = bearish · 1 = bullish</small></div><div class="metric"><span>Confidence</span><b>${Number(r.confidence||0).toFixed(0)}/100</b></div><div class="metric"><span>Risk regime</span><b>${esc(r.risk)}</b></div><div class="metric"><span>Investment score</span><b>${Number(r.investment_score||0).toFixed(1)}/10</b></div><div class="metric"><span>Pre-market / extended</span><b>${pmTxt}</b><small class="smallMuted">${pmSource}</small></div><div class="metric"><span>Learning MAE</span><b>${esc(learnMae)}</b></div><div class="metric"><span>Learning direction</span><b>${esc(learnDir)}</b></div></div><div class="analysisBox"><h4>Adaptive learning agent</h4><p>The agent evaluates older predictions against newly available closes at each run, updates realised error, bias and directional accuracy, then adjusts forecast shrinkage and confidence automatically. Pre-market/extended-session context is used as an intraday impulse, not as a blind replacement for the model.</p><ul class="analysisList"><li>Realised forecasts evaluated this run: ${Number(learn.evaluated_this_run||0)}</li><li>Total learned samples for this ticker/global fallback: ${Number(learn.count||0)}</li><li>Current reliability coefficient: ${Number(learn.reliability||0).toFixed(2)}</li><li>Bias correction applied: ${Number(learn.bias_correction_pct||0).toFixed(2)}%</li></ul></div><div class="analysisBox"><h4>Forecast rationale</h4><p>The central forecast is a conditional distribution, not a simple trend extension. It combines validation-weighted returns, stochastic volatility, public catalysts, pre-market context and realised-error learning.</p><ul class="analysisList">${drivers.map(d=>`<li>${esc(d)}</li>`).join('')}</ul></div><div class="analysisBox"><h4>Quant methods used</h4><ul class="analysisList">${methods.map(d=>`<li>${esc(d)}</li>`).join('')}</ul></div><div class="news"><h4>Multi-source headlines</h4>${headlines.map(articleCard).join('')||'<div class="newsItem">No recent headline used.</div>'}</div>`;
  const learningBox=root.querySelector('.analysisBox');
  if(learningBox)learningBox.innerHTML=`<h4>Adaptive learning v15</h4><p>Only exact target-session closes are accepted. Raw and calibrated forecasts remain separate, and a correction is applied only after a ticker/horizon/regime/model profile improves a temporal holdout.</p><ul class="analysisList"><li>Status: ${esc(srv.applied?'validated calibration applied':srv.reason||srv.status||'no validated profile')}</li><li>Evidence samples: ${Number(srv.sample_count||learn.count||0)} · reliability ${Number(srv.reliability||learn.reliability||0).toFixed(3)}</li><li>Profile: ${esc(srv.profile_scope||'not yet approved')}</li><li>Holdout MAE improvement: ${srv.mae_improvement_pct===null||srv.mae_improvement_pct===undefined?'n/a':Number(srv.mae_improvement_pct).toFixed(2)+'%'}</li><li>Raw → calibrated: ${Number.isFinite(Number(srv.raw_forecast_pct))?Number(srv.raw_forecast_pct).toFixed(3)+'% → '+Number(srv.calibrated_forecast_pct).toFixed(3)+'%':'no price correction'}</li><li>Confidence: ${Number((srv.raw_confidence??r.confidence)||0).toFixed(1)} → ${Number((srv.calibrated_confidence??r.confidence)||0).toFixed(1)}</li></ul>`;
  root._apexLiveLabel=String(r.label||'');
  root._apexLiveRefs={price:root.querySelector('[data-selected-price]'),horizon:root.querySelector('[data-selected-horizon]'),liveSource:root.querySelector('[data-selected-live-source]'),central:root.querySelector('[data-selected-central]'),range:root.querySelector('[data-selected-range]'),forecastPrices:[...root.querySelectorAll('[data-selected-forecast-price]')],forecastPcts:[...root.querySelectorAll('[data-selected-forecast-pct]')]};
  updateLearningSelectedImpact();
}
function sheetNum(x,d=0){ const n=Number(x); return Number.isFinite(n)?n:d; }
function sheetBound(v,lo,hi){ return Math.max(lo,Math.min(hi,v)); }
function sheetUSD(x){ return fmtUSD(sheetNum(x)); }
function sheetPct(x){ return (sheetNum(x)>=0?'+':'')+sheetNum(x).toFixed(2)+'%'; }
function sheetCloses(r){ return (r.history||[]).map(d=>({date:String(d.date||'').slice(0,10),close:Number(d.close),volume:Number(d.volume||0)})).filter(d=>d.date&&Number.isFinite(d.close)&&d.close>0).sort((a,b)=>a.date.localeCompare(b.date)); }
function sheetCurrentPrice(r){ const live=sheetNum(r.live_price,0); if(live>0) return live; const pm=r.premarket||{}; const px=sheetNum(pm.price,0)>0?sheetNum(pm.price):sheetNum(r.last,priceVal(r)); return px>0?px:priceVal(r); }
function sheetDailyReturns(r){ const h=sheetCloses(r), out=[]; for(let i=1;i<h.length;i++){ if(h[i-1].close>0) out.push((h[i].close/h[i-1].close)-1); } return out.filter(Number.isFinite); }
function sheetVolAnnPct(r){ const direct=sheetNum(r.professional_decision?.expected_volatility_ann_pct, sheetNum(r.volatility_ann_pct, NaN)); if(Number.isFinite(direct)&&direct>0) return direct; const rets=sheetDailyReturns(r).slice(-90); if(rets.length<8) return 24; const m=rets.reduce((a,b)=>a+b,0)/rets.length; const variance=rets.reduce((a,b)=>a+(b-m)*(b-m),0)/Math.max(1,rets.length-1); return Math.sqrt(variance)*Math.sqrt(252)*100; }
function sheetMomentumPct(r,n){ const h=sheetCloses(r); if(h.length<=n) return NaN; return (h.at(-1).close/h[h.length-1-n].close-1)*100; }
function sheetLiquidityUSD(r){ const h=sheetCloses(r).slice(-60).filter(d=>d.volume>0); if(!h.length) return NaN; const avgVol=h.reduce((a,b)=>a+b.volume,0)/h.length; return avgVol*sheetCurrentPrice(r); }
function sheetCalibration(r){
  const pro=r.professional_decision||{}, basis=pro.decision_basis||{}, learn=r.learning||{}, srv=r.server_learning||{};
  const samples=sheetNum(basis.sample_count, sheetNum(learn.count, sheetNum(srv.sample_count,0)));
  const reliability=sheetBound(sheetNum(learn.reliability, sheetNum(srv.reliability, samples>0?Math.min(1,samples/80):0)),0,1);
  const dir=sheetNum(learn.directional_accuracy, sheetNum(srv.directional_accuracy, NaN));
  const mae=sheetNum(learn.mae_pct, sheetNum(srv.mae_pct, NaN));
  const prob=probabilityValue(r);
  const raw=sheetNum(pro.calibrated_confidence, sheetNum(r.confidence,50));
  const sampleDecay=samples>0?Math.sqrt(Math.min(1,samples/80)):0.58;
  const dirAdj=Number.isFinite(dir)?(dir-.5)*18:0;
  const maePenalty=Number.isFinite(mae)?Math.min(18,mae*1.6):5;
  const probAdj=prob===null?0:(Math.abs(prob-.5)*24);
  const calibrated=sheetBound(raw*(.52+.48*sampleDecay)+reliability*16+dirAdj+probAdj-maePenalty,5,95);
  return {calibrated,samples,reliability,directional_accuracy:Number.isFinite(dir)?dir:null,mae_pct:Number.isFinite(mae)?mae:null,probability_up:prob,sample_decay:sampleDecay};
}
function sheetRange(r,amount){
  const f=r.forecast||[], px=sheetCurrentPrice(r), H=r.horizon_days||(f.length||horizon());
  const lowPx=f.length?Math.min(...f.map(x=>sheetNum(x.low,sheetNum(x.price,px))).filter(v=>v>0)):px*(1-sheetVolAnnPct(r)/100*Math.sqrt(H/252)*1.65);
  const highPx=f.length?Math.max(...f.map(x=>sheetNum(x.high,sheetNum(x.price,px))).filter(v=>v>0)):px*(1+sheetVolAnnPct(r)/100*Math.sqrt(H/252)*1.35);
  const lowPct=px>0?(lowPx/px-1)*100:0, highPct=px>0?(highPx/px-1)*100:0;
  return {lowPx,highPx,lowPct,highPct,lowUSD:amount*lowPct/100,highUSD:amount*highPct/100};
}
function sheetRegime(r){
  const pro=r.professional_decision||{}; if(pro.regime?.name) return pro.regime.name;
  const vol=sheetVolAnnPct(r), m20=sheetMomentumPct(r,20), m60=sheetMomentumPct(r,60);
  const trend=(m20>0&&m60>0)?'uptrend':(m20<0&&m60<0)?'downtrend':'mixed trend';
  const volName=vol>=45?'high volatility':vol>=24?'normal volatility':'low volatility';
  return `${trend}, ${volName}`;
}
function sheetRecommendedAction(r,expectedPct,cal,vol){
  const pro=String(r.professional_decision?.recommended_action||'').toLowerCase();
  if(pro.includes('rebalance')) return 'rebalance candidate';
  if(pro.includes('avoid')) return 'avoid';
  if(pro.includes('watch')) return 'watch';
  if(pro.includes('investigate')) return 'investigate';
  if(expectedPct>.65 && cal.calibrated>=60 && vol<45 && r.risk!=='High') return 'rebalance candidate';
  if(expectedPct>.1 && cal.calibrated>=43) return 'watch';
  if(expectedPct<-.35 || r.risk==='High') return 'avoid';
  return 'investigate';
}
function sheetPositionSizingHint(r,amount,cal,vol){
  const liq=sheetLiquidityUSD(r), liqHit=Number.isFinite(liq)&&liq>0?amount/liq:0;
  if(r.professional_decision?.position_sizing_hint) return r.professional_decision.position_sizing_hint;
  if(r.risk==='High'||vol>=45||cal.calibrated<42||liqHit>.12) return 'risk budget compatible: faible';
  if(r.risk==='Moderate'||cal.calibrated<62||liqHit>.04) return 'risk budget compatible: moyen';
  return 'risk budget compatible: élevé';
}
function sheetGroup(r){ const p=completeProfile(r); return `${p.sector} / ${p.industry}`; }
function sheetScore(r,amount){
  const er=sheetNum(r.professional_decision?.expected_return_pct,chg(r)), vol=sheetVolAnnPct(r), cal=sheetCalibration(r), liq=sheetLiquidityUSD(r);
  const liqPenalty=Number.isFinite(liq)&&liq>0?Math.min(35,(amount/liq)*100):0;
  return er*7 + cal.calibrated*.45 - vol*.22 + sheetNum(r.investment_score,0)*2 - liqPenalty;
}
function sheetSuggestTicker(r,amount){
  const peers=(results||[]).filter(x=>x && x.label!==r.label && !x.error);
  const scored=peers.map(x=>({r:x,score:sheetScore(x,amount),er:sheetNum(x.professional_decision?.expected_return_pct,chg(x)),vol:sheetVolAnnPct(x),cal:sheetCalibration(x).calibrated,liq:sheetLiquidityUSD(x)})).filter(x=>x.er>0 && x.cal>=38 && (!Number.isFinite(x.liq)||x.liq<=0||amount<=x.liq*.12)).sort((a,b)=>b.score-a.score);
  if(!scored.length) return 'No stronger alternative in the current run after risk/liquidity filters.';
  const x=scored[0];
  return `${x.r.label} (${x.r.name}) · expected return ${sheetPct(x.er)}, calibrated confidence ${x.cal.toFixed(0)}/100, volatility ${x.vol.toFixed(1)}%.`;
}
function sheetWhyNow(r,amount,metrics){
  const p=completeProfile(r), news=r.news||{}, newsScore=sheetNum(news.score,0), headlines=(news.headlines||[]).slice(0,3).map(n=>n.title).filter(Boolean);
  const spy=results.find(x=>x.label==='SPY'), ixic=results.find(x=>x.label==='IXIC'), vix=results.find(x=>x.label==='VIX');
  const m20=sheetMomentumPct(r,20), m60=sheetMomentumPct(r,60), rets=sheetDailyReturns(r), lastRet=rets.length?rets.at(-1)*100:NaN;
  const liq=sheetLiquidityUSD(r), volRows=sheetCloses(r).slice(-60).filter(d=>d.volume>0), lastVol=volRows.length?volRows.at(-1).volume:NaN, avgVol=volRows.length?volRows.reduce((a,b)=>a+b.volume,0)/volRows.length:NaN;
  const volumeImpulse=(Number.isFinite(lastVol)&&Number.isFinite(avgVol)&&avgVol>0)?lastVol/avgVol:NaN;
  const macro=[]; if(spy) macro.push(`vs SPY ${sheetPct(metrics.expectedPct-chg(spy))}`); if(ixic) macro.push(`vs Nasdaq ${sheetPct(metrics.expectedPct-chg(ixic))}`); if(vix) macro.push(`VIX forecast context ${sheetPct(chg(vix))}`);
  return {
    momentum:`20D ${Number.isFinite(m20)?sheetPct(m20):'n/a'} · 60D ${Number.isFinite(m60)?sheetPct(m60):'n/a'} · model horizon ${sheetPct(metrics.expectedPct)}.`,
    news:`Headline sentiment ${newsScore>=0?'+':''}${newsScore.toFixed(2)} across ${Number(news.count||0)} item(s). ${headlines.length?'Top: '+headlines.join(' | '):'No headline catalyst retained.'}`,
    volatilite:`Annualised realised volatility ${metrics.vol.toFixed(1)}%; forecast downside ${sheetPct(metrics.range.lowPct)} for ${sheetUSD(metrics.range.lowUSD)} on ${sheetUSD(amount)}.`,
    macro_proxy:macro.length?macro.join(' · '):'No SPY/Nasdaq/VIX proxy returned in the current run.',
    anomaly:`Last daily return ${Number.isFinite(lastRet)?sheetPct(lastRet):'n/a'}; volume impulse ${Number.isFinite(volumeImpulse)?volumeImpulse.toFixed(2)+'x 60D avg':'n/a'}; liquidity proxy ${Number.isFinite(liq)?sheetUSD(liq)+' ADTV':'unavailable'}.`,
    valuation:`${p.instrument}; sector ${p.sector}; P/E ${p.pe_ratio}; P/S ${p.price_sales}; P/B ${p.price_book}; beta ${p.beta}.`,
    trend_conflict:`Signal ${r.signal}; model/MA context ${p.tradingview_rating} / ${p.technical_rating}; risk regime ${r.risk}; calibrated confidence ${metrics.cal.calibrated.toFixed(1)}/100.`,
    suggested_other_ticker:sheetSuggestTicker(r,amount),
  };
}
function selectedAnalysisMetrics(r,amount){
  amount=sheetBound(sheetNum(amount,0),0,1000000000);
  const expectedPct=sheetNum(r.professional_decision?.expected_return_pct,chg(r)), vol=sheetVolAnnPct(r), range=sheetRange(r,amount), cal=sheetCalibration(r);
  const strength=sheetNum(r.professional_decision?.signal_strength, sheetBound(Math.abs(expectedPct)/Math.max(1,vol/8)*48 + cal.calibrated*.42,0,100));
  const action=sheetRecommendedAction(r,expectedPct,cal,vol);
  const sizing=sheetPositionSizingHint(r,amount,cal,vol);
  const currentPrice=sheetCurrentPrice(r);
  return {amount,expectedPct,expectedUSD:amount*expectedPct/100,vol,volUSD:amount*vol/100,downsidePct:range.lowPct,downsideUSD:range.lowUSD,range,cal,strength,action,sizing,currentPrice,regime:sheetRegime(r)};
}
function selectedAnalysisSheetBody(label,amount){
  const r=(results||[]).find(x=>x.label===label) || selected;
  if(!r) return '<div class="sheetEmpty">Run analysis and select a ticker first.</div>';
  const m=selectedAnalysisMetrics(r,amount), why=sheetWhyNow(r,m.amount,m), p=completeProfile(r);
  const rows=[
    ['expected_return',`${sheetPct(m.expectedPct)} · ${sheetUSD(m.expectedUSD)} on ${sheetUSD(m.amount)}`],
    ['expected_volatility',`${m.vol.toFixed(2)}% annualised · ${sheetUSD(m.volUSD)} notional 1σ proxy`],
    ['downside_risk',`${sheetPct(m.downsidePct)} · ${sheetUSD(m.downsideUSD)} low-band scenario`],
    ['confidence_interval',`${money(m.range.lowPx)} - ${money(m.range.highPx)} price band · ${sheetUSD(m.range.lowUSD)} to ${sheetUSD(m.range.highUSD)}`],
    ['signal_strength',`${m.strength.toFixed(1)}/100`],
    ['regime',m.regime],
    ['recommended_action',m.action],
    ['position_sizing_hint',m.sizing],
  ];
  const evidence=[
    `Calibrated confidence ${m.cal.calibrated.toFixed(1)}/100; raw confidence ${Number(r.confidence||0).toFixed(1)}/100.`,
    `Realised learning samples ${m.cal.samples}; reliability ${m.cal.reliability.toFixed(2)}; direction ${m.cal.directional_accuracy===null?'n/a':(m.cal.directional_accuracy*100).toFixed(0)+'%'}; MAE ${m.cal.mae_pct===null?'n/a':m.cal.mae_pct.toFixed(2)+'%'}.`,
    `Market value source: ${r.live_price?(r.live_provider||r.live_source||'live quote')+' '+(r.live_delayed?'delayed':r.live_real_time_hint?'low latency':'live')+' '+(r.live_update_mode||'quote'):r.premarket?.session?'extended/pre-market context':'latest validated market close'} at ${money(m.currentPrice)}; last close date ${esc(r.last_date||'unknown')}.`,
    `Defensible signal basis: forecast distribution, volatility scaling, realised-error learning, public profile, news and relative proxies from the current run.`,
  ];
  return `<section class="sheetGrid">${rows.map(([k,v])=>`<div class="sheetMetric"><span>${esc(k)}</span><b>${esc(v)}</b></div>`).join('')}</section><section class="sheetBlock"><h2>why_now</h2><div class="whyGrid">${Object.entries(why).map(([k,v])=>`<div><span>${esc(k)}</span><p>${esc(v)}</p></div>`).join('')}</div></section><section class="sheetBlock"><h2>Calibrated confidence and proof</h2><ul>${evidence.map(x=>`<li>${esc(x)}</li>`).join('')}</ul></section><section class="sheetBlock"><h2>Professional context</h2><p>${esc(r.label)} · ${esc(r.name)} · ${esc(sheetGroup(r))}</p><p>Instrument ${esc(p.instrument)} · Exchange ${esc(p.exchange)} · Currency ${esc(p.currency)} · Data source ${esc(p.data_source)}</p></section>`;
}
function openSelectedAnalysisSheet(){
  if(!selected){ alert('Run analysis and select a ticker first.'); return; }
  const lab=String(selected.label||'TICKER');
  const saved=Number(localStorage.getItem('apexSelectedAnalysisAmount_'+lab)||100000);
  const amount=sheetBound(Number.isFinite(saved)?saved:100000,0,1000000000);
  const selectedAnalysisFavicon=new URL('/assets/apex-tool-logo.png?v=15',window.location.href).href;
  const selectedAnalysisUrl=`${window.location.origin}/selected-analysis?symbol=${encodeURIComponent(lab)}`;
  const win=window.open('', 'apex_selected_analysis_'+lab);
  if(!win){ alert('Browser blocked the new tab. Allow pop-ups for this local dashboard.'); return; }
  try{win.history.replaceState({view:'selected-analysis',symbol:lab},'',selectedAnalysisUrl);}catch(e){}
  const css=`:root{--theme-bg:#070a10;--theme-panel:#0d111c;--theme-control:#090e19;--theme-section:#0f172a;--theme-line:#253047;--theme-accent-rgb:59,130,246;--blue:#3b82f6;--cyan:#22d3ee}body{margin:0;background:var(--theme-bg);color:#edf4ff;font-family:Inter,ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}.sheet{max-width:1180px;margin:0 auto;padding:28px}.top{display:flex;justify-content:space-between;gap:20px;align-items:flex-start;border-bottom:1px solid var(--theme-line);padding-bottom:18px;margin-bottom:18px}.top h1{margin:0;font-size:30px}.top p{margin:6px 0 0;color:#94a3b8}.amountBox{margin-top:14px;display:grid;grid-template-columns:minmax(220px,360px) 1fr;gap:12px;align-items:end}.amountBox label{display:grid;gap:6px;color:#94a3b8;font-size:12px;text-transform:uppercase;letter-spacing:.08em}.amountBox input{height:42px;background:var(--theme-control);color:#edf4ff;border:1px solid var(--theme-line);border-radius:12px;padding:0 12px;font-size:15px}.amountBox small{color:#94a3b8;line-height:1.4}.sheetGrid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px}.sheetMetric,.sheetBlock{background:var(--theme-panel);border:1px solid var(--theme-line);border-radius:14px;padding:14px}.sheetMetric span,.whyGrid span{display:block;color:#8fa1ba;font-size:10px;text-transform:uppercase;letter-spacing:.08em}.sheetMetric b{display:block;margin-top:7px;font-size:16px;line-height:1.35}.sheetBlock{margin-top:14px}.sheetBlock h2{font-size:14px;margin:0 0 10px}.sheetBlock p,.sheetBlock li{color:#cbd5e1;font-size:13px;line-height:1.55}.whyGrid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px}.whyGrid div{background:var(--theme-section);border:1px solid var(--theme-line);border-radius:12px;padding:12px}.whyGrid p{margin:7px 0 0}.badge{display:inline-flex;border:1px solid var(--blue);color:var(--cyan);background:rgba(var(--theme-accent-rgb),.10);border-radius:999px;padding:5px 9px;font-size:11px;font-weight:800}.sheetEmpty{padding:32px;color:#94a3b8}.legalFooter{margin-top:20px;padding-top:14px;border-top:1px solid var(--theme-line);color:#94a3b8;font-size:9.5px;line-height:1.55}.legalFooter strong,.legalCopyright{color:#edf4ff;font-weight:750}.legalFooter p{margin:5px 0 0}@media(max-width:900px){.sheetGrid,.whyGrid,.amountBox{grid-template-columns:1fr}.top{display:block}}`;
  win.document.open();
  win.document.write(`<!doctype html><html lang="en-US"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><base href="${window.location.origin}/"><link rel="icon" type="image/png" sizes="400x400" href="${selectedAnalysisFavicon}"><link rel="shortcut icon" type="image/png" href="${selectedAnalysisFavicon}"><link rel="apple-touch-icon" href="${selectedAnalysisFavicon}"><title>${esc(lab)} Selected analysis</title><style>${css}</style></head><body><main class="sheet"><header class="top"><div><h1>Selected analysis</h1><p>Market analysis sheet</p><div class="amountBox"><label>Investment amount USD<input id="sheetAmount" type="number" min="0" max="1000000000" step="1000" value="${String(amount)}"></label><small>Configurable from $0 to $1B. The sheet converts forecast, volatility and downside scenarios into dollar impact for the selected ticker.</small></div></div><span class="badge">${esc(lab)} · ${esc(selected.name||'')}</span></header><div id="sheetBody">${selectedAnalysisSheetBody(lab,amount)}</div><footer class="legalFooter" aria-label="Legal notice"><div class="legalCopyright">© 2026 Apex Tool. All rights reserved.</div><p><strong>Professional analytics notice.</strong> Apex Tool provides data analysis, research, analytical consulting and decision-support services for informational and professional use only. Outputs, scores, forecasts, scenarios and risk estimates are model-generated, may rely on delayed, incomplete, estimated or third-party data, and may change without notice. They do not constitute personalised investment advice, regulated investment research, an offer, solicitation, guarantee, official or contractual investment document, or any obligation to buy, hold or sell a financial instrument.</p><p>Investing involves risk, including possible loss of capital. Past, simulated and forecast performance is not a reliable indicator of future results. Users remain responsible for independent verification and suitability decisions. To the fullest extent permitted by applicable law, Apex Tool disclaims liability for losses or damages arising from use of or reliance on this analysis.</p></footer></main></body></html>`);
  win.document.close();
  applySelectedAnalysisTheme(win);
  const input=win.document.getElementById('sheetAmount'), body=win.document.getElementById('sheetBody');
  selectedAnalysisWindows.set(lab,{win,input,body});
  win.addEventListener('beforeunload',()=>selectedAnalysisWindows.delete(lab));
  const update=()=>{ const next=sheetBound(sheetNum(input.value,0),0,1000000000); localStorage.setItem('apexSelectedAnalysisAmount_'+lab,String(next)); body.innerHTML=selectedAnalysisSheetBody(lab,next); };
  input.addEventListener('input',update);
  input.addEventListener('blur',()=>{ input.value=String(sheetBound(sheetNum(input.value,0),0,1000000000)); update(); });
  win.focus();
}
function parseDateMs(d){
  const raw=String(d||'').trim(); if(!raw)return null;
  const value=/^\d{4}-\d{2}-\d{2}$/.test(raw)?raw+'T00:00:00Z':raw;
  const t=Date.parse(value); return Number.isFinite(t)?t:null;
}
function parseChartAxisMs(value){
  if(typeof value==='number')return Number.isFinite(value)?value:null;
  const raw=String(value??'').trim();if(!raw)return null;
  // Plotly emits wall-clock ranges without a timezone. Parse their components
  // as chart coordinates so the browser cannot apply Paris DST a second time.
  const match=raw.match(/^(\d{4})-(\d{2})-(\d{2})(?:[ T](\d{2})(?::(\d{2})(?::(\d{2})(?:\.(\d{1,9}))?)?)?)?$/);
  if(match){
    const fraction=String(match[7]||'').padEnd(3,'0').slice(0,3);
    const t=Date.UTC(Number(match[1]),Number(match[2])-1,Number(match[3]),Number(match[4]||0),Number(match[5]||0),Number(match[6]||0),Number(fraction||0));
    return Number.isFinite(t)?t:null;
  }
  const t=Date.parse(raw);return Number.isFinite(t)?t:null;
}
function chartAxisPixelToMs(axis,pixel,fallback=null){
  try{
    const value=typeof axis?.p2d==='function'?axis.p2d(pixel):null,parsed=parseChartAxisMs(value);
    if(Number.isFinite(parsed))return parsed;
  }catch(e){}
  return Number.isFinite(fallback)?fallback:null;
}
function chartWheelXRange(axis,size,pointerX,factor,minSpan,maxSpan){
  const range=axis?.range||[],x0=parseChartAxisMs(range[0]),x1=parseChartAxisMs(range[1]),width=Number(size?.w||0),left=Number(size?.l||0),pointer=Number(pointerX)-left;
  if(!Number.isFinite(x0)||!Number.isFinite(x1)||x0===x1||width<=0||!Number.isFinite(pointer)||!Number.isFinite(factor))return null;
  const span=Math.abs(x1-x0),targetSpan=clamp(span*factor,minSpan,maxSpan),zoom=targetSpan/span,cursor=clamp(pointer,0,width),ratio=cursor/width;
  const linearAnchor=x0+(x1-x0)*ratio,anchor=chartAxisPixelToMs(axis,cursor,linearAnchor);
  const fallbackStart=anchor+(x0-anchor)*zoom,fallbackEnd=anchor+(x1-anchor)*zoom;
  const startPixel=cursor+(0-cursor)*zoom,endPixel=cursor+(width-cursor)*zoom;
  let start=chartAxisPixelToMs(axis,startPixel,fallbackStart),end=chartAxisPixelToMs(axis,endPixel,fallbackEnd);
  if(!Number.isFinite(start)||!Number.isFinite(end)||start===end||Math.sign(end-start)!==Math.sign(x1-x0)){
    start=fallbackStart;end=fallbackEnd;
  }
  return [new Date(start).toISOString(),new Date(end).toISOString()];
}
function chartWheelYRange(axis,size,pointerY,factor,minSpan,maxSpan){
  const range=axis?.range||[],y0=Number(range[0]),y1=Number(range[1]),height=Number(size?.h||0),top=Number(size?.t||0),pointer=Number(pointerY)-top;
  if(!Number.isFinite(y0)||!Number.isFinite(y1)||y0===y1||height<=0||!Number.isFinite(pointer)||!Number.isFinite(factor))return null;
  const span=Math.abs(y1-y0),targetSpan=clamp(span*factor,minSpan,maxSpan),zoom=targetSpan/span,ratio=clamp(pointer,0,height)/height,anchor=y1+(y0-y1)*ratio,start=anchor+(y0-anchor)*zoom,end=anchor+(y1-anchor)*zoom;
  return Number.isFinite(start)&&Number.isFinite(end)&&start!==end?[start,end]:null;
}
function chartWheelPanXRange(axis,size,deltaPixels,rows=[]){
  const range=axis?.range||[],x0=parseChartAxisMs(range[0]),x1=parseChartAxisMs(range[1]),width=Number(size?.w||0),delta=Number(deltaPixels);
  if(!Number.isFinite(x0)||!Number.isFinite(x1)||x0===x1||width<=0||!Number.isFinite(delta)||delta===0)return null;
  const pixels=clamp(delta,-width*.8,width*.8),timeline=[...new Set((rows||[]).map(chartRowDisplayMs).filter(Number.isFinite))].sort((a,b)=>a-b);
  if(timeline.length>1){
    const coordinateAt=value=>{
      if(value<=timeline[0])return (value-timeline[0])/Math.max(1,timeline[1]-timeline[0]);
      const last=timeline.length-1;if(value>=timeline[last])return last+(value-timeline[last])/Math.max(1,timeline[last]-timeline[last-1]);
      let low=1,high=last;while(low<high){const middle=(low+high)>>1;if(timeline[middle]<value)low=middle+1;else high=middle;}
      const left=low-1,right=low,span=Math.max(1,timeline[right]-timeline[left]);return left+(value-timeline[left])/span;
    };
    const valueAt=coordinate=>{
      const last=timeline.length-1;
      if(coordinate<=0)return timeline[0]+coordinate*Math.max(1,timeline[1]-timeline[0]);
      if(coordinate>=last)return timeline[last]+(coordinate-last)*Math.max(1,timeline[last]-timeline[last-1]);
      const left=Math.floor(coordinate),fraction=coordinate-left;return timeline[left]+fraction*(timeline[left+1]-timeline[left]);
    };
    const startCoordinate=coordinateAt(x0),endCoordinate=coordinateAt(x1),coordinateSpan=endCoordinate-startCoordinate;
    if(Number.isFinite(coordinateSpan)&&coordinateSpan!==0){
      const shift=coordinateSpan*(pixels/width),start=valueAt(startCoordinate+shift),end=valueAt(endCoordinate+shift);
      if(Number.isFinite(start)&&Number.isFinite(end)&&start!==end)return [new Date(start).toISOString(),new Date(end).toISOString()];
    }
  }
  const sample=clamp(pixels,-width*.45,width*.45),center=width*.5,fallbackShift=(x1-x0)*(pixels/width);
  const centerMs=chartAxisPixelToMs(axis,center,x0+(x1-x0)*.5),sampleMs=chartAxisPixelToMs(axis,center+sample,centerMs+(x1-x0)*(sample/width));
  let shift=Number.isFinite(centerMs)&&Number.isFinite(sampleMs)&&sample!==0?(sampleMs-centerMs)*(pixels/sample):fallbackShift;
  if(!Number.isFinite(shift))shift=fallbackShift;
  return [new Date(x0+shift).toISOString(),new Date(x1+shift).toISOString()];
}
function median(arr){ const a=arr.filter(v=>Number.isFinite(v)).sort((x,y)=>x-y); return a.length?a[Math.floor(a.length/2)]:NaN; }
function clamp(v,lo,hi){ return Math.max(lo,Math.min(hi,v)); }
function normalizeChartXRange(range){
  if(!Array.isArray(range)||range.length!==2)return null;
  const x0=parseChartAxisMs(range[0]),x1=parseChartAxisMs(range[1]);
  if(!Number.isFinite(x0)||!Number.isFinite(x1)||x0===x1)return null;
  return [new Date(x0).toISOString(),new Date(x1).toISOString()];
}
function normalizeChartYRange(range){
  if(!Array.isArray(range)||range.length!==2)return null;
  const y0=Number(range[0]),y1=Number(range[1]);
  return Number.isFinite(y0)&&Number.isFinite(y1)&&y0!==y1?[y0,y1]:null;
}
function activeManualChartViewport(el=$('chart')){
  if(!chartManualViewport||!el)return null;
  const label=String(el.dataset?.label||chartRenderedLabel||selected?.label||'');
  return chartManualViewport.label===label&&chartManualViewport.timeframe===chartTimeframeKey?chartManualViewport:null;
}
function lockManualChartViewport(el=$('chart'),overrides={}){
  if(!el?._fullLayout)return null;
  const xRange=normalizeChartXRange(overrides.xRange||el._fullLayout.xaxis?.range);
  const yRange=normalizeChartYRange(overrides.yRange||el._fullLayout.yaxis?.range);
  if(!xRange&&!yRange)return null;
  chartManualViewport={
    label:String(el.dataset?.label||chartRenderedLabel||selected?.label||''),
    timeframe:chartTimeframeKey,
    xRange:xRange||activeManualChartViewport(el)?.xRange||null,
    yRange:yRange||activeManualChartViewport(el)?.yRange||null,
  };
  chartFollowLive=false;
  return chartManualViewport;
}
function beginManualChartNavigation(el=$('chart')){
  if(!el?._fullLayout)return null;
  chartUserRangeChangedAt=performance.now();
  chartViewportRevision++;
  chartFollowLive=false;
  return lockManualChartViewport(el);
}
function chartRangesMatch(a,b,isDate=false){
  if(!a&&!b)return true;
  if(!Array.isArray(a)||!Array.isArray(b)||a.length!==2||b.length!==2)return false;
  const av=isDate?a.map(parseChartAxisMs):a.map(Number),bv=isDate?b.map(parseChartAxisMs):b.map(Number);
  if(!av.every(Number.isFinite)||!bv.every(Number.isFinite))return false;
  const scale=Math.max(1,...av.map(Math.abs),...bv.map(Math.abs));
  return Math.abs(av[0]-bv[0])<=Math.max(isDate?1:1e-9,scale*1e-10)&&Math.abs(av[1]-bv[1])<=Math.max(isDate?1:1e-9,scale*1e-10);
}
function chartRelayoutRange(event,axis){
  const direct=event?.[`${axis}.range`];
  if(Array.isArray(direct)&&direct.length===2)return direct;
  const start=event?.[`${axis}.range[0]`],end=event?.[`${axis}.range[1]`];
  return start!==undefined&&end!==undefined?[start,end]:undefined;
}
function restoreManualChartViewport(el,snapshot,revision){
  const active=activeManualChartViewport(el);
  if(!window.Plotly||!el?._fullLayout||!snapshot||active!==snapshot||revision!==chartViewportRevision)return Promise.resolve();
  const update={};
  if(snapshot.xRange&&!chartRangesMatch(el._fullLayout.xaxis?.range,snapshot.xRange,true)){update['xaxis.autorange']=false;update['xaxis.range']=snapshot.xRange;}
  if(snapshot.yRange&&!chartRangesMatch(el._fullLayout.yaxis?.range,snapshot.yRange,false)){update['yaxis.autorange']=false;update['yaxis.range']=snapshot.yRange;}
  if(!Object.keys(update).length)return Promise.resolve();
  chartApplyingRange=true;
  return Promise.resolve(Plotly.relayout(el,update)).catch(()=>{}).finally(()=>setTimeout(()=>{chartApplyingRange=false;},0));
}
function cleanHistoryForChart(h){
  const byDate=new Map();
  (h||[]).forEach(d=>{
    const date=String(d?.date||'').slice(0,10); if(!date) return;
    const row={date,open:Number(d.open),high:Number(d.high),low:Number(d.low),close:Number(d.close),volume:Number(d.volume||0)};
    if([row.open,row.high,row.low,row.close].every(v=>Number.isFinite(v)&&v>0)) byDate.set(date,row);
  });
  let rows=[...byDate.values()].sort((a,b)=>a.date.localeCompare(b.date));
  if(!rows.length) return [];
  // Keep the latest validated closes. Do not remove real post-gap data; only repair impossible wicks.
  return rows.map((d,i)=>{
    const bodyHi=Math.max(d.open,d.close), bodyLo=Math.min(d.open,d.close);
    const prev=rows[i-1]?.close || d.close;
    const local=Math.max(bodyHi,bodyLo,prev);
    let high=Math.max(Number(d.high),bodyHi), low=Math.min(Number(d.low),bodyLo);
    high=clamp(high, bodyHi, Math.max(bodyHi*2.5, local*2.5));
    low=clamp(low, Math.max(0.000001, Math.min(bodyLo*0.22, local*0.22)), bodyLo);
    return {...d, high, low};
  });
}
function withLineBreaks(rows, valueKey='close'){
  const x=[], y=[];
  for(let i=0;i<rows.length;i++){
    if(i>0){
      const prev=parseDateMs(rows[i-1].date), cur=parseDateMs(rows[i].date);
      const dayGap=(prev&&cur)?(cur-prev)/86400000:0;
      const jump=Math.abs(Math.log(Number(rows[i][valueKey])/Number(rows[i-1][valueKey])));
      if(dayGap>9 || jump>0.75){ x.push(rows[i-1].date); y.push(null); }
    }
    x.push(rows[i].date); y.push(Number(rows[i][valueKey]));
  }
  return {x,y};
}
function yRangeForChart(h,f,startDate,endDate,includeForecast=true){
  const start=parseDateMs(startDate||''), end=parseDateMs(endDate||'');
  const hist=(h||[]).filter(d=>{
    const t=parseDateMs(d.date); return !start || !end || (t && t>=start && t<=end);
  });
  const pool=[];
  hist.forEach(d=>pool.push(Number(d.high),Number(d.low),Number(d.close),Number(d.open)));
  if(includeForecast)(f||[]).forEach(d=>pool.push(Number(d.high),Number(d.low),Number(d.price)));
  if(!pool.length)(h||[]).slice(-120).forEach(d=>pool.push(Number(d.high),Number(d.low),Number(d.close),Number(d.open)));
  const clean=pool.filter(v=>Number.isFinite(v)&&v>0).sort((a,b)=>a-b);
  if(clean.length<3) return undefined;
  const med=median(clean)||clean[Math.floor(clean.length/2)]||1;
  const trimmed=clean.filter(v=>v>med/12 && v<med*12);
  const arr=trimmed.length>=3?trimmed:clean;
  let lo=arr[0], hi=arr[arr.length-1];
  if(includeForecast)(f||[]).forEach(d=>{ [d.low,d.high,d.price].forEach(v=>{v=Number(v); if(Number.isFinite(v)&&v>0){lo=Math.min(lo,v); hi=Math.max(hi,v);}}); });
  const pad=Math.max((hi-lo)*.10, hi*.004);
  return [Math.max(0,lo-pad), hi+pad];
}
function rangeStartFor(h,lastDate){
  if(!h.length) return undefined;
  const last=parseDateMs(lastDate||h.at(-1).date); if(!last) return h[0].date;
  const threeYears=last-365*3*86400000;
  const firstAvailable=parseDateMs(h[0].date)||threeYears;
  return new Date(Math.max(firstAvailable, threeYears)).toISOString().slice(0,10);
}
function chartDisplayHistory(rows,maxBars=CHART_MAX_BARS){
  rows=rows||[];maxBars=Math.max(120,Number(maxBars)||CHART_MAX_BARS);
  if(rows.length<=maxBars) return rows;
  const recentKeep=Math.min(900,Math.floor(maxBars*.38)),recent=rows.slice(-recentKeep),older=rows.slice(0,-recentKeep),targetOlder=Math.max(60,maxBars-recentKeep),step=Math.max(1,Math.ceil(older.length/targetOlder)),compressed=[];
  for(let index=0;index<older.length;index+=step){const row=aggregateChartDisplayChunk(older.slice(index,Math.min(older.length,index+step)));if(row)compressed.push(row);}
  return compressed.concat(recent);
}
function isBtcChart(r){return String(r?.label||'').toUpperCase()==='BTCUSD';}
function aggregateChartDisplayChunk(chunk){
  if(!chunk?.length)return null;
  const first=chunk[0],last=chunk.at(-1);
  return {
    ...first,
    open:Number(first.open),
    high:Math.max(...chunk.map(row=>Number(row.high))),
    low:Math.min(...chunk.map(row=>Number(row.low))),
    close:Number(last.close),
    volume:chunk.reduce((sum,row)=>sum+Math.max(0,Number(row.volume||0)),0),
    _live:chunk.some(row=>!!row._live),
    _display_aggregate:chunk.length,
  };
}
function chartSessionGroups(rows,spec=chartTimeframeSpec()){
  const ordered=rows||[];if(!ordered.length)return [];
  const groups=[];let start=0;
  for(let index=1;index<ordered.length;index++){
    if(chartGapInfo(ordered,index,spec)){groups.push(ordered.slice(start,index));start=index;}
  }
  groups.push(ordered.slice(start));return groups.filter(group=>group.length);
}
function chartAggregateSessionRows(rows,target){
  const source=rows||[],count=Math.max(1,Math.min(source.length,Math.floor(Number(target)||1)));
  if(source.length<=count)return source;
  const output=[];
  for(let index=0;index<count;index++){
    const start=Math.floor(index*source.length/count),end=Math.max(start+1,Math.floor((index+1)*source.length/count)),row=aggregateChartDisplayChunk(source.slice(start,Math.min(source.length,end)));
    if(row)output.push(row);
  }
  return output;
}
function chartSessionAwareRenderRows(rows,maxBars,spec=chartTimeframeSpec()){
  const source=rows||[],limit=Math.max(1,Math.floor(Number(maxBars)||CHART_MAX_BARS));
  if(source.length<=limit)return source;
  const groups=chartSessionGroups(source,spec);if(groups.length<=1)return chartAggregateSessionRows(source,limit);
  if(groups.length>=limit){
    const selected=[];
    for(let index=0;index<limit;index++){
      const groupIndex=limit===1?groups.length-1:Math.round(index*(groups.length-1)/(limit-1)),row=chartAggregateSessionRows(groups[groupIndex],1)[0];
      if(row)selected.push(row);
    }
    return selected;
  }
  const budgets=groups.map(group=>Math.min(group.length,group.length>1?2:1));let remaining=Math.max(0,limit-budgets.reduce((sum,value)=>sum+value,0));
  const capacities=groups.map((group,index)=>Math.max(0,group.length-budgets[index])),capacityTotal=capacities.reduce((sum,value)=>sum+value,0);
  if(remaining>0&&capacityTotal>0){
    const fractions=[];let assigned=0;
    capacities.forEach((capacity,index)=>{const raw=remaining*capacity/capacityTotal,extra=Math.min(capacity,Math.floor(raw));budgets[index]+=extra;assigned+=extra;fractions.push({index,fraction:raw-Math.floor(raw)});});
    remaining-=assigned;fractions.sort((a,b)=>b.fraction-a.fraction||a.index-b.index);
    for(const item of fractions){if(remaining<=0)break;if(budgets[item.index]<groups[item.index].length){budgets[item.index]++;remaining--;}}
  }
  return groups.flatMap((group,index)=>chartAggregateSessionRows(group,budgets[index]));
}
function chartRowsForRender(r,rows,spec=chartTimeframeSpec()){
  rows=rows||[];
  const maxBars=isBtcChart(r)?BTC_CHART_MAX_BARS:CHART_MAX_BARS;
  return chartSessionAwareRenderRows(rows,maxBars,spec);
}
function chartRowsForViewport(r,rows,range=null,spec=chartTimeframeSpec()){
  rows=rows||[];const maxBars=isBtcChart(r)?BTC_CHART_MAX_BARS:CHART_MAX_BARS;
  if(rows.length<=maxBars)return rows;
  const x0=parseChartAxisMs(range?.[0]),x1=parseChartAxisMs(range?.[1]);
  if(!Number.isFinite(x0)||!Number.isFinite(x1)||x0===x1)return chartRowsForRender(r,rows,spec);
  const lowMs=Math.min(x0,x1),highMs=Math.max(x0,x1),firstAtOrAfter=value=>{let low=0,high=rows.length;while(low<high){const middle=(low+high)>>1;if(chartRowDisplayMs(rows[middle])<value)low=middle+1;else high=middle;}return low;};
  const visibleStart=clamp(firstAtOrAfter(lowMs),0,rows.length-1),visibleEnd=clamp(firstAtOrAfter(highMs)+1,visibleStart+1,rows.length),visibleCount=Math.max(1,visibleEnd-visibleStart);
  if(visibleCount>Math.floor(maxBars*.72)){
    const overscan=Math.max(120,Math.min(720,Math.ceil(visibleCount*.12))),start=Math.max(0,visibleStart-overscan),end=Math.min(rows.length,visibleEnd+overscan);
    return chartSessionAwareRenderRows(rows.slice(start,end),maxBars,spec);
  }
  const overscan=Math.max(180,Math.min(900,Math.ceil(visibleCount*.75))),room=Math.max(0,maxBars-visibleCount),before=Math.min(visibleStart,Math.min(overscan,Math.floor(room*.5))),after=Math.min(rows.length-visibleEnd,Math.min(overscan,room-before));
  let start=visibleStart-before,end=visibleEnd+after;
  if(end-start<Math.min(maxBars,visibleCount+overscan*2)){
    const missing=Math.min(maxBars,visibleCount+overscan*2)-(end-start),extraBefore=Math.min(start,missing);start-=extraBefore;end=Math.min(rows.length,end+missing-extraBefore);
  }
  return rows.slice(start,end);
}
function chartCandlePaintInterval(r){return isBtcChart(r)?BTC_CANDLE_PAINT_MS:LIVE_CANDLE_PAINT_MS;}
function aggregateOhlcv(rows,bucketFor){
  const buckets=new Map();
  for(const raw of (rows||[])){
    const ts=Number(raw?.timestamp??parseDateMs(raw?.date)), o=Number(raw?.open), h=Number(raw?.high), l=Number(raw?.low), c=Number(raw?.close);
    if(!Number.isFinite(ts)||![o,h,l,c].every(v=>Number.isFinite(v)&&v>0))continue;
    const bucket=bucketFor(raw,ts);
    if(!bucket||bucket.key===undefined)continue;
    const high=Math.max(o,h,l,c),low=Math.min(o,h,l,c),existing=buckets.get(bucket.key);
    if(!existing){
      buckets.set(bucket.key,{date:bucket.date||new Date(bucket.timestamp).toISOString(),timestamp:Number(bucket.timestamp??ts),open:o,high,low,close:c,volume:Math.max(0,Number(raw.volume||0)),_live:!!raw._live});
    }else{
      existing.high=Math.max(existing.high,high);existing.low=Math.min(existing.low,low);existing.close=c;existing.volume+=Math.max(0,Number(raw.volume||0));existing._live=existing._live||!!raw._live;
    }
  }
  return [...buckets.values()].sort((a,b)=>a.timestamp-b.timestamp);
}
function aggregateFixedBars(rows,bucketMs){
  const clean=cleanMinuteBars(rows,0),anchors=new Map();
  for(const row of clean){const day=new Date(row.timestamp).toISOString().slice(0,10);if(!anchors.has(day))anchors.set(day,row.timestamp);}
  return aggregateOhlcv(clean,(row,ts)=>{
    const day=new Date(ts).toISOString().slice(0,10),anchor=anchors.get(day)??ts,bucketTs=anchor+Math.floor(Math.max(0,ts-anchor)/bucketMs)*bucketMs;
    return {key:bucketTs,timestamp:bucketTs,date:new Date(bucketTs).toISOString()};
  });
}
function aggregateLiveTicks(ticks,bucketMs){
  return aggregateOhlcv((ticks||[]).map(t=>({timestamp:Number(t.timestamp),date:t.date,open:Number(t.price),high:Number(t.price),low:Number(t.price),close:Number(t.price),volume:0,_live:true})),(_,ts)=>{
    const bucketTs=Math.floor(ts/bucketMs)*bucketMs;return {key:bucketTs,timestamp:bucketTs,date:new Date(bucketTs).toISOString()};
  });
}
function aggregateCalendarBars(rows,calendar='day',months=0){
  return aggregateOhlcv(rows,(row,ts)=>{
    const d=new Date(ts);d.setUTCHours(0,0,0,0);
    if(calendar==='week')d.setUTCDate(d.getUTCDate()-((d.getUTCDay()+6)%7));
    if(calendar==='month'){
      const width=Math.max(1,Number(months||1)),absolute=d.getUTCFullYear()*12+d.getUTCMonth(),bucketMonth=Math.floor(absolute/width)*width;
      d.setUTCFullYear(Math.floor(bucketMonth/12),bucketMonth%12,1);
    }
    const bucketTs=d.getTime();return {key:bucketTs,timestamp:bucketTs,date:d.toISOString().slice(0,10)};
  });
}
function mergeOhlcRows(baseRows,overlayRows){
  const map=new Map((baseRows||[]).map(r=>[Number(r.timestamp??parseDateMs(r.date)),{...r}]));
  for(const row of (overlayRows||[])){
    const key=Number(row.timestamp??parseDateMs(row.date)),existing=map.get(key);
    if(!Number.isFinite(key))continue;
    if(!existing){map.set(key,{...row});continue;}
    map.set(key,{...existing,high:Math.max(Number(existing.high),Number(row.high)),low:Math.min(Number(existing.low),Number(row.low)),close:Number(row.close),volume:Math.max(Number(existing.volume||0),Number(row.volume||0)),_live:existing._live||row._live});
  }
  return [...map.values()].sort((a,b)=>Number(a.timestamp??parseDateMs(a.date))-Number(b.timestamp??parseDateMs(b.date)));
}
function dailyHistoryWithLive(r,history){
  const daily=(history||[]).map(row=>({...row,timestamp:parseDateMs(row.date)})),minute=cleanMinuteBars(r?._intraday_rows||[]);
  if(!minute.length)return daily;
  const latestDay=new Date(minute.at(-1).timestamp).toISOString().slice(0,10),currentSession=minute.filter(row=>new Date(row.timestamp).toISOString().slice(0,10)===latestDay);
  const liveDays=aggregateOhlcv(currentSession,(row,ts)=>{const date=new Date(ts).toISOString().slice(0,10),bucketTs=parseDateMs(date);return {key:bucketTs,timestamp:bucketTs,date};});
  return mergeOhlcRows(daily,liveDays);
}
function oneYearContextRows(r,daily,anchorTs){
  const contextSpec={interval:'1h',range:'1y'},cached=adaptiveSeriesCache.get(adaptiveFetchKey(r,contextSpec));
  let rows=cached?.bars?.length?cleanMinuteBars(cached.bars):(daily||[]);
  const anchor=Number(anchorTs||rows.at(-1)?.timestamp||parseDateMs(rows.at(-1)?.date)||Date.now()),cutoff=anchor-366*86400000;
  rows=rows.filter(row=>Number(row.timestamp??parseDateMs(row.date))>=cutoff);
  return {rows,source:cached?.meta?.source||'daily context while 1h history loads',hourly:!!cached?.bars?.length};
}
function prependHistoricalContext(fineRows,contextRows){
  const fine=[...(fineRows||[])].sort((a,b)=>Number(a.timestamp)-Number(b.timestamp));
  if(!fine.length)return [...(contextRows||[])];
  const firstFine=Number(fine[0].timestamp??parseDateMs(fine[0].date));
  return [...(contextRows||[]).filter(row=>Number(row.timestamp??parseDateMs(row.date))<firstFine),...fine];
}
function mergeFineWithContext(fineRows,contextRows,coverageIntervals=[]){
  const intervals=chartArchiveIntervals((coverageIntervals||[]).map(interval=>({start:Number(interval?.[0]),end:Number(interval?.[1])})));
  if(!intervals.length)return prependHistoricalContext(fineRows,contextRows);
  const context=(contextRows||[]).filter(row=>{
    const timestamp=Number(row?.timestamp??parseDateMs(row?.date));
    return Number.isFinite(timestamp)&&!intervals.some(interval=>timestamp>=interval[0]&&timestamp<=interval[1]);
  });
  return mergeOhlcRows(context,fineRows);
}
function chartMinuteCoverageIntervals(minuteRows,archiveIntervals=[]){
  const intervals=[...(archiveIntervals||[])],minute=minuteRows||[];
  if(minute.length)intervals.push([Number(minute[0].timestamp),Number.MAX_SAFE_INTEGER]);
  return chartArchiveIntervals(intervals.map(interval=>({start:Number(interval?.[0]),end:Number(interval?.[1])})));
}
function chartCurrentDisplayRange(r){
  const el=$('chart'),label=String(r?.label||'');
  if(!el?._fullLayout||chartRenderedLabel!==label||el?._apexRangeMeta?.timeframe!=='1m')return null;
  return activeManualChartViewport(el)?.xRange||el._fullLayout.xaxis?.range||null;
}
function chartAllHistoryRows(dailyRows,fineRows){
  const fine=[...(fineRows||[])].sort((a,b)=>Number(a.timestamp??parseDateMs(a.date))-Number(b.timestamp??parseDateMs(b.date)));
  if(!fine.length)return chartDisplayHistory(dailyRows||[]);
  const recentBudget=Math.min(1800,Math.floor(CHART_MAX_BARS*.5)),recent=fine.slice(-recentBudget),firstTimestamp=Number(recent[0]?.timestamp??parseDateMs(recent[0]?.date)),firstDay=Number.isFinite(firstTimestamp)?new Date(firstTimestamp).toISOString().slice(0,10):'';
  const older=(dailyRows||[]).filter(row=>!firstDay||String(row?.date||'').slice(0,10)<firstDay),contextBudget=Math.max(120,CHART_MAX_BARS-recent.length);
  return [...chartDisplayHistory(older,contextBudget),...recent];
}
function chartSeriesForTimeframe(r,history){
  const spec=chartTimeframeSpec(),minute=cleanMinuteBars(r?._intraday_rows||[]),daily=dailyHistoryWithLive(r,history),fetchSpec=spec.fetch||null,archive=chartTimeframeKey==='1m'?chartArchiveSnapshot(r,chartCurrentDisplayRange(r)):{rows:[],intervals:[]},exactMinute=archive.rows.length?mergeOhlcRows(archive.rows,minute):minute,exactIntervals=chartMinuteCoverageIntervals(minute,archive.intervals);
  if(spec.kind==='seconds'){
    const allLive=aggregateLiveTicks(r?._live_ticks||[],spec.bucketMs),live=spec.bucketMs<=1000?allLive.slice(-12000):allLive,firstLive=Number(live[0]?.timestamp||Infinity),referenceTs=Number(live.at(-1)?.timestamp||minute.at(-1)?.timestamp||Date.now()),referenceDay=new Date(referenceTs).toISOString().slice(0,10);
    const historyRows=minute.filter(row=>new Date(row.timestamp).toISOString().slice(0,10)===referenceDay&&row.timestamp<Math.floor(firstLive/60000)*60000),fine=[...historyRows,...live].sort((a,b)=>a.timestamp-b.timestamp),allHistory=chartHistoryRangeKey==='all',context=allHistory?null:oneYearContextRows(r,daily,referenceTs),rows=allHistory?chartAllHistoryRows(daily,fine):prependHistoricalContext(fine,context.rows);
    return {spec,rows,timed:true,label:allHistory?`${spec.label} recent · daily context since introduction`:(live.length?`${spec.label} live ticks · 1h context over 1 year`:`1m recent · ${context.hourly?'1h':'daily'} context over 1 year`)};
  }
  if(spec.kind==='intraday'){
    const adaptiveMeta=r?._adaptive_meta||{},adaptiveMatches=fetchSpec&&adaptiveMeta.interval===fetchSpec.interval&&adaptiveMeta.range===fetchSpec.range;
    const source=fetchSpec?.interval==='1m'?exactMinute:(adaptiveMatches?cleanMinuteBars(r?._adaptive_rows||[]):minute);
    let rows=aggregateFixedBars(source,spec.bucketMs);
    if(fetchSpec?.interval!=='1m')rows=mergeOhlcRows(rows,aggregateFixedBars(minute.filter(x=>x._live),spec.bucketMs));
    if(chartHistoryRangeKey==='all'){
      rows=archive.rows.length?mergeFineWithContext(rows,chartDisplayHistory(daily,Math.max(1200,Math.floor(CHART_MAX_BARS*.55))),exactIntervals):chartAllHistoryRows(daily,rows);
      if(rows.length)return {spec,rows,timed:true,label:archive.rows.length?`${spec.label} exact on zoom · daily context since introduction`:`${spec.label} recent · daily context since introduction`};
    }
    if(chartHistoryContextSpec(spec)){
      const context=oneYearContextRows(r,daily,rows.at(-1)?.timestamp||minute.at(-1)?.timestamp);rows=mergeFineWithContext(rows,context.rows,exactIntervals);
      if(rows.length)return {spec,rows,timed:true,label:archive.rows.length?`${spec.label} exact on zoom · ${context.hourly?'1h':'daily'} context over 1 year`:`${spec.label} recent · ${context.hourly?'1h':'daily'} context over 1 year`};
    }
    if(rows.length)return {spec,rows,timed:true,label:`${spec.label} · ${adaptiveMatches?adaptiveMeta.source||'market history':'temporary 1m fallback'}`};
  }
  if(spec.kind==='calendar')return {spec,rows:aggregateCalendarBars(daily,spec.calendar,spec.months),timed:false,label:`${spec.label} calendar OHLC`};
  if(spec.kind==='range')return {spec,rows:chartDisplayHistory(daily),timed:false,label:`${spec.label} range · daily OHLC`};
  return {spec,rows:chartDisplayHistory(daily),timed:false,label:'daily fallback'};
}
const CHART_DISPLAY_MODES=[
  {key:'bars',label:'Barres'},
  {key:'candles',label:'Bougies'},
  {key:'hollow_candles',label:'Bougies creuses'},
  {key:'volume_candles',label:'Bougies de volume'},
  {key:'line',label:'Ligne'},
  {key:'line_markers',label:'Ligne avec marqueurs'},
  {key:'step_line',label:'Ligne en escalier'},
  {key:'area',label:'Région'},
  {key:'hlc_area',label:'Zone HLC'},
  {key:'baseline',label:'Ligne de base'},
  {key:'columns',label:'Colonne'},
  {key:'high_low',label:'Haut bas'},
  {key:'volume_footprint',label:'Empreinte de volume'},
];
function chartDisplaySpec(key){return CHART_DISPLAY_MODES.find(x=>x.key===key)||CHART_DISPLAY_MODES[1];}
function chartStyleIcon(key){
  const stroke='currentColor', base=`fill="none" stroke="${stroke}" stroke-width="1.4" stroke-linecap="round" stroke-linejoin="round"`;
  let body='';
  if(key==='bars')body=`<path ${base} d="M4 3v12M4 7H1M4 11h3M13 2v13m0-9h-3m3 5h3"/>`;
  else if(key==='candles'||key==='hollow_candles'||key==='volume_candles')body=`<path ${base} d="M5 2v14M12 1v15"/><rect x="2.5" y="5" width="5" height="6" rx=".5" ${key==='hollow_candles'?base:`fill="${stroke}" stroke="${stroke}"`}/><rect x="9.5" y="4" width="5" height="8" rx=".5" ${base}/>${key==='volume_candles'?`<path ${base} d="M2 16v-2m3 2v-4m3 4v-3m3 3v-5m3 5v-2"/>`:''}`;
  else if(key==='columns')body=`<path ${base} d="M2 16V9h3v7M7 16V5h3v11m2 0V2h3v14"/>`;
  else if(key==='high_low')body=`<path ${base} d="M4 3v11m-2 0h4M12 1v12m-2-12h4"/>`;
  else if(key==='volume_footprint')body=`<circle cx="5" cy="11" r="2" ${base}/><circle cx="10" cy="7" r="3" ${base}/><circle cx="15" cy="3" r="1.5" ${base}/><path ${base} d="M2 16h14"/>`;
  else if(key==='hlc_area')body=`<path ${base} d="M1 7l4-3 4 2 4-4 4 3M1 13l4-2 4 2 4-3 4 1"/><path fill="currentColor" opacity=".18" d="M1 7l4-3 4 2 4-4 4 3v6l-4-1-4 3-4-2-4 2z"/>`;
  else if(key==='area'||key==='baseline')body=`<path ${base} d="M1 13l4-5 4 2 4-7 4 3"/>${key==='baseline'?`<path ${base} stroke-dasharray="2 2" d="M1 10h16"/>`:`<path fill="currentColor" opacity=".2" d="M1 13l4-5 4 2 4-7 4 3v10H1z"/>`}`;
  else if(key==='step_line')body=`<path ${base} d="M1 13h4V9h4V5h4V2h4"/>`;
  else body=`<path ${base} d="M1 13l4-5 4 2 4-7 4 3"/>${key==='line_markers'?`<circle cx="1" cy="13" r="1.2" fill="${stroke}"/><circle cx="5" cy="8" r="1.2" fill="${stroke}"/><circle cx="9" cy="10" r="1.2" fill="${stroke}"/><circle cx="13" cy="3" r="1.2" fill="${stroke}"/>`:''}`;
  return `<svg viewBox="0 0 18 18" aria-hidden="true">${body}</svg>`;
}
function crosshairWidthValue(value,fallback=.55){const raw=String(value??'').trim(),numeric=raw===''?NaN:Number(raw);return Number.isFinite(numeric)?clamp(numeric,.5,3):fallback;}
function crosshairWidthLabel(value){return `${Number(value).toFixed(2).replace(/\.?0+$/,'')} px`;}
function closeChartCrosshairWidthMenu(){const menu=$('chartCrosshairWidthMenu'),button=$('chartCrosshairWidthBtn');menu?.classList.remove('open');button?.setAttribute('aria-expanded','false');}
function setChartCrosshairWidth(value,persist=true){
  chartCrosshairWidth=crosshairWidthValue(value,chartCrosshairWidth);
  $('chartStage')?.style.setProperty('--chart-crosshair-width',String(chartCrosshairWidth));
  const input=$('chartCrosshairWidthInput'),output=$('chartCrosshairWidthOutput'),buttonValue=$('chartCrosshairWidthValue'),label=crosshairWidthLabel(chartCrosshairWidth);
  if(input){input.value=String(chartCrosshairWidth);input.setAttribute('aria-valuetext',label);}
  if(output)output.textContent=label;if(buttonValue)buttonValue.textContent=label.replace(' px','');
  if(persist){try{localStorage.setItem(CROSSHAIR_WIDTH_STORAGE_KEY,String(chartCrosshairWidth));}catch(error){}}
  $('chartStage')?._apexCrosshairRepaint?.();
}
function initChartCrosshairWidthControl(){
  const toolbar=$('chartRangeToolbar');if(!toolbar||$('chartCrosshairWidthControl'))return;
  const control=document.createElement('div');control.id='chartCrosshairWidthControl';control.className='chartCrosshairWidthControl';
  control.innerHTML=`<button class="chartCrosshairWidthBtn" id="chartCrosshairWidthBtn" type="button" aria-haspopup="true" aria-expanded="false" aria-controls="chartCrosshairWidthMenu" title="Crosshair thickness"><svg class="lucide lucide-crosshair" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" aria-hidden="true"><circle cx="12" cy="12" r="8"/><path d="M22 12h-4M6 12H2M12 6V2M12 22v-4"/></svg><span id="chartCrosshairWidthValue">0.55</span></button><div class="chartCrosshairWidthMenu" id="chartCrosshairWidthMenu" role="group" aria-label="Crosshair thickness"><label for="chartCrosshairWidthInput"><span>Crosshair thickness</span><output id="chartCrosshairWidthOutput">0.55 px</output></label><input id="chartCrosshairWidthInput" type="range" min="0.5" max="3" step="0.05" value="0.55" aria-label="Crosshair thickness"></div>`;
  toolbar.appendChild(control);
  const button=$('chartCrosshairWidthBtn'),menu=$('chartCrosshairWidthMenu'),input=$('chartCrosshairWidthInput');
  button.onclick=event=>{event.stopPropagation();$('chartTimeframeMenu')?.classList.remove('open');$('chartTimeframeBtn')?.setAttribute('aria-expanded','false');$('chartStyleMenu')?.classList.remove('open');$('chartStyleBtn')?.setAttribute('aria-expanded','false');const open=menu.classList.toggle('open');button.setAttribute('aria-expanded',String(open));};
  input.addEventListener('input',event=>setChartCrosshairWidth(event.target.value));
  control.addEventListener('click',event=>event.stopPropagation());
  document.addEventListener('click',event=>{if(!control.contains(event.target))closeChartCrosshairWidthMenu();});
  document.addEventListener('keydown',event=>{if(event.key==='Escape')closeChartCrosshairWidthMenu();});
  window.addEventListener('storage',event=>{if(event.key===CROSSHAIR_WIDTH_STORAGE_KEY)setChartCrosshairWidth(event.newValue,false);});
  let saved=null;try{saved=localStorage.getItem(CROSSHAIR_WIDTH_STORAGE_KEY);}catch(error){}setChartCrosshairWidth(saved,false);
}
function updateChartToolbarState(){
  const timeframe=chartTimeframeSpec(),timeframeButton=$('chartTimeframeBtn'),timeframeValue=$('chartTimeframeValue');
  if(timeframeValue)timeframeValue.textContent=timeframe.label;
  if(timeframeButton){timeframeButton.title=`Chart interval: ${timeframe.name}`;timeframeButton.setAttribute('aria-label',timeframeButton.title);}
  document.querySelectorAll('#chartTimeframeMenu .chartTimeframeOption').forEach(btn=>btn.classList.toggle('active',btn.dataset.timeframe===chartTimeframeKey));
  const spec=chartDisplaySpec(chartDisplayMode), button=$('chartStyleBtn');
  if(button){button.innerHTML=chartStyleIcon(spec.key);button.title=`Chart display: ${spec.label}`;button.setAttribute('aria-label',button.title);}
  document.querySelectorAll('#chartStyleMenu .chartStyleOption').forEach(btn=>btn.classList.toggle('active',btn.dataset.mode===chartDisplayMode));
  document.querySelectorAll('#chartHistoryToolbar .chartHistoryBtn').forEach(btn=>{
    const active=btn.dataset.historyRange===chartHistoryRangeKey;
    btn.classList.toggle('active',active);btn.setAttribute('aria-pressed',String(active));
  });
}
function updateChartForecastToggle(){
  const button=$('chartForecastToggle');
  if(!button)return;
  button.classList.toggle('off',!chartForecastVisible);
  button.setAttribute('aria-pressed',String(chartForecastVisible));
  button.title=chartForecastVisible?'Hide forecast':'Show forecast';
  button.setAttribute('aria-label',button.title);
}
function setChartForecastVisible(visible){
  chartForecastVisible=!!visible;
  updateChartForecastToggle();
  const el=$('chart'), indices=el?._apexForecastTraceIndices||[];
  if(chartFullRenderInFlight)return;
  if(!window.Plotly||!el||!indices.length)return;
  const lockedViewport=activeManualChartViewport(el),lockedRevision=chartViewportRevision;
  const shapes=chartForecastVisible?[...(el._apexBaseShapes||[]),...(el._apexForecastShapes||[])]:[...(el._apexBaseShapes||[])];
  const annotations=chartForecastVisible?[...(el._apexForecastAnnotations||[])]:[];
  Promise.all([
    Promise.resolve(Plotly.restyle(el,{visible:chartForecastVisible},indices)),
    Promise.resolve(Plotly.relayout(el,{shapes,annotations})),
  ]).then(()=>restoreManualChartViewport(el,lockedViewport,lockedRevision)).catch(()=>{});
}
function initChartForecastToggle(){
  const button=$('chartForecastToggle');
  if(!button||button.dataset.ready==='1')return;
  button.onclick=()=>setChartForecastVisible(!chartForecastVisible);
  button.dataset.ready='1';
  updateChartForecastToggle();
}
function setChartDisplayMode(mode){
  if(!CHART_DISPLAY_MODES.some(x=>x.key===mode))return;
  chartDisplayMode=mode;$('chartStyleMenu')?.classList.remove('open');$('chartStyleBtn')?.setAttribute('aria-expanded','false');updateChartToolbarState();
  if(selected)renderChart(selected);
}
function setChartTimeframe(key){
  if(!CHART_TIMEFRAMES.some(x=>x.key===key))return;
  stopChartArchiveRequests();
  chartTimeframeKey=key;chartFollowLive=true;chartManualViewport=null;chartViewportRevision++;
  $('chartTimeframeMenu')?.classList.remove('open');$('chartTimeframeBtn')?.setAttribute('aria-expanded','false');updateChartToolbarState();
  if(!selected)return;
  activateAdaptiveSeries(selected,key);
  if(key==='all')requestFullChartHistory(selected);
  const lab=String(selected.label||'').toUpperCase();
  Promise.resolve(renderChart(selected,{preserveViewport:false})).finally(()=>{
    if(selected&&String(selected.label||'').toUpperCase()===lab&&chartTimeframeKey===key){requestChartTimeframeSeries(selected,key);scheduleChartArchiveForViewport(selected);}
  });
}
function setChartHistoryRange(key){
  if(!CHART_HISTORY_RANGES.some(x=>x.key===key))return;
  chartHistoryRangeKey=key;chartFollowLive=true;chartManualViewport=null;chartUserRangeChangedAt=0;chartViewportRevision++;updateChartToolbarState();
  if(key==='all'&&selected)requestFullChartHistory(selected);
  if(chartFullRenderInFlight){if(selected)queueLatestChartRender(selected,{preserveViewport:false});return;}
  const el=$('chart'),rows=el?._apexRangeMeta?.candleRows||[];
  if(!window.Plotly||!el?._fullLayout||!rows.length)return;
  const viewport=chartHistoryViewport(rows,key,chartTimeframeSpec()),update={'xaxis.autorange':false,'xaxis.range':viewport.range};
  if(viewport.yr){update['yaxis.autorange']=false;update['yaxis.range']=viewport.yr;}
  chartApplyingRange=true;
  Promise.resolve(Plotly.relayout(el,update)).catch(()=>{}).finally(()=>{
    chartApplyingRange=false;$('chartStage')?._apexCrosshairRangeInvalidate?.();$('chartStage')?._apexCrosshairRepaint?.();updateChartLastPrice(selected);
    if(selected&&el?._apexRangeMeta?.seriesTimed){paintIntradayTrace(selected,true);scheduleChartArchiveForViewport(selected,el._fullLayout?.xaxis?.range);}
  });
}
function initChartToolbar(){
  const stage=$('chartStage');let historyToolbar=$('chartHistoryToolbar');
  if(!historyToolbar&&stage){historyToolbar=document.createElement('div');historyToolbar.id='chartHistoryToolbar';historyToolbar.className='chartHistoryToolbar';historyToolbar.setAttribute('role','group');historyToolbar.setAttribute('aria-label','Displayed market history');stage.appendChild(historyToolbar);}
  const toolbar=$('chartRangeToolbar'),timeframeMenu=$('chartTimeframeMenu'),timeframeButton=$('chartTimeframeBtn'),menu=$('chartStyleMenu'),button=$('chartStyleBtn');
  if(!toolbar||!timeframeMenu||!timeframeButton||!menu||!button||!historyToolbar||toolbar.dataset.ready==='1')return;
  const groups=[...new Set(CHART_TIMEFRAMES.map(x=>x.group))];
  timeframeMenu.innerHTML=groups.map(group=>`<div class="chartTimeframeGroup"><span class="chartTimeframeGroupTitle">${esc(group)}</span><div class="chartTimeframeOptions">${CHART_TIMEFRAMES.filter(x=>x.group===group).map(x=>`<button class="chartTimeframeOption" type="button" role="menuitem" data-timeframe="${x.key}" title="${esc(x.name)}">${esc(x.label)}</button>`).join('')}</div></div>`).join('');
  menu.innerHTML=CHART_DISPLAY_MODES.map(x=>`<button class="chartStyleOption" type="button" role="menuitem" data-mode="${x.key}">${chartStyleIcon(x.key)}<span>${esc(x.label)}</span></button>`).join('');
  historyToolbar.innerHTML=CHART_HISTORY_RANGES.map(x=>`<button class="chartHistoryBtn" type="button" data-history-range="${x.key}" aria-pressed="false" title="Display ${x.label} of market history">${x.label}</button>`).join('');
  timeframeMenu.querySelectorAll('.chartTimeframeOption').forEach(x=>x.onclick=e=>{e.stopPropagation();setChartTimeframe(x.dataset.timeframe);});
  menu.querySelectorAll('.chartStyleOption').forEach(x=>x.onclick=e=>{e.stopPropagation();setChartDisplayMode(x.dataset.mode);});
  historyToolbar.querySelectorAll('.chartHistoryBtn').forEach(x=>x.onclick=e=>{e.stopPropagation();setChartHistoryRange(x.dataset.historyRange);});
  timeframeButton.onclick=e=>{e.stopPropagation();closeChartCrosshairWidthMenu();menu.classList.remove('open');button.setAttribute('aria-expanded','false');const open=timeframeMenu.classList.toggle('open');timeframeButton.setAttribute('aria-expanded',String(open));};
  button.onclick=e=>{e.stopPropagation();closeChartCrosshairWidthMenu();timeframeMenu.classList.remove('open');timeframeButton.setAttribute('aria-expanded','false');const open=menu.classList.toggle('open');button.setAttribute('aria-expanded',String(open));};
  document.addEventListener('click',e=>{if(!toolbar.contains(e.target)){timeframeMenu.classList.remove('open');timeframeButton.setAttribute('aria-expanded','false');menu.classList.remove('open');button.setAttribute('aria-expanded','false');}});
  toolbar.dataset.ready='1';updateChartToolbarState();
}
function chartShiftDate(dateStr, amount, unit){
  const d=new Date(String(dateStr||'').slice(0,10)+'T00:00:00Z');
  if(!Number.isFinite(d.getTime())) return '';
  if(unit==='month') d.setUTCMonth(d.getUTCMonth()-amount);
  else if(unit==='year') d.setUTCFullYear(d.getUTCFullYear()-amount);
  else d.setUTCDate(d.getUTCDate()-amount);
  return d.toISOString().slice(0,10);
}
function chartRowDisplayMs(row){return Number(row?.timestamp??parseDateMs(row?.date));}
function chartRowSourceMs(row){return Number(row?._source_timestamp??row?.timestamp??parseDateMs(row?.date));}
function chartFallbackCadenceMs(spec=chartTimeframeSpec()){
  if(Number(spec?.bucketMs)>0)return Number(spec.bucketMs);
  if(spec?.kind==='calendar'&&spec.calendar==='week')return 7*86400000;
  if(spec?.kind==='calendar'&&spec.calendar==='month')return Math.max(1,Number(spec.months||1))*30*86400000;
  return 86400000;
}
function chartGapInfo(rows,index,spec=chartTimeframeSpec()){
  const previous=chartRowDisplayMs(rows?.[index-1]),current=chartRowDisplayMs(rows?.[index]);
  if(!Number.isFinite(previous)||!Number.isFinite(current)||current<=previous)return null;
  const before=index>1?previous-chartRowDisplayMs(rows[index-2]):NaN,after=index+1<rows.length?chartRowDisplayMs(rows[index+1])-current:NaN;
  const neighbors=[before,after].filter(v=>Number.isFinite(v)&&v>0),expected=neighbors.length?Math.min(...neighbors):chartFallbackCadenceMs(spec),gap=current-previous;
  if(!Number.isFinite(expected)||expected<=0||gap<=Math.max(expected*1.45,expected+1000))return null;
  const start=previous+expected,duration=current-start;
  return duration>=1000?{start,duration,expected,gap}:null;
}
function chartSessionRangeBreaks(rows,spec=chartTimeframeSpec()){
  const ordered=(rows||[]).filter(row=>Number.isFinite(chartRowDisplayMs(row))),groups=new Map();
  if(ordered.some((row,index)=>index>0&&chartRowDisplayMs(row)<chartRowDisplayMs(ordered[index-1])))ordered.sort((a,b)=>chartRowDisplayMs(a)-chartRowDisplayMs(b));
  for(let index=1;index<ordered.length;index++){
    const gap=chartGapInfo(ordered,index,spec);if(!gap)continue;
    const dvalue=Math.max(1000,Math.round(gap.duration/1000)*1000),values=groups.get(dvalue)||[];
    values.push(new Date(gap.start).toISOString());groups.set(dvalue,values);
  }
  return [...groups.entries()].sort((a,b)=>a[0]-b[0]).map(([dvalue,values])=>({values,dvalue}));
}
function chartRangeBreakSignature(rangebreaks){
  return (rangebreaks||[]).map(item=>`${item.dvalue}:${item.values?.length||0}:${item.values?.at(-1)||''}`).join('|');
}
function chartSessionBreakUpdate(rows,spec,currentSignature=''){
  const rangebreaks=chartSessionRangeBreaks(rows,spec),signature=chartRangeBreakSignature(rangebreaks);
  return signature===String(currentSignature||'')?null:{rangebreaks,signature};
}
function chartFirstRowAfter(rows,timestamp){
  let low=0,high=rows?.length||0;
  while(low<high){const middle=(low+high)>>1;if(chartRowDisplayMs(rows[middle])<=timestamp)low=middle+1;else high=middle;}
  return low<(rows?.length||0)?low:-1;
}
function chartHistoryRangeSpec(key=chartHistoryRangeKey){return CHART_HISTORY_RANGES.find(x=>x.key===key)||CHART_HISTORY_RANGES.at(-1);}
function chartSourceDay(row){const value=chartRowSourceMs(row);return Number.isFinite(value)?new Date(value).toISOString().slice(0,10):'';}
function chartShiftCalendarMs(timestamp,amount,unit){
  const source=new Date(timestamp),target=new Date(timestamp),day=source.getUTCDate();target.setUTCDate(1);
  if(unit==='month')target.setUTCMonth(source.getUTCMonth()-amount);else target.setUTCFullYear(source.getUTCFullYear()-amount);
  const lastDay=new Date(Date.UTC(target.getUTCFullYear(),target.getUTCMonth()+1,0)).getUTCDate();target.setUTCDate(Math.min(day,lastDay));return target.getTime();
}
function chartHistoryStartIndex(rows,key=chartHistoryRangeKey){
  if(!rows?.length)return 0;
  const range=chartHistoryRangeSpec(key),lastIndex=rows.length-1,latestSource=chartRowSourceMs(rows[lastIndex]);
  if(range.sessions){
    const sessions=[];for(const row of rows){const day=chartSourceDay(row);if(day&&sessions.at(-1)!==day)sessions.push(day);}
    const selectedSessions=new Set(sessions.slice(-range.sessions));
    const index=rows.findIndex(row=>selectedSessions.has(chartSourceDay(row)));return index>=0?index:lastIndex;
  }
  if(!Number.isFinite(latestSource)||range.key==='all')return 0;
  let targetMs;
  if(range.ytd){const target=new Date(latestSource);target.setUTCMonth(0,1);target.setUTCHours(0,0,0,0);targetMs=target.getTime();}
  else if(range.months)targetMs=chartShiftCalendarMs(latestSource,range.months,'month');
  else if(range.years)targetMs=chartShiftCalendarMs(latestSource,range.years,'year');
  else return 0;
  const index=rows.findIndex(row=>chartRowSourceMs(row)>=targetMs);
  return index>=0?index:lastIndex;
}
function chartHistoryViewport(rows,key=chartHistoryRangeKey,spec=chartTimeframeSpec()){
  if(!rows?.length)return {range:null,yr:undefined,startIndex:0};
  const startIndex=chartHistoryStartIndex(rows,key),visible=rows.slice(startIndex),start=String(visible[0]?.date||rows[0].date),realEnd=String(rows.at(-1).date),endMs=parseDateMs(realEnd),pad=spec?.bucketMs?Math.max(1000,Math.min(6*3600000,Number(spec.bucketMs)*.8)):86400000,end=endMs?new Date(endMs+pad).toISOString():realEnd;
  return {range:[start,end],yr:yRangeForChart(visible,[],start,realEnd,false),startIndex};
}
function chartAutomaticViewport(rows,spec=chartTimeframeSpec()){
  if(!rows?.length)return {range:null,yr:undefined};
  const first=String(rows[0].date),realEnd=String(rows.at(-1).date),endMs=parseDateMs(realEnd);let start=first;
  if(spec.kind==='seconds'){
    const lookback=spec.bucketMs<=1000?30*60000:6*3600000;start=new Date(Math.max(parseDateMs(first)||0,(endMs||0)-lookback)).toISOString();
  }else if(spec.kind==='calendar'&&spec.calendar==='day'){
    const target=chartShiftDate(realEnd,1,'year');start=String((rows.find(row=>String(row.date)>=target)||rows[0]).date);
  }else if(spec.kind==='calendar'&&spec.calendar==='week'){
    const target=chartShiftDate(realEnd,5,'year');start=String((rows.find(row=>String(row.date)>=target)||rows[0]).date);
  }else if(spec.kind==='range'&&spec.years){
    const target=chartShiftDate(realEnd,spec.years,'year');start=String((rows.find(row=>String(row.date)>=target)||rows[0]).date);
  }
  const pad=spec.bucketMs?Math.max(5000,Math.min(6*3600000,spec.bucketMs*.8)):86400000,end=endMs?new Date(endMs+pad).toISOString():realEnd;
  return {range:[start,end],yr:yRangeForChart(rows,[],start,realEnd,false)};
}
function chartDefaultViewport(rows,spec=chartTimeframeSpec()){return chartHistoryRangeKey==='auto'?chartAutomaticViewport(rows,spec):chartHistoryViewport(rows,chartHistoryRangeKey,spec);}
function liveSafeForChart(r){
  const live=Number(r?.live_price);
  return Number.isFinite(live)&&live>0&&r.live_chart_safe!==false&&isLiveQuotePlausibleForChart(r,live);
}
function forecastRowsForChart(r,rows,anchorPrice){
  const anchor=Number(anchorPrice);
  if(!Number.isFinite(anchor)||anchor<=0) return rows||[];
  const H=Number(r?.horizon_days||(rows||[]).length||horizon()||10);
  const vol=Math.max(0.08, sheetVolAnnPct(r)/100);
  const maxMove=clamp(vol*Math.sqrt(Math.max(1,H)/252)*4 + Math.abs(chg(r))/100*1.6 + 0.04, 0.12, 0.85);
  const hardLow=anchor*0.18, hardHigh=anchor*5.5;
  const softLow=anchor*(1-maxMove), softHigh=anchor*(1+maxMove);
  return (rows||[]).map(d=>{
    let price=Number(d.price), low=Number(d.low ?? d.price), high=Number(d.high ?? d.price);
    const absurd=[price,low,high].some(v=>!Number.isFinite(v)||v<=0||v<hardLow||v>hardHigh);
    if(absurd){
      price=clamp(Number.isFinite(price)&&price>0?price:anchor,softLow,softHigh);
      low=clamp(Number.isFinite(low)&&low>0?low:price,softLow,price);
      high=clamp(Number.isFinite(high)&&high>0?high:price,price,softHigh);
    }
    return {...d,price,low,high};
  });
}
function chartPriceText(value){
  const v=Number(value); if(!Number.isFinite(v))return '—';
  const digits=Math.abs(v)>=1000?2:Math.abs(v)>=1?2:4;
  if(digits===4){chartPriceFormatter4=chartPriceFormatter4||new Intl.NumberFormat('en-US',{minimumFractionDigits:4,maximumFractionDigits:4});return chartPriceFormatter4.format(v);}
  chartPriceFormatter2=chartPriceFormatter2||new Intl.NumberFormat('en-US',{minimumFractionDigits:2,maximumFractionDigits:2});return chartPriceFormatter2.format(v);
}
function formatCrosshairTime(ms,spanMs){
  const d=new Date(ms), intraday=spanMs<=8*86400000;
  try{
    if(intraday){chartIntradayTimeFormatter=chartIntradayTimeFormatter||new Intl.DateTimeFormat('en-GB',{timeZone:'UTC',day:'2-digit',month:'short',hour:'2-digit',minute:'2-digit',hour12:false});return chartIntradayTimeFormatter.format(d);}
    chartDateFormatter=chartDateFormatter||new Intl.DateTimeFormat('en-GB',{timeZone:'UTC',year:'numeric',month:'short',day:'2-digit'});return chartDateFormatter.format(d);
  }
  catch(e){const iso=d.toISOString();return intraday?`${iso.slice(8,10)} ${iso.slice(5,7)} ${iso.slice(11,16)}`:iso.slice(0,10);}
}
function nearestCandleIndex(times,targetMs){
  if(!times?.length)return -1;
  let lo=0,hi=times.length-1;
  while(lo<hi){const mid=(lo+hi)>>1;if(times[mid]<targetMs)lo=mid+1;else hi=mid;}
  const prev=Math.max(0,lo-1);
  return Math.abs(times[lo]-targetMs)<Math.abs(times[prev]-targetMs)?lo:prev;
}
function nearestCandle(rows,targetMs,times=null){
  const clean=rows||[];if(!clean.length)return null;
  const timeline=times&&times.length===clean.length?times:Float64Array.from(clean,r=>parseDateMs(r.date)||0),index=nearestCandleIndex(timeline,targetMs);
  return index>=0?clean[index]:null;
}
function cacheChartCandleRows(el,rows){
  if(!el)return;
  el._apexCandleRows=rows||[];
  el._apexCandleTimes=Float64Array.from(el._apexCandleRows,row=>Number(row?.timestamp??parseDateMs(row?.date))||0);
  el._apexCrosshairCandleIndex=-1;
}
function updateChartOhlc(row,r=selected){
  const strip=$('chartOhlcStrip'); if(!strip||!row||!r)return;
  const o=Number(row.open),h=Number(row.high),l=Number(row.low),c=Number(row.close),pct=o>0?(c/o-1)*100:0;
  const cls=c>=o?'up':'down',interval=chartTimeframeSpec().label,timeframe=chartTimeframeSpec();
  const meta=r._intraday_meta||{}, feed=(timeframe.kind==='seconds'||timeframe.kind==='intraday')?`${meta.delayed?'delayed':'live'} · ${meta.source||'market feed'}`:'full daily history';
  const signature=[r.label,interval,o,h,l,c,cls,feed].join('|');if(strip.dataset.paintSignature===signature)return;strip.dataset.paintSignature=signature;
  strip.innerHTML=`<span><b>${esc(r.label)}</b> ${interval}</span><span>O <b>${chartPriceText(o)}</b></span><span>H <b>${chartPriceText(h)}</b></span><span>L <b>${chartPriceText(l)}</b></span><span>C <b class="${cls}">${chartPriceText(c)}</b></span><span class="${cls}">${fmtPct(pct)}</span><span class="feed">${esc(feed)}</span>`;
}
function updateChartLastPrice(r=selected,row=null){
  const el=$('chart'), marker=$('chartLastPrice');
  if(!el?._fullLayout||!marker||!r){marker?.classList.remove('visible');return;}
  row=row||el._apexMinuteRows?.at(-1)||el._apexDailyRows?.at(-1);
  const rowPrice=Number(row?.close),rowTs=Number(row?.timestamp??parseDateMs(row?.date)),liveTs=Number(r.live_updated_at||0),rowIsCurrent=Number.isFinite(rowPrice)&&rowPrice>0&&(row?._live||r?._intraday_meta?.market_open)&&( !Number.isFinite(liveTs)||liveTs<=0||rowTs>=liveTs-120000 );
  const price=Number(rowIsCurrent?rowPrice:(r.live_price||rowPrice||r.last)), axis=el._fullLayout.yaxis, size=el._fullLayout._size;
  const yr=(axis?.range||[]).map(Number), lo=Math.min(...yr),hi=Math.max(...yr);
  if(!Number.isFinite(price)||!Number.isFinite(lo)||!Number.isFinite(hi)||hi<=lo||price<lo||price>hi){marker.classList.remove('visible');return;}
  const top=size.t+size.h*(1-(price-lo)/(hi-lo));
  setTextIfChanged(marker,chartPriceText(price));
  const left=(size.l+size.w+3)+'px';if(marker.style.left!==left)marker.style.left=left;if(marker.style.top!=='0px')marker.style.top='0px';marker.style.transform=`translate3d(0,${top}px,0) translateY(-50%)`;
  const cls='chartLastPrice visible '+(Number(row?.close||price)>=Number(row?.open||price)?'up':'down');if(marker.className!==cls)marker.className=cls;
}
function flushDeferredChartWork(){
  if(chartPointerActive||chartApplyingRange||chartFullRenderInFlight)return;
  if(pendingLiveLabels.size)scheduleLiveDomPaint([]);
  const full=deferredLiveChartUpdate,intraday=deferredIntradayPaint;
  deferredLiveChartUpdate=null;deferredIntradayPaint=null;
  if(!full&&!intraday)return;
  requestAnimationFrame(()=>{
    if(chartPointerActive){if(full)deferredLiveChartUpdate=full;if(intraday)deferredIntradayPaint=intraday;return;}
    if(full)updateLiveChart(full);
    else if(intraday)paintIntradayTrace(intraday.r,intraday.force,true);
  });
}
function settleChartPointer(){
  const remaining=CHART_POINTER_IDLE_MS-(performance.now()-chartPointerLastMove);
  if(remaining>0){chartPointerIdleTimer=setTimeout(settleChartPointer,remaining);return;}
  chartPointerIdleTimer=null;chartPointerActive=false;flushDeferredChartWork();
}
function noteChartPointerActivity(){
  chartPointerActive=true;chartPointerLastMove=performance.now();
  if(!chartPointerIdleTimer)chartPointerIdleTimer=setTimeout(settleChartPointer,CHART_POINTER_IDLE_MS);
}
function scheduleChartResize(force=false){
  const stage=$('chartStage'),el=$('chart');
  if(!stage||!el||!window.Plotly||!el._fullLayout)return;
  if(force)chartResizeLastSize='';
  if(chartResizeFrame)return;
  chartResizeFrame=requestAnimationFrame(()=>{
    chartResizeFrame=0;
    const rect=stage.getBoundingClientRect(),key=`${Math.round(rect.width)}x${Math.round(rect.height)}`;
    if(key===chartResizeLastSize)return;
    chartResizeLastSize=key;
    Promise.resolve(Plotly.Plots.resize(el)).then(()=>{$('chartStage')?._apexCrosshairInvalidate?.();$('chartStage')?._apexCrosshairRepaint?.();}).catch(()=>{});
  });
}
function requestChartResizeBurst(){
  if(chartResizeTimer80)clearTimeout(chartResizeTimer80);if(chartResizeTimer180)clearTimeout(chartResizeTimer180);
  scheduleChartResize(true);
  chartResizeTimer80=setTimeout(()=>{chartResizeTimer80=null;scheduleChartResize(true);},80);
  chartResizeTimer180=setTimeout(()=>{chartResizeTimer180=null;scheduleChartResize(true);},180);
}
function bindChartResizeObserver(){
  const stage=$('chartStage');
  if(!stage)return;
  if(!chartWindowResizeBound){window.addEventListener?.('resize',()=>scheduleChartResize(),{passive:true});chartWindowResizeBound=true;}
  if(chartResizeObserver||typeof ResizeObserver!=='function')return;
  chartResizeObserver=new ResizeObserver(()=>scheduleChartResize());
  chartResizeObserver.observe(stage);
}
function bindSmoothChartWheel(el){
  const stage=$('chartStage');if(!stage||stage._apexWheelReady)return;
  stage._apexWheelReady=true;
  let frame=0,pendingPanPixels=0,pendingZoomLog=0,inFlight=false,stageRect=null,shiftHeld=false,shiftActiveUntil=0,wheelMode='',pointerX=NaN,pointerY=NaN,gestureActive=false,gestureTimer=null;
  const invalidateRect=()=>{stageRect=null;};
  const disableNativeWheelZoom=()=>{if(el?._context)el._context.scrollZoom=false;};
  const touchGesture=()=>{
    if(!gestureActive){gestureActive=true;beginManualChartNavigation(el);}
    if(gestureTimer)clearTimeout(gestureTimer);
    gestureTimer=setTimeout(()=>{gestureTimer=null;gestureActive=false;},240);
  };
  const schedule=()=>{if(!frame&&!inFlight)frame=requestAnimationFrame(applyWheel);};
  const applyWheel=()=>{
    frame=0;
    if(inFlight||!el._fullLayout)return;
    if(chartApplyingRange||chartFullRenderInFlight){frame=requestAnimationFrame(applyWheel);return;}
    const size=el._fullLayout._size||{},rows=el._apexRangeMeta?.candleRows||[],mode=wheelMode||'both',pixels=pendingPanPixels,zoomLog=pendingZoomLog;
    pendingPanPixels=0;pendingZoomLog=0;wheelMode='';
    const update={};let nextX=null,nextY=null;
    if(Number.isFinite(pixels)&&Math.abs(pixels)>=.25){
      nextX=chartWheelPanXRange(el._fullLayout.xaxis,size,pixels,rows);
    }else if(Number.isFinite(zoomLog)&&Math.abs(zoomLog)>=.0005){
      const factor=Math.exp(clamp(zoomLog,-.42,.42)),spec=chartTimeframeSpec(),first=chartRowDisplayMs(rows[0]),last=chartRowDisplayMs(rows.at(-1)),dataSpan=Number.isFinite(first)&&Number.isFinite(last)?Math.max(1,last-first):366*86400000;
      if(mode==='x'||mode==='both')nextX=chartWheelXRange(el._fullLayout.xaxis,size,pointerX,factor,Math.max(1000,Number(spec?.bucketMs||60000)*8),Math.max(366*86400000,dataSpan*1.25));
      if(mode==='y'||mode==='both'){
        const yr=(el._fullLayout.yaxis?.range||[]).map(Number),center=Math.abs((Number(yr[0])+Number(yr[1]))*.5)||1,currentSpan=Math.abs(Number(yr[1])-Number(yr[0]));
        nextY=chartWheelYRange(el._fullLayout.yaxis,size,pointerY,factor,Math.max(1e-8,center*.0001),Math.max(currentSpan*80,center*20));
      }
    }
    if(nextX){update['xaxis.autorange']=false;update['xaxis.range']=nextX;}
    if(nextY){update['yaxis.autorange']=false;update['yaxis.range']=nextY;}
    if(!Object.keys(update).length)return;
    noteChartPointerActivity();lockManualChartViewport(el,{xRange:nextX||undefined,yRange:nextY||undefined});
    inFlight=true;chartApplyingRange=true;
    Promise.resolve().then(()=>Plotly.relayout(el,update)).catch(()=>{}).finally(()=>setTimeout(()=>{
      chartApplyingRange=false;inFlight=false;noteChartPointerActivity();$('chartStage')?._apexCrosshairRangeInvalidate?.();$('chartStage')?._apexCrosshairRepaint?.();updateChartLastPrice(selected);
      if(selected&&el?._apexRangeMeta?.seriesTimed){deferredIntradayPaint={r:selected,force:true};if(nextX)scheduleChartArchiveForViewport(selected,nextX,180);}
      if(Math.abs(pendingPanPixels)>=.25||Math.abs(pendingZoomLog)>=.0005)schedule();
    },0));
  };
  disableNativeWheelZoom();
  stage.addEventListener('wheel',ev=>{
    if(!el._fullLayout||ev.target?.closest?.('.chartTimeframeMenu,.chartStyleMenu,.chartCrosshairWidthMenu,.chartHistoryToolbar,.modebar'))return;
    const rect=stageRect||stage.getBoundingClientRect(),size=el._fullLayout._size||{},x=ev.clientX-rect.left,y=ev.clientY-rect.top,right=size.l+size.w,bottom=size.t+size.h,inPlot=x>=size.l&&x<=right&&y>=size.t&&y<=bottom,onXAxis=x>=size.l&&x<=right&&y>bottom&&y<=rect.height,onYAxis=x>right&&x<=rect.width&&y>=size.t&&y<=bottom;
    if(!inPlot&&!onXAxis&&!onYAxis)return;
    const shiftPan=!!ev.shiftKey||!!ev.getModifierState?.('Shift')||shiftHeld||performance.now()<shiftActiveUntil;
    disableNativeWheelZoom();touchGesture();noteChartPointerActivity();
    ev.preventDefault();ev.stopImmediatePropagation?.();
    const unit=ev.deltaMode===1?16:ev.deltaMode===2?Math.max(1,rect.height):1,raw=Math.abs(Number(ev.deltaX||0))>Math.abs(Number(ev.deltaY||0))?Number(ev.deltaX||0):Number(ev.deltaY||0),delta=clamp(raw*unit,-240,240);
    if(!delta)return;
    pointerX=x;pointerY=y;
    if(shiftPan){pendingZoomLog=0;wheelMode='pan';pendingPanPixels=clamp(pendingPanPixels+delta*.72,-Math.max(1,size.w)*.9,Math.max(1,size.w)*.9);}
    else{pendingPanPixels=0;wheelMode=onXAxis?'x':onYAxis?'y':'both';pendingZoomLog=clamp(pendingZoomLog+delta*.00135,-.5,.5);}
    schedule();
  },{passive:false,capture:true});
  window.addEventListener?.('keydown',ev=>{if(ev.key==='Shift'){shiftHeld=true;shiftActiveUntil=performance.now()+500;disableNativeWheelZoom();}},{passive:true,capture:true});
  window.addEventListener?.('keyup',ev=>{if(ev.key==='Shift'){shiftHeld=false;shiftActiveUntil=Math.max(shiftActiveUntil,performance.now()+180);}},{passive:true,capture:true});
  window.addEventListener?.('blur',()=>{shiftHeld=false;shiftActiveUntil=0;},{passive:true});
  stage.addEventListener('pointerdown',ev=>{
    if(ev.button!==undefined&&ev.button!==0)return;
    if(ev.target?.closest?.('.chartTimeframeMenu,.chartStyleMenu,.chartCrosshairWidthMenu,.chartHistoryToolbar,.modebar'))return;
    const rect=stageRect||stage.getBoundingClientRect(),size=el._fullLayout?._size||{},x=ev.clientX-rect.left,y=ev.clientY-rect.top;
    if(x>=size.l&&x<=size.l+size.w&&y>=size.t&&y<=size.t+size.h)beginManualChartNavigation(el);
  },{passive:true,capture:true});
  window.addEventListener?.('resize',invalidateRect,{passive:true});
  document.addEventListener?.('scroll',invalidateRect,{passive:true,capture:true});
  stage._apexWheelInvalidate=invalidateRect;
}
function bindChartCrosshair(el){
  const stage=$('chartStage'); if(!stage||stage._apexCrosshairReady)return;
  stage._apexCrosshairReady=true;
  const cross=$('chartCrosshair'),vLine=$('chartCrosshairV'),hLine=$('chartCrosshairH'),xLabel=$('chartCrosshairX'),yLabel=$('chartCrosshairY');
  let frame=0,pointX=NaN,pointY=NaN,pointerButtons=0,hasPoint=false,stageRect=null,geometryKey='',rangeKey='',scale=null,timeBucket=null,timeMode='';
  const invalidateRange=()=>{rangeKey='';timeBucket=null;timeMode='';};
  const invalidate=()=>{stageRect=null;geometryKey='';invalidateRange();};
  const updateRect=()=>{stageRect=stage.getBoundingClientRect();return stageRect;};
  const applyGeometry=size=>{
    const key=`${size.l}|${size.t}|${size.w}|${size.h}`;if(key===geometryKey)return;geometryKey=key;
    vLine.style.top=size.t+'px';vLine.style.height=size.h+'px';
    hLine.style.left=size.l+'px';hLine.style.width=size.w+'px';
    xLabel.style.top=(size.t+size.h+3)+'px';yLabel.style.left=(size.l+size.w+3)+'px';
  };
  const paint=()=>{
    frame=0;if(!hasPoint||!el._fullLayout)return;
    const size=el._fullLayout._size,x=pointX,y=pointY;
    if(!size||x<size.l||x>size.l+size.w||y<size.t||y>size.t+size.h){cross?.classList.remove('visible');return;}
    applyGeometry(size);
    const xr=el._fullLayout.xaxis?.range||[],yr=el._fullLayout.yaxis?.range||[],nextRangeKey=`${xr[0]}|${xr[1]}|${yr[0]}|${yr[1]}`;
    if(nextRangeKey!==rangeKey){
      rangeKey=nextRangeKey;
      const x0=parseChartAxisMs(xr[0]),x1=parseChartAxisMs(xr[1]),y0=Number(yr[0]),y1=Number(yr[1]);
      scale=(Number.isFinite(x0)&&Number.isFinite(x1)&&Number.isFinite(y0)&&Number.isFinite(y1)&&x1!==x0&&y1!==y0)?{x0,x1,y0,y1}:null;
    }
    if(!scale)return;
    const linearX=scale.x0+(x-size.l)/size.w*(scale.x1-scale.x0),xMs=chartAxisPixelToMs(el._fullLayout.xaxis,x-size.l,linearX),yValue=Math.max(scale.y0,scale.y1)-(y-size.t)/size.h*Math.abs(scale.y1-scale.y0),dpr=Math.max(1,Number(window.devicePixelRatio||1)),px=Math.round(x*dpr)/dpr,py=Math.round(y*dpr)/dpr,labelX=clamp(px,size.l+38,size.l+size.w-38);
    vLine.style.transform=`translate3d(${px}px,0,0) scaleX(var(--chart-crosshair-width,.55))`;
    hLine.style.transform=`translate3d(0,${py}px,0) scaleY(var(--chart-crosshair-width,.55))`;
    xLabel.style.transform=`translate3d(${labelX}px,0,0) translateX(-50%)`;
    yLabel.style.transform=`translate3d(0,${py}px,0) translateY(-50%)`;
    const span=Math.abs(scale.x1-scale.x0),mode=span<=8*86400000?'minute':'day',bucket=Math.floor(xMs/(mode==='minute'?60000:86400000));
    if(bucket!==timeBucket||mode!==timeMode){timeBucket=bucket;timeMode=mode;setTextIfChanged(xLabel,formatCrosshairTime(xMs,span));}
    setTextIfChanged(yLabel,chartPriceText(yValue));
    if(!cross.classList.contains('visible'))cross.classList.add('visible');
    const index=nearestCandleIndex(el._apexCandleTimes,xMs);
    if(pointerButtons===0&&index>=0&&index!==el._apexCrosshairCandleIndex){el._apexCrosshairCandleIndex=index;updateChartOhlc(el._apexCandleRows[index],selected);}
  };
  const capturePoint=ev=>{
    const samples=typeof ev.getCoalescedEvents==='function'?ev.getCoalescedEvents():null,point=samples?.length?samples[samples.length-1]:ev,rect=stageRect||updateRect();
    pointX=point.clientX-rect.left;pointY=point.clientY-rect.top;pointerButtons=Number(point.buttons||0);hasPoint=true;noteChartPointerActivity();if(!frame)frame=requestAnimationFrame(paint);
  };
  stage.addEventListener('pointerenter',ev=>{updateRect();capturePoint(ev);},{passive:true});
  stage.addEventListener(('onpointerrawupdate' in window)?'pointerrawupdate':'pointermove',capturePoint,{passive:true});
  stage.addEventListener('pointerup',()=>{pointerButtons=0;},{passive:true});
  stage.addEventListener('pointercancel',()=>{pointerButtons=0;},{passive:true});
  stage.addEventListener('pointerleave',()=>{pointerButtons=0;hasPoint=false;cross?.classList.remove('visible');el._apexCrosshairCandleIndex=-1;chartPointerActive=false;chartPointerLastMove=0;if(chartPointerIdleTimer){clearTimeout(chartPointerIdleTimer);chartPointerIdleTimer=null;}const latest=el._apexMinuteRows?.at(-1)||el._apexDailyRows?.at(-1);if(latest)updateChartOhlc(latest,selected);flushDeferredChartWork();});
  window.addEventListener?.('resize',invalidate,{passive:true});
  document.addEventListener?.('scroll',()=>{stageRect=null;},{passive:true,capture:true});
  stage._apexCrosshairInvalidate=invalidate;
  stage._apexCrosshairRangeInvalidate=invalidateRange;
  stage._apexCrosshairRepaint=()=>{if(hasPoint&&!frame)frame=requestAnimationFrame(paint);};
}
function chartHighLowSegments(rows){
  const x=[],y=[];(rows||[]).forEach(r=>{x.push(r.date,r.date,null);y.push(Number(r.low),Number(r.high),null);});return {x,y};
}
function chartVolumeSizes(rows){
  const volumes=(rows||[]).map(r=>Math.max(0,Number(r.volume||0))), max=Math.max(1,...volumes);
  return volumes.map(v=>5+15*Math.sqrt(v/max));
}
function chartDirectionColors(rows,alpha=1){
  return (rows||[]).map(r=>Number(r.close)>=Number(r.open)?`rgba(8,153,129,${alpha})`:`rgba(242,54,69,${alpha})`);
}
function chartDisplayRoles(mode=chartDisplayMode){
  if(mode==='volume_candles')return ['ohlc','volume'];
  if(mode==='hlc_area')return ['high','low','close'];
  if(mode==='baseline')return ['baseline','close'];
  if(mode==='columns')return ['close_bar'];
  if(mode==='high_low')return ['highlow_points'];
  if(mode==='volume_footprint')return ['footprint'];
  if(mode==='bars'||mode==='candles'||mode==='hollow_candles')return ['ohlc'];
  return ['close'];
}
function chartRowsPaintSignature(rows,mode=chartDisplayMode){
  const first=rows?.[0],last=rows?.at(-1);if(!first||!last)return '';
  return [mode,rows.length,chartRowDisplayMs(first),chartRowDisplayMs(last),last.open,last.high,last.low,last.close,last.volume||0].join('|');
}
function chartSeriesSourceSignature(series){return `${chartTimezone}|${chartTimeframeKey}|${series?.timed?'timed':'daily'}|${chartRowsPaintSignature(series?.rows||[],'source')}`;}
function buildMarketTraces(rows,name,mode=chartDisplayMode){
  rows=(rows||[]).filter(r=>r?.date&&[r.open,r.high,r.low,r.close].every(v=>Number.isFinite(Number(v))&&Number(v)>0));
  if(!rows.length)return {traces:[],roles:[]};
  const x=rows.map(r=>r.date),close=rows.map(r=>Number(r.close)),high=rows.map(r=>Number(r.high)),low=rows.map(r=>Number(r.low)),open=rows.map(r=>Number(r.open));
  const base={name,showlegend:false,hoverinfo:'skip'};
  const candle={...base,type:'candlestick',x,open,high,low,close,whiskerwidth:.35,increasing:{line:{color:'#089981',width:1},fillcolor:'#089981'},decreasing:{line:{color:'#f23645',width:1},fillcolor:'#f23645'}};
  if(mode==='bars')return {traces:[{...base,type:'ohlc',x,open,high,low,close,increasing:{line:{color:'#089981',width:1}},decreasing:{line:{color:'#f23645',width:1}}}],roles:['ohlc']};
  if(mode==='candles')return {traces:[candle],roles:['ohlc']};
  if(mode==='hollow_candles')return {traces:[{...candle,increasing:{line:{color:'#089981',width:1.2},fillcolor:'rgba(8,153,129,0)'},decreasing:{line:{color:'#f23645',width:1.2},fillcolor:'#f23645'}}],roles:['ohlc']};
  if(mode==='volume_candles')return {traces:[candle,{...base,type:'bar',x,y:rows.map(r=>Number(r.volume||0)),yaxis:'y2',marker:{color:chartDirectionColors(rows,.34),line:{width:0}},opacity:.85}],roles:['ohlc','volume']};
  if(mode==='line')return {traces:[{...base,type:'scatter',mode:'lines',x,y:close,line:{color:'#5b8cff',width:1.7},connectgaps:false}],roles:['close']};
  if(mode==='line_markers')return {traces:[{...base,type:'scatter',mode:'lines+markers',x,y:close,line:{color:'#5b8cff',width:1.6},marker:{size:5,color:'#8facff',line:{color:'#5b8cff',width:.5}},connectgaps:false}],roles:['close']};
  if(mode==='step_line')return {traces:[{...base,type:'scatter',mode:'lines',x,y:close,line:{color:'#5b8cff',width:1.5,shape:'hv'}}],roles:['close']};
  if(mode==='area')return {traces:[{...base,type:'scatter',mode:'lines',x,y:close,line:{color:'#5b8cff',width:1.4},fill:'tozeroy',fillcolor:'rgba(41,98,255,.20)'}],roles:['close']};
  if(mode==='hlc_area')return {traces:[
    {...base,type:'scatter',mode:'lines',x,y:high,line:{color:'rgba(91,140,255,.30)',width:.7}},
    {...base,type:'scatter',mode:'lines',x,y:low,line:{color:'rgba(91,140,255,.30)',width:.7},fill:'tonexty',fillcolor:'rgba(91,140,255,.16)'},
    {...base,type:'scatter',mode:'lines',x,y:close,line:{color:'#8facff',width:1.5}},
  ],roles:['high','low','close']};
  if(mode==='baseline'){
    const baseline=Number(rows[0]?.close||0), baselineY=rows.map(()=>baseline);
    return {traces:[{...base,type:'scatter',mode:'lines',x,y:baselineY,line:{color:'rgba(156,163,175,.55)',width:1,dash:'dot'}},{...base,type:'scatter',mode:'lines',x,y:close,line:{color:'#5b8cff',width:1.5},fill:'tonexty',fillcolor:'rgba(41,98,255,.18)'}],roles:['baseline','close']};
  }
  if(mode==='columns')return {traces:[{...base,type:'bar',x,y:close,marker:{color:chartDirectionColors(rows,.78),line:{width:0}}}],roles:['close_bar']};
  if(mode==='high_low'){
    const mid=rows.map(r=>(Number(r.high)+Number(r.low))/2);
    return {traces:[{...base,type:'scatter',mode:'markers',x,y:mid,marker:{size:4,color:chartDirectionColors(rows,1),symbol:'line-ew',line:{width:1}},error_y:{type:'data',symmetric:false,array:rows.map((r,i)=>Number(r.high)-mid[i]),arrayminus:rows.map((r,i)=>mid[i]-Number(r.low)),color:'rgba(174,182,194,.88)',thickness:1,width:0}}],roles:['highlow_points']};
  }
  if(mode==='volume_footprint'){
    const typical=rows.map(r=>(Number(r.high)+Number(r.low)+Number(r.close))/3);
    return {traces:[{...base,type:'scatter',mode:'markers',x,y:typical,marker:{size:chartVolumeSizes(rows),sizemode:'diameter',color:chartDirectionColors(rows,.72),line:{color:'rgba(255,255,255,.34)',width:.6}},error_y:{type:'data',symmetric:false,array:rows.map((r,i)=>Math.max(0,Number(r.high)-typical[i])),arrayminus:rows.map((r,i)=>Math.max(0,typical[i]-Number(r.low))),color:'rgba(174,182,194,.5)',thickness:1,width:0}}],roles:['footprint']};
  }
  return {traces:[{...base,type:'scatter',mode:'lines',x,y:close,line:{color:'#5b8cff',width:1.6}}],roles:['close']};
}
function marketTraceUpdate(role,rows){
  const x=rows.map(r=>r.date);
  if(role==='ohlc')return {x:[x],open:[rows.map(r=>r.open)],high:[rows.map(r=>r.high)],low:[rows.map(r=>r.low)],close:[rows.map(r=>r.close)]};
  if(role==='high')return {x:[x],y:[rows.map(r=>r.high)]};
  if(role==='low')return {x:[x],y:[rows.map(r=>r.low)]};
  if(role==='baseline'){const base=Number(rows[0]?.close||0);return {x:[x],y:[rows.map(()=>base)]};}
  if(role==='close_bar')return {x:[x],y:[rows.map(r=>r.close)],'marker.color':[chartDirectionColors(rows,.78)]};
  if(role==='volume')return {x:[x],y:[rows.map(r=>Number(r.volume||0))],'marker.color':[chartDirectionColors(rows,.34)]};
  if(role==='highlow'){const seg=chartHighLowSegments(rows);return {x:[seg.x],y:[seg.y]};}
  if(role==='highlow_points'){
    const mid=rows.map(r=>(Number(r.high)+Number(r.low))/2);return {x:[x],y:[mid],'marker.color':[chartDirectionColors(rows,1)],'error_y.array':[rows.map((r,i)=>Number(r.high)-mid[i])],'error_y.arrayminus':[rows.map((r,i)=>mid[i]-Number(r.low))]};
  }
  if(role==='footprint'){
    const typical=rows.map(r=>(Number(r.high)+Number(r.low)+Number(r.close))/3);return {x:[x],y:[typical],'marker.size':[chartVolumeSizes(rows)],'marker.color':[chartDirectionColors(rows,.72)],'error_y.array':[rows.map((r,i)=>Math.max(0,Number(r.high)-typical[i]))],'error_y.arrayminus':[rows.map((r,i)=>Math.max(0,typical[i]-Number(r.low)))]};
  }
  return {x:[x],y:[rows.map(r=>r.close)]};
}
function paintIntradayTrace(r,force=false,interactionBypass=false,archivePaint=false){
  const el=$('chart');
  if(!window.Plotly||!el||chartRenderedLabel!==String(r?.label||''))return;
  if(chartFullRenderInFlight){deferredIntradayPaint={r,force:true};return;}
  if(chartPointerActive&&!interactionBypass){
    deferredIntradayPaint={r,force:!!force||!!deferredIntradayPaint?.force};
    return;
  }
  const now=performance.now();
  if(!force&&now-liveCandleLastPaint<chartCandlePaintInterval(r))return;
  liveCandleLastPaint=now;
  const rawHistory=cleanHistoryForChart(r?.history||[]),series=chartSeriesForTimeframe(r,rawHistory),meta=el._apexRangeMeta||{},sourceSignature=chartSeriesSourceSignature(series),sourceReused=meta.sourceSignature===sourceSignature&&Array.isArray(meta.candleRows)&&meta.candleRows.length>0,fullRows=sourceReused?meta.candleRows:(series.timed?chartDisplayTimedBars(series.rows):series.rows);
  if(!fullRows.length)return;
  const currentRange=el._fullLayout?.xaxis?.range||[],rows=chartRowsForViewport(r,fullRows,currentRange,series.spec);
  if(!rows.length)return;
  const roleIndices=el._apexMarketRoleIndices;
  if(!Array.isArray(roleIndices)||!roleIndices.length){renderChart(r);return;}
  const firstMs=parseDateMs(fullRows[0].date),oldFirstMs=parseDateMs(meta.firstDate),coverageExpanded=!!(firstMs&&oldFirstMs&&firstMs<oldFirstMs-Math.max(60000,Number(series.spec.bucketMs||0)));
  if(coverageExpanded&&chartViewportRevision===meta.viewportRevision&&!archivePaint){renderChart(r,{preserveViewport:true});return;}
  if(meta.seriesTimed!==series.timed){
    const xr=el._fullLayout?.xaxis?.range||[],lastMs=parseDateMs(fullRows.at(-1).date),viewStart=parseChartAxisMs(xr[0]),viewEnd=parseChartAxisMs(xr[1]);
    const overlaps=!!(firstMs&&lastMs&&viewStart&&viewEnd&&viewEnd>=firstMs&&viewStart<=lastMs);
    renderChart(r,{preserveViewport:overlaps});
    return;
  }
  if(meta.paintInFlight){deferredIntradayPaint={r,force:true};return;}
  const oldLatest=Number(meta.latestRealMs||parseDateMs(meta.latestReal)||0),firstNewIndex=oldLatest>0?chartFirstRowAfter(fullRows,oldLatest):-1;
  if(firstNewIndex>0&&chartGapInfo(fullRows,firstNewIndex,series.spec)){renderChart(r,{preserveViewport:true});return;}
  const sessionBreakUpdate=archivePaint?chartSessionBreakUpdate(fullRows,series.spec,meta.rangeBreakSignature):null;
  const latest=fullRows.at(-1), latestMs=latest.timestamp;
  const expectedRoles=chartDisplayRoles(chartDisplayMode),paintSignature=chartRowsPaintSignature(rows,chartDisplayMode);
  if(expectedRoles.length!==roleIndices.length||expectedRoles.some((role,index)=>roleIndices[index]?.role!==role)){renderChart(r);return;}
  if(meta.paintSignature===paintSignature&&!sessionBreakUpdate){updateChartOhlc(latest,r);updateChartLastPrice(r,latest);return;}
  const lockedViewport=activeManualChartViewport(el),lockedRevision=chartViewportRevision;
  el._apexDailyRows=series.timed?[]:fullRows;el._apexAdaptiveRows=[];el._apexMinuteRows=series.timed?fullRows:[];if(!sourceReused)cacheChartCandleRows(el,fullRows);
  meta.h=rawHistory;meta.minute=series.timed?fullRows:[];meta.candleRows=fullRows;meta.renderRows=rows;meta.sourceSignature=sourceSignature;meta.firstDate=String(fullRows[0]?.date||meta.firstDate||'');meta.latestReal=latest.date;meta.latestRealMs=latestMs;meta.seriesTimed=series.timed;meta.seriesLabel=series.label;meta.viewportRevision=chartViewportRevision;
  const currentStart=parseChartAxisMs(currentRange[0]),currentEnd=parseChartAxisMs(currentRange[1]);
  const nearLive=oldLatest>0&&currentEnd&&Math.abs(currentEnd-oldLatest)<Math.max(30*60000,Number(series.spec.bucketMs||0)*3);
  let liveRangeUpdate=null;
  if(latestMs>oldLatest&&currentStart&&currentEnd&&chartFollowLive&&!lockedViewport&&nearLive){
    const delta=latestMs-oldLatest,preset=chartHistoryRangeKey==='auto'?null:chartHistoryViewport(fullRows,chartHistoryRangeKey,series.spec);
    liveRangeUpdate={'xaxis.range':preset?.range||[new Date(currentStart+delta).toISOString(),new Date(currentEnd+delta).toISOString()]};
    if(preset?.yr)liveRangeUpdate['yaxis.range']=preset.yr;
  }
  meta.paintInFlight=true;
  roleIndices.reduce((promise,x)=>promise.then(()=>Promise.resolve(Plotly.restyle(el,marketTraceUpdate(x.role,rows),[x.index]))),Promise.resolve())
    .then(()=>{
      if(!sessionBreakUpdate)return;
      chartApplyingRange=true;
      return Promise.resolve().then(()=>Plotly.relayout(el,{'xaxis.rangebreaks':sessionBreakUpdate.rangebreaks})).then(()=>{
        meta.rangeBreakSignature=sessionBreakUpdate.signature;
        $('chartStage')?._apexCrosshairRangeInvalidate?.();$('chartStage')?._apexCrosshairRepaint?.();
      }).finally(()=>{chartApplyingRange=false;});
    })
    .then(()=>{meta.paintSignature=paintSignature;return restoreManualChartViewport(el,lockedViewport,lockedRevision);})
    .then(()=>{
      if(!liveRangeUpdate||lockedRevision!==chartViewportRevision||chartPointerActive||!chartFollowLive||activeManualChartViewport(el))return;
      chartApplyingRange=true;
      return Promise.resolve(Plotly.relayout(el,liveRangeUpdate)).finally(()=>{
        chartApplyingRange=false;$('chartStage')?._apexCrosshairRangeInvalidate?.();$('chartStage')?._apexCrosshairRepaint?.();
      });
    })
    .then(()=>{updateChartOhlc(latest,r);updateChartLastPrice(r,latest);})
    .catch(()=>{})
    .finally(()=>{meta.paintInFlight=false;if((deferredIntradayPaint||deferredLiveChartUpdate)&&!chartPointerActive&&!chartApplyingRange)flushDeferredChartWork();});
}
function bindChartInteractionGuards(el){
  bindChartCrosshair(el);
  bindSmoothChartWheel(el);
  bindChartResizeObserver();
  if(!el || el._apexInteractionGuard) return;
  el._apexInteractionGuard=true;
  el.on?.('plotly_relayout',ev=>{
    if(chartApplyingRange) return;
    $('chartStage')?._apexCrosshairRangeInvalidate?.();
    const xRange=chartRelayoutRange(ev,'xaxis'),yRange=chartRelayoutRange(ev,'yaxis');
    const resetRequested=ev?.['xaxis.autorange']===true||ev?.['yaxis.autorange']===true;
    if(resetRequested){
      chartManualViewport=null;chartUserRangeChangedAt=0;chartFollowLive=true;chartViewportRevision++;
    }else if(xRange||yRange){
      chartUserRangeChangedAt=performance.now();
      chartViewportRevision++;
      lockManualChartViewport(el,{xRange,yRange});
      if(selected&&el?._apexRangeMeta?.seriesTimed){deferredIntradayPaint={r:selected,force:true};if(!chartPointerActive)flushDeferredChartWork();}
      if(selected&&xRange)scheduleChartArchiveForViewport(selected,xRange);
    }
  });
  el.on?.('plotly_relayouting',ev=>{
    noteChartPointerActivity();$('chartStage')?._apexCrosshairRangeInvalidate?.();$('chartStage')?._apexCrosshairRepaint?.();
    if(chartApplyingRange)return;
    const xRange=chartRelayoutRange(ev,'xaxis'),yRange=chartRelayoutRange(ev,'yaxis');
    if(xRange||yRange){chartUserRangeChangedAt=performance.now();chartFollowLive=false;lockManualChartViewport(el,{xRange,yRange});}
  });
  el.on?.('plotly_doubleclick',()=>{ chartManualViewport=null; chartUserRangeChangedAt=0; chartFollowLive=true; chartViewportRevision++;if(selected)scheduleChartArchiveForViewport(selected); });
}
function chartDecorations(lastClose,latestReal,hasForecast){
  const baseShapes=[{type:'line',xref:'paper',yref:'y',x0:0,x1:1,y0:lastClose,y1:lastClose,line:{color:'rgba(156,163,175,.42)',width:1,dash:'dot'}}];
  const forecastShapes=hasForecast?[{type:'line',xref:'x',yref:'paper',x0:latestReal,x1:latestReal,y0:0,y1:1,line:{color:'rgba(91,140,255,.48)',width:1,dash:'dot'}}]:[];
  const forecastAnnotations=hasForecast?[{xref:'x',yref:'paper',x:latestReal,y:1.015,text:'Forecast',showarrow:false,font:{size:10,color:'#7da1ff'}}]:[];
  return {baseShapes,forecastShapes,forecastAnnotations};
}
function paintForecastTraces(r){
  const el=$('chart'), indices=el?._apexForecastTraceIndices||[], meta=el?._apexRangeMeta||{};
  if(chartPointerActive||chartApplyingRange||chartFullRenderInFlight||meta.paintInFlight){deferredLiveChartUpdate=r;return;}
  if(!window.Plotly||!el||indices.length!==3||chartRenderedLabel!==String(r?.label||''))return;
  const latestRow=(meta.candleRows||[]).at(-1)||(meta.h||[]).at(-1)||{};
  const latestReal=String(meta.latestReal||latestRow.date||r.last_date||'');
  const anchorDay=latestReal.slice(0,10);
  const rawForecast=(r.forecast||[]).filter(d=>d&&d.date&&Number.isFinite(Number(d.price)));
  const baseClose=Number(latestRow.close||r._live_base_last||r.last||0);
  const lastClose=Number(liveSafeForChart(r)?r.live_price:baseClose);
  if(!latestReal||!Number.isFinite(lastClose)||lastClose<=0)return;
  const futureForecast=rawForecast.filter(d=>String(d.date).slice(0,10)>anchorDay);
  const f=forecastRowsForChart(r,futureForecast.length?futureForecast:rawForecast,lastClose);
  const forecastX=[latestReal,...f.map(d=>String(d.date).slice(0,10))];
  const forecastY=[lastClose,...f.map(d=>Number(d.price))];
  const upperY=[lastClose,...f.map(d=>Number(d.high))];
  const lowerY=[lastClose,...f.map(d=>Number(d.low))];
  const decoration=chartDecorations(lastClose,latestReal,!!f.length);
  el._apexBaseShapes=decoration.baseShapes;
  el._apexForecastShapes=decoration.forecastShapes;
  el._apexForecastAnnotations=decoration.forecastAnnotations;
  meta.f=f;
  meta.forecastEnd=String(f.at(-1)?.date||latestReal);
  const shapes=chartForecastVisible?[...decoration.baseShapes,...decoration.forecastShapes]:decoration.baseShapes;
  const annotations=chartForecastVisible?decoration.forecastAnnotations:[];
  const lockedViewport=activeManualChartViewport(el),lockedRevision=chartViewportRevision;
  Promise.all([
    Promise.resolve(Plotly.restyle(el,{x:[forecastX],y:[upperY]},[indices[0]])),
    Promise.resolve(Plotly.restyle(el,{x:[forecastX],y:[lowerY]},[indices[1]])),
    Promise.resolve(Plotly.restyle(el,{x:[forecastX],y:[forecastY]},[indices[2]])),
    Promise.resolve(Plotly.relayout(el,{shapes,annotations})),
  ]).then(()=>restoreManualChartViewport(el,lockedViewport,lockedRevision)).catch(()=>{});
}
function updateLiveChart(r){
  const el=$('chart');
  if(chartPointerActive){deferredLiveChartUpdate=r;return;}
  if(!window.Plotly||!el||!el._fullLayout||chartRenderedLabel!==String(r?.label||'')||!Array.isArray(el._apexForecastTraceIndices)){
    renderChart(r);
    return;
  }
  if(Array.isArray(el._apexMarketRoleIndices)&&el._apexMarketRoleIndices.length)paintIntradayTrace(r,true);
  paintForecastTraces(r);
}
function queueLatestChartRender(r,options={}){
  return new Promise(resolve=>{
    if(chartQueuedRender?.resolve)chartQueuedRender.resolve();
    chartQueuedRender={r,options:{...options},revision:chartViewportRevision,resolve};
  });
}
function releaseChartRenderQueue(){
  chartFullRenderInFlight=false;
  const queued=chartQueuedRender;chartQueuedRender=null;
  if(!queued){if((deferredIntradayPaint||deferredLiveChartUpdate)&&!chartPointerActive&&!chartApplyingRange)flushDeferredChartWork();return;}
  requestAnimationFrame(()=>{
    if(selected&&String(selected.label||'')!==String(queued.r?.label||'')){queued.resolve();return;}
    const preserveViewport=queued.options.preserveViewport!==false||chartViewportRevision!==queued.revision;
    Promise.resolve(renderChart(queued.r,{...queued.options,preserveViewport})).catch(()=>{}).finally(queued.resolve);
  });
}
function renderChart(r,options={}){
  if(chartFullRenderInFlight)return queueLatestChartRender(r,options);
  const renderToken=++chartRenderSeq;
  $('chartTitle').textContent=`${r.label} · Live OHLC & forecast`;
  const rawHistory=cleanHistoryForChart(r.history||[]),series=chartSeriesForTimeframe(r,rawHistory),fullMarketRows=series.timed?chartDisplayTimedBars(series.rows):series.rows;
  const rawForecast=(r.forecast||[]).filter(d=>d&&d.date&&Number.isFinite(Number(d.price)));
  if(!window.Plotly){$('chart').innerHTML='<div class="empty">Plotly could not load. Check internet connection for interactive charts.</div>';return;}
  if(!fullMarketRows.length){$('chart').innerHTML='<div class="empty">No clean chart history available for this ticker.</div>';return;}
  const chartEl=$('chart'),sameTicker=chartRenderedLabel===String(r.label||'')&&!!chartEl._fullLayout,preserveViewport=options.preserveViewport!==false,lockedViewport=sameTicker&&preserveViewport?activeManualChartViewport(chartEl):null;
  const preservedX=lockedViewport?.xRange||(sameTicker&&preserveViewport&&Array.isArray(chartEl._fullLayout?.xaxis?.range)?[...chartEl._fullLayout.xaxis.range]:null);
  const preservedY=lockedViewport?.yRange||(sameTicker&&preserveViewport&&Array.isArray(chartEl._fullLayout?.yaxis?.range)?[...chartEl._fullLayout.yaxis.range]:null);
  if(!sameTicker)chartEl.innerHTML='';
  const viewport=chartDefaultViewport(fullMarketRows,series.spec),marketRows=chartRowsForViewport(r,fullMarketRows,preservedX||viewport.range,series.spec),latest=fullMarketRows.at(-1),latestReal=String(latest.date),anchorDay=latestReal.slice(0,10),useLive=liveSafeForChart(r),baseClose=Number(latest.close||r._live_base_last||r.last||0),lastClose=Number(useLive?r.live_price:baseClose);
  const futureForecast=rawForecast.filter(d=>String(d.date).slice(0,10)>anchorDay),f=forecastRowsForChart(r,futureForecast.length?futureForecast:rawForecast,lastClose),fx=f.map(d=>String(d.date).slice(0,10));
  const minuteMeta=r._intraday_meta||{};
  $('chartSub').textContent=`${chartDisplaySpec(chartDisplayMode).label} · ${series.label} · ${minuteMeta.delayed?'delayed':'live/low-latency'} · ${chartZoneLabel(chartTimezone)} · ${r.signal} · confidence ${Number(r.confidence||0).toFixed(0)}/100${r.live_price&&!useLive?' · implausible quote excluded':''}`;
  const forecastX=[latestReal,...fx],forecastY=[lastClose,...f.map(d=>Number(d.price))],upperY=[lastClose,...f.map(d=>Number(d.high))],lowerY=[lastClose,...f.map(d=>Number(d.low))];
  const marketBundle=buildMarketTraces(marketRows,`${chartTimeframeSpec().label} market`,chartDisplayMode),data=[...marketBundle.traces];
  if(!marketBundle.traces.length){$('chart').innerHTML='<div class="empty">The selected chart style returned no visible market trace.</div>';return;}
  const marketRoleIndices=marketBundle.roles.map((role,index)=>({role,index})),forecastTraceStart=data.length;
  data.push(
    {type:'scatter',mode:'lines',x:forecastX,y:upperY,name:'Forecast upper band',line:{color:'rgba(41,98,255,.42)',dash:'dot',width:1},showlegend:false,hoverinfo:'skip',visible:chartForecastVisible},
    {type:'scatter',mode:'lines',x:forecastX,y:lowerY,name:'Forecast risk band',fill:'tonexty',fillcolor:'rgba(41,98,255,.10)',line:{color:'rgba(41,98,255,.42)',dash:'dot',width:1},showlegend:false,hoverinfo:'skip',visible:chartForecastVisible},
    {type:'scatter',mode:'lines+markers',x:forecastX,y:forecastY,name:'Forecast',line:{color:'#5b8cff',width:2},marker:{size:5,color:'#5b8cff'},connectgaps:false,hoverinfo:'skip',visible:chartForecastVisible}
  );
  const firstDate=String(fullMarketRows[0].date),forecastDataEnd=String(fx.at(-1)||latestReal),yr=viewport.yr||yRangeForChart(fullMarketRows,[],firstDate,latestReal,false),rangebreaks=chartSessionRangeBreaks(fullMarketRows,series.spec),rangeBreakSignature=chartRangeBreakSignature(rangebreaks);
  chartEl._apexDailyRows=series.timed?[]:fullMarketRows;chartEl._apexAdaptiveRows=[];chartEl._apexMinuteRows=series.timed?fullMarketRows:[];cacheChartCandleRows(chartEl,fullMarketRows);chartEl._apexMarketRoleIndices=marketRoleIndices;chartEl._apexIntradayRoleIndices=marketRoleIndices;
  chartEl._apexForecastTraceIndices=[forecastTraceStart,forecastTraceStart+1,forecastTraceStart+2];
  chartEl._apexRangeMeta={h:rawHistory,f,minute:series.timed?fullMarketRows:[],candleRows:fullMarketRows,renderRows:marketRows,sourceSignature:chartSeriesSourceSignature(series),anchorDate:latestReal,latestReal,latestRealMs:Number(latest.timestamp??parseDateMs(latestReal)),forecastEnd:forecastDataEnd,firstDate,lastHistDate:String(rawHistory.at(-1)?.date||anchorDay),label:String(r.label||''),timeframe:chartTimeframeKey,seriesTimed:series.timed,seriesLabel:series.label,rangeBreakSignature,paintSignature:chartRowsPaintSignature(marketRows,chartDisplayMode),paintInFlight:false,viewportRevision:chartViewportRevision};
  const decoration=chartDecorations(lastClose,latestReal,!!f.length);chartEl._apexBaseShapes=decoration.baseShapes;chartEl._apexForecastShapes=decoration.forecastShapes;chartEl._apexForecastAnnotations=decoration.forecastAnnotations;
  const shapes=chartForecastVisible?[...decoration.baseShapes,...decoration.forecastShapes]:decoration.baseShapes,annotations=chartForecastVisible?decoration.forecastAnnotations:[],maxVolume=Math.max(1,...marketRows.map(x=>Number(x.volume||0)));
  const plotTheme=visualThemeTokens(activeTheme()),layout={paper_bgcolor:plotTheme.panel,plot_bgcolor:plotTheme.panel,font:{color:plotTheme.muted,size:11},margin:{l:8,r:72,t:68,b:70},uirevision:`apex-chart-${String(r.label||'')}-${chartTimeframeKey}`,transition:{duration:0},hovermode:false,hoverdistance:-1,spikedistance:-1,showlegend:false,
    shapes,annotations,
    xaxis:{type:'date',range:preservedX||viewport.range||[firstDate,latestReal],rangebreaks,autorange:false,fixedrange:false,gridcolor:plotTheme.line,gridwidth:1,zeroline:false,showline:true,linecolor:plotTheme.line,ticks:'outside',tickcolor:plotTheme.line,tickfont:{color:plotTheme.muted,size:10},rangeslider:{visible:false}},
    yaxis:{side:'right',gridcolor:plotTheme.line,gridwidth:1,zeroline:false,showline:true,linecolor:plotTheme.line,ticks:'outside',tickcolor:plotTheme.line,tickfont:{color:plotTheme.muted,size:10},fixedrange:false,range:preservedY||yr,tickformat:'.2f'},
    yaxis2:{overlaying:'y',side:'left',range:[0,maxVolume*4.5],showgrid:false,zeroline:false,showticklabels:false,fixedrange:true},dragmode:'pan'};
  const config={responsive:false,displaylogo:false,displayModeBar:true,scrollZoom:false,doubleClick:'reset',showTips:true,modeBarButtonsToRemove:['select2d','lasso2d','hoverClosestCartesian','hoverCompareCartesian','toggleSpikelines']};
  chartFullRenderInFlight=true;
  let draw;
  try{draw=sameTicker?Plotly.react(chartEl,data,layout,config):Plotly.newPlot(chartEl,data,layout,config);}
  catch(error){releaseChartRenderQueue();chartEl.innerHTML='<div class="empty">The chart could not be rendered.</div>';return Promise.resolve();}
  const complete=Promise.resolve(draw).catch(()=>{
    const fallbackBundle=buildMarketTraces(marketRows,`${chartTimeframeSpec().label} market`,'line'),fallbackData=[...fallbackBundle.traces,...data.slice(forecastTraceStart)];
    chartEl._apexMarketRoleIndices=[{role:'close',index:0}];chartEl._apexIntradayRoleIndices=chartEl._apexMarketRoleIndices;chartEl._apexForecastTraceIndices=[1,2,3];
    return Plotly.newPlot(chartEl,fallbackData,layout,config);
  });
  return complete.then(async()=>{if(renderToken!==chartRenderSeq)return;chartRenderedLabel=String(r.label||'');chartEl.dataset.label=chartRenderedLabel;bindChartInteractionGuards(chartEl);const finalViewport=activeManualChartViewport(chartEl),finalRevision=chartViewportRevision;if(finalViewport)await restoreManualChartViewport(chartEl,finalViewport,finalRevision);await Promise.resolve(Plotly.relayout(chartEl,{dragmode:'pan'}));if(renderToken!==chartRenderSeq)return;Plotly.Plots.resize(chartEl);updateChartToolbarState();updateChartForecastToggle();updateChartOhlc(latest,r);requestAnimationFrame(()=>updateChartLastPrice(r,latest));scheduleChartArchiveForViewport(r,chartEl._fullLayout?.xaxis?.range);}).finally(releaseChartRenderQueue);
}
async function toggleChartFullscreen(){
  const panel=$('chartPanel');if(!panel)return;
  try{
    if(document.fullscreenElement)await document.exitFullscreen?.();
    else if(panel.requestFullscreen)await panel.requestFullscreen();
    else panel.classList.toggle('chartFull');
  }catch(e){panel.classList.toggle('chartFull');}
  requestChartResizeBurst();
}

function openPositionAdviceTab(){
  const win=window.open('/position-advice','apex_position_advice');
  if(!win)alert('Browser blocked the Position advice tab. Allow pop-ups for this dashboard.');
  else win.focus();
}

$('runBtn').onclick=run; $('stopBtn').onclick=stop; $('selectAll').onclick=()=>setAll(true); $('clearAll').onclick=()=>setAll(false); $('megaCaps').onclick=mega; $('tickerSearch').oninput=renderTickers; if($('returnRunBtn')) $('returnRunBtn').onclick=runReturnPrevision; if($('positionAdviceLaunch')) $('positionAdviceLaunch').onclick=openPositionAdviceTab; if($('chartFullBtn')) $('chartFullBtn').onclick=toggleChartFullscreen; document.addEventListener('fullscreenchange',requestChartResizeBurst); if($('rankSearch')) $('rankSearch').oninput=()=>renderResults({count:(allResults.length||tickers.length),results:(allResults.length?allResults:results)}); if($('rankSort')) $('rankSort').onchange=()=>renderResults({count:(allResults.length||tickers.length),results:(allResults.length?allResults:results)}); if($('rankOrder')) $('rankOrder').onchange=()=>renderResults({count:(allResults.length||tickers.length),results:(allResults.length?allResults:results)}); if($('selectedAnalysisHead')){ $('selectedAnalysisHead').onclick=openSelectedAnalysisSheet; $('selectedAnalysisHead').onkeydown=e=>{ if(e.key==='Enter'||e.key===' '){ e.preventDefault(); openSelectedAnalysisSheet(); } }; } if($('sideToggle')) $('sideToggle').onclick=()=>{ const app=document.querySelector('.app'); app.classList.toggle('side-collapsed'); requestChartResizeBurst(); if($('settingsPanel')?.classList.contains('open'))requestAnimationFrame(positionDisplaySettings); }; 

initDisplaySettings();
initChartToolbar();
initChartCrosshairWidthControl();
initChartForecastToggle();
initChartClock();
initChartLatencyControl();
loadTickers();
api('/api/auto-start',{method:'POST',body:JSON.stringify({})}).catch(()=>{});
</script>
</body></html>"""

POSITION_ADVICE_HTML = r"""<!doctype html>
<html lang="en-US">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width,initial-scale=1" />
<link rel="icon" type="image/png" sizes="400x400" href="/assets/apex-tool-logo.png?v=15" />
<link rel="apple-touch-icon" href="/assets/apex-tool-logo.png?v=15" />
<title>Position advice · Apex Predictor</title>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
<style>
:root{color-scheme:dark;--bg:#070a10;--panel:#0d111c;--control:#090e19;--section:#0f172a;--line:#263044;--theme-bg-rgb:7,10,16;--text:#edf4ff;--muted:#94a3b8;--cyan:#38bdf8;--blue:#3b82f6;--green:#22c55e;--red:#f43f5e;--amber:#f59e0b}
*{box-sizing:border-box}html,body{margin:0;min-height:100%;background:#070a10;color:var(--text);font-family:Inter,ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;letter-spacing:0}body{overflow-y:scroll}button,input{font:inherit}button{cursor:pointer}.topbar{position:sticky;top:0;z-index:30;min-height:64px;display:flex;align-items:center;justify-content:space-between;gap:18px;padding:10px 22px;border-bottom:1px solid var(--line);background:rgba(7,10,16,.94);backdrop-filter:blur(16px)}.brand{display:flex;align-items:center;gap:10px;min-width:0}.mark{width:36px;height:36px;display:grid;place-items:center;border-radius:6px;background:#0ea5e9;color:white;font-weight:900}.brand h1{margin:0;font-size:17px}.brand p{margin:3px 0 0;color:var(--muted);font-size:11px}.liveBadge{display:flex;align-items:center;gap:7px;height:34px;padding:0 11px;border:1px solid var(--line);border-radius:6px;color:#cbd5e1;font-size:11px;background:var(--control)}.liveDot{width:7px;height:7px;border-radius:50%;background:#64748b}.liveDot.on{background:var(--green);box-shadow:0 0 12px rgba(34,197,94,.65)}.page{width:min(1560px,100%);margin:0 auto;padding:18px 22px 32px}.controlBand{display:grid;grid-template-columns:minmax(240px,1.25fr) minmax(210px,.8fr) minmax(160px,.62fr) minmax(145px,.52fr) auto;gap:10px;align-items:end;padding-bottom:16px;border-bottom:1px solid var(--line)}label{display:grid;gap:6px;color:var(--muted);font-size:10px;text-transform:uppercase}input{width:100%;height:40px;padding:0 11px;border:1px solid var(--line);border-radius:6px;background:var(--control);color:var(--text);outline:none}input:focus{border-color:var(--cyan);box-shadow:0 0 0 3px rgba(56,189,248,.12)}.edgeSegment{height:40px;display:grid;grid-template-columns:1fr 1fr;padding:3px;border:1px solid var(--line);border-radius:6px;background:var(--control)}.edgeSegment button{min-width:0;border:0;border-radius:4px;background:transparent;color:#8492a7;font-size:11px;font-weight:750}.edgeSegment button.active{background:#172238;color:#f8fbff;box-shadow:inset 0 0 0 1px #33445f}.edgeSegment button[data-edge="long"].active{color:#bbf7d0;box-shadow:inset 0 0 0 1px rgba(34,197,94,.5)}.edgeSegment button[data-edge="short"].active{color:#fecdd3;box-shadow:inset 0 0 0 1px rgba(244,63,94,.45)}.refresh{width:40px;height:40px;border:1px solid var(--line);border-radius:6px;background:var(--control);color:#cbd5e1;display:grid;place-items:center}.refresh:hover{border-color:var(--cyan);color:var(--cyan)}.refresh svg{width:17px;height:17px}.refresh.busy svg{animation:spin .8s linear infinite}@keyframes spin{to{transform:rotate(360deg)}}.headline{display:flex;align-items:flex-start;justify-content:space-between;gap:20px;padding:18px 0 14px}.tickerTitle h2{margin:0;font-size:28px}.tickerTitle p{margin:5px 0 0;color:var(--muted);font-size:12px}.priceBlock{text-align:right}.currentPrice{font-size:28px;font-weight:900}.priceMeta{margin-top:4px;color:var(--muted);font-size:11px}.actionRow{display:flex;align-items:center;gap:9px;margin-top:10px}.action{display:inline-flex;align-items:center;min-height:28px;padding:0 9px;border:1px solid var(--line);border-radius:5px;font-size:11px;font-weight:850}.action.short{color:#fecdd3;border-color:rgba(244,63,94,.55);background:rgba(244,63,94,.10)}.action.long{color:#bbf7d0;border-color:rgba(34,197,94,.55);background:rgba(34,197,94,.10)}.action.watch,.action.wait{color:#fde68a;border-color:rgba(245,158,11,.5);background:rgba(245,158,11,.09)}.action.plan{color:#bae6fd;border-color:rgba(56,189,248,.48);background:rgba(56,189,248,.08)}.action.avoid{color:#cbd5e1}.setupName{color:#cbd5e1;font-size:12px}.metricGrid{display:grid;grid-template-columns:repeat(8,minmax(120px,1fr));gap:8px;margin-bottom:14px}.metric{min-height:82px;padding:11px;border:1px solid var(--line);border-radius:6px;background:var(--panel)}.metric span{display:block;color:var(--muted);font-size:9px;text-transform:uppercase}.metric b{display:block;margin-top:7px;font-size:17px}.metric small{display:block;margin-top:4px;color:#7f8da3;font-size:10px}.metric.entry{border-top:2px solid var(--cyan)}.metric.stop{border-top:2px solid var(--red)}.metric.target{border-top:2px solid var(--green)}.chartPanel{border-top:1px solid var(--line);border-bottom:1px solid var(--line);background:#0b0e14}.chartHead{height:46px;display:flex;align-items:center;justify-content:space-between;gap:14px;padding:0 10px}.chartHead h3{margin:0;font-size:13px}.ohlc{display:flex;gap:10px;color:var(--muted);font-size:10px;white-space:nowrap}.chart{height:610px;min-height:480px}.lowerGrid{display:grid;grid-template-columns:minmax(0,1.15fr) minmax(360px,.85fr);gap:18px;padding-top:18px}.block{min-width:0}.blockTitle{height:38px;display:flex;align-items:center;justify-content:space-between;border-bottom:1px solid var(--line)}.blockTitle h3{margin:0;font-size:13px}.blockTitle span{color:var(--muted);font-size:10px}.dataTable{width:100%;border-collapse:collapse}.dataTable th,.dataTable td{text-align:left;padding:9px 7px;border-bottom:1px solid rgba(38,48,68,.72);font-size:11px}.dataTable th{color:#8fa1ba;font-size:9px;text-transform:uppercase}.dataTable td:last-child{text-align:right;font-weight:750}.evidence{display:grid;gap:8px;padding-top:10px}.evidenceItem{padding:10px 0;border-bottom:1px solid rgba(38,48,68,.72);color:#cbd5e1;font-size:11px;line-height:1.45}.contribution{display:grid;grid-template-columns:minmax(130px,1fr) 100px 48px;align-items:center;gap:8px;padding:7px 0;border-bottom:1px solid rgba(38,48,68,.6);font-size:10px}.barTrack{height:5px;background:#172033;overflow:hidden}.barFill{height:100%;background:var(--cyan)}.barFill.neg{background:var(--red)}.riskLine{margin-top:12px;padding:10px;border-left:2px solid var(--amber);background:rgba(245,158,11,.06);color:#d7dee9;font-size:11px;line-height:1.45}.empty{display:none;margin:18px 0;padding:20px;border:1px solid var(--line);color:var(--muted);background:var(--panel)}.empty.show{display:block}.legalFooter{margin-top:20px;padding:14px 0 0;border-top:1px solid var(--line);color:var(--muted);font-size:9.5px;line-height:1.55}.legalFooter strong,.legalCopyright{color:var(--text);font-weight:750}.legalFooter p{max-width:1500px;margin:5px 0 0}.up{color:#4ade80}.down{color:#fb7185}.neutral{color:#fbbf24}@media(max-width:1180px){.controlBand{grid-template-columns:1.2fr 1fr 1fr 1fr auto}.metricGrid{grid-template-columns:repeat(4,minmax(120px,1fr))}.lowerGrid{grid-template-columns:1fr}.chart{height:540px}}@media(max-width:820px){.topbar{padding:9px 12px}.page{padding:12px}.controlBand{grid-template-columns:1fr 1fr}.controlBand label:first-child{grid-column:1/-1}.refresh{justify-self:end}.headline{align-items:flex-start}.metricGrid{grid-template-columns:repeat(2,minmax(0,1fr))}.chart{height:470px}.ohlc{display:none}.currentPrice,.tickerTitle h2{font-size:22px}}
</style>
<style id="sharedThemeOverrides">
html,body{background:var(--bg)}.topbar{background:rgba(var(--theme-bg-rgb),.94)}.mark{background:var(--cyan)}.edgeSegment button.active{background:var(--section);box-shadow:inset 0 0 0 1px var(--line)}.chartPanel{background:var(--panel)}.barTrack{background:var(--section)}.dataTable th,.dataTable td,.evidenceItem,.contribution{border-color:var(--line)}#positionChart .bg{fill:var(--panel)!important}#positionChart .gridlayer path,#positionChart .zerolinelayer path,#positionChart .xlines-below,#positionChart .ylines-below{stroke:var(--line)!important}
.mark{overflow:hidden;background:#07101d!important}.mark img{display:block;width:100%;height:100%;object-fit:cover}
body[data-visual-mode="light"] .edgeSegment button.active,body[data-visual-mode="light"] .policySegment button.active,body[data-visual-mode="light"] .candidateCard,body[data-visual-mode="light"] .refresh,body[data-visual-mode="light"] .positionCrosshairWidthBtn{color:var(--text)}body[data-visual-mode="light"] .setupName,body[data-visual-mode="light"] .riskLine,body[data-visual-mode="light"] .evidenceItem{color:var(--text)}body[data-visual-mode="light"] #positionChart text{fill:var(--muted)!important}
</style>
<style id="directionPolicyStyles">
.controlBand{grid-template-columns:minmax(260px,1.45fr) minmax(170px,.58fr) minmax(150px,.52fr) auto}.directionPolicy{padding:14px 0 16px;border-bottom:1px solid var(--line)}.policyHead{display:flex;align-items:center;justify-content:space-between;gap:14px;margin-bottom:10px}.policyHead h2{margin:0;color:var(--text);font-size:13px}.policyResolved{color:var(--muted);font-size:10px;text-align:right}.policyGrid{display:grid;grid-template-columns:minmax(250px,1fr) minmax(360px,1.45fr) minmax(250px,1fr);gap:10px}.policySegment{height:40px;display:grid;padding:3px;border:1px solid var(--line);border-radius:6px;background:var(--control)}.policySegment.three{grid-template-columns:repeat(3,minmax(0,1fr))}.policySegment.four{grid-template-columns:repeat(4,minmax(0,1fr))}.policySegment button{min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;border:0;border-radius:4px;background:transparent;color:#8492a7;font-size:11px;font-weight:750}.policySegment button.active{background:var(--section);color:var(--text);box-shadow:inset 0 0 0 1px var(--line)}.policySegment button[data-policy-value="long_only"].active{color:#bbf7d0;box-shadow:inset 0 0 0 1px rgba(34,197,94,.5)}.policySegment button[data-policy-value="short_only"].active{color:#fecdd3;box-shadow:inset 0 0 0 1px rgba(244,63,94,.45)}.policySegment button[data-policy-value="compare_both"].active{color:#bae6fd;box-shadow:inset 0 0 0 1px rgba(56,189,248,.48)}.policyNotice{margin:12px 0 0;padding:9px 11px;border-left:2px solid var(--amber);background:rgba(245,158,11,.07);color:#d7dee9;font-size:11px}.candidateCompare{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px;margin:12px 0 0}.candidateCard{min-height:72px;padding:10px 12px;text-align:left;border:1px solid var(--line);border-radius:6px;background:var(--panel);color:var(--text)}.candidateCard:hover{border-color:var(--cyan)}.candidateCard span,.candidateCard small{display:block;color:var(--muted);font-size:10px}.candidateCard b{display:block;margin:5px 0;font-size:13px}.candidateCard.long b{color:#bbf7d0}.candidateCard.short b{color:#fecdd3}.candidateCard.mismatch{opacity:.62}.candidateCard.unavailable{cursor:default;opacity:.5}.candidateCard.unavailable:hover{border-color:var(--line)}
@media(max-width:1180px){.controlBand{grid-template-columns:1.25fr .72fr .65fr auto}.policyGrid{grid-template-columns:1fr 1.35fr 1fr}}
@media(max-width:820px){.controlBand{grid-template-columns:1fr 1fr}.controlBand label:first-child{grid-column:1/-1}.policyHead{align-items:flex-start;flex-direction:column}.policyResolved{text-align:left}.policyGrid{grid-template-columns:1fr}.candidateCompare{grid-template-columns:1fr}}
</style>
<style id="positionCrosshairStyles">
.positionChartStage{--position-crosshair-width:.55;position:relative;overflow:hidden}
.positionChartHeadTools{margin-left:auto;display:flex;align-items:center;justify-content:flex-end;gap:9px;min-width:0}
.positionCrosshairWidthControl{position:relative;flex:0 0 auto}
.positionCrosshairWidthBtn{height:30px;min-width:58px;border:1px solid var(--line);border-radius:5px;background:var(--control);color:#cbd5e1;padding:0 7px;display:flex;align-items:center;justify-content:center;gap:5px;font-size:9px;font-weight:750;font-variant-numeric:tabular-nums}
.positionCrosshairWidthBtn:hover,.positionCrosshairWidthBtn[aria-expanded="true"]{border-color:var(--cyan);color:#f8fbff;background:#121a28}
.positionCrosshairWidthBtn svg{width:15px;height:15px;display:block;stroke:currentColor}
.positionCrosshairWidthMenu{display:none;position:absolute;top:35px;right:0;z-index:25;width:215px;padding:10px;border:1px solid var(--line);border-radius:6px;background:var(--panel);box-shadow:0 14px 36px rgba(0,0,0,.48)}
.positionCrosshairWidthMenu.open{display:grid;gap:8px}
.positionCrosshairWidthMenu label{display:flex;align-items:center;justify-content:space-between;gap:10px;color:#aeb6c2;font-size:10px;font-weight:700;text-transform:none}
.positionCrosshairWidthMenu output{color:#f3f4f6;font-variant-numeric:tabular-nums}
.positionCrosshairWidthMenu input[type="range"]{width:100%;height:18px;margin:0;padding:0;border:0;background:transparent;box-shadow:none;accent-color:var(--cyan);cursor:pointer}
.positionCrosshairWidthMenu input[type="range"]:focus{border:0;box-shadow:none}
.positionCrosshair{position:absolute;inset:0;z-index:8;visibility:hidden;opacity:0;pointer-events:none;contain:layout style;transform:translateZ(0)}
.positionCrosshair.visible{visibility:visible;opacity:1}
.positionCrosshairV,.positionCrosshairH{position:absolute;left:0;top:0;transform-origin:0 0;will-change:transform;backface-visibility:hidden;opacity:.68}
.positionCrosshairV{width:1px;background:repeating-linear-gradient(to bottom,rgba(207,214,224,.62) 0 1px,transparent 1px 5px)}
.positionCrosshairH{height:1px;background:repeating-linear-gradient(to right,rgba(207,214,224,.62) 0 1px,transparent 1px 5px)}
.positionCrosshairLabel{position:absolute;left:0;top:0;z-index:1;padding:4px 6px;border:0;border-radius:2px;background:#2d3440;color:#f3f4f6;font-size:10px;font-weight:650;line-height:1;font-variant-numeric:tabular-nums;white-space:nowrap;will-change:transform;backface-visibility:hidden}
@media(max-width:820px){.positionChartHeadTools{gap:6px}.positionCrosshairWidthBtn{min-width:52px}.positionCrosshairWidthMenu{right:0}}
</style>
</head>
<body>
<header class="topbar"><div class="brand"><div class="mark" aria-label="Apex Tool"><img src="/assets/apex-tool-logo.png" alt="Apex Tool"></div><div><h1>Position advice</h1><p id="edgeSubtitle">Intraday · Auto direction · Auto style</p></div></div><div class="liveBadge"><span class="liveDot" id="liveDot"></span><span id="liveStatus">Connecting</span></div></header>
<main class="page">
  <section class="controlBand">
    <label>Ticker<input id="tickerInput" list="tickerOptions" autocomplete="off" spellcheck="false" placeholder="Search ticker or company"><datalist id="tickerOptions"></datalist></label>
    <label>Account value USD<input id="accountValue" type="number" min="0" max="1000000000" step="1000" value="100000"></label>
    <label>Risk per trade %<input id="riskPercent" type="number" min="0.05" max="5" step="0.05" value="0.50"></label>
    <button class="refresh" id="refreshBtn" type="button" title="Refresh advice" aria-label="Refresh advice"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M21 12a9 9 0 0 1-15.2 6.5L3 16"/><path d="M3 21v-5h5"/><path d="M3 12A9 9 0 0 1 18.2 5.5L21 8"/><path d="M21 3v5h-5"/></svg></button>
  </section>
  <section class="directionPolicy" id="directionPolicyPanel" aria-labelledby="directionPolicyTitle">
    <div class="policyHead"><h2 id="directionPolicyTitle">Direction policy</h2><span class="policyResolved" id="policyResolved">Intraday · Auto · Auto</span></div>
    <div class="policyGrid">
      <label>Horizon<div class="policySegment three" id="horizonPolicy"><button type="button" data-policy-group="horizon" data-policy-value="intraday">Intraday</button><button type="button" data-policy-group="horizon" data-policy-value="swing">Swing</button><button type="button" data-policy-group="horizon" data-policy-value="long_term">Long term</button></div></label>
      <label>Direction<div class="policySegment four" id="directionPolicyOptions"><button type="button" data-policy-group="direction" data-policy-value="auto">Auto</button><button type="button" data-policy-group="direction" data-policy-value="long_only">Long / Call</button><button type="button" data-policy-group="direction" data-policy-value="short_only">Short only</button><button type="button" data-policy-group="direction" data-policy-value="compare_both">Compare both</button></div></label>
      <label>Style<div class="policySegment three" id="stylePolicy"><button type="button" data-policy-group="style" data-policy-value="trend">Trend</button><button type="button" data-policy-group="style" data-policy-value="countertrend">Countertrend</button><button type="button" data-policy-group="style" data-policy-value="auto">Auto</button></div></label>
    </div>
    <div class="policyNotice" id="policyNotice" hidden></div>
    <div class="candidateCompare" id="candidateCompare" hidden></div>
  </section>
  <div class="empty" id="emptyState"></div>
  <section id="adviceView" hidden>
    <div class="headline"><div class="tickerTitle"><h2 id="tickerTitle">—</h2><p id="tickerMeta">—</p><div class="actionRow"><span class="action" id="actionBadge">—</span><span class="setupName" id="setupName">—</span></div></div><div class="priceBlock"><div class="currentPrice" id="currentPrice">—</div><div class="priceMeta" id="priceMeta">—</div></div></div>
    <div class="metricGrid">
      <div class="metric entry"><span>Entry zone</span><b id="entryZone">—</b><small id="entryState">—</small></div>
      <div class="metric stop"><span>Stop loss</span><b id="stopLoss">—</b><small>Hard invalidation</small></div>
      <div class="metric target"><span>Take profit 1</span><b id="takeProfit1">—</b><small id="gainTp1">—</small></div>
      <div class="metric target"><span>Take profit 2</span><b id="takeProfit2">—</b><small>Extended target</small></div>
      <div class="metric"><span>Gain probability</span><b id="gainProbability">—</b><small>Estimated, evidence-decayed</small></div>
      <div class="metric"><span>Reward / risk</span><b id="rewardRisk">—</b><small>After slippage allowance</small></div>
      <div class="metric"><span>Risk budget</span><b id="riskBudget">—</b><small id="lossAtStop">—</small></div>
      <div class="metric"><span>Units ceiling</span><b id="unitsCeiling">—</b><small id="notional">—</small></div>
    </div>
    <section class="chartPanel"><div class="chartHead"><h3 id="chartTitle">One-day real-time market path</h3><div class="ohlc" id="ohlcStrip"></div></div><div class="positionChartStage" id="positionChartStage"><div class="chart" id="positionChart"></div><div class="positionCrosshair" id="positionCrosshair" aria-hidden="true"><div class="positionCrosshairV" id="positionCrosshairV"></div><div class="positionCrosshairH" id="positionCrosshairH"></div><div class="positionCrosshairLabel" id="positionCrosshairX"></div><div class="positionCrosshairLabel" id="positionCrosshairY"></div></div></div></section>
    <div class="lowerGrid">
      <section class="block"><div class="blockTitle"><h3>Technical dataframe</h3><span id="calcLatency">—</span></div><table class="dataTable"><thead><tr><th>Feature</th><th>Interpretation</th><th>Value</th></tr></thead><tbody id="technicalRows"></tbody></table></section>
      <section class="block"><div class="blockTitle"><h3>Signal proof</h3><span id="engineStatus">—</span></div><div class="evidence" id="drivers"></div><div id="contributions"></div><div class="riskLine" id="riskChecks"></div></section>
    </div>
  </section>
  <footer class="legalFooter" id="legalDisclaimer" aria-label="Legal notice">
    <div class="legalCopyright">© 2026 Apex Tool. All rights reserved.</div>
    <p><strong>Professional analytics notice.</strong> Position advice is a model-generated data-analysis and decision-support service for informational and professional use only. Entry zones, stops, targets, probabilities and risk budgets are conditional estimates that may rely on delayed, incomplete, estimated or third-party data and may change without notice. They do not constitute personalised investment advice, regulated investment research, an offer, solicitation, guarantee, official or contractual investment document, or any obligation to open, maintain or close a position. The platform does not transmit or execute orders.</p>
    <p>Trading and investing involve risk, including possible loss of capital. Past, simulated and forecast performance is not a reliable indicator of future results. Users remain responsible for independent verification, suitability, compliance and execution decisions and should consult appropriately authorised financial, legal and tax professionals. To the fullest extent permitted by applicable law, Apex Tool disclaims liability for losses or damages arising from use of, interruption of, or reliance on the platform or its data.</p>
  </footer>
</main>
<script>
const $=id=>document.getElementById(id);
const positionQuery=new URLSearchParams(location.search),legacyEdge=(positionQuery.get('edge')||'').toLowerCase();
const POLICY_VALUES={horizon:['intraday','swing','long_term'],direction:['auto','long_only','short_only','compare_both'],style:['trend','countertrend','auto']};
const POLICY_LABELS={intraday:'Intraday',swing:'Swing',long_term:'Long term',auto:'Auto',long_only:'Long / Call',short_only:'Short only',compare_both:'Compare both',trend:'Trend',countertrend:'Countertrend'};
function initialPolicyValue(group,fallback){const queryValue=(positionQuery.get(group)||'').toLowerCase(),saved=(localStorage.getItem(`apex-position-${group}`)||'').toLowerCase(),value=queryValue||saved||fallback;return POLICY_VALUES[group].includes(value)?value:fallback;}
const legacyPolicy=legacyEdge==='long'?{horizon:'long_term',direction:'long_only',style:'trend'}:legacyEdge==='short'?{horizon:'intraday',direction:'short_only',style:'trend'}:{horizon:'intraday',direction:'auto',style:'auto'};
const initialPolicy={horizon:initialPolicyValue('horizon',legacyPolicy.horizon),direction:initialPolicyValue('direction',legacyPolicy.direction),style:initialPolicyValue('style',legacyPolicy.style)};
const state={tickers:[],ticker:null,payload:null,payloads:{short:null,long:null,long_intraday:null},policy:initialPolicy,edge:'short',stream:null,request:null,poll:null,chartFrame:0,lastChartPaint:0,inputTimer:null,comparisonUpdatedAt:{short:0,long:0,long_intraday:0},comparisonErrors:{short:null,long:null,long_intraday:null}};
const VISUAL_THEME_STORAGE_KEY='apex-visual-theme-tokens';
function applyStoredVisualTheme(){
  try{
    const payload=JSON.parse(localStorage.getItem(VISUAL_THEME_STORAGE_KEY)||'{}'),root=document.documentElement,color=/^#[0-9a-f]{6}$/i,rgb=/^\d{1,3},\d{1,3},\d{1,3}$/;
    const properties={'--bg':payload.bg,'--panel':payload.panel,'--control':payload.control,'--section':payload.section,'--line':payload.line,'--text':payload.text,'--muted':payload.muted,'--blue':payload.accent,'--cyan':payload.accent2};
    Object.entries(properties).forEach(([name,value])=>{if(color.test(String(value||'')))root.style.setProperty(name,value);});
    if(rgb.test(String(payload.bgRgb||'')))root.style.setProperty('--theme-bg-rgb',payload.bgRgb);
    root.style.colorScheme=payload.scheme==='light'?'light':'dark';
    if(document.body)document.body.dataset.visualMode=['dark','balanced','light'].includes(payload.mode)?payload.mode:'dark';
  }catch(error){}
}
function positionPlotTheme(){const style=getComputedStyle(document.documentElement);return {panel:style.getPropertyValue('--panel').trim()||'#0b0e14',line:style.getPropertyValue('--line').trim()||'#242933',text:style.getPropertyValue('--text').trim()||'#edf4ff',muted:style.getPropertyValue('--muted').trim()||'#aeb6c2'};}
const POSITION_CROSSHAIR_WIDTH_STORAGE_KEY='apex-crosshair-width';
let positionCrosshairWidth=.55;
function positionCrosshairWidthValue(value,fallback=.55){const raw=String(value??'').trim(),numeric=raw===''?NaN:Number(raw);return Number.isFinite(numeric)?Math.max(.5,Math.min(3,numeric)):fallback;}
function positionCrosshairWidthLabel(value){return `${Number(value).toFixed(2).replace(/\.?0+$/,'')} px`;}
function closePositionCrosshairWidthMenu(){const menu=$('positionCrosshairWidthMenu'),button=$('positionCrosshairWidthBtn');menu?.classList.remove('open');button?.setAttribute('aria-expanded','false');}
function setPositionCrosshairWidth(value,persist=true){
  positionCrosshairWidth=positionCrosshairWidthValue(value,positionCrosshairWidth);
  $('positionChartStage')?.style.setProperty('--position-crosshair-width',String(positionCrosshairWidth));
  const input=$('positionCrosshairWidthInput'),output=$('positionCrosshairWidthOutput'),buttonValue=$('positionCrosshairWidthValue'),label=positionCrosshairWidthLabel(positionCrosshairWidth);
  if(input){input.value=String(positionCrosshairWidth);input.setAttribute('aria-valuetext',label);}
  if(output)output.textContent=label;if(buttonValue)buttonValue.textContent=label.replace(' px','');
  if(persist){try{localStorage.setItem(POSITION_CROSSHAIR_WIDTH_STORAGE_KEY,String(positionCrosshairWidth));}catch(error){}}
  $('positionChartStage')?._positionCrosshairRepaint?.();
}
function initPositionCrosshairWidthControl(){
  const stage=$('positionChartStage'),head=stage?.parentElement?.querySelector('.chartHead');if(!stage||!head||$('positionCrosshairWidthControl'))return;
  let tools=head.querySelector('.positionChartHeadTools');if(!tools){tools=document.createElement('div');tools.className='positionChartHeadTools';const ohlc=$('ohlcStrip');if(ohlc)tools.appendChild(ohlc);head.appendChild(tools);}
  const control=document.createElement('div');control.id='positionCrosshairWidthControl';control.className='positionCrosshairWidthControl';
  control.innerHTML=`<button class="positionCrosshairWidthBtn" id="positionCrosshairWidthBtn" type="button" aria-haspopup="true" aria-expanded="false" aria-controls="positionCrosshairWidthMenu" title="Crosshair thickness"><svg class="lucide lucide-crosshair" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" aria-hidden="true"><circle cx="12" cy="12" r="8"/><path d="M22 12h-4M6 12H2M12 6V2M12 22v-4"/></svg><span id="positionCrosshairWidthValue">0.55</span></button><div class="positionCrosshairWidthMenu" id="positionCrosshairWidthMenu" role="group" aria-label="Crosshair thickness"><label for="positionCrosshairWidthInput"><span>Crosshair thickness</span><output id="positionCrosshairWidthOutput">0.55 px</output></label><input id="positionCrosshairWidthInput" type="range" min="0.5" max="3" step="0.05" value="0.55" aria-label="Crosshair thickness"></div>`;
  tools.appendChild(control);
  const button=$('positionCrosshairWidthBtn'),menu=$('positionCrosshairWidthMenu'),input=$('positionCrosshairWidthInput');
  button.onclick=event=>{event.stopPropagation();const open=menu.classList.toggle('open');button.setAttribute('aria-expanded',String(open));};
  input.addEventListener('input',event=>setPositionCrosshairWidth(event.target.value));control.addEventListener('click',event=>event.stopPropagation());
  document.addEventListener('click',event=>{if(!control.contains(event.target))closePositionCrosshairWidthMenu();});document.addEventListener('keydown',event=>{if(event.key==='Escape')closePositionCrosshairWidthMenu();});
  window.addEventListener('storage',event=>{if(event.key===POSITION_CROSSHAIR_WIDTH_STORAGE_KEY)setPositionCrosshairWidth(event.newValue,false);});
  let saved=null;try{saved=localStorage.getItem(POSITION_CROSSHAIR_WIDTH_STORAGE_KEY);}catch(error){}setPositionCrosshairWidth(saved,false);
}
applyStoredVisualTheme();
window.addEventListener('storage',event=>{if(event.key===VISUAL_THEME_STORAGE_KEY){applyStoredVisualTheme();if(state.payload)requestAnimationFrame(()=>drawChart(state.payload));}});
const esc=value=>String(value??'').replace(/[&<>"']/g,char=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[char]));
const num=value=>Number.isFinite(Number(value))?Number(value):null;
const POSITION_CHART_TIMEZONE='Europe/Paris',POSITION_CHART_ZONE_LABEL='Paris';
const positionZonePartsFormatters=new Map(),positionZoneOffsetCache=new Map();
function positionZoneOffsetMinutes(ms,zone=POSITION_CHART_TIMEZONE){const value=Number(ms),timezone=String(zone||POSITION_CHART_TIMEZONE);if(!Number.isFinite(value)||timezone==='UTC')return 0;const bucket=Math.floor(value/3600000),key=`${timezone}|${bucket}`;if(positionZoneOffsetCache.has(key))return positionZoneOffsetCache.get(key);try{let formatter=positionZonePartsFormatters.get(timezone);if(!formatter){formatter=new Intl.DateTimeFormat('en-CA',{timeZone:timezone,year:'numeric',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit',second:'2-digit',hourCycle:'h23'});positionZonePartsFormatters.set(timezone,formatter);}const values={};formatter.formatToParts(new Date(value)).forEach(part=>{if(part.type!=='literal')values[part.type]=Number(part.value);});const wallAsUtc=Date.UTC(values.year,values.month-1,values.day,values.hour,values.minute,values.second),offset=Math.round((wallAsUtc-Math.floor(value/1000)*1000)/60000);positionZoneOffsetCache.set(key,offset);return offset;}catch(error){return 0;}}
function positionChartDate(value,zone=POSITION_CHART_TIMEZONE){const ms=Number(value)||Date.parse(String(value||''));return Number.isFinite(ms)?new Date(ms+positionZoneOffsetMinutes(ms,zone)*60000).toISOString():String(value||'');}
function positionSourceTimestamp(row){const numeric=Number(row?._source_timestamp??row?.timestamp);return Number.isFinite(numeric)?numeric:Date.parse(String(row?.date||''));}
function price(value){const n=num(value);if(n===null)return '—';const digits=n>=1000?2:n>=10?3:n>=1?4:6;return n.toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:digits});}
function pct(value,digits=1){const n=num(value);return n===null?'—':`${(n*100).toFixed(digits)}%`;}
function money(value){const n=num(value);return n===null?'—':n.toLocaleString('en-US',{style:'currency',currency:'USD',maximumFractionDigits:0});}
function setText(id,value){const el=$(id);if(el&&el.textContent!==String(value))el.textContent=String(value);}
async function api(url,options){const response=await fetch(url,options);const payload=await response.json();if(!response.ok)throw new Error(payload.error||`HTTP ${response.status}`);return payload;}
function selectedTicker(){const raw=$('tickerInput').value.trim().toUpperCase();return state.tickers.find(row=>row.label===raw)||state.tickers.find(row=>`${row.label} · ${row.name}`.toUpperCase()===raw)||null;}
function policyPrimaryEdge(policy=state.policy){if(policy.direction==='long_only')return policy.horizon==='intraday'?'long_intraday':'long';if(policy.direction==='short_only')return 'short';return policy.horizon==='intraday'?'short':'long';}
function policyEdges(policy=state.policy){const primary=policyPrimaryEdge(policy);if(policy.direction!=='compare_both')return [primary];const longEdge=policy.horizon==='intraday'?'long_intraday':'long';return primary==='short'?[primary,longEdge]:[primary,'short'];}
function policySignature(){return `${state.policy.horizon}|${state.policy.direction}|${state.policy.style}`;}
function payloadNativeHorizon(payload){return payload?.edge==='long'?'long_term':'intraday';}
function payloadStrategyStyle(payload){
  const t=payload?.technical||{},side=String(payload?.advice?.side||'').toUpperCase();
  if(payload?.edge==='long'){
    const votes=[],compare=(left,right)=>{const a=num(left),b=num(right);if(a!==null&&b!==null)votes.push(a>=b?1:-1);};
    compare(payload.current_price,t.sma_200);
    compare(t.sma_50,t.sma_200);
    compare(t.momentum_3m_pct,0);
    const direction=votes.reduce((total,value)=>total+value,0);
    if(!votes.length||direction===0)return 'unknown';
    return side==='LONG'?(direction>0?'trend':'countertrend'):(direction<0?'trend':'countertrend');
  }
  if(payload?.edge==='long_intraday'){
    const evidence=[],bullishVote=(value,weight)=>{const n=num(value);if(n!==null&&n!==0)evidence.push(Math.sign(n)*weight);};
    bullishVote(num(t.vwap)!==null&&num(payload.current_price)!==null?Number(payload.current_price)-Number(t.vwap):null,3.0);
    bullishVote(payload?.session?.delta_pct,3.0);
    bullishVote(t.momentum_30m_pct,2.5);
    bullishVote(t.long_edge,2.5);
    bullishVote(t.slope_15m_pct_per_bar,1.5);
    const probabilityUp=num(payload?.model_context?.intraday_probability_up)??num(payload?.model_context?.probability_up);
    bullishVote(probabilityUp===null?null:probabilityUp-0.5,1.0);
    bullishVote(num(t.ema_8)!==null&&num(t.ema_21)!==null?Number(t.ema_8)-Number(t.ema_21):null,0.75);
    bullishVote(t.momentum_15m_pct,0.75);
    const bullishScore=evidence.reduce((total,value)=>total+value,0),continuationSetup=/(bullish pullback|breakout retest|failed selloff|bullish rejection)/i.test(String(payload?.advice?.setup||''));
    if(!evidence.length||bullishScore===0)return continuationSetup?'trend':'unknown';
    return bullishScore>0?'trend':'countertrend';
  }
  const evidence=[],bearishVote=(value,weight)=>{const n=num(value);if(n!==null&&n!==0)evidence.push(Math.sign(n)*weight);};
  bearishVote(num(t.vwap)!==null&&num(payload.current_price)!==null?Number(t.vwap)-Number(payload.current_price):null,3.0);
  bearishVote(num(payload?.session?.delta_pct)!==null?-Number(payload.session.delta_pct):null,3.0);
  bearishVote(num(t.momentum_30m_pct)!==null?-Number(t.momentum_30m_pct):null,2.5);
  bearishVote(t.short_edge,2.5);
  bearishVote(num(t.slope_15m_pct_per_bar)!==null?-Number(t.slope_15m_pct_per_bar):null,1.5);
  bearishVote(num(payload?.model_context?.probability_down)!==null?Number(payload.model_context.probability_down)-0.5:null,1.0);
  bearishVote(num(t.ema_21)!==null&&num(t.ema_8)!==null?Number(t.ema_21)-Number(t.ema_8):null,0.75);
  bearishVote(num(t.momentum_15m_pct)!==null?-Number(t.momentum_15m_pct):null,0.75);
  const bearishScore=evidence.reduce((total,value)=>total+value,0),continuationSetup=/(bearish pullback|breakdown retest|failed rebound|bearish rejection)/i.test(String(payload?.advice?.setup||''));
  if(!evidence.length||bearishScore===0)return continuationSetup?'trend':'unknown';
  return bearishScore>0?'trend':'countertrend';
}
function policyCompatibility(payload,policy=state.policy){const edge=payload?.edge,nativeHorizon=payloadNativeHorizon(payload),actualStyle=payloadStrategyStyle(payload),horizonMatch=policy.horizon==='intraday'?edge==='short'||edge==='long_intraday':edge==='long',horizonProxy=policy.horizon==='swing'&&edge==='long',directionMatch=policy.direction==='auto'||policy.direction==='compare_both'||(policy.direction==='long_only'&&(edge==='long'||edge==='long_intraday'))||(policy.direction==='short_only'&&edge==='short'),styleMatch=policy.style==='auto'||actualStyle===policy.style;return {compatible:Boolean(payload?.ok&&horizonMatch&&directionMatch&&styleMatch),native_horizon:nativeHorizon,actual_style:actualStyle,horizon_match:horizonMatch,horizon_proxy:horizonProxy,direction_match:directionMatch,style_match:styleMatch};}
function policyDisplayPayload(payload){if(!payload?.ok)return payload;const meta=policyCompatibility(payload),display={...payload,policy_meta:{...meta,requested:{...state.policy}}};if(meta.compatible)return display;const originalAdvice={...(payload.advice||{})},reason=!meta.horizon_match?'HORIZON':!meta.direction_match?'DIRECTION':'STYLE',waitingForBearishTrend=reason==='STYLE'&&payload.edge==='short'&&state.policy.style==='trend',waitingForBullishTrend=reason==='STYLE'&&payload.edge==='long_intraday'&&state.policy.style==='trend',waitingForTrend=waitingForBearishTrend||waitingForBullishTrend;display.engine_advice=originalAdvice;display.advice={...originalAdvice,action:waitingForBearishTrend?'WAIT - BEARISH TREND':waitingForBullishTrend?'WAIT - BULLISH TREND':`NO TRADE - ${reason} FILTER`,action_code:waitingForTrend?'wait':'avoid',setup:waitingForBearishTrend?'Trend mode active · bearish short-limit confirmation required':waitingForBullishTrend?'Trend mode active · bullish long-limit confirmation required':`${originalAdvice.setup||'Engine candidate'} · retained for audit only`};return display;}
function policyNoticeText(payload){const meta=payload?.policy_meta;if(!meta)return '';const edgeName=payload.edge==='long_intraday'?'Intraday Long / Call edge':payload.edge==='long'?'Long edge':'Short edge';if(!meta.horizon_match)return `No setup is forced: ${edgeName} does not cover the selected ${POLICY_LABELS[state.policy.horizon]} horizon.`;if(!meta.direction_match)return `No setup is forced: ${edgeName} conflicts with ${POLICY_LABELS[state.policy.direction]}.`;if(!meta.style_match&&payload.edge==='short'&&state.policy.style==='trend')return 'Trend mode is active. The current structure is still countertrend, so the plan waits for bearish alignment instead of issuing a countertrend entry.';if(!meta.style_match&&payload.edge==='long_intraday'&&state.policy.style==='trend')return 'Trend mode is active. The current structure is still countertrend, so the plan waits for bullish alignment instead of issuing a countertrend entry.';if(!meta.style_match)return `No setup is forced: the engine classifies this candidate as ${POLICY_LABELS[meta.actual_style]||'Unclassified'}, while ${POLICY_LABELS[state.policy.style]} was requested.`;if(meta.horizon_proxy)return 'Swing uses the existing Long edge evidence as a proxy; its original no-fixed-expiry exit discipline remains unchanged.';return '';}
function renderPolicyNotice(payload){const notice=$('policyNotice'),text=policyNoticeText(payload);notice.hidden=!text;if(text)notice.textContent=text;}
function renderComparison(){const container=$('candidateCompare');if(!container)return;const comparing=state.policy.direction==='compare_both';container.hidden=!comparing;if(!comparing){container.innerHTML='';return;}const edges=state.policy.horizon==='intraday'?['long_intraday','short']:['long','short'];container.innerHTML=edges.map(edge=>{const payload=state.payloads[edge],error=state.comparisonErrors[edge],isLong=edge!=='short',sideClass=isLong?'long':'short',candidateName=edge==='long_intraday'?'Long / Call':isLong?'Long':'Short';if(!payload?.ok)return `<button type="button" class="candidateCard ${sideClass} unavailable" disabled><span>${candidateName} candidate</span><b>Unavailable</b><small>${esc(error||'Waiting for engine response')}</small></button>`;const advice=payload.advice||{},meta=policyCompatibility(payload),native=payloadNativeHorizon(payload)==='intraday'?'Intraday':'Long term',style=POLICY_LABELS[meta.actual_style]||'Unclassified',match=meta.compatible?'Policy match':meta.horizon_proxy&&meta.style_match?'Swing proxy':'Outside policy';return `<button type="button" class="candidateCard ${sideClass} ${meta.compatible?'':'mismatch'}" data-choose-direction="${isLong?'long_only':'short_only'}" title="Use ${candidateName} direction"><span>${candidateName} candidate · ${native} · ${style}</span><b>${esc(advice.action||'Candidate')}</b><small>${pct(advice.gain_probability)} gain · ${Number(advice.reward_risk_tp1||0).toFixed(2)} R · EV ${Number(advice.expected_value_r||0).toFixed(2)} R · ${match}</small></button>`;}).join('');}
function updatePositionUrl(){if(!state.ticker)return;const params=new URLSearchParams({ticker:state.ticker.label,horizon:state.policy.horizon,direction:state.policy.direction,style:state.policy.style});history.replaceState(null,'',`?${params.toString()}`);}
function syncPolicyUI(){state.edge=policyPrimaryEdge();document.querySelectorAll('button[data-policy-group]').forEach(button=>button.classList.toggle('active',state.policy[button.dataset.policyGroup]===button.dataset.policyValue));const summary=`${POLICY_LABELS[state.policy.horizon]} · ${POLICY_LABELS[state.policy.direction]} · ${POLICY_LABELS[state.policy.style]}`,engineName=state.edge==='long_intraday'?'Intraday Long / Call':state.edge==='long'?'Long-term Long':'Intraday Short',resolved=state.policy.direction==='compare_both'?'Long / Call + Short engine comparison':`${engineName} engine`;setText('edgeSubtitle',summary);setText('policyResolved',`${summary} · ${resolved}`);renderComparison();}
function setPolicy(group,value){if(!POLICY_VALUES[group]?.includes(value)||state.policy[group]===value){syncPolicyUI();return;}state.policy={...state.policy,[group]:value};localStorage.setItem(`apex-position-${group}`,value);state.payload=null;syncPolicyUI();updatePositionUrl();if(state.ticker)loadAdvice();}
async function loadTickers(){const payload=await api('/api/tickers');state.tickers=payload.tickers||[];$('tickerOptions').innerHTML=state.tickers.map(row=>`<option value="${esc(row.label)}">${esc(row.name)} · ${esc(row.exchange)}</option>`).join('');syncPolicyUI();const query=positionQuery.get('ticker')?.toUpperCase(),saved=localStorage.getItem('apex-position-ticker')?.toUpperCase();const initial=state.tickers.find(row=>row.label===(query||saved))||state.tickers.find(row=>row.label==='AAPL')||state.tickers[0];if(initial){$('tickerInput').value=initial.label;selectTicker(initial);}}
function selectTicker(row){if(!row)return;state.ticker=row;state.payload=null;state.payloads={short:null,long:null,long_intraday:null};state.comparisonUpdatedAt={short:0,long:0,long_intraday:0};state.comparisonErrors={short:null,long:null,long_intraday:null};localStorage.setItem('apex-position-ticker',row.label);updatePositionUrl();closeStream();openStream();loadAdvice();}
function status(text,on=false){setText('liveStatus',text);$('liveDot').classList.toggle('on',on);}
function showError(message){$('emptyState').textContent=message;$('emptyState').classList.add('show');$('adviceView').hidden=true;}
function clearError(){$('emptyState').classList.remove('show');$('adviceView').hidden=false;}
function closeStream(){if(state.stream){state.stream.close();state.stream=null;}}
function openStream(){if(!state.ticker||!window.EventSource)return;closeStream();const url=`/api/live-stream?labels=${encodeURIComponent(state.ticker.label)}&preferred=auto&cadence=250&client=position-advice`;const source=new EventSource(url);state.stream=source;source.onopen=()=>status('Live stream connected',true);source.onerror=()=>status('Fallback polling',false);source.onmessage=event=>{try{const payload=JSON.parse(event.data),quote=(payload.prices||{})[state.ticker.label];if(quote)applyQuote(quote);}catch(error){}};}
function mergeQuoteBar(quote){if(!state.payload?.chart?.bars?.length)return;const bars=state.payload.chart.bars,isLong=state.payload.edge==='long',bucket=isLong?3600000:60000,ts=Math.floor(Number(quote.bar_start||quote.updated_at||Date.now())/bucket)*bucket,latest=bars.at(-1);if(!isLong){if(Number(latest.timestamp)===ts){latest.open=num(quote.open)??latest.open;latest.high=Math.max(num(quote.high)??quote.price,latest.high,quote.price);latest.low=Math.min(num(quote.low)??quote.price,latest.low,quote.price);latest.close=quote.price;latest.volume=Math.max(num(quote.bar_volume)??0,num(latest.volume)??0);latest.live=true;}else if(ts>Number(latest.timestamp)){bars.push({timestamp:ts,date:new Date(ts).toISOString(),open:num(quote.open)??quote.price,high:num(quote.high)??quote.price,low:num(quote.low)??quote.price,close:quote.price,volume:num(quote.bar_volume)??0,live:true});}}else if(quote.market_open!==false){if(Number(latest.timestamp)===ts){latest.high=Math.max(Number(latest.high),Number(quote.price));latest.low=Math.min(Number(latest.low),Number(quote.price));latest.close=Number(quote.price);latest.live=true;}else if(ts>Number(latest.timestamp)){bars.push({timestamp:ts,date:new Date(ts).toISOString(),open:Number(quote.price),high:Number(quote.price),low:Number(quote.price),close:Number(quote.price),volume:num(quote.bar_volume)??0,live:true});if(bars.length>30000)bars.splice(0,bars.length-30000);}}state.payload.current_price=quote.price;state.payload.market_open=quote.market_open!==false;state.payload.source=quote.source||state.payload.source;state.payload.delayed=!!quote.delayed;}
function applyQuote(quote){if(!state.payload||!Number.isFinite(Number(quote.price)))return;mergeQuoteBar(quote);setText('currentPrice',price(quote.price));setText('priceMeta',`${quote.provider||quote.source||'Live'} · ${quote.delayed?'delayed':'live'} · ${Math.max(0,Number(quote.age_ms||0)).toFixed(0)} ms`);scheduleChart();}
async function requestPolicyPayload(edge,capital,risk,includeChart,controller,tickerLabel,signature){
  const prior=state.payloads[edge]?.label===tickerLabel?state.payloads[edge]:null;
  state.comparisonUpdatedAt[edge]=Date.now();
  state.comparisonErrors[edge]=null;
  try{
    const payload=await api(`/api/position-advice?label=${encodeURIComponent(tickerLabel)}&capital=${capital}&risk_pct=${risk}&edge=${edge}&include_chart=${includeChart?1:0}`,{signal:controller.signal});
    if(state.ticker?.label!==tickerLabel||policySignature()!==signature)return null;
    if(payload.label&&payload.label!==tickerLabel)throw new Error(`Unexpected ticker in ${edge} engine response.`);
    if(!payload.ok){state.comparisonErrors[edge]=payload.error||`${edge} candidate unavailable`;return payload;}
    if(payload.edge!==edge)throw new Error(`Unexpected ${edge} engine response.`);
    if(payload.chart?.reuse&&prior?.chart?.bars?.length)payload.chart={...prior.chart,...payload.chart,bars:prior.chart.bars,reuse:true};
    state.payloads[edge]=payload;
    return payload;
  }catch(error){
    if(state.ticker?.label===tickerLabel&&policySignature()===signature&&error.name!=='AbortError')state.comparisonErrors[edge]=error.message||`${edge} candidate unavailable`;
    throw error;
  }
}
function commitPolicyPayload(payload,tickerLabel,signature){if(!payload?.ok||state.ticker?.label!==tickerLabel||policySignature()!==signature)return false;state.edge=payload.edge;state.payload=policyDisplayPayload(payload);clearError();renderPolicyNotice(state.payload);render(state.payload);renderComparison();status(payload.market_open?(payload.delayed?'Delayed market data':'Live market data'):'Market closed',payload.market_open&&!payload.delayed);return true;}
async function loadAdvice(){
  if(!state.ticker)return;
  if(state.request)state.request.abort();
  const controller=new AbortController(),tickerLabel=state.ticker.label,signature=policySignature(),edges=policyEdges(),primaryEdge=edges[0];
  state.request=controller;
  state.edge=primaryEdge;
  let timedOut=false,committed=false,primaryError=null;
  const requestTimeout=setTimeout(()=>{timedOut=true;controller.abort();},4800);
  $('refreshBtn').classList.add('busy');
  status(state.policy.direction==='compare_both'?'Comparing Long / Call and Short engines':primaryEdge==='long'?'Loading calibrated Long edge':primaryEdge==='long_intraday'?'Calculating intraday Long / Call setup':'Calculating setup',false);
  const rawCapital=Number($('accountValue').value),rawRisk=Number($('riskPercent').value),capital=Number.isFinite(rawCapital)?Math.max(0,Math.min(1e9,rawCapital)):100000,risk=Number.isFinite(rawRisk)?Math.max(.05,Math.min(5,rawRisk)):.5,primaryPrior=state.payloads[primaryEdge],primaryIncludeChart=!primaryPrior?.chart?.bars?.length;
  let alternatePromise=null,alternateEdge=null;
  if(edges.length>1){
    alternateEdge=edges[1];
    const alternateFresh=state.payloads[alternateEdge]?.ok&&Date.now()-Number(state.comparisonUpdatedAt[alternateEdge]||0)<15000;
    if(!alternateFresh)alternatePromise=requestPolicyPayload(alternateEdge,capital,risk,false,controller,tickerLabel,signature).then(payload=>({payload,error:null}),error=>({payload:null,error}));
  }
  try{
    let primaryPayload=null;
    try{primaryPayload=await requestPolicyPayload(primaryEdge,capital,risk,primaryIncludeChart,controller,tickerLabel,signature);}catch(error){primaryError=error;}
    if(state.ticker?.label!==tickerLabel||policySignature()!==signature)return;
    if(primaryPayload?.ok)committed=commitPolicyPayload(primaryPayload,tickerLabel,signature);
    if(alternatePromise){const alternateResult=await alternatePromise;if(alternateResult.error&&!primaryError)primaryError=alternateResult.error;}
    renderComparison();
    if(!committed&&alternateEdge&&state.payloads[alternateEdge]?.ok)committed=commitPolicyPayload(state.payloads[alternateEdge],tickerLabel,signature);
    if(!committed){
      const edgeMessage=state.comparisonErrors[primaryEdge]||primaryError?.message;
      if(timedOut)showError(primaryEdge==='long'?'The first post-2016 history load exceeded 4.8 seconds. It continues in the background; refresh once more.':'The market feed exceeded the 4.8 second response budget. A background fallback is warming; refresh once more.');
      else showError(edgeMessage||'Position advice unavailable.');
    }
  }finally{
    clearTimeout(requestTimeout);
    if(state.request===controller){$('refreshBtn').classList.remove('busy');state.request=null;}
  }
}
function actionClass(code){return ['long','short','watch','wait','plan','avoid'].includes(code)?code:'avoid';}
function render(payload){const advice=payload.advice||{},sizing=payload.position_sizing||{},session=payload.session||{},isLong=payload.edge==='long',coverage=payload.coverage||{};setText('tickerTitle',`${payload.label} · ${payload.name}`);setText('tickerMeta',isLong?`${payload.exchange||'Exchange'} · ${payload.currency||'market currency'} · 1h regular sessions · ${coverage.start||'first available'} → ${coverage.end||'latest'}`:`${payload.exchange||'Exchange'} · ${payload.currency||'market currency'} · one-minute session from open`);setText('currentPrice',price(payload.current_price));setText('priceMeta',`${payload.source||'market feed'} · ${payload.delayed?'delayed':'live/low latency'} · age ${(Number(payload.source_age_ms||0)/1000).toFixed(1)}s`);setText('actionBadge',advice.action||'—');$('actionBadge').className=`action ${actionClass(advice.action_code)}`;setText('setupName',advice.setup||'—');setText('entryZone',`${price(advice.entry_zone?.low)} – ${price(advice.entry_zone?.high)}`);setText('entryState',advice.entry_state||'—');setText('stopLoss',price(advice.stop_loss));setText('takeProfit1',price(advice.take_profit_1));setText('takeProfit2',price(advice.take_profit_2));setText('gainTp1',`${money(sizing.estimated_gain_at_tp1)} estimated at TP1`);setText('gainProbability',pct(advice.gain_probability));setText('rewardRisk',`${Number(advice.reward_risk_tp1||0).toFixed(2)} R`);setText('riskBudget',money(sizing.risk_budget));setText('lossAtStop',`${money(sizing.estimated_loss_at_stop)} estimated loss`);setText('unitsCeiling',Number(sizing.suggested_units_ceiling||0).toLocaleString('en-US',{maximumFractionDigits:6}));setText('notional',`${money(sizing.estimated_notional)} notional ceiling`);setText('chartTitle',isLong?`${payload.label} · 1h regular sessions · closed periods compressed`:`${payload.label} · one-day real-time path from open`);setText('calcLatency',`${Number(payload.response_ms||0).toFixed(0)} ms response · ${Number(payload.calculation_ms||0).toFixed(1)} ms model`);setText('engineStatus',isLong?`${Number(payload.technical?.target_samples||0)} calibrated target observations`:payload.model_context?.available?'setup.stats.py context active':'intraday-only confidence decay');setText('disclaimer',payload.disclaimer||'Statistical setup research only.');renderTechnical(payload);renderEvidence(payload);payload.chart?.reuse?scheduleChart():drawChart(payload);const bars=payload.chart?.bars||[],last=bars.at(-1);if(last)setText('ohlcStrip',`O ${price(last.open)}  H ${price(last.high)}  L ${price(last.low)}  C ${price(last.close)}  Δ ${Number(session.delta_pct||0)>=0?'+':''}${Number(session.delta_pct||0).toFixed(2)}%`);}
function renderTechnical(payload){const t=payload.technical||{},m=payload.model_context||{},isLong=payload.edge==='long',rows=isLong?[['SMA 20 / 50 / 200','Daily regular-session trend structure',`${price(t.sma_20)} / ${price(t.sma_50)} / ${price(t.sma_200)}`],['EMA 50','Adaptive medium-term support',price(t.ema_50)],['ATR 14','Daily structural risk input',`${price(t.atr_14)} · ${Number(t.atr_pct||0).toFixed(2)}%`],['RSI 14','Daily momentum quality',Number(t.rsi_14||0).toFixed(1)],['Momentum 1m / 3m / 6m / 12m','Multi-horizon regular-session returns',`${Number(t.momentum_1m_pct||0).toFixed(2)}% / ${Number(t.momentum_3m_pct||0).toFixed(2)}% / ${Number(t.momentum_6m_pct||0).toFixed(2)}% / ${Number(t.momentum_12m_pct||0).toFixed(2)}%`],['Variance ratio 5','Above 1 favors persistence; below 1 mean reversion',Number(t.variance_ratio_5||0).toFixed(2)],['Realised volatility 20 / 60 / 252','Annualised daily estimates',`${Number(t.volatility_20_ann_pct||0).toFixed(1)}% / ${Number(t.volatility_60_ann_pct||0).toFixed(1)}% / ${Number(t.volatility_252_ann_pct||0).toFixed(1)}%`],['Drawdown 252 / since 2016','Current and maximum peak-to-trough loss',`${Number(t.drawdown_252_pct||0).toFixed(1)}% / ${Number(t.max_drawdown_since_2016_pct||0).toFixed(1)}%`],['Relative session volume','Latest session versus 20-session median',`${Number(t.relative_volume||0).toFixed(2)}x`],['Long edge','Weighted trend and analog ensemble',Number(t.long_edge||0).toFixed(3)],['TP1 historical hit rate','Maximum favorable excursion evidence',t.target_hit_rate===null?'Unavailable':`${pct(t.target_hit_rate)} · n=${Number(t.target_samples||0)}`],['Daily model probability up','setup.stats.py calibrated context',m.available?pct(m.probability_up):'Unavailable']]:[['VWAP','Session fair-value anchor',price(t.vwap)],['EMA 8 / 21','Fast and medium intraday structure',`${price(t.ema_8)} / ${price(t.ema_21)}`],['ATR 14','Adaptive stop-distance input',`${price(t.atr_14)} · ${Number(t.atr_pct||0).toFixed(2)}%`],['RSI 14','Momentum pressure',Number(t.rsi_14||0).toFixed(1)],['Momentum 5m / 15m / 30m','Multi-horizon delta',`${Number(t.momentum_5m_pct||0).toFixed(2)}% / ${Number(t.momentum_15m_pct||0).toFixed(2)}% / ${Number(t.momentum_30m_pct||0).toFixed(2)}%`],['Variance ratio 5','Above 1 favors persistence; below 1 mean reversion',Number(t.variance_ratio_5||0).toFixed(2)],['Realised volatility','Annualised one-minute estimate',`${Number(t.realised_volatility_ann_pct||0).toFixed(1)}%`],['Opening range','First 15 one-minute bars',`${price(t.opening_range_low)} – ${price(t.opening_range_high)}`],['Relative volume / z-score','Current minute versus recent minutes',`${Number(t.relative_volume||0).toFixed(2)}x / ${Number(t.volume_zscore||0).toFixed(2)}`],['Signed volume imbalance','Negative confirms selling pressure',Number(t.signed_volume_imbalance||0).toFixed(3)],['Short edge','Weighted feature ensemble',Number(t.short_edge||0).toFixed(3)],['Daily model probability down','setup.stats.py calibrated context',m.available?pct(m.probability_down):'Unavailable']];$('technicalRows').innerHTML=rows.map(([feature,meaning,value])=>`<tr><td>${esc(feature)}</td><td>${esc(meaning)}</td><td>${esc(value)}</td></tr>`).join('');}
const renderPositionTechnicalBase=renderTechnical;
renderTechnical=function(payload){
  if(payload.edge!=='long_intraday')return renderPositionTechnicalBase(payload);
  const t=payload.technical||{},m=payload.model_context||{},rows=[
    ['VWAP','Session fair-value anchor',price(t.vwap)],
    ['EMA 8 / 21','Fast and medium bullish intraday structure',`${price(t.ema_8)} / ${price(t.ema_21)}`],
    ['ATR 14','Adaptive stop-distance input',`${price(t.atr_14)} · ${Number(t.atr_pct||0).toFixed(2)}%`],
    ['RSI 14','Momentum pressure',Number(t.rsi_14||0).toFixed(1)],
    ['Momentum 5m / 15m / 30m','Multi-horizon delta',`${Number(t.momentum_5m_pct||0).toFixed(2)}% / ${Number(t.momentum_15m_pct||0).toFixed(2)}% / ${Number(t.momentum_30m_pct||0).toFixed(2)}%`],
    ['Variance ratio 5','Above 1 favors persistence; below 1 mean reversion',Number(t.variance_ratio_5||0).toFixed(2)],
    ['Realised volatility','Annualised one-minute estimate',`${Number(t.realised_volatility_ann_pct||0).toFixed(1)}%`],
    ['Opening range','First 15 one-minute bars',`${price(t.opening_range_low)} – ${price(t.opening_range_high)}`],
    ['Relative volume / z-score','Current minute versus recent minutes',`${Number(t.relative_volume||0).toFixed(2)}x / ${Number(t.volume_zscore||0).toFixed(2)}`],
    ['Signed volume imbalance','Positive confirms buying pressure',Number(t.signed_volume_imbalance||0).toFixed(3)],
    ['Long edge','Weighted bullish feature ensemble',Number(t.long_edge||0).toFixed(3)],
    ['Daily model probability up','setup.stats.py calibrated context',m.available?pct(m.probability_up):'Unavailable'],
  ];
  $('technicalRows').innerHTML=rows.map(([feature,meaning,value])=>`<tr><td>${esc(feature)}</td><td>${esc(meaning)}</td><td>${esc(value)}</td></tr>`).join('');
};
function renderEvidence(payload){$('drivers').innerHTML=(payload.drivers||[]).map(text=>`<div class="evidenceItem">${esc(text)}</div>`).join('');const features=(payload.feature_contributions||[]).slice(0,8);$('contributions').innerHTML=features.map(row=>{const width=Math.min(100,Math.abs(Number(row.value||0))*100),cls=Number(row.contribution||0)<0?'neg':'';return `<div class="contribution"><span>${esc(row.name)}</span><div class="barTrack"><div class="barFill ${cls}" style="width:${width.toFixed(1)}%"></div></div><b>${Number(row.contribution||0)>=0?'+':''}${Number(row.contribution||0).toFixed(3)}</b></div>`;}).join('');$('riskChecks').innerHTML=(payload.risk_checks||[]).map(text=>`<div>${esc(text)}</div>`).join('');}
function indicatorSeries(bars){let ema8=0,ema21=0,cumPV=0,cumV=0;const e8=[],e21=[],vwap=[];bars.forEach((row,index)=>{const close=Number(row.close),volume=Math.max(0,Number(row.volume||0)),typical=(Number(row.high)+Number(row.low)+close)/3;ema8=index?close*(2/9)+ema8*(7/9):close;ema21=index?close*(2/22)+ema21*(20/22):close;cumPV+=typical*volume;cumV+=volume;e8.push(ema8);e21.push(ema21);vwap.push(cumV>0?cumPV/cumV:typical);});return {e8,e21,vwap};}
function longIndicatorSeries(bars){let ema50=0,ema200=0;const e50=[],e200=[];bars.forEach((row,index)=>{const close=Number(row.close);ema50=index?close*(2/51)+ema50*(49/51):close;ema200=index?close*(2/201)+ema200*(199/201):close;e50.push(ema50);e200.push(ema200);});return {e50,e200};}
const positionCrosshairTimeFormatters={intraday:null,long:null};
function positionClamp(value,minimum,maximum){return Math.max(minimum,Math.min(maximum,value));}
function positionCrosshairDateMs(value){const numeric=Number(value);if(Number.isFinite(numeric)&&Math.abs(numeric)>1e11)return numeric;const parsed=Date.parse(String(value??''));return Number.isFinite(parsed)?parsed:null;}
function positionCrosshairAxisValue(axis,pixel){
  try{const converted=typeof axis?.p2d==='function'?axis.p2d(pixel):null;if(axis?.type==='date'){const parsed=positionCrosshairDateMs(converted);if(parsed!==null)return parsed;}else{const numeric=Number(converted);if(Number.isFinite(numeric))return numeric;}}catch(error){}
  const range=axis?.range||[],ratio=positionClamp(pixel/Math.max(1,Number(axis?._length||0)),0,1);
  if(axis?.type==='date'){const start=positionCrosshairDateMs(range[0]),end=positionCrosshairDateMs(range[1]);return start===null||end===null?null:start+(end-start)*ratio;}
  const start=Number(range[0]),end=Number(range[1]);return Number.isFinite(start)&&Number.isFinite(end)?start+(end-start)*ratio:null;
}
function positionCrosshairTimestamp(payload,xValue){
  if(payload?.edge!=='long')return Number.isFinite(Number(xValue))?Number(xValue):null;
  const bars=payload?.chart?.bars||[];if(!bars.length||!Number.isFinite(Number(xValue)))return null;
  const index=positionClamp(Math.round(Number(xValue)),0,bars.length-1),timestamp=Number(bars[index]?.timestamp);
  return Number.isFinite(timestamp)?timestamp:null;
}
function positionCrosshairTimeText(timestamp,isLong){
  if(!Number.isFinite(Number(timestamp)))return '—';
  try{
    if(isLong){positionCrosshairTimeFormatters.long=positionCrosshairTimeFormatters.long||new Intl.DateTimeFormat('en-GB',{timeZone:'UTC',year:'numeric',month:'short',day:'2-digit',hour:'2-digit',minute:'2-digit',hour12:false});return positionCrosshairTimeFormatters.long.format(new Date(timestamp));}
    positionCrosshairTimeFormatters.intraday=positionCrosshairTimeFormatters.intraday||new Intl.DateTimeFormat('en-GB',{timeZone:POSITION_CHART_TIMEZONE,day:'2-digit',month:'short',hour:'2-digit',minute:'2-digit',second:'2-digit',hour12:false});return positionCrosshairTimeFormatters.intraday.format(new Date(timestamp));
  }catch(error){return new Date(timestamp).toISOString().replace('T',' ').slice(0,isLong?16:19);}
}
function bindPositionCrosshair(el,payload){
  if(!el)return;el._positionCrosshairPayload=payload;
  const stage=$('positionChartStage');if(!stage||stage._positionCrosshairReady)return;stage._positionCrosshairReady=true;stage.dataset.crosshairReady='true';
  const cross=$('positionCrosshair'),vLine=$('positionCrosshairV'),hLine=$('positionCrosshairH'),xLabel=$('positionCrosshairX'),yLabel=$('positionCrosshairY');
  let frame=0,pointX=NaN,pointY=NaN,hasPoint=false,stageRect=null,geometryKey='';
  const invalidate=()=>{stageRect=null;geometryKey='';};
  const updateRect=()=>{stageRect=stage.getBoundingClientRect();return stageRect;};
  const applyGeometry=size=>{const key=`${size.l}|${size.t}|${size.w}|${size.h}`;if(key===geometryKey)return;geometryKey=key;vLine.style.top=`${size.t}px`;vLine.style.height=`${size.h}px`;hLine.style.left=`${size.l}px`;hLine.style.width=`${size.w}px`;xLabel.style.top=`${size.t+size.h+3}px`;yLabel.style.left=`${size.l+size.w+3}px`;};
  const paint=()=>{
    frame=0;if(!hasPoint||!el._fullLayout)return;
    const size=el._fullLayout._size,x=pointX,y=pointY;if(!size||x<size.l||x>size.l+size.w||y<size.t||y>size.t+size.h){cross.classList.remove('visible');return;}
    applyGeometry(size);
    const axisX=positionCrosshairAxisValue(el._fullLayout.xaxis,x-size.l),rangeY=el._fullLayout.yaxis?.range||[],y0=Number(rangeY[0]),y1=Number(rangeY[1]);
    if(axisX===null||!Number.isFinite(y0)||!Number.isFinite(y1)||y0===y1)return;
    const yValue=y1-(y-size.t)/size.h*(y1-y0),timestamp=positionCrosshairTimestamp(el._positionCrosshairPayload,axisX),dpr=Math.max(1,Number(window.devicePixelRatio||1)),px=Math.round(x*dpr)/dpr,py=Math.round(y*dpr)/dpr,labelX=positionClamp(px,size.l+46,size.l+size.w-46);
    vLine.style.transform=`translate3d(${px}px,0,0) scaleX(var(--position-crosshair-width,.55))`;hLine.style.transform=`translate3d(0,${py}px,0) scaleY(var(--position-crosshair-width,.55))`;xLabel.style.transform=`translate3d(${labelX}px,0,0) translateX(-50%)`;yLabel.style.transform=`translate3d(0,${py}px,0) translateY(-50%)`;
    const nextTime=positionCrosshairTimeText(timestamp,el._positionCrosshairPayload?.edge==='long'),nextPrice=price(yValue);if(xLabel.textContent!==nextTime)xLabel.textContent=nextTime;if(yLabel.textContent!==nextPrice)yLabel.textContent=nextPrice;
    if(!cross.classList.contains('visible'))cross.classList.add('visible');
  };
  const schedule=()=>{if(hasPoint&&!frame)frame=requestAnimationFrame(paint);};
  const capturePoint=event=>{const samples=typeof event.getCoalescedEvents==='function'?event.getCoalescedEvents():null,point=samples?.length?samples[samples.length-1]:event,rect=stageRect||updateRect();pointX=point.clientX-rect.left;pointY=point.clientY-rect.top;hasPoint=true;schedule();};
  const hideCrosshair=()=>{hasPoint=false;cross.classList.remove('visible');};
  stage.addEventListener('pointerenter',event=>{updateRect();capturePoint(event);},{passive:true});
  stage.addEventListener('pointermove',capturePoint,{passive:true});
  if('onpointerrawupdate' in window)stage.addEventListener('pointerrawupdate',capturePoint,{passive:true});
  stage.addEventListener('mousemove',capturePoint,{passive:true});
  stage.addEventListener('pointerleave',hideCrosshair,{passive:true});
  stage.addEventListener('mouseleave',hideCrosshair,{passive:true});
  window.addEventListener('resize',invalidate,{passive:true});document.addEventListener('scroll',()=>{stageRect=null;},{passive:true,capture:true});
  if(typeof ResizeObserver==='function')new ResizeObserver(()=>{invalidate();schedule();}).observe(stage);
  if(typeof el.on==='function')el.on('plotly_relayout',()=>{schedule();});
  stage._positionCrosshairRepaint=schedule;
}
function drawLongChart(payload){if(!window.Plotly)return;const bars=payload.chart?.bars||[];if(!bars.length)return;state.lastChartPaint=performance.now();const x=bars.map((_,index)=>index),last=bars.at(-1),ind=longIndicatorSeries(bars),a=payload.advice||{},entry=a.entry_zone||{},timezone=payload.chart?.timezone||'UTC',formatter=new Intl.DateTimeFormat('en-GB',{timeZone:timezone,year:'numeric',month:'short',day:'2-digit',hour:'2-digit',minute:'2-digit'}),labels=bars.map(row=>`${formatter.format(new Date(Number(row.timestamp)))}<br>O ${price(row.open)} H ${price(row.high)}<br>L ${price(row.low)} C ${price(row.close)}`),data=[{type:'candlestick',x,open:bars.map(row=>row.open),high:bars.map(row=>row.high),low:bars.map(row=>row.low),close:bars.map(row=>row.close),text:labels,hoverinfo:'text',name:payload.label,showlegend:false,increasing:{line:{color:'#089981',width:1},fillcolor:'#089981'},decreasing:{line:{color:'#f23645',width:1},fillcolor:'#f23645'}},{type:'scatter',mode:'lines',x,y:ind.e50,name:'EMA 50h',hoverinfo:'skip',line:{color:'#38bdf8',width:1}},{type:'scatter',mode:'lines',x,y:ind.e200,name:'EMA 200h',hoverinfo:'skip',line:{color:'#a78bfa',width:1}}],x0=0,x1=Math.max(0,bars.length-1),line=(value,color,dash='dash')=>({type:'line',xref:'x',yref:'y',x0,x1,y0:value,y1:value,line:{color,width:1.1,dash}}),shapes=[{type:'rect',xref:'x',yref:'y',x0,x1,y0:entry.low,y1:entry.high,fillcolor:'rgba(56,189,248,.10)',line:{color:'rgba(56,189,248,.62)',width:1}},line(a.stop_loss,'#f43f5e'),line(a.take_profit_1,'#22c55e'),line(a.take_profit_2,'rgba(34,197,94,.65)','dot'),line(payload.current_price,'rgba(226,232,240,.45)','dot')],annotations=[['ENTRY',entry.mid,'#38bdf8'],['SL',a.stop_loss,'#f43f5e'],['TP1',a.take_profit_1,'#22c55e'],['TP2',a.take_profit_2,'#22c55e']].filter(row=>Number.isFinite(Number(row[1]))).map(([text,y,color])=>({xref:'paper',yref:'y',x:1,y,text,showarrow:false,xanchor:'left',font:{size:10,color},bgcolor:'#0b0e14',bordercolor:color,borderwidth:1,borderpad:3})),values=[...bars.flatMap(row=>[row.high,row.low]),entry.low,entry.high,a.stop_loss,a.take_profit_1,a.take_profit_2].map(Number).filter(Number.isFinite),lo=Math.min(...values),hi=Math.max(...values),pad=Math.max((hi-lo)*.06,hi*.0015),tickCount=Math.min(9,bars.length),tickvals=Array.from({length:tickCount},(_,index)=>Math.round(index*(bars.length-1)/Math.max(1,tickCount-1))),ticktext=tickvals.map(index=>formatter.format(new Date(Number(bars[index].timestamp))).replace(',','')),uiKey=`position-long-${payload.label}`,chartEl=$('positionChart'),keepView=chartEl?.layout?.uirevision===uiKey,xRange=keepView&&Array.isArray(chartEl.layout?.xaxis?.range)?chartEl.layout.xaxis.range:[x0,x1+3],yRange=keepView&&Array.isArray(chartEl.layout?.yaxis?.range)?chartEl.layout.yaxis.range:[Math.max(0,lo-pad),hi+pad],layout={paper_bgcolor:'#0b0e14',plot_bgcolor:'#0b0e14',font:{color:'#aeb6c2',size:10},margin:{l:12,r:72,t:18,b:48},uirevision:uiKey,transition:{duration:0},hovermode:false,hoverdistance:-1,spikedistance:-1,showlegend:true,legend:{orientation:'h',x:0,y:1.04,font:{size:9}},shapes,annotations,xaxis:{type:'linear',range:xRange,autorange:false,fixedrange:false,rangeslider:{visible:false},tickmode:'array',tickvals,ticktext,gridcolor:'#242933',zeroline:false,showline:true,linecolor:'#2a303a'},yaxis:{side:'right',range:yRange,autorange:false,fixedrange:false,gridcolor:'#242933',zeroline:false,showline:true,linecolor:'#2a303a',tickformat:'.2f'},dragmode:'pan'};const config={responsive:true,displaylogo:false,scrollZoom:true,doubleClick:'reset',modeBarButtonsToRemove:['select2d','lasso2d','hoverClosestCartesian','hoverCompareCartesian','toggleSpikelines']};Plotly.react('positionChart',data,layout,config).catch(()=>{});setText('ohlcStrip',`O ${price(last.open)}  H ${price(last.high)}  L ${price(last.low)}  C ${price(last.close)}`);}
function scheduleChart(){if(state.chartFrame)return;const minimumDelay=state.payload?.edge==='long'?900:400,delay=Math.max(0,minimumDelay-(performance.now()-state.lastChartPaint));state.chartFrame=setTimeout(()=>{state.chartFrame=0;if(state.payload)drawChart(state.payload);},delay);}
function drawChart(payload){if(payload.edge==='long'){drawLongChart(payload);return;}if(!window.Plotly)return;const bars=payload.chart?.bars||[];if(!bars.length)return;state.lastChartPaint=performance.now();const x=bars.map(row=>row.date),ind=indicatorSeries(bars),a=payload.advice||{},entry=a.entry_zone||{},last=bars.at(-1),data=[{type:'candlestick',x,open:bars.map(row=>row.open),high:bars.map(row=>row.high),low:bars.map(row=>row.low),close:bars.map(row=>row.close),name:payload.label,showlegend:false,hoverinfo:'x+name',increasing:{line:{color:'#089981',width:1},fillcolor:'#089981'},decreasing:{line:{color:'#f23645',width:1},fillcolor:'#f23645'}},{type:'scatter',mode:'lines',x,y:ind.vwap,name:'VWAP',line:{color:'#f59e0b',width:1.2}},{type:'scatter',mode:'lines',x,y:ind.e8,name:'EMA 8',line:{color:'#38bdf8',width:1}},{type:'scatter',mode:'lines',x,y:ind.e21,name:'EMA 21',line:{color:'#a78bfa',width:1}}];const x0=x[0],x1=x.at(-1),line=(value,color,dash='dash')=>({type:'line',xref:'x',yref:'y',x0,x1,y0:value,y1:value,line:{color,width:1.2,dash}}),shapes=[{type:'rect',xref:'x',yref:'y',x0,x1,y0:entry.low,y1:entry.high,fillcolor:'rgba(56,189,248,.12)',line:{color:'rgba(56,189,248,.65)',width:1}},line(a.stop_loss,'#f43f5e'),line(a.take_profit_1,'#22c55e'),line(a.take_profit_2,'rgba(34,197,94,.65)','dot'),line(payload.current_price,'rgba(226,232,240,.45)','dot')],annotations=[['ENTRY',entry.mid,'#38bdf8'],['SL',a.stop_loss,'#f43f5e'],['TP1',a.take_profit_1,'#22c55e'],['TP2',a.take_profit_2,'#22c55e']].filter(row=>Number.isFinite(Number(row[1]))).map(([text,y,color])=>({xref:'paper',yref:'y',x:1,y,text,showarrow:false,xanchor:'left',font:{size:10,color},bgcolor:'#0b0e14',bordercolor:color,borderwidth:1,borderpad:3}));const values=[...bars.flatMap(row=>[row.high,row.low]),entry.low,entry.high,a.stop_loss,a.take_profit_1,a.take_profit_2].map(Number).filter(Number.isFinite),lo=Math.min(...values),hi=Math.max(...values),pad=Math.max((hi-lo)*.08,hi*.0015);const layout={paper_bgcolor:'#0b0e14',plot_bgcolor:'#0b0e14',font:{color:'#aeb6c2',size:10},margin:{l:12,r:72,t:18,b:42},uirevision:`position-${payload.label}`,transition:{duration:0},hovermode:false,hoverdistance:-1,spikedistance:-1,showlegend:true,legend:{orientation:'h',x:0,y:1.04,font:{size:9}},shapes,annotations,xaxis:{type:'date',range:[x0,new Date(Number(last.timestamp)+5*60000).toISOString()],autorange:false,fixedrange:false,rangeslider:{visible:false},gridcolor:'#242933',zeroline:false,showline:true,linecolor:'#2a303a'},yaxis:{side:'right',range:[Math.max(0,lo-pad),hi+pad],autorange:false,fixedrange:false,gridcolor:'#242933',zeroline:false,showline:true,linecolor:'#2a303a',tickformat:'.2f'},dragmode:'pan'};const config={responsive:true,displaylogo:false,scrollZoom:true,doubleClick:'reset',modeBarButtonsToRemove:['select2d','lasso2d','hoverClosestCartesian','hoverCompareCartesian','toggleSpikelines']};Plotly.react('positionChart',data,layout,config).catch(()=>{});setText('ohlcStrip',`O ${price(last.open)}  H ${price(last.high)}  L ${price(last.low)}  C ${price(last.close)}`);}
function positionDisplayPayload(payload){const chart=payload?.chart,bars=chart?.bars||[];if(!bars.length)return payload;const displayBars=bars.map(row=>{const sourceTimestamp=positionSourceTimestamp(row),date=positionChartDate(sourceTimestamp),timestamp=Date.parse(date);return {...row,_source_timestamp:sourceTimestamp,timestamp,date};});return {...payload,chart:{...chart,bars:displayBars,source_timezone:chart.timezone||'UTC',timezone:'UTC',display_timezone:POSITION_CHART_TIMEZONE}};}
const drawPositionAdviceChart=drawChart;
drawChart=function(payload){const title=$('chartTitle'),suffix=` · ${POSITION_CHART_ZONE_LABEL} time`,displayPayload=positionDisplayPayload(payload);if(title&&!title.textContent.endsWith(suffix))title.textContent+=suffix;const result=drawPositionAdviceChart(displayPayload);bindPositionCrosshair($('positionChart'),displayPayload);return result;};
initPositionCrosshairWidthControl();
$('directionPolicyPanel').addEventListener('click',event=>{const button=event.target.closest('button[data-policy-group]');if(button)setPolicy(button.dataset.policyGroup,button.dataset.policyValue);});$('candidateCompare').addEventListener('click',event=>{const button=event.target.closest('button[data-choose-direction]');if(button&&!button.disabled)setPolicy('direction',button.dataset.chooseDirection);});$('tickerInput').addEventListener('change',()=>{const row=selectedTicker();if(row)selectTicker(row);});$('tickerInput').addEventListener('keydown',event=>{if(event.key==='Enter'){event.preventDefault();const row=selectedTicker();if(row)selectTicker(row);}});$('refreshBtn').onclick=loadAdvice;['accountValue','riskPercent'].forEach(id=>$(id).addEventListener('input',()=>{clearTimeout(state.inputTimer);state.inputTimer=setTimeout(loadAdvice,350);}));document.addEventListener('visibilitychange',()=>{if(document.visibilityState==='visible'){openStream();if(!state.request)loadAdvice();}});window.addEventListener('beforeunload',closeStream);state.poll=setInterval(()=>{if(document.visibilityState==='visible'&&state.ticker&&!state.request)loadAdvice();},2500);loadTickers().catch(error=>showError(error.message));
</script>
</body>
</html>"""


def _yahoo_max_history_payload(label: str) -> dict[str, Any]:
    """Fetch split-adjusted daily OHLCV from the first Yahoo bar to today."""
    lab = str(label or "").upper().strip()
    watch_rows = load_watchlist()
    watch_by_label = {_tv_clean_token(row.get("label")): row for row in watch_rows}
    watch = watch_by_label.get(lab, {})
    item = {
        "label": lab,
        "symbol": watch.get("symbol") or lab,
        "exchange": watch.get("exchange") or "",
        "name": watch.get("name") or lab,
    }
    candidates = _yahoo_symbol_candidates(item, watch_by_label)
    if not candidates:
        raise RuntimeError("Yahoo symbol mapping unavailable")

    attempts: list[str] = []
    query_string = (
        f"interval=1d&period1=0&period2={int(time.time()) + 86400}&includePrePost=false"
        "&events=div%2Csplits&includeAdjustedClose=true"
    )
    for symbol in candidates:
        try:
            raw = _yahoo_chart_request(symbol, query_string, timeout=FULL_HISTORY_YAHOO_TIMEOUT)
            chart = raw.get("chart") or {}
            results = chart.get("result") or []
            if chart.get("error") or not results:
                raise RuntimeError(str(chart.get("error") or "empty max-range response"))
            result = results[0]
            timestamps = result.get("timestamp") or []
            indicators = result.get("indicators") or {}
            quotes = (indicators.get("quote") or [{}])[0]
            adjusted = (indicators.get("adjclose") or [{}])[0].get("adjclose") or []

            def value(values: list[Any], index: int) -> float | None:
                return _num_or_none(values[index] if index < len(values) else None)

            rows_by_date: dict[str, dict[str, Any]] = {}
            for index, raw_timestamp in enumerate(timestamps):
                try:
                    timestamp = int(raw_timestamp)
                    date = datetime.fromtimestamp(timestamp, tz=timezone.utc).date().isoformat()
                except Exception:
                    continue
                raw_close = value(quotes.get("close") or [], index)
                if raw_close is None or raw_close <= 0:
                    continue
                adjusted_close = value(adjusted, index)
                factor = adjusted_close / raw_close if adjusted_close and adjusted_close > 0 else 1.0
                open_price = (value(quotes.get("open") or [], index) or raw_close) * factor
                high_price = (value(quotes.get("high") or [], index) or raw_close) * factor
                low_price = (value(quotes.get("low") or [], index) or raw_close) * factor
                close_price = raw_close * factor
                if not all(math.isfinite(number) and number > 0 for number in (open_price, high_price, low_price, close_price)):
                    continue
                rows_by_date[date] = {
                    "date": date,
                    "open": round(open_price, 6),
                    "high": round(max(open_price, high_price, low_price, close_price), 6),
                    "low": round(min(open_price, high_price, low_price, close_price), 6),
                    "close": round(close_price, 6),
                    "volume": round(max(0.0, value(quotes.get("volume") or [], index) or 0.0), 3),
                }
            history = [rows_by_date[date] for date in sorted(rows_by_date)]
            if len(history) < 5:
                raise RuntimeError(f"only {len(history)} validated daily rows")
            meta = result.get("meta") or {}
            return {
                "label": lab,
                "name": watch.get("name") or meta.get("longName") or lab,
                "ok": True,
                "bars_requested": "max",
                "bars_returned": len(history),
                "coverage_start": history[0]["date"],
                "coverage_end": history[-1]["date"],
                "source": "Yahoo Finance max-range daily",
                "source_symbol": symbol,
                "source_exchange": meta.get("fullExchangeName") or meta.get("exchangeName") or watch.get("exchange"),
                "source_attempts": attempts,
                "adjusted_ohlc": True,
                "history": history,
            }
        except Exception as exc:
            attempts.append(f"Yahoo {symbol}: {exc}")
    raise RuntimeError("; ".join(attempts[-4:]) or "Yahoo max-range history unavailable")


def fetch_full_history_payload(label: str) -> dict[str, Any]:
    """Return maximum available validated multi-source OHLCV history for one chart.

    This is deliberately chart-only and on-demand: the visible watchlist run keeps
    the v14.4 turbo speed, while the selected graph gets a second lightweight
    Superchart-style OHLCV fetch in the background.
    """
    lab = str(label or "").upper().strip()
    if not lab:
        return {"ok": False, "error": "missing label", "history": []}
    now = time.time()
    with FULL_HISTORY_LOCK:
        cached = FULL_HISTORY_CACHE.get(lab)
        if cached and now - float(cached.get("ts", 0)) < FULL_HISTORY_TTL_SEC:
            out = dict(cached.get("payload") or {})
            out["cached"] = True
            return out
    max_range_error = ""
    try:
        payload = _yahoo_max_history_payload(lab)
        payload["cached"] = False
        with FULL_HISTORY_LOCK:
            FULL_HISTORY_CACHE[lab] = {"ts": now, "payload": payload}
            while len(FULL_HISTORY_CACHE) > max(2, FULL_HISTORY_CACHE_MAX_ENTRIES):
                oldest_key = min(FULL_HISTORY_CACHE, key=lambda key: float(FULL_HISTORY_CACHE[key].get("ts") or 0.0))
                FULL_HISTORY_CACHE.pop(oldest_key, None)
        return payload
    except Exception as exc:
        max_range_error = str(exc)
    JOBS_DIR.mkdir(exist_ok=True)
    tmp_dir = JOBS_DIR / f"hist_{lab}_{uuid.uuid4().hex[:8]}"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    out_path = tmp_dir / "history.json"
    log_path = tmp_dir / "history.log"
    cmd = [
        sys.executable, str(PREDICT_SCRIPT),
        "--history-only", lab,
        "--bars", str(FULL_HISTORY_BARS),
        "--json-out", str(out_path),
        "--quiet",
    ]
    try:
        with open(log_path, "w", encoding="utf-8") as log:
            proc = subprocess.run(
                cmd,
                cwd=str(BASE_DIR),
                text=True,
                stdout=log,
                stderr=subprocess.STDOUT,
                timeout=FULL_HISTORY_TIMEOUT,
            )
        if proc.returncode != 0 or not out_path.exists():
            # v14.7.4 safety fallback: if the maximum-history endpoint fails, run
            # a normal one-ticker forecast and reuse its embedded OHLCV history.
            # This keeps RETURN PREVISION usable instead of returning all-zero rows.
            fallback_path = tmp_dir / "fallback_history.json"
            fallback_cmd = [
                sys.executable, str(PREDICT_SCRIPT),
                "--only", lab,
                "--mode", "deep",
                "--bars", str(min(FULL_HISTORY_BARS, 5000)),
                "--json-out", str(fallback_path),
                "--quiet",
                "--no-news",
            ]
            try:
                with open(log_path, "a", encoding="utf-8") as log:
                    log.write("\n--- fallback one-ticker history run ---\n")
                    proc2 = subprocess.run(
                        fallback_cmd,
                        cwd=str(BASE_DIR),
                        text=True,
                        stdout=log,
                        stderr=subprocess.STDOUT,
                        timeout=max(12.0, min(FULL_HISTORY_TIMEOUT, 28.0)),
                    )
                if proc2.returncode == 0 and fallback_path.exists():
                    fb = json.loads(fallback_path.read_text(encoding="utf-8"))
                    candidates = [r for r in (fb.get("results") or []) if str(r.get("label", "")).upper() == lab and not r.get("error")]
                    if candidates and candidates[0].get("history"):
                        r0 = candidates[0]
                        payload = {
                            "label": lab,
                            "name": r0.get("name") or lab,
                            "ok": True,
                            "bars_requested": min(FULL_HISTORY_BARS, 5000),
                            "bars_returned": len(r0.get("history") or []),
                            "source": f"{r0.get('tv_exchange') or ''}:{r0.get('tv_symbol') or ''}".strip(":"),
                            "source_attempts": ["history-only failed; fallback forecast history used"],
                            "history": r0.get("history") or [],
                        }
                    else:
                        return {"ok": False, "label": lab, "error": "full history fetch failed", "history": []}
                else:
                    return {"ok": False, "label": lab, "error": "full history fetch failed", "history": []}
            except Exception:
                return {"ok": False, "label": lab, "error": "full history fallback failed", "history": []}
            finally:
                try:
                    if fallback_path.exists():
                        fallback_path.unlink()
                except Exception:
                    pass
        else:
            payload = json.loads(out_path.read_text(encoding="utf-8"))
        source_attempts = list(payload.get("source_attempts") or [])
        if max_range_error:
            source_attempts.insert(0, f"Yahoo max-range: {max_range_error}")
        payload["source_attempts"] = source_attempts
        payload["ok"] = bool(payload.get("history"))
        payload["cached"] = False
        with FULL_HISTORY_LOCK:
            FULL_HISTORY_CACHE[lab] = {"ts": now, "payload": payload}
            while len(FULL_HISTORY_CACHE) > max(2, FULL_HISTORY_CACHE_MAX_ENTRIES):
                oldest_key = min(FULL_HISTORY_CACHE, key=lambda key: float(FULL_HISTORY_CACHE[key].get("ts") or 0.0))
                FULL_HISTORY_CACHE.pop(oldest_key, None)
        return payload
    except subprocess.TimeoutExpired:
        return {"ok": False, "label": lab, "error": "full history fetch timeout", "history": []}
    except Exception as exc:
        return {"ok": False, "label": lab, "error": str(exc), "history": []}
    finally:
        try:
            if out_path.exists():
                out_path.unlink()
            if log_path.exists():
                log_path.unlink()
            tmp_dir.rmdir()
        except Exception:
            pass


def _parse_date_yyyy_mm_dd(value: Any) -> datetime | None:
    try:
        return datetime.strptime(str(value)[:10], "%Y-%m-%d")
    except Exception:
        return None


def _usd(value: float) -> str:
    try:
        v = float(value)
    except Exception:
        v = 0.0
    return f"${v:,.2f}"


def calculate_return_prevision(payload: dict[str, Any]) -> dict[str, Any]:
    """Compute allocated daily dollar return for up to 15 watchlist tickers.

    Daily return = daily percentage change / 100 * allocated USD funds.
    Dates without validated market close data are skipped.
    """
    start_dt = _parse_date_yyyy_mm_dd(payload.get("start"))
    end_dt = _parse_date_yyyy_mm_dd(payload.get("end"))
    floor_dt = datetime(2015, 1, 1)
    if start_dt is None or end_dt is None:
        return {"ok": False, "error": "Start and end dates are required in YYYY-MM-DD format."}
    if start_dt < floor_dt:
        start_dt = floor_dt
    if end_dt < start_dt:
        return {"ok": False, "error": "End date must be after start date."}
    positions_in = payload.get("positions") or []
    if not isinstance(positions_in, list):
        return {"ok": False, "error": "positions must be a list."}
    valid_labels = {str(x.get("label", "")).upper(): x for x in load_watchlist()}
    positions: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in positions_in:
        lab = str((row or {}).get("label") or "").upper().strip()
        if not lab or lab in seen:
            continue
        if lab not in valid_labels:
            return {"ok": False, "error": f"Unknown ticker: {lab}"}
        try:
            alloc = float((row or {}).get("allocated"))
        except Exception:
            return {"ok": False, "error": f"Allocated funds required for {lab}."}
        if not (0 < alloc <= 1_000_000_000):
            return {"ok": False, "error": f"Allocated funds for {lab} must be > 0 and <= $1B."}
        positions.append({"label": lab, "allocated": alloc, "name": valid_labels[lab].get("name") or lab})
        seen.add(lab)
        if len(positions) >= 15:
            break
    if not positions:
        return {"ok": False, "error": "Select at least one ticker and allocated funds."}

    all_dates: set[str] = set()
    series: dict[str, dict[str, dict[str, float]]] = {}
    sources: dict[str, str] = {}
    errors: dict[str, str] = {}
    latest_close_date: str | None = None
    for pos in positions:
        lab = pos["label"]
        hist_payload = fetch_full_history_payload(lab)
        hist = hist_payload.get("history") or []
        clean = []
        for r in hist:
            d = str(r.get("date") or "")[:10]
            c = r.get("close")
            try:
                close = float(c)
            except Exception:
                continue
            if not d or close <= 0:
                continue
            clean.append((d, close))
        clean.sort(key=lambda x: x[0])
        lab_map: dict[str, dict[str, float]] = {}
        prev_close = None
        for d, close in clean:
            if latest_close_date is None or d > latest_close_date:
                latest_close_date = d
            if prev_close is not None:
                dt = _parse_date_yyyy_mm_dd(d)
                if dt and start_dt <= dt <= end_dt:
                    pct = (close / prev_close - 1.0) * 100.0 if prev_close > 0 else 0.0
                    dollars = (pct / 100.0) * float(pos["allocated"])
                    lab_map[d] = {"pct": pct, "return": dollars, "close": close, "prev_close": prev_close}
                    all_dates.add(d)
            prev_close = close
        series[lab] = lab_map
        sources[lab] = str(hist_payload.get("source") or hist_payload.get("source_label") or "Validated multi-source OHLCV")
        if not lab_map:
            errors[lab] = str(hist_payload.get("error") or "No close variation in selected period")

    dates = sorted(all_dates)
    rows: list[dict[str, Any]] = []
    totals = {p["label"]: 0.0 for p in positions}
    for d in dates:
        row = {"date": d, "values": {}, "total_earning": None}
        for pos in positions:
            lab = pos["label"]
            cell = series.get(lab, {}).get(d)
            if cell is None:
                row["values"][lab] = None
                continue
            val = float(cell["return"])
            totals[lab] += val
            row["values"][lab] = {"return": val, "pct": float(cell["pct"]), "close": float(cell["close"])}
        rows.append(row)
    total_earning = float(sum(totals.values()))
    return {
        "ok": True,
        "start": start_dt.strftime("%Y-%m-%d"),
        "end": end_dt.strftime("%Y-%m-%d"),
        "latest_close_date": latest_close_date,
        "positions": positions,
        "rows": rows,
        "totals": totals,
        "total_earning": total_earning,
        "sources": sources,
        "errors": errors,
        "method": "daily return = TradingView daily % change / 100 × allocated USD funds",
    }


class DashboardHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def handle_error(self, request: Any, client_address: Any) -> None:
        error = sys.exc_info()[1]
        expected_socket_errors = {32, 53, 54, 57, 104}
        if isinstance(error, (ConnectionResetError, BrokenPipeError, ConnectionAbortedError)):
            return
        if isinstance(error, OSError) and getattr(error, "errno", None) in expected_socket_errors:
            return
        super().handle_error(request, client_address)


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_: Any) -> None:
        return

    def _send(self, code: int, body: bytes, ctype: str = "application/json; charset=utf-8") -> None:
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def json(self, payload: Any, code: int = 200) -> None:
        self._send(code, json.dumps(payload, ensure_ascii=False).encode("utf-8"))

    def sse_live_stream(self, labels: list[str], preferred: str, cadence_ms: int, client_id: str = "dashboard") -> None:
        watch = {_tv_clean_token(row.get("label")): row for row in load_watchlist()}
        clean_labels = list(dict.fromkeys(_tv_clean_token(label) for label in labels if _tv_clean_token(label)))[:160]
        subscription_items = [watch.get(label, {"label": label}) for label in clean_labels]
        LIVE_STREAM_HUB.subscribe(subscription_items, client_id=client_id)
        preferred = str(preferred or "auto").strip().lower()
        if preferred not in {"auto", "massive", "alpaca", "finnhub", "twelvedata", "coinbase"}:
            preferred = "auto"
        cadence_ms = max(100, min(2_000, int(cadence_ms or 250)))
        try:
            self.connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        except Exception:
            pass
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-cache, no-transform")
        self.send_header("Connection", "keep-alive")
        self.send_header("X-Accel-Buffering", "no")
        self.end_headers()
        last_sequence = 0
        next_send_at = 0.0
        next_subscription_refresh = time.monotonic() + 20.0
        try:
            self.wfile.write(b"retry: 1500\n\n")
            self.wfile.flush()
            while True:
                if time.monotonic() >= next_subscription_refresh:
                    LIVE_STREAM_HUB.subscribe(subscription_items, client_id=client_id)
                    next_subscription_refresh = time.monotonic() + 20.0
                snapshot = LIVE_STREAM_HUB.wait_snapshot(clean_labels, preferred, last_sequence, timeout=10.0)
                sequence = int(snapshot.get("sequence") or 0)
                now_monotonic = time.monotonic()
                if sequence > last_sequence and now_monotonic < next_send_at:
                    time.sleep(next_send_at - now_monotonic)
                    snapshot = LIVE_STREAM_HUB.snapshot(clean_labels, preferred, since_sequence=last_sequence)
                    sequence = int(snapshot.get("sequence") or sequence)
                snapshot["transport"] = "sse"
                snapshot["cadence_ms"] = cadence_ms
                data = json.dumps(snapshot, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
                self.wfile.write(b"data: " + data + b"\n\n")
                self.wfile.flush()
                last_sequence = max(last_sequence, sequence)
                next_send_at = time.monotonic() + cadence_ms / 1000.0
        except (BrokenPipeError, ConnectionResetError, OSError):
            return

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/assets/apex-tool-logo.png":
            if not LOGO_PATH.is_file():
                self._send(404, b"Logo asset not found", "text/plain; charset=utf-8")
                return
            self._send(200, LOGO_PATH.read_bytes(), "image/png")
            return
        if parsed.path == "/":
            start_silent_auto_runs()
            self._send(200, HTML.encode("utf-8"), "text/html; charset=utf-8")
            return
        if parsed.path == "/position-advice":
            self._send(200, POSITION_ADVICE_HTML.encode("utf-8"), "text/html; charset=utf-8")
            return
        if parsed.path == "/api/tickers":
            self.json({"tickers": load_watchlist(), "script": str(PREDICT_SCRIPT)})
            return
        if parsed.path == "/api/learning":
            query = parse_qs(parsed.query)
            force = query.get("force", ["0"])[0].strip().lower() in {"1", "true", "yes"}
            self.json(learning_status_payload(force=force))
            return
        if parsed.path == "/api/learning/diagnostics":
            query = parse_qs(parsed.query)
            force = query.get("force", ["0"])[0].strip().lower() in {"1", "true", "yes"}
            diagnostics = learning_client.diagnostics(force=force)
            with LEARNING_RUNTIME_LOCK:
                diagnostics["dashboard_runtime"] = dict(LEARNING_RUNTIME_STATUS)
            diagnostics["client_status"] = learning_client.last_status()
            self.json(diagnostics)
            return
        if parsed.path == "/api/full-history":
            label = parse_qs(parsed.query).get("label", [""])[0]
            self.json(fetch_full_history_payload(label))
            return
        if parsed.path == "/api/intraday":
            query = parse_qs(parsed.query)
            label = query.get("label", [""])[0]
            interval = query.get("interval", ["1m"])[0]
            range_name = query.get("range", ["5d"])[0]
            item = {
                "label": label,
                "symbol": query.get("symbol", [""])[0],
                "exchange": query.get("exchange", [""])[0],
                "tv_symbol": query.get("tv_symbol", [""])[0],
                "tv_exchange": query.get("tv_exchange", [""])[0],
            }
            self.json(fetch_intraday_payload(label, item, interval, range_name))
            return
        if parsed.path == "/api/intraday-archive":
            query = parse_qs(parsed.query)
            label = query.get("label", [""])[0]
            item = {
                "label": label,
                "symbol": query.get("symbol", [""])[0],
                "exchange": query.get("exchange", [""])[0],
                "tv_symbol": query.get("tv_symbol", [""])[0],
                "tv_exchange": query.get("tv_exchange", [""])[0],
            }
            self.json(
                fetch_intraday_archive_payload(
                    label,
                    item,
                    query.get("start", ["0"])[0],
                    query.get("end", ["0"])[0],
                )
            )
            return
        if parsed.path == "/api/position-advice":
            query = parse_qs(parsed.query)
            label = query.get("label", [""])[0]
            capital = query.get("capital", ["100000"])[0]
            risk_percent = query.get("risk_pct", ["0.5"])[0]
            edge = query.get("edge", ["short"])[0]
            include_chart = query.get("include_chart", ["1"])[0].strip().lower() not in {"0", "false", "no"}
            try:
                payload = fetch_position_advice(label, capital, risk_percent, edge=edge, include_chart=include_chart)
            except Exception as exc:
                payload = {
                    "ok": False,
                    "label": _tv_clean_token(label),
                    "error": f"Position advice calculation failed: {exc}",
                    "retryable": True,
                }
            self.json(payload)
            return
        if parsed.path == "/api/live-stream":
            query = parse_qs(parsed.query)
            labels = query.get("labels", [""])[0].split(",")
            preferred = query.get("preferred", ["auto"])[0]
            client_id = query.get("client", ["dashboard"])[0]
            try:
                cadence_ms = int(query.get("cadence", ["250"])[0])
            except Exception:
                cadence_ms = 250
            self.sse_live_stream(labels, preferred, cadence_ms, client_id=client_id)
            return
        if parsed.path == "/api/stream-status":
            self.json(LIVE_STREAM_HUB.status_payload())
            return
        if parsed.path == "/api/health":
            self.json({"ok": True, "app": APP_NAME, "python": sys.executable, "script": str(PREDICT_SCRIPT), "auto_running": AUTO_RUNNING, "learning": learning_client.health(), "learning_runtime": learning_status_payload(), "live_stream": LIVE_STREAM_HUB.status_payload()})
            return
        if parsed.path == "/api/job":
            job_id = parse_qs(parsed.query).get("id", [""])[0]
            with LOCK:
                job = JOBS.get(job_id)
            if not job:
                self.json({"error": "unknown job"}, 404)
                return
            self.json(job_payload(job))
            return
        self.json({"error": "not found"}, 404)

    def do_POST(self) -> None:
        try:
            length = int(self.headers.get("Content-Length", "0"))
            payload = json.loads(self.rfile.read(length).decode("utf-8") or "{}")
        except Exception:
            payload = {}
        if self.path == "/api/return-prevision":
            self.json(calculate_return_prevision(payload))
            return
        if self.path == "/api/live-prices":
            live_items = payload.get("items") if isinstance(payload, dict) else payload
            preferred = payload.get("preferred_provider", "auto") if isinstance(payload, dict) else "auto"
            self.json(fetch_live_prices(live_items or [], preferred))
            return
        if self.path == "/api/run":
            if not PREDICT_SCRIPT.exists():
                self.json({"error": "tv_predict2.py not found next to tv_dashboard.py"}, 500)
                return
            job = start_job(payload)
            self.json({"id": job.id})
            return
        if self.path == "/api/stop":
            self.json({"stopped": stop_job(str(payload.get("id", "")))})
            return
        if self.path == "/api/auto-start":
            started = start_silent_auto_runs()
            self.json({"running": AUTO_RUNNING, "started": started})
            return
        self.json({"error": "not found"}, 404)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default=os.environ.get("HOST", "127.0.0.1"))
    ap.add_argument("--port", type=int, default=int(os.environ.get("PORT", DEFAULT_PORT)))
    ap.add_argument("--no-browser", action="store_true")
    args = ap.parse_args()
    JOBS_DIR.mkdir(exist_ok=True)
    shown_host = "127.0.0.1" if args.host in {"0.0.0.0", "::"} else args.host
    url = f"http://{shown_host}:{args.port}"
    server = DashboardHTTPServer((args.host, args.port), Handler)
    if not args.no_browser:
        threading.Timer(0.8, lambda: webbrowser.open(url)).start()
    print(f"{APP_NAME} · Web dashboard only · {url}")
    print("Press Ctrl+C to stop the local web server.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
