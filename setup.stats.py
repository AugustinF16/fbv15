#!/usr/bin/env python3
"""
Apex Tool v15 - statistical market analysis and alpha-evidence engine.

This is the canonical research engine used by dashboard.live.prices.py. It
combines multi-source market-data validation, versioned feature engineering,
walk-forward evaluation, benchmark comparisons, probabilistic calibration,
risk metrics, portfolio ranking and auditable per-signal evidence.

Important: outputs are model-generated statistical research and decision-support
analytics. They are not personalised investment advice, regulated investment
research, trade-execution instructions or guarantees of future performance.
"""
from __future__ import annotations

# ── stdlib ────────────────────────────────────────────────────────────
import argparse
import hashlib
import html
from io import StringIO
import json
import math
import os
from pathlib import Path
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import quote_plus
import xml.etree.ElementTree as ET
import warnings
warnings.filterwarnings("ignore")


def _maybe_reexec_from_local_venv() -> None:
    """Make VS Code's Run button work when a local ./venv exists."""
    if os.environ.get("TVPREDICT_NO_VENV_REEXEC") == "1":
        return
    if getattr(sys, "base_prefix", sys.prefix) != sys.prefix:
        return
    here = Path(__file__).resolve().parent
    candidates = [here / "venv" / "bin" / "python", here / ".venv" / "bin" / "python"]
    for py in candidates:
        if py.exists() and py.resolve() != Path(sys.executable).resolve():
            os.environ["TVPREDICT_NO_VENV_REEXEC"] = "1"
            os.execv(str(py), [str(py), *sys.argv])


_maybe_reexec_from_local_venv()

# ── third-party ───────────────────────────────────────────────────────
try:
    import numpy as np
    import pandas as pd
except Exception as exc:  # pragma: no cover - user environment issue
    print("Missing dependency: install numpy and pandas in your venv.", file=sys.stderr)
    raise

try:
    import requests
except Exception:  # News becomes unavailable, predictions still work.
    requests = None

try:
    from tvDatafeed import TvDatafeed, Interval
except Exception:
    TvDatafeed = None
    Interval = None

try:
    from sklearn.pipeline import make_pipeline
    from sklearn.preprocessing import StandardScaler
    from sklearn.linear_model import Ridge, HuberRegressor
    from sklearn.metrics import mean_absolute_error
except Exception:
    make_pipeline = None
    StandardScaler = None
    Ridge = None
    HuberRegressor = None
    mean_absolute_error = None

try:
    from sklearn.ensemble import GradientBoostingRegressor
except Exception:
    GradientBoostingRegressor = None

try:
    import lightgbm as lgb
except Exception:
    lgb = None

try:
    import xgboost as xgb
except Exception:
    xgb = None


WATCHLIST = [('005930', '005930', 'KRX', 'Samsung Electronics Co., Ltd.'),
 ('0700', '0700', 'HKG', 'Tencent'),
 ('2222', '2222', 'TADAWUL', 'Saudi Aramco'),
 ('2317', '2317', 'TPE', 'Foxconn'),
 ('2330', '2330', 'TPE', 'TSMC (TPE)'),
 ('2357', '2357', 'TPE', 'ASUS'),
 ('AAPL', 'AAPL', 'NASDAQ', 'Apple'),
 ('ABBV', 'ABBV', 'NYSE', 'AbbVie'),
 ('AMAT', 'AMAT', 'NASDAQ', 'Applied Materials, Inc.'),
 ('AMD', 'AMD', 'NASDAQ', 'AMD'),
 ('AMZN', 'AMZN', 'NASDAQ', 'Amazon'),
 ('AON', 'AON', 'NYSE', 'Aon'),
 ('APH', 'APH', 'NYSE', 'Amphenol Corporation'),
 ('ARM', 'ARM', 'NASDAQ', 'ARM Holdings'),
 ('ASML', 'ASML', 'AMS', 'ASML'),
 ('ASML_NYC', 'ASML', 'NASDAQ', 'ASML Holding N.V. - New York Registry Shares'),
 ('AVGO', 'AVGO', 'NASDAQ', 'Broadcom'),
 ('AXP', 'AXP', 'NYSE', 'American Express'),
 ('BAC', 'BAC', 'NYSE', 'Bank of America'),
 ('BARC', 'BARC', 'LON', 'Barclays'),
 ('BLK', 'BLK', 'NYSE', 'BlackRock'),
 ('BTCUSD', 'BTCUSD', 'COINBASE', 'Bitcoin / U.S. Dollar'),
 ('BRKA', 'BRK.A', 'NYSE', 'Berkshire Hathaway A'),
 ('BRKB', 'BRK.B', 'NYSE', 'Berkshire Hathaway B'),
 ('BX', 'BX', 'NYSE', 'Blackstone'),
 ('C', 'C', 'NYSE', 'Citigroup'),
 ('CAC40', 'PX1', 'INDEXEURO', 'CAC 40'),
 ('CAT', 'CAT', 'NYSE', 'Caterpillar, Inc.'),
 ('CME', 'CME', 'NASDAQ', 'CME Group'),
 ('COST', 'COST', 'NASDAQ', 'Costco'),
 ('CSCO', 'CSCO', 'NASDAQ', 'Cisco Systems, Inc.'),
 ('DELL', 'DELL', 'NYSE', 'Dell Technologies Inc.'),
 ('DJI', '.DJI', 'INDEXDJX', 'Dow Jones Industrial'),
 ('FTSE', 'UKX', 'INDEXFTSE', 'FTSE 100'),
 ('GE', 'GE', 'NYSE', 'General Electric'),
 ('GEV', 'GEV', 'NYSE', 'General Electric Vernova'),
 ('GLE', 'GLE', 'EPA', 'Société Générale'),
 ('GOOG', 'GOOG', 'NASDAQ', 'Alphabet (GOOG)'),
 ('GOOGL', 'GOOGL', 'NASDAQ', 'Alphabet (GOOGL)'),
 ('GS', 'GS', 'NYSE', 'Goldman Sachs'),
 ('GSOX', 'GSOX', 'NASDAQ', 'Nasdaq Global Semiconductor Index'),
 ('GSOXNR', 'GSOXNR', 'NASDAQ', 'Nasdaq Global Semiconductor Net Total Return Index'),
 ('GSOXTR', 'GSOXTR', 'NASDAQ', 'Nasdaq Global Semiconductor Total Return Index'),
 ('HSBA', 'HSBA', 'LON', 'HSBC (London)'),
 ('HSBC', 'HSBC', 'NYSE', 'HSBC (NYSE)'),
 ('HSI', 'HSI', 'INDEXHANGSENG', 'Hang Seng Index'),
 ('IBIT', 'IBIT', 'NASDAQ', 'iShares Bitcoin ETF'),
 ('IBM', 'IBM', 'NYSE', 'IBM'),
 ('INTC', 'INTC', 'NASDAQ', 'Intel'),
 ('ISVAF', 'ISVAF', 'OTCMKTS', 'iShares NQ100 UCITS'),
 ('IUVL', 'IUVL', 'LSE', 'iShares Edge MSCI USA Value Factor UCITS ETF USD A'),
 ('IXIC', '.IXIC', 'INDEXNASDAQ', 'Nasdaq Composite'),
 ('JPM', 'JPM', 'NYSE', 'JPMorgan Chase'),
 ('KKR', 'KKR', 'NYSE', 'KKR & Co'),
 ('KO', 'KO', 'NYSE', 'Coca-Cola'),
 ('LLY', 'LLY', 'NYSE', 'Eli Lilly'),
 ('LMT', 'LMT', 'NYSE', 'Lockheed Martin'),
 ('LRCX', 'LRCX', 'NASDAQ', 'Lam Research Corporation'),
 ('LVMH', 'MC', 'EPA', 'LVMH'),
 ('MA', 'MA', 'NYSE', 'Mastercard'),
 ('META', 'META', 'NASDAQ', 'Meta'),
 ('MPWR', 'MPWR', 'NASDAQ', 'Monolithic Power Systems Inc'),
 ('MS', 'MS', 'NYSE', 'Morgan Stanley'),
 ('MSFT', 'MSFT', 'NASDAQ', 'Microsoft'),
 ('MU', 'MU', 'NASDAQ', 'Micron Technology'),
 ('NDAQ', 'NDAQ', 'NASDAQ', 'Nasdaq Inc'),
 ('NFLX', 'NFLX', 'NASDAQ', 'Netflix'),
 ('NVDA', 'NVDA', 'NASDAQ', 'Nvidia'),
 ('NYA', 'NYA', 'INDEXNYSEGIS', 'NYSE Composite'),
 ('NYFANG', 'NYFANG', 'INDEXNYSEGIS', 'NYSE FANG+ Index'),
 ('ORCL', 'ORCL', 'NYSE', 'Oracle'),
 ('PANW', 'PANW', 'NASDAQ', 'Palo Alto Networks, Inc.'),
 ('PFE', 'PFE', 'NYSE', 'Pfizer'),
 ('PLTR', 'PLTR', 'NASDAQ', 'Palantir'),
 ('QCOM', 'QCOM', 'NASDAQ', 'Qualcomm'),
 ('RMS', 'RMS', 'EPA', 'Hermès'),
 ('RTX', 'RTX', 'NYSE', 'RTX Corporation'),
 ('RY', 'RY', 'NYSE', 'Royal Bank of Canada'),
 ('SAF', 'SAF', 'EPA', 'Safran'),
 ('SEMIEW5T', 'SEMIEW5T', 'ICE', 'NYSE Semiconductor Top 5 Equal Weight Index TR'),
 ('SMCI', 'SMCI', 'NASDAQ', 'Supermicro'),
 ('SMH', 'SMH', 'NASDAQ', 'VanEck Semiconductor ETF'),
 ('SMH_EPA', 'SMH', 'EURONEXT', 'VanEck Semiconductor UCITS ETF USD A'),
 ('SNDK', 'SNDK', 'NASDAQ', 'Sandisk Corporation'),
 ('SONY', 'SONY', 'NYSE', 'Sony'),
 ('SPOT', 'SPOT', 'NYSE', 'Spotify'),
 ('SPCX', 'SPCX', 'NASDAQ', 'Space Exploration Technologies Corp'),
 ('SPX', '.INX', 'INDEXSP', 'S&P 500'),
 ('SPY', 'SPY', 'NYSEARCA', 'SPDR S&P 500 ETF'),
 ('STX', 'STX', 'NASDAQ', 'Seagate Technology Holdings PLC'),
 ('T', 'T', 'NYSE', 'AT&T'),
 ('TD', 'TD', 'NYSE', 'Toronto Dominion Bank (The)'),
 ('TSLA', 'TSLA', 'NASDAQ', 'Tesla'),
 ('TSM', 'TSM', 'NYSE', 'TSMC (NYC)'),
 ('TTWO', 'TTWO', 'NASDAQ', 'Take-Two Interactive'),
 ('TXN', 'TXN', 'NASDAQ', 'Texas Instruments Incorporated'),
 ('UAL', 'UAL', 'NASDAQ', 'United Airlines'),
 ('UBS', 'UBS', 'NYSE', 'UBS'),
 ('UNH', 'UNH', 'NYSE', 'UnitedHealth'),
 ('V', 'V', 'NYSE', 'Visa'),
 ('VGT', 'VGT', 'AMEX', 'Vanguard Information Technology ETF'),
 ('VIX', 'VIX', 'INDEXCBOE', 'VIX'),
 ('VXN', 'VXN', 'INDEXCBOE', 'CBOE NASDAQ Volatility'),
 ('WDC', 'WDC', 'NASDAQ', 'Western Digital Corporation'),
 ('WFC', 'WFC', 'NYSE', 'Wells Fargo'),
 ('WMT', 'WMT', 'NASDAQ', 'Walmart')]

# Primary mapping. Fetching also uses fallbacks below.
TV_MAP = {'005930': ('005930', 'KRX'),
 '0700': ('700', 'HKEX'),
 '2222': ('2222', 'TADAWUL'),
 '2317': ('2317', 'TWSE'),
 '2330': ('2330', 'TWSE'),
 '2357': ('2357', 'TWSE'),
 'AAPL': ('AAPL', 'NASDAQ'),
 'ABBV': ('ABBV', 'NYSE'),
 'AMAT': ('AMAT', 'NASDAQ'),
 'AMD': ('AMD', 'NASDAQ'),
 'AMZN': ('AMZN', 'NASDAQ'),
 'AON': ('AON', 'NYSE'),
 'APH': ('APH', 'NYSE'),
 'ARM': ('ARM', 'NASDAQ'),
 'ASML': ('ASML', 'EURONEXT'),
 'ASML_NYC': ('ASML', 'NASDAQ'),
 'AVGO': ('AVGO', 'NASDAQ'),
 'AXP': ('AXP', 'NYSE'),
 'BAC': ('BAC', 'NYSE'),
 'BARC': ('BARC', 'LSE'),
 'BLK': ('BLK', 'NYSE'),
 'BTCUSD': ('BTCUSD', 'COINBASE'),
 'BRKA': ('BRK.A', 'NYSE'),
 'BRKB': ('BRK.B', 'NYSE'),
 'BX': ('BX', 'NYSE'),
 'C': ('C', 'NYSE'),
 'CAC40': ('CAC40', 'TVC'),
 'CAT': ('CAT', 'NYSE'),
 'CME': ('CME', 'NASDAQ'),
 'COST': ('COST', 'NASDAQ'),
 'CSCO': ('CSCO', 'NASDAQ'),
 'DELL': ('DELL', 'NYSE'),
 'DJI': ('DJI', 'TVC'),
 'FTSE': ('UKX', 'TVC'),
 'GE': ('GE', 'NYSE'),
 'GEV': ('GEV', 'NYSE'),
 'GLE': ('GLE', 'EURONEXT'),
 'GOOG': ('GOOG', 'NASDAQ'),
 'GOOGL': ('GOOGL', 'NASDAQ'),
 'GS': ('GS', 'NYSE'),
 'GSOX': ('GSOX', 'NASDAQ'),
 'GSOXNR': ('GSOXNR', 'NASDAQ'),
 'GSOXTR': ('GSOXTR', 'NASDAQ'),
 'HSBA': ('HSBA', 'LSE'),
 'HSBC': ('HSBC', 'NYSE'),
 'HSI': ('HSI', 'TVC'),
 'IBIT': ('IBIT', 'NASDAQ'),
 'IBM': ('IBM', 'NYSE'),
 'INTC': ('INTC', 'NASDAQ'),
 'ISVAF': ('ISVAF', 'OTC'),
 'IUVL': ('IUVL', 'LSE'),
 'IXIC': ('IXIC', 'NASDAQ'),
 'JPM': ('JPM', 'NYSE'),
 'KKR': ('KKR', 'NYSE'),
 'KO': ('KO', 'NYSE'),
 'LLY': ('LLY', 'NYSE'),
 'LMT': ('LMT', 'NYSE'),
 'LRCX': ('LRCX', 'NASDAQ'),
 'LVMH': ('MC', 'EURONEXT'),
 'MA': ('MA', 'NYSE'),
 'META': ('META', 'NASDAQ'),
 'MPWR': ('MPWR', 'NASDAQ'),
 'MS': ('MS', 'NYSE'),
 'MSFT': ('MSFT', 'NASDAQ'),
 'MU': ('MU', 'NASDAQ'),
 'NDAQ': ('NDAQ', 'NASDAQ'),
 'NFLX': ('NFLX', 'NASDAQ'),
 'NVDA': ('NVDA', 'NASDAQ'),
 'NYA': ('NYA', 'TVC'),
 'NYFANG': ('NYFANG', 'ICEUS'),
 'ORCL': ('ORCL', 'NYSE'),
 'PANW': ('PANW', 'NASDAQ'),
 'PFE': ('PFE', 'NYSE'),
 'PLTR': ('PLTR', 'NASDAQ'),
 'QCOM': ('QCOM', 'NASDAQ'),
 'RMS': ('RMS', 'EURONEXT'),
 'RTX': ('RTX', 'NYSE'),
 'RY': ('RY', 'NYSE'),
 'SAF': ('SAF', 'EURONEXT'),
 'SEMIEW5T': ('SEMIEW5T', 'ICE'),
 'SMCI': ('SMCI', 'NASDAQ'),
 'SMH': ('SMH', 'NASDAQ'),
 'SMH_EPA': ('SMH', 'EURONEXT'),
 'SNDK': ('SNDK', 'NASDAQ'),
 'SONY': ('SONY', 'NYSE'),
 'SPOT': ('SPOT', 'NYSE'),
 'SPCX': ('SPCX', 'NASDAQ'),
 'SPX': ('SPX', 'TVC'),
 'SPY': ('SPY', 'AMEX'),
 'STX': ('STX', 'NASDAQ'),
 'T': ('T', 'NYSE'),
 'TD': ('TD', 'NYSE'),
 'TSLA': ('TSLA', 'NASDAQ'),
 'TSM': ('TSM', 'NYSE'),
 'TTWO': ('TTWO', 'NASDAQ'),
 'TXN': ('TXN', 'NASDAQ'),
 'UAL': ('UAL', 'NASDAQ'),
 'UBS': ('UBS', 'NYSE'),
 'UNH': ('UNH', 'NYSE'),
 'V': ('V', 'NYSE'),
 'VGT': ('VGT', 'AMEX'),
 'VIX': ('VIX', 'TVC'),
 'VXN': ('VXN', 'CBOE'),
 'WDC': ('WDC', 'NASDAQ'),
 'WFC': ('WFC', 'NYSE'),
 'WMT': ('WMT', 'NASDAQ')}

INDEX_FALLBACKS = {'CAC40': [('CAC40', 'TVC'), ('PX1', 'EURONEXT'), ('FRA40', 'CAPITALCOM')],
 'DJI': [('DJI', 'TVC'), ('US30', 'CAPITALCOM'), ('DJI', 'DJ')],
 'FTSE': [('UKX', 'TVC'), ('UK100', 'CAPITALCOM'), ('FTSE', 'TVC')],
 'GSOX': [('GSOX', 'NASDAQ')],
 'GSOXNR': [('GSOXNR', 'NASDAQ')],
 'GSOXTR': [('GSOXTR', 'NASDAQ')],
 'HSI': [('HSI', 'TVC'), ('HK50', 'CAPITALCOM'), ('HSI', 'HSI')],
 'IUVL': [('IUVL', 'LSE')],
 'IXIC': [('IXIC', 'NASDAQ'), ('NAS100', 'CAPITALCOM'), ('NDX', 'NASDAQ')],
 'NYA': [('NYA', 'TVC'), ('NYA', 'NYSE')],
 'NYFANG': [('NYFANG', 'ICEUS'), ('NYFANG', 'TVC')],
 'SEMIEW5T': [('SEMIEW5T', 'ICE')],
 'SMH': [('SMH', 'NASDAQ')],
 'SMH_EPA': [('SMH', 'EURONEXT')],
 'SPX': [('SPX', 'TVC'), ('US500', 'CAPITALCOM'), ('SPX', 'SP')],
 'VGT': [('VGT', 'AMEX')],
 'VIX': [('VIX', 'TVC'), ('VIX', 'CBOE')],
 'VXN': [('VXN', 'CBOE'), ('VXN', 'TVC')]}

SPECIAL_FALLBACKS = {'BRKA': [('BRK.A', 'NYSE'), ('BRK/A', 'NYSE')],
 'BRKB': [('BRK.B', 'NYSE'), ('BRK/B', 'NYSE')],
 'BTCUSD': [('BTCUSD', 'COINBASE'), ('BTCUSD', 'BITSTAMP')],
 'PLTR': [('PLTR', 'NASDAQ')],
 'SPY': [('SPY', 'AMEX'), ('SPY', 'NYSEARCA')],
 'WMT': [('WMT', 'NASDAQ')]}

POS_WORDS = {
    "beat", "beats", "raise", "raises", "raised", "upgrade", "upgraded", "bullish",
    "record", "growth", "strong", "buy", "outperform", "partnership", "launch", "demand",
    "profit", "profits", "revenue", "earnings", "rally", "rallies", "gain", "gains",
    "surge", "surges", "approval", "approved", "expands", "expansion", "optimistic",
    "winner", "positive", "accelerates", "recover", "recovery", "guidance", "resilient",
}
NEG_WORDS = {
    "miss", "misses", "cut", "cuts", "downgrade", "downgraded", "bearish", "weak",
    "lawsuit", "probe", "investigation", "fall", "falls", "drop", "drops", "plunge",
    "plunges", "recall", "delay", "delays", "ban", "bans", "antitrust", "warning",
    "warns", "loss", "losses", "layoffs", "slows", "lower", "tariff", "fraud",
    "negative", "selloff", "sell-off", "missed", "pressure", "risk", "risks", "cuts",
}

DEFAULT_DAYS = 5
DEFAULT_BARS = 1250
FAST_BARS = 650
DEEP_BARS = 2200
FULL_CHART_BARS = int(os.environ.get("APEX_FULL_CHART_BARS", "20000"))
MIN_ROWS_FOR_FORECAST = 85
RECENT_LISTING_MIN_ROWS = 15
RECENT_LISTING_SHORT_HISTORY = {"SPCX"}
MIN_DISPLAY_PCT = 0.005  # shown as "stable" instead of 0.00% in the UI
ENGINE_NAME = "setup-stats-bot"
ENGINE_VERSION = "15.0-alpha-evidence"
FEATURE_VERSION = "features-v3-technical-regime-quality"
ALPHA_HORIZONS = (1, 3, 5, 10, 20)
DEFAULT_TRANSACTION_COST_BPS = float(os.environ.get("APEX_TRANSACTION_COST_BPS", "5"))
MAX_BACKTEST_SAMPLES = int(os.environ.get("APEX_MAX_BACKTEST_SAMPLES", "420"))
QUALITY_MAX_MISSING_PCT = 2.0
QUALITY_MAX_GAP_DAYS = 10
HISTORY_HTTP_TIMEOUT = float(os.environ.get("APEX_HISTORY_HTTP_TIMEOUT", "6"))
HISTORY_CACHE_MAX_BARS = int(os.environ.get("APEX_HISTORY_CACHE_MAX_BARS", str(FULL_CHART_BARS)))
HISTORY_CACHE_DIR = Path(
    os.environ.get("APEX_HISTORY_CACHE_DIR", str(Path(__file__).resolve().parent / ".apex_market_cache" / "daily"))
)
US_HISTORY_EXCHANGES = {"NASDAQ", "NYSE", "AMEX", "NYSEARCA", "OTC", "OTCMKTS"}
INDEX_HISTORY_LABELS = {
    "CAC40", "DJI", "FTSE", "GSOX", "GSOXNR", "GSOXTR", "HSI", "IXIC",
    "NYA", "NYFANG", "SEMIEW5T", "SPX", "VIX", "VXN",
}
YAHOO_HISTORY_ALIASES = {
    "005930": "005930.KS",
    "BTCUSD": "BTC-USD",
    "CAC40": "^FCHI",
    "DJI": "^DJI",
    "FTSE": "^FTSE",
    "HSI": "^HSI",
    "IXIC": "^IXIC",
    "NYA": "^NYA",
    "NYFANG": "^NYFANG",
    "SPX": "^GSPC",
    "VIX": "^VIX",
    "VXN": "^VXN",
}
STOOQ_INDEX_ALIASES = {
    "CAC40": "^cac",
    "DJI": "^dji",
    "FTSE": "^ukx",
    "HSI": "^hsi",
    "IXIC": "^ndq",
    "SPX": "^spx",
    "VIX": "^vix",
}
HISTORY_PROVIDER_COOLDOWN_UNTIL: dict[str, float] = {}
HISTORY_PROVIDER_COOLDOWN_REASON: dict[str, str] = {}

_TV = None


def _tv() -> object:
    global _TV
    if TvDatafeed is None or Interval is None:
        raise RuntimeError("tvDatafeed is not installed. Run: python -m pip install git+https://github.com/rongardF/tvdatafeed.git")
    if _TV is None:
        _TV = TvDatafeed()
    return _TV


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _write_json(path: str | None, payload: dict) -> None:
    if not path:
        return
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(p)


def _progress(progress_file: str | None, current: int, total: int, message: str, **extra) -> None:
    pct = 0 if total <= 0 else round(current / total * 100, 1)
    payload = {"current": current, "total": total, "percent": pct, "message": message, "updated_at": _utc_now_iso()}
    payload.update(extra)
    _write_json(progress_file, payload)


def label_from_original(sym: str, exch: str) -> str | None:
    for label, original_sym, original_exch, _name in WATCHLIST:
        if original_sym == sym and original_exch == exch:
            return label
    return None


def info_from_label(label: str) -> tuple[str, str, str, str] | None:
    u = label.upper()
    for item in WATCHLIST:
        if item[0].upper() == u or item[1].upper() == u:
            return item
    return None


def tv_candidates_for(label: str) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    if label in INDEX_FALLBACKS:
        out.extend(INDEX_FALLBACKS[label])
    if label in SPECIAL_FALLBACKS:
        out.extend(SPECIAL_FALLBACKS[label])
    if label in TV_MAP:
        out.append(TV_MAP[label])
    item = info_from_label(label)
    if item:
        _label, original_sym, original_exch, _name = item
        if original_exch in {"NASDAQ", "NYSE", "AMEX", "LSE", "EURONEXT", "HKEX", "TWSE", "TADAWUL", "KRX"}:
            out.append((original_sym, original_exch))
        if original_exch == "NYSEARCA":
            out.append((original_sym, "AMEX"))
    # De-duplicate while preserving order.
    seen = set()
    clean = []
    for sym, exch in out:
        key = (str(sym).upper(), str(exch).upper())
        if key not in seen:
            seen.add(key)
            clean.append((str(sym), str(exch)))
    return clean


def normalize_history(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    rename = {"open": "Open", "high": "High", "low": "Low", "close": "Close", "volume": "Volume"}
    df.rename(columns=rename, inplace=True)
    needed = ["Open", "High", "Low", "Close", "Volume"]
    for c in needed:
        if c not in df.columns:
            df[c] = np.nan if c != "Volume" else 0.0
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df.dropna(subset=["Open", "High", "Low", "Close"], inplace=True)
    df["Volume"] = df["Volume"].fillna(0)
    df = df[needed].sort_index()
    df = df[~df.index.duplicated(keep="last")]
    df.index = pd.to_datetime(df.index).tz_localize(None)
    df.index.name = "date"
    return df


def _history_source(label: str, provider: str, symbol: str, exchange: str, **extra) -> dict:
    tv_candidates = tv_candidates_for(label)
    tv_symbol, tv_exchange = tv_candidates[0] if tv_candidates else (None, None)
    out = {
        "provider": provider,
        "symbol": symbol,
        "exchange": exchange,
        "tv_symbol": tv_symbol,
        "tv_exchange": tv_exchange,
        "errors": [],
        "commercial_license_required": provider not in {"Local validated cache"},
    }
    out.update(extra)
    return out


def _history_cache_paths(label: str) -> tuple[Path, Path, Path]:
    safe = re.sub(r"[^A-Z0-9_.-]+", "_", str(label or "").upper()).strip("._") or "UNKNOWN"
    return (
        HISTORY_CACHE_DIR / f"{safe}.parquet",
        HISTORY_CACHE_DIR / f"{safe}.json.gz",
        HISTORY_CACHE_DIR / f"{safe}.meta.json",
    )


def _read_history_cache(label: str, n_bars: int) -> tuple[pd.DataFrame | None, dict]:
    parquet_path, json_path, meta_path = _history_cache_paths(label)
    frame = None
    cache_format = None
    try:
        if parquet_path.exists():
            frame = pd.read_parquet(parquet_path)
            cache_format = "parquet"
        elif json_path.exists():
            frame = pd.read_json(json_path, orient="table", compression="gzip")
            cache_format = "json.gz"
    except Exception:
        frame = None
    if frame is None or frame.empty:
        return None, {}
    try:
        metadata = json.loads(meta_path.read_text(encoding="utf-8")) if meta_path.exists() else {}
    except Exception:
        metadata = {}
    clean = normalize_history(frame).tail(max(5, int(n_bars)))
    source = _history_source(
        label,
        "Local validated cache",
        str(metadata.get("symbol") or label),
        str(metadata.get("exchange") or "CACHE"),
        delayed=True,
        cache_format=cache_format,
        cached_at=metadata.get("cached_at"),
        original_provider=metadata.get("provider"),
        commercial_license_required=bool(metadata.get("commercial_license_required", True)),
    )
    return clean, source


def _write_history_cache(label: str, df: pd.DataFrame, source: dict) -> None:
    """Persist only completed UTC daily bars; Parquet is preferred when available."""
    try:
        clean = normalize_history(df)
        cutoff = pd.Timestamp(datetime.now(timezone.utc).date())
        clean = clean[clean.index.normalize() < cutoff].tail(max(5, HISTORY_CACHE_MAX_BARS))
        if len(clean) < 5:
            return
        HISTORY_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        parquet_path, json_path, meta_path = _history_cache_paths(label)
        cache_format = "parquet"
        try:
            tmp = parquet_path.with_suffix(parquet_path.suffix + ".tmp")
            clean.to_parquet(tmp)
            tmp.replace(parquet_path)
            json_path.unlink(missing_ok=True)
        except Exception:
            cache_format = "json.gz"
            tmp = json_path.with_suffix(json_path.suffix + ".tmp")
            clean.to_json(tmp, orient="table", date_format="iso", compression="gzip")
            tmp.replace(json_path)
            parquet_path.unlink(missing_ok=True)
        metadata = {
            "label": label,
            "provider": source.get("provider"),
            "symbol": source.get("symbol"),
            "exchange": source.get("exchange"),
            "commercial_license_required": source.get("commercial_license_required", True),
            "cached_at": _utc_now_iso(),
            "rows": len(clean),
            "first_date": clean.index[0].strftime("%Y-%m-%d"),
            "last_date": clean.index[-1].strftime("%Y-%m-%d"),
            "format": cache_format,
        }
        tmp_meta = meta_path.with_suffix(meta_path.suffix + ".tmp")
        tmp_meta.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp_meta.replace(meta_path)
    except Exception:
        return


def _merge_history_frames(cached: pd.DataFrame | None, fresh: pd.DataFrame, n_bars: int) -> pd.DataFrame:
    clean_fresh = normalize_history(fresh)
    if cached is None or cached.empty:
        return clean_fresh.tail(max(5, int(n_bars)))
    merged = pd.concat([normalize_history(cached), clean_fresh])
    merged = merged[~merged.index.duplicated(keep="last")].sort_index()
    return normalize_history(merged).tail(max(5, int(n_bars)))


def _history_provider_cooldown(provider: str, error: Exception) -> None:
    message = str(error or "")
    lower = message.lower()
    seconds = 0
    if any(token in lower for token in ("http 401", "http 403", "authentication", "unauthorized", "forbidden")):
        seconds = 300
    elif any(token in lower for token in ("http 429", "rate limit", "too many requests")):
        seconds = 60
    elif any(token in lower for token in ("name resolution", "failed to resolve", "nodename nor servname", "connection", "timed out", "timeout", "certificate")):
        seconds = 20
    if seconds:
        HISTORY_PROVIDER_COOLDOWN_UNTIL[provider] = time.monotonic() + seconds
        HISTORY_PROVIDER_COOLDOWN_REASON[provider] = message[:160]


def _history_provider_skip_reason(provider: str) -> str | None:
    remaining = HISTORY_PROVIDER_COOLDOWN_UNTIL.get(provider, 0.0) - time.monotonic()
    if remaining <= 0:
        HISTORY_PROVIDER_COOLDOWN_UNTIL.pop(provider, None)
        HISTORY_PROVIDER_COOLDOWN_REASON.pop(provider, None)
        return None
    reason = HISTORY_PROVIDER_COOLDOWN_REASON.get(provider, "temporary provider failure")
    return f"temporarily skipped for {max(1, int(math.ceil(remaining)))}s after {reason}"


def _fetch_alpaca_daily(label: str, n_bars: int) -> tuple[pd.DataFrame, dict]:
    if requests is None:
        raise RuntimeError("requests unavailable")
    item = info_from_label(label)
    if item is None or item[2] not in US_HISTORY_EXCHANGES or label in RECENT_LISTING_SHORT_HISTORY:
        raise RuntimeError("not an Alpaca-compatible listed US instrument")
    api_key = os.environ.get("APCA_API_KEY_ID") or os.environ.get("ALPACA_API_KEY") or ""
    api_secret = os.environ.get("APCA_API_SECRET_KEY") or os.environ.get("ALPACA_API_SECRET") or ""
    if not api_key or not api_secret:
        raise RuntimeError("Alpaca credentials not configured")
    symbol = str(item[1]).upper()
    now = datetime.now(timezone.utc)
    earliest = datetime(2016, 1, 1, tzinfo=timezone.utc)
    requested_start = now - timedelta(days=max(370, int(n_bars * 1.9)))
    start = max(earliest, requested_start)
    end = now - timedelta(minutes=16)
    preferred_feed = str(os.environ.get("APEX_ALPACA_HISTORY_FEED", "iex") or "iex").lower()
    feeds = list(dict.fromkeys([preferred_feed, "iex"]))
    attempts = []
    for feed in feeds:
        bars: list[dict] = []
        page_token = None
        try:
            for _page in range(4):
                params = {
                    "timeframe": "1Day",
                    "start": start.isoformat().replace("+00:00", "Z"),
                    "end": end.isoformat().replace("+00:00", "Z"),
                    "limit": min(10000, max(1000, int(n_bars))),
                    "adjustment": "all",
                    "feed": feed,
                    "sort": "asc",
                }
                if page_token:
                    params["page_token"] = page_token
                response = requests.get(
                    f"https://data.alpaca.markets/v2/stocks/{quote_plus(symbol)}/bars",
                    params=params,
                    headers={"APCA-API-KEY-ID": api_key, "APCA-API-SECRET-KEY": api_secret},
                    timeout=HISTORY_HTTP_TIMEOUT,
                )
                if response.status_code != 200:
                    raise RuntimeError(f"HTTP {response.status_code}: {response.text[:120]}")
                payload = response.json()
                bars.extend(payload.get("bars") or [])
                page_token = payload.get("next_page_token")
                if not page_token:
                    break
            if not bars:
                raise RuntimeError("empty daily series")
            frame = pd.DataFrame(
                {
                    "Open": [row.get("o") for row in bars],
                    "High": [row.get("h") for row in bars],
                    "Low": [row.get("l") for row in bars],
                    "Close": [row.get("c") for row in bars],
                    "Volume": [row.get("v", 0) for row in bars],
                },
                index=pd.to_datetime([row.get("t") for row in bars], utc=True, errors="coerce").tz_localize(None),
            )
            frame = normalize_history(frame)
            if frame.empty:
                raise RuntimeError("no valid daily bars")
            return frame, _history_source(label, f"Alpaca {feed.upper()} Historical", symbol, item[2], delayed=True, feed=feed)
        except Exception as exc:
            attempts.append(f"{feed}: {exc}")
    raise RuntimeError("; ".join(attempts))


def _fetch_yahoo_daily(label: str, n_bars: int) -> tuple[pd.DataFrame, dict]:
    if requests is None:
        raise RuntimeError("requests unavailable")
    symbol = _yahoo_symbol(label)
    if not symbol:
        raise RuntimeError("Yahoo symbol mapping unavailable")
    now_s = int(time.time())
    lookback_days = max(370, int(math.ceil(max(5, n_bars) * 1.9)))
    response = requests.get(
        f"https://query1.finance.yahoo.com/v8/finance/chart/{quote_plus(symbol)}",
        params={
            "period1": max(0, now_s - lookback_days * 86400),
            "period2": now_s + 86400,
            "interval": "1d",
            "events": "div,splits",
            "includeAdjustedClose": "true",
        },
        headers={"User-Agent": "Mozilla/5.0 ApexMarketPredictor/15.0 historical-fallback"},
        timeout=HISTORY_HTTP_TIMEOUT,
    )
    if response.status_code != 200:
        raise RuntimeError(f"HTTP {response.status_code}")
    chart = response.json().get("chart") or {}
    if chart.get("error"):
        raise RuntimeError(str(chart.get("error")))
    results = chart.get("result") or []
    if not results:
        raise RuntimeError("empty daily series")
    result = results[0]
    timestamps = result.get("timestamp") or []
    quote_rows = ((result.get("indicators") or {}).get("quote") or [{}])[0]
    adj_rows = ((result.get("indicators") or {}).get("adjclose") or [{}])[0].get("adjclose") or []

    def value(values: list, idx: int):
        return values[idx] if idx < len(values) else None

    rows = []
    for idx, raw_ts in enumerate(timestamps):
        raw_close = _safe_num(value(quote_rows.get("close") or [], idx))
        if raw_close is None or raw_close <= 0:
            continue
        adjusted_close = _safe_num(value(adj_rows, idx))
        factor = adjusted_close / raw_close if adjusted_close and adjusted_close > 0 else 1.0
        rows.append(
            {
                "date": pd.to_datetime(raw_ts, unit="s", utc=True).tz_localize(None).normalize(),
                "Open": (_safe_num(value(quote_rows.get("open") or [], idx)) or raw_close) * factor,
                "High": (_safe_num(value(quote_rows.get("high") or [], idx)) or raw_close) * factor,
                "Low": (_safe_num(value(quote_rows.get("low") or [], idx)) or raw_close) * factor,
                "Close": raw_close * factor,
                "Volume": _safe_num(value(quote_rows.get("volume") or [], idx)) or 0,
            }
        )
    if not rows:
        raise RuntimeError("no valid daily bars")
    frame = pd.DataFrame(rows).set_index("date")
    meta = result.get("meta") or {}
    exchange = str(meta.get("fullExchangeName") or meta.get("exchangeName") or (info_from_label(label) or (None, None, "YAHOO"))[2])
    delayed_by = int(_safe_num(meta.get("exchangeDataDelayedBy")) or 0)
    return normalize_history(frame).tail(max(5, int(n_bars))), _history_source(
        label,
        "Yahoo Finance Historical",
        symbol,
        exchange,
        delayed=delayed_by > 0,
        delay_seconds=delayed_by,
    )


def _stooq_candidates(label: str) -> list[str]:
    if label in STOOQ_INDEX_ALIASES:
        return [STOOQ_INDEX_ALIASES[label]]
    item = info_from_label(label)
    if item is None or label == "BTCUSD":
        return []
    _lab, symbol, exchange, _name = item
    root = str(symbol).lower().replace(".", "-")
    suffixes = {
        "NASDAQ": ".us", "NYSE": ".us", "AMEX": ".us", "NYSEARCA": ".us",
        "OTC": ".us", "OTCMKTS": ".us", "LON": ".uk", "LSE": ".uk",
        "EPA": ".fr", "EURONEXT": ".fr", "AMS": ".nl", "HKG": ".hk", "TPE": ".tw",
    }
    suffix = suffixes.get(exchange)
    return [root + suffix] if suffix else []


def _fetch_stooq_daily(label: str, n_bars: int) -> tuple[pd.DataFrame, dict]:
    if requests is None:
        raise RuntimeError("requests unavailable")
    candidates = _stooq_candidates(label)
    if not candidates:
        raise RuntimeError("Stooq symbol mapping unavailable")
    errors = []
    for symbol in candidates:
        try:
            response = requests.get(
                "https://stooq.com/q/d/l/",
                params={"s": symbol, "i": "d"},
                headers={"User-Agent": "Mozilla/5.0 ApexMarketPredictor/15.0 historical-fallback"},
                timeout=HISTORY_HTTP_TIMEOUT,
            )
            if response.status_code != 200:
                raise RuntimeError(f"HTTP {response.status_code}")
            text = response.text.strip()
            if not text or "No data" in text:
                raise RuntimeError("empty daily series")
            frame = pd.read_csv(StringIO(text))
            if "Date" not in frame.columns:
                raise RuntimeError("invalid CSV schema")
            frame["Date"] = pd.to_datetime(frame["Date"], errors="coerce")
            frame = frame.dropna(subset=["Date"]).set_index("Date")
            clean = normalize_history(frame).tail(max(5, int(n_bars)))
            if clean.empty:
                raise RuntimeError("no valid daily bars")
            return clean, _history_source(label, "Stooq Historical", symbol, "STOOQ", delayed=True)
        except Exception as exc:
            errors.append(f"{symbol}: {exc}")
    raise RuntimeError("; ".join(errors))


def _fetch_tradingview_daily(label: str, n_bars: int) -> tuple[pd.DataFrame, dict]:
    errors = []
    for tv_symbol, tv_exchange in tv_candidates_for(label):
        try:
            raw = _tv().get_hist(symbol=tv_symbol, exchange=tv_exchange, interval=Interval.in_daily, n_bars=n_bars)
            if raw is None or raw.empty:
                errors.append(f"{tv_exchange}:{tv_symbol} empty")
                continue
            clean = normalize_history(raw)
            if clean.empty:
                errors.append(f"{tv_exchange}:{tv_symbol} no valid rows")
                continue
            return clean, _history_source(label, "TradingView tvDatafeed", tv_symbol, tv_exchange, delayed=False)
        except Exception as exc:
            errors.append(f"{tv_exchange}:{tv_symbol} {exc}")
    raise RuntimeError("; ".join(errors) or "TradingView mapping unavailable")


def fetch_history_by_label(label: str, n_bars: int = DEFAULT_BARS, min_rows: int = MIN_ROWS_FOR_FORECAST) -> tuple[pd.DataFrame | None, dict]:
    """Load validated OHLCV through a deterministic free-source fallback chain."""
    label = str(label or "").upper().strip()
    item = info_from_label(label)
    exchange = item[2] if item else ""
    cached, cached_source = _read_history_cache(label, n_bars)
    fetchers = []
    if exchange in US_HISTORY_EXCHANGES and label not in INDEX_HISTORY_LABELS:
        fetchers.append(("Alpaca", _fetch_alpaca_daily))
    fetchers.extend([
        ("Yahoo", _fetch_yahoo_daily),
        ("Stooq", _fetch_stooq_daily),
        ("TradingView", _fetch_tradingview_daily),
    ])
    errors: list[str] = []
    best_df: pd.DataFrame | None = None
    best_source: dict | None = None
    for provider_name, fetcher in fetchers:
        skip_reason = _history_provider_skip_reason(provider_name)
        if skip_reason:
            errors.append(f"{provider_name}: {skip_reason}")
            continue
        try:
            fresh, source = fetcher(label, n_bars)
            HISTORY_PROVIDER_COOLDOWN_UNTIL.pop(provider_name, None)
            HISTORY_PROVIDER_COOLDOWN_REASON.pop(provider_name, None)
            merged = _merge_history_frames(cached, fresh, n_bars)
            source["errors"] = list(errors)
            if cached is not None and not cached.empty:
                source["cache_rows_merged"] = len(cached)
            if best_df is None or len(merged) > len(best_df):
                best_df, best_source = merged, source
            if len(merged) < min_rows:
                errors.append(f"{provider_name}: only {len(merged)} validated rows")
                continue
            _write_history_cache(label, merged, source)
            return merged, source
        except Exception as exc:
            _history_provider_cooldown(provider_name, exc)
            errors.append(f"{provider_name}: {exc}")
    if best_df is not None and best_source is not None:
        best_source["errors"] = list(errors)
        _write_history_cache(label, best_df, best_source)
        return best_df, best_source
    if cached is not None and not cached.empty:
        cached_source["errors"] = list(errors)
        return cached, cached_source
    return None, _history_source(label, "Unavailable", label, exchange or "UNKNOWN", errors=errors, delayed=True)


def _ema(s: pd.Series, span: int) -> pd.Series:
    return s.ewm(span=span, adjust=False, min_periods=max(2, min(span, 8))).mean()


def _rsi(close: pd.Series, window: int = 14) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0).ewm(alpha=1 / window, adjust=False, min_periods=window).mean()
    loss = (-delta.clip(upper=0)).ewm(alpha=1 / window, adjust=False, min_periods=window).mean()
    rs = gain / loss.replace(0, np.nan)
    return (100 - 100 / (1 + rs)).fillna(50)


def _atr(df: pd.DataFrame, window: int = 14) -> pd.Series:
    high, low, close = df["High"], df["Low"], df["Close"]
    prev = close.shift(1)
    tr = pd.concat([(high - low).abs(), (high - prev).abs(), (low - prev).abs()], axis=1).max(axis=1)
    return tr.ewm(alpha=1 / window, adjust=False, min_periods=window).mean()


def build_features(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    df = normalize_history(df)
    c = df["Close"].astype(float)
    lr = np.log(c / c.shift(1)).replace([np.inf, -np.inf], np.nan)
    df["LogRet1"] = lr
    for n in (2, 3, 5, 10, 20, 60):
        df[f"Ret{n}"] = np.log(c / c.shift(n)).replace([np.inf, -np.inf], np.nan)
    df["EMA10"] = _ema(c, 10)
    df["EMA20"] = _ema(c, 20)
    df["EMA50"] = _ema(c, 50)
    df["SMA20"] = c.rolling(20, min_periods=8).mean()
    df["SMA50"] = c.rolling(50, min_periods=15).mean()
    df["SMA200"] = c.rolling(200, min_periods=40).mean()
    df["DistEMA20"] = c / df["EMA20"] - 1
    df["DistSMA50"] = c / df["SMA50"] - 1
    df["DistSMA200"] = c / df["SMA200"] - 1
    df["RSI"] = _rsi(c)
    macd = _ema(c, 12) - _ema(c, 26)
    df["MACD"] = macd / c
    df["MACDSignal"] = _ema(macd, 9) / c
    df["ATRpct"] = _atr(df) / c
    for n in (5, 20, 60):
        df[f"Vol{n}"] = lr.rolling(n, min_periods=max(3, n // 3)).std()
    df["RangePct"] = (df["High"] - df["Low"]) / c
    df["VolumeRatio"] = (df["Volume"] / df["Volume"].rolling(20, min_periods=5).mean()).replace([np.inf, -np.inf], np.nan).fillna(1.0)
    df["TargetNext"] = lr.shift(-1)
    features = [
        "LogRet1", "Ret2", "Ret3", "Ret5", "Ret10", "Ret20", "Ret60",
        "DistEMA20", "DistSMA50", "DistSMA200", "RSI", "MACD", "MACDSignal",
        "ATRpct", "Vol5", "Vol20", "Vol60", "RangePct", "VolumeRatio",
    ]
    df.replace([np.inf, -np.inf], np.nan, inplace=True)
    return df, [f for f in features if f in df.columns]


def recent_vol(df: pd.DataFrame, window: int = 40) -> float:
    lr = np.log(df["Close"] / df["Close"].shift(1)).replace([np.inf, -np.inf], np.nan)
    val = float(lr.tail(window).std(skipna=True))
    if not np.isfinite(val) or val <= 1e-5:
        val = 0.012
    return val


def daily_cap(df: pd.DataFrame, low_risk: bool = True) -> float:
    # Low capital-loss tolerance: cap daily expected moves, especially when the model is uncertain.
    vol = recent_vol(df)
    hi = 0.035 if low_risk else 0.055
    return float(np.clip(vol * 1.65, 0.004, hi))


def prices_from_log_returns(last: float, rets: list[float] | np.ndarray) -> np.ndarray:
    out = []
    px = float(last)
    for r in rets:
        px *= float(math.exp(float(r)))
        out.append(px)
    return np.asarray(out, dtype=float)


def technical_returns(df: pd.DataFrame, days: int) -> np.ndarray:
    c = df["Close"].astype(float)
    lr = np.log(c / c.shift(1)).replace([np.inf, -np.inf], np.nan)
    short = float(lr.tail(5).mean(skipna=True))
    medium = float(lr.tail(20).mean(skipna=True))
    long = float(lr.tail(80).mean(skipna=True))
    drift = np.nan_to_num(0.42 * short + 0.38 * medium + 0.20 * long)
    feat, _ = build_features(df)
    rsi = float(feat["RSI"].dropna().iloc[-1]) if "RSI" in feat and feat["RSI"].notna().any() else 50.0
    dist = float(feat["DistEMA20"].dropna().iloc[-1]) if "DistEMA20" in feat and feat["DistEMA20"].notna().any() else 0.0
    vol = recent_vol(df)
    mean_reversion = 0.0
    if rsi > 72 and dist > 0:
        mean_reversion -= min(abs(dist) * 0.035, vol * 0.32)
    elif rsi < 28 and dist < 0:
        mean_reversion += min(abs(dist) * 0.035, vol * 0.32)
    base = float(drift + mean_reversion)
    cap = daily_cap(df)
    return np.clip([base * (0.82 ** i) for i in range(days)], -cap, cap)


def model_returns(df: pd.DataFrame, days: int) -> tuple[np.ndarray, dict]:
    feat_df, features = build_features(df)
    work = feat_df.dropna(subset=features + ["TargetNext"]).copy()
    if len(work) < 120 or make_pipeline is None or Ridge is None:
        return technical_returns(df, days), {"model": "technical-only", "mae": None, "directional_accuracy": None, "weight_hint": 0.0}
    X = work[features].values.astype(float)
    y = work["TargetNext"].values.astype(float)
    split = max(50, int(len(work) * 0.78))
    if split >= len(work) - 20:
        split = len(work) - 30
    models = []
    ridge = make_pipeline(StandardScaler(), Ridge(alpha=8.0))
    models.append(("ridge", ridge))
    if HuberRegressor is not None:
        models.append(("huber", make_pipeline(StandardScaler(), HuberRegressor(alpha=0.001, epsilon=1.25, max_iter=400))))
    if GradientBoostingRegressor is not None and len(work) >= 260:
        models.append(("sklearn_gradient_boosting", GradientBoostingRegressor(random_state=42, max_depth=2, n_estimators=80, learning_rate=0.035)))
    preds_val = []
    preds_live = []
    model_cards = []
    for model_name, m in models:
        try:
            m.fit(X[:split], y[:split])
            pv = m.predict(X[split:])
            live_pred = float(m.predict(X[-1:])[0])
            preds_val.append(pv)
            preds_live.append(live_pred)
            yv_tmp = y[split: split + len(pv)]
            model_cards.append({
                "model": model_name,
                "mae": float(np.mean(np.abs(pv - yv_tmp))) if len(yv_tmp) else None,
                "directional_accuracy": float(np.mean(np.sign(pv) == np.sign(yv_tmp))) if len(yv_tmp) else None,
                "live_return": live_pred,
            })
        except Exception:
            continue
    if not preds_live:
        return technical_returns(df, days), {"model": "technical-fallback", "mae": None, "directional_accuracy": None, "weight_hint": 0.0}
    pred_val = np.mean(np.vstack(preds_val), axis=0)
    yv = y[split: split + len(pred_val)]
    mae = float(np.mean(np.abs(pred_val - yv))) if len(yv) else None
    naive_mae = float(np.mean(np.abs(yv))) if len(yv) else None
    dir_acc = float(np.mean(np.sign(pred_val) == np.sign(yv))) if len(yv) else None
    live = float(np.mean(preds_live))
    # Shrink if validation is weak. This prevents false precision and ±20% nonsense.
    if mae is not None and naive_mae is not None and naive_mae > 0:
        edge = max(0.0, min(1.0, (naive_mae - mae) / naive_mae))
    else:
        edge = 0.0
    direction_edge = max(0.0, ((dir_acc or 0.5) - 0.5) * 2.0)
    weight_hint = float(np.clip(0.18 + 0.42 * edge + 0.20 * direction_edge, 0.15, 0.62))
    live *= weight_hint
    cap = daily_cap(df)
    path = [live * (0.80 ** i) for i in range(days)]
    return np.clip(path, -cap, cap), {
        "model": "ensemble-return" if len(model_cards) > 1 else (model_cards[0]["model"] if model_cards else "technical-fallback"),
        "mae": mae,
        "naive_mae": naive_mae,
        "directional_accuracy": dir_acc,
        "weight_hint": weight_hint,
        "features": features,
        "leaderboard": sorted(model_cards, key=lambda x: (x.get("mae") is None, x.get("mae") or 999)),
        "optional_models": {
            "sklearn_gradient_boosting": bool(GradientBoostingRegressor is not None),
            "lightgbm_available": bool(lgb is not None),
            "xgboost_available": bool(xgb is not None),
        },
    }


def _clean_text(s: str) -> str:
    s = html.unescape(s or "")
    s = re.sub(r"<[^>]+>", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def _headline_score(text: str) -> float:
    words = re.findall(r"[a-zA-Z][a-zA-Z\-']+", text.lower())
    if not words:
        return 0.0
    pos = sum(1 for w in words if w in POS_WORDS)
    neg = sum(1 for w in words if w in NEG_WORDS)
    raw = pos - neg
    if raw == 0:
        return 0.0
    return float(np.clip(raw / 3.0, -1.0, 1.0))



# ── Stable enrichment layer v14.6 ─────────────────────────────────────
# Centralised here so dashboard/UI revisions do not break profile/news fields again.
SCANNER_CACHE: dict[str, dict] = {}
YAHOO_QUOTE_CACHE: dict[str, dict] = {}
PROFILE_CACHE: dict[str, dict] = {}

TV_SCANNER_COLUMNS_FULL = [
    "name", "description", "type", "subtype", "sector", "industry", "exchange", "country", "currency",
    "close", "change", "change_abs", "volume", "average_volume_60d_calc", "Value.Traded",
    "market_cap_basic", "total_revenue", "net_income", "gross_margin_ttm", "operating_margin_ttm", "net_margin_ttm",
    "price_earnings_ttm", "price_sales_current", "price_book_fq", "dividend_yield_recent", "beta_1_year", "number_of_employees",
    "Recommend.All", "Recommend.MA", "Recommend.Other", "RSI", "Perf.3M", "Perf.6M", "Perf.Y",
]
TV_SCANNER_COLUMNS_SAFE = [
    "name", "description", "type", "sector", "industry", "exchange", "country", "currency",
    "close", "change", "change_abs", "volume", "average_volume_60d_calc", "market_cap_basic",
    "price_earnings_ttm", "price_sales_current", "price_book_fq", "dividend_yield_recent", "beta_1_year", "number_of_employees",
    "Recommend.All", "Recommend.MA", "RSI", "Perf.3M", "Perf.6M", "Perf.Y",
]
TV_SCANNER_COLUMNS_MIN = ["name", "description", "type", "sector", "industry", "exchange", "close", "change", "change_abs", "volume", "average_volume_60d_calc", "market_cap_basic", "Recommend.All", "Recommend.MA", "RSI", "Perf.3M", "Perf.6M", "Perf.Y"]


def _chunked(seq, n):
    return [seq[i:i+n] for i in range(0, len(seq), n)]


def _safe_num(x, default=None):
    try:
        if isinstance(x, dict):
            x = x.get("raw", x.get("value", x.get("fmt", x.get("longFmt"))))
        if x is None or x == "":
            return default
        v = float(x)
        if math.isfinite(v):
            return v
    except Exception:
        pass
    return default


def _clean_value(x):
    if isinstance(x, dict):
        for k in ("raw", "fmt", "longFmt", "value"):
            if x.get(k) not in (None, ""):
                return x.get(k)
        return None
    return x


def _fmt_large(x, currency=""):
    v = _safe_num(x)
    if v is None:
        return ""
    cur = "$" if str(currency).upper() == "USD" else ("€" if str(currency).upper() == "EUR" else ("£" if str(currency).upper() in {"GBX","GBP"} else (str(currency)+" " if currency else "")))
    av = abs(v)
    if av >= 1e12: return f"{cur}{v/1e12:.2f}T"
    if av >= 1e9: return f"{cur}{v/1e9:.2f}B"
    if av >= 1e6: return f"{cur}{v/1e6:.2f}M"
    if av >= 1e3: return f"{cur}{v/1e3:.2f}K"
    return f"{cur}{v:.2f}"


def _fmt_number(x, suffix=""):
    v = _safe_num(x)
    if v is None:
        return ""
    if abs(v) >= 1000:
        return f"{v:,.0f}{suffix}"
    return f"{v:.2f}{suffix}".rstrip("0").rstrip(".") + suffix if suffix and not str(v).endswith(suffix) else f"{v:.2f}".rstrip("0").rstrip(".")


def _fmt_pct(x):
    v = _safe_num(x)
    if v is None:
        return ""
    # Some feeds return ratios, others percentage points.
    if abs(v) <= 1.5:
        v *= 100.0
    sign = "+" if v > 0 else ""
    return f"{sign}{v:.2f}%"


def _fmt_ratio(x):
    v = _safe_num(x)
    if v is None:
        return ""
    return f"{v:.2f}".rstrip("0").rstrip(".")


def _tv_rating(x):
    v = _safe_num(x)
    if v is None:
        return ""
    if v >= 0.50: return "Strong buy (TradingView)"
    if v >= 0.10: return "Buy (TradingView)"
    if v <= -0.50: return "Strong sell (TradingView)"
    if v <= -0.10: return "Sell (TradingView)"
    return "Neutral (TradingView)"


def _colmap(columns, values):
    return {c: (values[i] if i < len(values) else None) for i, c in enumerate(columns)}


def _tv_ticker(label: str) -> str:
    item = info_from_label(label)
    if not item:
        return label
    sym, exch = tv_candidates_for(item[0])[0]
    return f"{exch}:{sym}"


def _yahoo_symbol(label: str) -> str:
    item = info_from_label(label)
    if not item:
        return label
    lab, sym, exch, _name = item
    lab = lab.upper()
    if lab in YAHOO_HISTORY_ALIASES: return YAHOO_HISTORY_ALIASES[lab]
    if lab == "BRKA": return "BRK-A"
    if lab == "BRKB": return "BRK-B"
    if lab == "SMH_EPA": return "SMH.PA"
    if exch in {"LON", "LSE"}: return f"{sym}.L"
    if exch in {"EPA", "EURONEXT"}: return f"{sym}.PA"
    if exch in {"AMS"}: return f"{sym}.AS"
    if exch in {"TPE"}: return f"{sym}.TW"
    if exch in {"HKG"}: return f"{str(sym).zfill(4)}.HK"
    if exch in {"TADAWUL"}: return f"{sym}.SR"
    if exch in {"OTCMKTS"}: return sym
    if lab in INDEX_HISTORY_LABELS:
        return ""
    return sym


def prefetch_tradingview_scanner(labels: list[str]) -> None:
    """Fast batch overview/screener fetch from TradingView.

    Failures never break forecasts. Cached rows are keyed by dashboard label.
    """
    if requests is None:
        return
    lab_by_ticker = {}
    tickers = []
    for lab in labels:
        try:
            tvt = _tv_ticker(lab)
            if ":" in tvt:
                tickers.append(tvt)
                lab_by_ticker[tvt.upper()] = lab.upper()
        except Exception:
            pass
    if not tickers:
        return
    domains = ["global", "america", "uk", "france", "netherlands", "hongkong", "taiwan", "korea", "saudiarabia"]
    column_sets = [TV_SCANNER_COLUMNS_FULL, TV_SCANNER_COLUMNS_SAFE, TV_SCANNER_COLUMNS_MIN]
    headers = {"User-Agent": "Mozilla/5.0", "Content-Type": "application/json"}
    for chunk in _chunked(tickers, 50):
        got = set()
        for cols in column_sets:
            if len(got) == len(chunk):
                break
            remaining = [t for t in chunk if t not in got]
            body = {"symbols": {"tickers": remaining, "query": {"types": []}}, "columns": cols, "range": [0, len(remaining)], "options": {"lang": "en"}}
            for domain in domains:
                try:
                    url = f"https://scanner.tradingview.com/{domain}/scan"
                    rr = requests.post(url, data=json.dumps(body), headers=headers, timeout=3.5)
                    if rr.status_code != 200:
                        continue
                    js = rr.json()
                    for row in js.get("data", []) or []:
                        s = str(row.get("s") or "").upper()
                        lab = lab_by_ticker.get(s)
                        if not lab:
                            continue
                        SCANNER_CACHE[lab] = _colmap(cols, row.get("d") or []) | {"_tv_symbol": s, "_scanner_domain": domain}
                        got.add(s)
                    if got:
                        break
                except Exception:
                    continue


def prefetch_yahoo_quotes(labels: list[str]) -> None:
    if requests is None:
        return
    syms=[]; by={}
    for lab in labels:
        y=_yahoo_symbol(lab)
        if y:
            syms.append(y); by[y.upper()]=lab.upper()
    if not syms:
        return
    headers={"User-Agent":"Mozilla/5.0"}
    for chunk in _chunked(syms, 60):
        try:
            url="https://query1.finance.yahoo.com/v7/finance/quote?symbols="+quote_plus(",".join(chunk))
            rr=requests.get(url,headers=headers,timeout=4)
            if rr.status_code!=200: continue
            for q in (rr.json().get("quoteResponse") or {}).get("result",[]) or []:
                sym=str(q.get("symbol") or "").upper(); lab=by.get(sym)
                if lab: YAHOO_QUOTE_CACHE[lab]=q
        except Exception:
            continue


def fetch_yahoo_summary(label: str) -> dict:
    # Optional per-ticker detailed fallback. Short timeout so it cannot destroy run speed.
    if requests is None:
        return {}
    y=_yahoo_symbol(label)
    if not y:
        return {}
    url=f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{quote_plus(y)}?modules=assetProfile,financialData,defaultKeyStatistics,summaryDetail,price,fundProfile"
    try:
        rr=requests.get(url,headers={"User-Agent":"Mozilla/5.0"},timeout=float(os.environ.get("APEX_PROFILE_TIMEOUT","2.2")))
        if rr.status_code!=200:
            return {}
        res=((rr.json().get("quoteSummary") or {}).get("result") or [{}])[0]
        return res or {}
    except Exception:
        return {}


def _qraw(d, *path):
    cur=d
    for p in path:
        if not isinstance(cur, dict): return None
        cur=cur.get(p)
    return _clean_value(cur)


def build_profile(label: str, name: str, df: pd.DataFrame, source: dict) -> dict:
    lab=label.upper(); sc=SCANNER_CACHE.get(lab,{}) or {}; yq=YAHOO_QUOTE_CACHE.get(lab,{}) or {}; ys={}
    if os.environ.get("APEX_PROFILE_DETAILED_YAHOO", "1") != "0":
        ys=fetch_yahoo_summary(lab)
    inst = str(sc.get("type") or yq.get("quoteType") or "").lower()
    lname = (name or lab).lower()
    if any(k in lname for k in ["etf", "ucits", "fund"]) or lab in {"SPY","IBIT","ISVAF","IUVL","VGT","SMH","SMH_EPA"}:
        instrument="ETF"
    elif "index" in lname or lab in {"CAC40","VXN","DJI","FTSE","HSI","IXIC","NYA","NYFANG","SPX","VIX","GSOX","GSOXNR","GSOXTR","SEMIEW5T"}:
        instrument="Index"
    elif inst:
        instrument="Equity" if "stock" in inst or "fund" not in inst else inst.title()
    else:
        instrument="Equity"
    last=float(df["Close"].iloc[-1]) if df is not None and len(df) else _safe_num(sc.get("close"), _safe_num(yq.get("regularMarketPrice"), 0))
    vols=[_safe_num(x) for x in (df["Volume"].tail(60).tolist() if df is not None and "Volume" in df else [])]
    vols=[v for v in vols if v is not None and v>0]
    avg_vol_60=sum(vols)/len(vols) if vols else _safe_num(sc.get("average_volume_60d_calc"), _safe_num(yq.get("averageDailyVolume3Month")))
    adtv=_safe_num(sc.get("Value.Traded"))
    if adtv is None and last and avg_vol_60:
        adtv=last*avg_vol_60
    currency=str(sc.get("currency") or yq.get("currency") or _qraw(ys,"price","currency") or "USD")
    desc=str(sc.get("description") or _qraw(ys,"assetProfile","longBusinessSummary") or _qraw(ys,"fundProfile","categoryName") or f"{name} is analysed with TradingView overview/screener, Superchart OHLCV, public fundamentals, news sentiment and local quantitative validation.")
    sector=str(sc.get("sector") or yq.get("sector") or _qraw(ys,"assetProfile","sector") or ("ETF" if instrument=="ETF" else "Market index" if instrument=="Index" else "Public company"))
    industry=str(sc.get("industry") or yq.get("industry") or _qraw(ys,"assetProfile","industry") or ("Diversified ETF" if instrument=="ETF" else "Benchmark/index" if instrument=="Index" else sector))
    def large_tv(tv_key, *ypaths):
        v=_safe_num(sc.get(tv_key))
        if v is None:
            for path in ypaths:
                v=_safe_num(_qraw(ys,*path))
                if v is not None: break
        return _fmt_large(v, currency)
    def ratio_tv(tv_key, *ypaths):
        v=_safe_num(sc.get(tv_key))
        if v is None:
            for path in ypaths:
                v=_safe_num(_qraw(ys,*path))
                if v is not None: break
        return _fmt_ratio(v)
    def pct_tv(tv_key, *ypaths):
        v=_safe_num(sc.get(tv_key))
        if v is None:
            for path in ypaths:
                v=_safe_num(_qraw(ys,*path))
                if v is not None: break
        return _fmt_pct(v)
    # Fallback for items that genuinely do not have corporate fundamentals.
    fund_na = "N/A for ETF/index" if instrument in {"ETF","Index"} else "Field not published by current public feeds"
    market_cap=_fmt_large(_safe_num(sc.get("market_cap_basic"), _safe_num(yq.get("marketCap"), _safe_num(_qraw(ys,"price","marketCap")))), currency)
    if not market_cap and adtv:
        market_cap=_fmt_large(adtv, currency)+" liquidity scale"
    profile={
        "description": desc,
        "instrument": instrument,
        "sector": sector,
        "industry": industry,
        "exchange": str(sc.get("exchange") or source.get("exchange") or yq.get("fullExchangeName") or yq.get("exchange") or "Chart exchange"),
        "market_cap": market_cap or fund_na,
        "adtv": (_fmt_large(adtv, currency)+" ADTV") if adtv else fund_na,
        "revenue": large_tv("total_revenue", ("financialData","totalRevenue")) or fund_na,
        "net_income": large_tv("net_income", ("defaultKeyStatistics","netIncomeToCommon")) or fund_na,
        "profit_margin": pct_tv("net_margin_ttm", ("financialData","profitMargins")) or fund_na,
        "gross_margin": pct_tv("gross_margin_ttm", ("financialData","grossMargins")) or fund_na,
        "operating_margin": pct_tv("operating_margin_ttm", ("financialData","operatingMargins")) or fund_na,
        "pe_ratio": ratio_tv("price_earnings_ttm", ("summaryDetail","trailingPE"), ("defaultKeyStatistics","trailingPE")) or ("N/A for ETF/index" if instrument in {"ETF","Index"} else fund_na),
        "price_sales": ratio_tv("price_sales_current", ("summaryDetail","priceToSalesTrailing12Months")) or ("N/A for ETF/index" if instrument in {"ETF","Index"} else fund_na),
        "price_book": ratio_tv("price_book_fq", ("defaultKeyStatistics","priceToBook")) or fund_na,
        "dividend_yield": pct_tv("dividend_yield_recent", ("summaryDetail","dividendYield")) or "0.00% or not distributed",
        "beta": ratio_tv("beta_1_year", ("summaryDetail","beta"), ("defaultKeyStatistics","beta")) or ("N/A for ETF/index" if instrument in {"ETF","Index"} else fund_na),
        "employees": _fmt_number(_safe_num(sc.get("number_of_employees"), _safe_num(_qraw(ys,"assetProfile","fullTimeEmployees")))) or ("N/A for ETF/index" if instrument in {"ETF","Index"} else fund_na),
        "country": str(sc.get("country") or _qraw(ys,"assetProfile","country") or yq.get("market") or "Global"),
        "currency": currency,
        "tradingview_rating": _tv_rating(sc.get("Recommend.All")) or "Neutral (local model)",
        "technical_rating": _tv_rating(sc.get("Recommend.MA")) or "Neutral (local MA)",
        "rsi": _fmt_number(sc.get("RSI")) or "50.0",
        "perf_3m": _fmt_pct(sc.get("Perf.3M")) or "0.00%",
        "perf_6m": _fmt_pct(sc.get("Perf.6M")) or "0.00%",
        "perf_1y": _fmt_pct(sc.get("Perf.Y")) or "0.00%",
        "data_source": "TradingView overview/screener + Superchart OHLCV + Yahoo fundamentals",
    }
    # Never return blanks: the UI must always have a value without destroying other fields.
    for k,v in list(profile.items()):
        if v is None or str(v).strip()=="":
            profile[k]=fund_na if k not in {"description","instrument","sector","industry","exchange","country","currency","data_source"} else "Public profile field"
    PROFILE_CACHE[lab]=profile
    return profile


def fetch_news_sentiment(label: str, name: str, limit: int = 8, days: int = 7) -> dict:
    out = {"score": 0.0, "count": 0, "headlines": [], "error": None}
    if requests is None:
        out["error"] = "requests unavailable"
        return out
    queries = [
        f'"{name}" OR {label} stock market when:{days}d',
        f'"{name}" earnings price target analyst stock when:{days}d',
    ]
    headers = {"User-Agent": "Mozilla/5.0 (compatible; ApexPredictor/14.6)"}
    seen=set(); scores=[]
    for q in queries:
        if len(out["headlines"]) >= limit:
            break
        url = f"https://news.google.com/rss/search?q={quote_plus(q)}&hl=en-US&gl=US&ceid=US:en"
        try:
            r = requests.get(url, timeout=float(os.environ.get("APEX_NEWS_TIMEOUT", "3.5")), headers=headers)
            r.raise_for_status()
            root = ET.fromstring(r.text)
            for item in root.findall(".//item"):
                if len(out["headlines"]) >= limit:
                    break
                title = _clean_text(item.findtext("title", ""))
                link = _clean_text(item.findtext("link", ""))
                source = _clean_text(item.findtext("source", "")) or "Google News"
                pub = _clean_text(item.findtext("pubDate", ""))
                key=(title.lower(), source.lower())
                if not title or key in seen:
                    continue
                seen.add(key)
                score = _headline_score(title)
                scores.append(score)
                out["headlines"].append({"title": title, "source": source, "published": pub, "link": link, "url": link, "score": round(float(score), 3), "summary": title})
        except Exception as exc:
            out["error"] = str(exc)
            continue
    if scores:
        out["score"] = float(np.clip(np.mean(scores), -1.0, 1.0))
        out["count"] = len(scores)
    return out


def probability_up_from_inputs(final_pct: float, vol_ann: float, confidence: float, news_score: float, tv_rating_text: str = "") -> float:
    # Stable 0..1 probability proxy from predicted edge, realised volatility, confidence and public catalysts.
    vol_scale = max(1.8, min(12.0, vol_ann / 6.0))
    z = (final_pct / vol_scale) + (float(news_score or 0) * 0.22) + ((confidence - 50.0) / 100.0)
    txt = (tv_rating_text or "").lower()
    if "strong buy" in txt: z += 0.22
    elif "buy" in txt: z += 0.12
    elif "strong sell" in txt: z -= 0.22
    elif "sell" in txt: z -= 0.12
    p = 1.0 / (1.0 + math.exp(-z))
    return float(np.clip(p, 0.01, 0.99))


def _pct_change(a: float, b: float) -> float:
    return ((b / a) - 1.0) * 100.0 if a and a > 0 else 0.0


def _safe_float(x, default: float = 0.0) -> float:
    try:
        if isinstance(x, dict):
            x = x.get("raw", x.get("value", x.get("fmt", x.get("longFmt"))))
        if x in (None, ""):
            return default
        v = float(x)
        return v if math.isfinite(v) else default
    except Exception:
        return default


def _safe_mean(values: list[float] | np.ndarray, default: float = 0.0) -> float:
    arr = np.asarray(values, dtype=float)
    arr = arr[np.isfinite(arr)]
    return float(arr.mean()) if len(arr) else default


def _safe_std(values: list[float] | np.ndarray, default: float = 0.0) -> float:
    arr = np.asarray(values, dtype=float)
    arr = arr[np.isfinite(arr)]
    return float(arr.std(ddof=1)) if len(arr) > 1 else default


def _sigmoid(x: float | np.ndarray) -> float | np.ndarray:
    return 1.0 / (1.0 + np.exp(-np.asarray(x)))


def _hash_df_snapshot(df: pd.DataFrame, label: str) -> str:
    """Small reproducibility hash for the exact OHLCV snapshot used."""
    try:
        cols = [c for c in ["Open", "High", "Low", "Close", "Volume"] if c in df.columns]
        sample = df[cols].tail(260).round(8).to_csv(index=True)
        return hashlib.sha256((label.upper() + "\n" + sample).encode("utf-8")).hexdigest()[:16]
    except Exception:
        return "unavailable"


def universe_group(label: str, name: str = "", profile: dict | None = None) -> str:
    lab = str(label or "").upper()
    text = f"{lab} {name} {(profile or {}).get('sector','')} {(profile or {}).get('industry','')}".lower()
    if lab in {"SPY", "IBIT", "ISVAF", "IUVL", "VGT", "SMH", "SMH_EPA"} or "etf" in text or "ucits" in text:
        return "ETF"
    if lab in {"CAC40", "DJI", "FTSE", "HSI", "IXIC", "NYA", "NYFANG", "SPX", "VIX", "VXN", "GSOX", "GSOXNR", "GSOXTR", "SEMIEW5T"} or "index" in text:
        return "Index"
    if any(x in text for x in ["semiconductor", "chip", "nvidia", "asml", "broadcom", "micron", "qualcomm", "tsmc", "intel", "amd"]):
        return "Semiconductors"
    if any(x in text for x in ["bank", "financial", "capital markets", "insurance", "asset management", "blackrock", "goldman", "morgan stanley"]):
        return "Financials"
    if any(x in text for x in ["software", "internet", "technology", "cloud", "digital", "computer"]):
        return "Technology"
    if any(x in text for x in ["health", "pharma", "biotech", "drug"]):
        return "Healthcare"
    return "Equity"


def data_quality_report(label: str, df: pd.DataFrame, source: dict) -> dict:
    """Enterprise-facing checks: missing prices, gaps, volume anomalies and traceability."""
    out = {
        "status": "pass",
        "score": 100.0,
        "issues": [],
        "rows": int(len(df) if df is not None else 0),
        "snapshot_hash": "unavailable",
        "source": {
            "provider": source.get("provider", "multi-source historical fallback") if isinstance(source, dict) else "multi-source historical fallback",
            "symbol": source.get("symbol") if isinstance(source, dict) else None,
            "exchange": source.get("exchange") if isinstance(source, dict) else None,
            "attempts": source.get("errors", []) if isinstance(source, dict) else [],
            "delayed": bool(source.get("delayed", False)) if isinstance(source, dict) else False,
            "commercial_license_required": bool(source.get("commercial_license_required", True)) if isinstance(source, dict) else True,
        },
        "checks": {},
    }
    if df is None or df.empty:
        out["status"] = "fail"
        out["score"] = 0.0
        out["issues"].append("No usable OHLCV rows.")
        return out
    clean = normalize_history(df)
    out["rows"] = int(len(clean))
    out["snapshot_hash"] = _hash_df_snapshot(clean, label)
    needed = ["Open", "High", "Low", "Close"]
    missing_cells = int(clean[needed].isna().sum().sum())
    missing_pct = float(missing_cells / max(1, len(clean) * len(needed)) * 100.0)
    zero_volume_pct = float((clean["Volume"].fillna(0) <= 0).mean() * 100.0) if "Volume" in clean else 100.0
    daily_ret = clean["Close"].pct_change().replace([np.inf, -np.inf], np.nan)
    large_moves = int((daily_ret.abs() > 0.35).sum())
    gaps = pd.Series(clean.index).diff().dt.days.dropna()
    max_gap = int(gaps.max()) if len(gaps) else 0
    stale_days = int((datetime.utcnow() - clean.index[-1].to_pydatetime()).days) if len(clean) else 999
    checks = {
        "missing_ohlc_pct": round(missing_pct, 3),
        "zero_volume_pct": round(zero_volume_pct, 3),
        "large_daily_move_count": large_moves,
        "max_calendar_gap_days": max_gap,
        "stale_calendar_days": stale_days,
        "first_date": clean.index[0].strftime("%Y-%m-%d"),
        "last_date": clean.index[-1].strftime("%Y-%m-%d"),
    }
    out["checks"] = checks
    penalties = 0.0
    if len(clean) < 250:
        out["issues"].append("Less than 250 rows: limited statistical evidence.")
        penalties += 18
    if missing_pct > QUALITY_MAX_MISSING_PCT:
        out["issues"].append("Missing OHLC values above enterprise threshold.")
        penalties += 22
    if max_gap > QUALITY_MAX_GAP_DAYS:
        out["issues"].append("Large calendar gap detected in OHLCV history.")
        penalties += 12
    if stale_days > 7:
        out["issues"].append("Latest close appears stale.")
        penalties += 18
    if large_moves:
        out["issues"].append("Potential split/outlier event detected; verify adjusted data.")
        penalties += min(20, large_moves * 5)
    if zero_volume_pct > 20:
        out["issues"].append("High zero-volume ratio; liquidity or index feed may distort metrics.")
        penalties += 8
    out["score"] = round(float(np.clip(100.0 - penalties, 0, 100)), 2)
    if out["score"] < 60:
        out["status"] = "fail"
    elif out["score"] < 82 or out["issues"]:
        out["status"] = "review"
    return out


def setup_score_series(feat_df: pd.DataFrame) -> pd.Series:
    """Fixed, explainable setup score in [-1, 1] using only information known at each date."""
    f = feat_df.copy()
    idx = f.index
    def col(name: str, default: float = 0.0) -> pd.Series:
        if name in f:
            return pd.to_numeric(f[name], errors="coerce").fillna(default)
        return pd.Series(default, index=idx)
    ret20 = np.tanh(col("Ret20") / 0.075)
    ret60 = np.tanh(col("Ret60") / 0.16)
    trend = np.tanh(col("DistSMA200") / 0.18)
    ema = np.tanh(col("DistEMA20") / 0.08)
    rsi = col("RSI", 50.0)
    mean_reversion = np.where(rsi < 32, (42 - rsi) / 35.0, np.where(rsi > 68, -(rsi - 58) / 35.0, 0.0))
    volatility = col("Vol20", 0.012)
    vol_penalty = np.clip((volatility - volatility.rolling(120, min_periods=20).median().fillna(volatility.median())) / 0.03, -0.4, 0.6)
    volume_confirm = np.tanh((col("VolumeRatio", 1.0) - 1.0) / 1.8) * 0.08
    raw = 0.30 * ret20 + 0.23 * ret60 + 0.20 * trend + 0.12 * ema + 0.13 * mean_reversion + volume_confirm - 0.10 * vol_penalty
    return pd.Series(np.clip(raw, -1.0, 1.0), index=idx).replace([np.inf, -np.inf], np.nan).fillna(0.0)


def current_setup_drivers(feat_df: pd.DataFrame, news_score: float, profile: dict) -> list[str]:
    if feat_df is None or feat_df.empty:
        return ["Insufficient feature history for a defensible proof card."]
    f = feat_df.iloc[-1]
    drivers: list[str] = []
    ret20 = _safe_float(f.get("Ret20")) * 100
    ret60 = _safe_float(f.get("Ret60")) * 100
    rsi = _safe_float(f.get("RSI"), 50.0)
    vol_ann = recent_vol(feat_df) * math.sqrt(252) * 100
    dist200 = _safe_float(f.get("DistSMA200")) * 100
    drivers.append(f"20d momentum {ret20:+.2f}% and 60d momentum {ret60:+.2f}%.")
    drivers.append(f"Price is {dist200:+.2f}% from SMA200; RSI is {rsi:.1f}.")
    drivers.append(f"Realised volatility regime is {vol_ann:.1f}% annualised.")
    drivers.append(f"News sentiment input is {float(news_score or 0):+.2f}.")
    drivers.append(f"Public technical context: {profile.get('tradingview_rating', 'not available')} / {profile.get('technical_rating', 'not available')}.")
    return drivers


def _max_drawdown_pct(returns_pct: np.ndarray) -> float:
    if len(returns_pct) == 0:
        return 0.0
    curve = np.cumprod(1.0 + np.asarray(returns_pct, dtype=float) / 100.0)
    peak = np.maximum.accumulate(curve)
    dd = curve / np.where(peak == 0, 1, peak) - 1.0
    return float(dd.min() * 100.0)


def _profit_factor(returns_pct: np.ndarray) -> float:
    gains = np.asarray(returns_pct, dtype=float)
    pos = gains[gains > 0].sum()
    neg = abs(gains[gains < 0].sum())
    if neg <= 1e-12:
        return float("inf") if pos > 0 else 0.0
    return float(pos / neg)


def strategy_metrics(score: pd.Series, future_ret_pct: pd.Series, horizon: int, cost_bps: float) -> dict:
    work = pd.DataFrame({"score": score, "future": future_ret_pct}).replace([np.inf, -np.inf], np.nan).dropna()
    if len(work) > MAX_BACKTEST_SAMPLES:
        work = work.tail(MAX_BACKTEST_SAMPLES)
    if len(work) < 30:
        return {"samples": int(len(work)), "status": "insufficient"}
    position = np.where(work["score"].abs() >= 0.18, np.sign(work["score"]), 0.0)
    turnover = np.abs(pd.Series(position, index=work.index).diff().fillna(0.0)).to_numpy()
    costs = turnover * (cost_bps / 100.0)
    strat_returns = position * work["future"].to_numpy(dtype=float) - costs
    mean_ret = _safe_mean(strat_returns)
    std_ret = _safe_std(strat_returns)
    periods = max(1.0, 252.0 / max(1, horizon))
    sharpe = 0.0 if std_ret <= 1e-12 else float((mean_ret / std_ret) * math.sqrt(periods))
    direction_ok = np.sign(work["score"].to_numpy(dtype=float)) == np.sign(work["future"].to_numpy(dtype=float))
    active = np.abs(position) > 0
    return {
        "samples": int(len(work)),
        "status": "ok",
        "avg_return_pct": round(mean_ret, 4),
        "median_return_pct": round(float(np.median(strat_returns)), 4),
        "hit_rate": round(float(np.mean(strat_returns > 0)), 4),
        "directional_accuracy": round(float(np.mean(direction_ok[active])) if np.any(active) else 0.0, 4),
        "mae_pct": round(float(np.mean(np.abs(work["future"].to_numpy(dtype=float) - work["score"].to_numpy(dtype=float) * std_ret))), 4),
        "sharpe": round(sharpe, 4),
        "max_drawdown_pct": round(_max_drawdown_pct(strat_returns), 4),
        "profit_factor": round(_profit_factor(strat_returns), 4) if np.isfinite(_profit_factor(strat_returns)) else "inf",
        "turnover": round(float(np.mean(turnover)), 4),
        "active_rate": round(float(np.mean(active)), 4),
        "transaction_cost_bps": float(cost_bps),
    }


def calibration_report(score: pd.Series, future_ret_pct: pd.Series) -> dict:
    work = pd.DataFrame({"score": score, "future": future_ret_pct}).replace([np.inf, -np.inf], np.nan).dropna()
    if len(work) > MAX_BACKTEST_SAMPLES:
        work = work.tail(MAX_BACKTEST_SAMPLES)
    if len(work) < 30:
        return {"samples": int(len(work)), "status": "insufficient", "brier_score": None, "confidence_decay": 0.0, "bins": []}
    prob = pd.Series(_sigmoid(work["score"].to_numpy(dtype=float) * 2.2), index=work.index)
    actual = (work["future"] > 0).astype(float)
    brier = float(np.mean((prob - actual) ** 2))
    bins = []
    edges = np.linspace(0.0, 1.0, 11)
    for lo, hi in zip(edges[:-1], edges[1:]):
        mask = (prob >= lo) & (prob < hi if hi < 1 else prob <= hi)
        if not bool(mask.any()):
            continue
        bins.append({
            "range": f"{lo:.1f}-{hi:.1f}",
            "count": int(mask.sum()),
            "mean_predicted_probability": round(float(prob[mask].mean()), 4),
            "observed_up_rate": round(float(actual[mask].mean()), 4),
        })
    high_mask = (prob >= 0.65) & (prob < 0.75)
    return {
        "samples": int(len(work)),
        "status": "ok",
        "brier_score": round(brier, 5),
        "confidence_decay": round(float(np.clip(math.sqrt(len(work) / 180.0), 0.0, 1.0)), 4),
        "bins": bins,
        "observed_up_when_65_75": round(float(actual[high_mask].mean()), 4) if bool(high_mask.any()) else None,
    }


def walk_forward_alpha_evidence(df: pd.DataFrame, requested_days: int, cost_bps: float = DEFAULT_TRANSACTION_COST_BPS) -> dict:
    """Strict historical evidence: signal at t is compared with realised t+h returns."""
    feat_df, _features = build_features(df)
    feat_df = feat_df.dropna(subset=["Close"]).copy()
    if len(feat_df) < 160:
        return {"status": "insufficient", "reason": "Need at least 160 rows for alpha evidence.", "horizons": {}, "model_leaderboard": []}
    score = setup_score_series(feat_df)
    close = feat_df["Close"].astype(float)
    horizons: dict[str, Any] = {}
    leaderboard = []
    for horizon in ALPHA_HORIZONS:
        future = (close.shift(-horizon) / close - 1.0) * 100.0
        momentum20 = np.sign(pd.to_numeric(feat_df.get("Ret20"), errors="coerce").fillna(0.0))
        momentum60 = np.sign(pd.to_numeric(feat_df.get("Ret60"), errors="coerce").fillna(0.0))
        mean_rev = -np.sign(pd.to_numeric(feat_df.get("DistEMA20"), errors="coerce").fillna(0.0))
        buy_hold = pd.Series(1.0, index=feat_df.index)
        setup_stats = strategy_metrics(score, future, horizon, cost_bps)
        baselines = {
            "buy_hold": strategy_metrics(buy_hold, future, horizon, cost_bps=0.0),
            "momentum_20d": strategy_metrics(pd.Series(momentum20, index=feat_df.index), future, horizon, cost_bps),
            "momentum_60d": strategy_metrics(pd.Series(momentum60, index=feat_df.index), future, horizon, cost_bps),
            "mean_reversion": strategy_metrics(pd.Series(mean_rev, index=feat_df.index), future, horizon, cost_bps),
        }
        cal = calibration_report(score, future)
        valid_baselines = {k: v for k, v in baselines.items() if v.get("status") == "ok"}
        best_base = max(valid_baselines.items(), key=lambda kv: _safe_float(kv[1].get("sharpe"))) if valid_baselines else ("none", {})
        edge = _safe_float(setup_stats.get("avg_return_pct")) - _safe_float(best_base[1].get("avg_return_pct"))
        horizons[str(horizon)] = {
            "method": "walk_forward_no_future_leakage_fixed_setup_score",
            "setup_stats": setup_stats,
            "benchmarks": baselines,
            "best_baseline": best_base[0],
            "edge_vs_best_baseline_pct": round(edge, 4),
            "calibration": cal,
        }
        if setup_stats.get("status") == "ok":
            leaderboard.append({
                "horizon": horizon,
                "model": "setup_stats_score",
                "sharpe": setup_stats.get("sharpe"),
                "avg_return_pct": setup_stats.get("avg_return_pct"),
                "directional_accuracy": setup_stats.get("directional_accuracy"),
                "samples": setup_stats.get("samples"),
                "edge_vs_best_baseline_pct": round(edge, 4),
            })
    leaderboard.sort(key=lambda x: (_safe_float(x.get("sharpe")), _safe_float(x.get("edge_vs_best_baseline_pct"))), reverse=True)
    nearest = min(ALPHA_HORIZONS, key=lambda h: abs(h - int(requested_days or DEFAULT_DAYS)))
    selected = horizons.get(str(nearest), {})
    return {
        "status": "ok" if selected else "insufficient",
        "selected_horizon": nearest,
        "horizons": horizons,
        "model_leaderboard": leaderboard[:8],
        "evidence_summary": {
            "samples": selected.get("setup_stats", {}).get("samples"),
            "selected_sharpe": selected.get("setup_stats", {}).get("sharpe"),
            "selected_directional_accuracy": selected.get("setup_stats", {}).get("directional_accuracy"),
            "selected_hit_rate": selected.get("setup_stats", {}).get("hit_rate"),
            "selected_brier_score": selected.get("calibration", {}).get("brier_score"),
            "edge_vs_best_baseline_pct": selected.get("edge_vs_best_baseline_pct"),
        },
    }


def regime_from_features(feat_df: pd.DataFrame) -> dict:
    if feat_df is None or feat_df.empty:
        return {"name": "unknown", "volatility": "unknown", "trend": "unknown"}
    vol_ann = recent_vol(feat_df) * math.sqrt(252) * 100.0
    last = feat_df.iloc[-1]
    trend_val = _safe_float(last.get("DistSMA200")) * 100.0
    mom20 = _safe_float(last.get("Ret20")) * 100.0
    if vol_ann >= 45:
        vol_name = "high volatility"
    elif vol_ann >= 24:
        vol_name = "normal volatility"
    else:
        vol_name = "low volatility"
    if trend_val > 5 and mom20 > 0:
        trend_name = "uptrend"
    elif trend_val < -5 and mom20 < 0:
        trend_name = "downtrend"
    else:
        trend_name = "mixed trend"
    return {"name": f"{trend_name}, {vol_name}", "volatility": vol_name, "trend": trend_name, "vol_ann_pct": round(vol_ann, 2)}


def decision_block(
    label: str,
    name: str,
    feat_df: pd.DataFrame,
    forecast: list[dict],
    final_pct: float,
    confidence: float,
    risk: str,
    news_score: float,
    profile: dict,
    alpha_evidence: dict,
    data_quality: dict,
) -> dict:
    regime = regime_from_features(feat_df)
    vol_ann = _safe_float(regime.get("vol_ann_pct"), recent_vol(feat_df) * math.sqrt(252) * 100)
    selected = alpha_evidence.get("horizons", {}).get(str(alpha_evidence.get("selected_horizon")), {})
    cal = selected.get("calibration", {}) or {}
    stats = selected.get("setup_stats", {}) or {}
    decay = _safe_float(cal.get("confidence_decay"), 0.0)
    brier = _safe_float(cal.get("brier_score"), 0.25)
    alpha_quality = max(0.0, 1.0 - min(1.0, brier / 0.35)) * decay
    quality_factor = _safe_float(data_quality.get("score"), 0.0) / 100.0
    calibrated_confidence = float(np.clip((confidence * 0.45) + (alpha_quality * 45.0) + (quality_factor * 18.0), 5.0, 95.0))
    signal_strength = float(np.clip((abs(final_pct) / max(1.0, vol_ann / 8.0)) * 50.0 + calibrated_confidence * 0.35, 0, 100))
    if forecast:
        low = min(_safe_float(x.get("low")) for x in forecast)
        high = max(_safe_float(x.get("high")) for x in forecast)
        last_price = _safe_float(forecast[0].get("price")) / (1 + _safe_float(forecast[0].get("pct")) / 100.0)
        downside = _pct_change(last_price, low) if last_price > 0 else 0.0
        ci = {"low_price": round(low, 6), "high_price": round(high, 6)}
    else:
        downside = 0.0
        ci = {"low_price": None, "high_price": None}
    if data_quality.get("status") == "fail":
        action = "investigate"
    elif final_pct > 0.65 and calibrated_confidence >= 58 and risk != "High":
        action = "rebalance candidate"
    elif final_pct > 0.1 and calibrated_confidence >= 45:
        action = "watch"
    elif final_pct < -0.35 or risk == "High":
        action = "avoid / risk-off candidate"
    else:
        action = "investigate"
    if risk == "High" or vol_ann > 45 or calibrated_confidence < 42:
        sizing = "low risk budget"
    elif risk == "Moderate" or calibrated_confidence < 62:
        sizing = "medium risk budget"
    else:
        sizing = "higher risk budget compatible"
    why_now = current_setup_drivers(feat_df, news_score, profile)
    why_now.append(f"Walk-forward evidence horizon {alpha_evidence.get('selected_horizon')}d: Sharpe {stats.get('sharpe', 'n/a')}, hit rate {stats.get('hit_rate', 'n/a')}.")
    if data_quality.get("issues"):
        why_now.append("Data quality caveat: " + "; ".join(data_quality.get("issues", [])[:2]))
    return {
        "expected_return_pct": round(float(final_pct), 4),
        "expected_volatility_ann_pct": round(vol_ann, 2),
        "downside_risk_pct": round(float(downside), 4),
        "confidence_interval": ci,
        "signal_strength": round(signal_strength, 2),
        "calibrated_confidence": round(calibrated_confidence, 2),
        "regime": regime,
        "recommended_action": action,
        "position_sizing_hint": sizing,
        "why_now": why_now,
        "decision_basis": {
            "original_confidence": round(float(confidence), 2),
            "alpha_quality": round(alpha_quality, 4),
            "data_quality_factor": round(quality_factor, 4),
            "brier_score": cal.get("brier_score"),
            "sample_count": cal.get("samples"),
        },
    }


def signal_proof_card(label: str, name: str, decision: dict, alpha_evidence: dict, data_quality: dict, model_meta: dict) -> dict:
    selected = alpha_evidence.get("horizons", {}).get(str(alpha_evidence.get("selected_horizon")), {})
    stats = selected.get("setup_stats", {}) or {}
    best = selected.get("best_baseline")
    edge = selected.get("edge_vs_best_baseline_pct")
    summary = (
        f"{label} is a {decision.get('recommended_action')} because expected return is "
        f"{decision.get('expected_return_pct')}%, calibrated confidence is "
        f"{decision.get('calibrated_confidence')}/100, regime is "
        f"{(decision.get('regime') or {}).get('name')}, and selected walk-forward evidence "
        f"shows Sharpe {stats.get('sharpe', 'n/a')} with edge {edge} pct vs {best}."
    )
    return {
        "label": label,
        "name": name,
        "summary": summary,
        "proof_points": [
            f"Current action: {decision.get('recommended_action')}.",
            f"Signal strength: {decision.get('signal_strength')}/100.",
            f"Walk-forward hit rate: {stats.get('hit_rate', 'n/a')}.",
            f"Directional accuracy: {stats.get('directional_accuracy', 'n/a')}.",
            f"Max drawdown in evidence window: {stats.get('max_drawdown_pct', 'n/a')}%.",
            f"Data quality: {data_quality.get('status')} ({data_quality.get('score')}/100).",
        ],
        "model_trace": {
            "engine": ENGINE_NAME,
            "version": ENGINE_VERSION,
            "feature_version": FEATURE_VERSION,
            "model": model_meta.get("model"),
            "optional_model_capabilities": {
                "lightgbm_available": bool(lgb is not None),
                "xgboost_available": bool(xgb is not None),
                "sklearn_gradient_boosting_available": bool(GradientBoostingRegressor is not None),
            },
        },
        "data_snapshot_hash": data_quality.get("snapshot_hash"),
    }


def attach_cross_sectional_context(results: list[dict]) -> None:
    clean = [r for r in results if not r.get("error")]
    if not clean:
        return
    spy = next((r for r in clean if r.get("label") == "SPY"), None)
    nasdaq = next((r for r in clean if r.get("label") in {"IXIC", "QQQ"}), None)
    smh = next((r for r in clean if r.get("label") == "SMH"), None)
    strengths = []
    for r in clean:
        dec = r.get("professional_decision") or {}
        strength = _safe_float(dec.get("signal_strength")) * (1 if _safe_float(r.get("change_horizon_pct")) >= 0 else -1)
        strengths.append(strength)
    ranks = pd.Series(strengths).rank(pct=True).to_list()
    for r, pct_rank in zip(clean, ranks):
        group = universe_group(r.get("label"), r.get("name"), r.get("profile") or {})
        rel = {}
        if spy:
            rel["vs_spy_pct"] = round(_safe_float(r.get("change_horizon_pct")) - _safe_float(spy.get("change_horizon_pct")), 4)
        if nasdaq:
            rel["vs_nasdaq_pct"] = round(_safe_float(r.get("change_horizon_pct")) - _safe_float(nasdaq.get("change_horizon_pct")), 4)
        if group == "Semiconductors" and smh and r.get("label") != "SMH":
            rel["vs_sector_proxy_smh_pct"] = round(_safe_float(r.get("change_horizon_pct")) - _safe_float(smh.get("change_horizon_pct")), 4)
        r["cross_sectional_context"] = {
            "universe_group": group,
            "setup_percentile": round(float(pct_rank), 4),
            "relative_strength": rel,
        }


def portfolio_ranking(results: list[dict], transaction_cost_bps: float = DEFAULT_TRANSACTION_COST_BPS) -> dict:
    clean = [r for r in results if not r.get("error")]
    rows = []
    for r in clean:
        dec = r.get("professional_decision") or {}
        alpha = r.get("alpha_evidence", {}).get("evidence_summary", {}) or {}
        data_q = r.get("data_quality", {}) or {}
        score = (
            _safe_float(dec.get("signal_strength")) * 0.42
            + _safe_float(dec.get("calibrated_confidence")) * 0.28
            + max(0.0, _safe_float(alpha.get("edge_vs_best_baseline_pct"))) * 8.0
            + _safe_float(data_q.get("score")) * 0.12
        )
        if _safe_float(dec.get("expected_return_pct")) < 0:
            score *= -0.65
        rows.append({
            "label": r.get("label"),
            "name": r.get("name"),
            "score": round(float(score), 3),
            "expected_return_pct": dec.get("expected_return_pct"),
            "calibrated_confidence": dec.get("calibrated_confidence"),
            "risk": r.get("risk"),
            "action": dec.get("recommended_action"),
            "group": (r.get("cross_sectional_context") or {}).get("universe_group") or universe_group(r.get("label"), r.get("name"), r.get("profile") or {}),
            "signal_strength": dec.get("signal_strength"),
        })
    top_long = sorted([x for x in rows if _safe_float(x.get("score")) > 0], key=lambda x: _safe_float(x.get("score")), reverse=True)[:12]
    risk_off = sorted([x for x in rows if _safe_float(x.get("expected_return_pct")) < 0 or x.get("action", "").startswith("avoid")], key=lambda x: _safe_float(x.get("score")))[:12]
    vol_alerts = sorted(
        [
            {
                "label": r.get("label"),
                "name": r.get("name"),
                "volatility_ann_pct": (r.get("professional_decision") or {}).get("expected_volatility_ann_pct"),
                "risk": r.get("risk"),
                "data_quality": (r.get("data_quality") or {}).get("status"),
            }
            for r in clean
        ],
        key=lambda x: _safe_float(x.get("volatility_ann_pct")),
        reverse=True,
    )[:12]
    group_scores: dict[str, list[float]] = {}
    for row in rows:
        group_scores.setdefault(str(row.get("group") or "Other"), []).append(_safe_float(row.get("score")))
    sector_rotation = [
        {"group": k, "avg_score": round(_safe_mean(v), 3), "count": len(v)}
        for k, v in sorted(group_scores.items(), key=lambda kv: _safe_mean(kv[1]), reverse=True)
    ]
    allocations = []
    total_positive = sum(max(0.0, _safe_float(x.get("score"))) for x in top_long) or 1.0
    group_alloc: dict[str, float] = {}
    for row in top_long:
        raw = max(0.0, _safe_float(row.get("score"))) / total_positive
        cap = 0.22
        group = str(row.get("group") or "Other")
        group_room = max(0.0, 0.35 - group_alloc.get(group, 0.0))
        weight = min(raw, cap, group_room)
        group_alloc[group] = group_alloc.get(group, 0.0) + weight
        allocations.append({"label": row["label"], "weight_pct": round(weight * 100, 2), "group": group, "reason": row.get("action")})
    return {
        "method": "risk-adjusted cross-sectional setup ranking, not personalised investment advice",
        "constraints": {"max_single_name_weight_pct": 22, "max_group_weight_pct": 35, "costs_in_backtests_bps": float(transaction_cost_bps)},
        "top_long_candidates": top_long,
        "risk_off_candidates": risk_off,
        "top_volatility_alerts": vol_alerts,
        "sector_rotation_score": sector_rotation,
        "paper_portfolio": {"allocations": allocations, "cash_weight_pct": round(max(0.0, 100.0 - sum(a["weight_pct"] for a in allocations)), 2)},
    }



def forecast_one(
    label: str,
    days: int = DEFAULT_DAYS,
    n_bars: int = DEFAULT_BARS,
    use_news: bool = True,
    news_limit: int = 8,
    transaction_cost_bps: float = DEFAULT_TRANSACTION_COST_BPS,
) -> dict:
    item = info_from_label(label)
    if item is None:
        return {"label": label, "error": "unknown ticker"}
    label, original_sym, original_exch, name = item
    minimum_rows = RECENT_LISTING_MIN_ROWS if label in RECENT_LISTING_SHORT_HISTORY else MIN_ROWS_FOR_FORECAST
    df, source = fetch_history_by_label(label, n_bars=n_bars, min_rows=minimum_rows)
    if df is None or len(df) < minimum_rows:
        return {"label": label, "name": name, "error": "no reliable multi-source history", "source_attempts": source.get("errors", [])}
    short_history = len(df) < MIN_ROWS_FOR_FORECAST
    feat_df, _ = build_features(df)
    feat_df = feat_df.dropna(subset=["Close"])
    if len(feat_df) < minimum_rows:
        return {"label": label, "name": name, "error": "too few usable rows after feature engineering", "rows": len(feat_df)}
    last = float(feat_df["Close"].iloc[-1])
    prev_close = float(feat_df["Close"].iloc[-2]) if len(feat_df) > 1 else last
    day_abs = last - prev_close
    day_pct = (last / prev_close - 1.0) * 100.0 if prev_close > 0 else 0.0

    model_rets, model_meta = model_returns(feat_df, days)
    tech_rets = technical_returns(feat_df, days)
    news = fetch_news_sentiment(label, name, limit=news_limit) if use_news else {"score": 0.0, "count": 0, "headlines": [], "error": None}
    vol = recent_vol(feat_df)
    news_score = float(news.get("score", 0.0) or 0.0)
    news_impact = news_score * min(0.0028, vol * 0.18)
    news_rets = np.asarray([news_impact * (0.65 ** i) for i in range(days)])
    mw = float(model_meta.get("weight_hint") or 0.25)
    tw = 1.0 - mw
    raw_rets = mw * np.asarray(model_rets) + tw * np.asarray(tech_rets) + news_rets
    if short_history:
        independent_model = mw > 0.0 and not str(model_meta.get("model") or "").startswith("technical")
        agreement = bool(independent_model and np.sign(model_rets[-1]) == np.sign(tech_rets[-1]))
        shrink = 0.50
    else:
        agreement = np.sign(model_rets[-1]) == np.sign(tech_rets[-1])
        shrink = 0.82 if agreement else 0.58
    final_rets = raw_rets * shrink
    cap = daily_cap(feat_df, low_risk=True)
    final_rets = np.clip(final_rets, -cap, cap)
    prices = prices_from_log_returns(last, final_rets)
    dates = pd.bdate_range(feat_df.index[-1] + timedelta(days=1), periods=days)
    vol_ann = float(vol * math.sqrt(252) * 100)
    direction_acc = model_meta.get("directional_accuracy")
    edge_component = 0.0 if direction_acc is None else max(0, (float(direction_acc) - 0.5) * 60)
    confidence = float(np.clip(48 + edge_component - min(vol_ann, 90) * 0.18 + (8 if agreement else -4) + abs(news_score) * 4, 18, 88))
    history_confidence_factor = float(np.clip(math.sqrt(len(feat_df) / MIN_ROWS_FOR_FORECAST), 0.0, 1.0))
    if short_history:
        confidence = min(confidence, 22.0 + 30.0 * history_confidence_factor)
        model_meta["model"] = "recent-listing-technical-baseline"
        model_meta["history_confidence_factor"] = round(history_confidence_factor, 4)
    final_pct = float((prices[-1] / last - 1) * 100)
    if abs(final_pct) < MIN_DISPLAY_PCT:
        signal = "Neutral"
    elif final_pct > 0:
        signal = "Constructive"
    else:
        signal = "Cautious"
    if confidence < 38:
        signal = "Low conviction"
    risk = "Low" if vol_ann < 22 and confidence >= 52 else "Moderate" if vol_ann < 45 else "High"
    band = np.asarray([max(0.003, vol * math.sqrt(i + 1) * 0.9) for i in range(days)])
    lows = prices_from_log_returns(last, final_rets - band)
    highs = prices_from_log_returns(last, final_rets + band)
    # Use the run history for fast first paint. The dashboard loads deeper validated history on demand.
    history = []
    for dt, row in feat_df.tail(900).iterrows():
        history.append({
            "date": dt.strftime("%Y-%m-%d"),
            "open": round(float(row["Open"]), 6),
            "high": round(float(row["High"]), 6),
            "low": round(float(row["Low"]), 6),
            "close": round(float(row["Close"]), 6),
            "volume": round(float(row.get("Volume", 0.0)), 2),
        })
    profile = build_profile(label, name, feat_df, source)
    prob_up = probability_up_from_inputs(final_pct, vol_ann, confidence, news_score, profile.get("tradingview_rating", ""))
    forecast = []
    for i, dt in enumerate(dates):
        pct = float((prices[i] / last - 1) * 100)
        forecast.append({
            "date": dt.strftime("%Y-%m-%d"),
            "price": round(float(prices[i]), 6),
            "low": round(float(lows[i]), 6),
            "high": round(float(highs[i]), 6),
            "pct": pct,
            "daily_return_pct": float((math.exp(final_rets[i]) - 1) * 100),
        })
    drivers = []
    if short_history:
        drivers.append(f"Recent listing: only {len(feat_df)} daily sessions are available; the technical baseline is shrunk and confidence is capped.")
    else:
        drivers.append("Model/technical agreement is positive." if agreement else "Model and technical signals are mixed; forecast is shrunk for safety.")
    if news.get("count", 0): drivers.append(f"Recent news sentiment score: {news_score:+.2f} across {news.get('count')} headline(s).")
    drivers.append(f"Annualised realised volatility estimate: {vol_ann:.1f}%.")
    drivers.append(f"TradingView/technical rating input: {profile.get('tradingview_rating')} / {profile.get('technical_rating')}.")
    methods = [
        "Validated daily OHLCV fallback: Alpaca for US listings, then Yahoo, Stooq, TradingView and the last local validated snapshot.",
        "Ridge/Huber/optional boosting ensemble on log-return technical features.",
        "EWMA-style realised volatility and probabilistic forecast bands.",
        "Strict walk-forward setup evidence across 1/3/5/10/20 trading-day horizons.",
        "Benchmark comparison against buy-hold, momentum 20d/60d and mean-reversion baselines.",
        "Brier-score reliability curve and confidence decay by available evidence.",
        "Enterprise data-quality checks with source trace and reproducibility hash.",
        "TradingView screener/overview fundamentals and ratings, independent from the OHLCV fallback chain.",
        "Multi-source headline sentiment with source links.",
        "Remote realised-error learning calibration when Render API is configured.",
    ]
    if short_history:
        methods.insert(1, "Recent-listing fallback: technical-only baseline, no synthetic backfill, explicit confidence decay.")
    investment_score = float(np.clip((prob_up * 5.0) + (confidence / 100.0 * 2.0) + (1.0 if risk == "Low" else 0.45 if risk == "Moderate" else 0.0) + (0.8 if final_pct > 0 else 0.0), 0, 10))
    investment_label = "Strong setup" if investment_score >= 7 else "Constructive setup" if investment_score >= 5.5 else "Weak setup" if investment_score >= 2 else "Investment view"
    data_quality = data_quality_report(label, feat_df, source)
    if short_history:
        data_quality["issues"] = [
            f"Recent listing: {len(feat_df)} sessions available versus {MIN_ROWS_FOR_FORECAST} required for the standard model.",
            *data_quality.get("issues", []),
        ]
        data_quality["score"] = min(float(data_quality.get("score", 0.0)), 55.0)
        data_quality["status"] = "fail"
        data_quality.setdefault("checks", {})["standard_model_min_rows"] = MIN_ROWS_FOR_FORECAST
        data_quality["checks"]["recent_listing_fallback_min_rows"] = RECENT_LISTING_MIN_ROWS
    try:
        alpha_evidence = walk_forward_alpha_evidence(feat_df, days, cost_bps=transaction_cost_bps)
    except Exception as exc:
        alpha_evidence = {"status": "error", "reason": str(exc), "horizons": {}, "model_leaderboard": []}
    professional_decision = decision_block(
        label=label,
        name=name,
        feat_df=feat_df,
        forecast=forecast,
        final_pct=final_pct,
        confidence=confidence,
        risk=risk,
        news_score=news_score,
        profile=profile,
        alpha_evidence=alpha_evidence,
        data_quality=data_quality,
    )
    proof = signal_proof_card(label, name, professional_decision, alpha_evidence, data_quality, model_meta)
    return {
        "label": label,
        "name": name,
        "original_symbol": original_sym,
        "original_exchange": original_exch,
        "tv_symbol": source.get("tv_symbol"),
        "tv_exchange": source.get("tv_exchange"),
        "history_symbol": source.get("symbol"),
        "history_exchange": source.get("exchange"),
        "history_provider": source.get("provider"),
        "rows": int(len(feat_df)),
        "history_mode": "recent-listing-short-history" if short_history else "standard",
        "history_confidence_factor": round(history_confidence_factor, 4),
        "last": round(last, 6),
        "previous_close": round(prev_close, 6),
        "day_change_abs": round(day_abs, 6),
        "day_change_pct": round(day_pct, 6),
        "last_date": feat_df.index[-1].strftime("%Y-%m-%d"),
        "forecast": forecast,
        "change_5d_pct": final_pct,
        "change_horizon_pct": final_pct,
        "horizon_days": days,
        "signal": signal,
        "confidence": round(confidence, 1),
        "risk": risk,
        "volatility_ann_pct": round(vol_ann, 2),
        "probability_up": round(prob_up, 4),
        "investment_score": round(investment_score, 2),
        "investment_label": investment_label,
        "professional_decision": professional_decision,
        "alpha_evidence": alpha_evidence,
        "data_quality": data_quality,
        "signal_proof": proof,
        "profile": profile,
        "news": news,
        "analysis": {
            "drivers": drivers,
            "method_summary": methods,
            "scenario": {"probability_up": round(prob_up, 4), "central_case_pct": round(final_pct, 4), "risk_regime": risk},
            "weights": {"model": round(mw, 3), "technical": round(tw, 3), "news_score": round(news_score, 3)},
        },
        "learning": {"mae_pct": None, "directional_accuracy": None, "evaluated_this_run": 0, "count": 0, "reliability": 0.0, "bias_correction_pct": 0.0, "source": "cold start/local"},
        "model": {
            "name": model_meta.get("model"),
            "weight_model": round(mw, 3),
            "weight_technical": round(tw, 3),
            "mae": model_meta.get("mae"),
            "directional_accuracy": direction_acc,
            "agreement": bool(agreement),
            "daily_cap_pct": round(cap * 100, 3),
            "leaderboard": model_meta.get("leaderboard", []),
            "optional_models": model_meta.get("optional_models", {}),
        },
        "audit_trail": {
            "engine": ENGINE_NAME,
            "engine_version": ENGINE_VERSION,
            "feature_version": FEATURE_VERSION,
            "generated_at": _utc_now_iso(),
            "data_snapshot_hash": data_quality.get("snapshot_hash"),
            "source_trace": data_quality.get("source"),
            "history_mode": "recent-listing-short-history" if short_history else "standard",
            "no_personalised_advice": True,
        },
        "history": history,
    }


def run_watchlist(
    labels: list[str],
    days: int,
    n_bars: int,
    use_news: bool,
    news_limit: int,
    progress_file: str | None = None,
    transaction_cost_bps: float = DEFAULT_TRANSACTION_COST_BPS,
) -> dict:
    results = []
    total = len(labels)
    _progress(progress_file, 0, total, "Preload market metadata")
    try:
        prefetch_tradingview_scanner(labels)
        prefetch_yahoo_quotes(labels)
    except Exception:
        pass
    _progress(progress_file, 0, total, "Initialisation")
    for i, label in enumerate(labels, 1):
        _progress(progress_file, i - 1, total, f"Analyse de {label}", active=label)
        try:
            result = forecast_one(label, days=days, n_bars=n_bars, use_news=use_news, news_limit=news_limit, transaction_cost_bps=transaction_cost_bps)
        except Exception as exc:
            result = {"label": label, "error": str(exc)}
        results.append(result)
        _progress(progress_file, i, total, f"{label} terminé", active=label)
    ok = [r for r in results if not r.get("error")]
    attach_cross_sectional_context(results)
    portfolio = portfolio_ranking(results, transaction_cost_bps=transaction_cost_bps)
    avg = float(np.mean([r["change_5d_pct"] for r in ok])) if ok else 0.0
    payload = {
        "generated_at": _utc_now_iso(),
        "engine": f"{ENGINE_NAME}-{ENGINE_VERSION}",
        "days": days,
        "count": len(results),
        "success_count": len(ok),
        "average_forecast_pct": avg,
        "portfolio_ranking": portfolio,
        "research_contract": {
            "positioning": "statistically measured setup engine, not a price-prediction promise",
            "evidence_horizons": list(ALPHA_HORIZONS),
            "transaction_cost_bps": float(transaction_cost_bps),
            "benchmarks": ["buy_hold", "momentum_20d", "momentum_60d", "mean_reversion", "SPY/IXIC/SMH relative context when present"],
            "metrics": ["hit_rate", "directional_accuracy", "mae_pct", "sharpe", "max_drawdown_pct", "profit_factor", "turnover", "brier_score"],
            "data_governance": "source trace, quality checks, snapshot hash and commercial-license flag emitted per ticker",
            "architecture_map": {
                "data": "Alpaca/Yahoo/Stooq/TradingView/cache history fallback, market metadata prefetch and data_quality_report",
                "features": "build_features, setup_score_series, regime_from_features",
                "models": "model_returns ensemble and optional model detection",
                "backtests": "walk_forward_alpha_evidence, strategy_metrics, calibration_report",
                "signals": "decision_block, signal_proof_card, attach_cross_sectional_context, portfolio_ranking",
                "reports": "alpha_evidence, professional_decision, signal_proof, portfolio_ranking JSON blocks",
            },
        },
        "results": results,
        "disclaimer": "Statistical research signals only. Not personalised financial advice or trade instructions.",
    }
    _progress(progress_file, total, total, "Terminé")
    return payload


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Apex Tool v15: professional alpha-evidence and market setup engine with JSON output")
    p.add_argument("--days", type=int, default=DEFAULT_DAYS, help="Forecast horizon in trading days, default 5")
    p.add_argument("--only", nargs="+", metavar="LABEL", help="Ticker labels to process")
    p.add_argument("--mode", choices=["fast", "balanced", "deep"], default="balanced", help="Data depth preset")
    p.add_argument("--bars", type=int, default=None, help="Daily candles requested from the validated multi-source history chain")
    p.add_argument("--history-only", metavar="LABEL", help="Return maximum available OHLCV history for one chart and exit")
    p.add_argument("--no-news", action="store_true", help="Disable news sentiment")
    p.add_argument("--news-limit", type=int, default=8, help="Headlines per ticker")
    p.add_argument("--transaction-cost-bps", type=float, default=DEFAULT_TRANSACTION_COST_BPS, help="Round-trip transaction cost assumption used in setup backtests")
    p.add_argument("--json-out", default=None, help="Write structured result JSON to this path")
    p.add_argument("--progress-file", default=None, help="Write progress JSON to this path")
    p.add_argument("--quiet", action="store_true", help="Suppress human console summary")
    # Kept for compatibility with older dashboard/buttons.
    p.add_argument("--fast", action="store_true", help=argparse.SUPPRESS)
    p.add_argument("--no-charts", action="store_true", help=argparse.SUPPRESS)
    p.add_argument("--skip-selenium", action="store_true", help=argparse.SUPPRESS)
    p.add_argument("--no-store-learning", action="store_true", help=argparse.SUPPRESS)
    return p.parse_args()


def main() -> None:
    args = parse_args()
    if args.history_only:
        lab = str(args.history_only).upper().strip()
        bars = int(args.bars or FULL_CHART_BARS)
        item = info_from_label(lab)
        label = item[0] if item else lab
        df, source = fetch_history_by_label(label, n_bars=bars, min_rows=5)
        history = []
        if df is not None and not df.empty:
            for dt, row in df.iterrows():
                history.append({
                    "date": pd.to_datetime(dt).strftime("%Y-%m-%d"),
                    "open": round(float(row["Open"]), 6),
                    "high": round(float(row["High"]), 6),
                    "low": round(float(row["Low"]), 6),
                    "close": round(float(row["Close"]), 6),
                    "volume": round(float(row.get("Volume", 0) or 0), 3),
                })
        payload = {
            "label": label,
            "name": item[3] if item else lab,
            "ok": bool(history),
            "bars_requested": bars,
            "bars_returned": len(history),
            "source": source.get("provider") or f"{source.get('exchange') or ''}:{source.get('symbol') or ''}".strip(':'),
            "source_symbol": source.get("symbol"),
            "source_exchange": source.get("exchange"),
            "source_attempts": source.get("errors", []),
            "history": history,
        }
        _write_json(args.json_out, payload)
        if not args.quiet:
            print(json.dumps(payload, ensure_ascii=False, indent=2))
        return
    mode = "fast" if args.fast else args.mode
    if args.bars is not None:
        bars = args.bars
    elif mode == "fast":
        bars = FAST_BARS
    elif mode == "deep":
        bars = DEEP_BARS
    else:
        bars = DEFAULT_BARS
    days = int(np.clip(args.days, 1, 15))
    if args.only:
        requested = [x.upper() for x in args.only]
        labels = []
        for req in requested:
            item = info_from_label(req)
            if item:
                labels.append(item[0])
        labels = list(dict.fromkeys(labels))
    else:
        labels = [x[0] for x in WATCHLIST]
    payload = run_watchlist(
        labels,
        days=days,
        n_bars=bars,
        use_news=not args.no_news,
        news_limit=args.news_limit,
        progress_file=args.progress_file,
        transaction_cost_bps=float(args.transaction_cost_bps),
    )
    _write_json(args.json_out, payload)
    if not args.quiet:
        print(f"Generated {payload['success_count']}/{payload['count']} forecasts · {days} trading days")
        for r in payload["results"]:
            if r.get("error"):
                print(f"{r.get('label','?'):<8} ERROR {r['error']}")
                continue
            pct = r["change_5d_pct"]
            pct_txt = "stable" if abs(pct) < MIN_DISPLAY_PCT else f"{pct:+.2f}%"
            print(f"{r['label']:<8} {r['last']:>10.3f} -> {r['forecast'][-1]['price']:>10.3f}  {pct_txt:>8}  {r['signal']:<15} C{r['confidence']:.0f}")


if __name__ == "__main__":
    main()
