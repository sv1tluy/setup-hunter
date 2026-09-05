"""
Market Setup Hunter — Trading Hub Bot
=====================================
Темы:
  Скринер   (thread 2)
  Новости   (thread 3)
  Журнал    (thread 4) — скипаем, будет web-app
  ИИ хелпер (thread 5)

Запуск на Render: gunicorn -w 1 bot:app
"""

import os
import json
import time
import sqlite3
import threading
import logging
import traceback
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List, Tuple

import requests
import pandas as pd
import mplfinance as mpf
import yfinance as yf
import ccxt
from flask import Flask, jsonify, render_template

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("trading_hub")

try:
    from zoneinfo import ZoneInfo
    NY_TZ = ZoneInfo("America/New_York")
except Exception:
    NY_TZ = None

app = Flask(__name__)

# =========================================================================
# НАСТРОЙКИ / ENV
# =========================================================================
BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID", "-1003970795061")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")  # задай в Render Environment

# Жёстко прописанные thread_id из /whereami (можно переопределить env)
SCREENER_TOPIC_ID = int(os.environ.get("SCREENER_TOPIC_ID", "2"))
NEWS_TOPIC_ID = int(os.environ.get("NEWS_TOPIC_ID", "3"))
# JOURNAL_TOPIC_ID = 4  — скипаем
AI_TOPIC_ID = int(os.environ.get("AI_TOPIC_ID", "5"))

if not BOT_TOKEN:
    raise RuntimeError("Не задан BOT_TOKEN")

SCAN_INTERVAL_SECONDS = int(os.environ.get("SCAN_INTERVAL_SECONDS", "300"))
API_URL = f"https://api.telegram.org/bot{BOT_TOKEN}"
DB_PATH = os.environ.get("ALERTS_DB_PATH", "bot_state.db")
PAGE_SIZE = 8
TOP_CRYPTO_N = 40

# Список бирж для fallback (порядок приоритета)
CRYPTO_EXCHANGE_CANDIDATES = [
    os.environ.get("CRYPTO_EXCHANGE_ID", "bybit"),
    "okx",
    "gate",
    "binanceusdm",
    "kucoin",
]

# =========================================================================
# Глобальные объекты
# =========================================================================
db_lock = threading.Lock()
_perp_symbol_cache: Dict[str, str] = {}
_active_exchange_id: Optional[str] = None
crypto_spot = None
crypto_perp = None

market_data: Dict[str, Any] = {"last_update": "Только что"}

# =========================================================================
# РЕЕСТР ИНСТРУМЕНТОВ (без изменений по смыслу)
# =========================================================================
FX_INSTRUMENTS = {
    "FX_EURUSD": {"label": "EUR/USD", "kind": "forex", "ticker": "EURUSD=X"},
    "FX_GBPUSD": {"label": "GBP/USD", "kind": "forex", "ticker": "GBPUSD=X"},
    "FX_USDJPY": {"label": "USD/JPY", "kind": "forex", "ticker": "USDJPY=X"},
    "FX_USDCHF": {"label": "USD/CHF", "kind": "forex", "ticker": "USDCHF=X"},
    "FX_AUDUSD": {"label": "AUD/USD", "kind": "forex", "ticker": "AUDUSD=X"},
    "FX_USDCAD": {"label": "USD/CAD", "kind": "forex", "ticker": "USDCAD=X"},
    "FX_NZDUSD": {"label": "NZD/USD", "kind": "forex", "ticker": "NZDUSD=X"},
}

MT_INSTRUMENTS = {
    "MT_XAUUSD_GCF": {"label": "XAU/USD (COMEX фьючерс)", "kind": "forex", "ticker": "GC=F"},
    "MT_XAUUSDT_PERP": {"label": "XAU/USDT (перп)", "kind": "crypto_perp", "ticker": "XAUUSDT"},
    "MT_XAGUSD_SIF": {"label": "XAG/USD (COMEX фьючерс, серебро)", "kind": "forex", "ticker": "SI=F"},
}

NQ_TICKERS = [
    ("NVDA", "NVIDIA"), ("AAPL", "Apple"), ("GOOGL", "Alphabet (Google)"),
    ("MSFT", "Microsoft"), ("AMZN", "Amazon"), ("AMD", "AMD"),
    ("AVGO", "Broadcom"), ("TSLA", "Tesla"), ("META", "Meta"),
    ("INTC", "Intel"), ("CSCO", "Cisco"), ("AMAT", "Applied Materials"),
    ("COST", "Costco"), ("LRCX", "Lam Research"), ("NFLX", "Netflix"),
    ("PLTR", "Palantir"), ("KLAC", "KLA"), ("PANW", "Palo Alto Networks"),
    ("TXN", "Texas Instruments"), ("TMUS", "T-Mobile US"), ("AMGN", "Amgen"),
    ("CRWD", "CrowdStrike"), ("ADI", "Analog Devices"), ("QCOM", "Qualcomm"),
    ("PEP", "PepsiCo"), ("GILD", "Gilead Sciences"), ("ASML", "ASML"),
    ("HON", "Honeywell"), ("BKNG", "Booking Holdings"), ("ISRG", "Intuitive Surgical"),
    ("VRTX", "Vertex Pharmaceuticals"), ("SBUX", "Starbucks"), ("ADBE", "Adobe"),
    ("MU", "Micron"), ("WDC", "Western Digital"),
]
NQ_INSTRUMENTS = {
    f"NQ_{sym}": {"label": f"{name} ({sym})", "kind": "equity", "ticker": sym}
    for sym, name in NQ_TICKERS
}


def _make_exchange(exchange_id: str, market_type: str):
    exchange_class = getattr(ccxt, exchange_id)
    return exchange_class({"enableRateLimit": True, "options": {"defaultType": market_type}})


def init_crypto_exchanges() -> str:
    """Пробуем биржи по списку, возвращаем id первой рабочей."""
    global crypto_spot, crypto_perp, _active_exchange_id, _perp_symbol_cache

    for eid in CRYPTO_EXCHANGE_CANDIDATES:
        if not eid or eid not in ccxt.exchanges:
            continue
        try:
            log.info(f"Пробую биржу {eid}...")
            spot = _make_exchange(eid, "spot")
            # Быстрая проверка: load_markets + один тикер
            markets = spot.load_markets()
            if not markets:
                raise RuntimeError("empty markets")
            # Проверяем, что есть хотя бы BTC/USDT
            has_btc = any("BTC" in s and "USDT" in s for s in markets)
            if not has_btc:
                raise RuntimeError("no BTC/USDT-like pair")

            perp = _make_exchange(eid, "swap")
            crypto_spot = spot
            crypto_perp = perp
            _active_exchange_id = eid
            _perp_symbol_cache.clear()
            log.info(f"✅ Активная крипто-биржа: {eid}")
            return eid
        except Exception as e:
            log.warning(f"Биржа {eid} недоступна: {e}")
            continue

    # Полный fallback — фиксированный набор без live-объёма
    log.error("Ни одна биржа не ответила. Использую фиксированный fallback.")
    _active_exchange_id = "fallback"
    return "fallback"


def build_crypto_universe(n: int = TOP_CRYPTO_N) -> Dict[str, dict]:
    must_include = ["BTC/USDT", "ETH/USDT", "SOL/USDT"]
    if _active_exchange_id == "fallback" or crypto_spot is None:
        fallback = ["BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT", "XRP/USDT", "ADA/USDT", "DOGE/USDT"]
        return {f"CR_{s.split('/')[0]}": {"label": s, "kind": "crypto", "ticker": s} for s in fallback}

    try:
        markets = crypto_spot.load_markets()
        tickers = crypto_spot.fetch_tickers()
        pairs = []
        for symbol, m in markets.items():
            if m.get("quote") == "USDT" and m.get("spot") and m.get("active", True):
                t = tickers.get(symbol) or {}
                vol = t.get("quoteVolume") or t.get("baseVolume") or 0
                if vol:
                    pairs.append((symbol, float(vol)))
        pairs.sort(key=lambda x: x[1], reverse=True)
        top_symbols = [s for s, _ in pairs[:n]]
        for m in must_include:
            if m not in top_symbols:
                top_symbols.insert(0, m)
        universe = {}
        for symbol in top_symbols:
            base = symbol.split("/")[0]
            universe[f"CR_{base}"] = {"label": symbol, "kind": "crypto", "ticker": symbol}
        log.info(f"Крипто-вселенная: {len(universe)} пар с {_active_exchange_id}")
        return universe
    except Exception as e:
        log.warning(f"Не удалось получить топ крипты: {e}. Fallback.")
        fallback = ["BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT", "XRP/USDT", "ADA/USDT", "DOGE/USDT"]
        return {f"CR_{s.split('/')[0]}": {"label": s, "kind": "crypto", "ticker": s} for s in fallback}


# Инициализация биржи и вселенной при старте
init_crypto_exchanges()
CR_INSTRUMENTS = build_crypto_universe()

AVAILABLE_INSTRUMENTS = {**FX_INSTRUMENTS, **MT_INSTRUMENTS, **CR_INSTRUMENTS, **NQ_INSTRUMENTS}

INSTRUMENT_CATEGORIES = {
    "fx": {"title": "Форекс (мажоры)", "items": FX_INSTRUMENTS},
    "mt": {"title": "Металлы", "items": MT_INSTRUMENTS},
    "cr": {
        "title": f"Крипто (топ {len(CR_INSTRUMENTS)}, {_active_exchange_id})",
        "items": CR_INSTRUMENTS,
    },
    "nq": {"title": "NASDAQ (популярные)", "items": NQ_INSTRUMENTS},
}

DEFAULT_SETTINGS = {
    "symbols": ["FX_EURUSD", "FX_GBPUSD", "MT_XAUUSD_GCF", "CR_BTC", "CR_ETH", "CR_SOL"],
    "enabled_strategies": ["smc_sweep_fvg"],
    "smc_trigger_sweep": True,
    "smc_trigger_fvg": True,
    "notify_always": False,
    "scanning_enabled": True,
}


# =========================================================================
# БАЗА ДАННЫХ (thread-safe)
# =========================================================================
def _get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    with db_lock:
        conn = _get_conn()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS sent_alerts (
                alert_key TEXT PRIMARY KEY,
                signal_time TEXT NOT NULL,
                sent_at TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS user_settings (
                chat_id TEXT PRIMARY KEY,
                settings_json TEXT NOT NULL,
                updated_at TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        conn.commit()
        conn.close()
    log.info("DB initialized")


def get_user_settings(chat_id: str) -> dict:
    with db_lock:
        conn = _get_conn()
        row = conn.execute(
            "SELECT settings_json FROM user_settings WHERE chat_id = ?", (chat_id,)
        ).fetchone()
        if row is None:
            settings = dict(DEFAULT_SETTINGS)
            conn.execute(
                "INSERT INTO user_settings (chat_id, settings_json, updated_at) VALUES (?, ?, ?)",
                (chat_id, json.dumps(settings), datetime.now(timezone.utc).isoformat()),
            )
            conn.commit()
        else:
            settings = dict(DEFAULT_SETTINGS)
            settings.update(json.loads(row[0]))
        conn.close()
    return settings


def save_user_settings(chat_id: str, **updates) -> dict:
    current = get_user_settings(chat_id)
    current.update(updates)
    with db_lock:
        conn = _get_conn()
        conn.execute(
            """
            INSERT INTO user_settings (chat_id, settings_json, updated_at) VALUES (?, ?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET settings_json = excluded.settings_json,
                                                updated_at = excluded.updated_at
            """,
            (chat_id, json.dumps(current), datetime.now(timezone.utc).isoformat()),
        )
        conn.commit()
        conn.close()
    return current


def toggle_symbol(chat_id: str, symbol_key: str) -> dict:
    settings = get_user_settings(chat_id)
    symbols = set(settings["symbols"])
    if symbol_key in symbols:
        symbols.discard(symbol_key)
    else:
        symbols.add(symbol_key)
    return save_user_settings(chat_id, symbols=list(symbols))


def toggle_bool_setting(chat_id: str, key: str) -> dict:
    settings = get_user_settings(chat_id)
    return save_user_settings(chat_id, **{key: not settings[key]})


def toggle_strategy(chat_id: str, strategy_id: str) -> dict:
    settings = get_user_settings(chat_id)
    enabled = set(settings["enabled_strategies"])
    if strategy_id in enabled:
        enabled.discard(strategy_id)
    else:
        enabled.add(strategy_id)
    return save_user_settings(chat_id, enabled_strategies=list(enabled))


def get_last_alert_time(alert_key: str) -> Optional[str]:
    with db_lock:
        conn = _get_conn()
        row = conn.execute(
            "SELECT signal_time FROM sent_alerts WHERE alert_key = ?", (alert_key,)
        ).fetchone()
        conn.close()
    return row[0] if row else None


def set_last_alert_time(alert_key: str, signal_time_str: str):
    with db_lock:
        conn = _get_conn()
        conn.execute(
            """
            INSERT INTO sent_alerts (alert_key, signal_time, sent_at) VALUES (?, ?, ?)
            ON CONFLICT(alert_key) DO UPDATE SET signal_time = excluded.signal_time, sent_at = excluded.sent_at
            """,
            (alert_key, signal_time_str, datetime.now(timezone.utc).isoformat()),
        )
        conn.commit()
        conn.close()


def get_update_offset() -> int:
    with db_lock:
        conn = _get_conn()
        row = conn.execute("SELECT value FROM meta WHERE key = 'update_offset'").fetchone()
        conn.close()
    return int(row[0]) if row else 0


def set_update_offset(offset: int):
    with db_lock:
        conn = _get_conn()
        conn.execute(
            "INSERT INTO meta (key, value) VALUES ('update_offset', ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (str(offset),),
        )
        conn.commit()
        conn.close()


# =========================================================================
# ИНДИКАТОРЫ И СТРАТЕГИИ (без изменений логики)
# =========================================================================
def find_fvg(df):
    fvg_list = []
    for i in range(2, len(df)):
        if df["Low"].iloc[i] > df["High"].iloc[i - 2]:
            fvg_list.append(
                {
                    "type": "Bullish FVG",
                    "top": df["Low"].iloc[i],
                    "bottom": df["High"].iloc[i - 2],
                    "time": df.index[i],
                }
            )
        elif df["High"].iloc[i] < df["Low"].iloc[i - 2]:
            fvg_list.append(
                {
                    "type": "Bearish FVG",
                    "top": df["Low"].iloc[i - 2],
                    "bottom": df["High"].iloc[i],
                    "time": df.index[i],
                }
            )
    return fvg_list


def check_sweep(df):
    if len(df) < 10:
        return None
    recent_high = df["High"].iloc[-10:-1].max()
    recent_low = df["Low"].iloc[-10:-1].min()
    current_high = df["High"].iloc[-1]
    current_low = df["Low"].iloc[-1]
    current_close = df["Close"].iloc[-1]

    if current_high > recent_high and current_close < recent_high:
        return "Bearish Sweep"
    if current_low < recent_low and current_close > recent_low:
        return "Bullish Sweep"
    return None


def check_4h_fvg_trigger(df_4h):
    fvgs = find_fvg(df_4h)
    if not fvgs:
        return None
    last = fvgs[-1]
    if df_4h.index.get_loc(last["time"]) >= len(df_4h) - 2:
        return last
    return None


def compute_ema(series, span):
    return series.ewm(span=span, adjust=False).mean()


def compute_rsi(series, period=14):
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / period, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1 / period, min_periods=period).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def find_swings(df, lookback=3):
    highs, lows = [], []
    for i in range(lookback, len(df) - lookback):
        window = df.iloc[i - lookback : i + lookback + 1]
        if df["High"].iloc[i] == window["High"].max():
            highs.append((df.index[i], df["High"].iloc[i]))
        if df["Low"].iloc[i] == window["Low"].min():
            lows.append((df.index[i], df["Low"].iloc[i]))
    return highs, lows


def detect_smc_sweep_fvg(df_4h, df_15m, settings):
    active_triggers = []
    if settings.get("smc_trigger_sweep", True):
        sweep = check_sweep(df_4h)
        if sweep:
            direction = "Bullish" if sweep == "Bullish Sweep" else "Bearish"
            active_triggers.append(("Sweep", direction))
    if settings.get("smc_trigger_fvg", True):
        fvg4h = check_4h_fvg_trigger(df_4h)
        if fvg4h:
            direction = "Bullish" if fvg4h["type"] == "Bullish FVG" else "Bearish"
            active_triggers.append(("FVG", direction))
    if not active_triggers:
        return None

    fvg_15m_list = find_fvg(df_15m)
    if not fvg_15m_list:
        return None
    last_fvg_15m = fvg_15m_list[-1]
    if df_15m.index.get_loc(last_fvg_15m["time"]) < len(df_15m) - 3:
        return None

    direction_15m = "Bullish" if last_fvg_15m["type"] == "Bullish FVG" else "Bearish"
    matching = [label for label, d in active_triggers if d == direction_15m]
    if not matching:
        return None

    direction_ru = "Лонг" if direction_15m == "Bullish" else "Шорт"
    return {"time": last_fvg_15m["time"], "direction": direction_ru, "trigger": " + ".join(matching)}


def detect_bos(df_4h, df_15m, settings):
    if len(df_15m) < 20:
        return None
    highs, lows = find_swings(df_15m, lookback=3)
    if not highs or not lows:
        return None
    last_high = highs[-1][1]
    last_low = lows[-1][1]
    last_close = df_15m["Close"].iloc[-1]
    last_time = df_15m.index[-1]
    if last_close > last_high:
        return {"time": last_time, "direction": "Лонг", "trigger": "BOS (пробой хая)"}
    if last_close < last_low:
        return {"time": last_time, "direction": "Шорт", "trigger": "BOS (пробой лоя)"}
    return None


def detect_order_block(df_4h, df_15m, settings):
    if len(df_15m) < 25:
        return None
    body = (df_15m["Close"] - df_15m["Open"]).abs()
    avg_body = body.rolling(20).mean()
    for i in range(len(df_15m) - 1, max(len(df_15m) - 8, 20), -1):
        if pd.isna(avg_body.iloc[i]) or avg_body.iloc[i] == 0:
            continue
        if body.iloc[i] <= 1.8 * avg_body.iloc[i]:
            continue
        impulse_bullish = df_15m["Close"].iloc[i] > df_15m["Open"].iloc[i]
        ob_index = i - 1
        while ob_index > 0:
            is_opposite = (
                (df_15m["Close"].iloc[ob_index] < df_15m["Open"].iloc[ob_index])
                if impulse_bullish
                else (df_15m["Close"].iloc[ob_index] > df_15m["Open"].iloc[ob_index])
            )
            if is_opposite:
                break
            ob_index -= 1
        if ob_index <= 0:
            continue
        ob_high = df_15m["High"].iloc[ob_index]
        ob_low = df_15m["Low"].iloc[ob_index]
        last_low = df_15m["Low"].iloc[-1]
        last_high = df_15m["High"].iloc[-1]
        touched = last_low <= ob_high and last_high >= ob_low
        if touched:
            direction = "Лонг" if impulse_bullish else "Шорт"
            return {"time": df_15m.index[-1], "direction": direction, "trigger": "Order Block retest"}
    return None


def detect_ema_pullback(df_4h, df_15m, settings):
    if len(df_4h) < 50 or len(df_15m) < 25:
        return None
    ema50_4h = compute_ema(df_4h["Close"], 50)
    trend_up = ema50_4h.iloc[-1] > ema50_4h.iloc[-5]
    trend_down = ema50_4h.iloc[-1] < ema50_4h.iloc[-5]
    ema21_15m = compute_ema(df_15m["Close"], 21)
    last_close = df_15m["Close"].iloc[-1]
    last_low = df_15m["Low"].iloc[-1]
    last_high = df_15m["High"].iloc[-1]
    last_ema = ema21_15m.iloc[-1]
    if trend_up and last_low <= last_ema < last_close:
        return {"time": df_15m.index[-1], "direction": "Лонг", "trigger": "EMA21 pullback (аптренд)"}
    if trend_down and last_high >= last_ema > last_close:
        return {"time": df_15m.index[-1], "direction": "Шорт", "trigger": "EMA21 pullback (даунтренд)"}
    return None


def detect_rsi_reversal(df_4h, df_15m, settings):
    if len(df_15m) < 20:
        return None
    rsi = compute_rsi(df_15m["Close"], 14)
    if rsi.iloc[-2:].isna().any():
        return None
    prev_rsi = rsi.iloc[-2]
    last_rsi = rsi.iloc[-1]
    if prev_rsi < 30 <= last_rsi:
        return {"time": df_15m.index[-1], "direction": "Лонг", "trigger": "RSI выход из перепроданности"}
    if prev_rsi > 70 >= last_rsi:
        return {"time": df_15m.index[-1], "direction": "Шорт", "trigger": "RSI выход из перекупленности"}
    return None


STRATEGIES = {
    "smc_sweep_fvg": {
        "label": "SMC: 4H Sweep/FVG + 15M FVG",
        "detect": detect_smc_sweep_fvg,
        "configurable": True,
    },
    "bos": {"label": "Break of Structure (15M)", "detect": detect_bos, "configurable": False},
    "order_block": {"label": "Order Block retest (15M)", "detect": detect_order_block, "configurable": False},
    "ema_pullback": {"label": "EMA21 Pullback по тренду 4H", "detect": detect_ema_pullback, "configurable": False},
    "rsi_reversal": {"label": "RSI(14) реверсал (15M)", "detect": detect_rsi_reversal, "configurable": False},
}


# =========================================================================
# ДАННЫЕ РЫНКА
# =========================================================================
def is_forex_market_open() -> bool:
    now = datetime.now(timezone.utc)
    if now.weekday() == 5:
        return False
    if now.weekday() == 6 and now.hour < 22:
        return False
    if now.weekday() == 4 and now.hour >= 22:
        return False
    return True


def is_us_equity_market_open() -> bool:
    if NY_TZ is not None:
        now_ny = datetime.now(NY_TZ)
        if now_ny.weekday() >= 5:
            return False
        minutes = now_ny.hour * 60 + now_ny.minute
        return 9 * 60 + 30 <= minutes <= 16 * 60
    now = datetime.now(timezone.utc)
    if now.weekday() >= 5:
        return False
    minutes = now.hour * 60 + now.minute
    return 13 * 60 + 30 <= minutes <= 21 * 60


def get_yfinance_data(ticker_symbol: str, timeframe: str):
    interval = "15m" if timeframe == "15M" else "60m"
    period = "5d" if timeframe == "15M" else "1mo"
    try:
        df = yf.download(tickers=ticker_symbol, period=period, interval=interval, progress=False, auto_adjust=True)
    except Exception as e:
        log.warning(f"yfinance {ticker_symbol} {timeframe}: {e}")
        return None
    if df is None or df.empty:
        return None
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    df = df[["Open", "High", "Low", "Close", "Volume"]].dropna()
    if df.empty:
        return None
    if timeframe == "4H":
        df = (
            df.resample("4h")
            .agg({"Open": "first", "High": "max", "Low": "min", "Close": "last", "Volume": "sum"})
            .dropna()
        )
    return df


def get_crypto_spot_data(symbol: str, timeframe: str):
    if crypto_spot is None:
        return None
    tf_map = {"15M": "15m", "4H": "4h"}
    try:
        ohlcv = crypto_spot.fetch_ohlcv(symbol, tf_map[timeframe], limit=100)
        df = pd.DataFrame(ohlcv, columns=["time", "Open", "High", "Low", "Close", "Volume"])
        df["time"] = pd.to_datetime(df["time"], unit="ms")
        df.set_index("time", inplace=True)
        return df
    except Exception as e:
        log.warning(f"crypto spot {symbol} {timeframe}: {e}")
        return None


def resolve_perp_symbol(preferred_ticker: str) -> str:
    if preferred_ticker in _perp_symbol_cache:
        return _perp_symbol_cache[preferred_ticker]
    if crypto_perp is None:
        raise RuntimeError("perp exchange not ready")
    markets = crypto_perp.load_markets()
    candidates = [
        preferred_ticker,
        f"{preferred_ticker[:3]}/{preferred_ticker[3:]}:USDT",
        preferred_ticker.replace("USDT", "/USDT:USDT"),
    ]
    for c in candidates:
        if c in markets:
            _perp_symbol_cache[preferred_ticker] = c
            return c
    base_guess = preferred_ticker.replace("USDT", "").replace("/", "")
    fallback = [s for s in markets if base_guess in s.upper() and "USDT" in s.upper()]
    if fallback:
        _perp_symbol_cache[preferred_ticker] = fallback[0]
        return fallback[0]
    raise RuntimeError(f"Не найден символ '{preferred_ticker}' на {_active_exchange_id}")


def get_crypto_perp_data(preferred_ticker: str, timeframe: str):
    if crypto_perp is None:
        return None
    tf_map = {"15M": "15m", "4H": "4h"}
    try:
        symbol = resolve_perp_symbol(preferred_ticker)
        ohlcv = crypto_perp.fetch_ohlcv(symbol, tf_map[timeframe], limit=100)
        df = pd.DataFrame(ohlcv, columns=["time", "Open", "High", "Low", "Close", "Volume"])
        df["time"] = pd.to_datetime(df["time"], unit="ms")
        df.set_index("time", inplace=True)
        return df
    except Exception as e:
        log.warning(f"crypto perp {preferred_ticker} {timeframe}: {e}")
        return None


def fetch_pair_data(instrument_key: str):
    info = AVAILABLE_INSTRUMENTS[instrument_key]
    kind = info["kind"]
    ticker = info["ticker"]

    if kind == "forex":
        if not is_forex_market_open():
            return None, None, "market_closed"
        df_4h = get_yfinance_data(ticker, "4H")
        df_15m = get_yfinance_data(ticker, "15M")
    elif kind == "equity":
        if not is_us_equity_market_open():
            return None, None, "market_closed"
        df_4h = get_yfinance_data(ticker, "4H")
        df_15m = get_yfinance_data(ticker, "15M")
    elif kind == "crypto":
        df_4h = get_crypto_spot_data(ticker, "4H")
        df_15m = get_crypto_spot_data(ticker, "15M")
    elif kind == "crypto_perp":
        df_4h = get_crypto_perp_data(ticker, "4H")
        df_15m = get_crypto_perp_data(ticker, "15M")
    else:
        return None, None, "unknown_kind"

    return df_4h, df_15m, None


# =========================================================================
# AI (Gemini) — TP/SL подсказки
# =========================================================================
def get_ai_tp_sl(label: str, direction: str, trigger: str, last_close: float, atr: float = None) -> str:
    """Короткая подсказка TP/SL для новичка. Если нет ключа — fallback-эвристика."""
    if not GEMINI_API_KEY:
        # Простая эвристика без AI
        if atr and atr > 0:
            if direction == "Лонг":
                sl = last_close - 1.5 * atr
                tp1 = last_close + 2.0 * atr
                tp2 = last_close + 3.5 * atr
            else:
                sl = last_close + 1.5 * atr
                tp1 = last_close - 2.0 * atr
                tp2 = last_close - 3.5 * atr
            return (
                f"📍 <b>Подсказка (эвристика):</b>\n"
                f"SL ≈ <code>{sl:.5g}</code>\n"
                f"TP1 ≈ <code>{tp1:.5g}</code>  |  TP2 ≈ <code>{tp2:.5g}</code>\n"
                f"<i>Риск 1 : 1.3–2.3. Ставь SL за ближайший свинг.</i>"
            )
        return "📍 <i>Укажи GEMINI_API_KEY для умных TP/SL</i>"

    try:
        from google import genai
        client = genai.Client(api_key=GEMINI_API_KEY)
        prompt = (
            f"Ты опытный трейдер. Инструмент: {label}. "
            f"Сигнал: {direction}. Триггер: {trigger}. "
            f"Текущая цена ≈ {last_close}. "
            f"Дай очень короткий ответ на русском (макс 4 строки) для новичка:\n"
            f"1) Куда примерно поставить Stop Loss\n"
            f"2) Take Profit 1 и Take Profit 2\n"
            f"3) Одно предложение риска/RR.\n"
            f"Без воды, только практика. Используй HTML-теги <b> и <code> если нужно."
        )
        resp = client.models.generate_content(
            model="gemini-3.6-flash",
            contents=prompt,
        )
        text = (resp.text or "").strip()
        if text:
            return f"🤖 <b>AI подсказка:</b>\n{text}"
    except Exception as e:
        log.warning(f"Gemini TP/SL error: {e}")
    return "📍 <i>AI временно недоступен</i>"


def tradingview_url(instrument_key: str) -> str:
    """Ссылка на TradingView для инструмента."""
    info = AVAILABLE_INSTRUMENTS.get(instrument_key, {})
    kind = info.get("kind", "")
    ticker = info.get("ticker", "")
    label = info.get("label", "")

    if kind == "crypto":
        # BTC/USDT → BINANCE:BTCUSDT (самый популярный)
        base = ticker.replace("/", "")
        return f"https://www.tradingview.com/chart/?symbol=BINANCE:{base}"
    if kind == "crypto_perp":
        base = ticker.replace("USDT", "USDT")
        return f"https://www.tradingview.com/chart/?symbol=BINANCE:{base}.P"
    if kind == "forex":
        # EURUSD=X → FX:EURUSD
        sym = ticker.replace("=X", "").replace("=F", "")
        if "GC" in ticker:
            return "https://www.tradingview.com/chart/?symbol=COMEX:GC1!"
        if "SI" in ticker:
            return "https://www.tradingview.com/chart/?symbol=COMEX:SI1!"
        return f"https://www.tradingview.com/chart/?symbol=FX:{sym}"
    if kind == "equity":
        return f"https://www.tradingview.com/chart/?symbol=NASDAQ:{ticker}"
    return "https://www.tradingview.com/"


def build_alert_keyboard(instrument_key: str) -> dict:
    """Кнопки в одну линию — как синие ссылки в примере."""
    tv = tradingview_url(instrument_key)
    return {
        "inline_keyboard": [
            [
                {"text": "Открыть в TradingView", "url": tv},
                {"text": "Настройки сетапа", "callback_data": "open_smc_settings"},
            ]
        ]
    }


# =========================================================================
# TELEGRAM + CHARTS
# =========================================================================
def _style_candles(ax, df):
    """Свечи в стиле Setup Hunter: белые / серо-синие на почти чёрном фоне."""
    from matplotlib.patches import Rectangle

    UP = "#E8E8E8"       # почти белый (бычья)
    DOWN = "#5B6B7A"     # серо-синий (медвежья)
    WICK_UP = "#C8C8C8"
    WICK_DOWN = "#4A5560"
    BG = "#0B0E14"       # почти чёрный
    GRID = "#161B22"
    SPINE = "#1C2128"
    TICK = "#6B7280"

    for i, (_, row) in enumerate(df.iterrows()):
        o, h, l, c = float(row["Open"]), float(row["High"]), float(row["Low"]), float(row["Close"])
        is_up = c >= o
        body_color = UP if is_up else DOWN
        wick_color = WICK_UP if is_up else WICK_DOWN
        ax.plot([i, i], [l, h], color=wick_color, linewidth=1.0, solid_capstyle="round", zorder=2)
        body_bottom = min(o, c)
        body_h = abs(c - o)
        if body_h < (h - l) * 0.015:
            body_h = max((h - l) * 0.015, 1e-12)
        ax.add_patch(
            Rectangle(
                (i - 0.32, body_bottom), 0.64, body_h,
                facecolor=body_color, edgecolor=body_color, linewidth=0, zorder=3,
            )
        )
    ax.set_xlim(-1, len(df))
    ax.set_facecolor(BG)
    ax.tick_params(colors=TICK, labelsize=7)
    for spine in ax.spines.values():
        spine.set_color(SPINE)
    ax.yaxis.tick_right()
    ax.grid(True, color=GRID, linewidth=0.4, alpha=0.8)


def _draw_zone(ax, df, top, bottom, color="#8A7A55", alpha=0.40):
    """Зона FVG — приглушённый коричнево-бежевый как в примере."""
    if top is None or bottom is None:
        return
    y0, y1 = min(float(top), float(bottom)), max(float(top), float(bottom))
    ax.axhspan(y0, y1, facecolor=color, alpha=alpha, zorder=1, edgecolor="none")


def render_setup_chart(df_4h, df_15m, label: str, filename: str, zone_4h=None, zone_15m=None):
    """
    Multi-panel как в Setup Hunter:
      сверху большой 4H, снизу два маленьких (1H-подобный + 15M).
    zone_* = (top, bottom) или None.
    """
    import matplotlib.pyplot as plt
    from matplotlib.gridspec import GridSpec

    # Ресемплим 15m → ~1H для среднего панели
    df_1h = None
    if df_15m is not None and len(df_15m) >= 20:
        try:
            df_1h = (
                df_15m.resample("1h")
                .agg({"Open": "first", "High": "max", "Low": "min", "Close": "last", "Volume": "sum"})
                .dropna()
            )
        except Exception:
            df_1h = df_15m.copy()

    fig = plt.figure(figsize=(11, 9), facecolor="#0B0E14")
    gs = GridSpec(2, 2, figure=fig, height_ratios=[2.2, 1.0], hspace=0.28, wspace=0.12)

    # --- 4H (верх на всю ширину) ---
    ax4 = fig.add_subplot(gs[0, :])
    d4 = df_4h.tail(48) if df_4h is not None else df_15m.tail(48)
    _style_candles(ax4, d4)
    if zone_4h:
        _draw_zone(ax4, d4, zone_4h[0], zone_4h[1])
    ax4.set_title(f"{label} · H4", color="#d1d4dc", fontsize=11, loc="left", pad=6)
    ax4.set_xticks([])

    # --- H1 (низ-лево) ---
    ax1 = fig.add_subplot(gs[1, 0])
    d1 = (df_1h.tail(40) if df_1h is not None else df_15m.tail(40))
    _style_candles(ax1, d1)
    if zone_15m:
        _draw_zone(ax1, d1, zone_15m[0], zone_15m[1])
    ax1.set_title(f"{label} · H1", color="#d1d4dc", fontsize=9, loc="left", pad=4)
    ax1.set_xticks([])

    # --- 15M (низ-право) ---
    ax15 = fig.add_subplot(gs[1, 1])
    d15 = df_15m.tail(50)
    _style_candles(ax15, d15)
    if zone_15m:
        _draw_zone(ax15, d15, zone_15m[0], zone_15m[1])
    ax15.set_title(f"{label} · M15", color="#d1d4dc", fontsize=9, loc="left", pad=4)
    ax15.set_xticks([])

    fig.savefig(filename, dpi=130, bbox_inches="tight", facecolor="#0B0E14", edgecolor="none")
    plt.close(fig)


def render_chart(df, title, filename, max_bars=50):
    """Fallback одиночный график (для совместимости)."""
    render_setup_chart(df, df, title, filename)


def send_photo_with_caption(photo_path, caption, reply_markup=None, message_thread_id=None):
    """Одно фото + caption + кнопки."""
    data = {
        "chat_id": CHAT_ID,
        "caption": caption,
        "parse_mode": "HTML",
    }
    if message_thread_id is not None:
        data["message_thread_id"] = str(message_thread_id)
    if reply_markup is not None:
        data["reply_markup"] = json.dumps(reply_markup)
    with open(photo_path, "rb") as f:
        files = {"photo": f}
        return tg_post("sendPhoto", files=files, data=data)


def tg_post(method, payload=None, files=None, data=None):
    url = f"{API_URL}/{method}"
    try:
        if files:
            resp = requests.post(url, data=data, files=files, timeout=25)
        else:
            resp = requests.post(url, json=payload, timeout=15)
        if resp.status_code != 200:
            log.warning(f"Telegram {method} error {resp.status_code}: {resp.text[:300]}")
        return resp.json()
    except Exception as e:
        log.error(f"Telegram {method} exception: {e}")
        return None


def send_message(chat_id, text, reply_markup=None, message_thread_id=None):
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
    if reply_markup is not None:
        payload["reply_markup"] = reply_markup
    if message_thread_id is not None:
        payload["message_thread_id"] = message_thread_id
    return tg_post("sendMessage", payload=payload)


def edit_message_text(chat_id, message_id, text, reply_markup=None):
    payload = {
        "chat_id": chat_id,
        "message_id": message_id,
        "text": text,
        "parse_mode": "HTML",
    }
    if reply_markup is not None:
        payload["reply_markup"] = reply_markup
    return tg_post("editMessageText", payload=payload)


def edit_message_reply_markup(chat_id, message_id, reply_markup):
    payload = {"chat_id": chat_id, "message_id": message_id, "reply_markup": reply_markup}
    return tg_post("editMessageReplyMarkup", payload=payload)


def answer_callback_query(callback_query_id, text=None, show_alert=False):
    payload = {"callback_query_id": callback_query_id}
    if text:
        payload["text"] = text
        payload["show_alert"] = show_alert
    return tg_post("answerCallbackQuery", payload=payload)


def send_telegram_media_group(photos, caption, message_thread_id=None):
    media = []
    files = {}
    for i, photo_path in enumerate(photos):
        attach_name = f"photo_{i}"
        media_item = {"type": "photo", "media": f"attach://{attach_name}"}
        if i == 0:
            media_item["caption"] = caption
            media_item["parse_mode"] = "HTML"
        media.append(media_item)
        files[attach_name] = open(photo_path, "rb")

    data = {"chat_id": CHAT_ID, "media": json.dumps(media)}
    if message_thread_id is not None:
        data["message_thread_id"] = str(message_thread_id)
    try:
        tg_post("sendMediaGroup", files=files, data=data)
    finally:
        for f in files.values():
            f.close()


# =========================================================================
# КЛАВИАТУРЫ (без изменений)
# =========================================================================
GLOBAL_SETUP_TEXT = (
    "🎯 <b>Панель управления</b>\n\n"
    "Здесь можно включить/выключить сканирование целиком, настроить стратегии "
    "и выбрать инструменты для наблюдения."
)


def build_global_setup_keyboard(chat_id):
    s = get_user_settings(chat_id)
    scan_label = "🟢 Сканирование: включено" if s["scanning_enabled"] else "🔴 Сканирование: выключено"
    notify_mark = "✅" if s["notify_always"] else "⬜"
    rows = [
        [{"text": "Стратегии →", "callback_data": "goto_strategies"}],
        [{"text": "Инструменты →", "callback_data": "instr_categories"}],
        [{"text": f"{notify_mark} Уведомлять всегда", "callback_data": "toggle_notify_always"}],
        [{"text": scan_label, "callback_data": "toggle_scanning"}],
    ]
    return {"inline_keyboard": rows}


def build_strategies_keyboard(chat_id):
    s = get_user_settings(chat_id)
    enabled = set(s["enabled_strategies"])
    rows = []
    for sid, info in STRATEGIES.items():
        mark = "✅" if sid in enabled else "⬜"
        row = [{"text": f"{mark} {info['label']}", "callback_data": f"strat:{sid}"}]
        if info.get("configurable"):
            row.append({"text": "⚙️", "callback_data": f"stratcfg:{sid}"})
        rows.append(row)
    rows.append([{"text": "< Назад", "callback_data": "setup_back"}])
    return {"inline_keyboard": rows}


SMC_SETUP_TEXT = (
    "● <b>4H Trigger + 15M FVG</b>\n\n"
    "Multi-timeframe setup · 4H-триггер, подтверждённый 15M-имбалансом\n\n"
    "<b>Что это:</b>\n"
    "Следит за 4H-триггерами и присылает алерт, когда после триггера появляется свежий 15M Fair Value Gap в том же направлении.\n\n"
    "<b>Как это работает:</b>\n"
    "• Бычий 4H-триггер → бычий 15M FVG → лонг\n"
    "• Медвежий 4H-триггер → медвежий 15M FVG → шорт\n\n"
    "<b>Режимы:</b>\n"
    "• Sweep — после 4H Liquidity Sweep\n"
    "• FVG — после касания 4H Fair Value Gap\n\n"
    "<b>Важно:</b>\n"
    "Это алерт сетапа. Торгуйте только когда общий рыночный контекст поддерживает сетап."
)


def build_smc_cfg_keyboard(chat_id):
    s = get_user_settings(chat_id)
    enabled = "smc_sweep_fvg" in s.get("enabled_strategies", [])
    sweep_mark = "✅" if s.get("smc_trigger_sweep", True) else "⬜"
    fvg_mark = "✅" if s.get("smc_trigger_fvg", True) else "⬜"
    notify_mark = "✅" if s.get("notify_always") else "⬜"
    n_pairs = len(s.get("symbols", []))

    rows = [
        [
            {"text": "📋 Пример", "callback_data": "smc_example"},
            {"text": f"{notify_mark} Уведомлять всегда", "callback_data": "toggle_notify_always"},
        ],
        [{"text": "Триггер", "callback_data": "noop"}],
        [
            {"text": f"{sweep_mark} Sweep", "callback_data": "smctrig:sweep"},
            {"text": f"{fvg_mark} FVG", "callback_data": "smctrig:fvg"},
        ],
        [
            {"text": f"● Мои пары ({n_pairs})", "callback_data": "instr_categories"},
            {"text": "○ Свой список", "callback_data": "instr_categories"},
        ],
    ]
    if enabled:
        rows.append([{"text": "❌ Выключить сетап", "callback_data": "strat:smc_sweep_fvg"}])
    else:
        rows.append([{"text": "✅ Включить сетап", "callback_data": "strat:smc_sweep_fvg"}])
    rows.append([{"text": "< Назад", "callback_data": "goto_strategies"}])
    return {"inline_keyboard": rows}


def build_category_menu_keyboard(chat_id):
    settings = get_user_settings(chat_id)
    selected = set(settings["symbols"])
    rows = []
    for code, cat in INSTRUMENT_CATEGORIES.items():
        count_sel = sum(1 for k in cat["items"] if k in selected)
        rows.append(
            [
                {
                    "text": f"{cat['title']} ({count_sel}/{len(cat['items'])})",
                    "callback_data": f"cat:{code}:0",
                }
            ]
        )
    rows.append([{"text": "< Назад", "callback_data": "setup_back"}])
    return {"inline_keyboard": rows}


def build_category_page_keyboard(chat_id, code, page):
    settings = get_user_settings(chat_id)
    selected = set(settings["symbols"])
    items = list(INSTRUMENT_CATEGORIES[code]["items"].items())
    start = page * PAGE_SIZE
    page_items = items[start : start + PAGE_SIZE]
    rows = []
    for key, info in page_items:
        mark = "✅" if key in selected else "⬜"
        rows.append([{"text": f"{mark} {info['label']}", "callback_data": f"sym:{code}:{key}:{page}"}])
    nav = []
    if page > 0:
        nav.append({"text": "« Пред", "callback_data": f"pg:{code}:{page - 1}"})
    if start + PAGE_SIZE < len(items):
        nav.append({"text": "След »", "callback_data": f"pg:{code}:{page + 1}"})
    if nav:
        rows.append(nav)
    rows.append([{"text": "< Категории", "callback_data": "instr_categories"}])
    return {"inline_keyboard": rows}


EXAMPLE_TEXT = (
    "📋 <b>Пример алерта</b>\n\n"
    "🎯 <b>XAUUSD_GCF</b>\n"
    "<b>Лонг сформирован</b>\n"
    "Триггер: Sweep + FVG\n"
    "Время: 08:35 UTC\n\n"
    "К алерту прикладываются два графика: контекст 4H и точка входа 15M."
)


# =========================================================================
# КОМАНДЫ И КНОПКИ
# =========================================================================
def handle_command(chat_id, text, thread_id=None):
    text = text.strip()
    if text in ("/start", "/help"):
        send_message(
            chat_id,
            "🎯 <b>Market Setup Hunter</b>\n\n"
            "/setup — панель управления (сканирование, стратегии, инструменты)\n"
            "/strategies — выбрать активные стратегии\n"
            "/instruments — выбрать инструменты для наблюдения\n"
            "/status — текущие настройки\n"
            "/example — пример алерта\n"
            "/testalert — искусственный алерт (тест UI + AI + кнопки)\n"
            "/whereami — показать chat_id и thread_id этой темы\n\n"
            f"<i>Активная крипто-биржа: {_active_exchange_id}</i>",
            message_thread_id=thread_id,
        )
    elif text == "/setup":
        send_message(
            chat_id,
            GLOBAL_SETUP_TEXT,
            reply_markup=build_global_setup_keyboard(chat_id),
            message_thread_id=thread_id,
        )
    elif text == "/strategies":
        send_message(
            chat_id,
            "Выбери активные стратегии (можно несколько):",
            reply_markup=build_strategies_keyboard(chat_id),
            message_thread_id=thread_id,
        )
    elif text == "/instruments":
        send_message(
            chat_id,
            "Выбери категорию инструментов:",
            reply_markup=build_category_menu_keyboard(chat_id),
            message_thread_id=thread_id,
        )
    elif text == "/whereami":
        send_message(
            chat_id,
            f"<b>chat_id:</b> <code>{chat_id}</code>\n"
            f"<b>thread_id:</b> <code>{thread_id if thread_id is not None else '(нет — это General/личка)'}</code>",
            message_thread_id=thread_id,
        )
    elif text == "/example":
        send_message(chat_id, EXAMPLE_TEXT, message_thread_id=thread_id)
    elif text == "/testalert":
        try:
            symbol_key = "CR_BTC"
            label = AVAILABLE_INSTRUMENTS.get(symbol_key, {}).get("label", "BTC/USDT")
            df_4h, df_15m, _ = fetch_pair_data(symbol_key)
            if df_15m is None or len(df_15m) < 10:
                send_message(chat_id, "⚠️ Нет данных по BTC. Попробуй позже.", message_thread_id=thread_id)
                return
            last_close = float(df_15m["Close"].iloc[-1])
            try:
                high_low = df_15m["High"] - df_15m["Low"]
                high_close = (df_15m["High"] - df_15m["Close"].shift()).abs()
                low_close = (df_15m["Low"] - df_15m["Close"].shift()).abs()
                tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
                atr = float(tr.rolling(14).mean().iloc[-1])
            except Exception:
                atr = last_close * 0.01

            zone_15m = zone_4h = None
            try:
                fvgs = find_fvg(df_15m)
                if fvgs:
                    zone_15m = (fvgs[-1]["top"], fvgs[-1]["bottom"])
                if df_4h is not None:
                    fvgs4 = find_fvg(df_4h)
                    if fvgs4:
                        zone_4h = (fvgs4[-1]["top"], fvgs4[-1]["bottom"])
            except Exception:
                pass

            img_path = "test_setup.png"
            render_setup_chart(
                df_4h if df_4h is not None else df_15m,
                df_15m,
                label,
                img_path,
                zone_4h=zone_4h,
                zone_15m=zone_15m,
            )

            ai_hint = get_ai_tp_sl(label, "Лонг", "FVG (тест)", last_close, atr)
            caption = (
                f"● <b>{label}</b> · SMC: 4H Sweep/FVG + 15M FVG <i>(ТЕСТ)</i>\n"
                f"<b>Лонг-сетап сформирован</b>\n"
                f"Триггер: FVG\n"
                f"Время алерта: {datetime.now(timezone.utc).strftime('%H:%M UTC')}\n"
                f"Цена: <code>{last_close:.5g}</code>\n\n"
                f"{ai_hint}"
            )
            send_photo_with_caption(
                img_path,
                caption,
                reply_markup=build_alert_keyboard(symbol_key),
                message_thread_id=SCREENER_TOPIC_ID,
            )
            send_message(chat_id, "✅ Тестовый алерт отправлен в тему «Скринер»", message_thread_id=thread_id)
            if os.path.exists(img_path):
                try:
                    os.remove(img_path)
                except Exception:
                    pass
        except Exception as e:
            send_message(chat_id, f"❌ Ошибка теста: {e}", message_thread_id=thread_id)
            log.error(f"/testalert error: {e}\n{traceback.format_exc()}")
    elif text == "/status":
        s = get_user_settings(chat_id)
        strat_txt = "\n".join(
            f"• {'✅' if sid in s['enabled_strategies'] else '❌'} {info['label']}"
            for sid, info in STRATEGIES.items()
        )
        symbols_txt = (
            "\n".join(
                f"• {AVAILABLE_INSTRUMENTS[k]['label']}"
                for k in s["symbols"]
                if k in AVAILABLE_INSTRUMENTS
            )
            or "(ничего не выбрано)"
        )
        send_message(
            chat_id,
            f"<b>Сканирование:</b> {'включено 🟢' if s['scanning_enabled'] else 'выключено 🔴'}\n"
            f"<b>Уведомлять всегда:</b> {'да' if s['notify_always'] else 'нет'}\n"
            f"<b>Крипто-биржа:</b> {_active_exchange_id}\n\n"
            f"<b>Стратегии:</b>\n{strat_txt}\n\n"
            f"<b>Инструменты ({len(s['symbols'])}):</b>\n{symbols_txt}",
            message_thread_id=thread_id,
        )


def handle_callback(callback_query):
    chat_id = str(callback_query["message"]["chat"]["id"])
    message_id = callback_query["message"]["message_id"]
    data = callback_query["data"]
    callback_id = callback_query["id"]

    if data == "setup_back":
        edit_message_text(chat_id, message_id, GLOBAL_SETUP_TEXT, reply_markup=build_global_setup_keyboard(chat_id))
        answer_callback_query(callback_id)
    elif data == "goto_strategies":
        edit_message_text(
            chat_id,
            message_id,
            "Выбери активные стратегии (можно несколько):",
            reply_markup=build_strategies_keyboard(chat_id),
        )
        answer_callback_query(callback_id)
    elif data == "toggle_scanning":
        toggle_bool_setting(chat_id, "scanning_enabled")
        edit_message_reply_markup(chat_id, message_id, build_global_setup_keyboard(chat_id))
        answer_callback_query(callback_id)
    elif data.startswith("strat:"):
        sid = data.split(":", 1)[1]
        toggle_strategy(chat_id, sid)
        # Если нажали из панели SMC — обновляем её, иначе список стратегий
        try:
            edit_message_text(
                chat_id, message_id, SMC_SETUP_TEXT, reply_markup=build_smc_cfg_keyboard(chat_id)
            )
        except Exception:
            edit_message_reply_markup(chat_id, message_id, build_strategies_keyboard(chat_id))
        answer_callback_query(callback_id)
    elif data.startswith("stratcfg:") or data == "open_smc_settings":
        # Всегда отправляем НОВОЕ сообщение — edit на фото-алерте не работает
        thread_id = callback_query.get("message", {}).get("message_thread_id")
        send_message(
            chat_id,
            SMC_SETUP_TEXT,
            reply_markup=build_smc_cfg_keyboard(chat_id),
            message_thread_id=thread_id,
        )
        answer_callback_query(callback_id)
    elif data.startswith("smctrig:"):
        which = data.split(":", 1)[1]
        key = "smc_trigger_sweep" if which == "sweep" else "smc_trigger_fvg"
        toggle_bool_setting(chat_id, key)
        edit_message_reply_markup(chat_id, message_id, build_smc_cfg_keyboard(chat_id))
        answer_callback_query(callback_id)
    elif data == "smc_example":
        answer_callback_query(callback_id, text="Пример алерта — смотри /example", show_alert=True)
    elif data == "noop":
        answer_callback_query(callback_id)
    elif data == "toggle_notify_always":
        # уже есть выше, но если пришли из SMC-панели — обновим её
        toggle_bool_setting(chat_id, "notify_always")
        try:
            edit_message_reply_markup(chat_id, message_id, build_smc_cfg_keyboard(chat_id))
        except Exception:
            edit_message_reply_markup(chat_id, message_id, build_global_setup_keyboard(chat_id))
        answer_callback_query(callback_id)
    elif data == "instr_categories":
        edit_message_text(
            chat_id,
            message_id,
            "Выбери категорию инструментов:",
            reply_markup=build_category_menu_keyboard(chat_id),
        )
        answer_callback_query(callback_id)
    elif data.startswith("cat:") or data.startswith("pg:"):
        _, code, page = data.split(":")
        page = int(page)
        cat_title = INSTRUMENT_CATEGORIES[code]["title"]
        edit_message_text(
            chat_id,
            message_id,
            f"Категория: <b>{cat_title}</b>",
            reply_markup=build_category_page_keyboard(chat_id, code, page),
        )
        answer_callback_query(callback_id)
    elif data.startswith("sym:"):
        _, code, symbol_key, page = data.split(":")
        toggle_symbol(chat_id, symbol_key)
        edit_message_reply_markup(
            chat_id, message_id, build_category_page_keyboard(chat_id, code, int(page))
        )
        answer_callback_query(callback_id)
    else:
        answer_callback_query(callback_id)


def run_telegram_polling():
    offset = get_update_offset()
    log.info("Telegram polling started")
    while True:
        try:
            resp = requests.get(
                f"{API_URL}/getUpdates",
                params={"offset": offset, "timeout": 25},
                timeout=35,
            )
            data = resp.json()
            if not data.get("ok"):
                time.sleep(3)
                continue
            for update in data.get("result", []):
                offset = update["update_id"] + 1
                set_update_offset(offset)
                if "message" in update and "text" in update["message"]:
                    chat_id = str(update["message"]["chat"]["id"])
                    thread_id = update["message"].get("message_thread_id")
                    handle_command(chat_id, update["message"]["text"], thread_id=thread_id)
                elif "callback_query" in update:
                    handle_callback(update["callback_query"])
        except Exception as e:
            log.error(f"Polling error: {e}")
            time.sleep(5)


# =========================================================================
# СКАНИРОВАНИЕ
# =========================================================================
def process_pair(symbol_key, df_4h, df_15m, settings):
    label = AVAILABLE_INSTRUMENTS[symbol_key]["label"]

    if df_4h is None or df_15m is None or len(df_15m) < 25:
        market_data[symbol_key] = f"{label}: нет данных"
        log.debug(f"{symbol_key}: недостаточно данных (4h={df_4h is not None}, 15m bars={0 if df_15m is None else len(df_15m)})")
        return

    fired_statuses = []

    for sid in settings["enabled_strategies"]:
        strat = STRATEGIES.get(sid)
        if not strat:
            continue
        try:
            result = strat["detect"](df_4h, df_15m, settings)
        except Exception as e:
            log.error(f"Ошибка стратегии {sid} на {symbol_key}: {e}")
            continue
        if result is None:
            continue

        alert_key = f"{symbol_key}:{sid}"
        signal_time_str = result["time"].isoformat()
        already_sent = get_last_alert_time(alert_key) == signal_time_str
        if already_sent and not settings["notify_always"]:
            fired_statuses.append(f"{strat['label']}: уже отправлено")
            continue

        direction = result["direction"]
        trigger = result["trigger"]
        fired_statuses.append(f"🔥 {strat['label']}: {direction}")

        img_path = f"{symbol_key}_{sid}_setup.png"
        try:
            last_close = float(df_15m["Close"].iloc[-1])
            try:
                high_low = df_15m["High"] - df_15m["Low"]
                high_close = (df_15m["High"] - df_15m["Close"].shift()).abs()
                low_close = (df_15m["Low"] - df_15m["Close"].shift()).abs()
                tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
                atr = float(tr.rolling(14).mean().iloc[-1])
            except Exception:
                atr = last_close * 0.008

            # Зоны для подсветки (примерная оценка по последним FVG)
            zone_15m = None
            zone_4h = None
            try:
                fvgs = find_fvg(df_15m)
                if fvgs:
                    last_f = fvgs[-1]
                    zone_15m = (last_f["top"], last_f["bottom"])
                fvgs4 = find_fvg(df_4h)
                if fvgs4:
                    last_f4 = fvgs4[-1]
                    zone_4h = (last_f4["top"], last_f4["bottom"])
            except Exception:
                pass

            render_setup_chart(
                df_4h, df_15m, label, img_path, zone_4h=zone_4h, zone_15m=zone_15m
            )

            ai_hint = get_ai_tp_sl(label, direction, trigger, last_close, atr)
            dir_label = "Лонг-сетап" if direction == "Лонг" else "Шорт-сетап"

            caption = (
                f"● <b>{label}</b> · {strat['label']}\n"
                f"<b>{dir_label} сформирован</b>\n"
                f"Триггер: {trigger}\n"
                f"Время алерта: {datetime.now(timezone.utc).strftime('%H:%M UTC')}\n"
                f"Цена: <code>{last_close:.5g}</code>\n\n"
                f"{ai_hint}"
            )

            send_photo_with_caption(
                img_path,
                caption,
                reply_markup=build_alert_keyboard(symbol_key),
                message_thread_id=SCREENER_TOPIC_ID,
            )
            set_last_alert_time(alert_key, signal_time_str)
            log.info(f"✅ Алерт по {symbol_key} ({sid}) отправлен в тему {SCREENER_TOPIC_ID}")
        except Exception as e:
            log.error(f"Ошибка отправки алерта {symbol_key}: {e}\n{traceback.format_exc()}")
        finally:
            if os.path.exists(img_path):
                try:
                    os.remove(img_path)
                except Exception:
                    pass

    market_data[symbol_key] = (
        f"{label}: " + (" | ".join(fired_statuses) if fired_statuses else "ожидание сетапа")
    )


def analyze_and_notify():
    log.info("=== Сканирование рынка ===")
    settings = get_user_settings(CHAT_ID)

    if not settings["scanning_enabled"]:
        for key in settings["symbols"]:
            if key in AVAILABLE_INSTRUMENTS:
                market_data[key] = f"{AVAILABLE_INSTRUMENTS[key]['label']}: сканирование выключено"
        market_data["last_update"] = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
        return

    if not settings["enabled_strategies"]:
        for key in settings["symbols"]:
            if key in AVAILABLE_INSTRUMENTS:
                market_data[key] = f"{AVAILABLE_INSTRUMENTS[key]['label']}: нет активных стратегий"
        market_data["last_update"] = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
        return

    for symbol_key in settings["symbols"]:
        if symbol_key not in AVAILABLE_INSTRUMENTS:
            continue
        label = AVAILABLE_INSTRUMENTS[symbol_key]["label"]
        market_data[symbol_key] = f"{label}: сканирование..."
        try:
            df_4h, df_15m, skip_reason = fetch_pair_data(symbol_key)
            if skip_reason == "market_closed":
                market_data[symbol_key] = f"{label}: рынок закрыт"
                continue
            if df_4h is None or df_15m is None:
                market_data[symbol_key] = f"{label}: нет данных"
                log.warning(f"{symbol_key}: данные не получены")
                continue
            process_pair(symbol_key, df_4h, df_15m, settings)
        except Exception as e:
            market_data[symbol_key] = f"{label}: ошибка ({e})"
            log.error(f"Ошибка {symbol_key}: {e}\n{traceback.format_exc()}")

    for stale_key in list(market_data.keys()):
        if stale_key != "last_update" and stale_key not in settings["symbols"]:
            del market_data[stale_key]

    market_data["last_update"] = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
    log.info(f"Сканирование завершено. Биржа: {_active_exchange_id}")


def run_scanner_background():
    time.sleep(5)
    while True:
        try:
            analyze_and_notify()
        except Exception as e:
            log.error(f"Scanner error: {e}")
        time.sleep(SCAN_INTERVAL_SECONDS)


# =========================================================================
# FLASK
# =========================================================================
@app.route("/api/status")
def get_status():
    return jsonify(market_data)


@app.route("/")
def home():
    return render_template("index.html")


# =========================================================================
# СТАРТ
# =========================================================================
init_db()

log.info(f"CHAT_ID={CHAT_ID}, SCREENER_TOPIC={SCREENER_TOPIC_ID}, NEWS_TOPIC={NEWS_TOPIC_ID}, AI_TOPIC={AI_TOPIC_ID}")
log.info(f"Крипто-биржа: {_active_exchange_id}, инструментов крипты: {len(CR_INSTRUMENTS)}")

threading.Thread(target=run_scanner_background, daemon=True).start()
threading.Thread(target=run_telegram_polling, daemon=True).start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
