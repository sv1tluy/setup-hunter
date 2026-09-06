"""
Market Setup Hunter — Trading Hub Bot
=====================================
Темы:
  Скринер      (thread 2)
  Новости      (thread 3)
  Журнал       (thread 4) — скипаем
  ИИ хелпер    (thread 5)
  Обзор рынка  (thread 6, MARKET_TOPIC_ID)

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
    MSK_TZ = ZoneInfo("Europe/Moscow")
except Exception:
    NY_TZ = None
    MSK_TZ = timezone(timedelta(hours=3))

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
MARKET_TOPIC_ID = int(os.environ.get("MARKET_TOPIC_ID", "6"))
OVERVIEW_TZ = os.environ.get("OVERVIEW_TZ", "Europe/Moscow")
OVERVIEW_MORNING_HOUR = int(os.environ.get("OVERVIEW_MORNING_HOUR", "8"))
OVERVIEW_WEEKLY_HOUR = int(os.environ.get("OVERVIEW_WEEKLY_HOUR", "10"))

if not BOT_TOKEN:
    raise RuntimeError("Не задан BOT_TOKEN")

SCAN_INTERVAL_SECONDS = int(os.environ.get("SCAN_INTERVAL_SECONDS", "300"))
API_URL = f"https://api.telegram.org/bot{BOT_TOKEN}"
DB_PATH = os.environ.get("ALERTS_DB_PATH", "bot_state.db")

# Меню команд (появляется при вводе «/» в Telegram)
BOT_COMMANDS = [
    {"command": "start", "description": "Старт / справка"},
    {"command": "help", "description": "Все команды"},
    {"command": "setup", "description": "Панель управления"},
    {"command": "strategies", "description": "Выбрать стратегии"},
    {"command": "instruments", "description": "Выбрать инструменты"},
    {"command": "status", "description": "Текущие настройки"},
    {"command": "testalert", "description": "Тестовый алерт"},
    {"command": "news", "description": "Новости Forex Factory"},
    {"command": "brief", "description": "Утренний обзор рынка"},
    {"command": "weekplan", "description": "Торговый план на неделю"},
    {"command": "risk", "description": "Риск-профиль / депозит / prop"},
    {"command": "deposit", "description": "Задать депозит: /deposit 10000"},
    {"command": "riskpct", "description": "Риск %: /riskpct 1"},
    {"command": "prop", "description": "Prop: /prop 50000 5 10"},
    {"command": "personal", "description": "Режим личного депозита"},
    {"command": "lot", "description": "Лот: /lot BTC entry SL"},
    {"command": "model", "description": "Выбрать модель ИИ"},
    {"command": "img", "description": "Картинка: /img описание"},
    {"command": "example", "description": "Пример алерта"},
    {"command": "whereami", "description": "chat_id и thread_id"},
]
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
    must_include = ["BTC/USDT", "ETH/USDT", "SOL/USDT", "DOGE/USDT", "TON/USDT", "XRP/USDT", "BNB/USDT"]
    if _active_exchange_id == "fallback" or crypto_spot is None:
        fallback = [
            "BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT",
            "XRP/USDT", "ADA/USDT", "DOGE/USDT", "TON/USDT", "AVAX/USDT", "LINK/USDT",
        ]
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
        fallback = [
            "BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT",
            "XRP/USDT", "ADA/USDT", "DOGE/USDT", "TON/USDT", "AVAX/USDT", "LINK/USDT",
        ]
        return {f"CR_{s.split('/')[0]}": {"label": s, "kind": "crypto", "ticker": s} for s in fallback}


# Инициализация биржи и вселенной при старте
init_crypto_exchanges()
CR_INSTRUMENTS = build_crypto_universe()

AVAILABLE_INSTRUMENTS = {**FX_INSTRUMENTS, **MT_INSTRUMENTS, **CR_INSTRUMENTS, **NQ_INSTRUMENTS}

# Обзор рынка: все мажоры FX + металлы + ядро крипты
OVERVIEW_ASSET_KEYS = [
    "FX_EURUSD", "FX_GBPUSD", "FX_USDJPY", "FX_USDCHF",
    "FX_AUDUSD", "FX_USDCAD", "FX_NZDUSD",
    "MT_XAUUSD_GCF", "MT_XAGUSD_SIF",
    "CR_BTC", "CR_ETH", "CR_SOL", "CR_DOGE", "CR_TON",
    "CR_XRP", "CR_BNB", "CR_AVAX", "CR_LINK",
]
# новости FF: валюты этих активов
OVERVIEW_FF_CCY = {"USD", "EUR", "GBP", "JPY", "CHF", "AUD", "CAD", "NZD"}

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
    "enabled_strategies": ["smc_liq_bos_ob", "break_hold", "compression_break"],
    "smc_trigger_sweep": True,
    "smc_trigger_fvg": True,
    "notify_always": False,
    "scanning_enabled": True,
    "ai_model": "auto",  # auto | gemini | groq | mistral | openrouter | claude | grok
    # --- Риск / депозит / prop ---
    "account_type": "personal",  # personal | prop
    "balance": 0.0,              # размер депозита / prop
    "risk_pct": 1.0,             # % риска на сделку
    "prop_daily_loss_pct": 5.0,  # макс. дневной убыток %
    "prop_max_loss_pct": 10.0,   # макс. общий drawdown %
    "prop_phase": "challenge",   # challenge | funded
    "max_notional_pct": 8.0,     # макс. номинал позиции % от баланса (prop ETH 200k ≈ 6 ETH)
}

# API keys for AI models (env)
CLAUDE_API_KEY = os.environ.get("CLAUDE_API_KEY") or os.environ.get("ANTHROPIC_API_KEY")
GROK_API_KEY = os.environ.get("GROK_API_KEY") or os.environ.get("XAI_API_KEY")
GROQ_API_KEY = os.environ.get("GROQ_API_KEY")
MISTRAL_API_KEY = os.environ.get("MISTRAL_API_KEY")
OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY")

# Приблизительная стоимость 1 пункта / pip для расчёта лота (USD)
# Для крипты: $ на 1 USDT движения цены на 1 монету
PIP_VALUE_HINTS = {
    # forex standard lot ≈ $10 per pip for XXXUSD
    "FX_EURUSD": 10.0,
    "FX_GBPUSD": 10.0,
    "FX_USDJPY": 9.0,
    "FX_AUDUSD": 10.0,
    "FX_USDCAD": 9.0,
    "FX_USDCHF": 10.0,
    "FX_NZDUSD": 10.0,
    "MT_XAUUSD_GCF": 1.0,   # gold: ~$1 per 0.01 move per 0.01 lot roughly — упрощённо
    "EQ_US100": 1.0,
    "CR_BTC": 1.0,   # $1 на $1 движения × размер позиции в монетах
    "CR_ETH": 1.0,
    "CR_SOL": 1.0,
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


# =========================================================================
# РИСК / ЛОТ / PROP
# =========================================================================
def resolve_account_profile(settings: dict) -> dict:
    """
    Единый профиль счёта.
    Если account_type=prop — баланс и лимиты из /prop.
    Если personal, но balance=0 — всё равно пробуем prop-поля (на случай смешанных настроек).
    """
    at = settings.get("account_type") or "personal"
    bal = float(settings.get("balance") or 0)
    risk_pct = float(settings.get("risk_pct") or 1)
    daily_pct = float(settings.get("prop_daily_loss_pct") or 5)
    max_pct = float(settings.get("prop_max_loss_pct") or 10)
    phase = settings.get("prop_phase") or "challenge"

    # Если тип prop или баланс задан через /prop — считаем prop-режимом
    if at == "prop" or (bal > 0 and daily_pct > 0 and max_pct > 0 and at != "personal"):
        at = "prop" if settings.get("account_type") == "prop" else at

    if at == "prop" and bal <= 0:
        # prop без размера — нельзя считать
        return {
            "account_type": "prop",
            "balance": 0.0,
            "risk_pct": risk_pct,
            "daily_pct": daily_pct,
            "max_pct": max_pct,
            "phase": phase,
            "ok": False,
            "error": "Prop без размера. Задай: /prop 50000 5 10",
        }

    if bal <= 0:
        return {
            "account_type": at,
            "balance": 0.0,
            "risk_pct": risk_pct,
            "daily_pct": daily_pct,
            "max_pct": max_pct,
            "phase": phase,
            "ok": False,
            "error": "Нет баланса. /deposit 10000 или /prop 50000 5 10",
        }

    return {
        "account_type": at,
        "balance": bal,
        "risk_pct": risk_pct,
        "daily_pct": daily_pct,
        "max_pct": max_pct,
        "phase": phase,
        "ok": True,
        "error": "",
    }


def calc_position_size(
    balance: float,
    risk_pct: float,
    entry: float,
    stop_loss: float,
    instrument_key: str = "",
    account_type: str = "personal",
    prop_daily_loss_pct: float = 5.0,
    prop_max_loss_pct: float = 10.0,
    prop_phase: str = "challenge",
    max_notional_pct: float = 8.0,
) -> dict:
    """
    Размер позиции с учётом правил prop:
      • риск на сделку ≤ risk_pct от баланса
      • риск на сделку ≤ 25% дневного лимита (не слить daily за 1–2 сделки)
      • риск на сделку ≤ 10% от max DD (запас по общей просадке)
      • challenge: ещё консервативнее (20% daily)
    """
    result = {
        "ok": False,
        "risk_usd": 0.0,
        "risk_pct_used": 0.0,
        "sl_distance": 0.0,
        "size": 0.0,
        "size_label": "лот",
        "max_daily_loss_usd": 0.0,
        "max_total_loss_usd": 0.0,
        "rules_applied": [],
        "note": "",
    }
    if balance <= 0 or entry <= 0:
        result["note"] = "Задай баланс: /deposit или /prop"
        return result
    if risk_pct <= 0:
        risk_pct = 1.0

    sl_dist = abs(entry - stop_loss)
    if sl_dist <= 0:
        result["note"] = "SL должен отличаться от цены входа"
        return result

    # Базовый риск по % пользователя
    risk_usd = balance * (risk_pct / 100.0)
    result["rules_applied"].append(f"risk {risk_pct}% = ${risk_usd:.2f}")

    if account_type == "prop":
        max_daily = balance * (prop_daily_loss_pct / 100.0)
        max_total = balance * (prop_max_loss_pct / 100.0)
        result["max_daily_loss_usd"] = round(max_daily, 2)
        result["max_total_loss_usd"] = round(max_total, 2)

        # Доля дневного лимита на 1 сделку
        daily_share = 0.20 if prop_phase == "challenge" else 0.25
        cap_daily = max_daily * daily_share
        # Доля от max DD на 1 сделку (не больше 10% общей просадки)
        cap_dd = max_total * 0.10

        caps = [("daily×{:.0%}".format(daily_share), cap_daily), ("maxDD×10%", cap_dd)]
        for name, cap in caps:
            if risk_usd > cap > 0:
                result["rules_applied"].append(f"cap {name}: ${risk_usd:.2f}→${cap:.2f}")
                risk_usd = cap

        # Абсолютный пол: риск не больше дневного лимита целиком
        if risk_usd > max_daily:
            result["rules_applied"].append(f"hard daily cap ${max_daily:.2f}")
            risk_usd = max_daily

        result["note"] = (
            f"Prop {prop_phase}: daily −{prop_daily_loss_pct}% (${max_daily:.0f}) · "
            f"max DD −{prop_max_loss_pct}% (${max_total:.0f})"
        )

    risk_pct_used = (risk_usd / balance) * 100.0 if balance else 0
    result["risk_pct_used"] = round(risk_pct_used, 3)

    kind = AVAILABLE_INSTRUMENTS.get(instrument_key, {}).get("kind", "")
    label = AVAILABLE_INSTRUMENTS.get(instrument_key, {}).get("label", instrument_key or "?")

    if kind in ("crypto", "crypto_perp") or (instrument_key or "").startswith("CR_"):
        size = risk_usd / sl_dist
        # Потолок номинала: нельзя открыть 40 ETH на 200k prop
        max_notional = balance * max(0.5, float(max_notional_pct)) / 100.0
        max_size = max_notional / entry if entry > 0 else size
        if size > max_size:
            size = max_size
            risk_usd = size * sl_dist
            result["rules_applied"].append(f"cap notional {max_notional_pct}% → {size:.4g}")
        result.update({
            "ok": True,
            "risk_usd": round(risk_usd, 2),
            "sl_distance": round(sl_dist, 6),
            "size": round(size, 4),
            "size_label": "монет",
            "notional": round(size * entry, 2),
        })
    else:
        if "JPY" in label.upper():
            pip = 0.01
        elif "XAU" in label.upper() or "GOLD" in label.upper():
            pip = 0.1
        else:
            pip = 0.0001
        pips = sl_dist / pip
        pip_value_per_lot = PIP_VALUE_HINTS.get(instrument_key, 10.0)
        if pips <= 0:
            result["note"] = "Нулевая дистанция SL"
            return result
        lots = risk_usd / (pips * pip_value_per_lot)
        lots = max(0.01, round(lots, 2))
        result.update({
            "ok": True,
            "risk_usd": round(risk_usd, 2),
            "sl_distance": round(sl_dist, 6),
            "size": lots,
            "size_label": "лот",
            "pips": round(pips, 1),
        })

    return result


def format_risk_card(settings: dict) -> str:
    bal = float(settings.get("balance") or 0)
    risk = float(settings.get("risk_pct") or 1)
    at = settings.get("account_type", "personal")
    lines = [
        "💰 <b>Риск-профиль</b>\n",
        f"Тип: <b>{'Prop-счёт' if at == 'prop' else 'Личный депозит'}</b>",
        f"Баланс / Prop: <code>${bal:,.2f}</code>",
        f"Риск на сделку: <code>{risk}%</code> → <code>${bal * risk / 100:,.2f}</code>",
    ]
    if at == "prop":
        d = float(settings.get("prop_daily_loss_pct") or 5)
        m = float(settings.get("prop_max_loss_pct") or 10)
        phase = settings.get("prop_phase", "challenge")
        lines += [
            f"Фаза prop: <b>{phase}</b>",
            f"Дневной лимит убытка: <code>{d}%</code> (${bal * d / 100:,.2f})",
            f"Макс. просадка: <code>{m}%</code> (${bal * m / 100:,.2f})",
            f"Безопасный риск/сделку (≤25% дневного): <code>${bal * d / 100 * 0.25:,.2f}</code>",
        ]
    lines.append(
        "\nКоманды:\n"
        "<code>/deposit 10000</code> — баланс\n"
        "<code>/riskpct 1</code> — % риска\n"
        "<code>/prop 50000 5 10</code> — prop: размер, daily%, max%\n"
        "<code>/personal</code> — обычный депозит\n"
        "<code>/lot SYMBOL entry SL</code> — посчитать лот\n"
        "Пример: <code>/lot BTC 104000 101500</code>"
    )
    return "\n".join(lines)


def format_lot_result(calc: dict, entry: float, sl: float, direction: str = "") -> str:
    if not calc.get("ok"):
        return f"⚠️ {calc.get('note') or 'Не удалось посчитать'}"
    dir_s = f" ({direction})" if direction else ""
    lines = [
        f"📐 <b>Расчёт позиции{dir_s}</b>\n",
        f"Вход: <code>{entry}</code>",
        f"SL: <code>{sl}</code>  (дист. {calc['sl_distance']})",
        f"Риск $: <code>{calc['risk_usd']}</code>",
        f"Размер: <b>{calc['size']}</b> {calc['size_label']}",
    ]
    if "notional" in calc:
        lines.append(f"Номинал ≈ <code>${calc['notional']}</code>")
    if "pips" in calc:
        lines.append(f"До SL: ≈ {calc['pips']} пп")
    if calc.get("max_daily_loss_usd"):
        lines.append(
            f"Prop daily limit: ${calc['max_daily_loss_usd']:.0f} | "
            f"max DD: ${calc['max_total_loss_usd']:.0f}"
        )
    if calc.get("note"):
        lines.append(f"\n<i>{calc['note']}</i>")
    lines.append("\n⚠️ Это ориентир, не фин. совет. Проверь контракт спецификации брокера.")
    return "\n".join(lines)


def build_alert_risk_block(
    settings: dict,
    symbol_key: str,
    direction: str,
    entry: float,
    atr: float = None,
    sl_override: float = None,
) -> str:
    """
    Блок лота для алерта.
    Баланс берётся из /deposit или /prop (что задано).
    Prop: режет риск по daily / max DD правилам платформы.
    """
    profile = resolve_account_profile(settings)
    if not profile["ok"]:
        return (
            f"\n💰 <i>{profile['error']}</i>\n"
            "<code>/deposit 10000</code> или <code>/prop 50000 5 10</code>"
        )

    if atr is None or atr <= 0:
        atr = entry * 0.01

    if sl_override is not None:
        sl_px = float(sl_override)
    elif direction == "Лонг":
        sl_px = entry - 1.5 * atr
    else:
        sl_px = entry + 1.5 * atr

    calc = calc_position_size(
        balance=profile["balance"],
        risk_pct=profile["risk_pct"],
        entry=entry,
        stop_loss=sl_px,
        instrument_key=symbol_key,
        account_type=profile["account_type"],
        prop_daily_loss_pct=profile["daily_pct"],
        prop_max_loss_pct=profile["max_pct"],
        prop_phase=profile["phase"],
    )
    if not calc.get("ok"):
        return f"\n💰 <i>{calc.get('note') or 'не удалось посчитать лот'}</i>"

    size_s = f"{calc['size']:.4g}" if calc["size_label"] == "монет" else str(calc["size"])
    tag = "prop" if profile["account_type"] == "prop" else "деп"
    return (
        f"\n💰 SL <code>{sl_px:.5g}</code> · "
        f"<b>{size_s}</b> {calc['size_label']} · "
        f"риск ${calc['risk_usd']:.0f} ({tag})"
    )


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


# Минимальный интервал между алертами по одной паре+стратегии (минуты)
ALERT_COOLDOWN_MINUTES = int(os.environ.get("ALERT_COOLDOWN_MINUTES", "1440"))  # 24ч: один сетап = один алерт


def get_last_alert_info(alert_key: str) -> Optional[dict]:
    """Возвращает {signal_time, sent_at} или None."""
    with db_lock:
        conn = _get_conn()
        row = conn.execute(
            "SELECT signal_time, sent_at FROM sent_alerts WHERE alert_key = ?", (alert_key,)
        ).fetchone()
        conn.close()
    if not row:
        return None
    return {"signal_time": row[0], "sent_at": row[1]}


def get_last_alert_time(alert_key: str) -> Optional[str]:
    info = get_last_alert_info(alert_key)
    return info["signal_time"] if info else None


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


def should_skip_alert(alert_key: str, signal_time_str: str, notify_always: bool = False) -> bool:
    """
    True = не слать.
    Каждый конкретный сетап (setup_id) отправляется ровно один раз.
    Новый сетап = новый setup_id = новый алерт.
    """
    info = get_last_alert_info(alert_key)
    return info is not None


PAIR_DIR_LOCK_MINUTES = int(os.environ.get("PAIR_DIR_LOCK_MINUTES", "120"))


def pair_dir_lock_key(symbol_key: str, direction: str) -> str:
    return f"lock:{symbol_key}:{direction}"


def pair_dir_is_locked(symbol_key: str, direction: str) -> bool:
    info = get_last_alert_info(pair_dir_lock_key(symbol_key, direction))
    if not info:
        return False
    try:
        sent_at = datetime.fromisoformat(info["sent_at"])
        if sent_at.tzinfo is None:
            sent_at = sent_at.replace(tzinfo=timezone.utc)
        elapsed = (datetime.now(timezone.utc) - sent_at).total_seconds() / 60.0
        return elapsed < PAIR_DIR_LOCK_MINUTES
    except Exception:
        return True


def lock_pair_dir(symbol_key: str, direction: str):
    set_last_alert_time(pair_dir_lock_key(symbol_key, direction), datetime.now(timezone.utc).isoformat())


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


def get_meta(key: str, default: str = "") -> str:
    with db_lock:
        conn = _get_conn()
        row = conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
        conn.close()
    return row[0] if row else default


def set_meta(key: str, value: str):
    with db_lock:
        conn = _get_conn()
        conn.execute(
            "INSERT INTO meta (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
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
    """Возвращает dict {type, time} или None. time — свеча свипа (4H)."""
    if len(df) < 10:
        return None
    recent_high = df["High"].iloc[-10:-1].max()
    recent_low = df["Low"].iloc[-10:-1].min()
    current_high = df["High"].iloc[-1]
    current_low = df["Low"].iloc[-1]
    current_close = df["Close"].iloc[-1]
    t = df.index[-1]

    if current_high > recent_high and current_close < recent_high:
        return {"type": "Bearish Sweep", "time": t}
    if current_low < recent_low and current_close > recent_low:
        return {"type": "Bullish Sweep", "time": t}
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
            direction = "Bullish" if sweep["type"] == "Bullish Sweep" else "Bearish"
            active_triggers.append(("Sweep", direction, sweep["time"]))
    if settings.get("smc_trigger_fvg", True):
        fvg4h = check_4h_fvg_trigger(df_4h)
        if fvg4h:
            direction = "Bullish" if fvg4h["type"] == "Bullish FVG" else "Bearish"
            active_triggers.append(("FVG", direction, fvg4h["time"]))
    if not active_triggers:
        return None

    fvg_15m_list = find_fvg(df_15m)
    if not fvg_15m_list:
        return None
    last_fvg_15m = fvg_15m_list[-1]
    if df_15m.index.get_loc(last_fvg_15m["time"]) < len(df_15m) - 3:
        return None

    direction_15m = "Bullish" if last_fvg_15m["type"] == "Bullish FVG" else "Bearish"
    matching = [(label, t) for label, d, t in active_triggers if d == direction_15m]
    if not matching:
        return None

    direction_ru = "Лонг" if direction_15m == "Bullish" else "Шорт"
    labels = " + ".join(lab for lab, _ in matching)
    # ID сетапа = 4H-триггер (не 15M FVG). Один 4H-свип = один алерт.
    trigger_time = matching[0][1]
    setup_id = f"smc:{direction_ru}:{trigger_time.isoformat()}"
    return {
        "time": last_fvg_15m["time"],
        "direction": direction_ru,
        "trigger": labels,
        "setup_id": setup_id,
    }


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
        return {
            "time": last_time,
            "direction": "Лонг",
            "trigger": "BOS (пробой хая)",
            "setup_id": f"bos:long:{round(last_high, 5)}",
        }
    if last_close < last_low:
        return {
            "time": last_time,
            "direction": "Шорт",
            "trigger": "BOS (пробой лоя)",
            "setup_id": f"bos:short:{round(last_low, 5)}",
        }
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
            ob_t = df_15m.index[ob_index]
            return {
                "time": df_15m.index[-1],
                "direction": direction,
                "trigger": "Order Block retest",
                "setup_id": f"ob:{direction}:{ob_t.isoformat()}",
            }
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
        return {
            "time": df_15m.index[-1],
            "direction": "Лонг",
            "trigger": "EMA21 pullback (аптренд)",
            "setup_id": f"ema:Лонг:{df_4h.index[-1].isoformat()}",
        }
    if trend_down and last_high >= last_ema > last_close:
        return {
            "time": df_15m.index[-1],
            "direction": "Шорт",
            "trigger": "EMA21 pullback (даунтренд)",
            "setup_id": f"ema:Шорт:{df_4h.index[-1].isoformat()}",
        }
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
        t = df_15m.index[-1]
        return {
            "time": t,
            "direction": "Лонг",
            "trigger": "RSI выход из перепроданности",
            "setup_id": f"rsi:Лонг:{t.isoformat()}",
        }
    if prev_rsi > 70 >= last_rsi:
        t = df_15m.index[-1]
        return {
            "time": t,
            "direction": "Шорт",
            "trigger": "RSI выход из перекупленности",
            "setup_id": f"rsi:Шорт:{t.isoformat()}",
        }
    return None


def htf_structure_trend(df_4h) -> Optional[str]:
    """HH/HL = Лонг, LH/LL = Шорт, иначе None."""
    if df_4h is None or len(df_4h) < 20:
        return None
    highs, lows = find_swings(df_4h, lookback=2)
    if len(highs) < 2 or len(lows) < 2:
        return None
    hh = highs[-1][1] > highs[-2][1]
    hl = lows[-1][1] > lows[-2][1]
    lh = highs[-1][1] < highs[-2][1]
    ll = lows[-1][1] < lows[-2][1]
    if hh and hl:
        return "Лонг"
    if lh and ll:
        return "Шорт"
    return None


def _m15_atr(df, period=14) -> float:
    high_low = df["High"] - df["Low"]
    high_close = (df["High"] - df["Close"].shift()).abs()
    low_close = (df["Low"] - df["Close"].shift()).abs()
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    atr = float(tr.rolling(period).mean().iloc[-1])
    return atr if atr and atr > 0 else float(df["Close"].iloc[-1]) * 0.001


def _pair_currencies(symbol_key: str) -> set:
    info = AVAILABLE_INSTRUMENTS.get(symbol_key) or {}
    kind = info.get("kind") or ""
    if kind in ("crypto", "crypto_perp") or (symbol_key or "").startswith("CR_"):
        return set()
    label = (info.get("label") or symbol_key).upper().replace("USDT", "")
    tick = (info.get("ticker") or "").upper().replace("USDT", "")
    found = set()
    for ccy in ("USD", "EUR", "GBP", "JPY", "CHF", "AUD", "CAD", "NZD"):
        if ccy in label or ccy in tick or ccy in (symbol_key or "").upper().replace("USDT", ""):
            found.add(ccy)
    if "XAU" in label or "GC=" in tick or "GOLD" in label:
        found.update({"USD", "XAU"})
    if "XAG" in label or "SI=" in tick:
        found.update({"USD", "XAG"})
    return found


def news_blocks_pair(symbol_key: str, minutes: int = 20) -> bool:
    """True = рядом high-новость по валюте пары — сетап не шлём."""
    info = AVAILABLE_INSTRUMENTS.get(symbol_key) or {}
    if (info.get("kind") in ("crypto", "crypto_perp")) or (symbol_key or "").startswith("CR_"):
        return False
    try:
        events = fetch_forexfactory_events()
    except Exception:
        return False
    now = datetime.now(timezone.utc)
    ccys = _pair_currencies(symbol_key)
    if not ccys:
        return False
    for e in events:
        if e.get("impact") != "high" or e.get("datetime_utc") is None:
            continue
        cur = (e.get("currency") or "").upper()
        hits = cur in ccys or (cur == "USD" and (("USD" in ccys) or ("XAU" in ccys) or ("XAG" in ccys)))
        if not hits:
            continue
        delta = abs((e["datetime_utc"] - now).total_seconds()) / 60.0
        if delta <= minutes:
            return True
    return False


def detect_smc_liq_bos_ob(df_4h, df_15m, settings):
    """
    Чистый сетап: тренд H4 → sweep M15 (≥0.35 ATR) → BOS с импульсом → OB/FVG.
    Алерт только когда зона актуальна: свежий BOS или ретест.
    """
    trend = htf_structure_trend(df_4h)
    if trend is None or df_15m is None or len(df_15m) < 40:
        return None

    symbol_key = settings.get("_symbol") or ""
    if symbol_key and news_blocks_pair(symbol_key, minutes=20):
        return None

    highs, lows = find_swings(df_15m, lookback=3)
    if len(highs) < 2 or len(lows) < 2:
        return None

    atr = _m15_atr(df_15m)
    min_wick = atr * 0.35
    look = min(36, len(df_15m) - 2)
    is_long = trend == "Лонг"

    sweep_i = sweep_lvl = None
    for i in range(len(df_15m) - look, len(df_15m) - 1):
        t = df_15m.index[i]
        if is_long:
            prior = [lv for ts, lv in lows if ts < t]
            if not prior:
                continue
            lvl = prior[-1]
            wick = lvl - float(df_15m["Low"].iloc[i])
            if float(df_15m["Low"].iloc[i]) < lvl and float(df_15m["Close"].iloc[i]) > lvl and wick >= min_wick:
                sweep_i, sweep_lvl = i, lvl
        else:
            prior = [hv for ts, hv in highs if ts < t]
            if not prior:
                continue
            lvl = prior[-1]
            wick = float(df_15m["High"].iloc[i]) - lvl
            if float(df_15m["High"].iloc[i]) > lvl and float(df_15m["Close"].iloc[i]) < lvl and wick >= min_wick:
                sweep_i, sweep_lvl = i, lvl
    if sweep_i is None:
        return None

    t_sw = df_15m.index[sweep_i]
    if is_long:
        prior_lvl = [hv for ts, hv in highs if ts < t_sw]
    else:
        prior_lvl = [lv for ts, lv in lows if ts < t_sw]
    if not prior_lvl:
        return None
    bos_lvl = prior_lvl[-1]
    bos_i = None
    for j in range(sweep_i + 1, len(df_15m)):
        cl = float(df_15m["Close"].iloc[j])
        body = abs(float(df_15m["Close"].iloc[j]) - float(df_15m["Open"].iloc[j]))
        if is_long and cl > bos_lvl and body >= atr * 0.25:
            bos_i = j
            break
        if (not is_long) and cl < bos_lvl and body >= atr * 0.25:
            bos_i = j
            break
    if bos_i is None:
        return None
    # зона жива до ~8 часов после BOS
    if bos_i < len(df_15m) - 32:
        return None

    def _is_ob_candle(k: int) -> bool:
        bear = float(df_15m["Close"].iloc[k]) < float(df_15m["Open"].iloc[k])
        return bear if is_long else (not bear)

    ob = None
    # 1) свеча сразу перед свипом — классический OB
    for k in range(sweep_i - 1, max(0, sweep_i - 7), -1):
        if _is_ob_candle(k):
            ob = (float(df_15m["High"].iloc[k]), float(df_15m["Low"].iloc[k]))
            break
    # 2) иначе последняя противоположная внутри импульса sweep→BOS
    if ob is None:
        for k in range(bos_i - 1, sweep_i - 1, -1):
            if _is_ob_candle(k):
                ob = (float(df_15m["High"].iloc[k]), float(df_15m["Low"].iloc[k]))
                break
    want = "Bullish FVG" if is_long else "Bearish FVG"
    fvgs = [f for f in find_fvg(df_15m.iloc[sweep_i : bos_i + 1]) if f["type"] == want]
    if ob:
        zone_top, zone_bot, zname = ob[0], ob[1], "OB"
    elif fvgs:
        f = fvgs[-1]
        zone_top, zone_bot, zname = f["top"], f["bottom"], "FVG"
    else:
        return None

    last_low = float(df_15m["Low"].iloc[-1])
    last_high = float(df_15m["High"].iloc[-1])
    touching = last_low <= zone_top and last_high >= zone_bot
    fresh_bos = bos_i >= len(df_15m) - 4
    if not touching and not fresh_bos:
        return None

    stage = "ретест зоны" if touching else "лимитка в зону"
    setup_id = f"lsb:{trend}:{df_15m.index[sweep_i].isoformat()}:{df_15m.index[bos_i].isoformat()}"
    return {
        "time": df_15m.index[-1] if touching else df_15m.index[bos_i],
        "direction": trend,
        "trigger": f"Sweep+BOS+{zname} ({stage})",
        "setup_id": setup_id,
    }


def detect_break_hold(df_4h, df_15m, settings):
    """
    Пробой уровня + закреп: закрытие за свинг и удержание / ретест.
    По тренду H4, вынос ≥ 0.3 ATR, новость не режет.
    """
    trend = htf_structure_trend(df_4h)
    if trend is None or df_15m is None or len(df_15m) < 30:
        return None
    symbol_key = settings.get("_symbol") or ""
    if symbol_key and news_blocks_pair(symbol_key, minutes=20):
        return None
    highs, lows = find_swings(df_15m, lookback=3)
    if len(highs) < 2 or len(lows) < 2:
        return None
    atr = _m15_atr(df_15m)
    is_long = trend == "Лонг"
    lvl = highs[-1][1] if is_long else lows[-1][1]
    lvl_t = highs[-1][0] if is_long else lows[-1][0]
    # уровень не должен быть самой последней свечой
    if lvl_t >= df_15m.index[-3]:
        return None
    closes = df_15m["Close"]
    last_c = float(closes.iloc[-1])
    prev_c = float(closes.iloc[-2])
    last_o = float(df_15m["Open"].iloc[-1])
    body = abs(last_c - last_o)
    if body < atr * 0.20:
        return None
    if is_long:
        broke = last_c > lvl and (last_c - lvl) >= atr * 0.30
        held = prev_c > lvl and last_c > lvl
        retest = float(df_15m["Low"].iloc[-1]) <= lvl <= float(df_15m["High"].iloc[-1]) and last_c > lvl
    else:
        broke = last_c < lvl and (lvl - last_c) >= atr * 0.30
        held = prev_c < lvl and last_c < lvl
        retest = float(df_15m["Low"].iloc[-1]) <= lvl <= float(df_15m["High"].iloc[-1]) and last_c < lvl
    # только закреп или ретест — голый пробой дублирует BOS
    if not (held or retest):
        return None
    window = df_15m.iloc[-8:]
    if is_long and not (window["Close"] > lvl).any():
        return None
    if (not is_long) and not (window["Close"] < lvl).any():
        return None
    stage = "ретест" if retest else "закреп"
    setup_id = f"bh:{trend}:{lvl_t.isoformat()}:{round(lvl, 5)}"
    return {
        "time": df_15m.index[-1],
        "direction": trend,
        "trigger": f"Пробой+закреп ({stage})",
        "setup_id": setup_id,
    }


def detect_compression_break(df_4h, df_15m, settings):
    """
    Выход из сжатия: узкий диапазон 8–16 свечей M15, затем импульсный выход по тренду H4.
    """
    trend = htf_structure_trend(df_4h)
    if trend is None or df_15m is None or len(df_15m) < 40:
        return None
    symbol_key = settings.get("_symbol") or ""
    if symbol_key and news_blocks_pair(symbol_key, minutes=20):
        return None
    atr = _m15_atr(df_15m)
    box = df_15m.iloc[-16:-1]
    rng = float(box["High"].max() - box["Low"].min())
    if rng <= 0 or rng > atr * 1.35:
        return None  # не сжатие
    top, bot = float(box["High"].max()), float(box["Low"].min())
    last_c = float(df_15m["Close"].iloc[-1])
    last_o = float(df_15m["Open"].iloc[-1])
    body = abs(last_c - last_o)
    if body < atr * 0.30:
        return None
    is_long = trend == "Лонг"
    if is_long and last_c <= top:
        return None
    if (not is_long) and last_c >= bot:
        return None
    # выход должен быть заметным
    if is_long and (last_c - top) < atr * 0.15:
        return None
    if (not is_long) and (bot - last_c) < atr * 0.15:
        return None
    setup_id = f"cb:{trend}:{box.index[0].isoformat()}:{round(top,5)}:{round(bot,5)}"
    return {
        "time": df_15m.index[-1],
        "direction": trend,
        "trigger": "Выход из сжатия",
        "setup_id": setup_id,
    }


STRATEGIES = {
    "smc_liq_bos_ob": {
        "label": "SMC: Sweep + BOS + OB/FVG",
        "detect": detect_smc_liq_bos_ob,
        "configurable": False,
    },
    "break_hold": {
        "label": "Пробой + закреп (M15 по тренду H4)",
        "detect": detect_break_hold,
        "configurable": False,
    },
    "compression_break": {
        "label": "Выход из сжатия (M15)",
        "detect": detect_compression_break,
        "configurable": False,
    },

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
    # 5M / 15M — короткий период; 4H — из 60m
    if timeframe == "5M":
        interval, period = "5m", "5d"
    elif timeframe == "15M":
        interval, period = "15m", "5d"
    else:
        interval, period = "60m", "1mo"
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
    tf_map = {"5M": "5m", "15M": "15m", "4H": "4h"}
    if timeframe not in tf_map:
        return None
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
    tf_map = {"5M": "5m", "15M": "15m", "4H": "4h"}
    if timeframe not in tf_map:
        return None
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
    """Возвращает (df_4h, df_15m, df_5m, skip_reason)."""
    info = AVAILABLE_INSTRUMENTS[instrument_key]
    kind = info["kind"]
    ticker = info["ticker"]

    if kind == "forex":
        if not is_forex_market_open():
            return None, None, None, "market_closed"
        df_4h = get_yfinance_data(ticker, "4H")
        df_15m = get_yfinance_data(ticker, "15M")
        df_5m = get_yfinance_data(ticker, "5M")
    elif kind == "equity":
        if not is_us_equity_market_open():
            return None, None, None, "market_closed"
        df_4h = get_yfinance_data(ticker, "4H")
        df_15m = get_yfinance_data(ticker, "15M")
        df_5m = get_yfinance_data(ticker, "5M")
    elif kind == "crypto":
        df_4h = get_crypto_spot_data(ticker, "4H")
        df_15m = get_crypto_spot_data(ticker, "15M")
        df_5m = get_crypto_spot_data(ticker, "5M")
    elif kind == "crypto_perp":
        df_4h = get_crypto_perp_data(ticker, "4H")
        df_15m = get_crypto_perp_data(ticker, "15M")
        df_5m = get_crypto_perp_data(ticker, "5M")
    else:
        return None, None, None, "unknown_kind"

    return df_4h, df_15m, df_5m, None


# =========================================================================
# AI (Gemini) — TP/SL подсказки
# =========================================================================
# Модели по приоритету (если одна упёрлась в квоту — пробуем следующую)
GEMINI_MODELS = [
    "gemini-2.5-flash",
    "gemini-2.5-flash-lite",
    "gemini-flash-latest",
    "gemini-2.0-flash",
    "gemini-3.6-flash",
]


def _atr_levels(direction: str, last_close: float, atr: float = None) -> dict:
    """SL/TP1/TP2 по ATR."""
    if not atr or atr <= 0:
        atr = last_close * 0.01
    if direction == "Лонг":
        return {
            "sl": last_close - 1.5 * atr,
            "tp1": last_close + 2.0 * atr,
            "tp2": last_close + 3.5 * atr,
        }
    return {
        "sl": last_close + 1.5 * atr,
        "tp1": last_close - 2.0 * atr,
        "tp2": last_close - 3.5 * atr,
    }


def _parse_levels_from_text(text: str, direction: str, last_close: float, atr: float = None) -> dict:
    """Достаёт числа SL/TP из ответа модели, иначе ATR."""
    import re
    levels = _atr_levels(direction, last_close, atr)
    if not text:
        return levels
    # ищем паттерны SL: 79100, TP1: 80600 и т.п.
    nums = re.findall(r"(?:SL|TP1|TP2|Stop|Take)[^\d]{0,12}([\d]+(?:[.,]\d+)?)", text, flags=re.I)
    cleaned = []
    for n in nums:
        try:
            cleaned.append(float(n.replace(",", ".").replace(" ", "")))
        except Exception:
            pass
    # fallback: любые крупные числа
    if len(cleaned) < 2:
        all_nums = re.findall(r"\b(\d{2,7}(?:[.,]\d+)?)\b", text)
        cleaned = []
        for n in all_nums:
            try:
                v = float(n.replace(",", "."))
                if last_close * 0.5 < v < last_close * 1.5:
                    cleaned.append(v)
            except Exception:
                pass
    if len(cleaned) >= 1:
        levels["sl"] = cleaned[0]
    if len(cleaned) >= 2:
        levels["tp1"] = cleaned[1]
    if len(cleaned) >= 3:
        levels["tp2"] = cleaned[2]
    return levels


def _gemini_generate(prompt: str) -> str:
    """Пробует несколько моделей; при 429/квоте — понятная ошибка."""
    if not GEMINI_API_KEY:
        raise RuntimeError("no_key")
    from google import genai
    client = genai.Client(api_key=GEMINI_API_KEY)
    last_err = None
    for model in GEMINI_MODELS:
        try:
            resp = client.models.generate_content(model=model, contents=prompt)
            text = (resp.text or "").strip()
            if text:
                return text
        except Exception as e:
            last_err = e
            err_s = str(e)
            log.warning(f"Gemini {model}: {err_s[:180]}")
            # квота / rate limit — пробуем следующую модель
            if "429" in err_s or "RESOURCE_EXHAUSTED" in err_s or "quota" in err_s.lower():
                continue
            # другие ошибки — тоже пробуем другую модель
            continue
    if last_err and ("429" in str(last_err) or "RESOURCE_EXHAUSTED" in str(last_err) or "quota" in str(last_err).lower()):
        raise RuntimeError("quota")
    raise RuntimeError(str(last_err) if last_err else "empty")


def get_ai_levels(label: str, direction: str, trigger: str, last_close: float, atr: float = None) -> dict:
    """
    Возвращает {sl, tp1, tp2} — из AI или ATR.
    """
    levels = _atr_levels(direction, last_close, atr)
    if not GEMINI_API_KEY:
        return levels
    prompt = (
        f"{label}, {direction}, {trigger}, цена {last_close}. "
        f"Только 3 числа в формате:\nSL: ...\nTP1: ...\nTP2: ..."
    )
    try:
        text = _gemini_generate(prompt)
        return _parse_levels_from_text(text, direction, last_close, atr)
    except Exception as e:
        log.warning(f"get_ai_levels: {e}")
        return levels


def format_alert_caption(
    label: str,
    direction: str,
    trigger: str,
    entry: float,
    levels: dict,
    lot_line: str,
    test: bool = False,
) -> str:
    """
    Короткий алерт:
      ● BTC/USDT · Лонг · FVG
      SL … · TP1 … · TP2 …
      Лот: …
    """
    dir_s = "Лонг" if direction == "Лонг" else "Шорт"
    test_s = " <i>(тест)</i>" if test else ""
    sl = levels.get("sl", entry)
    tp1 = levels.get("tp1", entry)
    tp2 = levels.get("tp2", entry)
    return (
        f"● <b>{label}</b> · {dir_s} · {trigger}{test_s}\n"
        f"SL <code>{sl:.5g}</code> · TP1 <code>{tp1:.5g}</code> · TP2 <code>{tp2:.5g}</code>\n"
        f"{lot_line}"
    )


def format_lot_line(settings: dict, symbol_key: str, direction: str, entry: float, sl: float) -> str:
    """Одна строка: Лот: 6.1 монет (риск $2000)."""
    profile = resolve_account_profile(settings)
    if not profile["ok"]:
        return "Лот: — (задайте /deposit или /prop)"
    max_n = float(settings.get("max_notional_pct") or (8.0 if profile["account_type"] == "prop" else 20.0))
    calc = calc_position_size(
        balance=profile["balance"],
        risk_pct=profile["risk_pct"],
        entry=entry,
        stop_loss=sl,
        instrument_key=symbol_key,
        account_type=profile["account_type"],
        prop_daily_loss_pct=profile["daily_pct"],
        prop_max_loss_pct=profile["max_pct"],
        prop_phase=profile["phase"],
        max_notional_pct=max_n,
    )
    if not calc.get("ok"):
        return f"Лот: — ({calc.get('note') or 'ошибка'})"
    size_s = f"{calc['size']:.4g}" if calc["size_label"] == "монет" else str(calc["size"])
    return f"Лот: <b>{size_s}</b> {calc['size_label']} · риск ${calc['risk_usd']:.0f}"


def tradingview_symbol(instrument_key: str) -> str:
    info = AVAILABLE_INSTRUMENTS.get(instrument_key, {})
    kind = info.get("kind", "")
    ticker = info.get("ticker", "")
    if kind == "crypto":
        return f"BINANCE:{ticker.replace('/', '')}"
    if kind == "crypto_perp":
        base = ticker.replace("/", "")
        if not base.endswith("USDT"):
            base = f"{base}USDT" if "USDT" not in base else base
        return f"BINANCE:{base}.P"
    if kind == "forex":
        if "GC" in ticker:
            return "COMEX:GC1!"
        if "SI" in ticker:
            return "COMEX:SI1!"
        sym = ticker.replace("=X", "").replace("=F", "")
        return f"FX:{sym}"
    if kind == "equity":
        return f"NASDAQ:{ticker}"
    return ticker or "BINANCE:BTCUSDT"


def tradingview_url(instrument_key: str, interval: str = "15") -> str:
    """Ссылка на TV: откроет приложение/сайт на нужном символе и ТФ."""
    sym = tradingview_symbol(instrument_key)
    return f"https://www.tradingview.com/chart/?symbol={sym}&interval={interval}"


def tradingview_links_html(instrument_key: str) -> str:
    h4 = tradingview_url(instrument_key, "240")
    m15 = tradingview_url(instrument_key, "15")
    m5 = tradingview_url(instrument_key, "5")
    return (
        f'<a href="{h4}">TV H4</a> · '
        f'<a href="{m15}">TV M15</a> · '
        f'<a href="{m5}">TV M5</a>'
    )


def build_alert_keyboard(instrument_key: str) -> dict:
    return {
        "inline_keyboard": [
            [
                {"text": "TV H4", "url": tradingview_url(instrument_key, "240")},
                {"text": "TV M15", "url": tradingview_url(instrument_key, "15")},
                {"text": "TV M5", "url": tradingview_url(instrument_key, "5")},
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


def render_single_chart(df, title: str, filename: str, max_bars: int = 60, zone=None):
    """Один таймфрейм — отдельная картинка в стиле Setup Hunter."""
    import matplotlib.pyplot as plt

    if df is None or len(df) < 5:
        return False
    data = df.tail(max_bars)
    fig, ax = plt.subplots(figsize=(11, 5.5), facecolor="#0B0E14")
    _style_candles(ax, data)
    if zone:
        _draw_zone(ax, data, zone[0], zone[1])
    ax.set_title(title, color="#d1d4dc", fontsize=12, loc="left", pad=8)
    ax.set_xticks([])
    fig.savefig(filename, dpi=130, bbox_inches="tight", facecolor="#0B0E14", edgecolor="none")
    plt.close(fig)
    return True


def render_chart(df, title, filename, max_bars=50):
    """Fallback одиночный график."""
    render_single_chart(df, title, filename, max_bars=max_bars)


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


def register_bot_commands():
    """Регистрирует меню команд — при вводе «/» Telegram показывает весь список."""
    resp = tg_post("setMyCommands", payload={"commands": BOT_COMMANDS})
    if resp and resp.get("ok"):
        log.info(f"Bot commands registered: {len(BOT_COMMANDS)}")
    else:
        log.warning(f"setMyCommands failed: {resp}")


def format_help_text() -> str:
    lines = [
        "🎯 <b>Market Setup Hunter</b> — все команды\n",
        "<b>Основное</b>",
        "/setup — панель управления",
        "/strategies — стратегии",
        "/instruments — инструменты",
        "/status — текущие настройки",
        "/testalert — тестовый алерт",
        "",
        "<b>Новости</b>",
        "/news — календарь на сегодня",
        "/news high — только High + Medium",
        "",
        "<b>Риск / лот / prop</b>",
        "/risk — профиль риска",
        "/deposit 10000 — депозит",
        "/riskpct 1 — % риска на сделку",
        "/prop 50000 5 10 — prop (size, daily%, max%)",
        "/personal — личный депозит",
        "/lot BTC 95000 93000 — рассчитать лот",
        "",
        "<b>ИИ-хелпер</b> (в теме «ИИ хелпер»)",
        "просто текст — ответ ИИ",
        "/model — Gemini / Claude / Grok",
        "/img описание — сгенерировать картинку",
        "",
        "<b>Служебное</b>",
        "/example — пример алерта",
        "/whereami — chat_id и thread_id",
        "/help — этот список",
    ]
    return "\n".join(lines)


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
            format_help_text() + f"\n\n<i>Крипто-биржа: {_active_exchange_id}</i>",
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
    elif text in ("/risk", "/deposit", "/riskprofile"):
        s = get_user_settings(chat_id)
        send_message(chat_id, format_risk_card(s), message_thread_id=thread_id)
    elif text.startswith("/deposit "):
        try:
            bal = float(text.split(None, 1)[1].replace(",", "").replace("$", ""))
            save_user_settings(chat_id, balance=bal, account_type="personal")
            s = get_user_settings(chat_id)
            send_message(chat_id, f"✅ Депозит: <b>${bal:,.2f}</b>\n\n" + format_risk_card(s), message_thread_id=thread_id)
        except Exception:
            send_message(chat_id, "Формат: <code>/deposit 10000</code>", message_thread_id=thread_id)
    elif text.startswith("/riskpct "):
        try:
            pct = float(text.split(None, 1)[1].replace("%", "").replace(",", "."))
            if pct <= 0 or pct > 10:
                raise ValueError("range")
            save_user_settings(chat_id, risk_pct=pct)
            s = get_user_settings(chat_id)
            send_message(chat_id, f"✅ Риск на сделку: <b>{pct}%</b>\n\n" + format_risk_card(s), message_thread_id=thread_id)
        except Exception:
            send_message(chat_id, "Формат: <code>/riskpct 1</code> (0.1–10%)", message_thread_id=thread_id)
    elif text.startswith("/prop"):
        # /prop 50000 5 10  — size, daily%, max%
        parts = text.split()
        try:
            if len(parts) < 2:
                raise ValueError("need size")
            bal = float(parts[1].replace(",", "").replace("$", ""))
            daily = float(parts[2]) if len(parts) > 2 else 5.0
            maxl = float(parts[3]) if len(parts) > 3 else 10.0
            save_user_settings(
                chat_id,
                account_type="prop",
                balance=bal,
                prop_daily_loss_pct=daily,
                prop_max_loss_pct=maxl,
            )
            # авто-риск: не больше 1% или 25% дневного
            s = get_user_settings(chat_id)
            safe_pct = min(float(s.get("risk_pct") or 1), daily * 0.25)
            save_user_settings(chat_id, risk_pct=round(safe_pct, 2))
            s = get_user_settings(chat_id)
            send_message(chat_id, f"✅ Prop-счёт настроен\n\n" + format_risk_card(s), message_thread_id=thread_id)
        except Exception:
            send_message(
                chat_id,
                "Формат: <code>/prop 50000 5 10</code>\n"
                "размер prop · дневной лимит % · макс. просадка %",
                message_thread_id=thread_id,
            )
    elif text == "/personal":
        save_user_settings(chat_id, account_type="personal")
        s = get_user_settings(chat_id)
        send_message(chat_id, "✅ Режим: личный депозит\n\n" + format_risk_card(s), message_thread_id=thread_id)
    elif text.startswith("/maxpos"):
        parts = text.split()
        try:
            pct = float(parts[1]) if len(parts) > 1 else 8.0
            if pct <= 0 or pct > 100:
                raise ValueError("range")
            save_user_settings(chat_id, max_notional_pct=pct)
            send_message(
                chat_id,
                f"✅ Макс. номинал позиции: <b>{pct}%</b> от баланса.\n"
                f"На prop $200k и ETH≈$2470 это ≈ <b>{200000 * pct / 100 / 2470:.1f} ETH</b>.",
                message_thread_id=thread_id,
            )
        except Exception:
            send_message(chat_id, "Формат: <code>/maxpos 8</code> — макс. % номинала от баланса (по умолч. 8%)", message_thread_id=thread_id)
    elif text.startswith("/lot "):
        # /lot BTC 104000 101500  или /lot EURUSD 1.0850 1.0800
        parts = text.split()
        try:
            if len(parts) < 4:
                raise ValueError("args")
            sym_raw = parts[1].upper().replace("/", "")
            entry = float(parts[2])
            sl = float(parts[3])
            # найти instrument_key
            ikey = None
            for k, info in AVAILABLE_INSTRUMENTS.items():
                lab = info["label"].upper().replace("/", "").replace("_", "")
                if sym_raw in lab or sym_raw in k:
                    ikey = k
                    break
            if not ikey:
                # попробуем CR_
                for k in AVAILABLE_INSTRUMENTS:
                    if sym_raw in k:
                        ikey = k
                        break
            s = get_user_settings(chat_id)
            profile = resolve_account_profile(s)
            if not profile["ok"]:
                send_message(chat_id, f"⚠️ {profile['error']}", message_thread_id=thread_id)
                return
            calc = calc_position_size(
                balance=profile["balance"],
                risk_pct=profile["risk_pct"],
                entry=entry,
                stop_loss=sl,
                instrument_key=ikey or "",
                account_type=profile["account_type"],
                prop_daily_loss_pct=profile["daily_pct"],
                prop_max_loss_pct=profile["max_pct"],
                prop_phase=profile["phase"],
                max_notional_pct=float(s.get("max_notional_pct") or 8.0),
            )
            direction = "Лонг" if entry > sl else "Шорт"
            send_message(chat_id, format_lot_result(calc, entry, sl, direction), message_thread_id=thread_id)
        except Exception:
            send_message(
                chat_id,
                "Формат: <code>/lot SYMBOL entry SL</code>\n"
                "Пример: <code>/lot BTC 104000 101500</code>\n"
                "Сначала задай депозит: <code>/deposit 10000</code>",
                message_thread_id=thread_id,
            )
    elif text.startswith("/news"):
        only_high = "high" in text.lower()
        send_message(chat_id, "⏳ Загружаю календарь Forex Factory…", message_thread_id=thread_id)
        try:
            events = fetch_forexfactory_events()
            text_out = format_news_list(events, only_high=only_high)
            # шлём и в текущую тему, и дублируем в NEWS_TOPIC если это не она
            send_message(chat_id, text_out, message_thread_id=thread_id)
            if thread_id != NEWS_TOPIC_ID:
                send_message(CHAT_ID, text_out, message_thread_id=NEWS_TOPIC_ID)
        except Exception as e:
            send_message(chat_id, f"❌ Ошибка загрузки новостей: {e}", message_thread_id=thread_id)
    elif text in ("/brief", "/overview"):
        send_message(chat_id, "⏳ Собираю утренний обзор…", message_thread_id=thread_id)
        try:
            text_out = format_morning_brief()
            send_message(chat_id, text_out, message_thread_id=thread_id)
            if thread_id != MARKET_TOPIC_ID:
                post_to_market_topic(text_out)
        except Exception as e:
            send_message(chat_id, f"❌ Обзор: {e}", message_thread_id=thread_id)
    elif text in ("/weekplan", "/plan"):
        send_message(chat_id, "⏳ Собираю план на неделю…", message_thread_id=thread_id)
        try:
            text_out = format_weekly_plan()
            send_message(chat_id, text_out, message_thread_id=thread_id)
            if thread_id != MARKET_TOPIC_ID:
                post_to_market_topic(text_out)
        except Exception as e:
            send_message(chat_id, f"❌ План: {e}", message_thread_id=thread_id)
    elif text == "/testalert":
        try:
            symbol_key = "CR_BTC"
            label = AVAILABLE_INSTRUMENTS.get(symbol_key, {}).get("label", "BTC/USDT")
            df_4h, df_15m, df_5m, _ = fetch_pair_data(symbol_key)
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

            zone_15m = zone_4h = zone_5m = None
            try:
                fvgs = find_fvg(df_15m)
                if fvgs:
                    zone_15m = (fvgs[-1]["top"], fvgs[-1]["bottom"])
                if df_4h is not None:
                    fvgs4 = find_fvg(df_4h)
                    if fvgs4:
                        zone_4h = (fvgs4[-1]["top"], fvgs4[-1]["bottom"])
                if df_5m is not None:
                    fvgs5 = find_fvg(df_5m)
                    if fvgs5:
                        zone_5m = (fvgs5[-1]["top"], fvgs5[-1]["bottom"])
            except Exception:
                pass

            imgs = []
            img_4h, img_15m, img_5m = "test_4H.png", "test_15M.png", "test_5M.png"
            render_single_chart(df_4h if df_4h is not None else df_15m, f"{label} · H4", img_4h, 48, zone_4h)
            render_single_chart(df_15m, f"{label} · M15", img_15m, 60, zone_15m)
            imgs = [img_4h, img_15m]
            if df_5m is not None and len(df_5m) >= 10:
                render_single_chart(df_5m, f"{label} · M5", img_5m, 60, zone_5m)
                imgs.append(img_5m)

            levels = get_ai_levels(label, "Лонг", "FVG", last_close, atr)
            s = get_user_settings(chat_id)
            if not float(s.get("balance") or 0):
                s = get_user_settings(str(CHAT_ID))
            lot_line = format_lot_line(s, symbol_key, "Лонг", last_close, levels["sl"])
            caption = format_alert_caption(label, "Лонг", "FVG", last_close, levels, lot_line, test=True)
            caption += "\n" + tradingview_links_html(symbol_key)
            send_telegram_media_group(imgs, caption=caption, message_thread_id=SCREENER_TOPIC_ID)
            send_message(chat_id, "✅ Тестовый алерт отправлен в тему «Скринер»", message_thread_id=thread_id)
            for p in imgs:
                if os.path.exists(p):
                    try:
                        os.remove(p)
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
    elif data.startswith("aimodel:"):
        model = data.split(":", 1)[1]
        if model in ("auto", "gemini", "groq", "mistral", "openrouter", "claude", "grok"):
            save_user_settings(chat_id, ai_model=model)
            thread_id = callback_query.get("message", {}).get("message_thread_id")
            try:
                edit_message_text(
                    chat_id,
                    message_id,
                    f"✅ Модель ИИ: <b>{model}</b>\nПросто напиши вопрос.",
                    reply_markup=build_ai_model_keyboard(chat_id),
                )
            except Exception:
                send_message(
                    chat_id,
                    f"✅ Модель ИИ: <b>{model}</b>",
                    reply_markup=build_ai_model_keyboard(chat_id),
                    message_thread_id=thread_id,
                )
            answer_callback_query(callback_id, text=f"Модель: {model}")
        else:
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


# =========================================================================
# ИИ-ХЕЛПЕР (тема 5)
# =========================================================================
AI_SYSTEM_PROMPT = (
    "Ты старший трейдер-наставник (SMC/ICT) в Telegram. Русский язык. "
    "Правила:\n"
    "1) Коротко. Без воды, без мотивашек, без «отличный RR».\n"
    "2) Если нет цены/графика — не выдумывай уровни. Скажи, каких данных не хватает.\n"
    "3) SL/TP — конкретные числа или логика «за свинг / за OB / за FVG».\n"
    "4) Крипта: сначала контекст BTC, потом альта.\n"
    "5) Риск: считай от депозита/prop, не советуй «1–2%» в вакууме, если есть цифры.\n"
    "6) Это обучение, не индивидуальная инвестиционная рекомендация.\n"
    "7) Картинка — команда /img."
)


class AIProviderError(Exception):
    """Провайдер недоступен / квота — пробуем следующего."""


def _openai_chat(base_url: str, api_key: str, model_name: str, user_text: str, extra_headers=None) -> str:
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    if extra_headers:
        headers.update(extra_headers)
    resp = requests.post(
        f"{base_url.rstrip('/')}/chat/completions",
        headers=headers,
        json={
            "model": model_name,
            "messages": [
                {"role": "system", "content": AI_SYSTEM_PROMPT},
                {"role": "user", "content": user_text},
            ],
            "temperature": 0.3,
            "max_tokens": 900,
        },
        timeout=60,
    )
    if resp.status_code in (429, 503, 402):
        raise AIProviderError(f"{model_name} {resp.status_code}")
    if resp.status_code != 200:
        raise AIProviderError(f"{model_name} {resp.status_code}: {resp.text[:160]}")
    data = resp.json()
    text = (
        data.get("choices", [{}])[0]
        .get("message", {})
        .get("content", "")
        .strip()
    )
    if not text:
        raise AIProviderError(f"{model_name} empty")
    return text


def _chat_gemini(user_text: str) -> str:
    if not GEMINI_API_KEY:
        raise AIProviderError("no gemini key")
    return _gemini_generate(f"{AI_SYSTEM_PROMPT}\n\nПользователь: {user_text}")


def _chat_claude(user_text: str) -> str:
    if not CLAUDE_API_KEY:
        raise AIProviderError("no claude key")
    resp = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "x-api-key": CLAUDE_API_KEY,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        },
        json={
            "model": "claude-sonnet-4-20250514",
            "max_tokens": 900,
            "system": AI_SYSTEM_PROMPT,
            "messages": [{"role": "user", "content": user_text}],
        },
        timeout=60,
    )
    if resp.status_code in (429, 529):
        raise AIProviderError("claude quota")
    if resp.status_code != 200:
        raise AIProviderError(f"claude {resp.status_code}")
    data = resp.json()
    parts = data.get("content") or []
    texts = [p.get("text", "") for p in parts if p.get("type") == "text"]
    text = "\n".join(texts).strip()
    if not text:
        raise AIProviderError("claude empty")
    return text


def _chat_grok(user_text: str) -> str:
    if not GROK_API_KEY:
        raise AIProviderError("no grok key")
    return _openai_chat("https://api.x.ai/v1", GROK_API_KEY, "grok-3", user_text)


def _chat_groq(user_text: str) -> str:
    if not GROQ_API_KEY:
        raise AIProviderError("no groq key")
    last = None
    for m in ("qwen/qwen3.6-27b", "openai/gpt-oss-120b", "openai/gpt-oss-20b"):
        try:
            return _openai_chat("https://api.groq.com/openai/v1", GROQ_API_KEY, m, user_text)
        except Exception as e:
            last = e
            continue
    raise AIProviderError(str(last) if last else "groq fail")


def _chat_mistral(user_text: str) -> str:
    if not MISTRAL_API_KEY:
        raise AIProviderError("no mistral key")
    last = None
    for m in ("mistral-small-latest", "mistral-medium-latest"):
        try:
            return _openai_chat("https://api.mistral.ai/v1", MISTRAL_API_KEY, m, user_text)
        except Exception as e:
            last = e
            continue
    raise AIProviderError(str(last) if last else "mistral fail")


def _chat_openrouter(user_text: str) -> str:
    if not OPENROUTER_API_KEY:
        raise AIProviderError("no openrouter key")
    last = None
    for m in (
        "openrouter/free",
        "qwen/qwen3-32b:free",
        "meta-llama/llama-3.3-70b-instruct:free",
    ):
        try:
            return _openai_chat(
                "https://openrouter.ai/api/v1",
                OPENROUTER_API_KEY,
                m,
                user_text,
                extra_headers={"HTTP-Referer": "https://t.me", "X-Title": "TradingHub"},
            )
        except Exception as e:
            last = e
            continue
    raise AIProviderError(str(last) if last else "openrouter fail")


AI_PROVIDERS = {
    "gemini": _chat_gemini,
    "groq": _chat_groq,
    "mistral": _chat_mistral,
    "openrouter": _chat_openrouter,
    "claude": _chat_claude,
    "grok": _chat_grok,
}

# Порядок авто: умные + высокие лимиты первыми
AI_AUTO_ORDER = ["groq", "gemini", "mistral", "openrouter", "claude", "grok"]


def ai_chat(user_text: str, model: str = "auto") -> str:
    model = (model or "auto").lower()
    if model == "auto":
        order = list(AI_AUTO_ORDER)
    else:
        order = [model] + [p for p in AI_AUTO_ORDER if p != model]

    errors = []
    for name in order:
        fn = AI_PROVIDERS.get(name)
        if not fn:
            continue
        try:
            text = fn(user_text)
            if text:
                if model == "auto" or name != model:
                    return f"<i>[{name}]</i>\n{text}"
                return text
        except Exception as e:
            log.warning(f"AI {name}: {e}")
            errors.append(f"{name}: {e}")
            continue
    return (
        "❌ Все ИИ-провайдеры недоступны или без ключей.\n"
        "Добавь в Render Environment хотя бы:\n"
        "<code>GROQ_API_KEY</code> и/или <code>GEMINI_API_KEY</code>\n"
        f"<i>{'; '.join(errors)[:400]}</i>"
    )


def build_ai_model_keyboard(chat_id: str) -> dict:
    s = get_user_settings(chat_id)
    current = s.get("ai_model", "auto")
    models = [
        ("auto", "Auto"),
        ("groq", "Groq"),
        ("gemini", "Gemini"),
        ("mistral", "Mistral"),
        ("openrouter", "OpenRouter"),
        ("claude", "Claude"),
        ("grok", "Grok"),
    ]
    rows, row = [], []
    for mid, label in models:
        mark = "✅ " if mid == current else ""
        row.append({"text": f"{mark}{label}", "callback_data": f"aimodel:{mid}"})
        if len(row) == 3:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    return {"inline_keyboard": rows}


def gemini_image(prompt: str, out_path: str) -> bool:
    """Генерация картинки через Gemini image model. True если файл создан."""
    if not GEMINI_API_KEY:
        return False
    try:
        from google import genai
        from google.genai import types
        import base64

        client = genai.Client(api_key=GEMINI_API_KEY)
        # Пробуем актуальные image-модели по очереди
        for model_id in ("gemini-3.1-flash-image", "gemini-2.5-flash-image"):
            try:
                resp = client.models.generate_content(
                    model=model_id,
                    contents=prompt,
                    config=types.GenerateContentConfig(
                        response_modalities=["IMAGE", "TEXT"],
                    ),
                )
                # Ищем image parts
                for cand in getattr(resp, "candidates", []) or []:
                    content = getattr(cand, "content", None)
                    if not content:
                        continue
                    for part in getattr(content, "parts", []) or []:
                        inline = getattr(part, "inline_data", None)
                        if inline and getattr(inline, "data", None):
                            data = inline.data
                            if isinstance(data, str):
                                data = base64.b64decode(data)
                            with open(out_path, "wb") as f:
                                f.write(data)
                            return True
            except Exception as e:
                log.warning(f"Image model {model_id}: {e}")
                continue
        return False
    except Exception as e:
        log.error(f"gemini_image error: {e}")
        return False


def handle_ai_message(chat_id: str, text: str, thread_id=None):
    text = (text or "").strip()
    if not text:
        return

    settings = get_user_settings(chat_id)
    model = settings.get("ai_model", "auto")

    if text.startswith("/img ") or text.startswith("/image "):
        prompt = text.split(" ", 1)[1].strip()
        send_message(chat_id, "🎨 Генерирую изображение (Gemini)…", message_thread_id=thread_id)
        path = f"ai_img_{int(time.time())}.png"
        ok = gemini_image(prompt, path)
        if ok and os.path.exists(path):
            send_photo_with_caption(path, f"🎨 <i>{prompt[:200]}</i>", message_thread_id=thread_id)
            try:
                os.remove(path)
            except Exception:
                pass
        else:
            send_message(
                chat_id,
                "❌ Не удалось сгенерировать картинку (лимит / модель / ключ).",
                message_thread_id=thread_id,
            )
        return

    if text in ("/model", "/models", "/aimodel"):
        send_message(
            chat_id,
            f"🤖 Выбери модель ИИ\nТекущая: <b>{model}</b>\n\n"
            "• <b>Auto</b> — сам берёт живого провайдера (рекомендую)\n"
            "• <b>Groq</b> — умный + ~1000 SMS/день (GROQ_API_KEY)\n"
            "• <b>Gemini</b> — GEMINI_API_KEY\n"
            "• <b>Mistral / OpenRouter</b> — свои ключи\n"
            "• Claude / Grok — платные ключи",
            reply_markup=build_ai_model_keyboard(chat_id),
            message_thread_id=thread_id,
        )
        return

    if text in ("/ai", "/help", "/start"):
        send_message(
            chat_id,
            "🤖 <b>ИИ-хелпер</b>\n\n"
            "Просто напиши вопрос — отвечу.\n"
            f"Модель: <b>{model}</b>  ·  <code>/model</code> — сменить\n"
            "<code>/img описание</code> — картинка (Gemini)\n\n"
            "Примеры:\n"
            "• Что такое FVG простыми словами?\n"
            "• Куда ставить SL после bullish sweep?",
            reply_markup=build_ai_model_keyboard(chat_id),
            message_thread_id=thread_id,
        )
        return

    send_message(chat_id, f"⏳ {model} думает…", message_thread_id=thread_id)
    answer = ai_chat(text, model=model)
    if len(answer) > 4000:
        answer = answer[:4000] + "…"
    send_message(chat_id, answer, message_thread_id=thread_id)


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
                    text = update["message"]["text"]
                    # Сообщения в теме ИИ-хелпер → AI
                    if thread_id == AI_TOPIC_ID and not text.startswith(
                        ("/setup", "/strategies", "/instruments", "/status", "/news", "/testalert",
                         "/whereami", "/example", "/brief", "/weekplan", "/overview", "/plan", "/risk", "/deposit", "/help")
                    ):
                        handle_ai_message(chat_id, text, thread_id=thread_id)
                    else:
                        handle_command(chat_id, text, thread_id=thread_id)
                elif "callback_query" in update:
                    handle_callback(update["callback_query"])
        except Exception as e:
            log.error(f"Polling error: {e}")
            time.sleep(5)


# =========================================================================
# СКАНИРОВАНИЕ
# =========================================================================
def process_pair(symbol_key, df_4h, df_15m, settings, df_5m=None):
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
            local_settings = dict(settings)
            local_settings["_symbol"] = symbol_key
            result = strat["detect"](df_4h, df_15m, local_settings)
        except Exception as e:
            log.error(f"Ошибка стратегии {sid} на {symbol_key}: {e}")
            continue
        if result is None:
            continue

        direction = result["direction"]
        trigger = result["trigger"]
        setup_id = result.get("setup_id") or result["time"].isoformat()
        alert_key = f"{symbol_key}:{sid}:{setup_id}"
        signal_time_str = str(setup_id)
        if should_skip_alert(alert_key, signal_time_str):
            fired_statuses.append(f"{strat['label']}: этот сетап уже отправлен")
            continue
        if pair_dir_is_locked(symbol_key, direction):
            fired_statuses.append(f"{strat['label']}: уже был алерт по {direction} недавно")
            continue
        fired_statuses.append(f"🔥 {strat['label']}: {direction}")

        imgs = []
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

            zone_15m = zone_4h = zone_5m = None
            try:
                fvgs = find_fvg(df_15m)
                if fvgs:
                    zone_15m = (fvgs[-1]["top"], fvgs[-1]["bottom"])
                fvgs4 = find_fvg(df_4h)
                if fvgs4:
                    zone_4h = (fvgs4[-1]["top"], fvgs4[-1]["bottom"])
                if df_5m is not None and len(df_5m) > 10:
                    fvgs5 = find_fvg(df_5m)
                    if fvgs5:
                        zone_5m = (fvgs5[-1]["top"], fvgs5[-1]["bottom"])
            except Exception:
                pass

            # Три отдельных графика: 4H, 15M, 5M
            img_4h = f"{symbol_key}_{sid}_4H.png"
            img_15m = f"{symbol_key}_{sid}_15M.png"
            img_5m = f"{symbol_key}_{sid}_5M.png"
            render_single_chart(df_4h, f"{label} · H4", img_4h, max_bars=48, zone=zone_4h)
            render_single_chart(df_15m, f"{label} · M15", img_15m, max_bars=60, zone=zone_15m)
            imgs = [img_4h, img_15m]
            if df_5m is not None and len(df_5m) >= 10:
                render_single_chart(df_5m, f"{label} · M5", img_5m, max_bars=60, zone=zone_5m)
                imgs.append(img_5m)

            levels = get_ai_levels(label, direction, trigger, last_close, atr)
            lot_line = format_lot_line(settings, symbol_key, direction, last_close, levels["sl"])
            caption = format_alert_caption(
                label, direction, trigger, last_close, levels, lot_line, test=False
            )
            caption += "\n" + tradingview_links_html(symbol_key)

            send_telegram_media_group(
                imgs,
                caption=caption,
                message_thread_id=SCREENER_TOPIC_ID,
            )
            set_last_alert_time(alert_key, signal_time_str)
            lock_pair_dir(symbol_key, direction)
            log.info(f"✅ Алерт по {symbol_key} ({sid}) отправлен в тему {SCREENER_TOPIC_ID}")
        except Exception as e:
            log.error(f"Ошибка отправки алерта {symbol_key}: {e}\n{traceback.format_exc()}")
        finally:
            for p in imgs:
                if os.path.exists(p):
                    try:
                        os.remove(p)
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
            df_4h, df_15m, df_5m, skip_reason = fetch_pair_data(symbol_key)
            if skip_reason == "market_closed":
                market_data[symbol_key] = f"{label}: рынок закрыт"
                continue
            if df_4h is None or df_15m is None:
                market_data[symbol_key] = f"{label}: нет данных"
                log.warning(f"{symbol_key}: данные не получены")
                continue
            process_pair(symbol_key, df_4h, df_15m, settings, df_5m=df_5m)
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
# НОВОСТИ — Forex Factory
# =========================================================================
NEWS_CHECK_INTERVAL = int(os.environ.get("NEWS_CHECK_INTERVAL", "120"))  # секунд
NEWS_ALERT_MINUTES = [15, 5, 0]  # за сколько минут предупреждать
_news_sent_keys = set()  # in-memory: event_id+offset уже слали


def _ff_day_url(dt: datetime = None) -> str:
    dt = dt or datetime.now(timezone.utc)
    # Forex Factory day format: sep5.2026
    return f"https://www.forexfactory.com/calendar?day={dt.strftime('%b%d.%Y').lower()}"


_FF_CACHE = {"key": None, "ts": 0.0, "events": []}


def fetch_forexfactory_events(for_date: datetime = None) -> List[dict]:
    """
    Парсит календарь FF на день.
    Возвращает список:
      {title, currency, impact, time_str, datetime_utc, forecast, previous, actual}
    impact: high / medium / low / holiday / unknown
    """
    dt = for_date or datetime.now(timezone.utc)
    cache_key = dt.strftime("%Y-%m-%d")
    now_ts = time.time()
    if _FF_CACHE["key"] == cache_key and (now_ts - _FF_CACHE["ts"]) < 300 and _FF_CACHE["events"]:
        return _FF_CACHE["events"]

    try:
        from bs4 import BeautifulSoup
    except ImportError:
        log.error("beautifulsoup4 не установлен")
        return []

    url = _ff_day_url(for_date)
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
    }
    try:
        resp = requests.get(url, headers=headers, timeout=20)
        if resp.status_code != 200:
            log.warning(f"FF calendar HTTP {resp.status_code}")
            return []
        soup = BeautifulSoup(resp.text, "html.parser")
    except Exception as e:
        log.warning(f"FF fetch error: {e}")
        return []

    events = []
    rows = soup.select("tr.calendar__row") or soup.select("table.calendar__table tr")
    current_date = (for_date or datetime.now(timezone.utc)).date()
    last_time = None

    impact_map = {
        "high": "high",
        "red": "high",
        "medium": "medium",
        "orange": "medium",
        "low": "low",
        "yellow": "low",
        "holiday": "holiday",
        "gray": "holiday",
        "grey": "holiday",
    }

    for row in rows:
        try:
            # impact
            impact_cell = row.select_one("td.calendar__impact") or row.select_one(".calendar__impact")
            impact = "unknown"
            if impact_cell:
                span = impact_cell.select_one("span")
                classes = " ".join((span.get("class") if span else []) or impact_cell.get("class") or [])
                classes_l = classes.lower()
                for k, v in impact_map.items():
                    if k in classes_l:
                        impact = v
                        break
                # icon title fallback
                title_attr = (span.get("title") if span else None) or impact_cell.get("title") or ""
                tl = title_attr.lower()
                if "high" in tl:
                    impact = "high"
                elif "medium" in tl:
                    impact = "medium"
                elif "low" in tl:
                    impact = "low"

            currency_el = row.select_one("td.calendar__currency") or row.select_one(".calendar__currency")
            currency = (currency_el.get_text(strip=True) if currency_el else "") or ""

            event_el = row.select_one("td.calendar__event") or row.select_one(".calendar__event-title")
            title = (event_el.get_text(strip=True) if event_el else "") or ""
            if not title:
                continue

            time_el = row.select_one("td.calendar__time") or row.select_one(".calendar__time")
            time_str = (time_el.get_text(strip=True) if time_el else "") or last_time or ""
            if time_str:
                last_time = time_str

            def _cell(cls):
                el = row.select_one(f"td.calendar__{cls}") or row.select_one(f".calendar__{cls}")
                return el.get_text(strip=True) if el else ""

            actual = _cell("actual")
            forecast = _cell("forecast")
            previous = _cell("previous")

            # Парсим время → UTC (FF обычно America/New_York)
            event_dt = None
            try:
                if time_str and ":" in time_str and "day" not in time_str.lower():
                    # e.g. "8:30am" or "14:30"
                    t = time_str.lower().replace(" ", "")
                    fmt = "%I:%M%p" if ("am" in t or "pm" in t) else "%H:%M"
                    parsed = datetime.strptime(t, fmt)
                    if NY_TZ is not None:
                        event_dt = datetime(
                            current_date.year, current_date.month, current_date.day,
                            parsed.hour, parsed.minute, tzinfo=NY_TZ,
                        ).astimezone(timezone.utc)
                    else:
                        # грубо NY ≈ UTC-4/-5
                        event_dt = datetime(
                            current_date.year, current_date.month, current_date.day,
                            parsed.hour, parsed.minute, tzinfo=timezone.utc,
                        ) + timedelta(hours=4)
            except Exception:
                event_dt = None

            events.append({
                "title": title,
                "currency": currency,
                "impact": impact,
                "time_str": time_str,
                "datetime_utc": event_dt,
                "forecast": forecast,
                "previous": previous,
                "actual": actual,
            })
        except Exception:
            continue

    log.info(f"FF calendar: {len(events)} events for {current_date}")
    _FF_CACHE["key"] = cache_key
    _FF_CACHE["ts"] = time.time()
    _FF_CACHE["events"] = events
    return events


def format_news_list(events: List[dict], only_high: bool = False) -> str:
    if only_high:
        events = [e for e in events if e["impact"] in ("high", "medium")]
    if not events:
        return "На сегодня важных новостей не найдено (или FF недоступен)."

    impact_emoji = {"high": "🔴", "medium": "🟠", "low": "🟡", "holiday": "⚪", "unknown": "•"}
    lines = [f"📅 <b>Новости на сегодня</b> ({len(events)})\n"]
    for e in events:
        em = impact_emoji.get(e["impact"], "•")
        t = e["time_str"] or "—"
        cur = e["currency"] or ""
        title = e["title"]
        extra = []
        if e.get("forecast"):
            extra.append(f"F: {e['forecast']}")
        if e.get("previous"):
            extra.append(f"P: {e['previous']}")
        extra_s = f"  <i>({' | '.join(extra)})</i>" if extra else ""
        lines.append(f"{em} <b>{t}</b> {cur} — {title}{extra_s}")
    lines.append("\n🔴 High  🟠 Medium  🟡 Low")
    return "\n".join(lines)


def check_upcoming_news_alerts():
    """Фон: шлёт в тему Новости за 15/5/0 мин до high/medium."""
    events = fetch_forexfactory_events()
    now = datetime.now(timezone.utc)
    for e in events:
        if e["impact"] not in ("high", "medium"):
            continue
        if e["datetime_utc"] is None:
            continue
        delta_min = (e["datetime_utc"] - now).total_seconds() / 60.0
        for offset in NEWS_ALERT_MINUTES:
            # окно ±1.5 мин вокруг точки
            if abs(delta_min - offset) <= 1.5:
                key = f"{e['title']}|{e['datetime_utc'].isoformat()}|{offset}"
                if key in _news_sent_keys:
                    continue
                _news_sent_keys.add(key)
                em = "🔴" if e["impact"] == "high" else "🟠"
                if offset == 0:
                    when = "СЕЙЧАС"
                else:
                    when = f"через {offset} мин"
                text = (
                    f"{em} <b>Новость {when}</b>\n\n"
                    f"<b>{e['currency']}</b> {e['title']}\n"
                    f"Время: {e['time_str']} (NY)\n"
                    f"Impact: {e['impact'].upper()}\n"
                )
                if e.get("forecast") or e.get("previous"):
                    text += f"Forecast: {e.get('forecast') or '—'} | Previous: {e.get('previous') or '—'}\n"
                send_message(CHAT_ID, text, message_thread_id=NEWS_TOPIC_ID)
                log.info(f"News alert: {e['title']} ({when})")


def run_news_background():
    time.sleep(8)
    log.info("News monitor started")
    while True:
        try:
            check_upcoming_news_alerts()
        except Exception as e:
            log.error(f"News monitor error: {e}")
        time.sleep(NEWS_CHECK_INTERVAL)


# =========================================================================
# ОБЗОР РЫНКА (утренний бриф + воскресный план)
# =========================================================================
def _now_msk():
    try:
        from zoneinfo import ZoneInfo as _ZI
        return datetime.now(_ZI(OVERVIEW_TZ))
    except Exception:
        return datetime.now(MSK_TZ)


def overview_keys() -> List[str]:
    return [k for k in OVERVIEW_ASSET_KEYS if k in AVAILABLE_INSTRUMENTS]


def snapshot_asset(key: str) -> Optional[dict]:
    """Цена и простой 4H-bias."""
    info = AVAILABLE_INSTRUMENTS.get(key) or {}
    label = info.get("label", key)
    try:
        df_4h, df_15m, _, _ = fetch_pair_data(key)
    except Exception:
        df_4h = df_15m = None
    df = df_4h if df_4h is not None and len(df_4h) >= 5 else df_15m
    if df is None or len(df) < 3:
        return {"key": key, "label": label, "price": None, "chg": None, "bias": "нет данных"}
    last = float(df["Close"].iloc[-1])
    prev = float(df["Close"].iloc[-2])
    chg = ((last - prev) / prev * 100.0) if prev else 0.0
    bias = "—"
    if df_4h is not None and len(df_4h) >= 30:
        ema = compute_ema(df_4h["Close"], 50)
        e = float(ema.iloc[-1])
        if last > e and ema.iloc[-1] >= ema.iloc[-5]:
            bias = "бычий 4H"
        elif last < e and ema.iloc[-1] <= ema.iloc[-5]:
            bias = "медвежий 4H"
        else:
            bias = "флэт / смешанный 4H"
    return {"key": key, "label": label, "price": last, "chg": chg, "bias": bias}


def format_morning_brief() -> str:
    now = _now_msk()
    lines = [
        f"☀️ <b>Утренний обзор</b> · {now.strftime('%d.%m.%Y %H:%M')} МСК\n",
        "<b>Форекс + металлы + крипта</b>",
    ]
    fx_keys = [k for k in overview_keys() if k.startswith("FX_") or k.startswith("MT_")]
    cr_keys = [k for k in overview_keys() if k.startswith("CR_")]

    lines.append("\n💱 <b>Валюты / металлы</b>")
    for k in fx_keys:
        s = snapshot_asset(k)
        if not s or s["price"] is None:
            lines.append(f"• {AVAILABLE_INSTRUMENTS.get(k, {}).get('label', k)} — нет котировки")
            continue
        sign = "+" if s["chg"] >= 0 else ""
        lines.append(f"• <b>{s['label']}</b> <code>{s['price']:.5g}</code> {sign}{s['chg']:.2f}% · {s['bias']}")

    lines.append("\n🪙 <b>Крипта</b>")
    for k in cr_keys:
        s = snapshot_asset(k)
        if not s or s["price"] is None:
            lines.append(f"• {k} — нет котировки")
            continue
        sign = "+" if s["chg"] >= 0 else ""
        lines.append(f"• <b>{s['label']}</b> <code>{s['price']:.5g}</code> {sign}{s['chg']:.2f}% · {s['bias']}")

    # Новости FF по валютам обзора
    try:
        events = fetch_forexfactory_events()
        important = [
            e for e in events
            if e.get("impact") in ("high", "medium")
            and (e.get("currency") or "").upper() in OVERVIEW_FF_CCY
        ]
    except Exception:
        important = []
    lines.append("\n📅 <b>Новости дня (High/Medium)</b>")
    if not important:
        lines.append("Важных событий по мажорам нет / календарь недоступен.")
    else:
        emap = {"high": "🔴", "medium": "🟠"}
        for e in important[:18]:
            em = emap.get(e["impact"], "•")
            lines.append(f"{em} <b>{e.get('time_str') or '—'}</b> {e.get('currency')} — {e['title']}")
        lines.append("\n<i>Крипта новостями FF не покрывается — смотри BTC как лидера альтов.</i>")
    text = "\n".join(lines)
    return text[:3900]


def format_weekly_plan() -> str:
    now = _now_msk()
    # понедельник текущей/следующей недели
    monday = now.date() + timedelta(days=(7 - now.weekday()) % 7)
    if now.weekday() == 6:  # воскресенье — план на грядущую неделю
        week_from = now.date() + timedelta(days=1)
    else:
        week_from = now.date() - timedelta(days=now.weekday())
    week_to = week_from + timedelta(days=6)
    lines = [
        f"🗓 <b>План на неделю</b> {week_from.strftime('%d.%m')}–{week_to.strftime('%d.%m.%Y')}\n",
        "Сценарий по 4H EMA50. Не сигнал на вход — карта рынка.\n",
    ]
    for k in overview_keys():
        s = snapshot_asset(k)
        if not s:
            continue
        px = f"{s['price']:.5g}" if s["price"] is not None else "—"
        lines.append(f"• <b>{s['label']}</b> {px} — {s['bias']}")
    lines.append(
        "\nПравила недели:\n"
        "1) Сначала BTC 4H, потом ETH/SOL/альты.\n"
        "2) Форекс — не торговать за 15 мин до 🔴 новости по валюте пары.\n"
        "3) Золото чувствительно к USD и ставке.\n"
        "4) Один сетап = один вход, без докупок в ту же идею."
    )
    return "\n".join(lines)[:3900]


def post_to_market_topic(text: str):
    send_message(CHAT_ID, text, message_thread_id=MARKET_TOPIC_ID)


def run_overview_background():
    """Пн–Сб ~08:30 МСК бриф; Вс ~10:00 МСК недельный план."""
    time.sleep(12)
    log.info(f"Market overview started → topic {MARKET_TOPIC_ID}")
    while True:
        try:
            now = _now_msk()
            today = now.strftime("%Y-%m-%d")
            # утренний бриф пн–сб
            if now.weekday() < 6 and now.hour == OVERVIEW_MORNING_HOUR and now.minute < 20:
                if get_meta("last_morning_brief") != today:
                    post_to_market_topic(format_morning_brief())
                    set_meta("last_morning_brief", today)
                    log.info("Morning brief posted")
            # воскресенье — план недели
            if now.weekday() == 6 and now.hour == OVERVIEW_WEEKLY_HOUR and now.minute < 20:
                if get_meta("last_weekly_plan") != today:
                    post_to_market_topic(format_weekly_plan())
                    set_meta("last_weekly_plan", today)
                    log.info("Weekly plan posted")
        except Exception as e:
            log.error(f"Overview error: {e}\n{traceback.format_exc()}")
        time.sleep(60)
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
register_bot_commands()

log.info(f"CHAT_ID={CHAT_ID}, SCREENER={SCREENER_TOPIC_ID}, NEWS={NEWS_TOPIC_ID}, AI={AI_TOPIC_ID}, MARKET={MARKET_TOPIC_ID}")
log.info(f"Крипто-биржа: {_active_exchange_id}, инструментов крипты: {len(CR_INSTRUMENTS)}")

threading.Thread(target=run_scanner_background, daemon=True).start()
threading.Thread(target=run_telegram_polling, daemon=True).start()
threading.Thread(target=run_news_background, daemon=True).start()
threading.Thread(target=run_overview_background, daemon=True).start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
