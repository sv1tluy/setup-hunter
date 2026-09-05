import os
import json
import time
import sqlite3
import threading
import requests
import pandas as pd
import mplfinance as mpf
import yfinance as yf
import ccxt
from datetime import datetime, timezone
from flask import Flask, jsonify, render_template

try:
    from zoneinfo import ZoneInfo
    NY_TZ = ZoneInfo("America/New_York")
except Exception:
    NY_TZ = None  # на некоторых минимальных образах может не быть базы tzdata

app = Flask(__name__)

# =========================================================================
# НАСТРОЙКИ
# =========================================================================
BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")

if not BOT_TOKEN or not CHAT_ID:
    raise RuntimeError(
        "Не заданы переменные окружения BOT_TOKEN и/или CHAT_ID. "
        "Установи их перед запуском (например: export BOT_TOKEN=... CHAT_ID=...)."
    )

SCAN_INTERVAL_SECONDS = 300  # раз в 5 минут
API_URL = f"https://api.telegram.org/bot{BOT_TOKEN}"
DB_PATH = os.environ.get("ALERTS_DB_PATH", "bot_state.db")
PAGE_SIZE = 8
TOP_CRYPTO_N = 50

# Binance.com блокирует запросы с IP облачных провайдеров из США (код 451,
# "restricted location") — а именно там хостится Render. Поэтому биржа для
# крипто-данных вынесена в переменную окружения: если выбранная тоже
# окажется заблокирована с твоего сервера, можно переключиться без
# изменения кода — просто поменяй CRYPTO_EXCHANGE_ID на Render и передеплой.
# Bybit исторически доступен с большинства облачных IP, но это стоит
# проверить по логам после первого деплоя.
CRYPTO_EXCHANGE_ID = os.environ.get("CRYPTO_EXCHANGE_ID", "bybit")


def _make_exchange(market_type):
    exchange_class = getattr(ccxt, CRYPTO_EXCHANGE_ID)
    return exchange_class({'enableRateLimit': True, 'options': {'defaultType': market_type}})


crypto_spot = _make_exchange('spot')
crypto_perp = _make_exchange('swap')
_perp_symbol_cache = {}

print(f"Крипто-биржа: {CRYPTO_EXCHANGE_ID} (переключается через env CRYPTO_EXCHANGE_ID)")

market_data = {"last_update": "Только что"}

# =========================================================================
# РЕЕСТР ИНСТРУМЕНТОВ
# =========================================================================
FX_INSTRUMENTS = {
    'FX_EURUSD': {'label': 'EUR/USD', 'kind': 'forex', 'ticker': 'EURUSD=X'},
    'FX_GBPUSD': {'label': 'GBP/USD', 'kind': 'forex', 'ticker': 'GBPUSD=X'},
    'FX_USDJPY': {'label': 'USD/JPY', 'kind': 'forex', 'ticker': 'USDJPY=X'},
    'FX_USDCHF': {'label': 'USD/CHF', 'kind': 'forex', 'ticker': 'USDCHF=X'},
    'FX_AUDUSD': {'label': 'AUD/USD', 'kind': 'forex', 'ticker': 'AUDUSD=X'},
    'FX_USDCAD': {'label': 'USD/CAD', 'kind': 'forex', 'ticker': 'USDCAD=X'},
    'FX_NZDUSD': {'label': 'NZD/USD', 'kind': 'forex', 'ticker': 'NZDUSD=X'},
}

MT_INSTRUMENTS = {
    'MT_XAUUSD_GCF':   {'label': 'XAU/USD (COMEX фьючерс)', 'kind': 'forex',       'ticker': 'GC=F'},
    'MT_XAUUSDT_PERP': {'label': 'XAU/USDT (перп)', 'kind': 'crypto_perp', 'ticker': 'XAUUSDT'},
    'MT_XAGUSD_SIF':   {'label': 'XAG/USD (COMEX фьючерс, серебро)', 'kind': 'forex', 'ticker': 'SI=F'},
}

# Curated список крупных бумаг Nasdaq (не официальный live-состав индекса —
# он периодически ребалансируется биржей, тут просто удобный стартовый набор
# заметных имён; список можно расширять).
NQ_TICKERS = [
    ('NVDA', 'NVIDIA'), ('AAPL', 'Apple'), ('GOOGL', 'Alphabet (Google)'),
    ('MSFT', 'Microsoft'), ('AMZN', 'Amazon'), ('AMD', 'AMD'),
    ('AVGO', 'Broadcom'), ('TSLA', 'Tesla'), ('META', 'Meta'),
    ('INTC', 'Intel'), ('CSCO', 'Cisco'), ('AMAT', 'Applied Materials'),
    ('COST', 'Costco'), ('LRCX', 'Lam Research'), ('NFLX', 'Netflix'),
    ('PLTR', 'Palantir'), ('KLAC', 'KLA'), ('PANW', 'Palo Alto Networks'),
    ('TXN', 'Texas Instruments'), ('TMUS', 'T-Mobile US'), ('AMGN', 'Amgen'),
    ('CRWD', 'CrowdStrike'), ('ADI', 'Analog Devices'), ('QCOM', 'Qualcomm'),
    ('PEP', 'PepsiCo'), ('GILD', 'Gilead Sciences'), ('ASML', 'ASML'),
    ('HON', 'Honeywell'), ('BKNG', 'Booking Holdings'), ('ISRG', 'Intuitive Surgical'),
    ('VRTX', 'Vertex Pharmaceuticals'), ('SBUX', 'Starbucks'), ('ADBE', 'Adobe'),
    ('MU', 'Micron'), ('WDC', 'Western Digital'),
]
NQ_INSTRUMENTS = {
    f'NQ_{sym}': {'label': f'{name} ({sym})', 'kind': 'equity', 'ticker': sym}
    for sym, name in NQ_TICKERS
}


def build_crypto_universe(n=TOP_CRYPTO_N):
    """Топ-N пар к USDT на выбранной бирже по реальному 24ч объёму — считается на старте,
    а не хардкодится, потому что ранжирование крипты по капитализации/объёму
    меняется слишком быстро, чтобы зашивать статический список в код."""
    must_include = ['BTC/USDT', 'ETH/USDT', 'SOL/USDT']
    try:
        markets = crypto_spot.load_markets()
        tickers = crypto_spot.fetch_tickers()
        pairs = []
        for symbol, m in markets.items():
            if m.get('quote') == 'USDT' and m.get('spot') and m.get('active', True):
                t = tickers.get(symbol)
                vol = (t or {}).get('quoteVolume')
                if vol:
                    pairs.append((symbol, vol))
        pairs.sort(key=lambda x: x[1], reverse=True)
        top_symbols = [s for s, _ in pairs[:n]]
        for m in must_include:
            if m not in top_symbols:
                top_symbols.insert(0, m)
        universe = {}
        for symbol in top_symbols:
            base = symbol.split('/')[0]
            universe[f'CR_{base}'] = {'label': symbol, 'kind': 'crypto', 'ticker': symbol}
        return universe
    except Exception as e:
        print(f"Не удалось получить топ крипты с биржи, использую фиксированный набор: {e}")
        fallback = ['BTC/USDT', 'ETH/USDT', 'SOL/USDT', 'BNB/USDT', 'XRP/USDT', 'ADA/USDT', 'DOGE/USDT']
        return {f'CR_{s.split("/")[0]}': {'label': s, 'kind': 'crypto', 'ticker': s} for s in fallback}


CR_INSTRUMENTS = build_crypto_universe()

AVAILABLE_INSTRUMENTS = {**FX_INSTRUMENTS, **MT_INSTRUMENTS, **CR_INSTRUMENTS, **NQ_INSTRUMENTS}

INSTRUMENT_CATEGORIES = {
    'fx': {'title': 'Форекс (мажоры)', 'items': FX_INSTRUMENTS},
    'mt': {'title': 'Металлы', 'items': MT_INSTRUMENTS},
    'cr': {'title': f'Крипто (топ {len(CR_INSTRUMENTS)} по объёму, {CRYPTO_EXCHANGE_ID})', 'items': CR_INSTRUMENTS},
    'nq': {'title': 'NASDAQ (популярные)', 'items': NQ_INSTRUMENTS},
}

DEFAULT_SETTINGS = {
    'symbols': ['FX_EURUSD', 'FX_GBPUSD', 'MT_XAUUSD_GCF', 'CR_BTC', 'CR_ETH', 'CR_SOL'],
    'enabled_strategies': ['smc_sweep_fvg'],
    'smc_trigger_sweep': True,
    'smc_trigger_fvg': True,
    'notify_always': False,
    'scanning_enabled': True,
}


# =========================================================================
# БАЗА ДАННЫХ
# =========================================================================
def init_db():
    conn = sqlite3.connect(DB_PATH)
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


def get_user_settings(chat_id):
    conn = sqlite3.connect(DB_PATH)
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


def save_user_settings(chat_id, **updates):
    current = get_user_settings(chat_id)
    current.update(updates)
    conn = sqlite3.connect(DB_PATH)
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


def toggle_symbol(chat_id, symbol_key):
    settings = get_user_settings(chat_id)
    symbols = set(settings['symbols'])
    if symbol_key in symbols:
        symbols.discard(symbol_key)
    else:
        symbols.add(symbol_key)
    return save_user_settings(chat_id, symbols=list(symbols))


def toggle_bool_setting(chat_id, key):
    settings = get_user_settings(chat_id)
    return save_user_settings(chat_id, **{key: not settings[key]})


def toggle_strategy(chat_id, strategy_id):
    settings = get_user_settings(chat_id)
    enabled = set(settings['enabled_strategies'])
    if strategy_id in enabled:
        enabled.discard(strategy_id)
    else:
        enabled.add(strategy_id)
    return save_user_settings(chat_id, enabled_strategies=list(enabled))


def get_last_alert_time(alert_key):
    conn = sqlite3.connect(DB_PATH)
    row = conn.execute("SELECT signal_time FROM sent_alerts WHERE alert_key = ?", (alert_key,)).fetchone()
    conn.close()
    return row[0] if row else None


def set_last_alert_time(alert_key, signal_time_str):
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """
        INSERT INTO sent_alerts (alert_key, signal_time, sent_at) VALUES (?, ?, ?)
        ON CONFLICT(alert_key) DO UPDATE SET signal_time = excluded.signal_time, sent_at = excluded.sent_at
        """,
        (alert_key, signal_time_str, datetime.now(timezone.utc).isoformat()),
    )
    conn.commit()
    conn.close()


def get_update_offset():
    conn = sqlite3.connect(DB_PATH)
    row = conn.execute("SELECT value FROM meta WHERE key = 'update_offset'").fetchone()
    conn.close()
    return int(row[0]) if row else 0


def set_update_offset(offset):
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        "INSERT INTO meta (key, value) VALUES ('update_offset', ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (str(offset),),
    )
    conn.commit()
    conn.close()


# =========================================================================
# ОБЩИЕ ИНДИКАТОРЫ
# =========================================================================
def find_fvg(df):
    fvg_list = []
    for i in range(2, len(df)):
        if df['Low'].iloc[i] > df['High'].iloc[i - 2]:
            fvg_list.append({'type': 'Bullish FVG', 'top': df['Low'].iloc[i],
                              'bottom': df['High'].iloc[i - 2], 'time': df.index[i]})
        elif df['High'].iloc[i] < df['Low'].iloc[i - 2]:
            fvg_list.append({'type': 'Bearish FVG', 'top': df['Low'].iloc[i - 2],
                              'bottom': df['High'].iloc[i], 'time': df.index[i]})
    return fvg_list


def check_sweep(df):
    if len(df) < 10:
        return None
    recent_high = df['High'].iloc[-10:-1].max()
    recent_low = df['Low'].iloc[-10:-1].min()
    current_high = df['High'].iloc[-1]
    current_low = df['Low'].iloc[-1]
    current_close = df['Close'].iloc[-1]

    if current_high > recent_high and current_close < recent_high:
        return 'Bearish Sweep'
    if current_low < recent_low and current_close > recent_low:
        return 'Bullish Sweep'
    return None


def check_4h_fvg_trigger(df_4h):
    fvgs = find_fvg(df_4h)
    if not fvgs:
        return None
    last = fvgs[-1]
    if df_4h.index.get_loc(last['time']) >= len(df_4h) - 2:
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
        window = df.iloc[i - lookback:i + lookback + 1]
        if df['High'].iloc[i] == window['High'].max():
            highs.append((df.index[i], df['High'].iloc[i]))
        if df['Low'].iloc[i] == window['Low'].min():
            lows.append((df.index[i], df['Low'].iloc[i]))
    return highs, lows


# =========================================================================
# СТРАТЕГИИ
# Каждая функция принимает (df_4h, df_15m, settings) и возвращает
# либо None (сетапа нет), либо {'time', 'direction', 'trigger'}.
# =========================================================================
def detect_smc_sweep_fvg(df_4h, df_15m, settings):
    """4H Sweep и/или 4H FVG (независимые тумблеры) → подтверждение свежим 15M FVG в ту же сторону."""
    active_triggers = []

    if settings.get('smc_trigger_sweep', True):
        sweep = check_sweep(df_4h)
        if sweep:
            direction = 'Bullish' if sweep == 'Bullish Sweep' else 'Bearish'
            active_triggers.append(('Sweep', direction))

    if settings.get('smc_trigger_fvg', True):
        fvg4h = check_4h_fvg_trigger(df_4h)
        if fvg4h:
            direction = 'Bullish' if fvg4h['type'] == 'Bullish FVG' else 'Bearish'
            active_triggers.append(('FVG', direction))

    if not active_triggers:
        return None

    fvg_15m_list = find_fvg(df_15m)
    if not fvg_15m_list:
        return None
    last_fvg_15m = fvg_15m_list[-1]
    if df_15m.index.get_loc(last_fvg_15m['time']) < len(df_15m) - 3:
        return None

    direction_15m = 'Bullish' if last_fvg_15m['type'] == 'Bullish FVG' else 'Bearish'
    matching = [label for label, d in active_triggers if d == direction_15m]
    if not matching:
        return None

    direction_ru = "Лонг" if direction_15m == 'Bullish' else "Шорт"
    return {'time': last_fvg_15m['time'], 'direction': direction_ru, 'trigger': " + ".join(matching)}


def detect_bos(df_4h, df_15m, settings):
    """Break of Structure: пробой последнего значимого свинга на 15M."""
    if len(df_15m) < 20:
        return None
    highs, lows = find_swings(df_15m, lookback=3)
    if not highs or not lows:
        return None
    last_high = highs[-1][1]
    last_low = lows[-1][1]
    last_close = df_15m['Close'].iloc[-1]
    last_time = df_15m.index[-1]

    if last_close > last_high:
        return {'time': last_time, 'direction': 'Лонг', 'trigger': 'BOS (пробой хая)'}
    if last_close < last_low:
        return {'time': last_time, 'direction': 'Шорт', 'trigger': 'BOS (пробой лоя)'}
    return None


def detect_order_block(df_4h, df_15m, settings):
    """Ретест ордер-блока: последняя противоположная свеча перед импульсным движением,
    к которой цена вернулась в ближайших барах."""
    if len(df_15m) < 25:
        return None
    body = (df_15m['Close'] - df_15m['Open']).abs()
    avg_body = body.rolling(20).mean()

    for i in range(len(df_15m) - 1, max(len(df_15m) - 8, 20), -1):
        if pd.isna(avg_body.iloc[i]) or avg_body.iloc[i] == 0:
            continue
        if body.iloc[i] <= 1.8 * avg_body.iloc[i]:
            continue

        impulse_bullish = df_15m['Close'].iloc[i] > df_15m['Open'].iloc[i]
        ob_index = i - 1
        while ob_index > 0:
            is_opposite = (df_15m['Close'].iloc[ob_index] < df_15m['Open'].iloc[ob_index]) if impulse_bullish \
                else (df_15m['Close'].iloc[ob_index] > df_15m['Open'].iloc[ob_index])
            if is_opposite:
                break
            ob_index -= 1
        if ob_index <= 0:
            continue

        ob_high = df_15m['High'].iloc[ob_index]
        ob_low = df_15m['Low'].iloc[ob_index]
        last_low = df_15m['Low'].iloc[-1]
        last_high = df_15m['High'].iloc[-1]
        touched = last_low <= ob_high and last_high >= ob_low

        if touched:
            direction = 'Лонг' if impulse_bullish else 'Шорт'
            return {'time': df_15m.index[-1], 'direction': direction, 'trigger': 'Order Block retest'}

    return None


def detect_ema_pullback(df_4h, df_15m, settings):
    """Тренд по 4H EMA50, вход от отбоя от 15M EMA21 в сторону тренда. Классический
    трендследящий сетап без концепций SMC."""
    if len(df_4h) < 50 or len(df_15m) < 25:
        return None

    ema50_4h = compute_ema(df_4h['Close'], 50)
    trend_up = ema50_4h.iloc[-1] > ema50_4h.iloc[-5]
    trend_down = ema50_4h.iloc[-1] < ema50_4h.iloc[-5]

    ema21_15m = compute_ema(df_15m['Close'], 21)
    last_close = df_15m['Close'].iloc[-1]
    last_low = df_15m['Low'].iloc[-1]
    last_high = df_15m['High'].iloc[-1]
    last_ema = ema21_15m.iloc[-1]

    if trend_up and last_low <= last_ema < last_close:
        return {'time': df_15m.index[-1], 'direction': 'Лонг', 'trigger': 'EMA21 pullback (аптренд)'}
    if trend_down and last_high >= last_ema > last_close:
        return {'time': df_15m.index[-1], 'direction': 'Шорт', 'trigger': 'EMA21 pullback (даунтренд)'}
    return None


def detect_rsi_reversal(df_4h, df_15m, settings):
    """RSI(14) выходит из зоны перепроданности/перекупленности на 15M — mean-reversion сигнал."""
    if len(df_15m) < 20:
        return None
    rsi = compute_rsi(df_15m['Close'], 14)
    if rsi.iloc[-2:].isna().any():
        return None
    prev_rsi = rsi.iloc[-2]
    last_rsi = rsi.iloc[-1]

    if prev_rsi < 30 <= last_rsi:
        return {'time': df_15m.index[-1], 'direction': 'Лонг', 'trigger': 'RSI выход из перепроданности'}
    if prev_rsi > 70 >= last_rsi:
        return {'time': df_15m.index[-1], 'direction': 'Шорт', 'trigger': 'RSI выход из перекупленности'}
    return None


STRATEGIES = {
    'smc_sweep_fvg':  {'label': 'SMC: 4H Sweep/FVG + 15M FVG', 'detect': detect_smc_sweep_fvg, 'configurable': True},
    'bos':            {'label': 'Break of Structure (15M)', 'detect': detect_bos, 'configurable': False},
    'order_block':    {'label': 'Order Block retest (15M)', 'detect': detect_order_block, 'configurable': False},
    'ema_pullback':   {'label': 'EMA21 Pullback по тренду 4H', 'detect': detect_ema_pullback, 'configurable': False},
    'rsi_reversal':   {'label': 'RSI(14) реверсал (15M)', 'detect': detect_rsi_reversal, 'configurable': False},
}


# =========================================================================
# ДАННЫЕ РЫНКА
# =========================================================================
def is_forex_market_open():
    now = datetime.now(timezone.utc)
    if now.weekday() == 5:
        return False
    if now.weekday() == 6 and now.hour < 22:
        return False
    if now.weekday() == 4 and now.hour >= 22:
        return False
    return True


def is_us_equity_market_open():
    """Обычная сессия NASDAQ 9:30–16:00 по Нью-Йорку, будни."""
    if NY_TZ is not None:
        now_ny = datetime.now(NY_TZ)
        if now_ny.weekday() >= 5:
            return False
        minutes = now_ny.hour * 60 + now_ny.minute
        return 9 * 60 + 30 <= minutes <= 16 * 60
    # запасной вариант без базы часовых поясов: грубая оценка по UTC без
    # учёта перехода на летнее/зимнее время (возможна погрешность до часа)
    now = datetime.now(timezone.utc)
    if now.weekday() >= 5:
        return False
    minutes = now.hour * 60 + now.minute
    return 13 * 60 + 30 <= minutes <= 21 * 60


def get_yfinance_data(ticker_symbol, timeframe):
    interval = '15m' if timeframe == '15M' else '60m'
    period = '5d' if timeframe == '15M' else '1mo'

    df = yf.download(tickers=ticker_symbol, period=period, interval=interval, progress=False)
    if df is None or df.empty:
        return None

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    df = df[['Open', 'High', 'Low', 'Close', 'Volume']].dropna()
    if df.empty:
        return None

    if timeframe == '4H':
        df = df.resample('4h').agg({
            'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum',
        }).dropna()

    return df


def get_crypto_spot_data(symbol, timeframe):
    tf_map = {'15M': '15m', '4H': '4h'}
    ohlcv = crypto_spot.fetch_ohlcv(symbol, tf_map[timeframe], limit=100)
    df = pd.DataFrame(ohlcv, columns=['time', 'Open', 'High', 'Low', 'Close', 'Volume'])
    df['time'] = pd.to_datetime(df['time'], unit='ms')
    df.set_index('time', inplace=True)
    return df


def resolve_perp_symbol(preferred_ticker):
    if preferred_ticker in _perp_symbol_cache:
        return _perp_symbol_cache[preferred_ticker]
    markets = crypto_perp.load_markets()
    candidates = [preferred_ticker, f"{preferred_ticker[:3]}/{preferred_ticker[3:]}:USDT"]
    for c in candidates:
        if c in markets:
            _perp_symbol_cache[preferred_ticker] = c
            return c
    base_guess = preferred_ticker.replace('USDT', '').replace('/', '')
    fallback = [s for s in markets if base_guess in s.upper() and 'USDT' in s.upper()]
    if fallback:
        _perp_symbol_cache[preferred_ticker] = fallback[0]
        return fallback[0]
    raise RuntimeError(f"Не найден символ '{preferred_ticker}' на выбранной бирже (perp/swap)")


def get_crypto_perp_data(preferred_ticker, timeframe):
    tf_map = {'15M': '15m', '4H': '4h'}
    symbol = resolve_perp_symbol(preferred_ticker)
    ohlcv = crypto_perp.fetch_ohlcv(symbol, tf_map[timeframe], limit=100)
    df = pd.DataFrame(ohlcv, columns=['time', 'Open', 'High', 'Low', 'Close', 'Volume'])
    df['time'] = pd.to_datetime(df['time'], unit='ms')
    df.set_index('time', inplace=True)
    return df


def fetch_pair_data(instrument_key):
    info = AVAILABLE_INSTRUMENTS[instrument_key]
    kind = info['kind']
    ticker = info['ticker']

    if kind == 'forex':
        if not is_forex_market_open():
            return None, None, 'market_closed'
        df_4h = get_yfinance_data(ticker, '4H')
        df_15m = get_yfinance_data(ticker, '15M')
    elif kind == 'equity':
        if not is_us_equity_market_open():
            return None, None, 'market_closed'
        df_4h = get_yfinance_data(ticker, '4H')
        df_15m = get_yfinance_data(ticker, '15M')
    elif kind == 'crypto':
        df_4h = get_crypto_spot_data(ticker, '4H')
        df_15m = get_crypto_spot_data(ticker, '15M')
    elif kind == 'crypto_perp':
        df_4h = get_crypto_perp_data(ticker, '4H')
        df_15m = get_crypto_perp_data(ticker, '15M')
    else:
        return None, None, 'unknown_kind'

    return df_4h, df_15m, None


# =========================================================================
# ГРАФИКИ И TELEGRAM
# =========================================================================
def render_chart(df, title, filename):
    mc = mpf.make_marketcolors(up='#26a69a', down='#ef5350', edge='inherit', wick='inherit', volume='in')
    style = mpf.make_mpf_style(base_mpf_style='nightclouds', marketcolors=mc, gridcolor='#2a2e39', facecolor='#131722')
    mpf.plot(df.tail(40), type='candle', style=style, title=f"\n{title}",
              savefig=filename, volume=False, figratio=(12, 7))


def tg_post(method, payload=None, files=None, data=None):
    url = f"{API_URL}/{method}"
    try:
        if files:
            resp = requests.post(url, data=data, files=files, timeout=20)
        else:
            resp = requests.post(url, json=payload, timeout=15)
        if resp.status_code != 200:
            print(f"Telegram {method} error {resp.status_code}: {resp.text}")
        return resp.json()
    except Exception as e:
        print(f"Telegram {method} exception: {e}")
        return None


def send_message(chat_id, text, reply_markup=None):
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
    if reply_markup is not None:
        payload["reply_markup"] = reply_markup
    return tg_post("sendMessage", payload=payload)


def edit_message_text(chat_id, message_id, text, reply_markup=None):
    payload = {"chat_id": chat_id, "message_id": message_id, "text": text, "parse_mode": "HTML"}
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


def send_telegram_media_group(photos, caption):
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
    try:
        tg_post("sendMediaGroup", files=files, data=data)
    finally:
        for f in files.values():
            f.close()


# =========================================================================
# КЛАВИАТУРЫ
# =========================================================================
GLOBAL_SETUP_TEXT = (
    "🎯 <b>Панель управления</b>\n\n"
    "Здесь можно включить/выключить сканирование целиком, настроить стратегии "
    "и выбрать инструменты для наблюдения."
)


def build_global_setup_keyboard(chat_id):
    s = get_user_settings(chat_id)
    scan_label = "🟢 Сканирование: включено" if s['scanning_enabled'] else "🔴 Сканирование: выключено"
    notify_mark = "✅" if s['notify_always'] else "⬜"
    rows = [
        [{"text": "Стратегии →", "callback_data": "goto_strategies"}],
        [{"text": "Инструменты →", "callback_data": "instr_categories"}],
        [{"text": f"{notify_mark} Уведомлять всегда", "callback_data": "toggle_notify_always"}],
        [{"text": scan_label, "callback_data": "toggle_scanning"}],
    ]
    return {"inline_keyboard": rows}


def build_strategies_keyboard(chat_id):
    s = get_user_settings(chat_id)
    enabled = set(s['enabled_strategies'])
    rows = []
    for sid, info in STRATEGIES.items():
        mark = "✅" if sid in enabled else "⬜"
        row = [{"text": f"{mark} {info['label']}", "callback_data": f"strat:{sid}"}]
        if info.get('configurable'):
            row.append({"text": "⚙️", "callback_data": f"stratcfg:{sid}"})
        rows.append(row)
    rows.append([{"text": "< Назад", "callback_data": "setup_back"}])
    return {"inline_keyboard": rows}


def build_smc_cfg_keyboard(chat_id):
    s = get_user_settings(chat_id)
    sweep_mark = "✅" if s['smc_trigger_sweep'] else "⬜"
    fvg_mark = "✅" if s['smc_trigger_fvg'] else "⬜"
    rows = [
        [{"text": f"{sweep_mark} Sweep (4H)", "callback_data": "smctrig:sweep"},
         {"text": f"{fvg_mark} FVG (4H)", "callback_data": "smctrig:fvg"}],
        [{"text": "< К стратегиям", "callback_data": "goto_strategies"}],
    ]
    return {"inline_keyboard": rows}


def build_category_menu_keyboard(chat_id):
    settings = get_user_settings(chat_id)
    selected = set(settings['symbols'])
    rows = []
    for code, cat in INSTRUMENT_CATEGORIES.items():
        count_sel = sum(1 for k in cat['items'] if k in selected)
        rows.append([{"text": f"{cat['title']} ({count_sel}/{len(cat['items'])})", "callback_data": f"cat:{code}:0"}])
    rows.append([{"text": "< Назад", "callback_data": "setup_back"}])
    return {"inline_keyboard": rows}


def build_category_page_keyboard(chat_id, code, page):
    settings = get_user_settings(chat_id)
    selected = set(settings['symbols'])
    items = list(INSTRUMENT_CATEGORIES[code]['items'].items())
    start = page * PAGE_SIZE
    page_items = items[start:start + PAGE_SIZE]

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
def handle_command(chat_id, text):
    text = text.strip()
    if text in ("/start", "/help"):
        send_message(chat_id,
            "🎯 <b>Market Setup Hunter</b>\n\n"
            "/setup — панель управления (сканирование, стратегии, инструменты)\n"
            "/strategies — выбрать активные стратегии\n"
            "/instruments — выбрать инструменты для наблюдения\n"
            "/status — текущие настройки\n"
            "/example — пример алерта"
        )
    elif text == "/setup":
        send_message(chat_id, GLOBAL_SETUP_TEXT, reply_markup=build_global_setup_keyboard(chat_id))
    elif text == "/strategies":
        send_message(chat_id, "Выбери активные стратегии (можно несколько):",
                      reply_markup=build_strategies_keyboard(chat_id))
    elif text == "/instruments":
        send_message(chat_id, "Выбери категорию инструментов:",
                      reply_markup=build_category_menu_keyboard(chat_id))
    elif text == "/example":
        send_message(chat_id, EXAMPLE_TEXT)
    elif text == "/status":
        s = get_user_settings(chat_id)
        strat_txt = "\n".join(
            f"• {'✅' if sid in s['enabled_strategies'] else '❌'} {info['label']}"
            for sid, info in STRATEGIES.items()
        )
        symbols_txt = "\n".join(f"• {AVAILABLE_INSTRUMENTS[k]['label']}" for k in s['symbols']
                                 if k in AVAILABLE_INSTRUMENTS) or "(ничего не выбрано)"
        send_message(chat_id,
            f"<b>Сканирование:</b> {'включено 🟢' if s['scanning_enabled'] else 'выключено 🔴'}\n"
            f"<b>Уведомлять всегда:</b> {'да' if s['notify_always'] else 'нет'}\n\n"
            f"<b>Стратегии:</b>\n{strat_txt}\n\n"
            f"<b>Инструменты ({len(s['symbols'])}):</b>\n{symbols_txt}"
        )


def handle_callback(callback_query):
    chat_id = str(callback_query['message']['chat']['id'])
    message_id = callback_query['message']['message_id']
    data = callback_query['data']
    callback_id = callback_query['id']

    if data == "setup_back":
        edit_message_text(chat_id, message_id, GLOBAL_SETUP_TEXT, reply_markup=build_global_setup_keyboard(chat_id))
        answer_callback_query(callback_id)

    elif data == "goto_strategies":
        edit_message_text(chat_id, message_id, "Выбери активные стратегии (можно несколько):",
                           reply_markup=build_strategies_keyboard(chat_id))
        answer_callback_query(callback_id)

    elif data == "toggle_notify_always":
        toggle_bool_setting(chat_id, 'notify_always')
        edit_message_reply_markup(chat_id, message_id, build_global_setup_keyboard(chat_id))
        answer_callback_query(callback_id)

    elif data == "toggle_scanning":
        toggle_bool_setting(chat_id, 'scanning_enabled')
        edit_message_reply_markup(chat_id, message_id, build_global_setup_keyboard(chat_id))
        answer_callback_query(callback_id)

    elif data.startswith("strat:"):
        sid = data.split(":", 1)[1]
        toggle_strategy(chat_id, sid)
        edit_message_reply_markup(chat_id, message_id, build_strategies_keyboard(chat_id))
        answer_callback_query(callback_id)

    elif data.startswith("stratcfg:"):
        sid = data.split(":", 1)[1]
        if sid == 'smc_sweep_fvg':
            edit_message_text(chat_id, message_id, "Настройка триггеров SMC-сетапа:",
                               reply_markup=build_smc_cfg_keyboard(chat_id))
        answer_callback_query(callback_id)

    elif data.startswith("smctrig:"):
        which = data.split(":", 1)[1]
        key = 'smc_trigger_sweep' if which == 'sweep' else 'smc_trigger_fvg'
        toggle_bool_setting(chat_id, key)
        edit_message_reply_markup(chat_id, message_id, build_smc_cfg_keyboard(chat_id))
        answer_callback_query(callback_id)

    elif data == "instr_categories":
        edit_message_text(chat_id, message_id, "Выбери категорию инструментов:",
                           reply_markup=build_category_menu_keyboard(chat_id))
        answer_callback_query(callback_id)

    elif data.startswith("cat:") or data.startswith("pg:"):
        _, code, page = data.split(":")
        page = int(page)
        cat_title = INSTRUMENT_CATEGORIES[code]['title']
        edit_message_text(chat_id, message_id, f"Категория: <b>{cat_title}</b>",
                           reply_markup=build_category_page_keyboard(chat_id, code, page))
        answer_callback_query(callback_id)

    elif data.startswith("sym:"):
        _, code, symbol_key, page = data.split(":")
        toggle_symbol(chat_id, symbol_key)
        edit_message_reply_markup(chat_id, message_id, build_category_page_keyboard(chat_id, code, int(page)))
        answer_callback_query(callback_id)

    else:
        answer_callback_query(callback_id)


def run_telegram_polling():
    offset = get_update_offset()
    while True:
        try:
            resp = requests.get(f"{API_URL}/getUpdates",
                                 params={"offset": offset, "timeout": 25}, timeout=30)
            data = resp.json()
            if not data.get("ok"):
                time.sleep(3)
                continue
            for update in data.get("result", []):
                offset = update["update_id"] + 1
                set_update_offset(offset)
                if "message" in update and "text" in update["message"]:
                    chat_id = str(update["message"]["chat"]["id"])
                    handle_command(chat_id, update["message"]["text"])
                elif "callback_query" in update:
                    handle_callback(update["callback_query"])
        except Exception as e:
            print(f"Polling error: {e}")
            time.sleep(5)


# =========================================================================
# СКАНИРОВАНИЕ
# =========================================================================
def process_pair(symbol_key, df_4h, df_15m, settings):
    label = AVAILABLE_INSTRUMENTS[symbol_key]['label']

    if df_4h is None or df_15m is None or len(df_15m) < 25:
        market_data[symbol_key] = f"{label}: нет данных"
        return

    fired_statuses = []

    for sid in settings['enabled_strategies']:
        strat = STRATEGIES.get(sid)
        if not strat:
            continue
        try:
            result = strat['detect'](df_4h, df_15m, settings)
        except Exception as e:
            print(f"Ошибка стратегии {sid} на {symbol_key}: {e}")
            continue
        if result is None:
            continue

        alert_key = f"{symbol_key}:{sid}"
        signal_time_str = result['time'].isoformat()
        already_sent = get_last_alert_time(alert_key) == signal_time_str
        if already_sent and not settings['notify_always']:
            fired_statuses.append(f"{strat['label']}: уже отправлено")
            continue

        direction = result['direction']
        trigger = result['trigger']
        fired_statuses.append(f"🔥 {strat['label']}: {direction}")

        img_4h = f"{symbol_key}_{sid}_4H.png"
        img_15m = f"{symbol_key}_{sid}_15M.png"
        try:
            render_chart(df_4h, f"{label} · 4H Context", img_4h)
            render_chart(df_15m, f"{label} · {strat['label']}", img_15m)

            caption = (
                f"🎯 <b>{label}</b>\n"
                f"Стратегия: {strat['label']}\n"
                f"<b>{direction} сформирован</b>\n"
                f"Триггер: {trigger}\n"
                f"Время: {datetime.now(timezone.utc).strftime('%H:%M UTC')}\n"
            )
            send_telegram_media_group([img_4h, img_15m], caption)
            set_last_alert_time(alert_key, signal_time_str)
            print(f"✅ Алерт по {symbol_key} ({sid}) отправлен!")
        finally:
            if os.path.exists(img_4h):
                os.remove(img_4h)
            if os.path.exists(img_15m):
                os.remove(img_15m)

    market_data[symbol_key] = f"{label}: " + (" | ".join(fired_statuses) if fired_statuses else "ожидание сетапа")


def analyze_and_notify():
    print(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}] Сканирование рынка...")
    settings = get_user_settings(CHAT_ID)

    if not settings['scanning_enabled']:
        for key in settings['symbols']:
            if key in AVAILABLE_INSTRUMENTS:
                market_data[key] = f"{AVAILABLE_INSTRUMENTS[key]['label']}: сканирование выключено"
        market_data["last_update"] = datetime.now(timezone.utc).strftime('%H:%M:%S UTC')
        return

    if not settings['enabled_strategies']:
        for key in settings['symbols']:
            if key in AVAILABLE_INSTRUMENTS:
                market_data[key] = f"{AVAILABLE_INSTRUMENTS[key]['label']}: нет активных стратегий"
        market_data["last_update"] = datetime.now(timezone.utc).strftime('%H:%M:%S UTC')
        return

    for symbol_key in settings['symbols']:
        if symbol_key not in AVAILABLE_INSTRUMENTS:
            continue
        label = AVAILABLE_INSTRUMENTS[symbol_key]['label']
        market_data[symbol_key] = f"{label}: сканирование..."
        try:
            df_4h, df_15m, skip_reason = fetch_pair_data(symbol_key)
            if skip_reason == 'market_closed':
                market_data[symbol_key] = f"{label}: рынок закрыт"
                continue
            process_pair(symbol_key, df_4h, df_15m, settings)
        except Exception as e:
            market_data[symbol_key] = f"{label}: ошибка ({e})"
            print(f"Ошибка {symbol_key}: {e}")

    for stale_key in list(market_data.keys()):
        if stale_key != "last_update" and stale_key not in settings['symbols']:
            del market_data[stale_key]

    market_data["last_update"] = datetime.now(timezone.utc).strftime('%H:%M:%S UTC')


def run_scanner_background():
    time.sleep(3)
    while True:
        try:
            analyze_and_notify()
        except Exception as e:
            print(f"Scanner error: {e}")
        time.sleep(SCAN_INTERVAL_SECONDS)


# =========================================================================
# FLASK: мини-веб-апп
# =========================================================================
@app.route('/api/status')
def get_status():
    return jsonify(market_data)


# HTML вынесен в templates/index.html — Flask ищет шаблоны в папке
# "templates" рядом с файлом bot.py по умолчанию.
@app.route('/')
def home():
    return render_template('index.html')


# =========================================================================
# СТАРТ
# =========================================================================
init_db()

# ВАЖНО для Gunicorn: используй ровно ОДИН воркер (-w 1) — иначе и сканер,
# и polling апдейтов запустятся в нескольких экземплярах.
threading.Thread(target=run_scanner_background, daemon=True).start()
threading.Thread(target=run_telegram_polling, daemon=True).start()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
