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
from flask import Flask, jsonify

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

# =========================================================================
# РЕЕСТР ДОСТУПНЫХ ИНСТРУМЕНТОВ
# =========================================================================
AVAILABLE_INSTRUMENTS = {
    'EURUSD':        {'label': 'EUR/USD',                  'kind': 'forex',       'ticker': 'EURUSD=X'},
    'GBPUSD':        {'label': 'GBP/USD',                  'kind': 'forex',       'ticker': 'GBPUSD=X'},
    'XAUUSD_GCF':    {'label': 'XAU/USD (COMEX фьючерс)',  'kind': 'forex',       'ticker': 'GC=F'},
    'XAUUSDT_PERP':  {'label': 'XAU/USDT (Binance перп)',  'kind': 'crypto_perp', 'ticker': 'XAUUSDT'},
    'BTCUSDT':       {'label': 'BTC/USDT',                 'kind': 'crypto',      'ticker': 'BTC/USDT'},
    'ETHUSDT':       {'label': 'ETH/USDT',                 'kind': 'crypto',      'ticker': 'ETH/USDT'},
    'SOLUSDT':       {'label': 'SOL/USDT',                 'kind': 'crypto',      'ticker': 'SOL/USDT'},
}

DEFAULT_SETTINGS = {
    'symbols': ['EURUSD', 'GBPUSD', 'XAUUSD_GCF', 'BTCUSDT', 'ETHUSDT', 'SOLUSDT'],
    'trigger_sweep': True,
    'trigger_fvg': True,
    'notify_always': False,
    'setup_enabled': True,
}

binance_spot = ccxt.binance()
binance_futures = ccxt.binanceusdm()
_perp_symbol_cache = {}

market_data = {"last_update": "Только что"}


# =========================================================================
# БАЗА ДАННЫХ
# =========================================================================
def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sent_alerts (
            symbol TEXT PRIMARY KEY,
            fvg_time TEXT NOT NULL,
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
        settings.update(json.loads(row[0]))  # на случай если позже добавятся новые поля
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


def get_last_alert_time(symbol):
    conn = sqlite3.connect(DB_PATH)
    row = conn.execute("SELECT fvg_time FROM sent_alerts WHERE symbol = ?", (symbol,)).fetchone()
    conn.close()
    return row[0] if row else None


def set_last_alert_time(symbol, fvg_time_str):
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """
        INSERT INTO sent_alerts (symbol, fvg_time, sent_at) VALUES (?, ?, ?)
        ON CONFLICT(symbol) DO UPDATE SET fvg_time = excluded.fvg_time, sent_at = excluded.sent_at
        """,
        (symbol, fvg_time_str, datetime.now(timezone.utc).isoformat()),
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
# ЛОГИКА СЕТАПА: 4H-триггер (Sweep и/или FVG, независимо) + 15M FVG-подтверждение
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
    """Свежий (в последних 2 свечах) FVG на 4H — второй тип триггера, независимый от sweep."""
    fvgs = find_fvg(df_4h)
    if not fvgs:
        return None
    last = fvgs[-1]
    if df_4h.index.get_loc(last['time']) >= len(df_4h) - 2:
        return last
    return None


def evaluate_setup(df_4h, df_15m, settings):
    """Единая логика: 4H Sweep и/или 4H FVG — независимые тумблеры-триггеры.
    Направление входа определяется 15M FVG-подтверждением в ту же сторону,
    что и хотя бы один сработавший 4H-триггер."""
    active_triggers = []  # [(label, 'Bullish'/'Bearish'), ...]

    if settings.get('trigger_sweep', True):
        sweep = check_sweep(df_4h)
        if sweep:
            direction = 'Bullish' if sweep == 'Bullish Sweep' else 'Bearish'
            active_triggers.append(('Sweep', direction))

    if settings.get('trigger_fvg', True):
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
    trigger_label = " + ".join(matching)
    return {'time': last_fvg_15m['time'], 'direction': direction_ru, 'trigger': trigger_label}


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


def get_forex_data(ticker_symbol, timeframe):
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
    ohlcv = binance_spot.fetch_ohlcv(symbol, tf_map[timeframe], limit=100)
    df = pd.DataFrame(ohlcv, columns=['time', 'Open', 'High', 'Low', 'Close', 'Volume'])
    df['time'] = pd.to_datetime(df['time'], unit='ms')
    df.set_index('time', inplace=True)
    return df


def resolve_perp_symbol(preferred_ticker):
    if preferred_ticker in _perp_symbol_cache:
        return _perp_symbol_cache[preferred_ticker]

    markets = binance_futures.load_markets()

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

    raise RuntimeError(f"Не найден символ '{preferred_ticker}' на Binance Futures")


def get_crypto_perp_data(preferred_ticker, timeframe):
    tf_map = {'15M': '15m', '4H': '4h'}
    symbol = resolve_perp_symbol(preferred_ticker)
    ohlcv = binance_futures.fetch_ohlcv(symbol, tf_map[timeframe], limit=100)
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
        df_4h = get_forex_data(ticker, '4H')
        df_15m = get_forex_data(ticker, '15M')
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
# ГРАФИКИ И ОТПРАВКА В TELEGRAM
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
# ЭКРАНЫ НАСТРОЕК
# =========================================================================
SETUP_DESCRIPTION = (
    "🔷 <b>4H Trigger + 15M FVG</b>\n"
    "Multi-timeframe setup · 4H-триггер, подтверждённый 15M-имбалансом\n\n"
    "<b>Что это:</b>\n"
    "Следит за 4H-триггерами и присылает алерт, когда после триггера появляется "
    "свежий 15M Fair Value Gap в том же направлении.\n\n"
    "<b>Как это работает:</b>\n"
    "• Бычий 4H-триггер → бычий 15M FVG → потенциальный лонг\n"
    "• Медвежий 4H-триггер → медвежий 15M FVG → потенциальный шорт\n\n"
    "<b>Режимы триггера (можно включить оба):</b>\n"
    "• Sweep — триггер по 4H Liquidity Sweep\n"
    "• FVG — триггер по свежему 4H Fair Value Gap\n\n"
    "<b>Подтверждение:</b> первый подходящий 15M FVG завершает сетап — "
    "один алерт на один триггер.\n\n"
    "⚠️ Это алерт сетапа, не торговый сигнал. Проверяй общий контекст рынка "
    "перед входом."
)

EXAMPLE_TEXT = (
    "📋 <b>Пример алерта</b>\n\n"
    "🎯 <b>XAUUSD_GCF</b>\n"
    "<b>Лонг сформирован</b>\n"
    "Триггер: Sweep + FVG\n"
    "Время: 08:35 UTC\n\n"
    "К алерту прикладываются два графика: 4H-контекст и 15M-триггер."
)


def build_setup_keyboard(chat_id):
    s = get_user_settings(chat_id)
    covered = len(s['symbols'])
    total = len(AVAILABLE_INSTRUMENTS)
    sweep_mark = "✅" if s['trigger_sweep'] else "⬜"
    fvg_mark = "✅" if s['trigger_fvg'] else "⬜"
    notify_mark = "✅" if s['notify_always'] else "⬜"
    enabled = s['setup_enabled']

    rows = [
        [{"text": f"{sweep_mark} Sweep", "callback_data": "trig:sweep"},
         {"text": f"{fvg_mark} FVG", "callback_data": "trig:fvg"}],
        [{"text": f"Мои инструменты ({covered} из {total})", "callback_data": "goto_instruments"}],
        [{"text": f"{notify_mark} Уведомлять всегда", "callback_data": "toggle_notify_always"}],
        [{"text": "Пример", "callback_data": "show_example"}],
        [{"text": "🔴 Выключить сетап" if enabled else "🟢 Включить сетап", "callback_data": "toggle_enabled"}],
    ]
    return {"inline_keyboard": rows}


def build_instruments_keyboard(chat_id):
    settings = get_user_settings(chat_id)
    selected = set(settings['symbols'])
    rows = []
    for key, info in AVAILABLE_INSTRUMENTS.items():
        mark = "✅" if key in selected else "⬜"
        rows.append([{"text": f"{mark} {info['label']}", "callback_data": f"sym:{key}"}])
    rows.append([{"text": "< Назад к сетапу", "callback_data": "back_to_setup"}])
    return {"inline_keyboard": rows}


# =========================================================================
# ОБРАБОТКА КОМАНД И КНОПОК
# =========================================================================
def handle_command(chat_id, text):
    text = text.strip()
    if text in ("/start", "/help"):
        send_message(chat_id,
            "🎯 <b>SMC Setup Hunter</b>\n\n"
            "/setup — настроить сетап (триггеры, уведомления, вкл/выкл)\n"
            "/instruments — выбрать, за какими инструментами следить\n"
            "/status — показать текущие настройки"
        )
    elif text in ("/setup", "/strategy"):
        send_message(chat_id, SETUP_DESCRIPTION, reply_markup=build_setup_keyboard(chat_id))
    elif text == "/instruments":
        send_message(chat_id, "Выбери инструменты для сканирования:",
                      reply_markup=build_instruments_keyboard(chat_id))
    elif text == "/status":
        s = get_user_settings(chat_id)
        symbols_txt = "\n".join(f"• {AVAILABLE_INSTRUMENTS[k]['label']}" for k in s['symbols']) or "(ничего не выбрано)"
        send_message(chat_id,
            f"<b>Сетап:</b> {'включён 🟢' if s['setup_enabled'] else 'выключен 🔴'}\n"
            f"<b>Триггеры:</b> Sweep {'✅' if s['trigger_sweep'] else '❌'}, "
            f"FVG {'✅' if s['trigger_fvg'] else '❌'}\n"
            f"<b>Уведомлять всегда:</b> {'да' if s['notify_always'] else 'нет'}\n\n"
            f"<b>Инструменты:</b>\n{symbols_txt}"
        )


def handle_callback(callback_query):
    chat_id = str(callback_query['message']['chat']['id'])
    message_id = callback_query['message']['message_id']
    data = callback_query['data']
    callback_id = callback_query['id']

    if data.startswith("sym:"):
        symbol_key = data.split(":", 1)[1]
        toggle_symbol(chat_id, symbol_key)
        edit_message_reply_markup(chat_id, message_id, build_instruments_keyboard(chat_id))
        answer_callback_query(callback_id)

    elif data == "back_to_setup":
        edit_message_text(chat_id, message_id, SETUP_DESCRIPTION, reply_markup=build_setup_keyboard(chat_id))
        answer_callback_query(callback_id)

    elif data == "goto_instruments":
        edit_message_text(chat_id, message_id, "Выбери инструменты для сканирования:",
                           reply_markup=build_instruments_keyboard(chat_id))
        answer_callback_query(callback_id)

    elif data == "trig:sweep":
        toggle_bool_setting(chat_id, 'trigger_sweep')
        edit_message_reply_markup(chat_id, message_id, build_setup_keyboard(chat_id))
        answer_callback_query(callback_id)

    elif data == "trig:fvg":
        toggle_bool_setting(chat_id, 'trigger_fvg')
        edit_message_reply_markup(chat_id, message_id, build_setup_keyboard(chat_id))
        answer_callback_query(callback_id)

    elif data == "toggle_notify_always":
        toggle_bool_setting(chat_id, 'notify_always')
        edit_message_reply_markup(chat_id, message_id, build_setup_keyboard(chat_id))
        answer_callback_query(callback_id)

    elif data == "toggle_enabled":
        toggle_bool_setting(chat_id, 'setup_enabled')
        edit_message_reply_markup(chat_id, message_id, build_setup_keyboard(chat_id))
        answer_callback_query(callback_id)

    elif data == "show_example":
        answer_callback_query(callback_id)
        send_message(chat_id, EXAMPLE_TEXT)

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
# ОСНОВНОЙ ЦИКЛ СКАНИРОВАНИЯ
# =========================================================================
def process_pair(symbol_key, df_4h, df_15m, settings):
    label = AVAILABLE_INSTRUMENTS[symbol_key]['label']

    if df_4h is None or df_15m is None or len(df_15m) < 5:
        market_data[symbol_key] = f"{label}: нет данных"
        return

    result = evaluate_setup(df_4h, df_15m, settings)

    if result is None:
        market_data[symbol_key] = f"{label}: ожидание сетапа"
        return

    fvg_time_str = result['time'].isoformat()
    already_sent = get_last_alert_time(symbol_key) == fvg_time_str
    if already_sent and not settings['notify_always']:
        market_data[symbol_key] = f"{label}: активен (уже отправлено)"
        return

    direction = result['direction']
    trigger = result['trigger']
    market_data[symbol_key] = f"{label}: 🔥 {direction} ({trigger})"

    img_4h = f"{symbol_key}_4H.png"
    img_15m = f"{symbol_key}_15M.png"
    try:
        render_chart(df_4h, f"{label} · 4H Context", img_4h)
        render_chart(df_15m, f"{label} · 15M Trigger", img_15m)

        caption = (
            f"🎯 <b>{label}</b>\n"
            f"<b>{direction} сформирован</b>\n"
            f"Триггер: {trigger}\n"
            f"Время: {datetime.now(timezone.utc).strftime('%H:%M UTC')}\n"
        )
        send_telegram_media_group([img_4h, img_15m], caption)
        set_last_alert_time(symbol_key, fvg_time_str)
        print(f"✅ Алерт по {symbol_key} отправлен!")
    finally:
        if os.path.exists(img_4h):
            os.remove(img_4h)
        if os.path.exists(img_15m):
            os.remove(img_15m)


def analyze_and_notify():
    print(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}] Сканирование рынка...")
    settings = get_user_settings(CHAT_ID)

    if not settings['setup_enabled']:
        for key in settings['symbols']:
            if key in AVAILABLE_INSTRUMENTS:
                market_data[key] = f"{AVAILABLE_INSTRUMENTS[key]['label']}: сетап выключен"
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
# FLASK: Веб-сервер
# =========================================================================
@app.route('/')
def home():
    return render_template('index.html')

@app.route('/api/status')
def get_status():
    return jsonify(market_data)


if __name__ == '__main__':
    init_db()
    
    # Фоновый запуск бота и сканера
    threading.Thread(target=run_telegram_polling, daemon=True).start()
    threading.Thread(target=run_scanner_background, daemon=True).start()
    
    # Запуск веб-сервера
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
