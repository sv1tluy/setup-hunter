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

# --- НАСТРОЙКИ ---
# Токен и chat_id теперь берутся ТОЛЬКО из переменных окружения.
# Никогда не хардкодь их в файле — если файл когда-нибудь попадёт в git/чат/лог,
# токен будет считаться скомпрометированным.
BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")

if not BOT_TOKEN or not CHAT_ID:
    raise RuntimeError(
        "Не заданы переменные окружения BOT_TOKEN и/или CHAT_ID. "
        "Установи их перед запуском (например: export BOT_TOKEN=... CHAT_ID=...)."
    )

SCAN_INTERVAL_SECONDS = 300  # раз в 5 минут

FOREX_SYMBOLS = {
    'EURUSD': 'EURUSD=X',
    'GBPUSD': 'GBPUSD=X',
    'XAUUSD': 'GC=F',
}

CRYPTO_SYMBOLS = ['BTC/USDT', 'ETH/USDT', 'SOL/USDT']

binance = ccxt.binance()

# Глобальное хранилище для Web App (только для отображения статуса в мини-аппе)
market_data = {
    'EURUSD': 'Инициализация...',
    'GBPUSD': 'Инициализация...',
    'XAUUSD': 'Инициализация...',
    'BTCUSDT': 'Инициализация...',
    'ETHUSDT': 'Инициализация...',
    'SOLUSDT': 'Инициализация...',
    'last_update': 'Только что',
}

# --- ПЕРСИСТЕНТНОЕ ХРАНЕНИЕ ОТПРАВЛЕННЫХ АЛЕРТОВ ---
# Нужно, чтобы:
# 1) не слать повторно алерт по одному и тому же FVG на каждом скане (свеча живёт
#    дольше, чем интервал сканирования);
# 2) не терять эту память при рестарте процесса/деплое.
DB_PATH = os.environ.get("ALERTS_DB_PATH", "alerts.db")


def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sent_alerts (
            symbol TEXT PRIMARY KEY,
            fvg_time TEXT NOT NULL,
            sent_at TEXT NOT NULL
        )
        """
    )
    conn.commit()
    conn.close()


def get_last_alert_time(symbol):
    conn = sqlite3.connect(DB_PATH)
    row = conn.execute(
        "SELECT fvg_time FROM sent_alerts WHERE symbol = ?", (symbol,)
    ).fetchone()
    conn.close()
    return row[0] if row else None


def set_last_alert_time(symbol, fvg_time_str):
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """
        INSERT INTO sent_alerts (symbol, fvg_time, sent_at)
        VALUES (?, ?, ?)
        ON CONFLICT(symbol) DO UPDATE SET fvg_time = excluded.fvg_time, sent_at = excluded.sent_at
        """,
        (symbol, fvg_time_str, datetime.now(timezone.utc).isoformat()),
    )
    conn.commit()
    conn.close()


def send_telegram(message):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": message, "parse_mode": "HTML"}
    try:
        response = requests.post(url, json=payload, timeout=10)
        if response.status_code != 200:
            print(f"Telegram sendMessage error {response.status_code}: {response.text}")
    except Exception as e:
        print(f"Error sending TG message: {e}")


def send_telegram_media_group(photos, caption):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMediaGroup"
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

    payload = {
        "chat_id": CHAT_ID,
        # json.dumps вместо str(...).replace(...) — устойчиво к апострофам/спецсимволам
        "media": json.dumps(media),
    }
    try:
        response = requests.post(url, data=payload, files=files, timeout=20)
        if response.status_code != 200:
            print(f"Telegram sendMediaGroup error {response.status_code}: {response.text}")
    except Exception as e:
        print(f"Ошибка отправки Telegram: {e}")
    finally:
        for f in files.values():
            f.close()


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
            'Open': 'first',
            'High': 'max',
            'Low': 'min',
            'Close': 'last',
            'Volume': 'sum',
        }).dropna()

    return df


def get_crypto_data(symbol, timeframe):
    tf_map = {'15M': '15m', '4H': '4h'}
    ohlcv = binance.fetch_ohlcv(symbol, tf_map[timeframe], limit=100)
    df = pd.DataFrame(ohlcv, columns=['time', 'Open', 'High', 'Low', 'Close', 'Volume'])
    df['time'] = pd.to_datetime(df['time'], unit='ms')
    df.set_index('time', inplace=True)
    return df


def find_fvg(df):
    fvg_list = []
    for i in range(2, len(df)):
        if df['Low'].iloc[i] > df['High'].iloc[i - 2]:
            fvg_list.append({
                'type': 'Bullish FVG',
                'top': df['Low'].iloc[i],
                'bottom': df['High'].iloc[i - 2],
                'time': df.index[i],
            })
        elif df['High'].iloc[i] < df['Low'].iloc[i - 2]:
            fvg_list.append({
                'type': 'Bearish FVG',
                'top': df['Low'].iloc[i - 2],
                'bottom': df['High'].iloc[i],
                'time': df.index[i],
            })
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


def render_chart(df, title, filename):
    mc = mpf.make_marketcolors(up='#26a69a', down='#ef5350', edge='inherit', wick='inherit', volume='in')
    style = mpf.make_mpf_style(base_mpf_style='nightclouds', marketcolors=mc, gridcolor='#2a2e39', facecolor='#131722')

    mpf.plot(
        df.tail(40),
        type='candle',
        style=style,
        title=f"\n{title}",
        savefig=filename,
        volume=False,
        figratio=(12, 7),
    )


def process_pair(symbol, df_4h, df_15m):
    if df_4h is None or df_15m is None or len(df_15m) < 5:
        market_data[symbol] = "Нет данных"
        return

    sweep_4h = check_sweep(df_4h)
    fvg_15m = find_fvg(df_15m)

    if not fvg_15m:
        market_data[symbol] = "Ожидание FVG"
        return

    last_fvg = fvg_15m[-1]
    fvg_time_str = last_fvg['time'].isoformat()

    # Сетап ещё "свежий" (сформировался в последних 3 свечах)?
    is_recent = df_15m.index.get_loc(last_fvg['time']) >= len(df_15m) - 3
    if not is_recent:
        market_data[symbol] = "Активен (ожидание)"
        return

    # Уже отправляли алерт именно по этому FVG? Тогда молчим.
    if get_last_alert_time(symbol) == fvg_time_str:
        market_data[symbol] = "Активен (уже отправлено)"
        return

    trigger = "Sweep" if sweep_4h else "FVG"
    direction = "Лонг" if last_fvg['type'] == 'Bullish FVG' else "Шорт"

    market_data[symbol] = f"🔥 {direction} ({trigger})"

    img_4h = f"{symbol}_4H.png"
    img_15m = f"{symbol}_15M.png"

    try:
        render_chart(df_4h, f"{symbol} · 4H Context", img_4h)
        render_chart(df_15m, f"{symbol} · 15M Trigger", img_15m)

        caption = (
            f"🎯 <b>{symbol} · 4H Trigger + 15M FVG</b>\n"
            f"<b>{direction} сформирован</b>\n"
            f"Триггер: {trigger}\n"
            f"Время: {datetime.now(timezone.utc).strftime('%H:%M UTC')}\n"
        )

        send_telegram_media_group([img_4h, img_15m], caption)
        set_last_alert_time(symbol, fvg_time_str)
        print(f"✅ Алерт по {symbol} отправлен!")
    finally:
        if os.path.exists(img_4h):
            os.remove(img_4h)
        if os.path.exists(img_15m):
            os.remove(img_15m)


def analyze_and_notify():
    print(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}] Сканирование рынка...")

    for name, ticker in FOREX_SYMBOLS.items():
        market_data[name] = "Сканирование..."
        try:
            df_4h = get_forex_data(ticker, '4H')
            df_15m = get_forex_data(ticker, '15M')
            process_pair(name, df_4h, df_15m)
        except Exception as e:
            market_data[name] = f"Ошибка: {e}"
            print(f"Ошибка {name}: {e}")

    for pair in CRYPTO_SYMBOLS:
        name = pair.replace('/', '')
        market_data[name] = "Сканирование..."
        try:
            df_4h = get_crypto_data(pair, '4H')
            df_15m = get_crypto_data(pair, '15M')
            process_pair(name, df_4h, df_15m)
        except Exception as e:
            market_data[name] = f"Ошибка: {e}"
            print(f"Ошибка {pair}: {e}")

    market_data["last_update"] = datetime.now(timezone.utc).strftime('%H:%M:%S UTC')


# --- Flask Routes ---
@app.route('/api/status')
def get_status():
    return jsonify(market_data)


WEBAPP_HTML = """
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>SMC Setup Hunter</title>
    <script src="https://telegram.org/js/telegram-web-app.js"></script>
    <style>
        body { background-color: #0f172a; color: #f8fafc; font-family: -apple-system, BlinkMacSystemFont, sans-serif; margin: 0; padding: 16px; }
        h1 { color: #38bdf8; font-size: 22px; margin-bottom: 4px; }
        .subtitle { color: #94a3b8; font-size: 12px; margin-bottom: 16px; }
        .card { background: #1e293b; border-radius: 12px; padding: 16px; margin-bottom: 12px; border: 1px solid #334155; }
        .status { color: #4ade80; font-weight: bold; }
        .symbol { font-weight: bold; color: #f1f5f9; }
        .setup-val { color: #38bdf8; font-weight: 600; float: right; }
        .market-row { padding: 6px 0; border-bottom: 1px solid #334155; font-size: 14px; }
        .market-row:last-child { border-bottom: none; }
        button { background: #0284c7; color: white; border: none; padding: 12px; border-radius: 8px; font-weight: bold; width: 100%; cursor: pointer; margin-top: 10px; }
    </style>
</head>
<body>
    <h1>🎯 SMC Setup Hunter</h1>
    <div class="subtitle">Статус: <span class="status">🟢 Онлайн (24/7)</span></div>

    <div class="card">
        <h3 style="margin-top:0; font-size:16px; color:#38bdf8;">Мониторинг рынков</h3>
        <div class="market-row"><span class="symbol">EURUSD:</span> <span id="eurusd" class="setup-val">Загрузка...</span></div>
        <div class="market-row"><span class="symbol">GBPUSD:</span> <span id="gbpusd" class="setup-val">Загрузка...</span></div>
        <div class="market-row"><span class="symbol">XAUUSD:</span> <span id="xauusd" class="setup-val">Загрузка...</span></div>
        <div class="market-row"><span class="symbol">BTCUSDT:</span> <span id="btcusdt" class="setup-val">Загрузка...</span></div>
        <div class="market-row"><span class="symbol">ETHUSDT:</span> <span id="ethusdt" class="setup-val">Загрузка...</span></div>
        <div class="market-row"><span class="symbol">SOLUSDT:</span> <span id="solusdt" class="setup-val">Загрузка...</span></div>
    </div>

    <button onclick="window.Telegram.WebApp.close()">Закрыть панель</button>

    <script>
        let tg = window.Telegram.WebApp;
        tg.expand();

        async function updateData() {
            try {
                let res = await fetch('/api/status');
                let data = await res.json();
                document.getElementById('eurusd').innerText = data.EURUSD || '...';
                document.getElementById('gbpusd').innerText = data.GBPUSD || '...';
                document.getElementById('xauusd').innerText = data.XAUUSD || '...';
                document.getElementById('btcusdt').innerText = data.BTCUSDT || '...';
                document.getElementById('ethusdt').innerText = data.ETHUSDT || '...';
                document.getElementById('solusdt').innerText = data.SOLUSDT || '...';
            } catch (e) {
                console.error("Ошибка загрузки данных", e);
            }
        }

        updateData();
        setInterval(updateData, 5000);
    </script>
</body>
</html>
"""


@app.route('/')
def home():
    return WEBAPP_HTML


def run_scanner_background():
    time.sleep(3)
    while True:
        try:
            analyze_and_notify()
        except Exception as e:
            print(f"Scanner error: {e}")
        time.sleep(SCAN_INTERVAL_SECONDS)


# Инициализация БД и запуск фонового потока при импорте модуля (важно для Gunicorn).
init_db()

# ВАЖНО: если деплоишь через Gunicorn, запускай ровно ОДИН воркер
# (gunicorn -w 1 ...), иначе каждый воркер поднимет свой поток сканера
# и алерты начнут дублироваться по числу воркеров.
threading.Thread(target=run_scanner_background, daemon=True).start()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
