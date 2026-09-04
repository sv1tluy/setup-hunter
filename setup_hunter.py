import os
import time
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
BOT_TOKEN = "8773030425:AAGNwPdc3NK9h2LmP-R-9ny9UgaTMilMJR0"
CHAT_ID = "8707344733"

FOREX_SYMBOLS = {
    'EURUSD': {'ticker': 'EURUSD=X', 'tv': 'OANDA:EURUSD'},
    'GBPUSD': {'ticker': 'GBPUSD=X', 'tv': 'OANDA:GBPUSD'},
    'XAUUSD': {'ticker': 'GC=F', 'tv': 'CAPITALCOM:XAUUSD'}
}

CRYPTO_SYMBOLS = [
    {'pair': 'BTC/USDT', 'name': 'BTCUSDT', 'tv': 'BINANCE:BTCUSDT'},
    {'pair': 'ETH/USDT', 'name': 'ETHUSDT', 'tv': 'BINANCE:ETHUSDT'},
    {'pair': 'SOL/USDT', 'name': 'SOLUSDT', 'tv': 'BINANCE:SOLUSDT'}
]

binance = ccxt.binance()

# Глобальное хранилище для Web App
market_data = {
    "EURUSD": "Инициализация...",
    "GBPUSD": "Инициализация...",
    "XAUUSD": "Инициализация...",
    "BTCUSDT": "Инициализация...",
    "ETHUSDT": "Инициализация...",
    "SOLUSDT": "Инициализация...",
    "last_update": "Только что"
}

# Словарь для защиты от повторной отправки одинаковых сетапов
last_sent_fvg = {}

def get_forex_data(ticker_symbol, timeframe):
    interval = '15m' if timeframe == '15M' else '60m'
    period = '5d' if timeframe == '15M' else '1mo'
    
    df = yf.download(tickers=ticker_symbol, period=period, interval=interval, progress=False)
    if df.empty:
        return None
        
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
        
    df = df[['Open', 'High', 'Low', 'Close', 'Volume']].dropna()
    
    if timeframe == '4H':
        df = df.resample('4h').agg({
            'Open': 'first',
            'High': 'max',
            'Low': 'min',
            'Close': 'last',
            'Volume': 'sum'
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
        if df['Low'].iloc[i] > df['High'].iloc[i-2]:
            fvg_list.append({
                'type': 'Bullish FVG',
                'top': df['Low'].iloc[i],
                'bottom': df['High'].iloc[i-2],
                'time': df.index[i]
            })
        elif df['High'].iloc[i] < df['Low'].iloc[i-2]:
            fvg_list.append({
                'type': 'Bearish FVG',
                'top': df['Low'].iloc[i-2],
                'bottom': df['Low'].iloc[i],
                'time': df.index[i]
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
        figratio=(12, 7)
    )

def send_telegram_media_group_with_button(photos, caption, tv_url):
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
        files[attach_name] = open(photo_path, 'rb')
        
    payload = {
        'chat_id': CHAT_ID,
        'media': str(media).replace("'", '"'),
        'reply_markup': str({
            "inline_keyboard": [
                [{"text": "📊 Открыть в TradingView", "url": tv_url}]
            ]
        }).replace("'", '"')
    }
    
    try:
        requests.post(url, data=payload, files=files, timeout=15)
    except Exception as e:
        print(f"Ошибка отправки Telegram: {e}")
    finally:
        for f in files.values():
            f.close()

def analyze_and_notify():
    print(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}] Сканирование рынка...")
    
    for name, info in FOREX_SYMBOLS.items():
        market_data[name] = "Сканирование..."
        try:
            df_4h = get_forex_data(info['ticker'], '4H')
            df_15m = get_forex_data(info['ticker'], '15M')
            process_pair(name, df_4h, df_15m, info['tv'])
        except Exception as e:
            market_data[name] = f"Ошибка: {e}"
            print(f"Ошибка {name}: {e}")

    for item in CRYPTO_SYMBOLS:
        name = item['name']
        market_data[name] = "Сканирование..."
        try:
            df_4h = get_crypto_data(item['pair'], '4H')
            df_15m = get_crypto_data(item['pair'], '15M')
            process_pair(name, df_4h, df_15m, item['tv'])
        except Exception as e:
            market_data[name] = f"Ошибка: {e}"
            print(f"Ошибка {name}: {e}")
            
    market_data["last_update"] = datetime.now(timezone.utc).strftime('%H:%M:%S UTC')

def process_pair(symbol, df_4h, df_15m, tv_symbol):
    if df_4h is None or df_15m is None or len(df_15m) < 5:
        market_data[symbol] = "Нет данных"
        return

    sweep_4h = check_sweep(df_4h)
    fvg_15m = find_fvg(df_15m)

    if not fvg_15m:
        market_data[symbol] = "Ожидание FVG"
        return

    last_fvg = fvg_15m[-1]
    
    if df_15m.index.get_loc(last_fvg['time']) >= len(df_15m) - 3:
        trigger = "Sweep" if sweep_4h else "FVG"
        direction = "Лонг" if last_fvg['type'] == 'Bullish FVG' else "Шорт"
        
        market_data[symbol] = f"🔥 {direction} ({trigger})"

        fvg_key = f"{symbol}_{last_fvg['time']}"
        if last_sent_fvg.get(symbol) == fvg_key:
            return  

        img_4h = f"{symbol}_4H.png"
        img_15m = f"{symbol}_15M.png"

        render_chart(df_4h, f"{symbol} · 4H Context", img_4h)
        render_chart(df_15m, f"{symbol} · 15M Trigger", img_15m)

        tv_url = f"https://www.tradingview.com/chart/?symbol={tv_symbol}"

        caption = (
            f"● <b>{symbol} · 4H Trigger + 15M FVG</b>\n"
            f"<b>{direction}-сетап сформирован</b>\n"
            f"Триггер: {trigger}\n"
            f"Время алерта: {datetime.now(timezone.utc).strftime('%H:%M UTC+0')}\n"
        )

        send_telegram_media_group_with_button([img_4h, img_15m], caption, tv_url)
        print(f"✅ Алерт по {symbol} отправлен!")

        last_sent_fvg[symbol] = fvg_key

        if os.path.exists(img_4h): os.remove(img_4h)
        if os.path.exists(img_15m): os.remove(img_15m)
    else:
        market_data[symbol] = "Активен (ожидание)"

# Flask Routes
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
        .market-row { 
            display: flex; 
            justify-content: space-between; 
            align-items: center; 
            padding: 10px 8px; 
            border-bottom: 1px solid #334155; 
            font-size: 14px; 
            cursor: pointer;
            border-radius: 6px;
            transition: background 0.2s;
        }
        .market-row:hover { background: #334155; }
        .market-row:last-child { border-bottom: none; }
        .symbol { font-weight: bold; color: #f1f5f9; display: flex; align-items: center; gap: 6px; }
        .symbol::after { content: "↗"; font-size: 11px; color: #94a3b8; }
        .setup-val { color: #38bdf8; font-weight: 600; text-align: right; }
        .hint { font-size: 11px; color: #64748b; text-align: center; margin-top: 6px; }
        button { background: #0284c7; color: white; border: none; padding: 12px; border-radius: 8px; font-weight: bold; width: 100%; cursor: pointer; margin-top: 10px; }
    </style>
</head>
<body>
    <h1>🎯 SMC Setup Hunter</h1>
    <div class="subtitle">Статус: <span class="status">🟢 Онлайн (24/7)</span></div>
    
    <div class="card">
        <h3 style="margin-top:0; font-size:16px; color:#38bdf8; margin-bottom:12px;">Мониторинг рынков</h3>
        
        <div class="market-row" onclick="openTradingView('OANDA:EURUSD')">
            <span class="symbol">EURUSD</span> <span id="eurusd" class="setup-val">Загрузка...</span>
        </div>
        <div class="market-row" onclick="openTradingView('OANDA:GBPUSD')">
            <span class="symbol">GBPUSD</span> <span id="gbpusd" class="setup-val">Загрузка...</span>
        </div>
        <div class="market-row" onclick="openTradingView('CAPITALCOM:XAUUSD')">
            <span class="symbol">XAUUSD</span> <span id="xauusd" class="setup-val">Загрузка...</span>
        </div>
        <div class="market-row" onclick="openTradingView('BINANCE:BTCUSDT')">
            <span class="symbol">BTCUSDT</span> <span id="btcusdt" class="setup-val">Загрузка...</span>
        </div>
        <div class="market-row" onclick="openTradingView('BINANCE:ETHUSDT')">
            <span class="symbol">ETHUSDT</span> <span id="ethusdt" class="setup-val">Загрузка...</span>
        </div>
        <div class="market-row" onclick="openTradingView('BINANCE:SOLUSDT')">
            <span class="symbol">SOLUSDT</span> <span id="solusdt" class="setup-val">Загрузка...</span>
        </div>
        
        <div class="hint">Нажми на актив, чтобы открыть график в TradingView</div>
    </div>

    <button onclick="window.Telegram.WebApp.close()">Закрыть панель</button>

    <script>
        let tg = window.Telegram.WebApp;
        tg.expand();

        function openTradingView(ticker) {
            let tvUrl = `https://www.tradingview.com/chart/?symbol=${ticker}`;
            tg.openLink(tvUrl);
        }

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
        time.sleep(300)

threading.Thread(target=run_scanner_background, daemon=True).start()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
