from flask import Flask
app = Flask(__name__)

import os
import time
import requests
import pandas as pd
import mplfinance as mpf
import yfinance as yf
import ccxt
from datetime import datetime, timezone

# --- НАСТРОЙКИ ---
BOT_TOKEN = "8773030425:AAGNwPdc3NK9h2LmP-R-9ny9UgaTMilMJR0"
CHAT_ID = "8707344733"

FOREX_SYMBOLS = {
    'EURUSD': 'EURUSD=X',
    'GBPUSD': 'GBPUSD=X',
    'XAUUSD': 'GC=F'
}

CRYPTO_SYMBOLS = ['BTC/USDT', 'ETH/USDT', 'SOL/USDT']

binance = ccxt.binance()

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
                'bottom': df['High'].iloc[i],
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
        files[attach_name] = open(photo_path, 'rb')
        
    payload = {
        'chat_id': CHAT_ID,
        'media': str(media).replace("'", '"')
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
    
    for name, ticker in FOREX_SYMBOLS.items():
        try:
            df_4h = get_forex_data(ticker, '4H')
            df_15m = get_forex_data(ticker, '15M')
            process_pair(name, df_4h, df_15m)
        except Exception as e:
            print(f"Ошибка {name}: {e}")

    for pair in CRYPTO_SYMBOLS:
        try:
            name = pair.replace('/', '')
            df_4h = get_crypto_data(pair, '4H')
            df_15m = get_crypto_data(pair, '15M')
            process_pair(name, df_4h, df_15m)
        except Exception as e:
            print(f"Ошибка {pair}: {e}")

def process_pair(symbol, df_4h, df_15m):
    if df_4h is None or df_15m is None or len(df_15m) < 5:
        return

    sweep_4h = check_sweep(df_4h)
    fvg_15m = find_fvg(df_15m)

    if not fvg_15m:
        return

    last_fvg = fvg_15m[-1]
    
    if df_15m.index.get_loc(last_fvg['time']) >= len(df_15m) - 3:
        trigger = "Sweep" if sweep_4h else "FVG"
        direction = "Лонг-сетап" if last_fvg['type'] == 'Bullish FVG' else "Шорт-сетап"

        img_4h = f"{symbol}_4H.png"
        img_15m = f"{symbol}_15M.png"

        render_chart(df_4h, f"{symbol} · 4H Context", img_4h)
        render_chart(df_15m, f"{symbol} · 15M Trigger", img_15m)

        caption = (
            f"🎯 <b>{symbol} · 4H Trigger + 15M FVG</b>\n"
            f"<b>{direction} сформирован</b>\n"
            f"Триггер: {trigger}\n"
            f"Время алерта: {datetime.now(timezone.utc).strftime('%H:%M UTC')}\n"
        )

        send_telegram_media_group([img_4h, img_15m], caption)
        print(f"✅ Алерт по {symbol} отправлен!")

        if os.path.exists(img_4h): os.remove(img_4h)
        if os.path.exists(img_15m): os.remove(img_15m)

if __name__ == "__main__":
    while True:
        analyze_and_notify()
        time.sleep(300) # Проверка каждые 5 минут

# Обеспечиваем доступность app для Gunicorn на Render
if 'app' not in globals() and 'application' in globals():
    app = application

if __name__ == '__main__':
    import os
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)

@app.route('/')
def health_check():
return "OK", 200

if __name__ == '__main__':
import os
port = int(os.environ.get('PORT', 5000))
app.run(host='0.0.0.0', port=port)
