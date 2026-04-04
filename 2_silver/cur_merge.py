import pandas as pd
import numpy as np
from datetime import datetime
import yfinance as yf

def parse_date(date_str):
    try:
        date_obj = datetime.strptime(str(date_str).strip(), '%d/%m/%Y')
        return date_obj.strftime('%Y-%m-%d')
    except:
        return None

def convert_volume(volume_str):
    if pd.isnull(volume_str) or str(volume_str).upper() == 'NAN':
        return 0
    volume_str = str(volume_str).strip().upper()
    if 'K' in volume_str:
        return float(volume_str.replace('K', '')) * 1_000
    elif 'M' in volume_str:
        return float(volume_str.replace('M', '')) * 1_000_000
    elif 'B' in volume_str:
        return float(volume_str.replace('B', '')) * 1_000_000_000
    else:
        try:
            return float(volume_str)
        except:
            return None

def parse_price(price_str):
    try:
        return float(str(price_str).strip().replace(',', ''))
    except:
        return None

def clean_raw_df(df):
    df = df.copy()
    df['date'] = df['Ngày'].apply(parse_date)
    df['close'] = df['Lần cuối'].apply(parse_price)
    df['open'] = df['Mở'].apply(parse_price)
    df['high'] = df['Cao'].apply(parse_price)
    df['low'] = df['Thấp'].apply(parse_price)
    df['volume'] = df['KL'].apply(convert_volume)
    def parse_change(val):
        try:
            s = str(val).strip().replace('%', '')
            return float(s)
        except:
            return None
    df['change_percent'] = df['% Thay đổi'].apply(parse_change)
    return df[['date', 'close', 'open', 'high', 'low', 'volume', 'change_percent']]

def get_currency_history_yf(symbol: str, period: str = "max"):
    try:
        yf_symbol = f"{symbol}=X"
        if symbol == "USDVND":
            yf_symbol = "VND=X"
        ticker = yf.Ticker(yf_symbol)
        hist = ticker.history(period=period, interval="1d")
        if hist.empty:
            print(f"Không có dữ liệu lịch sử cho {symbol}")
            return None
        df = hist.reset_index()
        if df['Date'].dt.tz is not None:
            df['Date'] = df['Date'].dt.tz_localize(None)
        df['date'] = df['Date'].dt.strftime('%Y-%m-%d')
        df = df.rename(columns={
            'Open': 'open',
            'High': 'high',
            'Low': 'low',
            'Close': 'close',
            'Volume': 'volume'
        })
        df['change_percent'] = None
        df = df[['date', 'close', 'open', 'high', 'low', 'volume', 'change_percent']]
        df['close'] = df['close'].apply(parse_price)
        df['open'] = df['open'].apply(parse_price)
        df['high'] = df['high'].apply(parse_price)
        df['low'] = df['low'].apply(parse_price)
        df['volume'] = df['volume'].apply(convert_volume)
        return df
    except Exception as e:
        print(f"Lỗi khi lấy dữ liệu lịch sử {symbol}: {e}")
        return None

def process_ohlc(df, product_type, unit):
    rows = []
    for idx, row in df.iterrows():
        rows.append({
            'date': row['date'],
            'type': product_type,
            'unit': unit,
            'open': row['open'],
            'high': row['high'],
            'low': row['low'],
            'close': row['close'],
            'volume': row['volume'],
            'change_percent': row['change_percent']
        })
    return pd.DataFrame(rows)

if __name__ == "__main__":
    df_raw = pd.read_csv('./historical_dataset/currency/USD_VND.csv')
    df_raw = clean_raw_df(df_raw)
    print(df_raw.info())
    raw_dates = set(df_raw['date'].dropna().unique())
    df_yf = get_currency_history_yf('USDVND', period="max")
    if df_yf is not None:
        yf_dates = set(df_yf['date'].unique())
        missing_dates = sorted(list(yf_dates - raw_dates))
        print("Những ngày thiếu:", missing_dates)
        df_missing = df_yf[df_yf['date'].isin(missing_dates)].copy()
        df_full = pd.concat([df_raw, df_missing], ignore_index=True)
    else:
        print("Không lấy được dữ liệu từ yfinance.")
        df_full = df_raw
    print(df_full.info())
    # Sắp xếp theo ngày để tính toán đúng
    df_full = df_full.sort_values('date').reset_index(drop=True)

    # Tính lại change_percent cho toàn bộ dữ liệu
    df_full['change_percent'] = df_full['close'].pct_change() * 100
    df_full.loc[0, 'change_percent'] = 0  # Dòng đầu tiên gán 0 hoặc NaN tùy ý

    usd_vnd = process_ohlc(df_full, 'USDVND', 'VND')
    ohlc = usd_vnd.sort_values(['date', 'type']).reset_index(drop=True)
    
    print(ohlc.head(20))
    print(ohlc.tail(10))