import yfinance as yf
import pandas as pd
import yaml
import time

# Đọc danh sách currency từ file YAML
with open("./ingestion/api_loaders/yaml/currency_list.yaml", "r", encoding="utf-8") as f:
    PRODUCT_CONFIG = yaml.safe_load(f)

def get_currency_history_yf(symbol: str, period: str = "max"):
    try:
        yf_symbol = f"{symbol}=X"
        if symbol == "USDVND":
            yf_symbol = "VND=X"
        ticker = yf.Ticker(yf_symbol)
        hist = ticker.history(period=period, interval="1d")
        if hist.empty:
            print(f"X Không có dữ liệu lịch sử cho {symbol}")
            return None
        df = hist.reset_index()
        if df['Date'].dt.tz is not None:
            df['Date'] = df['Date'].dt.tz_localize(None)
        df['Symbol'] = symbol
        df['YF_Symbol'] = yf_symbol

        cols = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Symbol', 'YF_Symbol']
        df = df[cols]
        print(f"V Đã tải {len(df)} dòng lịch sử cho {symbol}")
        return df
    except Exception as e:
        print(f"Lỗi khi lấy dữ liệu lịch sử {symbol}: {e}")
        return None

def process_yf_currency_df(df, product_type, unit):
    rows = []
    prev_close = None
    for idx, row in df.iterrows():
        date = row['Date'].strftime('%Y-%m-%d') if hasattr(row['Date'], 'strftime') else str(row['Date'])
        open_price = row['Open']
        high = row['High']
        low = row['Low']
        close = row['Close']
        volume = row['Volume'] if not pd.isnull(row['Volume']) else 0

        # Tính phần trăm thay đổi so với phiên trước
        if prev_close is not None and prev_close != 0:
            change_percent = ((close - prev_close) / prev_close) * 100
        else:
            change_percent = None
        prev_close = close

        rows.append({
            'date': date,
            'type': product_type,
            'unit': unit,
            'open': open_price,
            'high': high,
            'low': low,
            'close': close,
            'volume': volume,
            'change_percent': change_percent
        })
    return pd.DataFrame(rows)

if __name__ == "__main__":
    all_currencies_df = []
    for item in PRODUCT_CONFIG.get("currencies", []):
        symbol = item["symbol"]
        print(f"--- Đang lấy lịch sử tỷ giá: {symbol} ---")
        df_history = get_currency_history_yf(symbol, period="max")
        if df_history is not None:
            cleaned_df = process_yf_currency_df(df_history, symbol, 'VND')
            all_currencies_df.append(cleaned_df)
        time.sleep(1)
    if all_currencies_df:
        final_df = pd.concat(all_currencies_df, ignore_index=True)
        final_df.to_csv("currency_history_clean.csv", index=False)
        print(final_df.head(10))