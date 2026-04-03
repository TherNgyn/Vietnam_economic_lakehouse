import yfinance as yf
import json
from datetime import datetime
import yaml
import numpy as np

with open("./ingestion/scrapers/index_world.yaml", "r", encoding="utf-8") as f:
    PRODUCT_CONFIG = yaml.safe_load(f)

def get_index_data_yf(symbol_name: str, yf_ticker: str) -> dict:
    try:
        ticker = yf.Ticker(yf_ticker)
        
        # fast_info giúp lấy giá real-time nhanh
        info = ticker.fast_info
        
        # Lấy history 1 phút để có OHLV
        hist = ticker.history(period="1d", interval="1m")
        if hist.empty:
            hist = ticker.history(period="1d")
        
        current_day = hist.iloc[-1]
        
        # Tính toán biến động
        last_price = info['last_price']
        prev_close = info['previous_close']
        change = last_price - prev_close
        change_percent = (change / prev_close) * 100 if prev_close else 0

        # Xử lý Volume (Index thường trả về NaN)
        raw_volume = current_day.get('Volume', 0)
        volume = int(raw_volume) if not np.isnan(raw_volume) else 0

        data = {
            "symbol": symbol_name,
            "yf_ticker": yf_ticker,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "price": round(last_price, 2),
            "open": round(current_day['Open'], 2),
            "high": round(current_day['High'], 2),
            "low": round(current_day['Low'], 2),
            "prev_close": round(prev_close, 2),
            "volume": volume,
            "change": round(change, 2),
            "change_percent": round(change_percent, 2),
            "currency": info.get('currency', 'USD'),
            "data_type": "index-realtime",
            "source": "yfinance"
        }
        return data
    except Exception as e:
        print(f"Lỗi khi lấy dữ liệu Index {symbol_name} ({yf_ticker}): {e}")
        return None

if __name__ == "__main__":
    for item in PRODUCT_CONFIG.get("indices", []):
        name = item["symbol"]
        ticker = item["yf_ticker"]
        
        print(f"--- Đang lấy chỉ số: {name} ({ticker}) ---")
        data = get_index_data_yf(name, ticker)
        
        if data:
            print(json.dumps(data, indent=2, ensure_ascii=False))