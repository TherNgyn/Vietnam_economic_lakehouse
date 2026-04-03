import yfinance as yf
import json
from datetime import datetime
import yaml

# Giả sử file YAML của bạn được lưu tại path này
with open("./ingestion/scrapers/cur.yaml", "r", encoding="utf-8") as f:
    PRODUCT_CONFIG = yaml.safe_load(f)

def get_currency_data_yf(symbol: str) -> dict:
    try:
        # Chuẩn hóa Symbol: USDVND -> VND=X (Cách Yahoo đặt mã cho cặp USD)
        # Các cặp khác: EURVND -> EURVND=X
        yf_symbol = f"{symbol}=X"
        if symbol == "USDVND":
            yf_symbol = "VND=X" 
            
        ticker = yf.Ticker(yf_symbol)
        
        # Lấy thông tin nhanh
        info = ticker.fast_info
        
        # Lấy history để tính toán OHLV
        # Với Forex, đôi khi dữ liệu 1m bị trống, ta lấy 1d nếu cần
        hist = ticker.history(period="1d", interval="1m")
        if hist.empty:
            hist = ticker.history(period="1d")
        
        current_day = hist.iloc[-1]
        
        # Tỷ giá thường lấy 2-4 chữ số thập phân
        decimals = 2 if "VND" in symbol else 4

        data = {
            "symbol": symbol,
            "yf_symbol": yf_symbol,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "price": round(info['last_price'], decimals),
            "open": round(current_day['Open'], decimals),
            "high": round(current_day['High'], decimals),
            "low": round(current_day['Low'], decimals),
            "prev_close": round(info['previous_close'], decimals),
            "change": round(info['last_price'] - info['previous_close'], decimals),
            "change_percent": round(((info['last_price'] - info['previous_close']) / info['previous_close']) * 100, 2),
            "currency": "VND",
            "data_type": "currency-realtime",
            "source": "yfinance"
        }
        return data
    except Exception as e:
        print(f"Lỗi khi lấy dữ liệu {symbol}: {e}")
        return None

if __name__ == "__main__":
    for item in PRODUCT_CONFIG.get("currencies", []):
        symbol = item["symbol"]
        print(f"--- Đang lấy tỷ giá: {symbol} ---")
        data = get_currency_data_yf(symbol)
        if data:
            print(json.dumps(data, indent=2, ensure_ascii=False))