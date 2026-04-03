import yfinance as yf
import json
from datetime import datetime
import yaml

with open("./ingestion/scrapers/product.yaml", "r", encoding="utf-8") as f:
    PRODUCT_CONFIG = yaml.safe_load(f)

def get_product_data(symbol: str, product: dict) -> dict:
    try:
        ticker = yf.Ticker(symbol)

        info = ticker.fast_info 

        current_day = ticker.history(period="1d", interval="1m").iloc[-1]

        data = {
            "symbol": symbol,
            "name": product.get("name", "N/A"),
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "price": round(info['last_price'], 2),
            "open": round(current_day['Open'], 2),
            "high": round(current_day['High'], 2),
            "low": round(current_day['Low'], 2),
            "prev_close": round(info['previous_close'], 2),
            "volume": int(current_day['Volume']),
            "currency": info.get('currency', product.get("currency", "USD")),
            "unit": product.get("unit", "unit"),
            "exchange": info.get('exchange', 'N/A'),
            "data_type": "real-time",
            "source": "yfinance"
        }
        return data
    except Exception as e:
        print(f"Lỗi khi lấy dữ liệu WTI: {e}")
        return None

if __name__ == "__main__":
    for product in PRODUCT_CONFIG["products"]:
        for prod_key, prod_info in product.items():
            symbol = prod_info["symbol"]
            print(f"Đang lấy dữ liệu cho {symbol}...")
            product_data = get_product_data(symbol, prod_info)
            if product_data:
                print(json.dumps(product_data, indent=2))
