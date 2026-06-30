import os
import json
import time
import yaml
import requests
import threading
from datetime import datetime, timezone
from kafka import KafkaProducer
import yfinance as yf
import numpy as np

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
print(f"DEBUG: Thư mục làm việc hiện tại: {os.getcwd()}")
print("DEBUG: Cấu trúc thư mục tại gốc /app:")
try:
    # Liệt kê đệ quy một cấp để xem bên trong app có gì
    for root, dirs, files in os.walk('/app'):
        print(f"  {root} -> {dirs}")
        break 
except Exception as e:
    print(f"DEBUG: Không thể liệt kê /app: {e}")
kafka_producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
    acks='all',
    retries=3
)
BASE_PATH = "/app"
YAML_DIR = os.path.join(BASE_PATH, "ingestion", "api_loaders", "yaml")

def load_config(filename):
    path = os.path.join(YAML_DIR, filename)
    if not os.path.exists(path):
        content = os.listdir(YAML_DIR) if os.path.exists(YAML_DIR) else "Thư mục không tồn tại"
        raise FileNotFoundError(f"Không tìm thấy file {filename} tại {path}. Danh sách hiện tại: {content}")
    
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)
try:
    INDEX_WORLD_CONFIG = load_config("index_world.yaml")
    PRODUCT_CONFIG = load_config("product_list.yaml")
    print("[INIT] ✓ YAML configs loaded successfully")
except Exception as e:
    print(f"[ERROR] {e}")
    exit(1)

def producer_currency():
    assets = [
        {'symbol': 'USDVND', 'yf_symbol': 'VND=X', 'asset_class': 'currency', 'unit': 'VND'},
        
    ]
    print("[P1] Currency producer started")
    
    while True:
        for asset in assets:
            try:
                ticker = yf.Ticker(asset['yf_symbol'])
                fast_info = ticker.fast_info
                last_price = fast_info.get('lastPrice', 0)
                
                if last_price and last_price > 0:
                    tick = {
                        'timestamp': datetime.now(timezone.utc).isoformat(),
                        'symbol': asset['symbol'],
                        'asset_class': asset['asset_class'],
                        'unit': asset['unit'],
                        'yf_symbol': asset['yf_symbol'],
                        'lastPrice': float(last_price),
                        'previousClose': float(fast_info.get('previousClose', 0)),
                        'open': float(fast_info.get('open', 0)),
                        'dayHigh': float(fast_info.get('dayHigh', 0)),
                        'dayLow': float(fast_info.get('dayLow', 0)),
                        'volume': float(fast_info.get('lastVolume', 0)),
                    }
                    kafka_producer.send("ticker-realtime", value=tick)
                    kafka_producer.flush()
                    print(f"[P1] {asset['symbol']}: {last_price}")
            except Exception as e:
                print(f"[P1-ERROR] {asset.get('yf_symbol')}: {e}")
        
        time.sleep(300)

def producer_world_index():
    print("[P2] World index producer started")
    while True:
        for item in INDEX_WORLD_CONFIG.get("indices", []):
            try:
                ticker = yf.Ticker(item["yf_ticker"])
                info = ticker.fast_info
                if not info or info.get('last_price', 0) == 0:
                    info = ticker.info
                
                hist = ticker.history(period="1d", interval="1m")
                if hist.empty:
                    hist = ticker.history(period="5d")
                if hist.empty:
                    continue
                
                current_day = hist.iloc[-1]
                last_price = info.get('currentPrice') or info.get('last_price') or current_day['Close']
                prev_close = info.get('previousClose', current_day['Close'])
                
                if last_price and last_price > 0:
                    change = last_price - prev_close
                    change_percent = (change / prev_close) * 100 if prev_close else 0
                    raw_volume = current_day.get('Volume', 0)
                    volume = int(raw_volume) if not np.isnan(raw_volume) else 0
                    
                    data = {
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "symbol": item["symbol"],
                        "yf_ticker": item["yf_ticker"],
                        "price": float(round(last_price, 2)),
                        "open": float(round(current_day['Open'], 2)),
                        "high": float(round(current_day['High'], 2)),
                        "low": float(round(current_day['Low'], 2)),
                        "prev_close": float(round(prev_close, 2)),
                        "volume": volume,
                        "change": float(round(change, 2)),
                        "change_percent": float(round(change_percent, 2)),
                        "currency": info.get('currency', 'USD'),
                        "source": "yfinance"
                    }
                    kafka_producer.send("index-world-realtime", value=data)
                    kafka_producer.flush()
                    print(f"[P2] {item['symbol']}: {round(last_price, 2)}")
            except Exception as e:
                print(f"[P2-ERROR] {item.get('symbol')}: {e}")
        
        time.sleep(300)

def producer_product():
    print("[P3] Product producer started")
    while True:
        try:
            for product_group in PRODUCT_CONFIG.get("products", []):
                for prod_key, prod_info in product_group.items():
                    try:
                        symbol = prod_info["symbol"]
                        ticker = yf.Ticker(symbol)
                        info = ticker.fast_info
                        if not info or info.get('last_price', 0) == 0:
                            info = ticker.info
                        
                        hist = ticker.history(period="1d", interval="1m")
                        if hist.empty:
                            hist = ticker.history(period="5d")
                        if hist.empty:
                            continue
                        
                        current_day = hist.iloc[-1]
                        last_price = info.get('currentPrice') or info.get('last_price') or current_day['Close']
                        prev_close = info.get('previousClose', current_day['Close'])
                        
                        if last_price and last_price > 0:
                            change = last_price - prev_close
                            change_percent = (change / prev_close) * 100 if prev_close else 0
                            raw_volume = current_day.get('Volume', 0)
                            volume = int(raw_volume) if not np.isnan(raw_volume) else 0
                            
                            data = {
                                "timestamp": datetime.now(timezone.utc).isoformat(),
                                "symbol": symbol,
                                "name": prod_info.get("name", "N/A"),
                                "price": float(round(last_price, 2)),
                                "open": float(round(current_day['Open'], 2)),
                                "high": float(round(current_day['High'], 2)),
                                "low": float(round(current_day['Low'], 2)),
                                "prev_close": float(round(prev_close, 2)),
                                "volume": volume,
                                "change": float(round(change, 2)),
                                "change_percent": float(round(change_percent, 2)),
                                "currency": info.get('currency', prod_info.get("currency", "USD")),
                                "unit": prod_info.get("unit", "unit"),
                                "source": "yfinance"
                            }
                            kafka_producer.send("product-realtime", value=data)
                            kafka_producer.flush()
                            print(f"[P3] {symbol}: {round(last_price, 2)}")
                    except Exception as e:
                        print(f"[P3-ERROR] {prod_info.get('symbol', 'unknown')}: {e}")
        except Exception as e:
            print(f"[P3-OUTER-ERROR] {e}")
        
        time.sleep(300)

if __name__ == "__main__":
    print("[INIT] Producer unified starting...")
    threads = [
        threading.Thread(target=producer_currency, daemon=True),
        threading.Thread(target=producer_world_index, daemon=True),
        threading.Thread(target=producer_product, daemon=True),
    ]
    
    for t in threads:
        t.start()
    
    print("Producers (P1-P3) started")
    try:
        for t in threads:
            t.join()
    except KeyboardInterrupt:
        kafka_producer.close()
        print("Stopped")
