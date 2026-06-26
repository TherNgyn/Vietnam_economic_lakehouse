import os
import json
import time
import yaml
import numpy as np
from datetime import datetime, timezone
from kafka import KafkaProducer
import yfinance as yf

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
KAFKA_TOPIC =  "index_world_realtime"

print(f"Kafka Broker: {KAFKA_BROKER}")
print(f"Kafka Topic: {KAFKA_TOPIC}")
print("-" * 60)

kafka_producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
    acks='all',
    retries=3
)

try:
    with open("./ingestion/api_loaders/yaml/index_world.yaml", "r", encoding="utf-8") as f:
        PRODUCT_CONFIG = yaml.safe_load(f)
except FileNotFoundError:
    print("✗ index_world.yaml not found")
    exit(1)

def get_index_data_yf(symbol_name: str, yf_ticker: str) -> dict:
    """Fetch world index data from yfinance"""
    try:
        ticker = yf.Ticker(yf_ticker)
        
        # Try fast_info first, then fall back to info
        info = ticker.fast_info
        if not info or info.get('last_price', 0) == 0:
            info = ticker.info
        
        hist = ticker.history(period="1d", interval="1m")
        if hist.empty:
            hist = ticker.history(period="5d")
        
        if hist.empty:
            print(f"[{symbol_name}] No history data available")
            return None
        
        current_day = hist.iloc[-1]

        last_price = info.get('currentPrice') or info.get('last_price') or current_day['Close']
        prev_close = info.get('previousClose', current_day['Close'])
        
        if not last_price or last_price == 0:
            print(f"[{symbol_name}] Price is 0, skipping...")
            return None
        
        change = last_price - prev_close
        change_percent = (change / prev_close) * 100 if prev_close else 0
        
        raw_volume = current_day.get('Volume', 0)
        volume = int(raw_volume) if not np.isnan(raw_volume) else 0
        
        data = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "symbol": symbol_name,
            "yf_ticker": yf_ticker,
            "price": float(round(last_price, 2)),
            "open": float(round(current_day['Open'], 2)),
            "high": float(round(current_day['High'], 2)),
            "low": float(round(current_day['Low'], 2)),
            "prev_close": float(round(prev_close, 2)),
            "volume": volume,
            "change": float(round(change, 2)),
            "change_percent": float(round(change_percent, 2)),
            "currency": info.get('currency', 'USD'),
            "data_type": "index-realtime",
            "source": "yfinance"
        }
        return data
    except Exception as e:
        print(f"Error fetching {symbol_name} ({yf_ticker}): {e}")
        return None

def fetch_and_send_indices():

    try:
        success_count = 0
        error_count = 0
        
        for item in PRODUCT_CONFIG.get("indices", []):
            symbol = item["symbol"]
            yf_ticker = item["yf_ticker"]
            
            data = get_index_data_yf(symbol, yf_ticker)
            
            if data:
                kafka_producer.send(KAFKA_TOPIC, value=data)
                kafka_producer.flush()
                print(f"[{symbol}] {data['price']} {data['currency']} → Kafka")
                success_count += 1
            else:
                error_count += 1
        
        return success_count, error_count
    except Exception as e:
        print(f"✗ Error fetching indices: {e}")
        return 0, len(PRODUCT_CONFIG.get("indices", []))

def run_producer():
    print("Starting Producer (yfinance world indices → Kafka)")
    print("Interval: 5 minutes\n")
    
    iteration = 0
    while True:
        iteration += 1
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"\n[{timestamp}] Iteration {iteration}")
        print("-" * 60)
        
        success, error = fetch_and_send_indices()
        print(f"Summary: {success} sent, {error} failed")
        
        print(f"Waiting 300 seconds...", end='', flush=True)
        time.sleep(300)
        print(" Done!")

if __name__ == "__main__":
    try:
        run_producer()
    except KeyboardInterrupt:
        print("\n\nProducer stopped.")
        kafka_producer.close()