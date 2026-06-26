import os
import json
import time
from datetime import datetime, timezone
import yfinance as yf
from kafka import KafkaProducer

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
KAFKA_TOPIC = "ticket_realtime"

print(f"Kafka Broker: {KAFKA_BROKER}")
print(f"Kafka Topic: {KAFKA_TOPIC}")
print("-" * 60)

kafka_producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
    acks='all',
    retries=3
)

ASSETS = [
    {'symbol': 'USDVND', 'yf_symbol': 'VND=X', 'asset_class': 'currency', 'unit': 'VND'},
]

def fetch_and_send(asset):
    try:
        ticker = yf.Ticker(asset['yf_symbol'])
        fast_info = ticker.fast_info
        
        last_price = fast_info.get('lastPrice', 0)
        
        if not last_price or last_price == 0:
            print(f"[{asset['symbol']}] Price is 0, skipping...")
            return False
        
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
        
        kafka_producer.send(KAFKA_TOPIC, value=tick)
        kafka_producer.flush()
        
        print(f"[{asset['symbol']}] {tick['lastPrice']} VND → Kafka")
        return True
        
    except Exception as e:
        print(f"✗ [{asset['symbol']}] Error: {e}")
        return False

def run_producer():
    print("Starting Producer (yfinance → Kafka)")
    print("Interval: 5 minutes (300 seconds)\n")
    
    iteration = 0
    while True:
        iteration += 1
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"\n[{timestamp}] Iteration {iteration}")
        print("-" * 60)
        
        for asset in ASSETS:
            fetch_and_send(asset)
        
        print(f"Waiting 300 seconds...", end='', flush=True)
        time.sleep(300)
        print(" Done!")

if __name__ == "__main__":
    try:
        run_producer()
    except KeyboardInterrupt:
        print("\n\nProducer stopped.")
        kafka_producer.close()