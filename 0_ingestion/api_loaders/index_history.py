import yfinance as yf
import pandas as pd
import time
import yaml

with open("./ingestion/api_loaders/yaml/index_world.yaml", "r", encoding="utf-8") as f:
    index_config = yaml.safe_load(f)

indices = index_config["indices"] 

def fetch_all_history(name, symbol, category):
    print(f"--- Đang tải {category}: {name} ({symbol}) ---")
    ticker = yf.Ticker(symbol)
    df = ticker.history(period="max")
    if df.empty:
        print(f"X Không có dữ liệu cho {symbol}")
        return None
    df.reset_index(inplace=True)
    if df['Date'].dt.tz is not None:
        df['Date'] = df['Date'].dt.tz_localize(None)
    df['Product_Key'] = name
    df['Symbol'] = symbol
    df['Category'] = category
    print(f"V Đã tải {len(df)} dòng.")
    return df

all_data = []


for idx in indices:
    name = idx["symbol"]
    symbol = idx["yf_ticker"]
    df_item = fetch_all_history(name, symbol, "index")
    if df_item is not None:
        all_data.append(df_item)
    time.sleep(1.5)

if all_data:
    final_df = pd.concat(all_data, ignore_index=True)
    print(final_df.head(10))