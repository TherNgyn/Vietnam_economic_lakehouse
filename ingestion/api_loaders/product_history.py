import yfinance as yf
import pandas as pd
import time
import yaml

with open("./ingestion/api_loaders/yaml/product_list.yaml", "r", encoding="utf-8") as f:
    PRODUCT_CONFIG = yaml.safe_load(f)

products = PRODUCT_CONFIG["products"]

def fetch_all_history(name, symbol):
    print(f"Đang tải dữ liệu cho {name} ({symbol}) ---")
    ticker = yf.Ticker(symbol)

    df = ticker.history(period="max")
    
    if df.empty:
        print(f"X Không có dữ liệu cho {symbol}")
        return None

    df.reset_index(inplace=True)

    df['Product_Key'] = name
    df['Symbol'] = symbol
    
    print(f"V Đã tải {len(df)} dòng dữ liệu.")
    return df

all_dataframes = []
for product in products:
    key = list(product.keys())[0]         
    info = product[key]               
    symbol = info["symbol"]
    data = fetch_all_history(key, symbol)
    if data is not None:
        all_dataframes.append(data)
    time.sleep(1)

final_df = pd.concat(all_dataframes, ignore_index=True)
final_df.head(10)