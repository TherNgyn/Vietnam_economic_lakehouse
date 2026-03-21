from vnstock import *
import yaml
import pandas as pd
from datetime import datetime
# load Index data
# world
path = "./ingestion/api_loaders/index_list.yaml"
with open(path, "r", encoding="utf-8") as f:
    config = yaml.safe_load(f)

index_list = config["index"]
# index = Vnstock().world_index(symbol = '<INDEX_SYMBOL>', source='MSN')
# df = index.quote.history(start='2026-01-01', end='2026-03-14',interval= '1D')
all_data = []

for item in index_list:
    symbol = item["symbol"]
    name = item["name"]
    source = item.get("source", "MSN")
    try:
        index = Vnstock().world_index(symbol=symbol, source=source)
        df = index.quote.history(
            start='2000-01-01',
            end=datetime.now().strftime("%Y-%m-%d"),
            interval='1D'
        )
        df["symbol"] = symbol
        df["name"] = name
        df["country"] = item["country"]
        all_data.append(df)

    except Exception as e:
        print(f"Error with {symbol}: {e}")

final_df = pd.concat(all_data, ignore_index=True)

print(final_df.head())

final_df.to_csv("index_world_data.csv", index=False)

