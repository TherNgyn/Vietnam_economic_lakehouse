from vnstock import *
import yaml
from datetime import datetime
import pandas as pd
# load Currency data
path = "./ingestion/api_loaders/index_list.yaml"
with open(path, "r", encoding='utf-8') as f:
    config = yaml.safe_load(f)

currency_list = config["currency"]
all_data = []
for i in currency_list:
    symbol = i["symbol"]
    name = i["name"]
    try:
        fx = Vnstock().fx(symbol=symbol, source='MSN')
        df = fx.quote.history(
            start='2005-01-01',
            end=datetime.now().strftime("%Y-%m-%d"),
            interval='1D'
        )
        df["symbol"] = symbol
        df["name"] = name
        all_data.append(df)
    except Exception as e:
        print(f"Error with {symbol}: {e}")
final_df = pd.concat(all_data, ignore_index=True)
print(final_df.head())