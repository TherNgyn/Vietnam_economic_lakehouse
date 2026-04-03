import pandas as pd
import numpy as np
from datetime import datetime

def parse_date(date_str):
    date_obj = datetime.strptime(date_str, '%d/%m/%Y')
    return date_obj.strftime('%Y-%m-%d')

def convert_volume(volume_str):
    if pd.isnull(volume_str) or volume_str == 'NaN':
        return None
    
    volume_str = str(volume_str).strip().upper()
    
    if 'K' in volume_str:
        return float(volume_str.replace('K', '')) * 1_000
    elif 'M' in volume_str:
        return float(volume_str.replace('M', '')) * 1_000_000
    elif 'B' in volume_str:
        return float(volume_str.replace('B', '')) * 1_000_000_000
    else:
        try:
            return float(volume_str)
        except:
            return None

def parse_price(price_str):
    # "1,729.25" -> 1729.25
    return float(str(price_str).strip().replace(',', ''))

def process_ohlc(df, product_type, unit):
    """Process OHLC data (WTI, VN_index, USD_VND)"""
    rows = []
    for idx, row in df.iterrows():
        date = parse_date(row['Ngày'].strip())
        close = parse_price(row['Lần cuối'])
        open_price = parse_price(row['Mở'])
        high = parse_price(row['Cao'])
        low = parse_price(row['Thấp'])
        volume = convert_volume(row['KL'])
        change_str = str(row['% Thay đổi']).strip().replace('%', '')
        change_percent = float(change_str) if change_str != 'nan' else None
        
        rows.append({
            'date': date,
            'type': product_type,
            'unit': unit,
            'open': open_price,
            'high': high,
            'low': low,
            'close': close,
            'volume': volume,
            'change_percent': change_percent
        })
    return pd.DataFrame(rows)


df_wti = pd.read_csv('./historical_dataset/product_world/crude_oil_WTI.csv')
df_vn_index = pd.read_csv('./historical_dataset/stock_index/VN_index.csv')
df_usd_vnd = pd.read_csv('./historical_dataset/currency/USD_VND.csv')
# xử lý null từng DataFrame
print(df_wti.isnull().sum())
print(df_vn_index.isnull().sum())
print(df_usd_vnd.isnull().sum())
# Process từng loại
wti = process_ohlc(df_wti, 'WTI', 'USD/barrel')
vn_index = process_ohlc(df_vn_index, 'VN_INDEX', 'points')
usd_vnd = process_ohlc(df_usd_vnd, 'USD_VND', 'VND')

# Merge thành 1 DataFrame
ohlc = pd.concat([wti, vn_index, usd_vnd], ignore_index=True)
ohlc = ohlc.sort_values(['date', 'type']).reset_index(drop=True)

print(ohlc.head(20))
print(ohlc.tail(10))