import os
import pandas as pd
from datetime import date
from deltalake import DeltaTable, write_deltalake

MINIO_BUCKET_BRONZE = os.getenv("MINIO_BUCKET_BRONZE", "bronze")
MINIO_BUCKET_SILVER = os.getenv("MINIO_BUCKET_SILVER", "silver")

STORAGE_OPTIONS = {
    "AWS_ENDPOINT_URL": os.getenv("AWS_ENDPOINT_URL", "http://minio:9000"),
    "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
    "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin123"),
    "AWS_REGION": "us-east-1",
    "AWS_ALLOW_HTTP": "true",
}

def clean_realtime_to_silver(asset_class='currency', symbol='USDVND', target_date=None):
    if target_date is None:
        target_date = date.today().strftime('%Y-%m-%d')
    
    print(f"Cleaning {symbol} ({asset_class}) for {target_date}...")
    
    bronze_path = f"s3://{MINIO_BUCKET_BRONZE}/realtime/{asset_class}"
    silver_path = f"s3://{MINIO_BUCKET_SILVER}/{asset_class}/{symbol}"  # ← Unified path
    
    # Read from Bronze
    try:
        dt = DeltaTable(bronze_path, storage_options=STORAGE_OPTIONS)
        df = dt.to_pandas()
        df = df[(df['date'] == target_date) & (df['symbol'] == symbol)].copy()
    except Exception as e:
        print(f"Error reading bronze: {e}")
        return
    
    if df.empty:
        print(f"No data for {target_date}")
        return
    
    print(f"  Read {len(df)} rows from bronze")
    
    # Type conversion
    df['open'] = df['open'].astype('float64')
    df['high'] = df['high'].astype('float64')
    df['low'] = df['low'].astype('float64')
    df['close'] = df['close'].astype('float64')
    df['volume'] = df['volume'].astype('float64')
    df['prev_close'] = df['prev_close'].astype('float64')

    # Calculate metrics
    df['change'] = (df['close'] - df['prev_close']).round(4)
    df['change_percent'] = ((df['close'] - df['prev_close']) / df['prev_close'] * 100).round(4)
    
    df['symbol'] = df['symbol'].astype('str')
    df['asset_class'] = df['asset_class'].astype('str')
    df['unit'] = df['unit'].astype('str')
    df['source'] = df['source'].astype('str')

    df = df[['date', 'symbol', 'asset_class', 'unit', 'open', 'high', 'low', 'close', 'volume', 'prev_close', 'change', 'change_percent', 'source']]
    
    print(f"  {len(df)} rows cleaned")
    print(f"  Appending to {silver_path}...")
    

    try:
        dt_silver = DeltaTable(silver_path, storage_options=STORAGE_OPTIONS)
        dt_silver.merge(
            source=df,
            predicate="s.date = t.date AND s.symbol = t.symbol",
            source_alias='s',
            target_alias='t',
        ).when_matched_update_all().when_not_matched_insert_all().execute()
        print(f"  Merged {len(df)} rows")
    except Exception as e:

        print(f"  Creating new Silver table... ({e})")
        write_deltalake(silver_path, df, mode='append', partition_by=['symbol'], storage_options=STORAGE_OPTIONS)
        print(f"  ✓ Created and wrote {len(df)} rows")

if __name__ == "__main__":
    clean_realtime_to_silver()