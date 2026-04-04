from deltalake import DeltaTable
import os

storage_options = {
    'AWS_ENDPOINT_URL': 'http://minio:9000',
    'AWS_ACCESS_KEY_ID': 'minioadmin',
    'AWS_SECRET_ACCESS_KEY': 'minioadmin123',
    'AWS_REGION': 'us-east-1',
    'AWS_ALLOW_HTTP': 'true',
}

# Read Silver
try:
    dt = DeltaTable('s3://silver/currency/USDVND', storage_options=storage_options)
    df = dt.to_pandas()
    print(f'✓ Silver USDVND rows: {len(df)}')
    print(f'Date range: {df["date"].min()} → {df["date"].max()}')
    print(f'Sources: {df["source"].unique()}')
    print('\nLatest 5 rows:')
    print(df[['date', 'close', 'change_percent', 'source']].tail(5).to_string())
except Exception as e:
    print(f'X Error reading Silver: {e}')