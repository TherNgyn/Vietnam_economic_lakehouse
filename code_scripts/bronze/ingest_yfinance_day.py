import os
import sys
import argparse
from datetime import datetime
from deltalake import write_deltalake

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from ingestion.api_loaders.yfinance_loader import fetch_history_batch

MINIO_BUCKET = os.getenv('MINIO_BUCKET', 'bronze')
STORAGE_OPTIONS = {
    "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
    "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
    "AWS_ENDPOINT_URL": os.getenv("AWS_ENDPOINT_URL", "http://minio:9000"),
    "AWS_ALLOW_HTTP": "true",
    "AWS_REGION": "us-east-1",
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
}

ASSET_PATH = {
    'currency': 'daily/currency',
    'index': 'daily/world_index',
    'product': 'daily/product',
}


def ingest_yfinance_daily(period: str = '5d'):
    df = fetch_history_batch(asset_type='all', period=period, sleep_seconds=1.0)
    if df.empty:
        return

    df['processing_date'] = datetime.utcnow().strftime('%Y-%m-%d')

    for asset_type, group in df.groupby('asset_type'):
        path = ASSET_PATH.get(asset_type, f'daily/{asset_type}')
        write_deltalake(
            f's3://{MINIO_BUCKET}/{path}',
            group.reset_index(drop=True),
            mode='append',
            partition_by=['processing_date'],
            storage_options=STORAGE_OPTIONS,
        )


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--period', default='5d', help='yfinance period (e.g. 5d, 30d, max)')
    args = parser.parse_args()
    ingest_yfinance_daily(period=args.period)