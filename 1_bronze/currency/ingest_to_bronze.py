import os
import pandas as pd
from minio import Minio
from dotenv import load_dotenv

load_dotenv()

CSV_PATH = './historical_dataset/currency/USD_VND.csv'
SYMBOL   = 'USDVND'

minio_client = Minio(
    os.getenv("MINIO_HOST", "localhost:9000"),
    access_key=os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
    secret_key=os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin123"),
    secure=False,
)

MINIO_BUCKET = os.getenv("MINIO_BUCKET_BRONZE", "bronze")

def _ensure_bucket():
    if not minio_client.bucket_exists(MINIO_BUCKET):
        minio_client.make_bucket(MINIO_BUCKET)


if __name__ == "__main__":
    _ensure_bucket()
    minio_client.fput_object(
        MINIO_BUCKET,
        f"/currency/historical/{SYMBOL}.csv",
        CSV_PATH,
    )
    df = pd.read_csv(CSV_PATH)
    print(f"bronze done: {len(df)} rows → {MINIO_BUCKET}/currency/historical/{SYMBOL}.csv")