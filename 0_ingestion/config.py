import os
from dotenv import load_dotenv

load_dotenv()

MINIO_CONFIG = {
    "host": os.getenv("MINIO_HOST", "localhost:9000"),
    "access_key": os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
    "secret_key": os.getenv("MINIO_SECRET_KEY", "minioadmin123"),
    "secure": os.getenv("MINIO_SECURE", "False").lower() == "true"
}

BRONZE_BUCKET = os.getenv("MINIO_BUCKET", "bronze")
SILVER_BUCKET = "silver"
GOLD_BUCKET = "gold"
HISTORICAL_DATASET_PATH = os.getenv("HISTORICAL_DATASET_PATH", "historical_dataset")