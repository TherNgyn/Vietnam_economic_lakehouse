import os
import glob
import pandas as pd
from pathlib import Path
from minio import Minio
from minio.error import S3Error
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

MINIO_HOST = os.getenv("MINIO_HOST", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
MINIO_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "bronze")
HISTORICAL_DATASET_PATH = os.getenv("HISTORICAL_DATASET_PATH", "./historical_dataset")

TYPE_MAPPING = {
    "currency": "currency",
    "product_world": "product",
    "stock_index": "vietnam_index",
}

minio_client = Minio(
    MINIO_HOST,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

def ensure_bucket():
    try:
        if not minio_client.bucket_exists(MINIO_BUCKET):
            minio_client.make_bucket(MINIO_BUCKET)
            logger.info(f"✓ Bucket '{MINIO_BUCKET}' created")
        else:
            logger.info(f"✓ Bucket '{MINIO_BUCKET}' exists")
    except S3Error as e:
        logger.error(f"✗ Error creating bucket: {e}")
        raise

def get_asset_class(file_path):
    rel_path = os.path.relpath(file_path, HISTORICAL_DATASET_PATH)

    parts = Path(rel_path).parts
    if len(parts) > 1:
        folder = parts[0]
        return TYPE_MAPPING.get(folder, folder)
    else:
        return "economics"  

def upload_csv(local_file_path):
    try:

        file_name = os.path.basename(local_file_path)
        asset_class = get_asset_class(local_file_path)
        file_size = os.path.getsize(local_file_path)

        df = pd.read_csv(local_file_path)
        row_count = len(df)
        
        object_name = f"historical/{asset_class}/{file_name}"

        minio_client.fput_object(
            MINIO_BUCKET,
            object_name,
            local_file_path
        )
        
        logger.info(f"{asset_class:15} | {file_name:30} | {row_count:6} rows | {file_size/1024:.1f}KB")
        return True
        
    except Exception as e:
        logger.error(f"Error uploading {local_file_path}: {e}")
        return False

def upload_excel(local_file_path):
    try:
        file_name = os.path.basename(local_file_path)
        file_size = os.path.getsize(local_file_path)
        rel_path = os.path.relpath(local_file_path, HISTORICAL_DATASET_PATH)
        parts = Path(rel_path).parts
        
        year_folder = "unknown"
        for part in parts:
            if part.isdigit() and len(part) == 4:
                year_folder = part
                break
                
        object_name = f"historical/economic_report_excel_files/{year_folder}/{file_name}"
        
        minio_client.fput_object(
            MINIO_BUCKET,
            object_name,
            local_file_path
        )
        
        logger.info(f"{'excel_report':15} | {file_name:30} | Year: {year_folder} | {file_size/1024:.1f}KB")
        return True
    except Exception as e:
        logger.error(f"Error uploading excel {local_file_path}: {e}")
        return False

def ingest_all_historical():
    ensure_bucket()

    print("INGESTING HISTORICAL DATA TO BRONZE LAYER")

    csv_pattern = os.path.join(HISTORICAL_DATASET_PATH, "**/*.csv")
    csv_files = sorted(glob.glob(csv_pattern, recursive=True))
    
    if not csv_files:
        logger.warning(f"No CSV files found in {HISTORICAL_DATASET_PATH}")
    else:
        logger.info(f"Found {len(csv_files)} CSV files\n")
        logger.info(f"{'Asset Class':<15} | {'File Name':<30} | {'Rows':>6} | {'Size':<10}")
        
        uploaded = 0
        failed = 0
        
        for csv_file in csv_files:
            if upload_csv(csv_file):
                uploaded += 1
            else:
                failed += 1
        
        print("\n" + "-" * 80)
        logger.info(f"CSV Ingestion completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Successfully uploaded: {uploaded} files")
        if failed > 0:
            logger.warning(f"Failed: {failed} files")
        print("=" * 80 + "\n")

    excel_pattern = os.path.join(HISTORICAL_DATASET_PATH, "**/economic_report_excel_files/**/*.[xX][lL][sS]*")
    excel_files = sorted(glob.glob(excel_pattern, recursive=True))
    
    if excel_files:
        logger.info(f"Found {len(excel_files)} Excel files\n")
        excel_uploaded = 0
        excel_failed = 0
        for excel_file in excel_files:
            if upload_excel(excel_file):
                excel_uploaded += 1
            else:
                excel_failed += 1
        print("\n" + "-" * 80)
        logger.info(f"Excel Ingestion completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Successfully uploaded Excel: {excel_uploaded} files")
        if excel_failed > 0:
            logger.warning(f"Failed Excel: {excel_failed} files")
        print("=" * 80 + "\n")

if __name__ == "__main__":
    try:
        ingest_all_historical()
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        exit(1)