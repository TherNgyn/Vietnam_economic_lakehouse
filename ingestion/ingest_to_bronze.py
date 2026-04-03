import os
import glob
from minio import Minio
from minio.error import S3Error
import logging
from datetime import datetime


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

MINIO_HOST = os.getenv("MINIO_HOST", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "bronze")
HISTORICAL_DATASET_PATH = os.getenv("HISTORICAL_DATASET_PATH", "historical_dataset")

minio_client = Minio(
    MINIO_HOST,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

def create_bucket_if_not_exists(bucket_name):
    try:
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)
            logger.info(f"Bucket '{bucket_name}' created successfully")
        else:
            logger.info(f"Bucket '{bucket_name}' already exists")
    except S3Error as e:
        logger.error(f"Error creating bucket: {e}")
        raise

def upload_file_to_minio(local_file_path, bucket_name, object_name):
    try:
        file_size = os.path.getsize(local_file_path)
        minio_client.fput_object(
            bucket_name,
            object_name,
            local_file_path
        )
        logger.info(f"Uploaded: {object_name} ({file_size} bytes)")
        return True
    except S3Error as e:
        logger.error(f"Error uploading {object_name}: {e}")
        return False

def ingest_historical_data():
    create_bucket_if_not_exists(MINIO_BUCKET)
    csv_files = glob.glob(os.path.join(HISTORICAL_DATASET_PATH, "**/*.csv"), recursive=True)
    
    if not csv_files:
        logger.warning("No CSV files found in historical_dataset")
        return
    
    logger.info(f"Found {len(csv_files)} CSV files to upload")
    
    uploaded_count = 0
    failed_count = 0
    
    for csv_file in csv_files:
        object_name = os.path.relpath(csv_file, HISTORICAL_DATASET_PATH)
        # Normalize path để dùng forward slash
        object_name = object_name.replace("\\", "/")
        object_name = f"historical_dataset/{object_name}"
        
        if upload_file_to_minio(csv_file, MINIO_BUCKET, object_name):
            uploaded_count += 1
        else:
            failed_count += 1
    
    logger.info(f"Ingestion completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Successfully uploaded: {uploaded_count} files")
    logger.info(f"Failed: {failed_count} files")
# khi cào (cập nhật hàng ngày)
def ingest_single_file(file_path):
    logger.info(f"Ingesting single file: {file_path}")
    
    if not os.path.exists(file_path):
        logger.error(f"File not found: {file_path}")
        return False
    
    create_bucket_if_not_exists(MINIO_BUCKET)
    
    object_name = os.path.relpath(file_path, HISTORICAL_DATASET_PATH)
    object_name = object_name.replace("\\", "/")
    object_name = f"historical_dataset/{object_name}"
    
    return upload_file_to_minio(file_path, MINIO_BUCKET, object_name)

if __name__ == "__main__":
    try:
        ingest_historical_data()
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        exit(1)