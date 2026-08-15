import os
from pathlib import Path
import boto3
from botocore.client import Config
from config import MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def ensure_bucket(bucket: str):
    client = s3_client()
    buckets = [b["Name"] for b in client.list_buckets().get("Buckets", [])]
    if bucket not in buckets:
        client.create_bucket(Bucket=bucket)


def upload_dir(bucket: str, prefix: str, local_dir: str):
    ensure_bucket(bucket)
    client = s3_client()
    local_dir = Path(local_dir)
    for path in local_dir.rglob("*"):
        if path.is_file():
            key = f"{prefix.rstrip('/')}/{path.relative_to(local_dir).as_posix()}"
            client.upload_file(str(path), bucket, key)


def download_prefix(bucket: str, prefix: str, local_dir: str):
    client = s3_client()
    local_dir = Path(local_dir)
    local_dir.mkdir(parents=True, exist_ok=True)
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix.rstrip("/") + "/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            rel = key[len(prefix.rstrip("/") + "/"):]
            if not rel:
                continue
            out = local_dir / rel
            out.parent.mkdir(parents=True, exist_ok=True)
            client.download_file(bucket, key, str(out))
