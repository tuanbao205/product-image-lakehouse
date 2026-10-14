import os
from typing import Any

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError


MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "password123")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "product-images")
MINIO_REGION = os.environ.get("MINIO_REGION", "us-east-1")


def create_s3_client() -> Any:
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4", max_pool_connections=16),
        region_name=MINIO_REGION,
    )


def ensure_bucket(client: Any, bucket_name: str = MINIO_BUCKET) -> None:
    try:
        client.head_bucket(Bucket=bucket_name)
    except ClientError as error:
        error_code = error.response.get("Error", {}).get("Code")
        status_code = error.response.get("ResponseMetadata", {}).get(
            "HTTPStatusCode"
        )
        if error_code not in {"404", "NoSuchBucket", "NotFound"} and status_code != 404:
            raise
        client.create_bucket(Bucket=bucket_name)
