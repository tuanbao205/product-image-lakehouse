import boto3
from botocore.client import Config

s3_client = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="admin",
    aws_secret_access_key="password123",
    config=Config(signature_version="s3v4"),
    region_name="us-east-1"
)

# Ví dụ download một ảnh từ MinIO về máy local
s3_client.download_file("product-images", "raw/1591.jpg", "downloaded_sample.jpg")
print("Đã download file thành công về máy local!")