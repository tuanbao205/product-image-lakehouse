import boto3
from botocore.client.config import Config

s3_client = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="admin",
    aws_secret_access_key="password123",
    config=Config(signature_version="s3v4"),
    region_name="us-east-1"
)

# Ví dụ xóa một object trên MinIO
s3_client.delete_object(Bucket="product-images", Key="raw/1591.jpg")
print("Đã xóa object thành công trên MinIO!")