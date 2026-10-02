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

response = s3_client.list_objects_v2(Bucket="product-images", MaxKeys=10)
print("Danh sách các object trong MinIO:")
for obj in response.get('Contents', []):
    print(f"- {obj['Key']} ({obj['Size']} bytes, Last Modified: {obj['LastModified']})")
    