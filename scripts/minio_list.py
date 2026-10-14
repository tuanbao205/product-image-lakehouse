try:
    from minio_config import MINIO_BUCKET, create_s3_client
except ImportError:
    from scripts.minio_config import MINIO_BUCKET, create_s3_client

s3_client = create_s3_client()
response = s3_client.list_objects_v2(Bucket=MINIO_BUCKET, Prefix="raw/", MaxKeys=10)
print("Danh sách các object trong MinIO:")
for obj in response.get('Contents', []):
    print(f"- {obj['Key']} ({obj['Size']} bytes, Last Modified: {obj['LastModified']})")
    