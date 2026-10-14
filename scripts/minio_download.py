try:
    from minio_config import MINIO_BUCKET, create_s3_client
except ImportError:
    from scripts.minio_config import MINIO_BUCKET, create_s3_client

s3_client = create_s3_client()

# Ví dụ download một ảnh từ MinIO về máy local
s3_client.download_file(MINIO_BUCKET, "raw/10000.jpg", "downloaded_sample.jpg")
print("Đã download file thành công về máy local!")