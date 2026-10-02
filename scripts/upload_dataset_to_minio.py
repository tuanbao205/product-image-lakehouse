import os
import boto3
from botocore.client import Config
from botocore.exceptions import NoCredentialsError

# 1. Cấu hình kết nối MinIO
MINIO_ENDPOINT = "http://localhost:9000"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"
BUCKET_NAME = "product-images"

# Đường dẫn tới thư mục chứa ảnh trên máy cá nhân
IMAGES_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../data/images"))

# Khởi tạo S3 Client tương thích với MinIO
s3_client = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    config=Config(signature_version="s3v4"),
    region_name="us-east-1"
)

def upload_images_to_minio():
    # Kiểm tra bucket đã tồn tại chưa, nếu chưa thì tạo mới
    try:
        s3_client.head_bucket(Bucket=BUCKET_NAME)
        print(f"Bucket '{BUCKET_NAME}' đã tồn tại trên MinIO.")
    except Exception:
        s3_client.create_bucket(Bucket=BUCKET_NAME)
        print(f"Đã tạo thành công bucket: '{BUCKET_NAME}'")

    if not os.path.exists(IMAGES_DIR):
        print(f"Lỗi: Không tìm thấy thư mục ảnh tại đường dẫn {IMAGES_DIR}")
        return

    # Lấy danh sách tất cả các file ảnh trong thư mục
    image_files = os.listdir(IMAGES_DIR)
    total_files = len(image_files)
    print(f"Tìm thấy {total_files} file trong thư mục images. Bắt đầu quá trình upload...")

    success_count = 0
    for filename in image_files:
        if filename.lower().endswith(('.jpg', '.jpeg', '.png')):
            local_path = os.path.join(IMAGES_DIR, filename)
            # Cấu trúc lưu trữ trên MinIO object path (ví dụ: raw/filename hoặc trực tiếp trong bucket)
            s3_object_key = f"raw/{filename}"

            try:
                s3_client.upload_file(local_path, BUCKET_NAME, s3_object_key)
                success_count += 1
                if success_count % 50 == 0:
                    print(f"Đã upload thành công {success_count}/{total_files} ảnh...")
            except Exception as e:
                print(f"Lỗi khi upload file {filename}: {e}")

    print(f"Hoàn tất! Đã upload thành công tổng cộng {success_count} ảnh lên MinIO bucket '{BUCKET_NAME}/raw/'.")

if __name__ == "__main__":
    upload_images_to_minio()