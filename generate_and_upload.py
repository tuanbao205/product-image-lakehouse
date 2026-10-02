import os
import csv
import random
from datetime import datetime
import boto3
from botocore.client import Config

# Cấu hình MinIO
MINIO_ENDPOINT = "http://localhost:9000"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"
BUCKET_NAME = "product-images"

# Khởi tạo S3 Client
s3_client = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    config=Config(signature_version="s3v4"),
    region_name="us-east-1"
)

categories = ["electronics", "fashion", "food"]
formats = ["JPG", "PNG"]
metadata_records = []

print("Đang tiến hành tạo và upload ảnh mẫu lên MinIO...")

# Tạo từ 100 đến 1000 ảnh (ví dụ ở đây chúng ta tạo 150 ảnh để test nhanh, bạn có thể tăng lên tùy ý)
num_images = 150

# Tạo bucket nếu chưa tồn tại
try:
    s3_client.head_bucket(Bucket=BUCKET_NAME)
    print(f"Bucket '{BUCKET_NAME}' đã tồn tại.")
except:
    print(f"Tạo bucket '{BUCKET_NAME}'...")
    s3_client.create_bucket(Bucket=BUCKET_NAME)

for i in range(1, num_images + 1):
    image_id = f"IMG{i:03d}"
    product_id = f"P{i:03d}"
    category = random.choice(categories)
    fmt = random.choice(formats)
    
    object_path = f"product-images/{category}/{product_id}.{fmt.lower()}"
    
    # Dữ liệu ảnh giả lập (dummy binary data)
    dummy_image_data = f"Fake image content for {product_id}".encode('utf-8')
    
    # Upload lên MinIO
    # Lưu ý: Trong bucket product-images, ta đẩy theo cấu trúc category/product_id.fmt
    s3_object_key = f"{category}/{product_id}.{fmt.lower()}"
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_object_key,
        Body=dummy_image_data,
        ContentType=f'image/{fmt.lower()}'
    )
    
    # Thu thập metadata theo đúng yêu cầu đề tài
    record = {
        "image_id": image_id,
        "product_id": product_id,
        "category": category.capitalize(),
        "object_path": f"{category}/{product_id}.{fmt.lower()}",
        "format": fmt,
        "size": f"{random.uniform(0.5, 5.0):.1f} MB",
        "width": random.choice([1280, 1920, 2560]),
        "height": random.choice([720, 1080, 1440]),
        "upload_time": datetime.now().strftime("%d/%m/%Y")
    }
    metadata_records.append(record)
    
    if i % 50 == 0:
        print(f"Đã upload {i}/{num_images} ảnh...")

# Lưu file products.csv làm dataset metadata cho Spark
csv_file = "products.csv"
fields = ["image_id", "product_id", "category", "object_path", "format", "size", "width", "height", "upload_time"]

with open(csv_file, mode='w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=fields)
    writer.writeheader()
    writer.writerows(metadata_records)

print(f"\n✅ Đã upload thành công {num_images} ảnh lên MinIO bucket '{BUCKET_NAME}'!")
print(f"✅ Đã tạo file metadata '{csv_file}' sẵn sàng cho phần xử lý của Người 2 (Apache Spark).")
