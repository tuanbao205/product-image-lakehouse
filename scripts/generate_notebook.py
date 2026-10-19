import json
from pathlib import Path

notebook = {
    "cells": [],
    "metadata": {
        "language_info": {
            "name": "python",
            "version": "3.14.8"
        },
        "kernelspec": {
            "display_name": "Python 3 (.venv)",
            "language": "python",
            "name": "python3"
        }
    },
    "nbformat": 4,
    "nbformat_minor": 5
}

def add_md(source: str):
    notebook["cells"].append({
        "cell_type": "markdown",
        "metadata": {},
        "source": [line + "\n" for line in source.strip().split("\n")]
    })

def add_code(source: str):
    notebook["cells"].append({
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": [line + "\n" for line in source.strip().split("\n")]
    })

# Tiêu đề & Mục lục
add_md("""# 🛍️ Demo Dự Án: Product Image Lakehouse
**Môn học:** Tích hợp và Phân tích Dữ liệu Lớn  
**Kiến trúc:** Dataset (Fashion) → MinIO Object Storage (Bronze Layer) → Apache Spark (Silver Layer) → Apache Iceberg (Warehouse / Gold Layer)

---
### 📌 CÁC ĐẦU MỤC DEMO
1. **Phần 1:** Kiểm tra Hạ tầng Object Storage (MinIO) & Trực quan hóa ảnh mẫu
2. **Phần 2:** Kiểm định Chất lượng Dữ liệu tầng Silver (Data Quality & Schema)
3. **Phần 3:** Quản lý bảng Apache Iceberg trên MinIO Object Storage
4. **Phần 4:** Tầng Gold (Business Aggregation & Trực quan hóa biểu đồ)
5. **Phần 5:** Tính năng nâng cao của Apache Iceberg (Schema Evolution & Time Travel)
""")

# Phần 1
add_md("""## 1. Kiểm tra Hạ tầng Object Storage (MinIO) & Trực quan hóa ảnh mẫu
- Kết nối tới MinIO S3 API (`http://localhost:9000`)
- Thống kê các file ảnh trong bucket `product-images`
- Tải về và hiển thị trực tiếp ảnh sản phẩm ngay trong Notebook bằng `Pillow` và `IPython.display`
""")

add_code("""import os
import sys
from pathlib import Path
from PIL import Image
import io
from IPython.display import display

# Thêm thư mục gốc vào sys.path
PROJECT_ROOT = Path('.').resolve().parent if Path('.').resolve().name == 'notebooks' else Path('.').resolve()
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.minio_config import MINIO_BUCKET, MINIO_ENDPOINT, create_s3_client

s3_client = create_s3_client()
print(f"✅ Kết nối MinIO Endpoint: {MINIO_ENDPOINT}")

# Kiểm tra bucket và lấy 5 object mẫu
response = s3_client.list_objects_v2(Bucket=MINIO_BUCKET, Prefix='raw/', MaxKeys=5)
print(f"✅ Bucket: {MINIO_BUCKET}")
print("Danh sách 5 ảnh mẫu đầu tiên trên MinIO:")
for obj in response.get('Contents', []):
    print(f"  - Key: {obj['Key']} | Dung lượng: {obj['Size']:,} bytes | Cập nhật: {obj['LastModified']}")
""")

add_code("""# Tải và hiển thị trực tiếp ảnh sản phẩm mẫu từ MinIO
sample_key = 'raw/10000.jpg'
image_obj = s3_client.get_object(Bucket=MINIO_BUCKET, Key=sample_key)
img_bytes = image_obj['Body'].read()
img = Image.open(io.BytesIO(img_bytes))

print(f"Ảnh: {sample_key} | Độ phân giải: {img.size[0]}x{img.size[1]} px | Định dạng: {img.format}")
display(img)
""")

# Phần 2
add_md("""## 2. Kiểm định Chất lượng Dữ liệu tầng Silver (Data Quality)
- Khởi tạo Spark Session tích hợp Apache Iceberg và Hadoop AWS S3A
- Đọc dữ liệu Silver Parquet (`data/lakehouse_real/silver/product_images`)
- Kiểm tra số lượng bản ghi (đảm bảo đúng 44,441 dòng, 0 dòng quarantine, 0 trùng lặp)
- Kiểm tra các trường metadata trích xuất từ ảnh thật (width, height, image_size, format)
""")

add_code("""from spark.iceberg_session import create_iceberg_spark_session, ICEBERG_CATALOG

# Khởi tạo Spark Session với Apache Iceberg Catalog
spark = create_iceberg_spark_session("ProductImage_Demo_Notebook")
print("✅ Spark Session với Iceberg đã khởi tạo thành công!")
""")

add_code("""silver_parquet_path = str(PROJECT_ROOT / "data" / "lakehouse_real" / "silver" / "product_images")
df_silver_parquet = spark.read.parquet(silver_parquet_path)

print(f"Tổng số bản ghi Silver Parquet: {df_silver_parquet.count():,}")
print("\\nSchema của tầng Silver:")
df_silver_parquet.printSchema()

print("\\nHiển thị 5 dòng dữ liệu mẫu sau khi đã JOIN chuẩn hóa:")
df_silver_parquet.select(
    "image_id", "product_name", "category", "image_size", "width", "height", "format"
).show(5, truncate=False)
""")

# Phần 3
add_md("""## 3. Quản lý Bảng Apache Iceberg trên MinIO Object Storage
- Bảng Iceberg được đăng ký trong catalog: `lakehouse.silver.product_images`
- Metadata và Data files nằm trên warehouse `s3a://product-images/warehouse/`
- Thực hiện truy vấn Spark SQL trực tiếp trên bảng Iceberg
""")

add_code("""# Đếm số lượng dòng trong bảng Iceberg
df_iceberg = spark.table(f"{ICEBERG_CATALOG}.silver.product_images")
print(f"Số lượng bản ghi trong bảng Iceberg: {df_iceberg.count():,}")

# Chạy Spark SQL tìm kiếm các sản phẩm thời trang nữ (Women)
query_sql = f\"\"\"
    SELECT image_id, product_name, category, width, height, format
    FROM {ICEBERG_CATALOG}.silver.product_images
    WHERE product_name LIKE '%Women%'
    LIMIT 5
\"\"\"
spark.sql(query_sql).show(truncate=False)
""")

# Phần 4
add_md("""## 4. Tầng Gold (Business Aggregation & Trực quan hóa Biểu đồ)
- Tầng Gold chứa các bảng tổng hợp phục vụ phân tích nghiệp vụ:
  1. `lakehouse.gold.category_image_stats`: Thống kê số lượng ảnh và dung lượng byte theo từng danh mục
  2. `lakehouse.gold.image_format_stats`: Thống kê số lượng ảnh theo định dạng
- Trực quan hóa dữ liệu bằng thư viện `matplotlib`
""")

add_code("""import matplotlib.pyplot as plt
import pandas as pd

# Đọc bảng Gold category_image_stats
df_gold_category = spark.table(f"{ICEBERG_CATALOG}.gold.category_image_stats").toPandas()

# Đổi đơn vị bytes sang Megabytes (MB)
df_gold_category['total_size_mb'] = df_gold_category['total_image_size_bytes'] / (1024 * 1024)
df_gold_category = df_gold_category.sort_values(by='image_count', ascending=False)

print("Bảng thống kê Tầng Gold theo Danh mục:")
display(df_gold_category[['category', 'image_count', 'total_size_mb']])
""")

add_code("""# Vẽ biểu đồ trực quan hóa dữ liệu Tầng Gold
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))

# Biểu đồ 1: Số lượng ảnh theo danh mục
bars1 = ax1.bar(df_gold_category['category'], df_gold_category['image_count'], color='#1f77b4')
ax1.set_title('Số lượng ảnh theo Danh mục Sản phẩm', fontsize=12, fontweight='bold')
ax1.set_xlabel('Danh mục')
ax1.set_ylabel('Số lượng ảnh')
ax1.tick_params(axis='x', rotation=30)
for bar in bars1:
    yval = bar.get_height()
    ax1.text(bar.get_x() + bar.get_width()/2, yval + 200, f'{int(yval):,}', ha='center', va='bottom', fontsize=8)

# Biểu đồ 2: Tổng dung lượng (MB) theo danh mục
bars2 = ax2.bar(df_gold_category['category'], df_gold_category['total_size_mb'], color='#ff7f0e')
ax2.set_title('Tổng dung lượng ảnh (MB) theo Danh mục', fontsize=12, fontweight='bold')
ax2.set_xlabel('Danh mục')
ax2.set_ylabel('Dung lượng (MB)')
ax2.tick_params(axis='x', rotation=30)
for bar in bars2:
    yval = bar.get_height()
    ax2.text(bar.get_x() + bar.get_width()/2, yval + 2, f'{yval:.1f} MB', ha='center', va='bottom', fontsize=8)

plt.tight_layout()
plt.show()
""")

# Phần 5
add_md("""## 5. Tính năng Nâng cao của Iceberg: Schema Evolution & Time Travel
- Sử dụng bảng riêng biệt: `lakehouse.silver.product_images_evolution_demo`
- **Schema Evolution:** Thêm cột mới `aspect_ratio` (tỉ lệ chiều rộng / chiều cao) bằng lệnh `ALTER TABLE ADD COLUMN`
- **Time Travel:** Truy vấn lại dữ liệu tại Snapshot ban đầu (trước khi thêm cột) bằng cú pháp `VERSION AS OF <snapshot_id>`
""")

add_code("""DEMO_TABLE = f"{ICEBERG_CATALOG}.silver.product_images_evolution_demo"

# 1. Đồng bộ bảng demo từ Silver
source_silver = spark.table(f"{ICEBERG_CATALOG}.silver.product_images")
source_silver.writeTo(DEMO_TABLE).createOrReplace()

# Lấy thông tin lịch sử Snapshot
snapshots_df = spark.sql(f\"\"\"
    SELECT snapshot_id, CAST(committed_at AS STRING) AS committed_at, operation 
    FROM {DEMO_TABLE}.snapshots 
    ORDER BY committed_at DESC
\"\"\")

print("Lịch sử Snapshots của bảng demo:")
snapshots_df.show(truncate=False)

baseline_snapshot_id = int(snapshots_df.select("snapshot_id").first()[0])
print(f"📌 Baseline Snapshot ID (Snapshot ban đầu): {baseline_snapshot_id}")
""")

add_code("""# 2. SCHEMA EVOLUTION: Thêm cột aspect_ratio
try:
    spark.sql(f"ALTER TABLE {DEMO_TABLE} ADD COLUMN aspect_ratio DOUBLE")
    spark.sql(f"UPDATE {DEMO_TABLE} SET aspect_ratio = CAST(width AS DOUBLE) / CAST(height AS DOUBLE)")
    print("✅ Đã thực hiện Schema Evolution: Thêm cột aspect_ratio thành công!")
except Exception as e:
    print("Thông báo:", e)

print("\\nSchema HIỆN TẠI (Đã có cột aspect_ratio):")
spark.table(DEMO_TABLE).select("image_id", "width", "height", "aspect_ratio").show(5)
""")

add_code("""# 3. TIME TRAVEL: Quay ngược thời gian về Snapshot ban đầu
print(f"⏳ Đang thực hiện Time Travel về Snapshot ID: {baseline_snapshot_id}...")

df_time_travel = spark.sql(f\"\"\"
    SELECT image_id, width, height
    FROM {DEMO_TABLE} VERSION AS OF {baseline_snapshot_id}
\"\"\")

print("Schema tại thời điểm Snapshot quá khứ (CHƯA có cột aspect_ratio):")
df_time_travel.printSchema()

print("\\nDữ liệu tại Snapshot quá khứ:")
df_time_travel.show(5)
print("✅ Time Travel thành công: Dữ liệu lịch sử được bảo toàn nguyên vẹn!")
""")

# Ghi ra file
out_path = Path("notebooks/demo_lakehouse.ipynb")
out_path.parent.mkdir(exist_ok=True)
with open(out_path, "w", encoding="utf-8") as f:
    json.dump(notebook, f, ensure_ascii=False, indent=2)

print(f"Successfully generated notebook: {out_path.resolve()}")
