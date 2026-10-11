# Product Image Lakehouse

## Giới thiệu
Product Image Lakehouse là bài tập lớn môn Tích hợp và Phân tích Dữ liệu lớn.
Mục tiêu của dự án là xây dựng Data Lakehouse để quản lý và phân tích kho ảnh sản phẩm, sử dụng Object Storage thay cho cách lưu trữ file truyền thống.

## Công nghệ sử dụng
- **MinIO** - Object Storage tương thích S3 (Tầng Bronze / Storage)
- **Apache Spark** - Xử lý dữ liệu lớn (ETL / Tầng Silver)
- **Apache Iceberg** - Quản lý bảng Lakehouse (Tầng Gold)
- **Parquet** - Định dạng lưu trữ dữ liệu tối ưu
- **Docker / Native Services** - Triển khai môi trường hạ tầng

## Trạng thái triển khai (Người 1 — Hoàn thành 100%)
- **Hệ thống Object Storage (MinIO):** Đã khởi chạy thành công, sẵn sàng cung cấp S3 API.
- **Bucket & Dataset:** Đã khởi tạo bucket `product-images` và chuẩn bị đầy đủ bộ dataset ảnh sản phẩm cùng file metadata (`styles.csv` / `products.csv`).
- **Kiểm thử CRUD:** Đã kiểm tra toàn diện các chức năng Upload, List, Download, Delete thông qua `boto3` và đảm bảo hoạt động hoàn hảo.

## Dataset
- **Dataset:** Fashion Product Images (Small)
- **Link tải:** [Kaggle Dataset](https://www.kaggle.com/datasets/paramaggarwal/fashion-product-images-small?select=images)
- **Thống kê dữ liệu:**
  - 44,446 bản ghi metadata trong `styles.csv`
  - 44,441 ảnh sản phẩm thực tế đã được chuẩn bị trên hệ thống lưu trữ.

## Kiến trúc dự kiến
Dataset 
  -> MinIO Object Storage (Bronze Layer) 
  -> Apache Spark (Silver Layer) 
  -> Gold Layer 
  -> Apache Iceberg / Spark SQL

## Cấu trúc project
product-image-lakehouse/
├── data/        - Dữ liệu đầu vào (styles.csv và thư mục ảnh)
├── docker/      - Cấu hình môi trường triển khai
├── docs/        - Tài liệu và báo cáo đề tài
├── notebooks/   - Jupyter Notebook phân tích dữ liệu
├── scripts/     - Script hỗ trợ CRUD và ingest dữ liệu (test_crud.py, upload,...)
└── spark/       - Spark jobs và code ETL bàn giao (etl_bronze_to_silver.py)

---

##  Hướng dẫn kết nối MinIO dành cho Người 2 & Người 3
Kho Object Storage (MinIO) của hạ tầng đang chạy tại máy cục bộ với thông số cấu hình tiêu chuẩn sau:
- **S3 API Endpoint:** `http://localhost:9000`
- **MinIO Web Console:** `http://localhost:9001`
- **Access Key:** `minioadmin`
- **Secret Key:** `minioadmin`
- **Bucket chính:** `product-images`

### Mẫu cấu hình Spark kết nối MinIO (dành cho Người 2):
```python
spark = SparkSession.builder \
    .appName("ProductImage_Lakehouse_ETL") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.endpoint", "[http://127.0.0.1:9000](http://127.0.0.1:9000)") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()