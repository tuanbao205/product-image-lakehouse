# Product Image Lakehouse

## Giới thiệu

Product Image Lakehouse là bài tập lớn môn Tích hợp và Phân tích Dữ liệu lớn.

Mục tiêu của dự án là xây dựng Data Lakehouse để quản lý và phân tích kho ảnh sản phẩm, sử dụng Object Storage thay cho cách lưu trữ file truyền thống.

## Công nghệ dự kiến

- MinIO - Object Storage tương thích S3
- Apache Spark - xử lý dữ liệu
- Apache Iceberg - quản lý bảng Lakehouse
- Parquet - định dạng lưu trữ dữ liệu
- Docker - triển khai môi trường

## Dataset

Dataset: Fashion Product Images (Small)

Dữ liệu hiện tại:

- 44,446 bản ghi metadata trong styles.csv
- 44,441 ảnh sản phẩm
- 5 bản ghi metadata không có ảnh tương ứng

Ảnh sản phẩm không được lưu trực tiếp trên GitHub. Dataset ảnh sẽ được đưa vào MinIO trong quá trình triển khai.

## Kiến trúc dự kiến

Dataset
  -> MinIO Object Storage
  -> Apache Spark
  -> Bronze
  -> Silver
  -> Gold
  -> Apache Iceberg / Spark SQL

## Cấu trúc project

product-image-lakehouse/
  data/        - Dữ liệu đầu vào
  docker/      - Cấu hình Docker
  docs/        - Tài liệu và báo cáo
  notebooks/   - Notebook phân tích dữ liệu
  scripts/     - Script hỗ trợ và ingest dữ liệu
  spark/       - Spark jobs
