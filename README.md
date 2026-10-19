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

## Trạng thái triển khai
- Dataset Fashion thật nằm trong `data/raw/images/`; catalog nằm trong `data/styles.csv`.
- Đã upload và kiểm tra đủ 44,441 object trong bucket `product-images` theo key `raw/<tên-file>`.
- `products.csv` đã được tạo lại từ metadata thật của ảnh; mỗi ID trong manifest khớp với catalog.
- Spark Bronze → Silver đã chạy và được validator xác nhận tại `data/lakehouse_real/`: 44,441 dòng Silver, 0 dòng quarantine, 44,441 dòng join được với catalog. Các output cũ trong `data/bronze/`, `data/silver/` và `data/quarantine/` được giữ nguyên.
- MinIO CRUD smoke test đã pass: upload, list, download, so sánh bytes và xóa object test tạm.

## Dataset
- **Dataset:** Fashion Product Images (Small)
- **Link tải:** [Kaggle Dataset](https://www.kaggle.com/datasets/paramaggarwal/fashion-product-images-small?select=images)
- **Thống kê dữ liệu:**
  - 44,446 bản ghi metadata trong `styles.csv`
  - 44,441 file ảnh JPEG có trong `data/raw/images/`.
  - Tên cả 44,441 file hiện khớp với ID trong catalog; 5 sản phẩm trong catalog không có ảnh.

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

## MinIO và nạp dataset

Docker Compose hiện chỉ chạy MinIO; Spark ETL chạy cục bộ. Các script MinIO dùng chung cấu hình trong `scripts/minio_config.py`:

- S3 API: `http://localhost:9000`
- Web Console: `http://localhost:9001`
- Bucket: `product-images`
- Thông tin đăng nhập phát triển mặc định: `admin` / `password123`.

Có thể đặt `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY` và `MINIO_BUCKET` trong môi trường trước khi chạy Docker Compose hoặc các script. Nếu đổi credentials, cần dùng cùng giá trị khi khởi tạo MinIO và khi chạy script. Không dùng credentials mặc định cho môi trường chia sẻ hoặc production.

Chạy từ thư mục gốc trên Windows:

```powershell
docker compose -f .\docker\docker-compose.yml up -d
.\.venv\Scripts\python.exe -m pip install -r .\requirements.txt
.\.venv\Scripts\python.exe .\scripts\upload_dataset_to_minio.py --dry-run
.\.venv\Scripts\python.exe .\scripts\upload_dataset_to_minio.py
.\.venv\Scripts\python.exe .\scripts\test_crud.py
```

`--dry-run` chỉ kiểm tra ảnh, định dạng và việc khớp ID với catalog; không kết nối MinIO, không upload và không tạo manifest. Lệnh upload thật đẩy ảnh theo key `raw/<đường-dẫn-tương-đối>`; với bộ ảnh hiện tại, ví dụ `raw/10000.jpg`. Sau khi upload thành công, script ghi metadata ảnh thật vào `products.csv`. Các trường `product_id` và `image_id` lấy từ tên file, còn `product_name`/`category` lấy từ `styles.csv`. Ảnh lỗi, không khớp catalog hoặc upload thất bại được báo riêng và làm lệnh trả về mã lỗi; manifest chỉ chứa object upload thành công.

Các script `minio_list.py` và `minio_download.py` là ví dụ thao tác thủ công; script download lấy `raw/10000.jpg`. Script xóa yêu cầu truyền key cụ thể, ví dụ `.\.venv\Scripts\python.exe .\scripts\minio_delete.py --key raw/10000.jpg`; dùng cẩn thận vì thao tác này xóa object thật.

## Spark ETL Người 2 (Bronze → Silver)

ETL chạy cục bộ và đọc manifest CSV được sinh sau bước upload; Spark chưa đọc object trực tiếp từ MinIO ở giai đoạn này. Mặc định:

- `products.csv`: manifest ảnh thật (`image_id`, `product_id`, `product_name`, `category`, `object_path`, `image_size`, `format`, `width`, `height`, `upload_time`).
- `data/styles.csv`: catalog sản phẩm (`id`, `productDisplayName`, `masterCategory`, `subCategory`).
- `object_path` là key trong bucket `product-images`, chẳng hạn `raw/10000.jpg`; cột này được giữ trong Silver dưới tên `image_path`.
- Cả kích thước, định dạng và độ phân giải đều được đọc từ file ảnh thật bằng Pillow; không dùng các giá trị ngẫu nhiên từ script demo cũ.

Cài thư viện và chạy từ thư mục gốc trên Windows:

```powershell
.\.venv\Scripts\Activate.ps1
python -m pip install -r .\requirements.txt
$env:LAKEHOUSE_DATA_DIR = (Join-Path $PWD "data\lakehouse_real")
$env:PRODUCT_CATALOG_CSV = (Join-Path $PWD "data\styles.csv")
python .\spark\etl_bronze_to_silver.py
python .\spark\validate_output.py
```

Trên Windows, cần JDK 17/21 và thư mục Hadoop local chứa `bin\winutils.exe`, `bin\hadoop.dll`. Nếu `JAVA_HOME`/`HADOOP_HOME` chưa có trong môi trường, script tự tìm JDK trong `C:\Program Files\Java` hoặc `C:\Program Files\Eclipse Adoptium` và Hadoop trong `%USERPROFILE%\.hadoop` hoặc `C:\hadoop`. Spark Python workers tự dùng đúng `.venv` hiện tại.

Với các biến môi trường trong lệnh trên, Bronze Parquet/CSV nằm trong `data/lakehouse_real/bronze/`, Silver Parquet nằm trong `data/lakehouse_real/silver/product_images/`, quarantine nằm trong `data/lakehouse_real/quarantine/product_images/`, và thống kê nằm trong `data/lakehouse_real/etl_report.json`. Nếu không đặt `LAKEHOUSE_DATA_DIR`, ETL dùng `data/` làm output root. Silver giữ lại ảnh không khớp catalog nhưng đếm riêng trong báo cáo; với dataset hiện tại, ID file đã được xác nhận khớp catalog. Thư mục Parquet gồm một hoặc nhiều file `part-*.parquet`.

Kích thước có đơn vị `B`, `KB`/`MB`/`GB` thập phân, `KiB`/`MiB`/`GiB` nhị phân; dữ liệu đầu ra được chuẩn hóa về byte. Quy tắc chọn bản ghi khi trùng `image_id`: `created_at` mới nhất, sau đó `image_path`, rồi `product_id`. JOIN dùng `product_id` với `styles.csv.id` sau khi trim chuỗi; bản ghi không khớp vẫn được giữ trong Silver, tên sản phẩm có thể NULL, và được đếm riêng trong báo cáo để không làm mất dữ liệu âm thầm.

Có thể thay đường dẫn mặc định bằng `IMAGE_METADATA_CSV`, `PRODUCT_CATALOG_CSV`, `LOCAL_IMAGE_DIR`, `LAKEHOUSE_DATA_DIR` hoặc đặt Spark master qua `SPARK_MASTER`.

## Spark – Iceberg trên MinIO (Người 3)

Spark và Iceberg dùng cùng cấu hình MinIO trong `scripts/minio_config.py`. Runtime đang ghim theo phiên bản PySpark của project: Spark `4.0.1`, Scala `2.13`, Iceberg runtime `1.12.0`, Hadoop/S3A `3.4.1`. Iceberg Hadoop Catalog lưu cả dữ liệu bảng và metadata trong warehouse `s3a://product-images/warehouse/`; MinIO dùng S3 path-style access.

Khởi động MinIO rồi kiểm tra cấu hình bằng một bảng tạm: lệnh tạo namespace/table ngẫu nhiên, ghi/đọc một dòng qua Iceberg rồi xóa bảng với `PURGE`.

```powershell
docker compose -f .\docker\docker-compose.yml up -d
.\.venv\Scripts\python.exe .\spark\validate_iceberg_minio.py
```

Lệnh kiểm tra tải các JAR Maven ở lần chạy đầu; cần truy cập Maven Central. Cấu hình phiên Spark có thể được dùng trong job qua `create_iceberg_spark_session()` từ `spark/iceberg_session.py`. Có thể tùy chỉnh `ICEBERG_CATALOG`, `ICEBERG_WAREHOUSE`, `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, `MINIO_BUCKET`, `MINIO_REGION` và `SPARK_MASTER` bằng environment variables. `ICEBERG_WAREHOUSE` mặc định là `s3a://<MINIO_BUCKET>/warehouse/`.

Để nạp Silver Parquet của dataset đã upload lên Iceberg, chạy:

```powershell
.\.venv\Scripts\python.exe .\spark\load_silver_to_iceberg.py
```

Job đọc `data/lakehouse_real/silver/product_images/`, kiểm tra báo cáo ETL và yêu cầu đúng 44.441 dòng trước khi ghi bảng `lakehouse.silver.product_images`. Sau đó đọc lại bảng Iceberg và xác nhận lại số dòng. Nếu bảng đã tồn tại, lần chạy mới sẽ thay thế bảng bằng Silver hiện tại. Có thể đặt `SILVER_PARQUET_PATH` để dùng đường dẫn Silver khác; job vẫn yêu cầu báo cáo ETL trong `LAKEHOUSE_DATA_DIR` (mặc định `data/lakehouse_real`).

### Tạo Gold tables

Sau khi đã có bảng Silver Iceberg, tạo các bảng tổng hợp Gold bằng:

```powershell
.\.venv\Scripts\python.exe .\spark\build_gold_tables.py
```

Job tạo hoặc thay thế `lakehouse.gold.category_image_stats` (số ảnh và tổng dung lượng byte theo danh mục) và `lakehouse.gold.image_format_stats` (số ảnh theo định dạng). Job đọc lại cả hai bảng, kiểm tra chúng khớp với kết quả tổng hợp và đối chiếu tổng số ảnh/số byte với Silver. Danh mục hoặc định dạng bị thiếu được gom vào `UNKNOWN`; định dạng `JPEG` được chuẩn hóa thành `JPG`.

### Snapshot, Time Travel và Schema Evolution

Để demo an toàn, job làm việc trên bảng riêng `lakehouse.silver.product_images_evolution_demo`; bảng Silver nguồn không bị sửa. Job tạo snapshot cơ sở từ Silver, thêm cột `aspect_ratio` kiểu `DOUBLE` bằng `ALTER TABLE ... ADD COLUMN`, rồi tính tỷ lệ chiều rộng trên chiều cao. Nó đọc lại snapshot cũ theo ID, đối chiếu số dòng và hiển thị snapshot/schema trước và sau thay đổi.

```powershell
.\.venv\Scripts\python.exe .\spark\demo_iceberg_evolution.py
```

Trong đầu ra, dùng ID snapshot cơ sở được in ra để truy vấn dữ liệu trước khi thêm cột:

```sql
SELECT image_id, width, height
FROM lakehouse.silver.product_images_evolution_demo
VERSION AS OF <BASELINE_SNAPSHOT_ID>;
```

Để xem schema và các giá trị mới nhất:

```sql
SELECT image_id, width, height, aspect_ratio
FROM lakehouse.silver.product_images_evolution_demo;

SELECT snapshot_id, committed_at, operation
FROM lakehouse.silver.product_images_evolution_demo.snapshots
ORDER BY committed_at, snapshot_id;
```

Mỗi lần chạy thay thế dữ liệu trong bảng demo cô lập bằng Silver hiện tại để tạo snapshot cơ sở nhất quán; job không thay thế hay sửa `lakehouse.silver.product_images`.

---

## Demo & Trực quan hóa

### 1. Trực quan hóa qua Streamlit Web Dashboard

Dự án cung cấp giao diện Web Dashboard trực quan tích hợp đầy đủ các tầng từ MinIO đến Iceberg:

```powershell
.\.venv\Scripts\streamlit.exe run dashboard.py
```

Dashboard hỗ trợ:
- **Hàng KPI:** Tổng số lượng ảnh (`44,441`), dung lượng (`541.5 MB`), số danh mục, tỉ lệ khớp catalog (100%).
- **Tầng Gold (BI):** Biểu đồ tương tác số lượng ảnh và dung lượng lưu trữ theo từng danh mục sản phẩm.
- **MinIO Image Explorer:** Lọc theo danh mục, tìm kiếm theo tên và hiển thị ảnh sản phẩm thật tải trực tiếp từ MinIO qua S3 API.
- **Kiến trúc Lakehouse:** Tóm tắt mô hình Medallion và câu lệnh minh họa Schema Evolution / Time Travel.

### 2. Demo từng bước qua Jupyter Notebook

Chạy file notebook:
👉 `notebooks/demo_lakehouse.ipynb` (sử dụng kernel `.venv`).