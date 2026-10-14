# ĐỀ TÀI: PRODUCT IMAGE LAKEHOUSE
## Xây dựng Lakehouse quản lý kho ảnh sản phẩm trên Object Storage

---

## 1. Tổng quan đề tài

Trong các hệ thống thương mại điện tử, số lượng hình ảnh sản phẩm có thể tăng lên rất lớn. Việc lưu toàn bộ hình ảnh trực tiếp trên ổ đĩa của server (Local File System) gây khó khăn trong việc mở rộng dung lượng, quản lý dữ liệu và xử lý khi số lượng file tăng cao.

Đề tài xây dựng một **Data Lakehouse** trên **Object Storage**, trong đó:
- **MinIO** được sử dụng để lưu trữ ảnh sản phẩm thay cho ổ đĩa truyền thống.
- **Apache Spark** đảm nhiệm việc đọc, xử lý và phân tích metadata của ảnh.
- **Apache Iceberg** quản lý dữ liệu dưới dạng bảng Lakehouse, hỗ trợ các tính năng như snapshot, schema evolution và time travel.

---

## 2. Mục tiêu

Hệ thống cần thực hiện được các chức năng chính:
- Lưu trữ số lượng lớn ảnh sản phẩm trên MinIO Object Storage.
- Quản lý thông tin sản phẩm và metadata của ảnh như tên file, kích thước, định dạng, độ phân giải, thời gian upload và đường dẫn object.
- Sử dụng Apache Spark để xử lý, làm sạch và tổng hợp metadata.
- Sử dụng Apache Iceberg để xây dựng các bảng Lakehouse.
- Truy vấn và phân tích kho ảnh bằng Spark SQL.
- Demo các đặc trưng của Lakehouse: Snapshot, Time Travel và Schema Evolution.

---

## 3. Kiến trúc hệ thống

```text
         PRODUCT DATASET
         /               \
 products.csv          Images
       |                  |
       |                  v
       |          +---------------+
       |          |     MinIO     |
       |          | Object Storage|
       |          +-------+-------+
       |                  |
       +--------+---------+
                |
                v
        +---------------+
        | Apache Spark  |
        | ETL / Process |
        +-------+-------+
                |
                v
        +---------------+
        |Apache Iceberg |
        |   Lakehouse   |
        +-------+-------+
                |
        +-------+-------+
        |               |
        v               v
    Spark SQL        Analytics
```

### Vai trò công nghệ:

| Công nghệ | Vai trò |
| :--- | :--- |
| **MinIO** | Object Storage lưu ảnh |
| **Apache Spark** | Xử lý và phân tích dữ liệu |
| **Apache Iceberg** | Quản lý bảng Lakehouse |
| **Parquet** | Định dạng lưu dữ liệu có cấu trúc |
| **Spark SQL** | Truy vấn và phân tích |

---

## 4. Dữ liệu

### Ảnh thực tế được lưu trên MinIO:
```text
product-images/
├── electronics/
│   ├── P001.jpg
│   └── P002.jpg
├── fashion/
└── food/
```

### Lakehouse quản lý metadata tương ứng:
| Thẻ / Metadata | Giá trị |
| :--- | :--- |
| `image_id` | IMG001 |
| `product_id` | P001 |
| `category` | Electronics |
| `object_path` | product-images/electronics/P001.jpg |
| `format` | JPG |
| `size` | 2.4 MB |
| `width` | 1920 |
| `height` | 1080 |
| `upload_time` | 09/09/2026 |

Từ dữ liệu này có thể phân tích số lượng ảnh theo danh mục, tổng dung lượng lưu trữ, định dạng ảnh phổ biến, ảnh có kích thước lớn, sản phẩm thiếu ảnh...

---

## 5. Nội dung demo

Khi bảo vệ, nhóm sẽ demo một quy trình hoàn chỉnh:

```text
Dataset
   ↓
Upload ảnh lên MinIO
   ↓
Spark đọc & xử lý metadata
   ↓
Tạo Iceberg Table
   ↓
Spark SQL Analytics
   ↓
Thêm/sửa dữ liệu
   ↓
Snapshot mới
   ↓
Time Travel về dữ liệu cũ
```

Đặc biệt:
- **Schema Evolution** sẽ được demo bằng việc thêm thuộc tính mới cho metadata mà không phải xây dựng lại toàn bộ dataset.
- **Time Travel** cho phép truy vấn lại trạng thái dữ liệu trước khi bị thay đổi.

---

## 6. Phân chia công việc nhóm 3 người

| Thành viên | Công việc |
| :--- | :--- |
| **Member 1** | MinIO, Object Storage, bucket, upload/download ảnh, S3 API |
| **Member 2** | Apache Spark, ETL, xử lý metadata, Parquet |
| **Member 3** | Apache Iceberg, Spark SQL, Snapshot, Time Travel, Schema Evolution |

---

## 7. Kế hoạch 4 tuần

- **Tuần 1:** Tìm hiểu kiến trúc, chuẩn bị dataset, Docker và MinIO.
- **Tuần 2:** Hoàn thành Object Storage và Spark ETL.
- **Tuần 3:** Tích hợp Iceberg, SQL, Snapshot, Time Travel, Schema Evolution.
- **Tuần 4:** Test dữ liệu lớn, hoàn thiện demo, báo cáo và slide.

---

## 8. Kết quả mong đợi

Sau khi hoàn thành, nhóm có một mini Data Lakehouse chạy được trên máy cá nhân, chứng minh khả năng sử dụng Object Storage để lưu dữ liệu phi cấu trúc thay cho Local File System, đồng thời sử dụng Spark và Iceberg để quản lý, xử lý và phân tích metadata ở quy mô lớn.

> **Stack chính:** Docker → MinIO → Apache Spark → Apache Iceberg → Spark SQL

---
---

# CHI TIẾT THỰC HIỆN DỰ ÁN

## 1. Bài toán

Các hệ thống thương mại điện tử có thể chứa số lượng rất lớn hình ảnh sản phẩm. Nếu lưu ảnh trực tiếp trong thư mục trên server:

```text
Server
└── uploads/
    ├── p001.jpg
    ├── p002.jpg
    ├── p003.jpg
    └── ...
```

thì khi dữ liệu tăng lên sẽ khó quản lý, mở rộng và phân tích.

Project xây dựng một Data Lakehouse trên Object Storage:
- **MinIO:** lưu trữ ảnh sản phẩm dưới dạng object.
- **Apache Spark:** đọc, làm sạch, xử lý và phân tích metadata.
- **Apache Iceberg:** quản lý dữ liệu dưới dạng bảng Lakehouse.
- **Parquet:** lưu dữ liệu có cấu trúc.
- **Spark SQL:** truy vấn dữ liệu.

---

## 2. Kiến trúc tổng thể

```text
            PRODUCT DATASET
            /               \
     products.csv          Images
          |                  |
          |                  v
          |             +---------+
          |             |  MinIO  |
          |             | Object  |
          |             | Storage |
          |             +----+----+
          |                  |
          +--------+---------+
                   |
                   v
             Apache Spark
           Extract Metadata
             Clean / ETL
                   |
                   v
            Apache Iceberg
             Data Lakehouse
                   |
          +--------+--------+
          |                 |
          v                 v
      Spark SQL         Analytics
```

---

## 3. Quy trình thực hiện dự án

### Bước 1 – Chuẩn bị Dataset
Tìm một dataset sản phẩm có cấu trúc:
```text
dataset/
│
├── products.csv
│
└── images/
    ├── 00001.jpg
    ├── 00002.jpg
    ├── 00003.jpg
    └── ...
```

`products.csv` có thể chứa: `product_id`, `product_name`, `category`, `price`, `image_name`.

*Ví dụ:*
```csv
P001 | iPhone  | Electronics | 25000000 | P001.jpg
P002 | T-Shirt | Fashion     | 300000   | P002.jpg
```
*Mục tiêu:* Ban đầu khoảng 10.000–50.000 ảnh là đủ để thử nghiệm. Nếu máy yếu có thể bắt đầu với 1.000 ảnh.

---

### Bước 2 – Dựng môi trường bằng Docker
Cả nhóm thống nhất chạy bằng Docker Compose để tránh tình trạng máy người này chạy được, máy người kia không chạy được.

```text
Docker
│
├── MinIO
│
├── Spark Master
├── Spark Worker 1
├── Spark Worker 2
│
└── Iceberg environment
```

Sau khi chạy lệnh:
```bash
docker compose up -d
```
Cần kiểm tra:
- MinIO → Running
- Spark Master → Running
- Spark Worker → Running

Đây là nền tảng trước khi code pipeline.

---

### Bước 3 – Xây dựng Object Storage
Tạo bucket trên MinIO:
```text
product-lakehouse
│
├── raw/
│
├── images/
│
├── silver/
│
└── warehouse/
```

Luồng Upload ảnh:
```text
Local Dataset ──> Upload Script ──> MinIO ──> product-lakehouse/images/
```

*Ví dụ:*
- `images/electronics/P001.jpg`
- `images/electronics/P002.jpg`
- `images/fashion/P003.jpg`

> Sau bước này phải demo được: Upload → List → Download → Delete object.

---

### Bước 4 – Thu thập metadata ảnh
Không đưa toàn bộ binary của ảnh vào bảng Iceberg. Ảnh nằm trong MinIO, còn Spark lấy thông tin của ảnh để tạo metadata.

*Ví dụ trích xuất:*
```text
P001.jpg
    ↓
Spark / Python
    ↓
--------------------------------
image_id      IMG001
product_id    P001
format        JPG
size          2457600
width         1920
height        1080
object_path   images/P001.jpg
--------------------------------
```

Sau đó kết hợp với `products.csv` để tạo metadata hoàn chỉnh:
`product_id`, `product_name`, `category`, `price`, `image_id`, `image_path`, `image_size`, `image_format`, `width`, `height`, `upload_time`.

---

### Bước 5 – Xây dựng Bronze Layer
Bronze chứa dữ liệu gần với dữ liệu gốc nhất (chưa xử lý quá nhiều).

```text
                 BRONZE
products.csv ───────┐
                    │
Image Metadata ─────┼──→ Bronze
                    │
MinIO Object Info ──┘
```
*Mục tiêu:* Giữ được raw data để khi pipeline có vấn đề có thể xử lý lại.

---

### Bước 6 – Spark ETL
Spark đọc Bronze Data và thực hiện cleaning/transformation:

```text
Bronze ──> Apache Spark ──> Check NULL ──> Remove duplicate ──> Validate format ──> Transform ──> Join Product + Image ──> Silver
```

*Ví dụ kiểm tra:*
- `product_id = NULL` → invalid
- `image_size <= 0` → invalid
- `format = exe` → invalid
- `duplicate image_id` → remove/flag

Sau xử lý tạo ra dữ liệu sạch.

---

### Bước 7 – Silver Layer
Silver chứa dữ liệu đã được chuẩn hóa.

**Bảng: `product_images`**
- `image_id`
- `product_id`
- `product_name`
- `category`
- `image_path`
- `image_size`
- `format`
- `width`
- `height`
- `created_at`

*Ví dụ dữ liệu:*
`IMG001` | `P001` | `iPhone 17` | `Electronics` | `images/P001.jpg` | `2457600` | `JPG` | `1920` | `1080` | `2026-09-09`

---

### Bước 8 – Apache Iceberg
Spark ghi Silver Data thành Iceberg Table.

```text
Spark ──> Iceberg ──> Metadata + Parquet ──> MinIO
```

Iceberg chịu trách nhiệm quản lý: Table, Schema, Snapshots, Partitions, Metadata, Transaction.
> Đây là bước biến Data Lake thành mô hình Lakehouse.

---

### Bước 9 – Gold Layer
Từ Silver, Spark tổng hợp thành dữ liệu phục vụ analytics.

*Ví dụ bảng `storage_by_category`:*
| category | images | storage |
| :--- | :--- | :--- |
| Electronics | 15,230 | 28 GB |
| Fashion | 22,310 | 35 GB |
| Food | 8,210 | 12 GB |

*Ví dụ bảng `image_format_summary`:*
| format | count |
| :--- | :--- |
| JPG | 32,130 |
| PNG | 12,530 |
| WEBP | 1,090 |

Có thể tạo thêm: `large_images`, `products_without_images`, `images_by_resolution`, `images_by_format`.

---

### Bước 10 – Spark SQL Analytics
Cho phép truy vấn trực tiếp Lakehouse.

*Ví dụ SQL:*
```sql
SELECT 
    category, 
    COUNT(*) AS total_images, 
    SUM(image_size) AS total_storage
FROM product_images
GROUP BY category;
```

*Hoặc tìm ảnh quá lớn:*
```sql
SELECT * 
FROM product_images
WHERE image_size > 5000000;
```

---

### Bước 11 – Demo Schema Evolution
Ban đầu schema Version 1:
`image_id`, `product_id`, `image_path`, `image_size`

Có yêu cầu mới bổ sung: `width`, `height`, `compression_type`.

Cập nhật Iceberg thành Version 2:
```text
Version 1
image_id | product_id | image_path | image_size
                    ↓
Version 2
image_id | product_id | image_path | image_size | width | height | compression_type
```
*Mục tiêu:* Chứng minh schema có thể phát triển khi yêu cầu dữ liệu thay đổi mà không phải ghi đè hay tốn chi phí rebuild lại dữ liệu cũ.

---

### Bước 12 – Demo Snapshot & Time Travel
Đây nên là demo chính khi bảo vệ.

```text
Lần 1: 50,000 image records ───────────> Snapshot 1
                                             │
Sau đó thêm 5.000 ảnh: 55,000 records ──> Snapshot 2
                                             │
Sau đó vô tình xóa: 45,000 records ────> Snapshot 3
```

Hiện tại Snapshot 3 đang hiển thị 45,000 dòng. Nhưng Iceberg cho phép truy vấn:
- **Snapshot 1:** 50,000 dòng
- **Snapshot 2:** 55,000 dòng
- **Snapshot 3:** 45,000 dòng

→ Chứng minh khả năng **Time Travel** khôi phục/truy vấn dữ liệu quá khứ.

---

## 4. Pipeline cuối cùng

Khi hoàn thiện, toàn bộ project chạy theo pipeline:

```text
                DATASET
                    │
          ┌─────────┴─────────┐
          │                   │
     products.csv           Images
          │                   │
          │                   ▼
          │                 MinIO
          │                   │
          │             Object Storage
          │                   │
          └─────────┬─────────┘
                    ▼
               BRONZE LAYER
                    │
                    ▼
              Apache Spark
                    │
            Clean / Validate
              Transform / Join
                    │
                    ▼
               SILVER LAYER
                    │
                    ▼
              Apache Iceberg
                    │
             Parquet + Metadata
                    │
                    ▼
                GOLD LAYER
                    │
              Aggregation
                    │
                    ▼
               Spark SQL
                    │
                    ▼
                ANALYTICS
```

---

## 5. Chia công việc 3 thành viên

| Member | Phụ trách | Kết quả phải có |
| :--- | :--- | :--- |
| **Member 1** | Object Storage | Docker, MinIO, Bucket, Upload/Download, tổ chức ảnh |
| **Member 2** | Data Processing | Spark, đọc metadata, ETL, Bronze → Silver |
| **Member 3** | Lakehouse | Iceberg, Silver → Gold, SQL, Snapshot, Time Travel, Schema Evolution |

*Lưu ý:* Không nên tách hoàn toàn. Cuối cùng cả 3 cùng tích hợp thành một pipeline duy nhất.

---

## 6. Kế hoạch 4 tuần cụ thể

| Tuần | Công việc | Kết quả |
| :--- | :--- | :--- |
| **Tuần 1** | Nghiên cứu + Dataset + Docker + MinIO | Upload/download ảnh được |
| **Tuần 2** | Spark + metadata + ETL | Bronze → Silver chạy được |
| **Tuần 3** | Iceberg + Gold + SQL | Lakehouse hoàn chỉnh |
| **Tuần 4** | Time Travel + Schema Evolution + Test + báo cáo | Demo hoàn chỉnh |

### Milestones:
```text
TUẦN 1: Dataset + MinIO
   ↓
TUẦN 2: Spark ETL
   ↓
TUẦN 3: Iceberg Lakehouse
   ↓
TUẦN 4: Demo + Test + Report
```
> **Quan trọng:** Cuối tuần 3 phải có pipeline chạy được. Tuần 4 chỉ nên dành cho sửa lỗi, test, báo cáo và slide; không nên đến tuần cuối vẫn phát triển chức năng chính.

---

## 7. Kịch bản demo cuối kỳ

Đề xuất nhóm demo khoảng **8–10 phút** theo thứ tự:

1. **Giới thiệu bài toán:** Tại sao không lưu hàng loạt ảnh bằng local filesystem.
2. **MinIO:** Cho xem bucket chứa hàng nghìn ảnh.
3. **Upload:** Thêm một batch ảnh mới.
4. **Spark:** Chạy ETL và xử lý metadata.
5. **Iceberg:** Cho xem bảng Lakehouse.
6. **Spark SQL:** Query thống kê kho ảnh.
7. **Thay đổi dữ liệu:** Thêm/xóa một số records.
8. **Time Travel:** Truy vấn snapshot cũ.
9. **Schema Evolution:** Thêm field mới.
10. **Kết luận:** So sánh kiến trúc trước/sau.