from pyspark.sql import SparkSession

def create_spark_session():
    # Khởi tạo Spark Session tích hợp Apache Hadoop AWS S3A để kết nối MinIO
    spark = SparkSession.builder \
        .appName("ProductImage_Lakehouse_ETL") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://127.0.0.1:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
    
    return spark

def main():
    spark = create_spark_session()
    print("=== Khởi tạo Spark Session kết nối MinIO thành công! ===")

    # Đọc file metadata styles.csv hoặc products.csv từ MinIO (hoặc thư mục data local)
    # Ví dụ đọc trực tiếp từ bucket product-images:
    bucket_name = "product-images"
    csv_path = f"s3a://{bucket_name}/products.csv"
    
    try:
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_path)
        print(f"Đã đọc thành công metadata từ MinIO. Số lượng bản ghi: {df.count()}")
        df.show(5)
    except Exception as e:
        print(f"Lưu ý: Chưa tìm thấy file products.csv trên bucket. Chi tiết lỗi: {e}")

    spark.stop()

if __name__ == "__main__":
    main()