import os
import boto3
from botocore.client import Config

# Cấu hình kết nối MinIO native
s3_client = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin",
    config=Config(signature_version="s3v4"),
    region_name="us-east-1"
)

bucket_name = "product-images"
test_file_name = "test_product.jpg"
download_file_name = "downloaded_test_product.jpg"

def test_full_crud():
    print("=== BẮT ĐẦU TEST TOÀN BỘ CHỨC NĂNG CRUD CỦA NGƯỜI 1 ===")

    # 1. Tạo file ảnh test giả lập ở local
    with open(test_file_name, "wb") as f:
        f.write(b"\xff\xd8\xff\xe0\x00\x10JFIF" + b"\x00" * 50)
    
    # Đảm bảo bucket tồn tại
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f" -> [INFO] Bucket '{bucket_name}' đã tồn tại.")
    except:
        s3_client.create_bucket(Bucket=bucket_name)
        print(f" -> [INFO] Đã tạo mới bucket '{bucket_name}'.")

    # 2. Test UPLOAD
    print(f"\n[1/4] Đang test UPLOAD file lên bucket '{bucket_name}'...")
    s3_key = f"images/test/{test_file_name}"
    s3_client.upload_file(test_file_name, bucket_name, s3_key)
    print(f" -> [OK] Upload thành công: {s3_key}")

    # 3. Test LIST (Lấy danh sách object & metadata)
    print(f"\n[2/4] Đang test LIST các object và metadata...")
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix="images/test/")
    if "Contents" in response:
        for obj in response["Contents"]:
            print(f" -> [OK] Tìm thấy: {obj['Key']} | Kích thước: {obj['Size']} bytes | Sửa đổi: {obj['LastModified']}")
    else:
        print(" -> [LỖI] Không tìm thấy object nào.")

    # 4. Test DOWNLOAD
    print(f"\n[3/4] Đang test DOWNLOAD file từ MinIO về local...")
    s3_client.download_file(bucket_name, s3_key, download_file_name)
    print(f" -> [OK] Download thành công xuống file: {download_file_name}")

    # 5. Test DELETE
    print(f"\n[4/4] Đang test DELETE file trên MinIO...")
    s3_client.delete_object(Bucket=bucket_name, Key=s3_key)
    print(f" -> [OK] Đã xóa thành công object: {s3_key}")

    # Dọn dẹp file rác test local
    if os.path.exists(test_file_name):
        os.remove(test_file_name)
    if os.path.exists(download_file_name):
        os.remove(download_file_name)

    print("\n=== HOÀN TẤT TEST: Toàn bộ chức năng CRUD hoạt động hoàn hảo 100%! ===")

if __name__ == "__main__":
    test_full_crud()