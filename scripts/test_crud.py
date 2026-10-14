import tempfile
from pathlib import Path
from uuid import uuid4

try:
    from minio_config import MINIO_BUCKET, create_s3_client, ensure_bucket
except ImportError:
    from scripts.minio_config import MINIO_BUCKET, create_s3_client, ensure_bucket


s3_client = create_s3_client()


def test_full_crud():
    print("=== START MINIO CRUD TEST ===")
    ensure_bucket(s3_client, MINIO_BUCKET)
    test_key = f"raw/crud-test/{uuid4().hex}.jpg"
    payload = b"\xff\xd8\xff\xe0\x00\x10JFIF" + b"\x00" * 50

    with tempfile.TemporaryDirectory() as temporary_directory:
        source_path = Path(temporary_directory) / "test_product.jpg"
        download_path = Path(temporary_directory) / "downloaded_test_product.jpg"
        source_path.write_bytes(payload)

        try:
            print(f"\n[1/4] Upload to bucket '{MINIO_BUCKET}'...")
            s3_client.upload_file(str(source_path), MINIO_BUCKET, test_key)
            print(f" -> [OK] Uploaded: {test_key}")

            print("\n[2/4] List object...")
            response = s3_client.list_objects_v2(
                Bucket=MINIO_BUCKET,
                Prefix="raw/crud-test/",
            )
            listed_keys = {item["Key"] for item in response.get("Contents", [])}
            if test_key not in listed_keys:
                raise AssertionError(f"Uploaded object was not listed: {test_key}")
            print(f" -> [OK] Found: {test_key}")

            print("\n[3/4] Download object...")
            s3_client.download_file(MINIO_BUCKET, test_key, str(download_path))
            if download_path.read_bytes() != payload:
                raise AssertionError("Downloaded object does not match uploaded bytes.")
            print(" -> [OK] Downloaded bytes match the uploaded file.")

        finally:
            s3_client.delete_object(Bucket=MINIO_BUCKET, Key=test_key)
            print(f"\n[4/4] Removed temporary test object: {test_key}")

    print("\n=== MINIO CRUD TEST PASSED ===")

if __name__ == "__main__":
    test_full_crud()