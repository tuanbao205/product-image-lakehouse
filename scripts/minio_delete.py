import argparse

try:
    from minio_config import MINIO_BUCKET, create_s3_client
except ImportError:
    from scripts.minio_config import MINIO_BUCKET, create_s3_client


def main() -> None:
    parser = argparse.ArgumentParser(description="Delete an object from MinIO.")
    parser.add_argument(
        "--key",
        required=True,
        help="Exact object key to delete, for example raw/10000.jpg.",
    )
    object_key = parser.parse_args().key
    create_s3_client().delete_object(Bucket=MINIO_BUCKET, Key=object_key)
    print(f"Deleted object: {object_key}")


if __name__ == "__main__":
    main()