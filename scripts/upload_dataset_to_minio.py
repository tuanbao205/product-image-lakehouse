import argparse
import csv
import mimetypes
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from PIL import Image, UnidentifiedImageError
from botocore.exceptions import BotoCoreError

try:
    from minio_config import MINIO_BUCKET, create_s3_client, ensure_bucket
except ImportError:
    from scripts.minio_config import MINIO_BUCKET, create_s3_client, ensure_bucket


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_IMAGE_DIR = PROJECT_ROOT / "data" / "raw" / "images"
DEFAULT_CATALOG = PROJECT_ROOT / "data" / "styles.csv"
DEFAULT_METADATA_OUTPUT = PROJECT_ROOT / "products.csv"
SUPPORTED_FORMATS = {"JPEG", "PNG", "WEBP"}
UPLOAD_WORKERS = 8
MANIFEST_FIELDS = [
    "image_id",
    "product_id",
    "product_name",
    "category",
    "object_path",
    "image_size",
    "format",
    "width",
    "height",
    "upload_time",
]


def load_product_catalog(catalog_path: Path) -> dict[str, dict[str, str]]:
    if not catalog_path.is_file():
        raise FileNotFoundError(f"Product catalog not found: {catalog_path}")

    with catalog_path.open("r", encoding="utf-8-sig", newline="") as source:
        reader = csv.DictReader(source)
        required_fields = {"id", "productDisplayName"}
        missing_fields = required_fields.difference(reader.fieldnames or [])
        if missing_fields:
            raise ValueError(
                "Product catalog is missing required column(s): "
                + ", ".join(sorted(missing_fields))
            )

        catalog: dict[str, dict[str, str]] = {}
        for row in reader:
            product_id = (row.get("id") or "").strip()
            if not product_id:
                continue
            if product_id in catalog:
                raise ValueError(
                    f"Product catalog contains duplicate product ID: {product_id}"
                )
            catalog[product_id] = row

    if not catalog:
        raise ValueError(f"Product catalog has no valid product IDs: {catalog_path}")
    return catalog


def discover_images(image_dir: Path) -> list[Path]:
    if not image_dir.is_dir():
        raise FileNotFoundError(f"Image directory not found: {image_dir}")

    supported_extensions = {".jpg", ".jpeg", ".png", ".webp"}
    image_paths = sorted(
        path
        for path in image_dir.rglob("*")
        if path.is_file() and path.suffix.lower() in supported_extensions
    )
    if not image_paths:
        raise ValueError(f"No supported image files found under: {image_dir}")
    return image_paths


def inspect_image(
    image_path: Path,
    image_dir: Path,
    catalog: dict[str, dict[str, str]],
) -> dict[str, Any]:
    product_id = image_path.stem.strip()
    product = catalog.get(product_id)
    if product is None:
        raise ValueError(f"Image filename ID is absent from catalog: {product_id}")

    with Image.open(image_path) as image:
        image_format = image.format
        width, height = image.size
        image.verify()

    if image_format not in SUPPORTED_FORMATS:
        raise ValueError(f"Unsupported image format: {image_format}")

    category = (product.get("masterCategory") or "").strip() or (
        product.get("subCategory") or ""
    ).strip()
    relative_path = image_path.relative_to(image_dir).as_posix()
    object_key = f"raw/{relative_path}"

    return {
        "image_id": product_id,
        "product_id": product_id,
        "product_name": (product.get("productDisplayName") or "").strip(),
        "category": category,
        "object_path": object_key,
        "image_size": image_path.stat().st_size,
        "format": "JPG" if image_format == "JPEG" else image_format,
        "width": width,
        "height": height,
    }


def write_manifest(records: list[dict[str, Any]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = output_path.with_name(f"{output_path.name}.tmp")
    try:
        with temporary_path.open("w", encoding="utf-8", newline="") as destination:
            writer = csv.DictWriter(
                destination,
                fieldnames=MANIFEST_FIELDS,
                lineterminator="\n",
            )
            writer.writeheader()
            writer.writerows(records)
        temporary_path.replace(output_path)
    finally:
        temporary_path.unlink(missing_ok=True)


def _upload_record(
    client: Any,
    record: dict[str, Any],
) -> tuple[dict[str, Any] | None, str | None]:
    local_path: Path = record["_local_path"]
    content_type = mimetypes.guess_type(local_path.name)[0]
    extra_args = {"ContentType": content_type} if content_type else {}
    try:
        client.upload_file(
            str(local_path),
            MINIO_BUCKET,
            record["object_path"],
            ExtraArgs=extra_args,
        )
    except (BotoCoreError, OSError) as error:
        return None, f"{local_path.name}: upload failed: {error}"

    uploaded_record = {
        field: value for field, value in record.items() if field != "_local_path"
    }
    uploaded_record["upload_time"] = datetime.now(timezone.utc).isoformat()
    return uploaded_record, None


def upload_dataset(
    image_dir: Path,
    catalog_path: Path,
    metadata_output: Path,
    dry_run: bool = False,
    client: Any | None = None,
) -> dict[str, int]:
    catalog = load_product_catalog(catalog_path)
    image_paths = discover_images(image_dir)
    records: list[dict[str, Any]] = []
    failures: list[str] = []
    seen_ids: set[str] = set()

    for image_path in image_paths:
        try:
            record = inspect_image(image_path, image_dir, catalog)
            if record["product_id"] in seen_ids:
                raise ValueError(
                    f"Multiple image files use product ID {record['product_id']}"
                )
            seen_ids.add(record["product_id"])
            record["_local_path"] = image_path
            records.append(record)
        except (OSError, UnidentifiedImageError, ValueError) as error:
            failures.append(f"{image_path.name}: {error}")

    if not records:
        raise ValueError("No valid catalog-matched images are available to upload.")

    if dry_run:
        print(
            f"Dry run: {len(records)} valid images, "
            f"{len(failures)} skipped; no MinIO changes made."
        )
        return {"images_found": len(image_paths), "uploaded": 0, "failed": len(failures)}

    s3_client = client or create_s3_client()
    ensure_bucket(s3_client, MINIO_BUCKET)
    uploaded_records: list[dict[str, Any]] = []

    with ThreadPoolExecutor(max_workers=min(UPLOAD_WORKERS, len(records))) as executor:
        futures = [
            executor.submit(_upload_record, s3_client, record) for record in records
        ]
        for completed, future in enumerate(as_completed(futures), start=1):
            uploaded_record, failure = future.result()
            if uploaded_record is not None:
                uploaded_records.append(uploaded_record)
            if failure is not None:
                failures.append(failure)
            if completed % 500 == 0 or completed == len(futures):
                print(
                    f"Processed {completed}/{len(futures)} images; "
                    f"uploaded {len(uploaded_records)}.",
                    flush=True,
                )

    if uploaded_records:
        write_manifest(uploaded_records, metadata_output)

    print(
        f"Images found: {len(image_paths)}; uploaded: {len(uploaded_records)}; "
        f"failed or skipped: {len(failures)}."
    )
    if uploaded_records:
        print(f"Metadata manifest written to: {metadata_output}")
    for failure in failures[:20]:
        print(f"  - {failure}")
    if len(failures) > 20:
        print(f"  ... and {len(failures) - 20} more failure(s).")

    return {
        "images_found": len(image_paths),
        "uploaded": len(uploaded_records),
        "failed": len(failures),
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Upload real catalog-matched images to MinIO and write their metadata."
    )
    parser.add_argument("--image-dir", type=Path, default=DEFAULT_IMAGE_DIR)
    parser.add_argument("--catalog", type=Path, default=DEFAULT_CATALOG)
    parser.add_argument("--metadata-output", type=Path, default=DEFAULT_METADATA_OUTPUT)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate image/catalog matches without connecting to MinIO or writing a manifest.",
    )
    args = parser.parse_args()

    result = upload_dataset(
        args.image_dir,
        args.catalog,
        args.metadata_output,
        dry_run=args.dry_run,
    )
    if not args.dry_run and result["failed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
