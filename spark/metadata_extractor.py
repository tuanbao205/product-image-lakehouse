from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from PIL import Image


def extract_metadata(image_path: str | Path) -> dict[str, Any]:
    """Read image properties; upload_time is extraction time, not object-store time."""
    path = Path(image_path)
    stat = path.stat()

    with Image.open(path) as image:
        width, height = image.size
        image_format = image.format

    if image_format is None:
        raise ValueError(f"Cannot determine image format for {path}")

    return {
        "image_name": path.name,
        "image_path": str(path.resolve()),
        "image_size": stat.st_size,
        "format": image_format.upper(),
        "width": width,
        "height": height,
        "upload_time": datetime.now(timezone.utc).isoformat(),
    }
