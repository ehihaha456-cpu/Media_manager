from __future__ import annotations

import hashlib
from pathlib import Path


VIDEO_MIME_PREFIXES = ("video/",)
PHOTO_MIME_PREFIXES = ("image/",)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        while chunk := file.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def media_kind(message) -> str | None:
    if getattr(message, "photo", None):
        return "photo"

    if getattr(message, "video", None):
        return "video"

    document = getattr(message, "document", None)
    mime_type = (getattr(document, "mime_type", "") or "").lower()

    if mime_type.startswith(VIDEO_MIME_PREFIXES):
        return "video"

    if mime_type.startswith(PHOTO_MIME_PREFIXES):
        return "photo"

    return None
