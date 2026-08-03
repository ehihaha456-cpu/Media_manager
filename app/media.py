from __future__ import annotations

import hashlib
from pathlib import Path


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

    if getattr(message, "audio", None) or getattr(message, "voice", None):
        return "audio"

    document = getattr(message, "document", None)
    if document is None:
        return None

    mime_type = (getattr(document, "mime_type", "") or "").lower()

    if mime_type.startswith("video/"):
        return "video"

    if mime_type.startswith("image/"):
        return "photo"

    if mime_type.startswith("audio/"):
        return "audio"

    return "file"
