from pathlib import Path
import hashlib


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        while chunk := file.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def media_kind(message) -> str | None:
    if message.photo:
        return "photo"
    if message.video:
        return "video"
    return None
