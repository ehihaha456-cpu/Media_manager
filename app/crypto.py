from __future__ import annotations

from pathlib import Path
from cryptography.fernet import Fernet


def load_fernet(configured_key: str | None) -> Fernet:
    if configured_key:
        return Fernet(configured_key.encode())

    key_path = Path("data/master.key")
    key_path.parent.mkdir(parents=True, exist_ok=True)
    if key_path.exists():
        key = key_path.read_bytes().strip()
    else:
        key = Fernet.generate_key()
        key_path.write_bytes(key)
    return Fernet(key)


def encrypt_text(fernet: Fernet, value: str | None) -> str | None:
    if not value:
        return None
    return fernet.encrypt(value.encode()).decode()


def decrypt_text(fernet: Fernet, value: str | None) -> str | None:
    if not value:
        return None
    return fernet.decrypt(value.encode()).decode()
