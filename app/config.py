from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os

from dotenv import load_dotenv

load_dotenv()


def required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing environment variable: {name}")
    return value


@dataclass(frozen=True, slots=True)
class AppConfig:
    bot_token: str
    owner_id: int
    master_key: str | None
    sqlite_path: Path
    temp_dir: Path
    log_level: str


def load_app_config() -> AppConfig:
    config = AppConfig(
        bot_token=required("BOT_TOKEN"),
        owner_id=int(required("OWNER_ID")),
        master_key=os.getenv("MASTER_KEY", "").strip() or None,
        sqlite_path=Path(os.getenv("SQLITE_PATH", "data/media_manager.sqlite3")),
        temp_dir=Path(os.getenv("TEMP_DIR", "data/tmp")),
        log_level=os.getenv("LOG_LEVEL", "INFO").upper().strip(),
    )
    config.sqlite_path.parent.mkdir(parents=True, exist_ok=True)
    config.temp_dir.mkdir(parents=True, exist_ok=True)
    return config
