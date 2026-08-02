from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import json
import aiosqlite


SCHEMA = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS settings (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    api_id INTEGER,
    api_hash_encrypted TEXT,
    phone_number TEXT,
    session_encrypted TEXT,
    source_chat_ids TEXT NOT NULL DEFAULT '[]',
    database_chat_id INTEGER,
    destination_chat_ids TEXT NOT NULL DEFAULT '[]',
    delete_duplicates INTEGER NOT NULL DEFAULT 0,
    copy_to_database INTEGER NOT NULL DEFAULT 1,
    queue_for_publishing INTEGER NOT NULL DEFAULT 1,
    publish_interval_minutes INTEGER NOT NULL DEFAULT 60,
    publish_batch_size INTEGER NOT NULL DEFAULT 1,
    service_enabled INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS media (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sha256 TEXT NOT NULL UNIQUE,
    media_kind TEXT NOT NULL,
    size_bytes INTEGER NOT NULL,
    source_chat_id INTEGER NOT NULL,
    source_message_id INTEGER NOT NULL,
    database_chat_id INTEGER,
    database_message_id INTEGER,
    caption TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS publish_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    media_id INTEGER NOT NULL,
    destination_chat_id INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at TEXT NOT NULL,
    published_at TEXT,
    UNIQUE(media_id, destination_chat_id)
);
"""


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


class Database:
    def __init__(self, path: Path):
        self.path = path

    async def initialize(self):
        async with aiosqlite.connect(self.path) as db:
            await db.executescript(SCHEMA)
            await db.execute(
                "INSERT OR IGNORE INTO settings (id, updated_at) VALUES (1, ?)",
                (now(),),
            )
            await db.commit()

    async def get_settings(self) -> dict:
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute("SELECT * FROM settings WHERE id=1")
            row = dict(await cur.fetchone())
            row["source_chat_ids"] = json.loads(row["source_chat_ids"])
            row["destination_chat_ids"] = json.loads(row["destination_chat_ids"])
            return row

    async def update_settings(self, **values):
        if not values:
            return
        for key in ("source_chat_ids", "destination_chat_ids"):
            if key in values:
                values[key] = json.dumps(values[key])
        values["updated_at"] = now()
        columns = ", ".join(f"{key}=?" for key in values)
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                f"UPDATE settings SET {columns} WHERE id=1",
                tuple(values.values()),
            )
            await db.commit()

    async def find_by_hash(self, sha256: str):
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute("SELECT * FROM media WHERE sha256=?", (sha256,))
            row = await cur.fetchone()
            return dict(row) if row else None

    async def add_media(self, sha256, kind, size, chat_id, message_id, caption):
        async with aiosqlite.connect(self.path) as db:
            cur = await db.execute(
                """INSERT INTO media
                (sha256, media_kind, size_bytes, source_chat_id,
                 source_message_id, caption, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (sha256, kind, size, chat_id, message_id, caption, now()),
            )
            await db.commit()
            return int(cur.lastrowid)

    async def set_database_message(self, media_id, chat_id, message_id):
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                """UPDATE media SET database_chat_id=?, database_message_id=?
                WHERE id=?""",
                (chat_id, message_id, media_id),
            )
            await db.commit()

    async def enqueue(self, media_id, destination_chat_id):
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                """INSERT OR IGNORE INTO publish_queue
                (media_id, destination_chat_id, created_at)
                VALUES (?, ?, ?)""",
                (media_id, destination_chat_id, now()),
            )
            await db.commit()

    async def pending(self, limit):
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(
                """SELECT q.id queue_id, q.destination_chat_id,
                m.source_chat_id, m.source_message_id,
                m.database_chat_id, m.database_message_id, m.caption
                FROM publish_queue q JOIN media m ON m.id=q.media_id
                WHERE q.status='pending' ORDER BY q.id LIMIT ?""",
                (limit,),
            )
            return [dict(x) for x in await cur.fetchall()]

    async def mark_published(self, queue_id):
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                """UPDATE publish_queue SET status='published',
                published_at=?, last_error=NULL WHERE id=?""",
                (now(), queue_id),
            )
            await db.commit()

    async def mark_failed(self, queue_id, error):
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                """UPDATE publish_queue SET attempts=attempts+1,
                last_error=? WHERE id=?""",
                (str(error)[:1000], queue_id),
            )
            await db.commit()

    async def statistics(self):
        async with aiosqlite.connect(self.path) as db:
            media = (await (await db.execute("SELECT COUNT(*) FROM media")).fetchone())[0]
            queued = (await (await db.execute(
                "SELECT COUNT(*) FROM publish_queue WHERE status='pending'"
            )).fetchone())[0]
            published = (await (await db.execute(
                "SELECT COUNT(*) FROM publish_queue WHERE status='published'"
            )).fetchone())[0]
            return {"media": media, "queued": queued, "published": published}
