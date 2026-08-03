from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from pymongo import ASCENDING, AsyncMongoClient, ReturnDocument
from pymongo.errors import DuplicateKeyError


def now() -> datetime:
    return datetime.now(timezone.utc)


DEFAULT_SETTINGS: dict[str, Any] = {
    "_id": "owner",
    "api_id": None,
    "api_hash_encrypted": None,
    "phone_number": None,
    "session_encrypted": None,
    "source_chat_ids": [],
    "database_chat_id": None,
    "destination_chat_ids": [],
    "delete_duplicates": 0,
    "duplicate_alerts": 1,
    "copy_to_database": 1,
    "queue_for_publishing": 1,
    "publish_interval_minutes": 60,
    "publish_batch_size": 3,
    "performance_mode": "balanced",
    "service_enabled": 0,
    "updated_at": None,
}


class Database:
    def __init__(self, mongodb_uri: str, database_name: str):
        self.client = AsyncMongoClient(
            mongodb_uri,
            serverSelectionTimeoutMS=15000,
            connectTimeoutMS=15000,
            retryWrites=True,
        )
        self.database = self.client[database_name]
        self.settings = self.database["settings"]
        self.media = self.database["media"]
        self.publish_queue = self.database["publish_queue"]
        self.counters = self.database["counters"]

    async def initialize(self) -> None:
        await self.client.admin.command("ping")

        await self.settings.update_one(
            {"_id": "owner"},
            {
                "$setOnInsert": {
                    **DEFAULT_SETTINGS,
                    "updated_at": now(),
                }
            },
            upsert=True,
        )

        await self.media.create_index(
            [("sha256", ASCENDING)],
            unique=True,
            name="unique_media_sha256",
        )
        await self.publish_queue.create_index(
            [
                ("media_id", ASCENDING),
                ("destination_chat_id", ASCENDING),
            ],
            unique=True,
            name="unique_media_destination",
        )
        await self.publish_queue.create_index(
            [("status", ASCENDING), ("id", ASCENDING)],
            name="pending_queue_order",
        )

    async def close(self) -> None:
        await self.client.close()

    async def _next_id(self, name: str) -> int:
        document = await self.counters.find_one_and_update(
            {"_id": name},
            {"$inc": {"value": 1}},
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        return int(document["value"])

    async def get_settings(self) -> dict:
        document = await self.settings.find_one({"_id": "owner"})
        if document is None:
            await self.initialize()
            document = await self.settings.find_one({"_id": "owner"})

        result = dict(DEFAULT_SETTINGS)
        result.update(document or {})
        result["source_chat_ids"] = [
            int(value)
            for value in result.get("source_chat_ids", [])
        ]
        result["destination_chat_ids"] = [
            int(value)
            for value in result.get("destination_chat_ids", [])
        ]
        return result

    async def update_settings(self, **values) -> None:
        if not values:
            return

        values["updated_at"] = now()
        await self.settings.update_one(
            {"_id": "owner"},
            {"$set": values},
            upsert=True,
        )

    async def find_by_hash(self, sha256: str) -> dict | None:
        document = await self.media.find_one({"sha256": sha256})
        return dict(document) if document else None


async def register_media(
    self,
    sha256: str,
    kind: str,
    size: int,
    chat_id: int,
    message_id: int,
    caption: str | None,
) -> tuple[bool, dict]:
    media_id = await self._next_id("media")
    document = {
        "_id": media_id,
        "id": media_id,
        "sha256": sha256,
        "media_kind": kind,
        "size_bytes": int(size),
        "source_chat_id": int(chat_id),
        "source_message_id": int(message_id),
        "database_chat_id": int(chat_id),
        "database_message_id": int(message_id),
        "caption": caption,
        "created_at": now(),
    }

    try:
        await self.media.insert_one(document)
        return True, document
    except DuplicateKeyError:
        existing = await self.media.find_one({"sha256": sha256})
        if existing is None:
            raise
        return False, dict(existing)

    async def add_media(
        self,
        sha256: str,
        kind: str,
        size: int,
        chat_id: int,
        message_id: int,
        caption: str | None,
    ) -> int:
        media_id = await self._next_id("media")
        await self.media.insert_one(
            {
                "_id": media_id,
                "id": media_id,
                "sha256": sha256,
                "media_kind": kind,
                "size_bytes": int(size),
                "source_chat_id": int(chat_id),
                "source_message_id": int(message_id),
                "database_chat_id": None,
                "database_message_id": None,
                "caption": caption,
                "created_at": now(),
            }
        )
        return media_id

    async def set_database_message(
        self,
        media_id: int,
        chat_id: int,
        message_id: int,
    ) -> None:
        await self.media.update_one(
            {"_id": int(media_id)},
            {
                "$set": {
                    "database_chat_id": int(chat_id),
                    "database_message_id": int(message_id),
                }
            },
        )

    async def enqueue(
        self,
        media_id: int,
        destination_chat_id: int,
    ) -> None:
        existing = await self.publish_queue.find_one(
            {
                "media_id": int(media_id),
                "destination_chat_id": int(destination_chat_id),
            },
            {"_id": 1},
        )
        if existing:
            return

        queue_id = await self._next_id("publish_queue")
        try:
            await self.publish_queue.insert_one(
                {
                    "_id": queue_id,
                    "id": queue_id,
                    "media_id": int(media_id),
                    "destination_chat_id": int(destination_chat_id),
                    "status": "pending",
                    "attempts": 0,
                    "last_error": None,
                    "created_at": now(),
                    "published_at": None,
                }
            )
        except Exception:
            # A simultaneous duplicate insert can safely be ignored.
            existing = await self.publish_queue.find_one(
                {
                    "media_id": int(media_id),
                    "destination_chat_id": int(destination_chat_id),
                },
                {"_id": 1},
            )
            if not existing:
                raise

    async def pending(self, limit: int) -> list[dict]:
        cursor = (
            self.publish_queue.find({"status": "pending"})
            .sort("id", ASCENDING)
            .limit(int(limit))
        )

        rows: list[dict] = []
        async for queue in cursor:
            media = await self.media.find_one(
                {"_id": int(queue["media_id"])}
            )
            if not media:
                await self.publish_queue.update_one(
                    {"_id": queue["_id"]},
                    {
                        "$set": {
                            "status": "failed",
                            "last_error": "Media record not found",
                        }
                    },
                )
                continue

            rows.append(
                {
                    "queue_id": int(queue["id"]),
                    "destination_chat_id": int(
                        queue["destination_chat_id"]
                    ),
                    "source_chat_id": int(
                        media["source_chat_id"]
                    ),
                    "source_message_id": int(
                        media["source_message_id"]
                    ),
                    "database_chat_id": media.get(
                        "database_chat_id"
                    ),
                    "database_message_id": media.get(
                        "database_message_id"
                    ),
                    "caption": media.get("caption"),
                }
            )
        return rows

    async def mark_published(self, queue_id: int) -> None:
        await self.publish_queue.update_one(
            {"_id": int(queue_id)},
            {
                "$set": {
                    "status": "published",
                    "published_at": now(),
                    "last_error": None,
                }
            },
        )

    async def mark_failed(
        self,
        queue_id: int,
        error: Exception | str,
    ) -> None:
        await self.publish_queue.update_one(
            {"_id": int(queue_id)},
            {
                "$inc": {"attempts": 1},
                "$set": {"last_error": str(error)[:1000]},
            },
        )

    async def statistics(self) -> dict:
        return {
            "media": await self.media.count_documents({}),
            "queued": await self.publish_queue.count_documents(
                {"status": "pending"}
            ),
            "published": await self.publish_queue.count_documents(
                {"status": "published"}
            ),
        }
