from __future__ import annotations

import logging
from datetime import datetime, timezone, timedelta
from typing import Any

from pymongo import ASCENDING, AsyncMongoClient, ReturnDocument
from pymongo.errors import DuplicateKeyError

log = logging.getLogger(__name__)


def now() -> datetime:
    return datetime.now(timezone.utc)


DEFAULT_SETTINGS: dict[str, Any] = {
    "_id": "owner",
    "api_id": None,
    "api_hash_encrypted": None,
    "phone_number": None,
    "session_encrypted": None,
    "source_chat_ids": [],
    "disabled_source_chat_ids": [],
    "database_chat_id": None,
    "database_chat_enabled": 1,
    "destination_chat_ids": [],
    "disabled_destination_chat_ids": [],
    "delete_duplicates": 0,
    "duplicate_alerts": 1,
    "copy_to_database": 1,
    "queue_for_publishing": 1,
    "publish_interval_minutes": 60,
    "publish_batch_size": 1,
    "service_enabled": 0,
    "updated_at": None,
}


class Database:
    def __init__(self, mongodb_uri: str, database_name: str) -> None:
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
        self.chat_offsets = self.database["chat_offsets"]
        self.activity_stats = self.database["activity_stats"]
        self.source_history_scans = self.database["source_history_scans"]
        self.runtime_locks = self.database["runtime_locks"]

    async def initialize(self) -> None:
        await self.client.admin.command("ping")
        await self.settings.update_one(
            {"_id": "owner"},
            {"$setOnInsert": {**DEFAULT_SETTINGS, "updated_at": now()}},
            upsert=True,
        )
        await self.media.create_index(
            [("sha256", ASCENDING)],
            unique=True,
            name="unique_media_sha256",
        )
        await self.publish_queue.create_index(
            [("media_id", ASCENDING), ("destination_chat_id", ASCENDING)],
            unique=True,
            name="unique_media_destination",
        )
        await self.publish_queue.create_index(
            [("status", ASCENDING), ("id", ASCENDING)],
            name="pending_queue_order",
        )

    async def close(self) -> None:
        await self.client.close()

    async def acquire_runtime_lock(
        self,
        lock_id: str,
        owner_id: str,
        lease_seconds: int = 45,
    ) -> bool:
        current = now()
        expires_at = current + timedelta(seconds=lease_seconds)

        # First, atomically renew our own lease or take over an expired one.
        document = await self.runtime_locks.find_one_and_update(
            {
                "_id": lock_id,
                "$or": [
                    {"owner_id": owner_id},
                    {"expires_at": {"$lte": current}},
                    {"expires_at": {"$exists": False}},
                ],
            },
            {
                "$set": {
                    "owner_id": owner_id,
                    "expires_at": expires_at,
                    "updated_at": current,
                }
            },
            upsert=False,
            return_document=ReturnDocument.AFTER,
        )
        if document and document.get("owner_id") == owner_id:
            return True

        # If the lock does not exist, create it. Two deployments may race
        # here; only one insert can win because _id is unique.
        try:
            await self.runtime_locks.insert_one(
                {
                    "_id": lock_id,
                    "owner_id": owner_id,
                    "expires_at": expires_at,
                    "created_at": current,
                    "updated_at": current,
                }
            )
            return True
        except DuplicateKeyError:
            return False

    async def renew_runtime_lock(
        self,
        lock_id: str,
        owner_id: str,
        lease_seconds: int = 45,
    ) -> bool:
        current = now()
        result = await self.runtime_locks.update_one(
            {"_id": lock_id, "owner_id": owner_id},
            {
                "$set": {
                    "expires_at": current + timedelta(seconds=lease_seconds),
                    "updated_at": current,
                }
            },
        )
        return result.modified_count == 1

    async def release_runtime_lock(
        self,
        lock_id: str,
        owner_id: str,
    ) -> None:
        await self.runtime_locks.delete_one(
            {"_id": lock_id, "owner_id": owner_id}
        )

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
        result = dict(DEFAULT_SETTINGS)
        result.update(document or {})
        result["source_chat_ids"] = [
            int(value) for value in result.get("source_chat_ids", [])
        ]
        result["destination_chat_ids"] = [
            int(value)
            for value in result.get("destination_chat_ids", [])
        ]
        result["disabled_source_chat_ids"] = [
            int(value)
            for value in result.get("disabled_source_chat_ids", [])
        ]
        result["disabled_destination_chat_ids"] = [
            int(value)
            for value in result.get("disabled_destination_chat_ids", [])
        ]

        raw_database_enabled = result.get(
            "database_chat_enabled",
            1,
        )
        if isinstance(raw_database_enabled, str):
            normalized = raw_database_enabled.strip().lower()
            result["database_chat_enabled"] = (
                0
                if normalized in {
                    "",
                    "0",
                    "false",
                    "off",
                    "no",
                    "disabled",
                }
                else 1
            )
        else:
            result["database_chat_enabled"] = (
                1 if bool(raw_database_enabled) else 0
            )

        disabled_sources = set(result["disabled_source_chat_ids"])
        disabled_destinations = set(
            result["disabled_destination_chat_ids"]
        )
        result["active_source_chat_ids"] = [
            chat_id for chat_id in result["source_chat_ids"]
            if chat_id not in disabled_sources
        ]
        result["active_destination_chat_ids"] = [
            chat_id for chat_id in result["destination_chat_ids"]
            if chat_id not in disabled_destinations
        ]
        result["database_chat_active"] = bool(
            result.get("database_chat_id")
            and result.get("database_chat_enabled", 1)
        )
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

    async def get_chat_offset(self, chat_id: int) -> int | None:
        document = await self.chat_offsets.find_one(
            {"_id": str(int(chat_id))}
        )
        if document is None:
            return None
        return int(document.get("last_message_id", 0))

    async def set_chat_offset(
        self,
        chat_id: int,
        message_id: int,
    ) -> None:
        await self.chat_offsets.update_one(
            {"_id": str(int(chat_id))},
            {
                "$set": {
                    "chat_id": int(chat_id),
                    "last_message_id": int(message_id),
                    "updated_at": now(),
                }
            },
            upsert=True,
        )


    async def reset_chat_offset(
        self,
        chat_id: int,
    ) -> None:
        await self.chat_offsets.delete_one(
            {"_id": str(int(chat_id))}
        )


    async def get_source_history_scan(
        self,
        chat_id: int,
    ) -> dict | None:
        document = await self.source_history_scans.find_one(
            {"_id": str(int(chat_id))}
        )
        return dict(document) if document else None

    async def upsert_source_history_scan(
        self,
        chat_id: int,
        **values,
    ) -> None:
        values = dict(values)
        values["chat_id"] = int(chat_id)
        values["updated_at"] = now()

        defaults = {
            "created_at": now(),
            "cursor_message_id": 0,
            "processed": 0,
            "uploaded": 0,
            "duplicates": 0,
            "failed": 0,
            "total_media": 0,
            "videos": 0,
            "photos": 0,
            "audio": 0,
            "files": 0,
        }
        # MongoDB rejects the same path in $set and $setOnInsert.
        set_on_insert = {
            key: value
            for key, value in defaults.items()
            if key not in values
        }
        update = {"$set": values}
        if set_on_insert:
            update["$setOnInsert"] = set_on_insert

        await self.source_history_scans.update_one(
            {"_id": str(int(chat_id))},
            update,
            upsert=True,
        )

    async def pending_source_history_scans(self) -> list[dict]:
        cursor = self.source_history_scans.find(
            {
                "status": {
                    "$in": [
                        "pending_count",
                        "counting",
                        "pending",
                        "scanning",
                    ]
                }
            }
        )
        return [dict(document) async for document in cursor]

    async def delete_source_history_scan(
        self,
        chat_id: int,
    ) -> None:
        await self.source_history_scans.delete_one(
            {"_id": str(int(chat_id))}
        )

    async def find_by_hash(self, sha256: str) -> dict | None:
        document = await self.media.find_one({"sha256": sha256})
        return dict(document) if document else None

    async def find_by_database_message(
        self,
        database_chat_id: int,
        database_message_id: int,
    ) -> dict | None:
        document = await self.media.find_one(
            {
                "database_chat_id": int(database_chat_id),
                "database_message_id": int(database_message_id),
            }
        )
        return dict(document) if document else None

    async def delete_media_record(self, media_id: int) -> None:
        await self.media.delete_one({"_id": int(media_id)})

    async def register_database_media(
        self,
        *,
        sha256: str,
        kind: str,
        size: int,
        database_chat_id: int,
        database_message_id: int,
        caption: str | None,
        source_chat_id: int | None = None,
        source_message_id: int | None = None,
    ) -> tuple[bool, dict]:
        media_id = await self._next_id("media")
        document = {
            "_id": media_id,
            "id": media_id,
            "sha256": sha256,
            "media_kind": kind,
            "size_bytes": int(size),
            "source_chat_id": int(
                source_chat_id
                if source_chat_id is not None
                else database_chat_id
            ),
            "source_message_id": int(
                source_message_id
                if source_message_id is not None
                else database_message_id
            ),
            "database_chat_id": int(database_chat_id),
            "database_message_id": int(database_message_id),
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
        except DuplicateKeyError:
            return

    async def pending(self, limit: int) -> list[dict]:
        rows: list[dict] = []
        cursor = (
            self.publish_queue.find({"status": "pending"})
            .sort("id", ASCENDING)
            .limit(int(limit))
        )
        async for queue in cursor:
            queue_id = int(queue["_id"])
            media = await self.media.find_one(
                {"_id": int(queue["media_id"])}
            )
            if not media:
                await self.publish_queue.update_one(
                    {"_id": queue_id},
                    {"$set": {
                        "status": "failed",
                        "last_error": "Linked media record was not found",
                        "failed_at": now(),
                    }, "$inc": {"attempts": 1}},
                )
                continue

            source_chat_id = media.get("source_chat_id")
            source_message_id = media.get("source_message_id")
            database_chat_id = media.get("database_chat_id")
            database_message_id = media.get("database_message_id")

            has_source = source_chat_id is not None and source_message_id is not None
            has_database = database_chat_id is not None and database_message_id is not None
            if not has_source and not has_database:
                await self.publish_queue.update_one(
                    {"_id": queue_id},
                    {"$set": {
                        "status": "failed",
                        "last_error": "Media has no Source or Database reference",
                        "failed_at": now(),
                    }, "$inc": {"attempts": 1}},
                )
                continue

            rows.append({
                "queue_id": int(queue.get("id", queue_id)),
                "destination_chat_id": int(queue["destination_chat_id"]),
                "source_chat_id": int(source_chat_id) if has_source else None,
                "source_message_id": int(source_message_id) if has_source else None,
                "database_chat_id": int(database_chat_id) if has_database else None,
                "database_message_id": int(database_message_id) if has_database else None,
                "caption": media.get("caption"),
            })
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
        queue = await self.publish_queue.find_one(
            {"_id": int(queue_id)}, {"attempts": 1}
        ) or {}
        attempts = int(queue.get("attempts", 0)) + 1
        status = "failed" if attempts >= 5 else "pending"
        await self.publish_queue.update_one(
            {"_id": int(queue_id)},
            {
                "$set": {
                    "status": status,
                    "last_error": str(error)[:1000],
                    "failed_at": now() if status == "failed" else None,
                },
                "$inc": {"attempts": 1},
            },
        )

    async def cancel_destination_queue(
        self,
        destination_chat_id: int,
    ) -> None:
        await self.publish_queue.update_many(
            {
                "destination_chat_id": int(destination_chat_id),
                "status": "pending",
            },
            {
                "$set": {
                    "status": "cancelled",
                    "last_error": "Destination disconnected",
                    "failed_at": now(),
                }
            },
        )

    async def increment_activity(
        self,
        *,
        processed: int = 0,
        uploaded: int = 0,
        duplicates: int = 0,
        failed: int = 0,
    ) -> None:
        increments = {
            "processed": int(processed),
            "uploaded": int(uploaded),
            "duplicates": int(duplicates),
            "failed": int(failed),
        }
        increments = {
            key: value
            for key, value in increments.items()
            if value
        }
        if not increments:
            return
    
        india_now = datetime.now(
            timezone(timedelta(hours=5, minutes=30))
        )
        day_key = india_now.strftime("%Y-%m-%d")
    
        await self.activity_stats.update_one(
            {"_id": "total"},
            {
                "$inc": increments,
                "$set": {"updated_at": now()},
            },
            upsert=True,
        )
        await self.activity_stats.update_one(
            {"_id": f"day:{day_key}"},
            {
                "$inc": increments,
                "$set": {
                    "date": day_key,
                    "updated_at": now(),
                },
            },
            upsert=True,
        )
    
    async def dashboard_statistics(self) -> dict:
        india_now = datetime.now(
            timezone(timedelta(hours=5, minutes=30))
        )
        day_key = india_now.strftime("%Y-%m-%d")
    
        total = (
            await self.activity_stats.find_one({"_id": "total"})
            or {}
        )
        today = (
            await self.activity_stats.find_one(
                {"_id": f"day:{day_key}"}
            )
            or {}
        )
    
        destination_queue = (
            await self.publish_queue.count_documents(
                {"status": "pending"}
            )
        )
    
        def values(document: dict) -> dict:
            return {
                "processed": int(document.get("processed", 0)),
                "uploaded": int(document.get("uploaded", 0)),
                "duplicates": int(document.get("duplicates", 0)),
                "failed": int(document.get("failed", 0)),
            }
    
        return {
            "total": values(total),
            "today": values(today),
            # Source -> Database is processed immediately, so there is no
            # separate persistent Database queue in this engine.
            "database_queue": 0,
            "destination_queue": int(destination_queue),
        }
    
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
