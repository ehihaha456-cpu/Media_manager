from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from uuid import uuid4

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from telethon import TelegramClient, events
from telethon.sessions import StringSession

from .crypto import decrypt_text
from .media import media_kind, sha256_file

log = logging.getLogger(__name__)


class MediaRuntime:
    def __init__(self, db, fernet, temp_dir):
        self.db = db
        self.fernet = fernet
        self.temp_dir = temp_dir
        self.client = None
        self.scheduler = None
        self._process_lock = asyncio.Lock()
        self._started = False

    async def start(self):
        settings = await self.db.get_settings()

        if not settings["service_enabled"]:
            return

        if self._started and self.client and self.client.is_connected():
            return

        api_hash = decrypt_text(
            self.fernet,
            settings["api_hash_encrypted"],
        )
        session = decrypt_text(
            self.fernet,
            settings["session_encrypted"],
        )

        if not settings["api_id"] or not api_hash or not session:
            raise RuntimeError("Telegram account is not connected")

        watched_ids = self._watched_chat_ids(settings)
        if not watched_ids:
            raise RuntimeError(
                "Select at least one Source or Database chat"
            )

        self.client = TelegramClient(
            StringSession(session),
            int(settings["api_id"]),
            api_hash,
        )
        await self.client.connect()

        if not await self.client.is_user_authorized():
            raise RuntimeError(
                "Saved Telegram session is no longer authorized"
            )

        # Listen globally and check the latest database settings dynamically.
        # This prevents stale source/database filters after a setting change.
        @self.client.on(events.NewMessage())
        async def on_message(event):
            try:
                current = await self.db.get_settings()
                current_watched = self._watched_chat_ids(current)
                if int(event.chat_id) not in current_watched:
                    return
                await self.process_message(event.message, int(event.chat_id))
            except Exception:
                log.exception(
                    "New-message handler failed: chat=%s message=%s",
                    getattr(event, "chat_id", None),
                    getattr(event, "id", None),
                )

        self.scheduler = AsyncIOScheduler(timezone="Asia/Kolkata")
        self.scheduler.add_job(
            self.publish_pending,
            "interval",
            minutes=max(
                1,
                int(settings["publish_interval_minutes"]),
            ),
            max_instances=1,
            coalesce=True,
            id="publish_pending",
            replace_existing=True,
        )
        self.scheduler.start()
        self._started = True

        log.info(
            "Media runtime started | watched=%s destinations=%s",
            sorted(watched_ids),
            settings["destination_chat_ids"],
        )

        # Index recent messages so testing does not depend only on messages
        # posted after the exact service-start moment.
        asyncio.create_task(self.scan_recent_messages(limit_per_chat=100))

    def _watched_chat_ids(self, settings) -> set[int]:
        watched = {
            int(chat_id)
            for chat_id in settings.get("source_chat_ids", [])
        }
        database_chat_id = settings.get("database_chat_id")
        if database_chat_id:
            watched.add(int(database_chat_id))
        return watched

    async def stop(self):
        self._started = False

        if self.scheduler:
            self.scheduler.shutdown(wait=False)
            self.scheduler = None

        if self.client:
            await self.client.disconnect()
            self.client = None

        log.info("Media runtime stopped")

    async def restart(self):
        await self.stop()
        settings = await self.db.get_settings()
        if settings["service_enabled"]:
            await self.start()

    async def scan_recent_messages(self, limit_per_chat: int = 100):
        if not self.client or not self.client.is_connected():
            return

        settings = await self.db.get_settings()
        watched_ids = self._watched_chat_ids(settings)

        for chat_id in watched_ids:
            try:
                entity = await self.client.get_entity(chat_id)
                messages = await self.client.get_messages(
                    entity,
                    limit=limit_per_chat,
                )

                # Oldest to newest gives predictable duplicate behavior.
                for message in reversed(messages):
                    if media_kind(message):
                        await self.process_message(
                            message,
                            chat_id,
                            historical=True,
                        )

                log.info(
                    "Recent scan completed: chat=%s checked=%s",
                    chat_id,
                    len(messages),
                )
            except Exception:
                log.exception(
                    "Recent scan failed for chat=%s",
                    chat_id,
                )

    async def process_message(
        self,
        message,
        chat_id: int,
        historical: bool = False,
    ):
        if not self.client:
            return

        kind = media_kind(message)
        if not kind:
            return

        settings = await self.db.get_settings()
        current_chat_id = int(chat_id)
        current_message_id = int(message.id)

        if getattr(message, "noforwards", False):
            log.warning(
                "Protected media skipped: chat=%s message=%s",
                current_chat_id,
                current_message_id,
            )
            return

        database_chat_id = (
            int(settings["database_chat_id"])
            if settings.get("database_chat_id")
            else None
        )

        temp = self.temp_dir / f"media_{uuid4().hex}"

        async with self._process_lock:
            try:
                downloaded = await message.download_media(file=str(temp))
                if not downloaded:
                    log.warning(
                        "Media download returned no file: chat=%s message=%s",
                        current_chat_id,
                        current_message_id,
                    )
                    return

                path = Path(downloaded)
                digest = await asyncio.to_thread(sha256_file, path)
                existing = await self.db.find_by_hash(digest)

                if existing:
                    same_original = (
                        int(existing["source_chat_id"]) == current_chat_id
                        and int(existing["source_message_id"])
                        == current_message_id
                    )

                    if same_original:
                        return

                    log.info(
                        "Duplicate detected: chat=%s message=%s "
                        "original=%s/%s",
                        current_chat_id,
                        current_message_id,
                        existing["source_chat_id"],
                        existing["source_message_id"],
                    )

                    if settings["delete_duplicates"]:
                        try:
                            await self.client.delete_messages(
                                current_chat_id,
                                [current_message_id],
                            )
                            log.info(
                                "Duplicate deleted: chat=%s message=%s",
                                current_chat_id,
                                current_message_id,
                            )
                        except Exception:
                            log.exception(
                                "Duplicate could not be deleted. "
                                "Connected account needs delete permission."
                            )
                    return

                media_id = await self.db.add_media(
                    digest,
                    kind,
                    path.stat().st_size,
                    current_chat_id,
                    current_message_id,
                    message.message or None,
                )

                is_database_message = (
                    database_chat_id is not None
                    and current_chat_id == database_chat_id
                )

                if (
                    settings["copy_to_database"]
                    and database_chat_id is not None
                    and not is_database_message
                ):
                    sent = await self.client.send_file(
                        database_chat_id,
                        path,
                        caption=message.message or None,
                        supports_streaming=(kind == "video"),
                    )

                    await self.db.set_database_message(
                        media_id,
                        database_chat_id,
                        int(sent.id),
                    )

                    log.info(
                        "Copied source media to database: "
                        "source=%s/%s database=%s/%s",
                        current_chat_id,
                        current_message_id,
                        database_chat_id,
                        sent.id,
                    )

                if settings["queue_for_publishing"]:
                    queued = 0
                    for destination in settings["destination_chat_ids"]:
                        destination_id = int(destination)

                        if destination_id == current_chat_id:
                            continue

                        await self.db.enqueue(
                            media_id,
                            destination_id,
                        )
                        queued += 1

                    log.info(
                        "Media queued: media_id=%s destinations=%s",
                        media_id,
                        queued,
                    )

                log.info(
                    "Unique media indexed: chat=%s message=%s "
                    "kind=%s historical=%s",
                    current_chat_id,
                    current_message_id,
                    kind,
                    historical,
                )

            except Exception:
                log.exception(
                    "Media processing failed: chat=%s message=%s",
                    current_chat_id,
                    current_message_id,
                )
            finally:
                try:
                    Path(temp).unlink(missing_ok=True)
                except OSError:
                    pass

    async def publish_pending(self):
        if not self.client or not self.client.is_connected():
            log.warning("Publish job skipped: MTProto client is disconnected")
            return

        settings = await self.db.get_settings()
        rows = await self.db.pending(
            int(settings["publish_batch_size"])
        )

        log.info("Publish job started | batch=%s", len(rows))

        for row in rows:
            queue_id = int(row["queue_id"])
            temp = self.temp_dir / f"publish_{uuid4().hex}"

            try:
                source_chat_id = (
                    row["database_chat_id"]
                    or row["source_chat_id"]
                )
                source_message_id = (
                    row["database_message_id"]
                    or row["source_message_id"]
                )

                message = await self.client.get_messages(
                    int(source_chat_id),
                    ids=int(source_message_id),
                )

                if not message:
                    raise RuntimeError(
                        "Stored media message was not found"
                    )

                if getattr(message, "noforwards", False):
                    raise RuntimeError(
                        "Protected source content was skipped"
                    )

                downloaded = await message.download_media(
                    file=str(temp)
                )
                if not downloaded:
                    raise RuntimeError(
                        "Queued media download failed"
                    )

                path = Path(downloaded)

                await self.client.send_file(
                    int(row["destination_chat_id"]),
                    path,
                    caption=row["caption"] or None,
                    supports_streaming=(
                        path.suffix.lower()
                        in {".mp4", ".mov", ".mkv", ".webm"}
                    ),
                )

                await self.db.mark_published(queue_id)

                log.info(
                    "Published media: queue=%s destination=%s",
                    queue_id,
                    row["destination_chat_id"],
                )

            except Exception as exc:
                await self.db.mark_failed(queue_id, exc)
                log.exception(
                    "Scheduled publishing failed: queue=%s",
                    queue_id,
                )
            finally:
                try:
                    Path(temp).unlink(missing_ok=True)
                except OSError:
                    pass
