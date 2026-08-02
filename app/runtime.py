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
        self._generated_database_messages: set[tuple[int, int]] = set()

    async def start(self):
        settings = await self.db.get_settings()

        if not settings["service_enabled"]:
            return

        if self.client and self.client.is_connected():
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

        source_ids = {
            int(chat_id)
            for chat_id in settings["source_chat_ids"]
        }
        database_chat_id = (
            int(settings["database_chat_id"])
            if settings["database_chat_id"]
            else None
        )

        watched_ids = set(source_ids)
        if database_chat_id is not None:
            watched_ids.add(database_chat_id)

        if not watched_ids:
            raise RuntimeError(
                "Select at least one source or database chat"
            )

        @self.client.on(events.NewMessage(chats=list(watched_ids)))
        async def on_message(event):
            await self.process_message(event)

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

        log.info(
            "Media runtime started | sources=%s database=%s watched=%s",
            sorted(source_ids),
            database_chat_id,
            sorted(watched_ids),
        )

    async def stop(self):
        if self.scheduler:
            self.scheduler.shutdown(wait=False)
            self.scheduler = None

        if self.client:
            await self.client.disconnect()
            self.client = None

        self._generated_database_messages.clear()
        log.info("Media runtime stopped")

    async def restart(self):
        await self.stop()
        await self.start()

    async def process_message(self, event):
        settings = await self.db.get_settings()

        kind = media_kind(event.message)
        if not kind:
            return

        current_chat_id = int(event.chat_id)
        current_message_id = int(event.id)

        # Ignore the database copy created by this service itself.
        generated_key = (current_chat_id, current_message_id)
        if generated_key in self._generated_database_messages:
            self._generated_database_messages.discard(generated_key)
            log.info(
                "Ignored self-created database copy: chat=%s message=%s",
                current_chat_id,
                current_message_id,
            )
            return

        if (
            getattr(event.chat, "noforwards", False)
            or getattr(event.message, "noforwards", False)
        ):
            log.warning(
                "Protected media skipped: chat=%s message=%s",
                current_chat_id,
                current_message_id,
            )
            return

        database_chat_id = (
            int(settings["database_chat_id"])
            if settings["database_chat_id"]
            else None
        )

        temp = self.temp_dir / uuid4().hex

        try:
            downloaded = await event.message.download_media(
                file=str(temp)
            )
            if not downloaded:
                log.warning(
                    "Media download failed: chat=%s message=%s",
                    current_chat_id,
                    current_message_id,
                )
                return

            path = Path(downloaded)
            digest = await asyncio.to_thread(sha256_file, path)
            existing = await self.db.find_by_hash(digest)

            if existing:
                log.info(
                    "Duplicate detected: chat=%s message=%s",
                    current_chat_id,
                    current_message_id,
                )

                if settings["delete_duplicates"]:
                    try:
                        await event.delete()
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
                event.message.message or None,
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
                    caption=event.message.message or None,
                    supports_streaming=(kind == "video"),
                )

                self._generated_database_messages.add(
                    (database_chat_id, int(sent.id))
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
                for destination in settings["destination_chat_ids"]:
                    destination_id = int(destination)
                    if destination_id == current_chat_id:
                        continue
                    await self.db.enqueue(media_id, destination_id)

                log.info(
                    "Queued media for destinations: media_id=%s count=%s",
                    media_id,
                    len(settings["destination_chat_ids"]),
                )

            log.info(
                "Unique media indexed: chat=%s message=%s kind=%s",
                current_chat_id,
                current_message_id,
                kind,
            )

        except Exception:
            log.exception(
                "Media processing failed: chat=%s message=%s",
                current_chat_id,
                current_message_id,
            )
        finally:
            temp.unlink(missing_ok=True)

    async def publish_pending(self):
        settings = await self.db.get_settings()
        rows = await self.db.pending(
            int(settings["publish_batch_size"])
        )

        log.info("Publish job started | pending_batch=%s", len(rows))

        for row in rows:
            queue_id = int(row["queue_id"])
            temp = None

            try:
                source_chat_id = (
                    row["database_chat_id"]
                    or row["source_chat_id"]
                )
                source_message_id = (
                    row["database_message_id"]
                    or row["source_message_id"]
                )

                entity = await self.client.get_entity(
                    int(source_chat_id)
                )
                message = await self.client.get_messages(
                    entity,
                    ids=int(source_message_id),
                )

                if not message:
                    raise RuntimeError(
                        "Stored media message was not found"
                    )

                if (
                    getattr(entity, "noforwards", False)
                    or getattr(message, "noforwards", False)
                ):
                    raise RuntimeError(
                        "Protected source content was skipped"
                    )

                temp = self.temp_dir / f"publish_{uuid4().hex}"
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
                if temp:
                    Path(temp).unlink(missing_ok=True)
