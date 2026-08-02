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

    async def start(self):
        settings = await self.db.get_settings()
        if not settings["service_enabled"]:
            return
        if self.client and self.client.is_connected():
            return

        api_hash = decrypt_text(self.fernet, settings["api_hash_encrypted"])
        session = decrypt_text(self.fernet, settings["session_encrypted"])
        if not settings["api_id"] or not api_hash or not session:
            raise RuntimeError("Telegram account is not connected")

        self.client = TelegramClient(
            StringSession(session), int(settings["api_id"]), api_hash
        )
        await self.client.connect()
        if not await self.client.is_user_authorized():
            raise RuntimeError("Saved Telegram session is no longer authorized")

        source_ids = [int(x) for x in settings["source_chat_ids"]]

        @self.client.on(events.NewMessage(chats=source_ids or None))
        async def on_message(event):
            if source_ids and int(event.chat_id) in source_ids:
                await self.process_message(event)

        self.scheduler = AsyncIOScheduler(timezone="Asia/Kolkata")
        self.scheduler.add_job(
            self.publish_pending,
            "interval",
            minutes=max(1, int(settings["publish_interval_minutes"])),
            max_instances=1,
            coalesce=True,
        )
        self.scheduler.start()
        log.info("Media runtime started")

    async def stop(self):
        if self.scheduler:
            self.scheduler.shutdown(wait=False)
            self.scheduler = None
        if self.client:
            await self.client.disconnect()
            self.client = None
        log.info("Media runtime stopped")

    async def restart(self):
        await self.stop()
        await self.start()

    async def process_message(self, event):
        settings = await self.db.get_settings()
        kind = media_kind(event.message)
        if not kind:
            return
        if getattr(event.chat, "noforwards", False) or getattr(
            event.message, "noforwards", False
        ):
            return

        temp = self.temp_dir / uuid4().hex
        try:
            downloaded = await event.message.download_media(file=str(temp))
            if not downloaded:
                return
            path = Path(downloaded)
            digest = await asyncio.to_thread(sha256_file, path)
            existing = await self.db.find_by_hash(digest)
            if existing:
                if settings["delete_duplicates"]:
                    try:
                        await event.delete()
                    except Exception:
                        log.exception("Could not delete duplicate")
                return

            media_id = await self.db.add_media(
                digest, kind, path.stat().st_size,
                int(event.chat_id), int(event.id),
                event.message.message or None,
            )

            if settings["copy_to_database"] and settings["database_chat_id"]:
                sent = await self.client.send_file(
                    int(settings["database_chat_id"]),
                    path,
                    caption=event.message.message or None,
                    supports_streaming=(kind == "video"),
                )
                await self.db.set_database_message(
                    media_id, int(settings["database_chat_id"]), int(sent.id)
                )

            if settings["queue_for_publishing"]:
                for destination in settings["destination_chat_ids"]:
                    await self.db.enqueue(media_id, int(destination))
        finally:
            temp.unlink(missing_ok=True)

    async def publish_pending(self):
        settings = await self.db.get_settings()
        for row in await self.db.pending(int(settings["publish_batch_size"])):
            try:
                chat_id = row["database_chat_id"] or row["source_chat_id"]
                message_id = row["database_message_id"] or row["source_message_id"]
                entity = await self.client.get_entity(int(chat_id))
                message = await self.client.get_messages(entity, ids=int(message_id))
                if not message:
                    raise RuntimeError("Stored media not found")
                if getattr(entity, "noforwards", False) or getattr(
                    message, "noforwards", False
                ):
                    raise RuntimeError("Protected content skipped")

                temp = self.temp_dir / f"publish_{uuid4().hex}"
                downloaded = await message.download_media(file=str(temp))
                if not downloaded:
                    raise RuntimeError("Media download failed")
                path = Path(downloaded)
                try:
                    await self.client.send_file(
                        int(row["destination_chat_id"]),
                        path,
                        caption=row["caption"] or None,
                        supports_streaming=path.suffix.lower()
                        in {".mp4", ".mov", ".mkv", ".webm"},
                    )
                finally:
                    path.unlink(missing_ok=True)
                await self.db.mark_published(int(row["queue_id"]))
            except Exception as exc:
                await self.db.mark_failed(int(row["queue_id"]), exc)
                log.exception("Scheduled publishing failed")
