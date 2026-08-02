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

MODE_LIMITS = {
    "low": {
        "workers": 1,
        "publish_parallel": 1,
        "publish_batch": 1,
    },
    "balanced": {
        "workers": 2,
        "publish_parallel": 3,
        "publish_batch": 6,
    },
    "turbo": {
        "workers": 4,
        "publish_parallel": 6,
        "publish_batch": 12,
    },
}


class MediaRuntime:
    def __init__(self, db, fernet, temp_dir):
        self.db = db
        self.fernet = fernet
        self.temp_dir = temp_dir
        self.client: TelegramClient | None = None
        self.scheduler: AsyncIOScheduler | None = None
        self.media_queue: asyncio.Queue[tuple[int, int]] = asyncio.Queue(
            maxsize=500
        )
        self.worker_tasks: list[asyncio.Task] = []
        self._started = False
        self._queued_messages: set[tuple[int, int]] = set()

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

        database_chat_id = settings.get("database_chat_id")
        if not database_chat_id:
            raise RuntimeError("Select a Database chat first")

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

        @self.client.on(events.NewMessage())
        async def on_new_message(event):
            try:
                await self.route_message(event)
            except Exception:
                log.exception(
                    "Message routing failed: chat=%s message=%s",
                    event.chat_id,
                    event.id,
                )

        mode = self._mode(settings)
        self.worker_tasks = [
            asyncio.create_task(
                self._database_worker(number + 1),
                name=f"database-worker-{number + 1}",
            )
            for number in range(mode["workers"])
        ]

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
            "Media runtime started | mode=%s workers=%s sources=%s "
            "database=%s destinations=%s",
            settings.get("performance_mode", "balanced"),
            mode["workers"],
            settings["source_chat_ids"],
            database_chat_id,
            settings["destination_chat_ids"],
        )

        asyncio.create_task(self.scan_database_recent(limit=100))

    def _mode(self, settings: dict) -> dict:
        name = str(
            settings.get("performance_mode", "balanced")
        ).lower()
        return MODE_LIMITS.get(name, MODE_LIMITS["balanced"])

    async def stop(self):
        self._started = False

        if self.scheduler:
            self.scheduler.shutdown(wait=False)
            self.scheduler = None

        for task in self.worker_tasks:
            task.cancel()
        if self.worker_tasks:
            await asyncio.gather(
                *self.worker_tasks,
                return_exceptions=True,
            )
        self.worker_tasks.clear()
        self._queued_messages.clear()

        while not self.media_queue.empty():
            try:
                self.media_queue.get_nowait()
                self.media_queue.task_done()
            except asyncio.QueueEmpty:
                break

        if self.client:
            await self.client.disconnect()
            self.client = None

        log.info("Media runtime stopped")

    async def restart(self):
        await self.stop()
        settings = await self.db.get_settings()
        if settings["service_enabled"]:
            await self.start()

    async def route_message(self, event):
        if not self.client:
            return

        settings = await self.db.get_settings()
        kind = media_kind(event.message)
        if not kind:
            return

        chat_id = int(event.chat_id)
        message_id = int(event.id)
        database_chat_id = int(settings["database_chat_id"])
        source_ids = {
            int(item)
            for item in settings.get("source_chat_ids", [])
        }

        if chat_id == database_chat_id:
            await self._enqueue_database_message(
                chat_id,
                message_id,
            )
            return

        if chat_id not in source_ids:
            return

        # Content-protected sources are intentionally not copied or bypassed.
        if (
            getattr(event.chat, "noforwards", False)
            or getattr(event.message, "noforwards", False)
        ):
            log.warning(
                "Protected source skipped: chat=%s message=%s",
                chat_id,
                message_id,
            )
            return

        # Fast path: reuse Telegram's existing media reference.
        # No server download or re-upload is required.
        try:
            sent = await self.client.send_file(
                database_chat_id,
                event.message.media,
                caption=event.message.message or None,
                supports_streaming=(kind == "video"),
            )
            log.info(
                "Fast copied Source → Database: %s/%s → %s/%s",
                chat_id,
                message_id,
                database_chat_id,
                sent.id,
            )
        except Exception:
            log.exception(
                "Fast Source → Database copy failed: chat=%s message=%s",
                chat_id,
                message_id,
            )

    async def _enqueue_database_message(
        self,
        chat_id: int,
        message_id: int,
    ):
        key = (int(chat_id), int(message_id))
        if key in self._queued_messages:
            return

        self._queued_messages.add(key)
        try:
            self.media_queue.put_nowait(key)
        except asyncio.QueueFull:
            self._queued_messages.discard(key)
            log.error(
                "Database processing queue is full; skipped %s/%s",
                chat_id,
                message_id,
            )

    async def _database_worker(self, worker_number: int):
        while True:
            chat_id, message_id = await self.media_queue.get()
            try:
                await self.process_database_message(
                    chat_id,
                    message_id,
                    worker_number,
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception(
                    "Database worker failed: worker=%s chat=%s msg=%s",
                    worker_number,
                    chat_id,
                    message_id,
                )
            finally:
                self._queued_messages.discard(
                    (chat_id, message_id)
                )
                self.media_queue.task_done()

    async def scan_database_recent(self, limit: int = 100):
        if not self.client:
            return

        settings = await self.db.get_settings()
        database_chat_id = settings.get("database_chat_id")
        if not database_chat_id:
            return

        try:
            messages = await self.client.get_messages(
                int(database_chat_id),
                limit=limit,
            )
            for message in reversed(messages):
                if media_kind(message):
                    await self._enqueue_database_message(
                        int(database_chat_id),
                        int(message.id),
                    )
            log.info(
                "Database recent scan queued %s messages",
                len(messages),
            )
        except Exception:
            log.exception("Database recent scan failed")

    async def process_database_message(
        self,
        chat_id: int,
        message_id: int,
        worker_number: int,
    ):
        if not self.client:
            return

        settings = await self.db.get_settings()
        database_chat_id = int(settings["database_chat_id"])
        if int(chat_id) != database_chat_id:
            return

        message = await self.client.get_messages(
            database_chat_id,
            ids=int(message_id),
        )
        if not message:
            return

        kind = media_kind(message)
        if not kind:
            return

        temp = self.temp_dir / f"db_{uuid4().hex}"

        try:
            downloaded = await message.download_media(
                file=str(temp)
            )
            if not downloaded:
                raise RuntimeError("Database media download failed")

            path = Path(downloaded)
            digest = await asyncio.to_thread(sha256_file, path)
            existing = await self.db.find_by_hash(digest)

            if existing:
                same_database_message = (
                    existing.get("database_chat_id") is not None
                    and existing.get("database_message_id") is not None
                    and int(existing["database_chat_id"])
                    == database_chat_id
                    and int(existing["database_message_id"])
                    == int(message_id)
                )
                same_original = (
                    int(existing["source_chat_id"])
                    == database_chat_id
                    and int(existing["source_message_id"])
                    == int(message_id)
                )

                if same_database_message or same_original:
                    return

                log.info(
                    "Database duplicate detected: message=%s "
                    "worker=%s",
                    message_id,
                    worker_number,
                )

                if settings["delete_duplicates"]:
                    try:
                        await self.client.delete_messages(
                            database_chat_id,
                            [int(message_id)],
                        )
                        log.info(
                            "Database duplicate deleted: message=%s",
                            message_id,
                        )
                    except Exception:
                        log.exception(
                            "Database duplicate delete failed. "
                            "Connected account needs delete permission."
                        )
                return

            media_id = await self.db.add_media(
                digest,
                kind,
                path.stat().st_size,
                database_chat_id,
                int(message_id),
                message.message or None,
            )
            await self.db.set_database_message(
                media_id,
                database_chat_id,
                int(message_id),
            )

            if settings["queue_for_publishing"]:
                destinations = [
                    int(item)
                    for item in settings["destination_chat_ids"]
                    if int(item) != database_chat_id
                ]
                await asyncio.gather(
                    *[
                        self.db.enqueue(media_id, destination)
                        for destination in destinations
                    ]
                )

            log.info(
                "Database media indexed: media_id=%s message=%s "
                "worker=%s",
                media_id,
                message_id,
                worker_number,
            )
        finally:
            try:
                Path(temp).unlink(missing_ok=True)
            except OSError:
                pass

    async def publish_pending(self):
        if not self.client or not self.client.is_connected():
            return

        settings = await self.db.get_settings()
        mode = self._mode(settings)
        rows = await self.db.pending(mode["publish_batch"])
        if not rows:
            return

        semaphore = asyncio.Semaphore(
            mode["publish_parallel"]
        )

        async def publish_one(row: dict):
            async with semaphore:
                queue_id = int(row["queue_id"])
                try:
                    source_chat_id = int(
                        row["database_chat_id"]
                        or row["source_chat_id"]
                    )
                    source_message_id = int(
                        row["database_message_id"]
                        or row["source_message_id"]
                    )

                    message = await self.client.get_messages(
                        source_chat_id,
                        ids=source_message_id,
                    )
                    if not message:
                        raise RuntimeError(
                            "Stored Database media was not found"
                        )
                    if getattr(message, "noforwards", False):
                        raise RuntimeError(
                            "Protected Database media cannot be reused"
                        )

                    # Fast destination path: existing Telegram media reference.
                    await self.client.send_file(
                        int(row["destination_chat_id"]),
                        message.media,
                        caption=row["caption"] or None,
                        supports_streaming=(
                            media_kind(message) == "video"
                        ),
                    )
                    await self.db.mark_published(queue_id)
                    log.info(
                        "Fast published Database → Destination: "
                        "queue=%s destination=%s",
                        queue_id,
                        row["destination_chat_id"],
                    )
                except Exception as exc:
                    await self.db.mark_failed(queue_id, exc)
                    log.exception(
                        "Destination publishing failed: queue=%s",
                        queue_id,
                    )

        await asyncio.gather(
            *[publish_one(row) for row in rows]
        )
