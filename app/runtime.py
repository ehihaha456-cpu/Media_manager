from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from uuid import uuid4

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telegram import Bot

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
    def __init__(
        self,
        db,
        fernet,
        temp_dir,
        bot_token: str,
        owner_id: int,
    ):
        self.db = db
        self.fernet = fernet
        self.temp_dir = temp_dir
        self.alert_bot = Bot(token=bot_token)
        self.owner_id = int(owner_id)
        self.client: TelegramClient | None = None
        self.scheduler: AsyncIOScheduler | None = None
        self.media_queue: asyncio.Queue[tuple[int, int]] = asyncio.Queue(
            maxsize=500
        )
        self.worker_tasks: list[asyncio.Task] = []
        self._started = False
        self._queued_messages: set[tuple[int, int]] = set()


async def _message_link(
    self,
    chat_id: int,
    message_id: int,
) -> str | None:
    if not self.client:
        return None

    try:
        entity = await self.client.get_entity(int(chat_id))
        username = getattr(entity, "username", None)
        if username:
            return f"https://t.me/{username}/{int(message_id)}"

        raw_id = str(abs(int(chat_id)))
        if raw_id.startswith("100"):
            return (
                f"https://t.me/c/{raw_id[3:]}/"
                f"{int(message_id)}"
            )
    except Exception:
        log.exception(
            "Could not build message link: chat=%s message=%s",
            chat_id,
            message_id,
        )
    return None

async def _send_duplicate_alert(
    self,
    *,
    database_chat_id: int,
    duplicate_message_id: int,
    original: dict,
    media_kind_name: str,
    file_size: int,
    sha256: str,
    delete_status: str,
) -> None:
    settings = await self.db.get_settings()
    if not settings.get("duplicate_alerts", 1):
        return

    original_chat_id = int(
        original.get("database_chat_id")
        or original.get("source_chat_id")
    )
    original_message_id = int(
        original.get("database_message_id")
        or original.get("source_message_id")
    )

    original_link = await self._message_link(
        original_chat_id,
        original_message_id,
    )
    duplicate_link = await self._message_link(
        database_chat_id,
        duplicate_message_id,
    )

    original_text = (
        f'<a href="{original_link}">Open original media</a>'
        if original_link
        else (
            f"Original: chat <code>{original_chat_id}</code>, "
            f"message <code>{original_message_id}</code>"
        )
    )
    duplicate_text = (
        f'<a href="{duplicate_link}">Open duplicate media</a>'
        if duplicate_link
        else (
            f"Duplicate: chat <code>{database_chat_id}</code>, "
            f"message <code>{duplicate_message_id}</code>"
        )
    )

    size_mb = file_size / (1024 * 1024)
    alert_text = (
        "⚠️ <b>Duplicate Media Detected</b>\n\n"
        f"Type: <b>{media_kind_name.title()}</b>\n"
        f"Size: <b>{size_mb:.2f} MB</b>\n"
        f"Hash: <code>{sha256[:16]}…</code>\n\n"
        f"📂 {original_text}\n"
        f"🆕 {duplicate_text}\n\n"
        f"Action: {delete_status}"
    )

    try:
        await self.alert_bot.send_message(
            chat_id=self.owner_id,
            text=alert_text,
            parse_mode="HTML",
            disable_web_page_preview=True,
        )
    except Exception:
        log.exception("Could not send duplicate alert")

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
            inserted, media_record = await self.db.register_media(
                digest,
                kind,
                path.stat().st_size,
                database_chat_id,
                int(message_id),
                message.message or None,
            )

            if not inserted:
                original_chat_id = int(
                    media_record.get("database_chat_id")
                    or media_record.get("source_chat_id")
                )
                original_message_id = int(
                    media_record.get("database_message_id")
                    or media_record.get("source_message_id")
                )

                if (
                    original_chat_id == database_chat_id
                    and original_message_id == int(message_id)
                ):
                    return

                delete_status = "ℹ️ Duplicate retained"

                if settings["delete_duplicates"]:
                    try:
                        entity = await self.client.get_entity(
                            database_chat_id
                        )
                        await self.client.delete_messages(
                            entity,
                            [int(message_id)],
                            revoke=True,
                        )
                        delete_status = "✅ Duplicate deleted automatically"
                        log.info(
                            "Database duplicate deleted: message=%s",
                            message_id,
                        )
                    except Exception as exc:
                        delete_status = (
                            "❌ Auto-delete failed: "
                            f"<code>{type(exc).__name__}</code>"
                        )
                        log.exception(
                            "Database duplicate delete failed: "
                            "chat=%s message=%s",
                            database_chat_id,
                            message_id,
                        )

                await self._send_duplicate_alert(
                    database_chat_id=database_chat_id,
                    duplicate_message_id=int(message_id),
                    original=media_record,
                    media_kind_name=kind,
                    file_size=path.stat().st_size,
                    sha256=digest,
                    delete_status=delete_status,
                )
                return

            media_id = int(media_record["id"])

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
