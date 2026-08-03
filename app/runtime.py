from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from pathlib import Path
from uuid import uuid4

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from telegram import Bot
from telethon import TelegramClient
from telethon.sessions import StringSession

from .crypto import decrypt_text
from .media import media_kind, sha256_file

log = logging.getLogger(__name__)

POLL_SECONDS = 2
INITIAL_RECENT_MESSAGES = 50
NEW_MEDIA_GROUP_WINDOW = 3


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
        self.owner_id = int(owner_id)
        self.alert_bot = Bot(token=bot_token)

        self.client: TelegramClient | None = None
        self.scheduler: AsyncIOScheduler | None = None
        self.poll_task: asyncio.Task | None = None
        self.processing_lock = asyncio.Lock()
        self.running = False

        self.pending_notifications: dict[str, dict] = {}
        self.notification_tasks: dict[str, asyncio.Task] = {}

    async def start(self) -> None:
        settings = await self.db.get_settings()
        if not settings["service_enabled"] or self.running:
            return

        if not settings.get("database_chat_id"):
            raise RuntimeError("Select a Database chat first")
        if not settings.get("source_chat_ids"):
            raise RuntimeError("Select at least one Source chat")
        if not settings.get("destination_chat_ids"):
            raise RuntimeError("Select at least one Destination chat")

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

        self.running = True
        self.poll_task = asyncio.create_task(self._poll_loop())

        log.info(
            "Media polling runtime started | sources=%s database=%s",
            settings["source_chat_ids"],
            settings["database_chat_id"],
        )

    async def stop(self) -> None:
        self.running = False

        if self.poll_task:
            self.poll_task.cancel()
            await asyncio.gather(
                self.poll_task,
                return_exceptions=True,
            )
            self.poll_task = None

        for task in self.notification_tasks.values():
            task.cancel()
        if self.notification_tasks:
            await asyncio.gather(
                *self.notification_tasks.values(),
                return_exceptions=True,
            )
        self.notification_tasks.clear()
        self.pending_notifications.clear()

        if self.scheduler:
            self.scheduler.shutdown(wait=False)
            self.scheduler = None

        if self.client:
            await self.client.disconnect()
            self.client = None

        log.info("Media runtime stopped")

    async def restart(self) -> None:
        await self.stop()
        settings = await self.db.get_settings()
        if settings["service_enabled"]:
            await self.start()

    async def _poll_loop(self) -> None:
        while self.running:
            try:
                await self._poll_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("Media polling cycle failed")
            await asyncio.sleep(POLL_SECONDS)

    async def _poll_once(self) -> None:
        if not self.client or not self.client.is_connected():
            return

        settings = await self.db.get_settings()

        for source_chat_id in settings["source_chat_ids"]:
            await self._poll_source_chat(int(source_chat_id))

    async def _poll_source_chat(self, chat_id: int) -> None:
        offset = await self.db.get_chat_offset(chat_id)

        if offset is None:
            messages = list(
                reversed(
                    await self.client.get_messages(
                        chat_id,
                        limit=INITIAL_RECENT_MESSAGES,
                    )
                )
            )
        else:
            messages = await self.client.get_messages(
                chat_id,
                min_id=int(offset),
                limit=100,
                reverse=True,
            )

        for message in messages:
            message_id = int(message.id)
            kind = media_kind(message)

            if not kind:
                await self.db.set_chat_offset(chat_id, message_id)
                continue

            try:
                result = await self._process_source_message(
                    chat_id,
                    message,
                    kind,
                )

                if result is False:
                    break

                await self.db.set_chat_offset(chat_id, message_id)

            except Exception:
                log.exception(
                    "Source media failed; retained for retry: "
                    "chat=%s message=%s",
                    chat_id,
                    message_id,
                )
                break

    async def _process_source_message(
        self,
        source_chat_id: int,
        message,
        kind: str,
    ) -> bool:
        if not self.client:
            return False

        settings = await self.db.get_settings()
        database_chat_id = int(settings["database_chat_id"])

        if getattr(message, "noforwards", False):
            log.warning(
                "Protected source skipped: chat=%s message=%s",
                source_chat_id,
                message.id,
            )
            return True

        notification_key = await self._register_new_media_notification(
            source_chat_id,
            message,
            kind,
        )

        temp = self.temp_dir / f"source_{uuid4().hex}"

        async with self.processing_lock:
            try:
                downloaded = await message.download_media(file=str(temp))
                if not downloaded:
                    raise RuntimeError(
                        "Source media download returned no file"
                    )

                path = Path(downloaded)
                digest = await asyncio.to_thread(sha256_file, path)

                sent = await self.client.send_file(
                    database_chat_id,
                    path,
                    caption=message.message or None,
                    supports_streaming=(kind == "video"),
                )

                inserted, record = await self.db.register_database_media(
                    sha256=digest,
                    kind=kind,
                    size=path.stat().st_size,
                    database_chat_id=database_chat_id,
                    database_message_id=int(sent.id),
                    caption=message.message or None,
                    source_chat_id=source_chat_id,
                    source_message_id=int(message.id),
                )

                if not inserted:
                    try:
                        entity = await self.client.get_entity(
                            database_chat_id
                        )
                        await self.client.delete_messages(
                            entity,
                            [int(sent.id)],
                            revoke=True,
                        )
                    except Exception:
                        log.exception(
                            "Duplicate database copy could not be deleted"
                        )

                    await self._update_notification_result(
                        notification_key,
                        duplicate=1,
                        processed=1,
                    )
                    return True

                for destination in settings["destination_chat_ids"]:
                    destination_id = int(destination)
                    if destination_id != database_chat_id:
                        await self.db.enqueue(
                            int(record["id"]),
                            destination_id,
                        )

                await self._update_notification_result(
                    notification_key,
                    uploaded=1,
                    queued=1,
                    processed=1,
                )

                log.info(
                    "Source copied to Database: %s/%s -> %s/%s",
                    source_chat_id,
                    message.id,
                    database_chat_id,
                    sent.id,
                )
                return True
            finally:
                Path(temp).unlink(missing_ok=True)


    def _notification_key(
        self,
        source_chat_id: int,
        message,
    ) -> str:
        grouped_id = getattr(message, "grouped_id", None)
        if grouped_id:
            return f"album:{int(source_chat_id)}:{int(grouped_id)}"

        # Non-album messages from the same source are grouped only during
        # a short debounce window.
        return f"single:{int(source_chat_id)}"

    async def _register_new_media_notification(
        self,
        source_chat_id: int,
        message,
        kind: str,
    ) -> str:
        key = self._notification_key(
            source_chat_id,
            message,
        )

        bucket = self.pending_notifications.setdefault(
            key,
            {
                "source_chat_id": int(source_chat_id),
                "counts": defaultdict(int),
                "expected": 0,
                "processed": 0,
                "uploaded": 0,
                "duplicate": 0,
                "queued": 0,
                "failed": 0,
                "message_id": None,
                "chat_name": None,
                "last_update": asyncio.get_running_loop().time(),
            },
        )

        bucket["counts"][kind] += 1
        bucket["expected"] += 1
        bucket["last_update"] = asyncio.get_running_loop().time()

        current = self.notification_tasks.get(key)
        if current and not current.done():
            current.cancel()

        self.notification_tasks[key] = asyncio.create_task(
            self._notification_debounce(key)
        )

        return key

    async def _update_notification_result(
        self,
        key: str,
        *,
        processed: int = 0,
        uploaded: int = 0,
        duplicate: int = 0,
        queued: int = 0,
        failed: int = 0,
    ) -> None:
        bucket = self.pending_notifications.get(key)
        if not bucket:
            return

        bucket["processed"] += processed
        bucket["uploaded"] += uploaded
        bucket["duplicate"] += duplicate
        bucket["queued"] += queued
        bucket["failed"] += failed

        # If the initial message has already been sent and all detected media
        # finished processing, edit that same message immediately.
        if (
            bucket["message_id"] is not None
            and bucket["processed"] + bucket["failed"]
            >= bucket["expected"]
        ):
            await self._finalize_notification(key)

    async def _notification_debounce(
        self,
        key: str,
    ) -> None:
        try:
            # Resetting this task whenever another item arrives ensures
            # one album/burst produces one notification.
            await asyncio.sleep(NEW_MEDIA_GROUP_WINDOW)

            bucket = self.pending_notifications.get(key)
            if not bucket:
                return

            await self._send_processing_notification(key)

            if (
                bucket["processed"] + bucket["failed"]
                >= bucket["expected"]
            ):
                await self._finalize_notification(key)

        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception(
                "Notification debounce failed: key=%s",
                key,
            )

    async def _resolve_chat_name(
        self,
        source_chat_id: int,
    ) -> str:
        try:
            entity = await self.client.get_entity(
                source_chat_id
            )
            return (
                getattr(entity, "title", None)
                or getattr(entity, "username", None)
                or str(source_chat_id)
            )
        except Exception:
            log.exception(
                "Could not resolve source name: %s",
                source_chat_id,
            )
            return str(source_chat_id)

    def _media_count_lines(
        self,
        counts,
    ) -> list[str]:
        lines: list[str] = []

        if counts["video"] > 0:
            lines.append(
                f"🎬 Videos: <b>{counts['video']}</b>"
            )
        if counts["photo"] > 0:
            lines.append(
                f"🖼 Images: <b>{counts['photo']}</b>"
            )
        if counts["audio"] > 0:
            lines.append(
                f"🎵 Audio: <b>{counts['audio']}</b>"
            )
        if counts["file"] > 0:
            lines.append(
                f"📁 Files: <b>{counts['file']}</b>"
            )

        return lines

    async def _send_processing_notification(
        self,
        key: str,
    ) -> None:
        bucket = self.pending_notifications.get(key)
        if not bucket or bucket["message_id"] is not None:
            return

        source_chat_id = int(bucket["source_chat_id"])
        chat_name = await self._resolve_chat_name(
            source_chat_id
        )
        bucket["chat_name"] = chat_name

        lines = self._media_count_lines(
            bucket["counts"]
        )
        total = sum(bucket["counts"].values())

        text = (
            "🆕 <b>New Media Detected</b>\n\n"
            f"Source: <b>{chat_name}</b>\n"
            f"Chat ID: <code>{source_chat_id}</code>\n\n"
            + "\n".join(lines)
            + f"\n\nTotal: <b>{total}</b>\n\n"
            "⏳ <b>Processing...</b>"
        )

        sent = await self.alert_bot.send_message(
            chat_id=self.owner_id,
            text=text,
            parse_mode="HTML",
        )
        bucket["message_id"] = int(sent.message_id)

    async def _finalize_notification(
        self,
        key: str,
    ) -> None:
        bucket = self.pending_notifications.get(key)
        if not bucket:
            return

        if bucket["message_id"] is None:
            await self._send_processing_notification(key)
            bucket = self.pending_notifications.get(key)
            if not bucket or bucket["message_id"] is None:
                return

        source_chat_id = int(bucket["source_chat_id"])
        chat_name = (
            bucket.get("chat_name")
            or await self._resolve_chat_name(source_chat_id)
        )

        lines = self._media_count_lines(
            bucket["counts"]
        )
        total = sum(bucket["counts"].values())

        final_text = (
            "✅ <b>Media Processed</b>\n\n"
            f"Source: <b>{chat_name}</b>\n"
            f"Chat ID: <code>{source_chat_id}</code>\n\n"
            + "\n".join(lines)
            + f"\n\nTotal: <b>{total}</b>\n\n"
            f"Processed: <b>{bucket['processed']}</b>\n"
            f"Uploaded to Database: "
            f"<b>{bucket['uploaded']}</b>\n"
            f"Duplicates: <b>{bucket['duplicate']}</b>\n"
            f"Destination Queue: <b>{bucket['queued']}</b>"
        )

        if bucket["failed"] > 0:
            final_text += (
                f"\nFailed: <b>{bucket['failed']}</b>"
            )

        try:
            await self.alert_bot.edit_message_text(
                chat_id=self.owner_id,
                message_id=int(bucket["message_id"]),
                text=final_text,
                parse_mode="HTML",
            )
        except Exception:
            log.exception(
                "Could not edit media notification: key=%s",
                key,
            )
            return

        task = self.notification_tasks.pop(key, None)
        if (
            task
            and task is not asyncio.current_task()
            and not task.done()
        ):
            task.cancel()

        self.pending_notifications.pop(key, None)

    async def publish_pending(self) -> None:
        if not self.client or not self.client.is_connected():
            return

        settings = await self.db.get_settings()
        rows = await self.db.pending(
            max(1, int(settings["publish_batch_size"]))
        )

        for row in rows:
            queue_id = int(row["queue_id"])
            try:
                message = await self.client.get_messages(
                    int(row["database_chat_id"]),
                    ids=int(row["database_message_id"]),
                )
                if not message:
                    raise RuntimeError(
                        "Database media message was not found"
                    )

                await self.client.send_file(
                    int(row["destination_chat_id"]),
                    message.media,
                    caption=row["caption"] or None,
                    supports_streaming=(
                        media_kind(message) == "video"
                    ),
                )

                await self.db.mark_published(queue_id)

            except Exception as exc:
                await self.db.mark_failed(queue_id, exc)
                log.exception(
                    "Scheduled publishing failed: queue=%s",
                    queue_id,
                )
