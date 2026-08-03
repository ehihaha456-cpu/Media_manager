from __future__ import annotations

import asyncio
import logging
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
INITIAL_RECENT_MESSAGES = 5


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

    async def start(self) -> None:
        settings = await self.db.get_settings()

        if not settings["service_enabled"]:
            return

        if self.running:
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

        # Resolve all selected chats now, so invalid IDs fail immediately.
        selected_ids = [
            *[int(x) for x in settings["source_chat_ids"]],
            int(settings["database_chat_id"]),
            *[int(x) for x in settings["destination_chat_ids"]],
        ]
        for chat_id in dict.fromkeys(selected_ids):
            await self.client.get_entity(chat_id)

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
        self.poll_task = asyncio.create_task(
            self._poll_loop(),
            name="media-manager-poll-loop",
        )

        log.info(
            "Media polling runtime started | sources=%s database=%s "
            "destinations=%s poll=%ss",
            settings["source_chat_ids"],
            settings["database_chat_id"],
            settings["destination_chat_ids"],
            POLL_SECONDS,
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

        source_ids = [
            int(x) for x in settings.get("source_chat_ids", [])
        ]
        database_chat_id = int(settings["database_chat_id"])

        for source_chat_id in source_ids:
            await self._poll_chat(
                source_chat_id,
                role="source",
            )

        await self._poll_chat(
            database_chat_id,
            role="database",
        )

    async def _poll_chat(
        self,
        chat_id: int,
        *,
        role: str,
    ) -> None:
        if not self.client:
            return

        offset = await self.db.get_chat_offset(chat_id)

        if offset is None:
            recent = await self.client.get_messages(
                chat_id,
                limit=INITIAL_RECENT_MESSAGES,
            )
            messages = list(reversed(recent))
        else:
            messages = await self.client.get_messages(
                chat_id,
                min_id=int(offset),
                limit=100,
                reverse=True,
            )

        highest_id = int(offset or 0)

        for message in messages:
            highest_id = max(highest_id, int(message.id))

            kind = media_kind(message)
            if not kind:
                continue

            try:
                if role == "source":
                    await self._process_source_message(
                        chat_id,
                        message,
                        kind,
                    )
                else:
                    await self._process_database_message(
                        chat_id,
                        message,
                        kind,
                    )
            except Exception:
                log.exception(
                    "%s media processing failed: chat=%s message=%s",
                    role.title(),
                    chat_id,
                    message.id,
                )

        if highest_id > int(offset or 0):
            await self.db.set_chat_offset(
                chat_id,
                highest_id,
            )

    async def _process_source_message(
        self,
        source_chat_id: int,
        message,
        kind: str,
    ) -> None:
        if not self.client:
            return

        settings = await self.db.get_settings()
        database_chat_id = int(settings["database_chat_id"])

        if getattr(message, "noforwards", False):
            log.warning(
                "Protected source skipped: chat=%s message=%s",
                source_chat_id,
                message.id,
            )
            return

        temp = self.temp_dir / f"source_{uuid4().hex}"

        async with self.processing_lock:
            try:
                downloaded = await message.download_media(
                    file=str(temp)
                )
                if not downloaded:
                    raise RuntimeError(
                        "Source media download returned no file"
                    )

                path = Path(downloaded)
                digest = await asyncio.to_thread(
                    sha256_file,
                    path,
                )

                sent = await self.client.send_file(
                    database_chat_id,
                    path,
                    caption=message.message or None,
                    supports_streaming=(kind == "video"),
                )

                inserted, record = (
                    await self.db.register_database_media(
                        sha256=digest,
                        kind=kind,
                        size=path.stat().st_size,
                        database_chat_id=database_chat_id,
                        database_message_id=int(sent.id),
                        caption=message.message or None,
                        source_chat_id=source_chat_id,
                        source_message_id=int(message.id),
                    )
                )

                # Advance Database offset over our own newly-uploaded message.
                current_database_offset = (
                    await self.db.get_chat_offset(
                        database_chat_id
                    )
                    or 0
                )
                if int(sent.id) > current_database_offset:
                    await self.db.set_chat_offset(
                        database_chat_id,
                        int(sent.id),
                    )

                if not inserted:
                    await self._handle_duplicate(
                        database_chat_id=database_chat_id,
                        duplicate_message_id=int(sent.id),
                        original=record,
                        kind=kind,
                        size=path.stat().st_size,
                        digest=digest,
                    )
                    return

                await self._enqueue_destinations(
                    int(record["id"]),
                    settings,
                )

                log.info(
                    "Source copied to Database: %s/%s -> %s/%s",
                    source_chat_id,
                    message.id,
                    database_chat_id,
                    sent.id,
                )
            finally:
                Path(temp).unlink(missing_ok=True)

    async def _process_database_message(
        self,
        database_chat_id: int,
        message,
        kind: str,
    ) -> None:
        settings = await self.db.get_settings()
        temp = self.temp_dir / f"database_{uuid4().hex}"

        async with self.processing_lock:
            try:
                downloaded = await message.download_media(
                    file=str(temp)
                )
                if not downloaded:
                    raise RuntimeError(
                        "Database media download returned no file"
                    )

                path = Path(downloaded)
                digest = await asyncio.to_thread(
                    sha256_file,
                    path,
                )

                inserted, record = (
                    await self.db.register_database_media(
                        sha256=digest,
                        kind=kind,
                        size=path.stat().st_size,
                        database_chat_id=database_chat_id,
                        database_message_id=int(message.id),
                        caption=message.message or None,
                    )
                )

                if not inserted:
                    original_chat_id = int(
                        record.get("database_chat_id")
                        or record.get("source_chat_id")
                    )
                    original_message_id = int(
                        record.get("database_message_id")
                        or record.get("source_message_id")
                    )

                    if (
                        original_chat_id == database_chat_id
                        and original_message_id == int(message.id)
                    ):
                        return

                    await self._handle_duplicate(
                        database_chat_id=database_chat_id,
                        duplicate_message_id=int(message.id),
                        original=record,
                        kind=kind,
                        size=path.stat().st_size,
                        digest=digest,
                    )
                    return

                await self._enqueue_destinations(
                    int(record["id"]),
                    settings,
                )

                log.info(
                    "Database media indexed: %s/%s",
                    database_chat_id,
                    message.id,
                )
            finally:
                Path(temp).unlink(missing_ok=True)

    async def _enqueue_destinations(
        self,
        media_id: int,
        settings: dict,
    ) -> None:
        database_chat_id = int(settings["database_chat_id"])

        for destination in settings["destination_chat_ids"]:
            destination_id = int(destination)

            if destination_id == database_chat_id:
                continue

            await self.db.enqueue(
                media_id,
                destination_id,
            )

        log.info(
            "Media queued for destinations: media_id=%s count=%s",
            media_id,
            len(settings["destination_chat_ids"]),
        )

    async def _handle_duplicate(
        self,
        *,
        database_chat_id: int,
        duplicate_message_id: int,
        original: dict,
        kind: str,
        size: int,
        digest: str,
    ) -> None:
        if not self.client:
            return

        settings = await self.db.get_settings()

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

        delete_status = "ℹ️ Duplicate retained"

        if settings["delete_duplicates"]:
            try:
                entity = await self.client.get_entity(
                    database_chat_id
                )
                await self.client.delete_messages(
                    entity,
                    [int(duplicate_message_id)],
                    revoke=True,
                )
                delete_status = (
                    "✅ Duplicate deleted automatically"
                )
                log.info(
                    "Database duplicate deleted: %s/%s",
                    database_chat_id,
                    duplicate_message_id,
                )
            except Exception as exc:
                delete_status = (
                    "❌ Auto-delete failed: "
                    f"<code>{type(exc).__name__}</code>"
                )
                log.exception(
                    "Database duplicate delete failed: %s/%s",
                    database_chat_id,
                    duplicate_message_id,
                )

        if not settings.get("duplicate_alerts", 1):
            return

        original_text = (
            f'<a href="{original_link}">Open original media</a>'
            if original_link
            else (
                f"Original: <code>{original_chat_id}/"
                f"{original_message_id}</code>"
            )
        )
        duplicate_text = (
            f'<a href="{duplicate_link}">Open duplicate media</a>'
            if duplicate_link
            else (
                f"Duplicate: <code>{database_chat_id}/"
                f"{duplicate_message_id}</code>"
            )
        )

        alert = (
            "⚠️ <b>Duplicate Media Detected</b>\n\n"
            f"Type: <b>{kind.title()}</b>\n"
            f"Size: <b>{size / (1024 * 1024):.2f} MB</b>\n"
            f"Hash: <code>{digest[:16]}…</code>\n\n"
            f"📂 {original_text}\n"
            f"🆕 {duplicate_text}\n\n"
            f"Action: {delete_status}"
        )

        try:
            await self.alert_bot.send_message(
                chat_id=self.owner_id,
                text=alert,
                parse_mode="HTML",
                disable_web_page_preview=True,
            )
        except Exception:
            log.exception(
                "Duplicate owner alert could not be sent"
            )

    async def _message_link(
        self,
        chat_id: int,
        message_id: int,
    ) -> str | None:
        if not self.client:
            return None

        try:
            entity = await self.client.get_entity(
                int(chat_id)
            )
            username = getattr(entity, "username", None)

            if username:
                return (
                    f"https://t.me/{username}/"
                    f"{int(message_id)}"
                )

            raw = str(abs(int(chat_id)))
            if raw.startswith("100"):
                return (
                    f"https://t.me/c/{raw[3:]}/"
                    f"{int(message_id)}"
                )
        except Exception:
            log.exception(
                "Message link generation failed: %s/%s",
                chat_id,
                message_id,
            )

        return None

    async def publish_pending(self) -> None:
        if not self.client or not self.client.is_connected():
            return

        settings = await self.db.get_settings()
        rows = await self.db.pending(
            max(
                1,
                int(settings["publish_batch_size"]),
            )
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

                if getattr(message, "noforwards", False):
                    raise RuntimeError(
                        "Protected Database media cannot be reused"
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

                log.info(
                    "Database media published: queue=%s "
                    "destination=%s",
                    queue_id,
                    row["destination_chat_id"],
                )
            except Exception as exc:
                await self.db.mark_failed(
                    queue_id,
                    exc,
                )
                log.exception(
                    "Scheduled publishing failed: queue=%s",
                    queue_id,
                )
