from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from uuid import uuid4

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from telegram import Bot
from telethon import TelegramClient, events
from telethon.sessions import StringSession

from .crypto import decrypt_text
from .media import media_kind, sha256_file

log = logging.getLogger(__name__)


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
        self._lock = asyncio.Lock()
        self.source_scan_tasks: dict[int, asyncio.Task] = {}

    async def start(self):
        settings = await self.db.get_settings()
        if not settings["service_enabled"]:
            return "skipped"
        if self.client and self.client.is_connected():
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

        @self.client.on(events.NewMessage())
        async def on_message(event):
            try:
                await self._handle_event(event)
            except Exception:
                log.exception(
                    "Media event failed: chat=%s message=%s",
                    event.chat_id,
                    event.id,
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

        log.info(
            "Media runtime started | sources=%s database=%s "
            "destinations=%s",
            settings["source_chat_ids"],
            settings["database_chat_id"],
            settings["destination_chat_ids"],
        )

        for scan in await self.db.pending_source_scans():
            chat_id = int(scan["chat_id"])
            if chat_id in {
                int(value)
                for value in settings["source_chat_ids"]
            }:
                self._start_source_import_task(chat_id)

    async def stop(self):
        for task in self.source_scan_tasks.values():
            task.cancel()
        if self.source_scan_tasks:
            await asyncio.gather(
                *self.source_scan_tasks.values(),
                return_exceptions=True,
            )
        self.source_scan_tasks.clear()

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


async def register_new_source(self, chat_id: int) -> None:
    chat_id = int(chat_id)

    existing = await self.db.get_source_scan(chat_id)
    if existing and existing.get("status") == "completed":
        return

    client = self.client
    temporary_client = None

    if not client or not client.is_connected():
        settings = await self.db.get_settings()
        api_hash = decrypt_text(
            self.fernet,
            settings["api_hash_encrypted"],
        )
        session = decrypt_text(
            self.fernet,
            settings["session_encrypted"],
        )
        if not settings["api_id"] or not api_hash or not session:
            return

        temporary_client = TelegramClient(
            StringSession(session),
            int(settings["api_id"]),
            api_hash,
        )
        await temporary_client.connect()
        client = temporary_client

    counts = {
        "video": 0,
        "photo": 0,
        "audio": 0,
        "file": 0,
    }
    total_messages = 0
    chat_name = str(chat_id)

    try:
        entity = await client.get_entity(chat_id)
        chat_name = (
            getattr(entity, "title", None)
            or getattr(entity, "username", None)
            or str(chat_id)
        )

        async for message in client.iter_messages(
            entity,
            reverse=True,
        ):
            total_messages += 1
            kind = media_kind(message)
            if kind in counts:
                counts[kind] += 1

        total_media = sum(counts.values())

        await self.db.upsert_source_scan(
            chat_id,
            chat_name=chat_name,
            status="counted",
            total_messages=total_messages,
            total_media=total_media,
            videos=counts["video"],
            photos=counts["photo"],
            audio=counts["audio"],
            files=counts["file"],
            processed=0,
            uploaded=0,
            duplicates=0,
            failed=0,
            last_message_id=0,
        )

        notification = (
            "✅ <b>New Source Group Detected</b>\n\n"
            f"Name: <b>{chat_name}</b>\n"
            f"Chat ID: <code>{chat_id}</code>\n\n"
            "Media Found:\n"
            f"🎬 Videos: <b>{counts['video']}</b>\n"
            f"🖼 Photos: <b>{counts['photo']}</b>\n"
            f"🎵 Audio: <b>{counts['audio']}</b>\n"
            f"📁 Files: <b>{counts['file']}</b>\n\n"
            f"Total Media: <b>{total_media}</b>\n\n"
            "Status: Initial media scan ready."
        )

        await self.alert_bot.send_message(
            chat_id=self.owner_id,
            text=notification,
            parse_mode="HTML",
        )

    except Exception as exc:
        await self.db.upsert_source_scan(
            chat_id,
            chat_name=chat_name,
            status="failed",
            last_error=str(exc)[:1000],
        )
        log.exception(
            "New source counting failed: chat=%s",
            chat_id,
        )
        return
    finally:
        if temporary_client:
            await temporary_client.disconnect()

    settings = await self.db.get_settings()
    if (
        settings["service_enabled"]
        and self.client
        and self.client.is_connected()
    ):
        self._start_source_import_task(chat_id)

def _start_source_import_task(self, chat_id: int) -> None:
    chat_id = int(chat_id)
    current = self.source_scan_tasks.get(chat_id)
    if current and not current.done():
        return

    task = asyncio.create_task(
        self._import_source_history(chat_id),
        name=f"source-history-{chat_id}",
    )
    self.source_scan_tasks[chat_id] = task

    def cleanup(_task):
        self.source_scan_tasks.pop(chat_id, None)

    task.add_done_callback(cleanup)

async def _import_source_history(self, chat_id: int) -> None:
    if not self.client or not self.client.is_connected():
        return

    scan = await self.db.get_source_scan(chat_id)
    if not scan:
        await self.register_new_source(chat_id)
        scan = await self.db.get_source_scan(chat_id)
        if not scan:
            return

    last_message_id = int(scan.get("last_message_id", 0))
    processed = int(scan.get("processed", 0))
    uploaded = int(scan.get("uploaded", 0))
    duplicates = int(scan.get("duplicates", 0))
    failed = int(scan.get("failed", 0))
    total_media = int(scan.get("total_media", 0))
    chat_name = scan.get("chat_name", str(chat_id))

    await self.db.upsert_source_scan(
        chat_id,
        status="scanning",
    )

    try:
        entity = await self.client.get_entity(chat_id)

        async for message in self.client.iter_messages(
            entity,
            min_id=last_message_id,
            reverse=True,
        ):
            kind = media_kind(message)
            if not kind:
                last_message_id = int(message.id)
                await self.db.upsert_source_scan(
                    chat_id,
                    last_message_id=last_message_id,
                )
                continue

            try:
                result = await self._copy_source_to_database(
                    message,
                    chat_id,
                    int(
                        (
                            await self.db.get_settings()
                        )["database_chat_id"]
                    ),
                    kind,
                )
                processed += 1
                if result == "uploaded":
                    uploaded += 1
                elif result == "duplicate":
                    duplicates += 1
            except asyncio.CancelledError:
                raise
            except Exception:
                failed += 1
                log.exception(
                    "Historical source media failed: "
                    "chat=%s message=%s",
                    chat_id,
                    message.id,
                )

            last_message_id = int(message.id)

            await self.db.upsert_source_scan(
                chat_id,
                status="scanning",
                processed=processed,
                uploaded=uploaded,
                duplicates=duplicates,
                failed=failed,
                last_message_id=last_message_id,
            )

            if processed and processed % 100 == 0:
                await self.alert_bot.send_message(
                    chat_id=self.owner_id,
                    text=(
                        "⏳ <b>Source Scan Progress</b>\n\n"
                        f"Source: <b>{chat_name}</b>\n"
                        f"Processed: <b>{processed} / "
                        f"{total_media}</b>\n"
                        f"Uploaded: <b>{uploaded}</b>\n"
                        f"Duplicates: <b>{duplicates}</b>\n"
                        f"Failed: <b>{failed}</b>"
                    ),
                    parse_mode="HTML",
                )

        await self.db.upsert_source_scan(
            chat_id,
            status="completed",
            processed=processed,
            uploaded=uploaded,
            duplicates=duplicates,
            failed=failed,
            last_message_id=last_message_id,
        )

        await self.alert_bot.send_message(
            chat_id=self.owner_id,
            text=(
                "✅ <b>Source Scan Completed</b>\n\n"
                f"Source: <b>{chat_name}</b>\n"
                f"Processed: <b>{processed}</b>\n"
                f"Uploaded to Database: <b>{uploaded}</b>\n"
                f"Duplicates Found: <b>{duplicates}</b>\n"
                f"Failed: <b>{failed}</b>"
            ),
            parse_mode="HTML",
        )

    except asyncio.CancelledError:
        await self.db.upsert_source_scan(
            chat_id,
            status="pending",
        )
        raise
    except Exception as exc:
        await self.db.upsert_source_scan(
            chat_id,
            status="pending",
            last_error=str(exc)[:1000],
        )
        log.exception(
            "Source history scan failed: chat=%s",
            chat_id,
        )

    async def _handle_event(self, event):
        if not self.client:
            return

        settings = await self.db.get_settings()
        kind = media_kind(event.message)
        if not kind:
            return

        chat_id = int(event.chat_id)
        database_chat_id = int(settings["database_chat_id"])
        source_ids = {
            int(x) for x in settings["source_chat_ids"]
        }

        if chat_id in source_ids:
            await self._copy_source_to_database(
                event.message,
                chat_id,
                database_chat_id,
                kind,
            )
            return

        if chat_id == database_chat_id:
            await self._process_direct_database_media(
                event.message,
                database_chat_id,
                kind,
            )

    async def _copy_source_to_database(
        self,
        message,
        source_chat_id: int,
        database_chat_id: int,
        kind: str,
    ) -> str:
        # Content-protected sources are not bypassed.
        if getattr(message, "noforwards", False):
            log.warning(
                "Protected source skipped: chat=%s message=%s",
                source_chat_id,
                message.id,
            )
            return

        temp = self.temp_dir / f"source_{uuid4().hex}"
        async with self._lock:
            try:
                downloaded = await message.download_media(file=str(temp))
                if not downloaded:
                    raise RuntimeError("Source media download failed")

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
                    await self._delete_duplicate_and_alert(
                        database_chat_id=database_chat_id,
                        duplicate_message_id=int(sent.id),
                        original=record,
                        kind=kind,
                        size=path.stat().st_size,
                        digest=digest,
                    )
                    return "duplicate"

                await self._enqueue_destinations(
                    int(record["id"]),
                    settings=await self.db.get_settings(),
                )

                log.info(
                    "Source copied to Database: %s/%s -> %s/%s",
                    source_chat_id,
                    message.id,
                    database_chat_id,
                    sent.id,
                )
                return "uploaded"
            finally:
                Path(temp).unlink(missing_ok=True)

    async def _process_direct_database_media(
        self,
        message,
        database_chat_id: int,
        kind: str,
    ):
        temp = self.temp_dir / f"database_{uuid4().hex}"
        async with self._lock:
            try:
                downloaded = await message.download_media(file=str(temp))
                if not downloaded:
                    raise RuntimeError("Database media download failed")

                path = Path(downloaded)
                digest = await asyncio.to_thread(sha256_file, path)

                inserted, record = await self.db.register_database_media(
                    sha256=digest,
                    kind=kind,
                    size=path.stat().st_size,
                    database_chat_id=database_chat_id,
                    database_message_id=int(message.id),
                    caption=message.message or None,
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

                    await self._delete_duplicate_and_alert(
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
                    settings=await self.db.get_settings(),
                )

                log.info(
                    "Direct Database media indexed: %s/%s",
                    database_chat_id,
                    message.id,
                )
            finally:
                Path(temp).unlink(missing_ok=True)

    async def _enqueue_destinations(
        self,
        media_id: int,
        settings: dict,
    ):
        database_chat_id = int(settings["database_chat_id"])
        for destination in settings["destination_chat_ids"]:
            destination_id = int(destination)
            if destination_id == database_chat_id:
                continue
            await self.db.enqueue(media_id, destination_id)

    async def _delete_duplicate_and_alert(
        self,
        *,
        database_chat_id: int,
        duplicate_message_id: int,
        original: dict,
        kind: str,
        size: int,
        digest: str,
    ):
        settings = await self.db.get_settings()
        delete_status = "ℹ️ Duplicate retained"

        duplicate_link = await self._message_link(
            database_chat_id,
            duplicate_message_id,
        )
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

        if settings["delete_duplicates"]:
            try:
                entity = await self.client.get_entity(database_chat_id)
                await self.client.delete_messages(
                    entity,
                    [duplicate_message_id],
                    revoke=True,
                )
                delete_status = "✅ Duplicate deleted automatically"
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
                f"Original message: <code>{original_message_id}</code>"
            )
        )
        duplicate_text = (
            f'<a href="{duplicate_link}">Open duplicate media</a>'
            if duplicate_link
            else (
                f"Duplicate message: <code>{duplicate_message_id}</code>"
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
            log.exception("Duplicate alert could not be sent")

    async def _message_link(
        self,
        chat_id: int,
        message_id: int,
    ) -> str | None:
        try:
            entity = await self.client.get_entity(chat_id)
            username = getattr(entity, "username", None)
            if username:
                return f"https://t.me/{username}/{message_id}"

            raw = str(abs(chat_id))
            if raw.startswith("100"):
                return f"https://t.me/c/{raw[3:]}/{message_id}"
        except Exception:
            log.exception(
                "Message link generation failed: %s/%s",
                chat_id,
                message_id,
            )
        return None

    async def publish_pending(self):
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
                log.info(
                    "Database media published: queue=%s destination=%s",
                    queue_id,
                    row["destination_chat_id"],
                )
            except Exception as exc:
                await self.db.mark_failed(queue_id, exc)
                log.exception(
                    "Scheduled publishing failed: queue=%s",
                    queue_id,
                )
