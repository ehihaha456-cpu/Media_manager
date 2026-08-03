from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from collections import defaultdict
from pathlib import Path
from uuid import uuid4

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from telegram import Bot
from telethon import TelegramClient
from telethon.errors import FloodWaitError, PeerIdInvalidError
from telethon.sessions import StringSession
from telethon.tl.types import (
    DocumentAttributeAudio,
    DocumentAttributeFilename,
    DocumentAttributeVideo,
)

from .crypto import decrypt_text
from .media import media_kind, sha256_file

log = logging.getLogger(__name__)

POLL_SECONDS = 2
INITIAL_RECENT_MESSAGES = 50
NEW_MEDIA_GROUP_WINDOW = 3
HISTORY_PAGE_SIZE = 100
HISTORY_MEDIA_BATCH_SIZE = 10
HISTORY_PROGRESS_EVERY = 100
UPLOAD_FLOOD_RETRIES = 3
ENTITY_CACHE_LIMIT = 256


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
        self.source_history_tasks: dict[int, asyncio.Task] = {}
        self.entity_cache: dict[int, object] = {}

    async def start(self) -> None:
        settings = await self.db.get_settings()
        if not settings["service_enabled"] or self.running:
            return

        if not settings.get("database_chat_active"):
            raise RuntimeError("Enable a Database chat first")
        if not settings.get("active_source_chat_ids"):
            raise RuntimeError("Select at least one Source chat")
        if not settings.get("active_destination_chat_ids"):
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
        interval_seconds = int(
            settings.get(
                "publish_interval_seconds",
                int(settings.get(
                    "publish_interval_minutes",
                    60,
                )) * 60,
            )
        )

        self.scheduler.add_job(
            self.publish_pending,
            "interval",
            seconds=max(1, interval_seconds),
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
            settings["active_source_chat_ids"],
            settings["database_chat_id"],
        )

        selected_sources = {
            int(chat_id)
            for chat_id in settings["active_source_chat_ids"]
        }
        for scan in await self.db.pending_source_history_scans():
            chat_id = int(scan["chat_id"])
            if chat_id in selected_sources:
                self._start_source_history_task(chat_id)

    async def stop(self) -> None:
        self.running = False

        for task in self.source_history_tasks.values():
            task.cancel()
        if self.source_history_tasks:
            await asyncio.gather(
                *self.source_history_tasks.values(),
                return_exceptions=True,
            )
        self.source_history_tasks.clear()

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

        self.entity_cache.clear()
        log.info("Media runtime stopped")

    async def restart(self) -> None:
        await self.stop()
        settings = await self.db.get_settings()
        if settings["service_enabled"]:
            await self.start()


    async def register_source_history_scan(
        self,
        chat_id: int,
        *,
        force: bool = False,
    ) -> None:
        chat_id = int(chat_id)
        existing = await self.db.get_source_history_scan(chat_id)

        if force:
            task = self.source_history_tasks.pop(chat_id, None)
            if task and not task.done():
                task.cancel()
                await asyncio.gather(
                    task,
                    return_exceptions=True,
                )

            await self.db.delete_source_history_scan(chat_id)
            await self.db.reset_chat_offset(chat_id)
            existing = None

        if existing and existing.get("status") in {
            "pending_count",
            "counting",
            "pending",
            "scanning",
            "completed",
        }:
            if (
                self.running
                and existing.get("status") != "completed"
            ):
                self._start_source_history_task(chat_id)
            return

        await self.db.upsert_source_history_scan(
            chat_id,
            status="pending_count",
            cursor_message_id=0,
            processed=0,
            uploaded=0,
            duplicates=0,
            failed=0,
            total_media=0,
            videos=0,
            photos=0,
            audio=0,
            files=0,
        )

        if self.running and self.client and self.client.is_connected():
            self._start_source_history_task(chat_id)

    async def remove_source_history_scan(
        self,
        chat_id: int,
    ) -> None:
        chat_id = int(chat_id)
        task = self.source_history_tasks.pop(chat_id, None)
        if task and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

        await self.db.delete_source_history_scan(chat_id)

    def _start_source_history_task(
        self,
        chat_id: int,
    ) -> None:
        chat_id = int(chat_id)
        current = self.source_history_tasks.get(chat_id)

        if current and not current.done():
            return

        task = asyncio.create_task(
            self._run_source_history_scan(chat_id),
            name=f"source-history-{chat_id}",
        )
        self.source_history_tasks[chat_id] = task

        def cleanup(_task):
            self.source_history_tasks.pop(chat_id, None)

        task.add_done_callback(cleanup)

    async def _count_source_history(
        self,
        chat_id: int,
    ) -> dict:
        counts = {
            "video": 0,
            "photo": 0,
            "audio": 0,
            "file": 0,
        }

        entity = await self.client.get_entity(chat_id)
        chat_name = (
            getattr(entity, "title", None)
            or getattr(entity, "username", None)
            or str(chat_id)
        )

        await self.db.upsert_source_history_scan(
            chat_id,
            status="counting",
            chat_name=chat_name,
        )

        async for message in self.client.iter_messages(
            entity,
            reverse=True,
        ):
            kind = media_kind(message)
            if kind in counts:
                counts[kind] += 1

        total_media = sum(counts.values())

        await self.db.upsert_source_history_scan(
            chat_id,
            status="pending",
            chat_name=chat_name,
            total_media=total_media,
            videos=counts["video"],
            photos=counts["photo"],
            audio=counts["audio"],
            files=counts["file"],
        )

        media_lines = []
        if counts["video"]:
            media_lines.append(
                f"🎬 Videos: <b>{counts['video']}</b>"
            )
        if counts["photo"]:
            media_lines.append(
                f"🖼 Images: <b>{counts['photo']}</b>"
            )
        if counts["audio"]:
            media_lines.append(
                f"🎵 Audio: <b>{counts['audio']}</b>"
            )
        if counts["file"]:
            media_lines.append(
                f"📁 Files: <b>{counts['file']}</b>"
            )

        await self.alert_bot.send_message(
            chat_id=self.owner_id,
            text=(
                "🔎 <b>Source Full History Scan</b>\n\n"
                f"Source: <b>{chat_name}</b>\n"
                f"Chat ID: <code>{chat_id}</code>\n\n"
                + ("\n".join(media_lines) or "No media found")
                + f"\n\nTotal: <b>{total_media}</b>\n\n"
                "Status: Full history processing started.\n"
                "Order: Oldest → Newest"
            ),
            parse_mode="HTML",
        )

        return {
            "chat_name": chat_name,
            "total_media": total_media,
            **counts,
        }

    def _history_media_batches(
        self,
        messages: list,
    ) -> list[list]:
        batches: list[list] = []
        current: list = []
        current_grouped_id = None

        for message in messages:
            if not media_kind(message):
                continue

            grouped_id = getattr(message, "grouped_id", None)

            if (
                current
                and len(current) >= HISTORY_MEDIA_BATCH_SIZE
                and (
                    grouped_id is None
                    or grouped_id != current_grouped_id
                )
            ):
                batches.append(current)
                current = []

            current.append(message)
            current_grouped_id = grouped_id

        if current:
            batches.append(current)

        return batches

    async def _run_source_history_scan(
        self,
        chat_id: int,
    ) -> None:
        if not self.client or not self.client.is_connected():
            return

        try:
            scan = await self.db.get_source_history_scan(chat_id)
            if not scan:
                return

            if scan.get("status") in {
                "pending_count",
                "counting",
            }:
                await self._count_source_history(chat_id)
                scan = await self.db.get_source_history_scan(chat_id)
                if not scan:
                    return

            chat_name = scan.get("chat_name", str(chat_id))
            total_media = int(scan.get("total_media", 0))
            cursor = int(scan.get("cursor_message_id", 0))
            processed = int(scan.get("processed", 0))
            uploaded = int(scan.get("uploaded", 0))
            duplicates = int(scan.get("duplicates", 0))
            failed = int(scan.get("failed", 0))

            await self.db.upsert_source_history_scan(
                chat_id,
                status="scanning",
            )

            entity = await self.client.get_entity(chat_id)

            while self.running:
                page = await self.client.get_messages(
                    entity,
                    min_id=cursor,
                    limit=HISTORY_PAGE_SIZE,
                    reverse=True,
                )
                page = list(page)

                if not page:
                    break

                page.sort(key=lambda message: int(message.id))

                for batch in self._history_media_batches(page):
                    notification_key = (
                        await self._register_media_batch(
                            chat_id,
                            batch,
                        )
                    )

                    for message in batch:
                        message_id = int(message.id)
                        kind = media_kind(message)

                        try:
                            before_bucket = (
                                self.pending_notifications.get(
                                    notification_key,
                                    {},
                                )
                            )
                            before_uploaded = int(
                                before_bucket.get("uploaded", 0)
                            )
                            before_duplicates = int(
                                before_bucket.get("duplicate", 0)
                            )

                            result = await self._process_source_message(
                                chat_id,
                                message,
                                kind,
                                notification_key=notification_key,
                            )

                            if result is False:
                                raise RuntimeError(
                                    "Source media processing did not complete"
                                )

                            processed += 1

                            after_bucket = (
                                self.pending_notifications.get(
                                    notification_key,
                                    {},
                                )
                            )
                            uploaded += max(
                                0,
                                int(after_bucket.get("uploaded", 0))
                                - before_uploaded,
                            )
                            duplicates += max(
                                0,
                                int(after_bucket.get("duplicate", 0))
                                - before_duplicates,
                            )

                        except asyncio.CancelledError:
                            raise
                        except Exception as exc:
                            failed += 1
                            source_link = await self._message_link(
                                chat_id,
                                message_id,
                            )
                            await self._update_notification_result(
                                notification_key,
                                failed=1,
                                failed_item={
                                    "message_id": message_id,
                                    "kind": kind or "unknown",
                                    "reason": (
                                        f"{type(exc).__name__}: {str(exc)}"
                                    )[:300],
                                    "link": source_link,
                                },
                            )
                            log.exception(
                                "Full-history media failed: "
                                "chat=%s message=%s",
                                chat_id,
                                message_id,
                            )

                        cursor = message_id
                        await self.db.upsert_source_history_scan(
                            chat_id,
                            status="scanning",
                            cursor_message_id=cursor,
                            processed=processed,
                            uploaded=uploaded,
                            duplicates=duplicates,
                            failed=failed,
                        )

                        if (
                            processed > 0
                            and processed % HISTORY_PROGRESS_EVERY == 0
                        ):
                            await self.alert_bot.send_message(
                                chat_id=self.owner_id,
                                text=(
                                    "⏳ <b>Source History Progress</b>\n\n"
                                    f"Source: <b>{chat_name}</b>\n"
                                    f"Processed: <b>{processed} / "
                                    f"{total_media}</b>\n"
                                    f"Uploaded: <b>{uploaded}</b>\n"
                                    f"Duplicates: <b>{duplicates}</b>\n"
                                    f"Failed: <b>{failed}</b>\n\n"
                                    "Status: Oldest → Newest"
                                ),
                                parse_mode="HTML",
                            )

                cursor = max(
                    cursor,
                    max(int(message.id) for message in page),
                )
                await self.db.upsert_source_history_scan(
                    chat_id,
                    cursor_message_id=cursor,
                )

                if len(page) < HISTORY_PAGE_SIZE:
                    break

            await self.db.set_chat_offset(chat_id, cursor)
            await self.db.upsert_source_history_scan(
                chat_id,
                status="completed",
                cursor_message_id=cursor,
                processed=processed,
                uploaded=uploaded,
                duplicates=duplicates,
                failed=failed,
                completed_at=datetime.now(timezone.utc),
            )

            await self.alert_bot.send_message(
                chat_id=self.owner_id,
                text=(
                    "✅ <b>Source History Completed</b>\n\n"
                    f"Source: <b>{chat_name}</b>\n"
                    f"Processed: <b>{processed}</b>\n"
                    f"Uploaded to Database: <b>{uploaded}</b>\n"
                    f"Duplicates: <b>{duplicates}</b>\n"
                    f"Failed: <b>{failed}</b>\n\n"
                    "Live monitoring is now active."
                ),
                parse_mode="HTML",
            )

        except asyncio.CancelledError:
            await self.db.upsert_source_history_scan(
                chat_id,
                status="pending",
            )
            raise
        except Exception as exc:
            await self.db.upsert_source_history_scan(
                chat_id,
                status="pending",
                last_error=str(exc)[:1000],
            )
            log.exception(
                "Full source history scan failed: chat=%s",
                chat_id,
            )

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

        database_chat_id = int(settings["database_chat_id"])

        for source_chat_id in settings["active_source_chat_ids"]:
            source_chat_id = int(source_chat_id)
            if source_chat_id == database_chat_id:
                continue

            scan = await self.db.get_source_history_scan(
                source_chat_id
            )
            if scan and scan.get("status") in {
                "pending_count",
                "counting",
                "pending",
                "scanning",
            }:
                self._start_source_history_task(source_chat_id)
                continue

            await self._poll_source_chat(source_chat_id)

        if settings.get("database_chat_active"):
            await self._poll_database_chat(database_chat_id)



    async def _poll_database_chat(
        self,
        chat_id: int,
    ) -> None:
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
    
        batches: list[list] = []
        album_map: dict[int, list] = {}
        single_batch: list = []
    
        for message in messages:
            message_id = int(message.id)
            kind = media_kind(message)
    
            if not kind:
                if single_batch:
                    batches.append(single_batch)
                    single_batch = []
    
                grouped_id = getattr(message, "grouped_id", None)
                if grouped_id and int(grouped_id) in album_map:
                    batches.append(album_map.pop(int(grouped_id)))
    
                await self.db.set_chat_offset(chat_id, message_id)
                continue
    
            # Media uploaded by the runtime from a Source chat was already
            # registered before this Database scanner sees it.
            existing_message = await self.db.find_by_database_message(
                chat_id,
                message_id,
            )
            if existing_message:
                await self.db.set_chat_offset(chat_id, message_id)
                continue
    
            grouped_id = getattr(message, "grouped_id", None)
            if grouped_id:
                album_map.setdefault(int(grouped_id), []).append(message)
            else:
                single_batch.append(message)
    
        if single_batch:
            batches.append(single_batch)
        batches.extend(album_map.values())
    
        batches.sort(
            key=lambda batch: min(int(item.id) for item in batch)
        )
    
        for batch in batches:
            batch.sort(key=lambda item: int(item.id))
            notification_key = await self._register_media_batch(
                chat_id,
                batch,
            )
    
            for message in batch:
                message_id = int(message.id)
                kind = media_kind(message)
    
                try:
                    await self._process_database_message(
                        chat_id,
                        message,
                        kind,
                        notification_key=notification_key,
                    )
                    await self.db.set_chat_offset(chat_id, message_id)
                except Exception as exc:
                    source_link = await self._message_link(
                        chat_id,
                        message_id,
                    )
                    await self._update_notification_result(
                        notification_key,
                        failed=1,
                        failed_item={
                            "message_id": message_id,
                            "kind": kind or "unknown",
                            "reason": (
                                f"{type(exc).__name__}: {str(exc)}"
                            )[:300],
                            "link": source_link,
                        },
                    )
                    log.exception(
                        "Database media scan failed: chat=%s message=%s",
                        chat_id,
                        message_id,
                    )
                    return
    
    async def _process_database_message(
        self,
        database_chat_id: int,
        message,
        kind: str,
        *,
        notification_key: str,
    ) -> None:
        settings = await self.db.get_settings()
        temp = self.temp_dir / f"database_{uuid4().hex}"
    
        async with self.processing_lock:
            try:
                downloaded = await message.download_media(file=str(temp))
                if not downloaded:
                    raise RuntimeError(
                        "Database media download returned no file"
                    )
    
                path = Path(downloaded)
                digest = await asyncio.to_thread(sha256_file, path)
                existing = await self.db.find_by_hash(digest)
    
                if existing:
                    original_chat_id = int(
                        existing.get("database_chat_id")
                        or existing.get("source_chat_id")
                    )
                    original_message_id = int(
                        existing.get("database_message_id")
                        or existing.get("source_message_id")
                    )
    
                    # The current message is already the canonical original.
                    if (
                        original_chat_id == int(database_chat_id)
                        and original_message_id == int(message.id)
                    ):
                        current_link = await self._message_link(
                            database_chat_id,
                            int(message.id),
                        )
                        file_index = self.pending_notifications[
                            notification_key
                        ]["file_results"][int(message.id)]["index"]
                        await self._update_notification_result(
                            notification_key,
                            processed=1,
                            processed_pair={
                                "index": file_index,
                                "source_link": current_link,
                                "database_link": current_link,
                            },
                            file_result={
                                "message_id": int(message.id),
                                "status": "processed",
                                "source_link": current_link,
                                "database_link": current_link,
                            },
                        )
                        return
    
                    original_message = None
                    try:
                        original_message = await self.client.get_messages(
                            original_chat_id,
                            ids=original_message_id,
                        )
                    except Exception:
                        log.exception(
                            "Database original verification failed: "
                            "chat=%s message=%s",
                            original_chat_id,
                            original_message_id,
                        )
    
                    original_is_valid = bool(
                        original_message
                        and media_kind(original_message)
                    )
    
                    if not original_is_valid:
                        # The hash record points to deleted/missing Telegram
                        # media. It must not cause the current media to be
                        # deleted as a duplicate.
                        await self.db.delete_media_record(
                            int(existing["_id"])
                        )
    
                        inserted, record = (
                            await self.db.register_database_media(
                                sha256=digest,
                                kind=kind,
                                size=path.stat().st_size,
                                database_chat_id=database_chat_id,
                                database_message_id=int(message.id),
                                caption=message.message or None,
                                source_chat_id=database_chat_id,
                                source_message_id=int(message.id),
                            )
                        )
                        if not inserted:
                            raise RuntimeError(
                                "Could not replace stale media record"
                            )
    
                        queued = 0
                        for destination in settings[
                            "destination_chat_ids"
                        ]:
                            destination_id = int(destination)
                            if destination_id != database_chat_id:
                                await self.db.enqueue(
                                    int(record["id"]),
                                    destination_id,
                                )
                                queued += 1
    
                        current_link = await self._message_link(
                            database_chat_id,
                            int(message.id),
                        )
                        file_index = self.pending_notifications[
                            notification_key
                        ]["file_results"][int(message.id)]["index"]
                        await self._update_notification_result(
                            notification_key,
                            uploaded=1,
                            queued=queued,
                            processed=1,
                            processed_pair={
                                "index": file_index,
                                "source_link": current_link,
                                "database_link": current_link,
                            },
                            file_result={
                                "message_id": int(message.id),
                                "status": "processed",
                                "source_link": current_link,
                                "database_link": current_link,
                            },
                        )
                        return
    
                    original_link = await self._message_link(
                        original_chat_id,
                        original_message_id,
                    )
                    duplicate_link = await self._message_link(
                        database_chat_id,
                        int(message.id),
                    )
    
                    if settings.get("delete_duplicates"):
                        entity = await self.client.get_entity(
                            database_chat_id
                        )
                        await self.client.delete_messages(
                            entity,
                            [int(message.id)],
                            revoke=True,
                        )
    
                    await self._update_notification_result(
                        notification_key,
                        duplicate=1,
                        processed=1,
                        duplicate_pair={
                            "original_link": original_link,
                            "duplicate_link": duplicate_link,
                            "original_chat_id": original_chat_id,
                            "original_message_id": original_message_id,
                            "duplicate_chat_id": database_chat_id,
                            "duplicate_message_id": int(message.id),
                        },
                    )
                    return
    
                inserted, record = await self.db.register_database_media(
                    sha256=digest,
                    kind=kind,
                    size=path.stat().st_size,
                    database_chat_id=database_chat_id,
                    database_message_id=int(message.id),
                    caption=message.message or None,
                    source_chat_id=database_chat_id,
                    source_message_id=int(message.id),
                )
                if not inserted:
                    raise RuntimeError(
                        "Database media registration race detected"
                    )
    
                queued = 0
                for destination in settings["active_destination_chat_ids"]:
                    destination_id = int(destination)
                    if destination_id != database_chat_id:
                        await self.db.enqueue(
                            int(record["id"]),
                            destination_id,
                        )
                        queued += 1
    
                current_link = await self._message_link(
                    database_chat_id,
                    int(message.id),
                )
                file_index = self.pending_notifications[
                    notification_key
                ]["file_results"][int(message.id)]["index"]
                await self._update_notification_result(
                    notification_key,
                    uploaded=1,
                    queued=queued,
                    processed=1,
                    processed_pair={
                        "index": file_index,
                        "source_link": current_link,
                        "database_link": current_link,
                    },
                    file_result={
                        "message_id": int(message.id),
                        "status": "processed",
                        "source_link": current_link,
                        "database_link": current_link,
                    },
                )
            finally:
                Path(temp).unlink(missing_ok=True)
    
    async def _poll_source_chat(
        self,
        chat_id: int,
    ) -> None:
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

        # Group Telegram albums by grouped_id. Non-album media arriving in
        # the same polling cycle are grouped into one burst notification.
        batches: list[list] = []
        album_map: dict[int, list] = {}
        single_batch: list = []

        for message in messages:
            kind = media_kind(message)

            if not kind:
                # Flush pending non-album burst before committing text/service
                # messages so ordering stays correct.
                if single_batch:
                    batches.append(single_batch)
                    single_batch = []

                grouped_id = getattr(message, "grouped_id", None)
                if grouped_id and int(grouped_id) in album_map:
                    batches.append(album_map.pop(int(grouped_id)))

                await self.db.set_chat_offset(
                    chat_id,
                    int(message.id),
                )
                continue

            grouped_id = getattr(message, "grouped_id", None)

            if grouped_id:
                album_map.setdefault(
                    int(grouped_id),
                    [],
                ).append(message)
            else:
                single_batch.append(message)

        if single_batch:
            batches.append(single_batch)

        batches.extend(album_map.values())

        # Ensure oldest batch is processed first.
        batches.sort(
            key=lambda batch: min(
                int(item.id) for item in batch
            )
        )

        for batch in batches:
            batch.sort(key=lambda item: int(item.id))

            notification_key = await self._register_media_batch(
                chat_id,
                batch,
            )

            for message in batch:
                message_id = int(message.id)
                kind = media_kind(message)

                try:
                    result = await self._process_source_message(
                        chat_id,
                        message,
                        kind,
                        notification_key=notification_key,
                    )

                    if result is False:
                        return

                    await self.db.set_chat_offset(
                        chat_id,
                        message_id,
                    )

                except Exception as exc:
                    source_link = await self._message_link(
                        chat_id,
                        message_id,
                    )

                    await self._update_notification_result(
                        notification_key,
                        failed=1,
                        failed_item={
                            "message_id": message_id,
                            "kind": kind or "unknown",
                            "reason": (
                                f"{type(exc).__name__}: {str(exc)}"
                            )[:300],
                            "link": source_link,
                        },
                    )

                    log.exception(
                        "Source media failed; retained for retry: "
                        "chat=%s message=%s",
                        chat_id,
                        message_id,
                    )
                    return

    @staticmethod
    def _document_attributes(message, kind: str) -> tuple[list, str | None]:
        document = getattr(message, "document", None)
        mime_type = getattr(document, "mime_type", None) if document else None
        source_attributes = list(getattr(document, "attributes", []) or [])
        attributes: list = []

        filename = None
        for attribute in source_attributes:
            if isinstance(attribute, DocumentAttributeFilename):
                filename = attribute.file_name
                break

        if not filename:
            file_obj = getattr(message, "file", None)
            filename = getattr(file_obj, "name", None)

        if filename:
            attributes.append(DocumentAttributeFilename(str(filename)))

        if kind == "video":
            video_attribute = next(
                (
                    attribute
                    for attribute in source_attributes
                    if isinstance(attribute, DocumentAttributeVideo)
                ),
                None,
            )
            if video_attribute:
                attributes.append(
                    DocumentAttributeVideo(
                        duration=max(0.0, float(video_attribute.duration or 0)),
                        w=max(0, int(video_attribute.w or 0)),
                        h=max(0, int(video_attribute.h or 0)),
                        round_message=bool(
                            getattr(video_attribute, "round_message", False)
                        ),
                        supports_streaming=True,
                        nosound=bool(
                            getattr(video_attribute, "nosound", False)
                        ),
                    )
                )

        elif kind == "audio":
            audio_attribute = next(
                (
                    attribute
                    for attribute in source_attributes
                    if isinstance(attribute, DocumentAttributeAudio)
                ),
                None,
            )
            if audio_attribute:
                attributes.append(
                    DocumentAttributeAudio(
                        duration=max(0, int(audio_attribute.duration or 0)),
                        voice=bool(getattr(audio_attribute, "voice", False)),
                        title=getattr(audio_attribute, "title", None),
                        performer=getattr(audio_attribute, "performer", None),
                        waveform=getattr(audio_attribute, "waveform", None),
                    )
                )

        return attributes, mime_type

    async def _download_media_thumbnail(
        self,
        message,
        base_path: Path,
    ) -> Path | None:
        document = getattr(message, "document", None)
        thumbs = list(getattr(document, "thumbs", []) or []) if document else []
        if not thumbs:
            return None

        thumb_base = Path(str(base_path) + "_thumb.jpg")
        try:
            downloaded = await message.download_media(
                file=str(thumb_base),
                thumb=-1,
            )
            if not downloaded:
                return None
            path = Path(downloaded)
            return path if path.exists() and path.stat().st_size > 0 else None
        except Exception:
            log.exception(
                "Could not preserve media thumbnail: chat=%s message=%s",
                getattr(message, "chat_id", None),
                getattr(message, "id", None),
            )
            return None

    async def _resolve_peer(
        self,
        chat_id: int,
        *,
        force_refresh: bool = False,
    ):
        if not self.client:
            raise RuntimeError("Telegram client is not connected")

        chat_id = int(chat_id)
        if force_refresh:
            self.entity_cache.pop(chat_id, None)

        cached = self.entity_cache.get(chat_id)
        if cached is not None:
            return cached

        try:
            peer = await self.client.get_input_entity(chat_id)
        except Exception:
            # Refresh every dialog and match the exact Telegram dialog ID.
            # This restores the access hash for private groups/channels and
            # avoids reusing a stale InputPeer after reconnect/deploy.
            peer = None
            async for dialog in self.client.iter_dialogs(limit=None):
                if int(dialog.id) == chat_id:
                    peer = await self.client.get_input_entity(dialog.entity)
                    break

            if peer is None:
                raise RuntimeError(
                    f"Telegram account cannot access chat {chat_id}"
                )

        if len(self.entity_cache) >= ENTITY_CACHE_LIMIT:
            self.entity_cache.pop(next(iter(self.entity_cache)))
        self.entity_cache[chat_id] = peer
        return peer

    async def _send_file_with_retry(
        self,
        target,
        file,
        *,
        target_chat_id: int | None = None,
        **kwargs,
    ):
        if not self.client:
            raise RuntimeError("Telegram client is not connected")

        current_target = target
        peer_refreshed = False

        for attempt in range(UPLOAD_FLOOD_RETRIES + 1):
            try:
                return await self.client.send_file(
                    current_target,
                    file,
                    **kwargs,
                )
            except PeerIdInvalidError:
                if target_chat_id is None or peer_refreshed:
                    raise

                peer_refreshed = True
                chat_id = int(target_chat_id)
                self.entity_cache.pop(chat_id, None)
                log.warning(
                    "Invalid/stale Telegram peer; refreshing target chat %s",
                    chat_id,
                )
                current_target = await self._resolve_peer(
                    chat_id,
                    force_refresh=True,
                )
                continue
            except FloodWaitError as exc:
                if attempt >= UPLOAD_FLOOD_RETRIES:
                    raise
                wait_seconds = max(1, int(exc.seconds))
                log.warning(
                    "Telegram FloodWait during upload; waiting %ss "
                    "(attempt %s/%s)",
                    wait_seconds,
                    attempt + 1,
                    UPLOAD_FLOOD_RETRIES,
                )
                await asyncio.sleep(wait_seconds)

    async def _try_server_side_copy(
        self,
        target_chat_id: int,
        message,
        kind: str,
        caption: str | None,
    ):
        if not self.client or not getattr(message, "media", None):
            return None

        target = await self._resolve_peer(target_chat_id)
        try:
            return await self._send_file_with_retry(
                target,
                message.media,
                target_chat_id=target_chat_id,
                caption=caption,
                supports_streaming=(kind == "video"),
            )
        except Exception:
            # Protected/restricted chats and expired file references can reject
            # server-side reuse. The caller will use the existing local
            # download/re-upload path without changing behavior.
            log.info(
                "Fast server-side copy unavailable; using upload fallback: "
                "source=%s/%s target=%s",
                getattr(message, "chat_id", None),
                getattr(message, "id", None),
                target_chat_id,
            )
            return None

    async def _upload_downloaded_media(
        self,
        target_chat_id: int,
        path: Path,
        source_message,
        kind: str,
        caption: str | None,
    ):
        if not self.client:
            raise RuntimeError("Telegram client is not connected")

        target = await self._resolve_peer(target_chat_id)
        attributes, mime_type = self._document_attributes(
            source_message,
            kind,
        )
        thumb_path: Path | None = None

        try:
            if kind == "video":
                thumb_path = await self._download_media_thumbnail(
                    source_message,
                    path,
                )

            kwargs = {
                "caption": caption,
                "supports_streaming": kind == "video",
                "force_document": kind == "file",
            }
            if attributes:
                kwargs["attributes"] = attributes
            if mime_type:
                kwargs["mime_type"] = mime_type
            if thumb_path:
                kwargs["thumb"] = thumb_path

            return await self._send_file_with_retry(
                target,
                path,
                target_chat_id=target_chat_id,
                **kwargs,
            )
        finally:
            if thumb_path:
                thumb_path.unlink(missing_ok=True)

    async def _process_source_message(
        self,
        source_chat_id: int,
        message,
        kind: str,
        *,
        notification_key: str,
    ) -> bool:
        if not self.client:
            return False

        settings = await self.db.get_settings()
        database_chat_id = int(settings["database_chat_id"])


        temp = self.temp_dir / f"source_{uuid4().hex}"

        async with self.processing_lock:
            try:
                # For normal chats, Telegram can copy the existing media on
                # its own servers. Run that copy in parallel with the local
                # download/hash needed for duplicate detection. Restricted
                # media automatically falls back to the original upload path.
                fast_copy_task = asyncio.create_task(
                    self._try_server_side_copy(
                        database_chat_id,
                        message,
                        kind,
                        message.message or None,
                    )
                )

                downloaded = await message.download_media(file=str(temp))
                if not downloaded:
                    fast_copy_task.cancel()
                    await asyncio.gather(
                        fast_copy_task,
                        return_exceptions=True,
                    )
                    raise RuntimeError(
                        "Source media download returned no file"
                    )

                path = Path(downloaded)
                digest = await asyncio.to_thread(sha256_file, path)

                sent = await fast_copy_task
                if sent is None:
                    sent = await self._upload_downloaded_media(
                        database_chat_id,
                        path,
                        message,
                        kind,
                        message.message or None,
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
                    original_chat_id = int(
                        record.get("database_chat_id")
                        or record.get("source_chat_id")
                    )
                    original_message_id = int(
                        record.get("database_message_id")
                        or record.get("source_message_id")
                    )

                    original_message = None
                    try:
                        original_message = await self.client.get_messages(
                            original_chat_id,
                            ids=original_message_id,
                        )
                    except Exception:
                        log.exception(
                            "Original duplicate candidate could not be fetched: "
                            "chat=%s message=%s",
                            original_chat_id,
                            original_message_id,
                        )

                    original_is_valid = bool(
                        original_message
                        and media_kind(original_message)
                    )

                    if not original_is_valid:
                        # Stale database entry: the original Telegram media no
                        # longer exists. Remove the broken hash record and
                        # register this newly uploaded Database copy as the new
                        # canonical original.
                        await self.db.media.delete_one(
                            {"_id": record["_id"]}
                        )

                        inserted_again, fresh_record = (
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

                        if not inserted_again:
                            raise RuntimeError(
                                "Stale duplicate record could not be replaced"
                            )

                        for destination in settings["active_destination_chat_ids"]:
                            destination_id = int(destination)
                            if destination_id != database_chat_id:
                                await self.db.enqueue(
                                    int(fresh_record["id"]),
                                    destination_id,
                                )

                        source_link = await self._message_link(
                            source_chat_id,
                            int(message.id),
                        )
                        database_link = await self._message_link(
                            database_chat_id,
                            int(sent.id),
                        )
                        file_index = self.pending_notifications[
                            notification_key
                        ]["file_results"][int(message.id)]["index"]

                        await self._update_notification_result(
                            notification_key,
                            uploaded=1,
                            queued=1,
                            processed=1,
                            processed_pair={
                                "index": file_index,
                                "source_link": source_link,
                                "database_link": database_link,
                            },
                            file_result={
                                "message_id": int(message.id),
                                "status": "processed",
                                "source_link": source_link,
                                "database_link": database_link,
                            },
                        )

                        log.info(
                            "Stale duplicate record replaced with new original: "
                            "source=%s/%s database=%s/%s",
                            source_chat_id,
                            message.id,
                            database_chat_id,
                            sent.id,
                        )
                        return True

                    original_link = await self._message_link(
                        original_chat_id,
                        original_message_id,
                    )
                    duplicate_link = await self._message_link(
                        source_chat_id,
                        int(message.id),
                    )

                    if settings.get("delete_duplicates"):
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
                                "Duplicate Database copy could not be deleted"
                            )

                    file_index = self.pending_notifications[
                        notification_key
                    ]["file_results"][int(message.id)]["index"]

                    await self._update_notification_result(
                        notification_key,
                        duplicate=1,
                        processed=1,
                        duplicate_pair={
                            "index": file_index,
                            "original_link": original_link,
                            "duplicate_link": duplicate_link,
                            "original_chat_id": original_chat_id,
                            "original_message_id": original_message_id,
                            "duplicate_chat_id": source_chat_id,
                            "duplicate_message_id": int(message.id),
                        },
                        file_result={
                            "message_id": int(message.id),
                            "status": "duplicate",
                            "source_link": duplicate_link,
                        },
                    )
                    return True

                for destination in settings["active_destination_chat_ids"]:
                    destination_id = int(destination)
                    if destination_id != database_chat_id:
                        await self.db.enqueue(
                            int(record["id"]),
                            destination_id,
                        )

                source_link = await self._message_link(
                    source_chat_id,
                    int(message.id),
                )
                database_link = await self._message_link(
                    database_chat_id,
                    int(sent.id),
                )
                file_index = self.pending_notifications[
                    notification_key
                ]["file_results"][int(message.id)]["index"]

                await self._update_notification_result(
                    notification_key,
                    uploaded=1,
                    queued=1,
                    processed=1,
                    processed_pair={
                        "index": file_index,
                        "source_link": source_link,
                        "database_link": database_link,
                    },
                    file_result={
                        "message_id": int(message.id),
                        "status": "processed",
                        "source_link": source_link,
                        "database_link": database_link,
                    },
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
        messages: list,
    ) -> str:
        grouped_ids = {
            int(grouped_id)
            for grouped_id in (
                getattr(message, "grouped_id", None)
                for message in messages
            )
            if grouped_id is not None
        }

        if len(grouped_ids) == 1:
            grouped_id = next(iter(grouped_ids))
            return (
                f"album:{int(source_chat_id)}:"
                f"{grouped_id}"
            )

        first_id = min(int(message.id) for message in messages)
        last_id = max(int(message.id) for message in messages)
        return (
            f"burst:{int(source_chat_id)}:"
            f"{first_id}:{last_id}"
        )

    async def _register_media_batch(
        self,
        source_chat_id: int,
        messages: list,
    ) -> str:
        key = self._notification_key(
            source_chat_id,
            messages,
        )

        counts = defaultdict(int)
        for message in messages:
            kind = media_kind(message)
            if kind:
                counts[kind] += 1

        file_results = {}
        for index, message in enumerate(messages, start=1):
            message_id = int(message.id)
            file_results[message_id] = {
                "index": index,
                "message_id": message_id,
                "kind": media_kind(message) or "file",
                "status": "processing",
                "source_link": await self._message_link(
                    int(source_chat_id),
                    message_id,
                ),
                "database_link": None,
                "reason": None,
            }

        bucket = {
            "source_chat_id": int(source_chat_id),
            "counts": counts,
            "expected": sum(counts.values()),
            "processed": 0,
            "uploaded": 0,
            "duplicate": 0,
            "processed_pairs": [],
            "duplicate_pairs": [],
            "queued": 0,
            "failed": 0,
            "failed_items": [],
            "file_results": file_results,
            "message_id": None,
            "chat_name": None,
        }
        self.pending_notifications[key] = bucket

        # Send one Processing message before any item in the batch completes.
        await self._send_processing_notification(key)
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
        failed_item: dict | None = None,
        processed_pair: dict | None = None,
        duplicate_pair: dict | None = None,
        file_result: dict | None = None,
    ) -> None:
        bucket = self.pending_notifications.get(key)
        if not bucket:
            return

        # Persist dashboard counters for every completed media result.
        # The notification bucket alone is temporary and disappears after
        # the owner message is finalized, so it cannot power Statistics.
        await self.db.increment_activity(
            processed=processed,
            uploaded=uploaded,
            duplicates=duplicate,
            failed=failed,
        )

        bucket["processed"] += processed
        bucket["uploaded"] += uploaded
        bucket["duplicate"] += duplicate
        bucket["queued"] += queued
        bucket["failed"] += failed

        if failed_item:
            bucket.setdefault("failed_items", []).append(
                dict(failed_item)
            )
            message_id = int(failed_item.get("message_id", 0))
            item = bucket.get("file_results", {}).get(message_id)
            if item:
                item["status"] = "failed"
                item["reason"] = failed_item.get("reason")
                if failed_item.get("link"):
                    item["source_link"] = failed_item.get("link")

        if processed_pair:
            bucket.setdefault("processed_pairs", []).append(
                dict(processed_pair)
            )

        if duplicate_pair:
            bucket.setdefault("duplicate_pairs", []).append(
                dict(duplicate_pair)
            )

        if file_result:
            message_id = int(file_result.get("message_id", 0))
            item = bucket.get("file_results", {}).get(message_id)
            if item:
                item.update(dict(file_result))

        if (
            bucket["message_id"] is not None
            and bucket["processed"] + bucket["failed"]
            < bucket["expected"]
        ):
            await self._refresh_processing_notification(key)

        # If the initial message has already been sent and all detected media
        # finished processing, edit that same message immediately.
        if (
            bucket["message_id"] is not None
            and bucket["processed"] + bucket["failed"]
            >= bucket["expected"]
        ):
            await self._finalize_notification(key)

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

            raw_id = str(abs(int(chat_id)))
            if raw_id.startswith("100"):
                return (
                    f"https://t.me/c/{raw_id[3:]}/"
                    f"{int(message_id)}"
                )
        except Exception:
            log.exception(
                "Failed media link generation failed: "
                "chat=%s message=%s",
                chat_id,
                message_id,
            )

        return None

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

    def _file_link_text(
        self,
        link: str | None,
        label: str,
    ) -> str:
        if link:
            return f'<a href="{link}">{label}</a>'
        return label

    def _processing_file_lines(self, bucket: dict) -> list[str]:
        lines: list[str] = []
        items = sorted(
            bucket.get("file_results", {}).values(),
            key=lambda item: int(item.get("index", 0)),
        )

        for item in items:
            index = int(item.get("index", 0))
            source_text = self._file_link_text(
                item.get("source_link"),
                "📂 Original File",
            )
            status = item.get("status", "processing")
            icon = {
                "processed": "✅",
                "duplicate": "✅",
                "failed": "❌",
                "processing": "⏳",
            }.get(status, "⏳")
            lines.append(
                f"File {index}: {source_text} {icon}"
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
            "📂 <b>Processing Files</b>\n"
            + "\n".join(self._processing_file_lines(bucket))
            + "\n\n⏳ <b>Processing...</b>"
        )

        sent = await self.alert_bot.send_message(
            chat_id=self.owner_id,
            text=text,
            parse_mode="HTML",
        )
        bucket["message_id"] = int(sent.message_id)

    async def _refresh_processing_notification(
        self,
        key: str,
    ) -> None:
        bucket = self.pending_notifications.get(key)
        if not bucket or bucket.get("message_id") is None:
            return

        source_chat_id = int(bucket["source_chat_id"])
        chat_name = (
            bucket.get("chat_name")
            or await self._resolve_chat_name(source_chat_id)
        )
        lines = self._media_count_lines(bucket["counts"])
        total = sum(bucket["counts"].values())

        text = (
            "🆕 <b>New Media Detected</b>\n\n"
            f"Source: <b>{chat_name}</b>\n"
            f"Chat ID: <code>{source_chat_id}</code>\n\n"
            + "\n".join(lines)
            + f"\n\nTotal: <b>{total}</b>\n\n"
            "📂 <b>Processing Files</b>\n"
            + "\n".join(self._processing_file_lines(bucket))
            + "\n\n⏳ <b>Processing...</b>"
        )

        try:
            await self.alert_bot.edit_message_text(
                chat_id=self.owner_id,
                message_id=int(bucket["message_id"]),
                text=text,
                parse_mode="HTML",
                disable_web_page_preview=True,
            )
        except Exception as exc:
            if "Message is not modified" not in str(exc):
                log.exception(
                    "Could not refresh processing notification: key=%s",
                    key,
                )

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

        processed_pairs = bucket.get(
            "processed_pairs",
            [],
        )

        if processed_pairs:
            final_text += "\n\n📂 <b>Processed Files</b>"

            for pair in processed_pairs[:20]:
                index = int(pair.get("index", 0))
                source_text = self._file_link_text(
                    pair.get("source_link"),
                    "📂 Source Media",
                )
                database_text = self._file_link_text(
                    pair.get("database_link"),
                    "🗄 Database Media",
                )
                final_text += (
                    f"\nFile {index}: "
                    f"{source_text}    {database_text} ✅"
                )

            if len(processed_pairs) > 20:
                final_text += (
                    f"\n+{len(processed_pairs) - 20} "
                    "more processed files"
                )

        duplicate_pairs = bucket.get(
            "duplicate_pairs",
            [],
        )

        if duplicate_pairs:
            final_text += (
                "\n\n📂 <b>Duplicate Files</b>"
            )

            for fallback_index, pair in enumerate(
                duplicate_pairs[:20],
                start=1,
            ):
                index = int(
                    pair.get("index", fallback_index)
                )
                original_link = pair.get("original_link")
                duplicate_link = pair.get("duplicate_link")

                original_text = (
                    f'<a href="{original_link}">📂 Original Media</a>'
                    if original_link
                    else (
                        "📂 Original "
                        f"<code>{pair.get('original_message_id')}</code>"
                    )
                )
                duplicate_text = (
                    f'<a href="{duplicate_link}">🆕 Duplicate Media</a>'
                    if duplicate_link
                    else (
                        "🆕 Duplicate "
                        f"<code>{pair.get('duplicate_message_id')}</code>"
                    )
                )

                final_text += (
                    f"\nFile {index}: "
                    f"{original_text}    {duplicate_text}"
                )

            if len(duplicate_pairs) > 20:
                final_text += (
                    f"\n+{len(duplicate_pairs) - 20} "
                    "more duplicate pairs"
                )

        if bucket["failed"] > 0:
            final_text += (
                f"\nFailed: <b>{bucket['failed']}</b>"
            )

            failed_items = bucket.get(
                "failed_items",
                [],
            )

            if failed_items:
                final_text += (
                    "\n\n━━━━━━━━━━━━━━"
                    "\n\n❌ <b>Failed Media</b>"
                )

                for index, item in enumerate(
                    failed_items[:10],
                    start=1,
                ):
                    kind_name = str(
                        item.get("kind", "unknown")
                    ).title()
                    message_id = int(
                        item.get("message_id", 0)
                    )
                    reason = str(
                        item.get("reason", "Unknown error")
                    )
                    link = item.get("link")

                    final_text += (
                        f"\n\n<b>{index}.</b> "
                        f"{kind_name}"
                        f"\nMessage ID: "
                        f"<code>{message_id}</code>"
                        f"\nReason: "
                        f"<code>{reason}</code>"
                    )

                    if link:
                        final_text += (
                            f'\n🔗 <a href="{link}">'
                            "Open Source Media</a>"
                        )

                if len(failed_items) > 10:
                    final_text += (
                        f"\n\n+{len(failed_items) - 10} "
                        "more failed items"
                    )

        try:
            await self.alert_bot.edit_message_text(
                chat_id=self.owner_id,
                message_id=int(bucket["message_id"]),
                text=final_text,
                parse_mode="HTML",
                disable_web_page_preview=True,
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
        active_destinations = {
            int(value)
            for value in settings.get("active_destination_chat_ids", [])
        }
        rows = await self.db.pending(
            max(1, int(settings.get("publish_batch_size", 1)))
        )

        for row in rows:
            queue_id = int(row["queue_id"])
            temp_path: Path | None = None
            try:
                destination_id = int(row["destination_chat_id"])
                if destination_id not in active_destinations:
                    await self.db.publish_queue.update_one(
                        {"_id": queue_id},
                        {"$set": {
                            "status": "cancelled",
                            "last_error": "Destination is disabled or disconnected",
                        }},
                    )
                    continue

                message = None
                if row.get("database_chat_id") is not None:
                    message = await self.client.get_messages(
                        int(row["database_chat_id"]),
                        ids=int(row["database_message_id"]),
                    )
                if not message or not getattr(message, "media", None):
                    if row.get("source_chat_id") is not None:
                        message = await self.client.get_messages(
                            int(row["source_chat_id"]),
                            ids=int(row["source_message_id"]),
                        )

                if not message or not getattr(message, "media", None):
                    await self.db.publish_queue.update_one(
                        {"_id": queue_id},
                        {"$set": {
                            "status": "failed",
                            "last_error": "Database and Source media were not found",
                            "failed_at": datetime.now(timezone.utc),
                        }, "$inc": {"attempts": 1}},
                    )
                    continue

                kind = media_kind(message)
                try:
                    destination_peer = await self._resolve_peer(
                        destination_id
                    )
                    await self._send_file_with_retry(
                        destination_peer,
                        message.media,
                        target_chat_id=destination_id,
                        caption=row.get("caption") or None,
                        supports_streaming=(kind == "video"),
                    )
                except Exception:
                    temp_path = self.temp_dir / f"publish_{queue_id}_{uuid4().hex}"
                    downloaded = await message.download_media(file=str(temp_path))
                    if not downloaded:
                        raise RuntimeError("Media download fallback returned no file")
                    temp_path = Path(downloaded)
                    await self._upload_downloaded_media(
                        destination_id,
                        temp_path,
                        message,
                        kind,
                        row.get("caption") or None,
                    )

                await self.db.mark_published(queue_id)
                log.info("Published queued media: queue=%s destination=%s", queue_id, destination_id)
            except Exception as exc:
                await self.db.mark_failed(queue_id, exc)
                log.exception("Scheduled publishing failed: queue=%s", queue_id)
            finally:
                if temp_path:
                    temp_path.unlink(missing_ok=True)

