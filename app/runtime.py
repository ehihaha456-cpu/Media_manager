from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from html import escape
from collections import defaultdict
from pathlib import Path
from uuid import uuid4

from PIL import Image

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
DATABASE_SELF_UPLOAD_GRACE_SECONDS = 15
DATABASE_SELF_UPLOAD_MARKER = "\u2063"
DATABASE_UPLOAD_TOKEN_PREFIX = "\u2063\u2063"
DATABASE_UPLOAD_TOKEN_SUFFIX = "\u2063\u2063"
DATABASE_UPLOAD_TOKEN_ZERO = "\u200b"
DATABASE_UPLOAD_TOKEN_ONE = "\u200c"
DATABASE_UPLOAD_TOKEN_BITS = 128
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
        self.self_uploaded_database_ids: set[int] = set()
        self.reverse_index_task: asyncio.Task | None = None

    # Reverse-search fingerprint format/version.
    REVERSE_FP_VERSION = 3


    @staticmethod
    def _image_hashes(path_or_image) -> list[str]:
        """Create grouped multi-signal perceptual fingerprints."""
        import numpy as np
        image = path_or_image

        def dhash(img):
            img = img.convert("L").resize((17, 16), Image.Resampling.LANCZOS)
            arr = np.asarray(img, dtype=np.uint8)
            bits = (arr[:, :-1] > arr[:, 1:]).flatten()
            value = 0
            for bit in bits: value = (value << 1) | int(bit)
            return f"{value:032x}"

        def ahash(img):
            img = img.convert("L").resize((16, 16), Image.Resampling.LANCZOS)
            arr = np.asarray(img, dtype=np.float32)
            bits = (arr >= float(arr.mean())).flatten()
            value = 0
            for bit in bits: value = (value << 1) | int(bit)
            return f"{value:032x}"

        def phash(img):
            arr = np.asarray(
                img.convert("L").resize((32, 32), Image.Resampling.LANCZOS),
                dtype=np.float32,
            )
            x = np.arange(32, dtype=np.float32)
            c = np.cos(np.pi * (2*x[:, None] + 1) * x[None, :] / 64.0)
            c[0, :] *= 1.0 / np.sqrt(2.0)
            dct = (c @ arr @ c.T) / 16.0
            low = dct[:8, :8]
            med = float(np.median(low[1:, 1:]))
            bits = (low >= med).flatten()
            value = 0
            for bit in bits: value = (value << 1) | int(bit)
            return f"{value:016x}"

        w, h = image.size
        boxes = [
            ("full", (0, 0, w, h)),
            ("c90", (int(w*.05), int(h*.05), max(int(w*.95), 32), max(int(h*.95), 32))),
            ("center", (int(w*.15), int(h*.15), max(int(w*.85), 32), max(int(h*.85), 32))),
        ]
        out=[]
        for name, box in boxes:
            region=image.crop(box)
            out += [f"d:{name}:{dhash(region)}", f"a:{name}:{ahash(region)}", f"p:{name}:{phash(region)}"]
        return out

    @staticmethod
    def _hash_similarity(left: str, right: str) -> float:
        try:
            lp, rp = left.split(":"), right.split(":")
            if len(lp) != 3 or len(rp) != 3 or lp[:2] != rp[:2]:
                return 0.0
            distance=(int(lp[2],16)^int(rp[2],16)).bit_count()
            bits=64 if lp[0] in ("d","a") and len(lp[2]) <= 16 else 256 if lp[0] in ("d","a") else 64
            return max(0.0, 100.0*(bits-distance)/bits)
        except Exception:
            return 0.0

    @staticmethod
    def _group_hashes(hashes: list[str]) -> list[list[str]]:
        groups={}
        legacy=[]
        for value in hashes:
            value=str(value)
            if "|" in value and value.startswith("g"):
                group, fp=value.split("|",1)
                groups.setdefault(group,[]).append(fp)
            else:
                legacy.append(value)
        return list(groups.values()) if groups else ([legacy] if legacy else [])

    async def _video_duration(self, path: Path) -> float:
        try:
            proc=await asyncio.create_subprocess_exec(
                "ffprobe","-v","error","-show_entries","format=duration",
                "-of","default=noprint_wrappers=1:nokey=1",str(path),
                stdout=asyncio.subprocess.PIPE,stderr=asyncio.subprocess.DEVNULL)
            stdout,_=await proc.communicate()
            return max(0.0,float((stdout or b"0").decode().strip() or 0))
        except Exception:
            return 0.0

    async def _video_frame_hashes(self, path: Path, count: int = 16) -> list[str]:
        duration=await self._video_duration(path)
        frame_dir=self.temp_dir/f"reverse_frames_{uuid4().hex}"
        frame_dir.mkdir(parents=True,exist_ok=True)
        output=frame_dir/"frame_%03d.jpg"
        try:
            fps=max(0.01,float(count)/max(duration,1.0))
            proc=await asyncio.create_subprocess_exec(
                "ffmpeg","-hide_banner","-loglevel","error","-y","-i",str(path),
                "-vf",f"fps={fps},scale=640:-2","-frames:v",str(count),str(output),
                stdout=asyncio.subprocess.DEVNULL,stderr=asyncio.subprocess.PIPE)
            _,stderr=await proc.communicate()
            if proc.returncode != 0:
                log.warning("Reverse-search frame extraction failed: %s",(stderr or b"").decode(errors="replace")[-500:])
                return []
            hashes=[]
            for n,frame in enumerate(sorted(frame_dir.glob("frame_*.jpg"))):
                try:
                    with Image.open(frame) as image:
                        vals=await asyncio.to_thread(self._image_hashes,image.copy())
                    hashes.extend([f"g{n}|{v}" for v in vals])
                except Exception:
                    log.exception("Could not hash extracted frame: %s",frame)
            return hashes
        finally:
            for frame in frame_dir.glob("*"): frame.unlink(missing_ok=True)
            try: frame_dir.rmdir()
            except OSError: pass

    async def _fingerprint_local_media(self, path: Path, kind: str) -> list[str]:
        if kind == "photo":
            try:
                with Image.open(path) as image:
                    hashes = await asyncio.to_thread(self._image_hashes, image.copy())
                    return [f"g0|{value}" for value in hashes]
            except Exception:
                log.exception("Could not fingerprint image: %s", path)
                return []
        if kind == "video":
            return await self._video_frame_hashes(path, count=12)
        return []

    async def _index_local_database_media(
        self,
        *,
        record: dict,
        path: Path,
        kind: str,
        database_chat_id: int,
        database_message_id: int,
    ) -> None:
        if kind not in {"video", "photo"}:
            return
        try:
            hashes = await self._fingerprint_local_media(path, kind)
            if not hashes:
                return
            await self.db.upsert_reverse_media_fingerprint(
                media_id=int(record["id"]),
                media_kind=kind,
                database_chat_id=int(database_chat_id),
                database_message_id=int(database_message_id),
                frame_hashes=hashes,
            )
        except Exception:
            log.exception(
                "Automatic reverse-search indexing failed: %s/%s",
                database_chat_id,
                database_message_id,
            )


    async def start_reverse_index_build(self) -> bool:
        if not self.running or not self.client:
            return False
        if self.reverse_index_task and not self.reverse_index_task.done():
            return True
        self.reverse_index_task=asyncio.create_task(
            self._build_reverse_search_index(),name="reverse-search-index")
        return True

    async def _build_reverse_search_index(self) -> None:
        failed_ids=set()
        try:
            while self.running:
                rows=await self.db.unindexed_reverse_media(limit=8,exclude_media_ids=failed_ids)
                if not rows: break
                progress=False
                for record in rows:
                    media_id=int(record.get("id") or 0)
                    chat_id=int(record.get("database_chat_id") or 0)
                    message_id=int(record.get("database_message_id") or 0)
                    kind=str(record.get("media_kind") or "")
                    temp=self.temp_dir/f"reverse_index_{uuid4().hex}"
                    try:
                        message=await asyncio.wait_for(self.client.get_messages(chat_id,ids=message_id),timeout=45)
                        if not message or not getattr(message,"media",None):
                            failed_ids.add(media_id); continue
                        downloaded=await asyncio.wait_for(message.download_media(file=str(temp)),timeout=120)
                        if not downloaded:
                            failed_ids.add(media_id); continue
                        hashes=await asyncio.wait_for(
                            self._fingerprint_local_media(Path(downloaded),kind),timeout=90)
                        if not hashes:
                            failed_ids.add(media_id); continue
                        await self.db.upsert_reverse_media_fingerprint(
                            media_id=media_id,media_kind=kind,
                            database_chat_id=chat_id,database_message_id=message_id,
                            frame_hashes=hashes)
                        progress=True
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        failed_ids.add(media_id)
                        log.exception("Could not index Database media: %s/%s",chat_id,message_id)
                    finally:
                        Path(temp).unlink(missing_ok=True)
                if not progress:
                    await asyncio.sleep(5)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Automatic reverse-search index build failed")
        finally:
            self.reverse_index_task=None


    async def reverse_search_file(self, path: Path, kind: str) -> list[dict]:
        query_hashes=await self._fingerprint_local_media(path,kind)
        if not query_hashes: return []
        qgroups=self._group_hashes(query_hashes)
        candidates=await self.db.all_reverse_fingerprints()
        results=[]
        for candidate in candidates:
            sgroups=self._group_hashes([str(x) for x in candidate.get("frame_hashes",[]) if x])
            if not sgroups: continue
            region_scores=[]
            for qg in qgroups:
                best=0.0
                for sg in sgroups:
                    scores={}
                    for q in qg:
                        typ=q.split(":")[0]
                        vals=[self._hash_similarity(q,s) for s in sg if s.split(":")[0]==typ]
                        if vals: scores[typ]=max(vals)
                    if scores:
                        frame=(0.50*scores.get("p",0)+0.30*scores.get("d",0)+0.20*scores.get("a",0))
                        best=max(best,frame)
                region_scores.append(best)
            region_scores.sort(reverse=True)
            if not region_scores: continue
            best=region_scores[0]
            second=region_scores[1] if len(region_scores)>1 else best
            score=0.75*best+0.25*second
            results.append({
                "score":round(score,1),"best_frame_score":round(best,1),
                "supporting_regions":sum(x>=88 for x in region_scores),
                "database_chat_id":int(candidate["database_chat_id"]),
                "database_message_id":int(candidate["database_message_id"]),
                "media_kind":candidate.get("media_kind"),
            })
        results.sort(key=lambda x:(x["score"],x["best_frame_score"]),reverse=True)
        if not results: return []
        top=results[0]
        runner=results[1] if len(results)>1 else None
        margin=top["score"]-(runner["score"] if runner else 0)
        # Conservative verification: don't return guesses.
        reliable=(top["best_frame_score"]>=93 and top["score"]>=89 and
                  (runner is None or margin>=4 or top["best_frame_score"]>=97))
        return [top] if reliable else []

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
        # Automatically build/rebuild the visual search index in the background.
        await self.start_reverse_index_build()

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

        if self.reverse_index_task and not self.reverse_index_task.done():
            self.reverse_index_task.cancel()
            await asyncio.gather(
                self.reverse_index_task,
                return_exceptions=True,
            )
        self.reverse_index_task = None

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
        self.self_uploaded_database_ids.clear()
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

        try:
            chat_name = str(chat_id)
            if self.client and self.client.is_connected():
                entity = await self.client.get_entity(chat_id)
                chat_name = (
                    getattr(entity, "title", None)
                    or getattr(entity, "username", None)
                    or str(chat_id)
                )
            await self.alert_bot.send_message(
                chat_id=self.owner_id,
                text=(
                    "🔎 <b>Source Full History Scan</b>\n\n"
                    f"Source: <b>{chat_name}</b>\n"
                    f"Chat ID: <code>{chat_id}</code>\n\n"
                    "Status: Counting media...\n"
                    "Order: Oldest → Newest"
                ),
                parse_mode="HTML",
            )
        except Exception:
            log.exception(
                "Could not send immediate source history notification: %s",
                chat_id,
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
                "📊 <b>Source History Count Complete</b>\n\n"
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



    def _mark_self_uploaded_database_message(
        self,
        message_id: int,
    ) -> None:
        self.self_uploaded_database_ids.add(int(message_id))
        if len(self.self_uploaded_database_ids) > 5000:
            keep = sorted(self.self_uploaded_database_ids)[-2500:]
            self.self_uploaded_database_ids = set(keep)

    async def _silently_verify_bot_database_message(
        self,
        chat_id: int,
        message,
        record: dict,
    ) -> None:
        """Verify a runtime-owned Database upload without owner messages."""
        temp = self.temp_dir / f"database_verify_{uuid4().hex}"
        try:
            downloaded = await message.download_media(file=str(temp))
            if not downloaded:
                raise RuntimeError("Silent Database verification download failed")
            path = Path(downloaded)
            digest = await asyncio.to_thread(sha256_file, path)
            expected = str(record.get("sha256") or "")
            if expected and digest != expected:
                log.warning(
                    "Runtime-owned Database media hash mismatch: chat=%s message=%s",
                    chat_id,
                    int(message.id),
                )
            else:
                log.debug(
                    "Runtime-owned Database media silently verified: chat=%s message=%s",
                    chat_id,
                    int(message.id),
                )
        except Exception:
            log.exception(
                "Silent runtime-owned Database verification failed: chat=%s message=%s",
                chat_id,
                int(message.id),
            )
        finally:
            for candidate in (
                temp,
                Path(str(temp) + ".mp4"),
                Path(str(temp) + ".jpg"),
                Path(str(temp) + ".jpeg"),
                Path(str(temp) + ".png"),
                Path(str(temp) + ".webm"),
                Path(str(temp) + ".mkv"),
            ):
                try:
                    candidate.unlink(missing_ok=True)
                except Exception:
                    pass

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
    
            # Cross-process ownership token: created before Telegram upload.
            # The first exact Database message that claims this token is the
            # runtime-owned upload and must never produce owner notifications.
            message_text = str(getattr(message, "message", None) or "")
            upload_token, clean_caption = self._decode_database_upload_token(
                message_text
            )
            if upload_token:
                claimed = await self.db.claim_database_upload_token(
                    upload_token,
                    chat_id,
                    message_id,
                )
                if claimed:
                    await self.db.mark_database_message_origin(
                        chat_id,
                        message_id,
                        "bot",
                    )
                    await self._clear_database_upload_marker(
                        chat_id,
                        message,
                    )
                    await self.db.set_chat_offset(chat_id, message_id)
                    log.debug(
                        "Silently claimed runtime Database upload: "
                        "chat=%s message=%s",
                        chat_id,
                        message_id,
                    )
                    continue

            # The invisible marker is temporary. Only the exact Database
            # message already registered by this runtime is silently skipped.
            # A copied/forwarded marked message has a different message ID and
            # must continue through normal duplicate detection.
            message_text = str(getattr(message, "message", None) or "")
            if message_text.startswith(DATABASE_SELF_UPLOAD_MARKER):
                registered_marker_message = (
                    await self.db.find_by_database_message(
                        chat_id,
                        message_id,
                    )
                )
                if registered_marker_message:
                    await self._clear_database_upload_marker(
                        chat_id,
                        message,
                    )
                    self.self_uploaded_database_ids.discard(message_id)
                    await self.db.set_chat_offset(chat_id, message_id)
                    log.debug(
                        "Silently skipping registered runtime Database media: "
                        "chat=%s message=%s",
                        chat_id,
                        message_id,
                    )
                    continue

            # Permanent ownership ledger is independent of the media hash
            # index. This is essential when a bot-uploaded message is itself
            # a duplicate and therefore is not inserted as the canonical
            # media record.
            message_origin = await self.db.get_database_message_origin(
                chat_id,
                message_id,
            )
            if message_origin == "bot":
                await self.db.set_chat_offset(chat_id, message_id)
                log.debug(
                    "Silently skipping bot-owned Database message: "
                    "chat=%s message=%s",
                    chat_id,
                    message_id,
                )
                continue

            # Skip media uploaded by this runtime from a Source chat.
            # The message ID is marked immediately after Telegram confirms
            # the Database upload and before MongoDB registration begins.
            if message_id in self.self_uploaded_database_ids:
                self.self_uploaded_database_ids.discard(message_id)
                await self.db.set_chat_offset(chat_id, message_id)
                log.debug(
                    "Skipping runtime-owned Database media: "
                    "chat=%s message=%s",
                    chat_id,
                    message_id,
                )
                continue

            # A Source -> Database upload may become visible in Telegram
            # slightly before its MongoDB media record is committed. Do not
            # classify a very recent Database message as a manual upload yet.
            # Leaving the offset unchanged makes the next poll re-check it.
            message_date = getattr(message, "date", None)
            if message_date is not None:
                if message_date.tzinfo is None:
                    message_date = message_date.replace(
                        tzinfo=timezone.utc
                    )
                age_seconds = (
                    datetime.now(timezone.utc) - message_date
                ).total_seconds()
                if age_seconds < DATABASE_SELF_UPLOAD_GRACE_SECONDS:
                    log.debug(
                        "Deferring recent Database media ownership check: "
                        "chat=%s message=%s age=%.1fs",
                        chat_id,
                        message_id,
                        age_seconds,
                    )
                    continue

            # Exact Database message ownership is permanent in MongoDB.
            # Runtime-owned uploads are still hash-verified, but completely
            # silently. Owner/manual uploads continue to the notification path.
            existing_message = await self.db.find_by_database_message(
                chat_id,
                message_id,
            )
            if existing_message:
                origin = str(existing_message.get("origin") or "").lower()
                if not origin:
                    source_chat = existing_message.get("source_chat_id")
                    origin = (
                        "bot"
                        if source_chat is not None
                        and int(source_chat) != int(chat_id)
                        else "manual"
                    )

                if origin == "bot":
                    await self._silently_verify_bot_database_message(
                        chat_id,
                        message,
                        existing_message,
                    )
                    await self.db.set_chat_offset(chat_id, message_id)
                    continue

                # A manual message already registered as the canonical
                # original does not need another notification on restart.
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
                                origin="manual",
                            )
                        )
                        if not inserted:
                            raise RuntimeError(
                                "Could not replace stale media record"
                            )
                        await self._index_local_database_media(
                            record=record,
                            path=path,
                            kind=kind,
                            database_chat_id=database_chat_id,
                            database_message_id=int(message.id),
                        )
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
                            "duplicate_chat_id": database_chat_id,
                            "duplicate_message_id": int(message.id),
                        },
                        file_result={
                            "message_id": int(message.id),
                            "status": "duplicate",
                            "source_link": duplicate_link,
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
                    origin="manual",
                )
                if not inserted:
                    raise RuntimeError(
                        "Database media registration race detected"
                    )
                await self._index_local_database_media(
                    record=record,
                    path=path,
                    kind=kind,
                    database_chat_id=database_chat_id,
                    database_message_id=int(message.id),
                )
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

    async def _generate_video_thumbnail(
        self,
        video_path: Path,
    ) -> Path | None:
        """Generate a Telegram-compatible fallback thumbnail with FFmpeg."""
        thumb_path = Path(str(video_path) + "_generated_thumb.jpg")
        try:
            process = await asyncio.create_subprocess_exec(
                "ffmpeg",
                "-hide_banner",
                "-loglevel",
                "error",
                "-y",
                "-ss",
                "1",
                "-i",
                str(video_path),
                "-frames:v",
                "1",
                "-vf",
                "scale=320:320:force_original_aspect_ratio=decrease",
                "-q:v",
                "4",
                str(thumb_path),
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.PIPE,
            )
            _, stderr = await process.communicate()
            if (
                process.returncode == 0
                and thumb_path.exists()
                and thumb_path.stat().st_size > 0
            ):
                return thumb_path

            log.warning(
                "FFmpeg thumbnail fallback failed for %s: %s",
                video_path,
                (stderr or b"").decode(
                    "utf-8",
                    errors="replace",
                )[-500:],
            )
        except FileNotFoundError:
            log.warning(
                "FFmpeg is unavailable; video thumbnail fallback skipped"
            )
        except Exception:
            log.exception(
                "Could not generate fallback video thumbnail: %s",
                video_path,
            )

        thumb_path.unlink(missing_ok=True)
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

    @staticmethod
    def _encode_database_upload_token(token: str) -> str:
        raw = bytes.fromhex(str(token).replace("-", ""))
        bits = "".join(f"{byte:08b}" for byte in raw)
        encoded = "".join(
            DATABASE_UPLOAD_TOKEN_ONE if bit == "1"
            else DATABASE_UPLOAD_TOKEN_ZERO
            for bit in bits
        )
        return (
            DATABASE_UPLOAD_TOKEN_PREFIX
            + encoded
            + DATABASE_UPLOAD_TOKEN_SUFFIX
        )

    @staticmethod
    def _decode_database_upload_token(
        caption: str | None,
    ) -> tuple[str | None, str]:
        text = str(caption or "")
        prefix = DATABASE_UPLOAD_TOKEN_PREFIX
        suffix = DATABASE_UPLOAD_TOKEN_SUFFIX
        if not text.startswith(prefix):
            return None, text

        start = len(prefix)
        end = start + DATABASE_UPLOAD_TOKEN_BITS
        if len(text) < end + len(suffix):
            return None, text
        if text[end:end + len(suffix)] != suffix:
            return None, text

        encoded = text[start:end]
        if any(
            char not in {
                DATABASE_UPLOAD_TOKEN_ZERO,
                DATABASE_UPLOAD_TOKEN_ONE,
            }
            for char in encoded
        ):
            return None, text

        bits = "".join(
            "1" if char == DATABASE_UPLOAD_TOKEN_ONE else "0"
            for char in encoded
        )
        token = "".join(
            f"{int(bits[index:index + 8], 2):02x}"
            for index in range(0, len(bits), 8)
        )
        clean = text[end + len(suffix):]
        return token, clean

    @classmethod
    def _database_upload_caption(
        cls,
        caption: str | None,
        token: str,
    ) -> str:
        return cls._encode_database_upload_token(token) + (caption or "")

    async def _clear_database_upload_marker(
        self,
        database_chat_id: int,
        message,
    ) -> None:
        current_caption = str(
            getattr(message, "message", None) or ""
        )
        token, clean_caption = self._decode_database_upload_token(
            current_caption
        )
        if not token:
            return
        try:
            target = await self._resolve_peer(database_chat_id)
            await self.client.edit_message(
                target,
                int(message.id),
                clean_caption,
            )
            try:
                message.message = clean_caption
            except Exception:
                pass
        except Exception:
            # MongoDB message-ID registration remains the permanent fallback,
            # so a caption edit failure must never fail media processing.
            log.warning(
                "Could not remove temporary Database upload marker: "
                "chat=%s message=%s",
                database_chat_id,
                getattr(message, "id", None),
            )

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
                # Preserve Telegram's original thumbnail first. If Telegram
                # does not expose one (seen with some large re-uploads),
                # generate a JPEG preview from the downloaded video.
                thumb_path = await self._download_media_thumbnail(
                    source_message,
                    path,
                )
                if thumb_path is None:
                    thumb_path = await self._generate_video_thumbnail(path)

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
                # Download and hash first. Duplicate detection must finish
                # before any Source -> Database upload starts.
                downloaded = await message.download_media(file=str(temp))
                if not downloaded:
                    raise RuntimeError(
                        "Source media download returned no file"
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

                    if original_message and media_kind(original_message):
                        original_link = await self._message_link(
                            original_chat_id,
                            original_message_id,
                        )
                        duplicate_link = await self._message_link(
                            source_chat_id,
                            int(message.id),
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
                        log.info(
                            "Source duplicate detected before Database upload: "
                            "source=%s/%s original=%s/%s",
                            source_chat_id,
                            message.id,
                            original_chat_id,
                            original_message_id,
                        )
                        return True

                    # The hash record points to media that no longer exists.
                    # Remove it and allow this Source media to become the new
                    # canonical Database original.
                    await self.db.media.delete_one(
                        {"_id": existing["_id"]}
                    )

                # Unique media (or stale original replacement) can now be
                # copied/uploaded to Database and marked as bot-owned.
                upload_token = uuid4().hex
                await self.db.create_database_upload_token(
                    upload_token,
                    database_chat_id,
                    source_chat_id,
                    int(message.id),
                )
                upload_caption = self._database_upload_caption(
                    message.message or None,
                    upload_token,
                )

                sent = await self._try_server_side_copy(
                    database_chat_id,
                    message,
                    kind,
                    upload_caption,
                )
                if sent is None:
                    sent = await self._upload_downloaded_media(
                        database_chat_id,
                        path,
                        message,
                        kind,
                        upload_caption,
                    )

                await self.db.bind_database_upload_token(
                    upload_token,
                    database_chat_id,
                    int(sent.id),
                )
                self._mark_self_uploaded_database_message(int(sent.id))

                # Save ownership immediately, before hash registration.
                # Even if the hash is duplicate and register_database_media()
                # returns the existing canonical record, this exact Telegram
                # message remains permanently identifiable as bot-owned.
                await self.db.mark_database_message_origin(
                    database_chat_id,
                    int(sent.id),
                    "bot",
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
                    origin="bot",
                )

                if inserted:
                    await self._index_local_database_media(
                        record=record,
                        path=path,
                        kind=kind,
                        database_chat_id=database_chat_id,
                        database_message_id=int(sent.id),
                    )

                await self._clear_database_upload_marker(
                    database_chat_id,
                    sent,
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

                        await self._index_local_database_media(
                            record=fresh_record,
                            path=path,
                            kind=kind,
                            database_chat_id=database_chat_id,
                            database_message_id=int(sent.id),
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
            telegram_file = getattr(message, "file", None)
            raw_name = getattr(telegram_file, "name", None)
            raw_size = getattr(telegram_file, "size", None)

            if not raw_name:
                extension = {
                    "video": "mp4",
                    "photo": "jpg",
                    "audio": "mp3",
                    "file": "bin",
                }.get(media_kind(message) or "file", "bin")
                raw_name = (
                    f"{media_kind(message) or 'file'}_"
                    f"{message_id}.{extension}"
                )

            file_results[message_id] = {
                "index": index,
                "message_id": message_id,
                "kind": media_kind(message) or "file",
                "file_name": str(raw_name),
                "file_size": int(raw_size or 0),
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

    @staticmethod
    def _format_file_size(size: int | None) -> str:
        value = max(0, int(size or 0))
        units = ("B", "KB", "MB", "GB", "TB")
        amount = float(value)

        for unit in units:
            if amount < 1024 or unit == units[-1]:
                if unit == "B":
                    return f"{int(amount)} {unit}"
                return f"{amount:.1f} {unit}"
            amount /= 1024

        return f"{value} B"

    def _file_detail_line(
        self,
        item: dict | None,
        index: int,
    ) -> str:
        item = item or {}
        raw_name = str(
            item.get("file_name")
            or f"media_{index}"
        )
        # Keep Telegram owner messages readable and under 4096 chars.
        if len(raw_name) > 64:
            raw_name = raw_name[:61] + "..."

        name = escape(raw_name)
        size = self._format_file_size(
            item.get("file_size")
        )
        return f"File {index}: {name} • {size}"

    def _file_result_by_index(
        self,
        bucket: dict,
        index: int,
    ) -> dict:
        for item in bucket.get("file_results", {}).values():
            if int(item.get("index", 0)) == int(index):
                return item
        return {}

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
                self._file_detail_line(item, index)
                + "\n"
                + f"{source_text} {icon}"
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
            disable_web_page_preview=True,
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
                item = self._file_result_by_index(
                    bucket,
                    index,
                )
                final_text += (
                    "\n\n"
                    + self._file_detail_line(item, index)
                    + "\n"
                    + f"{source_text}    {database_text} ✅"
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

                item = self._file_result_by_index(
                    bucket,
                    index,
                )
                final_text += (
                    "\n\n"
                    + self._file_detail_line(item, index)
                    + "\n"
                    + f"{original_text}    {duplicate_text}"
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

