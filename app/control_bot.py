from __future__ import annotations

import logging
import re
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError, PhoneCodeInvalidError
from telethon.sessions import StringSession
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    Application, CallbackQueryHandler, CommandHandler,
    ContextTypes, MessageHandler, filters,
)

from .crypto import encrypt_text, decrypt_text

log = logging.getLogger(__name__)


def keyboard(rows):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(text, callback_data=data) for text, data in row]
        for row in rows
    ])


def main_keyboard(connected: bool):
    account_button = (
        ("📱 Connected Account", "account")
        if connected
        else ("🔐 Connect Account", "connect")
    )

    return keyboard([
        [("▶️ Start / Stop", "service")],
        [account_button],
        [
            ("📥 Source Chats", "source"),
            ("🗄 Database Chat", "database"),
        ],
        [("📤 Destination Chats", "destination")],
        [
            ("🧹 Duplicate Settings", "duplicates"),
            ("⏰ Scheduler", "scheduler"),
        ],
    ])


class ControlBot:
    def __init__(self, config, db, fernet, runtime):
        self.config = config
        self.db = db
        self.fernet = fernet
        self.runtime = runtime
        self.pending_clients = {}

    def owner_only(self, update):
        return bool(update.effective_user and update.effective_user.id == self.config.owner_id)


    @staticmethod
    def _short_name(value: str | None, limit: int = 32) -> str:
        text = str(value or "Unknown")
        if len(text) <= limit:
            return text
        return text[: limit - 1] + "…"
    
    async def show_main(self, update, text="Media Manager"):
        settings = await self.db.get_settings()
        dashboard = await self.db.dashboard_statistics()
    
        phone = settings.get("phone_number") or ""
        digits = phone.replace(" ", "")
        if digits and len(digits) > 7:
            masked_phone = (
                digits[:3]
                + "*" * max(5, len(digits) - 6)
                + digits[-3:]
            )
        else:
            masked_phone = digits or "Unknown"
    
        connected = bool(settings.get("session_encrypted"))
        account_text = (
            f"✅ Connected ({masked_phone})"
            if connected
            else "❌ Not Connected"
        )
    
        database_name = None
        source_names: list[str] = []
        destination_names: list[str] = []
    
        if connected:
            try:
                chats = await self.get_accessible_chats()
                chat_names = {
                    int(chat["id"]): str(chat["name"])
                    for chat in chats
                }
    
                database_id = settings.get("database_chat_id")
                if database_id:
                    database_name = chat_names.get(
                        int(database_id),
                        str(database_id),
                    )
    
                disabled_sources = set(
                    settings.get("disabled_source_chat_ids", [])
                )
                disabled_destinations = set(
                    settings.get(
                        "disabled_destination_chat_ids", []
                    )
                )
                source_names = [
                    (
                        chat_names.get(int(chat_id), str(chat_id)),
                        int(chat_id) not in disabled_sources,
                    )
                    for chat_id in settings.get(
                        "source_chat_ids", []
                    )
                ]
                destination_names = [
                    (
                        chat_names.get(int(chat_id), str(chat_id)),
                        int(chat_id) not in disabled_destinations,
                    )
                    for chat_id in settings.get(
                        "destination_chat_ids", []
                    )
                ]
            except Exception:
                log.exception(
                    "Could not resolve chat names for dashboard"
                )
    
        service_running = bool(settings.get("service_enabled"))
        service_line = (
            "🟢 <b>Service Status:</b> Running"
            if service_running
            else "🔴 <b>Service Status:</b> Stopped"
        )
    
        database_line = (
            f"└ {'✅' if settings.get('database_chat_active') else '❌'} "
            f"{self._short_name(database_name)}"
            if database_name
            else "└ ❌ Not Connected"
        )
    
        source_lines = (
            "\n".join(
                f"└ {'✅' if enabled else '❌'} "
                f"{self._short_name(name)}"
                for name, enabled in source_names
            )
            if source_names
            else "└ ❌ No Source Chats"
        )
    
        destination_lines = (
            "\n".join(
                f"└ {'✅' if enabled else '❌'} "
                f"{self._short_name(name)}"
                for name, enabled in destination_names
            )
            if destination_names
            else "└ ❌ No Destination Chats"
        )
    
        total = dashboard["total"]
        today = dashboard["today"]
    
        pending = (
            int(dashboard.get("database_queue", 0))
            + int(dashboard.get("destination_queue", 0))
        )

        stats_block = (
            "<code>Total                    Today</code>\n"
            "<code>────────────────────────────</code>\n"
            f"<code>Processed : {total['processed']:,}</code>"
            "        "
            f"<code>Processed : {today['processed']:,}</code>\n"
            f"<code>Uploaded  : {total['uploaded']:,}</code>"
            "        "
            f"<code>Uploaded  : {today['uploaded']:,}</code>\n"
            f"<code>Duplicate : {total['duplicates']:,}</code>"
            "        "
            f"<code>Duplicate : {today['duplicates']:,}</code>\n"
            f"<code>Failed    : {total['failed']:,}</code>"
            "        "
            f"<code>Failed    : {today['failed']:,}</code>\n\n"
            f"<code>Pending   : {pending:,}</code>"
        )
    
        duplicate_line = (
            "✅ Enabled"
            if settings.get("delete_duplicates")
            else "❌ Disabled"
        )
        interval_seconds = int(
            settings.get(
                "publish_interval_seconds",
                int(settings.get(
                    "publish_interval_minutes",
                    60,
                )) * 60,
            )
        )

        if interval_seconds < 60:
            interval_text = f"{interval_seconds} Seconds"
        elif interval_seconds % 3600 == 0:
            interval_text = (
                f"{interval_seconds // 3600} Hours"
            )
        else:
            interval_text = (
                f"{interval_seconds // 60} Minutes"
            )
    
        body = (
            "🎬 <b>Telegram Media Manager</b>\n\n"
            f"{service_line}\n\n"
            "👤 <b>Account</b>\n"
            f"└ {account_text}\n\n"
            "🗄 <b>Database</b>\n"
            f"{database_line}\n\n"
            f"📥 <b>Source Chats ({len(source_names)})</b>\n"
            f"{source_lines}\n\n"
            f"📤 <b>Destination Chats "
            f"({len(destination_names)})</b>\n"
            f"{destination_lines}\n\n"
            "📊 <b>Statistics</b>\n\n"
            f"{stats_block}\n\n"
            f"🧹 <b>Duplicate Delete</b> : "
            f"{duplicate_line}\n"
            f"⏰ <b>Scheduler</b> : Every {interval_text}\n\n"
            "Select an option below."
        )
    
        if update.callback_query:
            await update.callback_query.edit_message_text(
                body,
                parse_mode="HTML",
                reply_markup=main_keyboard(connected),
                disable_web_page_preview=True,
            )
        else:
            await update.effective_message.reply_text(
                body,
                parse_mode="HTML",
                reply_markup=main_keyboard(connected),
                disable_web_page_preview=True,
            )
    
    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.owner_only(update):
            return
        context.user_data.clear()
        await self.show_main(update)

    async def callbacks(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.owner_only(update):
            return
        q = update.callback_query
        await q.answer()
        action = q.data
        settings = await self.db.get_settings()

        if action == "back":
            context.user_data.clear()
            return await self.show_main(update)

        if action == "connect":
            context.user_data["state"] = "phone"
            return await q.edit_message_text(
                "Send your phone number with country code, for example "
                "<code>+91XXXXXXXXXX</code>.",
                parse_mode="HTML",
                reply_markup=keyboard([[("⬅️ Back", "back")]]),
            )

        if action == "account":
            phone = settings["phone_number"] or "Not connected"
            return await q.edit_message_text(
                f"📱 <b>Connected Account</b>\n\nPhone: <code>{phone}</code>",
                parse_mode="HTML",
                reply_markup=keyboard([
                    [("🔌 Disconnect", "disconnect")],
                    [("⬅️ Back", "back")],
                ]),
            )

        if action == "disconnect":
            await self.runtime.stop()
            await self.db.update_settings(
                session_encrypted=None, phone_number=None, service_enabled=0
            )
            return await self.show_main(update)

        if action in {"source", "database", "destination"}:
            context.user_data["state"] = f"chat_id:{action}"
            return await self.show_chat_manager(update, context, action)

        if action.startswith("togglechat:"):
            _, mode, raw_chat_id = action.split(":", 2)
            chat_id = int(raw_chat_id)
            settings = await self.db.get_settings()

            if mode == "source":
                disabled = set(
                    settings.get("disabled_source_chat_ids", [])
                )
                if chat_id in disabled:
                    disabled.remove(chat_id)
                else:
                    disabled.add(chat_id)
                await self.db.update_settings(
                    disabled_source_chat_ids=sorted(disabled)
                )

            elif mode == "destination":
                disabled = set(
                    settings.get(
                        "disabled_destination_chat_ids", []
                    )
                )
                if chat_id in disabled:
                    disabled.remove(chat_id)
                else:
                    disabled.add(chat_id)
                await self.db.update_settings(
                    disabled_destination_chat_ids=sorted(disabled)
                )

            elif mode == "database":
                current_value = int(
                    settings.get("database_chat_enabled", 1)
                )
                new_value = 0 if current_value == 1 else 1

                await self.db.update_settings(
                    database_chat_enabled=new_value
                )


            updated = await self.db.get_settings()

            if updated["service_enabled"]:
                if (
                    mode == "database"
                    and not updated.get("database_chat_active")
                ):
                    await self.db.update_settings(
                        service_enabled=0
                    )
                    await self.runtime.stop()
                else:
                    await self.runtime.restart()

            context.user_data["state"] = f"chat_id:{mode}"
            return await self.show_chat_manager(
                update,
                context,
                mode,
            )

        if action.startswith("disconnectchat:"):
            _, mode, raw_chat_id = action.split(":", 2)
            chat_id = int(raw_chat_id)
            settings = await self.db.get_settings()

            if mode == "source":
                selected = [
                    item for item in settings["source_chat_ids"]
                    if int(item) != chat_id
                ]
                disabled = [
                    item for item in settings.get(
                        "disabled_source_chat_ids", []
                    ) if int(item) != chat_id
                ]
                await self.runtime.remove_source_history_scan(chat_id)
                await self.db.update_settings(
                    source_chat_ids=selected,
                    disabled_source_chat_ids=disabled,
                )

            elif mode == "destination":
                selected = [
                    item for item in settings[
                        "destination_chat_ids"
                    ] if int(item) != chat_id
                ]
                disabled = [
                    item for item in settings.get(
                        "disabled_destination_chat_ids", []
                    ) if int(item) != chat_id
                ]
                await self.db.update_settings(
                    destination_chat_ids=selected,
                    disabled_destination_chat_ids=disabled,
                )

            elif mode == "database":
                await self.db.update_settings(
                    database_chat_id=None,
                    database_chat_enabled=1,
                )

            updated = await self.db.get_settings()
            if updated["service_enabled"]:
                await self.runtime.restart()
            context.user_data["state"] = f"chat_id:{mode}"
            return await self.show_chat_manager(update, context, mode)

        if action == "duplicates":
            enabled = bool(settings["delete_duplicates"])
            alerts = bool(settings.get("duplicate_alerts", 1))
            return await q.edit_message_text(
                "🧹 <b>Duplicate Settings</b>\n\n"
                f"Auto delete: {'Enabled ✅' if enabled else 'Disabled ❌'}\n"
                f"Owner alerts: {'Enabled ✅' if alerts else 'Disabled ❌'}",
                parse_mode="HTML",
                reply_markup=keyboard([
                    [("Toggle Auto Delete", "toggle_duplicates")],
                    [("Toggle Owner Alerts", "toggle_duplicate_alerts")],
                    [("⬅️ Back", "back")],
                ]),
            )

        if action == "toggle_duplicates":
            new_value = 0 if settings["delete_duplicates"] else 1
            await self.db.update_settings(delete_duplicates=new_value)
            return await q.edit_message_text(
                f"🧹 <b>Duplicate Settings</b>\\n\\nAuto delete: "
                f"{'Enabled ✅' if new_value else 'Disabled ❌'}",
                parse_mode="HTML",
                reply_markup=keyboard([
                    [("Toggle Auto Delete", "toggle_duplicates")],
                    [("⬅️ Back", "back")],
                ]),
            )


        if action == "performance":
            current = str(
                settings.get("performance_mode", "balanced")
            ).lower()
            return await q.edit_message_text(
                "⚡ <b>Performance Mode</b>\n\n"
                f"Current: <b>{current.title()}</b>\n\n"
                "Low: 1 worker / 1 parallel post\n"
                "Balanced: 2 workers / 3 parallel posts\n"
                "Turbo: 4 workers / 6 parallel posts",
                parse_mode="HTML",
                reply_markup=keyboard([
                    [
                        ("🟢 Low", "set_performance:low"),
                        ("🟡 Balanced", "set_performance:balanced"),
                    ],
                    [("🔴 Turbo", "set_performance:turbo")],
                    [("⬅️ Back", "back")],
                ]),
            )

        if action.startswith("set_performance:"):
            mode = action.split(":", 1)[1]
            if mode not in {"low", "balanced", "turbo"}:
                return
            await self.db.update_settings(
                performance_mode=mode
            )
            updated = await self.db.get_settings()
            if updated["service_enabled"]:
                await self.runtime.restart()
            await q.answer(
                f"{mode.title()} mode enabled ✅"
            )
            return await self.show_main(update)

        if action == "scheduler":
            context.user_data["state"] = "interval"
            current_seconds = int(
                settings.get(
                    "publish_interval_seconds",
                    int(settings.get(
                        "publish_interval_minutes",
                        60,
                    )) * 60,
                )
            )

            if current_seconds < 60:
                current_text = f"{current_seconds} seconds"
            elif current_seconds % 3600 == 0:
                current_text = (
                    f"{current_seconds // 3600} hours"
                )
            else:
                current_text = (
                    f"{current_seconds // 60} minutes"
                )

            return await q.edit_message_text(
                "⏰ <b>Scheduler Interval</b>\n\n"
                f"Current interval: <b>{current_text}</b>\n\n"
                "Send a new interval using one of these formats:\n"
                "• <code>10s</code> or <code>10 seconds</code>\n"
                "• <code>1m</code> or <code>1 minute</code>\n"
                "• <code>60 minutes</code>\n"
                "• <code>2h</code> or <code>2 hours</code>",
                parse_mode="HTML",
                reply_markup=keyboard([[("⬅️ Back", "back")]]),
            )

        if action == "service":
            if settings["service_enabled"]:
                await self.db.update_settings(service_enabled=0)
                await self.runtime.stop()
            else:
                if not settings["session_encrypted"]:
                    return await q.answer("Connect Telegram account first.", show_alert=True)
                if not settings["active_source_chat_ids"]:
                    return await q.answer(
                        "Add at least one Source chat.",
                        show_alert=True,
                    )
                if not settings["database_chat_active"]:
                    return await q.answer(
                        "Select a Database chat.",
                        show_alert=True,
                    )
                if not settings["active_destination_chat_ids"]:
                    return await q.answer(
                        "Add at least one Destination chat.",
                        show_alert=True,
                    )
                await self.db.update_settings(service_enabled=1)
                try:
                    await self.runtime.start()
                except Exception as exc:
                    await self.db.update_settings(service_enabled=0)
                    return await q.answer(str(exc), show_alert=True)
            return await self.show_main(update)

        if action == "stats":
            stats = await self.db.statistics()
            return await q.edit_message_text(
                "📊 <b>Statistics</b>\n\n"
                f"Unique media: {stats['media']}\n"
                f"Pending posts: {stats['queued']}\n"
                f"Published posts: {stats['published']}",
                parse_mode="HTML",
                reply_markup=keyboard([[("⬅️ Back", "back")]]),
            )

    async def text(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.owner_only(update):
            return
        state = context.user_data.get("state")
        value = update.effective_message.text.strip()
        if not state:
            return

        try:
            if state == "phone":
                client = TelegramClient(
                    StringSession(),
                    self.config.api_id,
                    self.config.api_hash,
                )
                await client.connect()
                sent = await client.send_code_request(value)
                self.pending_clients[update.effective_user.id] = client
                context.user_data.update(
                    phone=value,
                    phone_code_hash=sent.phone_code_hash,
                    state="otp",
                )
                return await update.message.reply_text(
                    "OTP sent. Enter digits separated by spaces, for example: "
                    "<code>1 2 3 4 5</code>",
                    parse_mode="HTML",
                )

            if state == "otp":
                client = self.pending_clients[update.effective_user.id]
                code = value.replace(" ", "")
                try:
                    await client.sign_in(
                        phone=context.user_data["phone"],
                        code=code,
                        phone_code_hash=context.user_data["phone_code_hash"],
                    )
                except SessionPasswordNeededError:
                    context.user_data["state"] = "password"
                    return await update.message.reply_text("Send your Telegram 2FA password.")
                except PhoneCodeInvalidError:
                    return await update.message.reply_text("Invalid OTP. Try again.")

                return await self.finish_login(update, context, client)

            if state == "password":
                client = self.pending_clients[update.effective_user.id]
                await client.sign_in(password=value)
                return await self.finish_login(update, context, client)

            if state.startswith("chat_id:"):
                mode = state.split(":", 1)[1]
                try:
                    chat_id = int(value.replace(" ", ""))
                except ValueError:
                    return await update.message.reply_text(
                        "Send a valid numeric Chat ID, for example: "
                        "<code>-1001234567890</code>",
                        parse_mode="HTML",
                    )

                chats = await self.get_accessible_chats()
                chat_map = {int(chat["id"]): chat for chat in chats}
                if chat_id not in chat_map:
                    return await update.message.reply_text(
                        "This Chat ID is not available in the connected "
                        "Telegram account. Copy an ID from the list above.",
                    )

                settings = await self.db.get_settings()
                chat_name = chat_map[chat_id]["name"]

                if mode == "source":
                    selected = list(settings["source_chat_ids"])
                    if chat_id not in selected:
                        selected.append(chat_id)
                        await self.db.update_settings(
                            source_chat_ids=selected
                        )
                        await self.runtime.register_source_history_scan(
                            chat_id
                        )
                    else:
                        await update.message.reply_text(
                            "Source is already connected."
                        )

                elif mode == "destination":
                    selected = list(settings[
                        "destination_chat_ids"
                    ])
                    if chat_id not in selected:
                        selected.append(chat_id)
                        await self.db.update_settings(
                            destination_chat_ids=selected
                        )
                    else:
                        await update.message.reply_text(
                            "Destination is already connected."
                        )

                elif mode == "database":
                    await self.db.update_settings(
                        database_chat_id=chat_id,
                        database_chat_enabled=1,
                    )

                updated = await self.db.get_settings()
                if updated["service_enabled"]:
                    await self.runtime.restart()

                await update.message.reply_text(
                    f"✅ {chat_name} connected successfully."
                )
                context.user_data["state"] = f"chat_id:{mode}"
                return await self.show_chat_manager(
                    update, context, mode, send_new=True
                )

            if state == "interval":
                raw = value.strip().lower()

                second_match = re.fullmatch(
                    r"(\d+)\s*(s|sec|secs|second|seconds)",
                    raw,
                )
                minute_match = re.fullmatch(
                    r"(\d+)\s*(m|min|mins|minute|minutes)?",
                    raw,
                )
                hour_match = re.fullmatch(
                    r"(\d+)\s*(h|hr|hrs|hour|hours)",
                    raw,
                )

                if second_match:
                    interval_seconds = max(
                        1,
                        int(second_match.group(1)),
                    )
                elif hour_match:
                    interval_seconds = max(
                        1,
                        int(hour_match.group(1)) * 3600,
                    )
                elif minute_match:
                    interval_seconds = max(
                        1,
                        int(minute_match.group(1)) * 60,
                    )
                else:
                    return await update.message.reply_text(
                        "⏰ <b>Invalid interval</b>\n\n"
                        "Use one of these formats:\n"
                        "• <code>10s</code> or <code>10 seconds</code>\n"
                        "• <code>1m</code> or <code>1 minute</code>\n"
                        "• <code>60 minutes</code>\n"
                        "• <code>2h</code> or <code>2 hours</code>",
                        parse_mode="HTML",
                    )

                await self.db.update_settings(
                    publish_interval_seconds=interval_seconds,
                    publish_interval_minutes=max(
                        1,
                        interval_seconds // 60,
                    ),
                )
                context.user_data.clear()

                settings = await self.db.get_settings()
                if settings["service_enabled"]:
                    await self.runtime.restart()

                return await self.show_main(update)
        except Exception as exc:
            log.exception("Control input failed")
            await update.message.reply_text(f"Error: {exc}")

    async def finish_login(self, update, context, client):
        session = StringSession.save(client.session)
        await self.db.update_settings(
            api_id=self.config.api_id,
            api_hash_encrypted=encrypt_text(
                self.fernet, self.config.api_hash
            ),
            phone_number=context.user_data["phone"],
            session_encrypted=encrypt_text(self.fernet, session),
        )
        await client.disconnect()
        self.pending_clients.pop(update.effective_user.id, None)
        context.user_data.clear()
        await update.message.reply_text("Telegram account connected successfully ✅")
        await self.show_main(update)

    async def chats(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.owner_only(update):
            return
        settings = await self.db.get_settings()
        if not settings["session_encrypted"]:
            return await update.message.reply_text("Connect Telegram account first.")

        api_hash = decrypt_text(self.fernet, settings["api_hash_encrypted"])
        session = decrypt_text(self.fernet, settings["session_encrypted"])
        client = TelegramClient(
            StringSession(session), int(settings["api_id"]), api_hash
        )
        await client.connect()
        lines = []
        async for dialog in client.iter_dialogs():
            protected = bool(getattr(dialog.entity, "noforwards", False))
            lines.append(
                f"<code>{dialog.id}</code> — {dialog.name}"
                f"{' 🔒' if protected else ''}"
            )
            if len(lines) >= 40:
                break
        await client.disconnect()
        await update.message.reply_text(
            "Accessible chats:\n\n" + "\n".join(lines),
            parse_mode="HTML",
        )


    async def get_accessible_chats(self):
        settings = await self.db.get_settings()
        if not settings["session_encrypted"]:
            raise RuntimeError("Connect Telegram account first.")

        api_hash = decrypt_text(self.fernet, settings["api_hash_encrypted"])
        session = decrypt_text(self.fernet, settings["session_encrypted"])

        client = TelegramClient(
            StringSession(session),
            int(settings["api_id"]),
            api_hash,
        )
        await client.connect()

        chats = []
        try:
            async for dialog in client.iter_dialogs():
                entity = dialog.entity
                is_group_or_channel = bool(
                    getattr(entity, "megagroup", False)
                    or getattr(entity, "broadcast", False)
                    or entity.__class__.__name__ in {"Chat", "Channel"}
                )
                if not is_group_or_channel:
                    continue

                chats.append({
                    "id": int(dialog.id),
                    "name": dialog.name or str(dialog.id),
                    "protected": bool(getattr(entity, "noforwards", False)),
                })

                if len(chats) >= 80:
                    break
        finally:
            await client.disconnect()

        return chats

    async def show_chat_manager(
        self,
        update,
        context,
        mode: str,
        *,
        send_new: bool = False,
    ):
        try:
            chats = await self.get_accessible_chats()
        except Exception as exc:
            if update.callback_query:
                return await update.callback_query.answer(
                    str(exc), show_alert=True
                )
            return await update.effective_message.reply_text(str(exc))

        settings = await self.db.get_settings()
        context.user_data["state"] = f"chat_id:{mode}"
        chat_names = {
            int(chat["id"]): str(chat["name"])
            for chat in chats
        }

        if mode == "source":
            title = "📥 <b>Source Groups/Channels</b>"
            connected = list(settings["source_chat_ids"])
            disabled = set(
                settings.get("disabled_source_chat_ids", [])
            )
        elif mode == "destination":
            title = "📤 <b>Destination Groups/Channels</b>"
            connected = list(settings["destination_chat_ids"])
            disabled = set(
                settings.get(
                    "disabled_destination_chat_ids", []
                )
            )
        else:
            title = "🗄 <b>Database Group/Channel</b>"
            connected = (
                [int(settings["database_chat_id"])]
                if settings.get("database_chat_id")
                else []
            )
            disabled = set()
            if connected and not settings.get(
                "database_chat_enabled", 1
            ):
                disabled.add(connected[0])

        joined_lines = []
        for index, chat in enumerate(chats[:40], start=1):
            lock = " 🔒" if chat.get("protected") else ""
            joined_lines.append(
                f"{index}. <b>{self._short_name(chat['name'], 40)}</b>"
                f"{lock}\n   <code>{int(chat['id'])}</code>"
            )

        if len(chats) > 40:
            joined_lines.append(
                f"\nShowing first 40 of {len(chats)} chats."
            )

        text = (
            f"{title}\n\n"
            f"Connected: <b>{len(connected)}</b>\n\n"
            "<b>Your joined groups/channels</b>\n\n"
            + ("\n\n".join(joined_lines) if joined_lines else "No chats found.")
            + "\n\n━━━━━━━━━━━━━━━━━━\n\n"
            "Copy a Chat ID from above, paste it below, and send it.\n\n"
            "Example: <code>-1001234567890</code>"
        )

        rows = []
        for chat_id in connected:
            enabled = int(chat_id) not in disabled
            name = self._short_name(
                chat_names.get(int(chat_id), str(chat_id)), 22
            )
            rows.append([
                (
                    f"{'✅' if enabled else '❌'} {name}",
                    f"togglechat:{mode}:{int(chat_id)}",
                ),
                (
                    "❌ Disconnect",
                    f"disconnectchat:{mode}:{int(chat_id)}",
                ),
            ])

        rows.append([("⬅️ Back", "back")])
        markup = keyboard(rows)

        if update.callback_query and not send_new:
            return await update.callback_query.edit_message_text(
                text,
                parse_mode="HTML",
                reply_markup=markup,
                disable_web_page_preview=True,
            )

        return await update.effective_message.reply_text(
            text,
            parse_mode="HTML",
            reply_markup=markup,
            disable_web_page_preview=True,
        )

    def build(self):
        app = Application.builder().token(self.config.bot_token).build()
        app.add_handler(CommandHandler("start", self.start))
        app.add_handler(CommandHandler("chats", self.chats))
        app.add_handler(CallbackQueryHandler(self.callbacks))
        app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.text))
        return app
