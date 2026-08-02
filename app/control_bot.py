from __future__ import annotations

import logging
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


MAIN = keyboard([
    [("🔐 Connect Account", "connect"), ("📱 Connected Account", "account")],
    [("📥 Source Chats", "source"), ("🗄 Database Chat", "database")],
    [("📤 Destination Chats", "destination")],
    [("🧹 Duplicate Settings", "duplicates"), ("⏰ Scheduler", "scheduler")],
    [("▶️ Start / Stop", "service"), ("📊 Statistics", "stats")],
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

    async def show_main(self, update, text="Media Manager"):
        settings = await self.db.get_settings()
        connected = "Connected ✅" if settings["session_encrypted"] else "Not connected ❌"
        service = "Running ✅" if settings["service_enabled"] else "Stopped ⏹"
        body = (
            f"🎬 <b>Telegram Media Manager</b>\n\n"
            f"Account: {connected}\nService: {service}\n\n"
            "Manage the complete media system below."
        )
        if update.callback_query:
            await update.callback_query.edit_message_text(
                body, parse_mode="HTML", reply_markup=MAIN
            )
        else:
            await update.effective_message.reply_text(
                body, parse_mode="HTML", reply_markup=MAIN
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
            context.user_data["state"] = "api_id"
            return await q.edit_message_text(
                "Send your Telegram <b>API ID</b>.",
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
            context.user_data["state"] = action
            instructions = {
                "source": "Send source chat IDs separated by commas.",
                "database": "Send one database channel/group ID.",
                "destination": "Send destination chat IDs separated by commas.",
            }
            return await q.edit_message_text(
                instructions[action] + "\n\nUse /chats to view accessible chat IDs.",
                reply_markup=keyboard([[("⬅️ Back", "back")]]),
            )

        if action == "duplicates":
            enabled = bool(settings["delete_duplicates"])
            return await q.edit_message_text(
                f"🧹 <b>Duplicate Settings</b>\n\nAuto delete: "
                f"{'Enabled ✅' if enabled else 'Disabled ❌'}",
                parse_mode="HTML",
                reply_markup=keyboard([
                    [("Toggle Auto Delete", "toggle_duplicates")],
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

        if action == "scheduler":
            context.user_data["state"] = "interval"
            return await q.edit_message_text(
                f"⏰ Current interval: {settings['publish_interval_minutes']} minutes\n\n"
                "Send the new interval in minutes.",
                reply_markup=keyboard([[("⬅️ Back", "back")]]),
            )

        if action == "service":
            if settings["service_enabled"]:
                await self.db.update_settings(service_enabled=0)
                await self.runtime.stop()
            else:
                if not settings["session_encrypted"]:
                    return await q.answer("Connect Telegram account first.", show_alert=True)
                if not settings["source_chat_ids"]:
                    return await q.answer("Add at least one source chat.", show_alert=True)
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
            if state == "api_id":
                context.user_data["api_id"] = int(value)
                context.user_data["state"] = "api_hash"
                return await update.message.reply_text("Send your Telegram API Hash.")

            if state == "api_hash":
                context.user_data["api_hash"] = value
                context.user_data["state"] = "phone"
                return await update.message.reply_text(
                    "Send phone number with country code, for example +91XXXXXXXXXX."
                )

            if state == "phone":
                api_id = context.user_data["api_id"]
                api_hash = context.user_data["api_hash"]
                client = TelegramClient(StringSession(), api_id, api_hash)
                await client.connect()
                sent = await client.send_code_request(value)
                self.pending_clients[update.effective_user.id] = client
                context.user_data.update(
                    phone=value,
                    phone_code_hash=sent.phone_code_hash,
                    state="otp",
                )
                return await update.message.reply_text(
                    "OTP sent. Enter digits separated by spaces, for example: 1 2 3 4 5"
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

            if state in {"source", "destination"}:
                ids = [int(x.strip()) for x in value.split(",") if x.strip()]
                key = "source_chat_ids" if state == "source" else "destination_chat_ids"
                await self.db.update_settings(**{key: ids})
                context.user_data.clear()
                return await self.show_main(update)

            if state == "database":
                await self.db.update_settings(database_chat_id=int(value))
                context.user_data.clear()
                return await self.show_main(update)

            if state == "interval":
                minutes = max(1, int(value))
                await self.db.update_settings(publish_interval_minutes=minutes)
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
            api_id=context.user_data["api_id"],
            api_hash_encrypted=encrypt_text(
                self.fernet, context.user_data["api_hash"]
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

    def build(self):
        app = Application.builder().token(self.config.bot_token).build()
        app.add_handler(CommandHandler("start", self.start))
        app.add_handler(CommandHandler("chats", self.chats))
        app.add_handler(CallbackQueryHandler(self.callbacks))
        app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.text))
        return app
