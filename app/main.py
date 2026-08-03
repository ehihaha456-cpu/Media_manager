from __future__ import annotations

import asyncio
import logging
import os
from contextlib import suppress
from uuid import uuid4

from .config import load_app_config
from .control_bot import ControlBot
from .crypto import load_fernet
from .db import Database
from .runtime import MediaRuntime


async def health_handler(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
) -> None:
    try:
        await reader.read(4096)
        body = b"Telegram Media Manager is running"
        response = (
            b"HTTP/1.1 200 OK\r\n"
            b"Content-Type: text/plain; charset=utf-8\r\n"
            + f"Content-Length: {len(body)}\r\n".encode()
            + b"Connection: close\r\n\r\n"
            + body
        )
        writer.write(response)
        await writer.drain()
    finally:
        writer.close()
        await writer.wait_closed()


async def start_health_server() -> asyncio.AbstractServer:
    port = int(os.getenv("PORT", "10000"))
    server = await asyncio.start_server(
        health_handler,
        host="0.0.0.0",
        port=port,
    )
    logging.getLogger(__name__).info(
        "Health server listening on 0.0.0.0:%s",
        port,
    )
    return server




async def maintain_polling_lock(
    database: Database,
    lock_id: str,
    owner_id: str,
) -> None:
    while True:
        await asyncio.sleep(15)
        renewed = await database.renew_runtime_lock(
            lock_id,
            owner_id,
            lease_seconds=45,
        )
        if not renewed:
            raise RuntimeError("Polling ownership lock was lost")


async def wait_for_polling_lock(
    database: Database,
    lock_id: str,
    owner_id: str,
) -> None:
    log = logging.getLogger(__name__)
    while True:
        try:
            acquired = await database.acquire_runtime_lock(
                lock_id,
                owner_id,
                lease_seconds=45,
            )
        except Exception:
            log.exception("Could not acquire polling lock")
            acquired = False

        if acquired:
            log.info("Exclusive bot polling lock acquired")
            return

        log.warning(
            "Another deployment owns bot polling; retrying in 10 seconds"
        )
        await asyncio.sleep(10)


async def main() -> None:
    config = load_app_config()
    logging.basicConfig(
        level=getattr(logging, config.log_level, logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    database = Database(
        config.mongodb_uri,
        config.mongodb_database,
    )
    await database.initialize()
    logging.getLogger(__name__).info(
        "MongoDB connected successfully"
    )

    fernet = load_fernet(config.master_key)
    runtime = MediaRuntime(
        database,
        fernet,
        config.temp_dir,
        config.bot_token,
        config.owner_id,
    )
    health_server = await start_health_server()

    settings = await database.get_settings()
    if settings["service_enabled"]:
        try:
            await runtime.start()
        except Exception:
            logging.exception("Could not restore media runtime")
            await database.update_settings(service_enabled=0)

    control = ControlBot(
        config,
        database,
        fernet,
        runtime,
    )
    app = control.build()

    lock_id = f"telegram-polling:{config.bot_token.split(':', 1)[0]}"
    lock_owner = uuid4().hex
    lock_task: asyncio.Task | None = None

    try:
        await wait_for_polling_lock(
            database,
            lock_id,
            lock_owner,
        )
        lock_task = asyncio.create_task(
            maintain_polling_lock(
                database,
                lock_id,
                lock_owner,
            ),
            name="telegram-polling-lock",
        )

        async with app:
            await app.start()
            await app.updater.start_polling(
                allowed_updates=[
                    "message",
                    "callback_query",
                ],
                drop_pending_updates=False,
            )

            wait_forever = asyncio.create_task(
                asyncio.Event().wait(),
                name="application-wait",
            )
            done, pending = await asyncio.wait(
                {wait_forever, lock_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()
            for task in done:
                task.result()
    finally:
        if lock_task:
            lock_task.cancel()
            with suppress(asyncio.CancelledError):
                await lock_task

        await runtime.stop()
        health_server.close()
        await health_server.wait_closed()

        if app.updater.running:
            await app.updater.stop()
        if app.running:
            await app.stop()

        with suppress(Exception):
            await database.release_runtime_lock(
                lock_id,
                lock_owner,
            )
        await database.close()


if __name__ == "__main__":
    asyncio.run(main())
