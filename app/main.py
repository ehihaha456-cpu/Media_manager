from __future__ import annotations

import asyncio
import logging

from .config import load_app_config
from .control_bot import ControlBot
from .crypto import load_fernet
from .db import Database
from .runtime import MediaRuntime


async def main():
    config = load_app_config()
    logging.basicConfig(
        level=getattr(logging, config.log_level, logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    db = Database(config.sqlite_path)
    await db.initialize()
    fernet = load_fernet(config.master_key)
    runtime = MediaRuntime(db, fernet, config.temp_dir)

    settings = await db.get_settings()
    if settings["service_enabled"]:
        try:
            await runtime.start()
        except Exception:
            logging.exception("Could not restore media runtime")
            await db.update_settings(service_enabled=0)

    control = ControlBot(config, db, fernet, runtime)
    app = control.build()

    async with app:
        await app.start()
        await app.updater.start_polling(allowed_updates=["message", "callback_query"])
        try:
            await asyncio.Event().wait()
        finally:
            await runtime.stop()
            await app.updater.stop()
            await app.stop()


if __name__ == "__main__":
    asyncio.run(main())
