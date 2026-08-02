# Telegram Media Manager — Single Owner Control Bot

All Telegram account and media settings are entered through the bot interface.

## Environment variables

Only these startup values are required:

```env
BOT_TOKEN=BotFather bot token
OWNER_ID=your numeric Telegram user ID
MASTER_KEY=optional persistent encryption key
```

`BOT_TOKEN` cannot be entered from inside the bot because the bot needs it before
it can start. `OWNER_ID` prevents other people from opening the control panel.

Generate a persistent encryption key:

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

## Start

```bash
pip install -r requirements.txt
python -m app.main
```

Open the bot and send `/start`.

## Control panel

- Connect Telegram Account
- Connected Account / Disconnect
- Source Chats
- Database Chat
- Destination Chats
- Duplicate Auto Delete
- Scheduler interval
- Start / Stop service
- Statistics
- `/chats` lists accessible chat IDs

The OTP is accepted with spaces, such as `1 2 3 4 5`, to reduce Telegram's
automatic login-code security warning.

The project skips chats/messages marked with Telegram content protection.
