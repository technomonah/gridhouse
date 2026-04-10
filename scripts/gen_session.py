"""Generate a Telegram StringSession for use in CI/CD and orchestrators.

Run once interactively to authenticate. Prints a session string that can be
stored in TG_SESSION_STRING env var — no interactive login needed afterwards.

Usage:
    python scripts/gen_session.py
"""

import os
import sys
from pathlib import Path

from telethon.sync import TelegramClient
from telethon.sessions import StringSession

sys.path.insert(0, str(Path(__file__).parent.parent))

# Load .env
_env_path = Path(__file__).parent.parent / ".env"
if _env_path.exists():
    for _line in _env_path.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _key, _, _value = _line.partition("=")
            os.environ.setdefault(_key.strip(), _value.strip())

TG_API_ID = int(os.environ.get("TG_API_ID", "0"))
TG_API_HASH = os.environ.get("TG_API_HASH", "")

if not TG_API_ID or not TG_API_HASH:
    print("Set TG_API_ID and TG_API_HASH in .env first.")
    sys.exit(1)

with TelegramClient(StringSession(), TG_API_ID, TG_API_HASH) as client:
    session_string = client.session.save()

print("\nYour session string (add to .env as TG_SESSION_STRING):\n")
print(f"TG_SESSION_STRING={session_string}")
