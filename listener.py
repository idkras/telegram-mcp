#!/usr/bin/env python3
"""Long-lived Telegram-to-Supabase listener for systemd deployments."""

from __future__ import annotations

import asyncio
import os
import sys

from telethon import TelegramClient
from telethon.sessions import StringSession

from heroes_platform.credentials.service_env import get_service_credentials
from heroes_platform.heroes_telegram_mcp.event_handlers import register_event_handlers


async def run_listener() -> None:
    profile = os.getenv("TELEGRAM_USER", "ikrasinsky")
    credentials = get_service_credentials("telegram")
    missing = [
        name
        for name in ("TELEGRAM_SESSION_STRING", "TELEGRAM_API_ID", "TELEGRAM_API_HASH")
        if not credentials.get(name)
    ]
    if missing:
        raise RuntimeError(f"Registry credentials are required: {', '.join(missing)}")
    client = TelegramClient(
        StringSession(credentials["TELEGRAM_SESSION_STRING"]),
        int(credentials["TELEGRAM_API_ID"]),
        credentials["TELEGRAM_API_HASH"],
    )
    await client.connect()
    try:
        if not await client.is_user_authorized():
            raise RuntimeError(f"Telegram session for {profile} is not authorized")
        me = await client.get_me()
        identity = getattr(me, "username", None) or getattr(me, "id", "unknown")
        print(f"Telegram listener authorized as {identity} ({profile})", file=sys.stderr)
        register_event_handlers(client)
        await client.run_until_disconnected()
    finally:
        await client.disconnect()


if __name__ == "__main__":
    asyncio.run(run_listener())
