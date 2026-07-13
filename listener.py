#!/usr/bin/env python3
"""Long-lived Telegram-to-Supabase listener for systemd deployments."""

from __future__ import annotations

import asyncio
import os
import sys

from telethon import TelegramClient
from telethon.sessions import StringSession

from heroes_platform.heroes_telegram_mcp.event_handlers import register_event_handlers


def _required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"{name} is required")
    return value


async def run_listener() -> None:
    profile = os.getenv("TELEGRAM_USER", "ikrasinsky")
    client = TelegramClient(
        StringSession(_required("TELEGRAM_SESSION_STRING")),
        int(_required("TELEGRAM_API_ID")),
        _required("TELEGRAM_API_HASH"),
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
