#!/usr/bin/env python3
"""Single-owner Telegram listener plus optional HTTP MCP for systemd.

One process owns one profile's Telethon session.  In streamable-http mode the
same connected client serves MCP tools and ingestion handlers, so exposing an
interactive endpoint never creates a second session connection.
"""

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
        transport = os.getenv("TELEGRAM_MCP_TRANSPORT", "listener").strip().lower()
        if transport == "streamable-http":
            # Import after the primary client is authorized.  main.py registers
            # the canonical MCP tools; replacing its unconnected bootstrap
            # client keeps every tool on this process's single session owner.
            from heroes_platform.heroes_telegram_mcp import main as telegram_mcp

            telegram_mcp.client = client
            print(
                "Telegram MCP streamable-http ready "
                f"on {telegram_mcp.MCP_HOST}:{telegram_mcp.MCP_PORT}{telegram_mcp.MCP_PATH} "
                f"({profile})",
                file=sys.stderr,
            )
            await telegram_mcp.mcp.run_streamable_http_async()
        else:
            await client.run_until_disconnected()
    finally:
        await client.disconnect()


if __name__ == "__main__":
    asyncio.run(run_listener())
