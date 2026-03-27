#!/usr/bin/env python3
"""Catch up recent Telegram messages missed during runtime downtime.

JTBD: When the realtime listener was down for a while, I want to replay only the
messages newer than the last seen cursor so Supabase bronze becomes whole again
without rerunning a full history backfill.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from pathlib import Path

script_dir = Path(__file__).resolve().parent
workspace_root = script_dir.parents[3]
sys.path.insert(0, str(workspace_root))

from heroes_platform.shared.import_setup import enable

enable(__file__)

from heroes_platform.shared.credentials_wrapper import get_service_credentials
from heroes_platform.heroes_telegram_mcp.chat_search_utils import (
    get_all_chats_list_impl,
    search_chats_by_keyword_impl,
)
from heroes_platform.heroes_telegram_mcp.supabase_writer import SupabaseWriter
from telethon import TelegramClient
from telethon.sessions import StringSession

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def _chat_type_to_supabase(chat_type: str) -> str:
    mapping = {
        "user": "private",
        "supergroup": "supergroup",
        "group": "group",
        "channel": "channel",
    }
    return mapping.get(chat_type, "unknown")


async def main() -> int:
    parser = argparse.ArgumentParser(description="Catch up recent Telegram messages to Supabase")
    parser.add_argument("--profile", default=None, help="Telegram profile: ikrasinsky or lisa")
    parser.add_argument("--keyword", default=None, help="Optional chat title filter")
    parser.add_argument("--limit-chats", type=int, default=None, help="Max chats to process")
    parser.add_argument("--limit-messages", type=int, default=1000, help="Max recent messages/chat")
    parser.add_argument("--max-dialogs", type=int, default=2000, help="Dialogs to scan")
    args = parser.parse_args()

    profile = (args.profile or os.getenv("TELEGRAM_USER", "ikrasinsky")).strip().lower()
    if profile == "lisa":
        os.environ["TELEGRAM_USER"] = "lisa"
    elif profile in ("ik", "ilyakrasinsky"):
        os.environ["TELEGRAM_USER"] = "ikrasinsky"

    credentials = get_service_credentials("telegram")
    if not credentials:
        logger.error("No Telegram credentials")
        return 1

    api_id = int(credentials.get("TELEGRAM_API_ID", 0))
    api_hash = credentials.get("TELEGRAM_API_HASH", "")
    session_str = credentials.get("TELEGRAM_SESSION_STRING", "")
    if not api_hash or api_id == 0:
        logger.error("Invalid Telegram credentials")
        return 1

    client = TelegramClient(StringSession(session_str), api_id, api_hash)
    await client.start()

    if args.keyword:
        result = await search_chats_by_keyword_impl(
            client,
            args.keyword,
            limit=args.limit_chats,
            max_dialogs_to_scan=args.max_dialogs,
        )
    else:
        result = await get_all_chats_list_impl(
            client,
            limit=args.limit_chats,
            max_dialogs_to_scan=args.max_dialogs,
        )
    chats = result.get("chats", [])
    if not chats:
        logger.warning("No chats to catch up")
        await client.disconnect()
        return 0

    writer = SupabaseWriter(telegram_user_id=os.getenv("TELEGRAM_USER", "ikrasinsky"))
    total_written = 0
    processed = 0

    for chat in chats:
        chat_id = chat.get("id")
        if chat_id is None:
            continue
        processed += 1
        chat_type = _chat_type_to_supabase(chat.get("type", "unknown"))
        title = chat.get("title", "") or ""
        username = chat.get("username")
        await writer.upsert_chat(chat_id, chat_type=chat_type, chat_title=title, chat_username=username)
        written = await writer.catch_up_recent(
            client,
            chat_id=chat_id,
            chat_type=chat_type,
            limit=args.limit_messages,
        )
        total_written += written
        if processed % 50 == 0:
            logger.info("Progress: %d chats processed, %d messages written", processed, total_written)
        await asyncio.sleep(0.05)

    await client.disconnect()
    logger.info("Done. Catch-up wrote %d messages across %d chats.", total_written, processed)
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
