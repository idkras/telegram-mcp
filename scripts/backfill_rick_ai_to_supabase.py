#!/usr/bin/env python3
"""Incremental backfill of Rick.ai Telegram chats to Supabase.

JTBD: Когда мне нужно загрузить историю сообщений из всех Rick.ai чатов в Supabase,
я хочу использовать инкрементальную выгрузку с курсорами,
чтобы можно было возобновлять и не терять данные.

Usage:
  # Full backfill all Rick.ai chats (1830+), 5000 msg/chat
  python -m heroes_platform.heroes_telegram_mcp.scripts.backfill_rick_ai_to_supabase

  # Test with 3 chats, 100 msg each
  python -m heroes_platform.heroes_telegram_mcp.scripts.backfill_rick_ai_to_supabase --limit-chats 3 --limit-messages 100

  # Incremental (resume from cursors in telegram_chats)
  python -m heroes_platform.heroes_telegram_mcp.scripts.backfill_rick_ai_to_supabase --incremental

  # All my conversations (not just rick.ai)
  python -m heroes_platform.heroes_telegram_mcp.scripts.backfill_rick_ai_to_supabase --all-chats

Prerequisites:
  - Migrations applied: 20250110000001_telegram_tdlib_tables.sql
  - SUPABASE_API_KEY or supabase_rick_api_key in Keychain
  - Telegram credentials (TELEGRAM_API_ID, TELEGRAM_API_HASH, SESSION_STRING)
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from pathlib import Path

# Setup imports before heroes_platform
script_dir = Path(__file__).resolve().parent
workspace_root = script_dir.parents[3]
sys.path.insert(0, str(workspace_root))

from heroes_platform.shared.import_setup import enable
enable(__file__)

from heroes_platform.shared.credentials_wrapper import get_service_credentials
from heroes_platform.heroes_telegram_mcp.chat_search_utils import (
    search_chats_by_keyword_impl,
    get_all_chats_list_impl,
)
from heroes_platform.heroes_telegram_mcp.supabase_writer import SupabaseWriter
from telethon import TelegramClient
from telethon.sessions import StringSession

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def _get_chat_type(t: str) -> str:
    """Map search result type to supabase chat_type."""
    m = {"user": "private", "supergroup": "supergroup", "group": "group", "channel": "channel"}
    return m.get(t, "unknown")


async def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill Rick.ai chats to Supabase")
    parser.add_argument("--keyword", default="rick.ai", help="Keyword to filter chats")
    parser.add_argument(
        "--all-chats",
        action="store_true",
        help="Load ALL conversations (ignore keyword, use get_all_chats_list)",
    )
    parser.add_argument("--limit-chats", type=int, default=None, help="Max chats to process")
    parser.add_argument("--limit-messages", type=int, default=5000, help="Max messages per chat")
    args = parser.parse_args()

    # 1. Init Telegram client
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

    # 2. Get chats: all or by keyword
    if args.all_chats:
        result = await get_all_chats_list_impl(client)
        chats = result.get("chats", [])
        logger.info("Loading ALL conversations: %d chats", len(chats))
    else:
        result = await search_chats_by_keyword_impl(client, args.keyword)
        chats = result.get("chats", [])
    if not chats:
        msg = "No chats found" + (f" for keyword {args.keyword!r}" if not args.all_chats else "")
        logger.warning(msg)
        await client.disconnect()
        return 0

    total_chats = len(chats)
    if args.limit_chats:
        chats = chats[: args.limit_chats]
    logger.info("Processing %d / %d chats (limit=%s)", len(chats), total_chats, args.limit_chats)

    # 3. Init writer and backfill
    writer = SupabaseWriter(telegram_user_id=os.getenv("TELEGRAM_USER", "ikrasinsky"))
    total_written = 0

    for i, c in enumerate(chats, 1):
        chat_id = c.get("id")
        title = c.get("title", "")
        ctype = _get_chat_type(c.get("type", "unknown"))

        # Register chat first, then backfill (uses telegram_chats cursor for resume)
        await writer.upsert_chat(chat_id, chat_type=ctype, chat_title=title)
        logger.info("[%d/%d] Backfilling %s (id=%s)", i, len(chats), title[:50], chat_id)
        try:
            written = await writer.backfill_chat(
                client, chat_id, chat_type=ctype, limit=args.limit_messages
            )
            total_written += written
            logger.info("  -> wrote %d messages", written)
        except Exception as e:
            logger.error("  -> error: %s", e)
        await asyncio.sleep(0.5)  # Rate limit

    await client.disconnect()
    logger.info("Done. Total messages written: %d", total_written)
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
