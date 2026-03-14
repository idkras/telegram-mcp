#!/usr/bin/env python3
"""Sync Telegram chat list to Supabase (telegram_chats only, no message backfill).

JTBD: Когда нужно заполнить индекс чатов в Supabase для lookup по title/username
без вызова get_direct_chat_by_contact, я запускаю этот скрипт один раз (или по расписанию),
чтобы таблица telegram_chats содержала актуальный список чатов.

Usage:
  # Sync all chats (up to 500 dialogs) — default profile (ikrasinsky)
  python -m heroes_platform.heroes_telegram_mcp.scripts.sync_telegram_chats_to_supabase

  # For advising: sync Lisa's chats (index для ответов клиентам от Лизы)
  python -m heroes_platform.heroes_telegram_mcp.scripts.sync_telegram_chats_to_supabase --profile lisa

  # Sync only chats matching keyword
  python -m heroes_platform.heroes_telegram_mcp.scripts.sync_telegram_chats_to_supabase --keyword "rick.ai"

  # Limit number of chats to sync
  python -m heroes_platform.heroes_telegram_mcp.scripts.sync_telegram_chats_to_supabase --limit 200

  # Full sync: run in stages (e.g. --limit 200, then 400) or run manually in terminal with --max-dialogs N
  # to avoid long single runs that may be aborted in IDE (see ai.incidents 15 Feb 2026, Standard 1.13).

Prerequisites:
  - Migrations applied (tasks.telegram_chats or rick_messages_tasks.telegram_chats)
  - SUPABASE_API_KEY or supabase_rick_api_key in Keychain
  - SUPABASE_TELEGRAM_SCHEMA=tasks if migration used schema "tasks"
  - Telegram credentials: default from TELEGRAM_USER / Keychain; --profile lisa uses lisa_tg_* (Keychain)
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
    search_chats_by_keyword_impl,
    get_all_chats_list_impl,
)
from heroes_platform.heroes_telegram_mcp.supabase_writer import SupabaseWriter
from telethon import TelegramClient
from telethon.sessions import StringSession

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def _chat_type_to_supabase(t: str) -> str:
    m = {"user": "private", "supergroup": "supergroup", "group": "group", "channel": "channel"}
    return m.get(t, "unknown")


async def main() -> int:
    parser = argparse.ArgumentParser(
        description="Sync Telegram chat list to Supabase (no messages)"
    )
    parser.add_argument(
        "--profile",
        default=None,
        help="Telegram profile: ikrasinsky (default) or lisa (advising index)",
    )
    parser.add_argument(
        "--keyword", default=None, help="Only sync chats with keyword in title (default: all)"
    )
    parser.add_argument("--limit", type=int, default=None, help="Max chats to sync")
    parser.add_argument(
        "--max-dialogs", type=int, default=500, help="Max dialogs to scan (default 500)"
    )
    args = parser.parse_args()

    # Profile: lisa -> Lisa's client (advising); else default (ikrasinsky)
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
            client, args.keyword, limit=args.limit, max_dialogs_to_scan=args.max_dialogs
        )
        chats = result.get("chats", [])
        logger.info("Found %d chats for keyword %r", len(chats), args.keyword)
    else:
        result = await get_all_chats_list_impl(
            client, limit=args.limit, max_dialogs_to_scan=args.max_dialogs
        )
        chats = result.get("chats", [])

    if not chats:
        logger.warning("No chats to sync")
        await client.disconnect()
        return 0

    writer = SupabaseWriter(telegram_user_id=os.getenv("TELEGRAM_USER", "ikrasinsky"))
    synced = 0
    for i, c in enumerate(chats, 1):
        chat_id = c.get("id")
        title = c.get("title", "") or ""
        ctype = _chat_type_to_supabase(c.get("type", "unknown"))
        username = c.get("username")
        ok = await writer.upsert_chat(
            chat_id, chat_type=ctype, chat_title=title, chat_username=username
        )
        if ok:
            synced += 1
        if i % 50 == 0:
            logger.info("Progress: %d/%d synced", synced, i)
        await asyncio.sleep(0.05)

    await client.disconnect()
    logger.info("Done. Synced %d chats to Supabase (telegram_chats).", synced)
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
