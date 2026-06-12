#!/usr/bin/env python3
"""Catch up recent Telegram messages missed during runtime downtime.

JTBD: When the realtime listener was down for a while, I want to replay only the
messages newer than the last seen cursor so Supabase bronze becomes whole again
without rerunning a full history backfill.

Stage-7 architectural fix (B1, RCA 2026-06-12):
    Раньше deep_backfill_history.py имел СВОЙ launchd plist с отдельным
    TelegramClient (StringSession). Параллельно с persistent listener (laba) и
    периодическим catch-up получалось ДВА-ТРИ независимых подключения той же
    session string → §Telegram session-per-endpoint invariant нарушался →
    AuthKeyDuplicated убивал прод-сессию.

    Новый канон: deep-backfill — это BOUNDED backward-фаза ВНУТРИ существующего
    короткоживущего catch-up cycle. Один TelegramClient на cycle:
        1. start() → connect
        2. forward catch-up (catch_up_recent — новые сообщения)
        3. bounded backward deep-backfill (если --deep-backfill-budget > 0)
        4. disconnect()
    Соединение живёт секунды-минуту, не часами; параллельных listeners нет.
    launchd теперь только периодически запускает catch_up_recent — отдельного
    deep-backfill plist больше нет.
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
from heroes_platform.heroes_telegram_mcp.deep_backfill import (
    deep_backfill_all_chats,
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
    parser.add_argument(
        "--limit-messages", type=int, default=1000, help="Max recent messages/chat"
    )
    parser.add_argument("--max-dialogs", type=int, default=2000, help="Dialogs to scan")
    parser.add_argument(
        "--deep-backfill-budget",
        type=int,
        default=int(os.getenv("DEEP_BACKFILL_TOTAL_BUDGET", "500")),
        help=(
            "Stage-7 B1: bounded backward deep-backfill ВНУТРИ того же cycle "
            "(reuses connection, не плодит параллельный listener). 0 = выкл. "
            "Default 500 msgs/cycle: безопасно для каждые-5-мин (12 cycles/h = "
            "6000 msgs/h backward + forward catch-up в том же соединении)."
        ),
    )
    parser.add_argument(
        "--deep-backfill-per-chat",
        type=int,
        default=int(os.getenv("DEEP_BACKFILL_PER_CHAT_LIMIT", "200")),
        help="Max backward msgs per chat per cycle (default 200, чтобы дать многим чатам прогресс).",
    )
    parser.add_argument(
        "--deep-backfill-select-limit",
        type=int,
        default=int(os.getenv("DEEP_BACKFILL_SELECT_LIMIT", "50")),
        help="Max chats для backward-phase per cycle (default 50, FIFO + priority).",
    )
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
        await writer.upsert_chat(
            chat_id, chat_type=chat_type, chat_title=title, chat_username=username
        )
        written = await writer.catch_up_recent(
            client,
            chat_id=chat_id,
            chat_type=chat_type,
            limit=args.limit_messages,
        )
        total_written += written
        if processed % 50 == 0:
            logger.info(
                "Progress: %d chats processed, %d messages written", processed, total_written
            )
        await asyncio.sleep(0.05)

    # ── Stage-7 B1: bounded backward deep-backfill в ТОМ ЖЕ соединении ──
    # Принципиально: НЕ открываем второй TelegramClient. Используем тот же
    # `client`, который сейчас jet жив (forward catch-up завершился). После
    # backward-phase делаем единый disconnect() — connection lifecycle
    # «секунды-минута», не часами. §Telegram session-per-endpoint держится.
    deep_msgs = 0
    if args.deep_backfill_budget and args.deep_backfill_budget > 0:
        try:
            logger.info(
                "Backward deep-backfill phase: budget=%d per_chat=%d select_limit=%d",
                args.deep_backfill_budget,
                args.deep_backfill_per_chat,
                args.deep_backfill_select_limit,
            )
            deep_res = await deep_backfill_all_chats(
                client,
                writer,
                total_budget=args.deep_backfill_budget,
                per_chat_limit=args.deep_backfill_per_chat,
                chat_select_limit=args.deep_backfill_select_limit,
            )
            deep_msgs = deep_res.messages_written
            logger.info(
                "Backward deep-backfill: processed=%d completed=%d failed=%d "
                "written=%d budget_exhausted=%s stalled=%s marker=%s",
                deep_res.chats_processed,
                deep_res.chats_completed,
                deep_res.chats_failed,
                deep_res.messages_written,
                deep_res.budget_exhausted,
                deep_res.stalled,
                deep_res.marker_mode(),
            )
        except Exception as exc:  # noqa: BLE001
            # Не валим catch-up успех из-за backward-phase: forward уже записан.
            # Просто логируем — runtime_event с конкретным маркером всё равно
            # записывается внутри deep_backfill_all_chats.
            logger.warning("Backward deep-backfill phase failed: %s", exc)

    await client.disconnect()
    logger.info(
        "Done. Catch-up wrote %d msgs forward + %d msgs backward across %d chats.",
        total_written,
        deep_msgs,
        processed,
    )
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
