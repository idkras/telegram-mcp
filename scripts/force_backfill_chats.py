#!/usr/bin/env python3
"""Force-backfill specific Telegram chat_ids to Supabase (seed path).

JTBD: Когда конкретный чат зарегистрирован в telegram_chats но last_seen_message_id
NULL (live-handler не зацепил, periodic backfill не запускался), мы хотим
прицельно засеять последние N сообщений через тот же _seed_recent путь, что и
startup_backfill — без ожидания периодика.

Универсально: --chat-id повторяется, --seed-limit задаётся, --profile любой.
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

from heroes_platform.shared.import_setup import enable  # noqa: E402

enable(__file__)

from heroes_platform.credentials.service_env import (  # noqa: E402
    get_service_credentials,
)
from heroes_platform.heroes_telegram_mcp.startup_backfill import (  # noqa: E402
    backfill_one_chat,
)
from heroes_platform.heroes_telegram_mcp.supabase_writer import (  # noqa: E402
    SupabaseWriter,
)
from telethon import TelegramClient  # noqa: E402
from telethon.sessions import StringSession  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


async def _detect_chat_type(client, chat_id: int) -> str:
    """Получить chat_type через get_entity (точнее чем dialog inference)."""
    try:
        entity = await client.get_entity(chat_id)
        if hasattr(entity, "broadcast"):
            return "channel" if entity.broadcast else "supergroup"
        if getattr(entity, "megagroup", False):
            return "supergroup"
        if hasattr(entity, "first_name"):
            return "private"
        return "group"
    except Exception as exc:
        logger.warning("get_entity(%s) failed: %s — defaulting to 'unknown'", chat_id, exc)
        return "unknown"


async def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Force seed-backfill specific Telegram chats via startup_backfill"
            ".backfill_one_chat path."
        )
    )
    parser.add_argument(
        "--chat-id",
        action="append",
        required=True,
        help="chat_id to backfill (repeatable)",
    )
    parser.add_argument(
        "--profile",
        default=None,
        help="TELEGRAM_USER alias (default: env or 'ikrasinsky')",
    )
    parser.add_argument(
        "--seed-limit",
        type=int,
        default=2000,
        help="Max recent messages to seed for a chat without cursor (default 2000)",
    )
    parser.add_argument(
        "--per-chat-limit",
        type=int,
        default=5000,
        help="Max messages per catch_up if cursor exists (default 5000)",
    )
    args = parser.parse_args()

    profile = (args.profile or os.getenv("TELEGRAM_USER", "ikrasinsky")).strip().lower()
    if profile in ("ik", "ilyakrasinsky"):
        profile = "ikrasinsky"
    os.environ["TELEGRAM_USER"] = profile

    creds = get_service_credentials("telegram")
    if not creds:
        logger.error("No Telegram credentials for profile=%s", profile)
        return 1
    api_id = int(creds.get("TELEGRAM_API_ID", 0))
    api_hash = creds.get("TELEGRAM_API_HASH", "")
    session_str = creds.get("TELEGRAM_SESSION_STRING", "")
    if not api_hash or api_id == 0 or not session_str:
        logger.error("Invalid Telegram credentials")
        return 1

    client = TelegramClient(StringSession(session_str), api_id, api_hash)
    await client.start()
    me = await client.get_me()
    logger.info(
        "Connected as %s (id=%s) profile=%s",
        getattr(me, "username", "?"),
        getattr(me, "id", "?"),
        profile,
    )

    writer = SupabaseWriter(telegram_user_id=profile)

    total_written = 0
    successes = 0
    failures = 0

    for raw_id in args.chat_id:
        try:
            chat_id = int(raw_id)
        except ValueError:
            logger.error("chat_id must be integer: %s", raw_id)
            failures += 1
            continue

        chat_type = await _detect_chat_type(client, chat_id)
        logger.info(
            "→ chat_id=%s type=%s — seed_limit=%s per_chat_limit=%s",
            chat_id,
            chat_type,
            args.seed_limit,
            args.per_chat_limit,
        )
        try:
            written, truncated = await backfill_one_chat(
                client,
                writer,
                chat_id,
                chat_type,
                per_chat_limit=args.per_chat_limit,
                seed_limit=args.seed_limit,
            )
            total_written += written
            successes += 1
            logger.info(
                "  ✓ written=%d truncated=%s (cursor will reflect max id)",
                written,
                truncated,
            )
        except Exception as exc:
            logger.error("  ✗ failed for chat %s: %s", chat_id, exc)
            failures += 1

    await client.disconnect()
    logger.info(
        "Done. successes=%d failures=%d total_messages_written=%d",
        successes,
        failures,
        total_written,
    )
    return 0 if failures == 0 else 2


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
