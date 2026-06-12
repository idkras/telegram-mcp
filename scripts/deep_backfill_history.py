#!/usr/bin/env python3
"""Manual one-shot deep backfill (backward-walk) of Telegram chats to Supabase.

⚠️ ARCHITECTURAL CHANGE (Stage-7, RCA 2026-06-12, B1 fix):

    Раньше этот скрипт запускался отдельным launchd-агентом (плодил отдельное
    persistent TelegramClient соединение). Параллельно с Mac MCP listener /
    laba listener / 5-мин catch-up — это давало §Telegram session-per-endpoint
    нарушение → AuthKeyDuplicated убивал прод-сессию.

    Новый канон: deep-backfill — это bounded backward-фаза ВНУТРИ существующего
    catch-up cycle. См.
        scripts/catch_up_recent_telegram_to_supabase.py --deep-backfill-budget=N
    Один TelegramClient на cycle (start → forward → backward → disconnect).

    Этот скрипт остаётся как **manual one-shot** для аварийных целевых прогонов
    (например прицельный backfill большого клиентского чата под steering
    владельца). НЕ запускать через launchd параллельно с listener.

JTBD: Когда нужна уверенность что в Supabase лежит ВСЯ история каждого чата
(а не только последние seed N сообщений или новее last_seen курсора), я хочу
ОДНУ универсальную команду которая:

    1. Возьмёт активный профиль (любой; по умолчанию из TELEGRAM_USER).
    2. Сама выберет чаты которым нужен deep backfill
       (priority_tier ASC, потом last_backfill_ts ASC NULLS FIRST).
    3. Для каждого пойдёт ВНИЗ от floor (last_backfill OR last_seen) до начала
       чата либо до total_budget.
    4. Resumable: следующий запуск продолжит с floor предыдущего.
    5. Идемпотентно: ON CONFLICT (chat_id, message_id) → повтор без дублей.

Универсально для любого нового профиля: добавил TELEGRAM_USER alias + ключи в
Keychain → запустил с `--profile <alias>` → история догнаётся. Никаких правок
кода / hardcodes / per-client веток.

Использование (one-shot):
    # Все чаты профиля ikrasinsky, по 10k сообщений за прогон, 2k на чат:
    .venv/bin/python heroes_platform/heroes_telegram_mcp/scripts/deep_backfill_history.py \\
        --profile ikrasinsky --budget 10000 --per-chat 2000

    # Прицельно один или несколько чатов (без DB selection):
    .venv/bin/python ... --profile ikrasinsky \\
        --chat-id -1003722483787 --chat-id 1253846223 --per-chat 5000

    # Сухой прогон — какие чаты были бы выбраны:
    .venv/bin/python ... --profile ikrasinsky --list-only --select-limit 50

    # Приоритетные клиентские чаты (env-driven, universal):
    DEEP_BACKFILL_PRIORITY_CHATS=-1001234,567890 \\
        .venv/bin/python ... --profile ikrasinsky --budget 5000

⚠️ ПРЕДУПРЕЖДЕНИЕ при запуске: скрипт ловит running listener и предупреждает,
если на той же машине detected persistent endpoint (защита от случайного
параллельного hourly bootstrap, который сломал бы прод).
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
from pathlib import Path

script_dir = Path(__file__).resolve().parent
workspace_root = script_dir.parents[3]
sys.path.insert(0, str(workspace_root))

from heroes_platform.shared.import_setup import enable  # noqa: E402

enable(__file__)

from heroes_platform.shared.credentials_wrapper import (  # noqa: E402
    get_service_credentials,
)
from heroes_platform.heroes_telegram_mcp.deep_backfill import (  # noqa: E402
    _select_chats_for_deep_backfill,
    deep_backfill_all_chats,
)
from heroes_platform.heroes_telegram_mcp.supabase_writer import (  # noqa: E402
    SupabaseWriter,
)
from telethon import TelegramClient  # noqa: E402
from telethon.sessions import StringSession  # noqa: E402

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


async def _detect_chat_type(client, chat_id: int) -> str:
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


def _normalize_profile(value: str | None) -> str:
    profile = (value or os.getenv("TELEGRAM_USER", "ikrasinsky")).strip().lower()
    if profile in ("ik", "ilyakrasinsky"):
        profile = "ikrasinsky"
    return profile


async def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "System-wide deep backfill (backward-walk) of Telegram chats to "
            "Supabase. Universal: works for any TELEGRAM_USER profile."
        )
    )
    parser.add_argument(
        "--profile",
        default=None,
        help="TELEGRAM_USER alias (default: env or 'ikrasinsky')",
    )
    parser.add_argument(
        "--budget",
        type=int,
        default=10000,
        help="Max messages to write across all chats in this run (default 10000)",
    )
    parser.add_argument(
        "--per-chat",
        type=int,
        default=2000,
        help="Max messages to walk in a single chat per run (default 2000)",
    )
    parser.add_argument(
        "--chat-id",
        action="append",
        default=None,
        help="Explicit chat_id (repeatable). Bypasses DB selection.",
    )
    parser.add_argument(
        "--select-limit",
        type=int,
        default=500,
        help="Max chats to select from DB by priority (default 500)",
    )
    parser.add_argument(
        "--list-only",
        action="store_true",
        help="Print chats that WOULD be processed and exit (no Telegram calls).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit final result as JSON to stdout (for CI / monitoring).",
    )
    args = parser.parse_args()

    profile = _normalize_profile(args.profile)
    os.environ["TELEGRAM_USER"] = profile

    writer = SupabaseWriter(telegram_user_id=profile)

    # --- list-only path (без Telegram-логина) ---
    if args.list_only:
        chats = await _select_chats_for_deep_backfill(
            writer, limit=args.select_limit
        )
        print(f"\n## Deep backfill candidates · profile={profile} · "
              f"top {args.select_limit}\n")
        print("| chat_id | chat_type |")
        print("|---|---|")
        for cid, ctype in chats:
            print(f"| {cid} | {ctype} |")
        print(f"\nTotal: {len(chats)} chats need deep backfill.")
        return 0

    creds = get_service_credentials("telegram")
    if not creds:
        logger.error("No Telegram credentials for profile=%s", profile)
        return 1
    api_id = int(creds.get("TELEGRAM_API_ID", 0))
    api_hash = creds.get("TELEGRAM_API_HASH", "")
    session_str = creds.get("TELEGRAM_SESSION_STRING", "")
    if not api_hash or api_id == 0 or not session_str:
        logger.error(
            "Invalid Telegram credentials: api_id=%s api_hash_len=%s session_len=%s",
            api_id, len(api_hash), len(session_str)
        )
        return 1

    client = TelegramClient(StringSession(session_str), api_id, api_hash)
    await client.start()
    me = await client.get_me()
    logger.info(
        "Connected as %s (id=%s) profile=%s schema=%s",
        getattr(me, "username", "?"),
        getattr(me, "id", "?"),
        profile,
        writer.schema,
    )

    explicit_chats: list[tuple[str, str]] | None = None
    if args.chat_id:
        explicit_chats = []
        for raw_id in args.chat_id:
            try:
                cid = int(raw_id)
            except ValueError:
                logger.error("chat_id must be integer: %s", raw_id)
                continue
            ctype = await _detect_chat_type(client, cid)
            explicit_chats.append((str(cid), ctype))
            logger.info("Resolved chat %s → type=%s", cid, ctype)

    res = await deep_backfill_all_chats(
        client,
        writer,
        total_budget=args.budget,
        per_chat_limit=args.per_chat,
        chat_select_limit=args.select_limit,
        explicit_chats=explicit_chats,
    )
    await client.disconnect()

    if args.json:
        out = {
            "profile": profile,
            "schema": writer.schema,
            "chats_processed": res.chats_processed,
            "chats_completed": res.chats_completed,
            "chats_failed": res.chats_failed,
            "messages_written": res.messages_written,
            "budget_exhausted": res.budget_exhausted,
            "session_dead": res.session_dead,
            "marker_mode": res.marker_mode(),
            "per_chat": [
                {
                    "chat_id": c.chat_id,
                    "written": c.written,
                    "floor_before": c.floor_before,
                    "floor_after": c.floor_after,
                    "completed": c.completed,
                    "error": c.error,
                }
                for c in res.per_chat
            ],
        }
        print(json.dumps(out, indent=2, default=str))
    else:
        print(
            f"\n## Deep backfill done · profile={profile}\n\n"
            f"| метрика | значение |\n"
            f"|---|---|\n"
            f"| chats_processed | {res.chats_processed} |\n"
            f"| chats_completed | {res.chats_completed} |\n"
            f"| chats_failed | {res.chats_failed} |\n"
            f"| messages_written | {res.messages_written} |\n"
            f"| budget_exhausted | {res.budget_exhausted} |\n"
            f"| session_dead | {res.session_dead} |\n"
            f"| marker | {res.marker_mode()} |\n"
        )
        if res.per_chat:
            print("### Per-chat detail (top 20)\n")
            print("| chat_id | written | floor_before | floor_after | completed | error |")
            print("|---|---|---|---|---|---|")
            for c in res.per_chat[:20]:
                err = (c.error or "—")[:50]
                print(
                    f"| {c.chat_id} | {c.written} | {c.floor_before} | "
                    f"{c.floor_after} | {c.completed} | {err} |"
                )

    if res.session_dead:
        return 3
    if res.chats_failed > 0:
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
