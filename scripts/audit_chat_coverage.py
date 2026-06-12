#!/usr/bin/env python3
"""Audit Telegram → Supabase chat coverage.

JTBD: Когда нужна УВЕРЕННОСТЬ что все Telegram-чаты залиты в Supabase
(не только targeted-подмножество клиента), я хочу cross-check ВСЕХ диалогов
активного профиля через iter_dialogs против telegram_messages_raw,
чтобы видеть точное число «зарегистрированных но пустых» и MISSING
(не зарегистрированных вообще). Без этого скрипта поиск переписки клиента
ходит по 0 строк и молча возвращает «не найдено».

Архитектура (§Wiring-first — переиспользует SupabaseWriter + iter_dialogs,
не строит параллельную систему):
    1. iter_dialogs() активного профиля → всё что физически есть в Telegram.
    2. SELECT chat_id FROM rick_messages_tasks.telegram_chats WHERE telegram_user_id=?
       → то что зарегистрировано в bookkeeping таблице.
    3. SELECT DISTINCT chat_id FROM rick_messages_tasks.telegram_messages_raw
       WHERE telegram_user_id=? → то что РЕАЛЬНО имеет хоть одно сообщение.
    4. set-diff в трёх плоскостях:
       - dialogs - chats_table = MISSING_FROM_BOOKKEEPING (не upsertнуты)
       - chats_table - msgs = REGISTERED_BUT_ZERO (backfill не дошёл)
       - dialogs - msgs = TOTAL_GAP (агрегат)

Универсальность: --profile <alias> через TELEGRAM_USER env (любой клиент,
любая сессия). Никаких client-specific hardcodes.

Универсально для любого нового профиля: запустил → получил число.
Не модифицирует данные. Только read.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from pathlib import Path
from typing import Any

script_dir = Path(__file__).resolve().parent
workspace_root = script_dir.parents[3]
sys.path.insert(0, str(workspace_root))

from heroes_platform.shared.import_setup import enable  # noqa: E402

enable(__file__)

from heroes_platform.shared.credentials_manager import (  # noqa: E402
    CredentialsManager,
)
from heroes_platform.shared.credentials_wrapper import (  # noqa: E402
    get_service_credentials,
)
from telethon import TelegramClient  # noqa: E402
from telethon.sessions import StringSession  # noqa: E402

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


def _supabase_conn() -> Any:
    """Direct psycopg2 connect via Keychain supabase_rick_db_url."""
    import psycopg2

    cm = CredentialsManager()
    url = cm.get_credential("supabase_rick_db_url").value
    if not url:
        raise RuntimeError("supabase_rick_db_url not in Keychain")
    return psycopg2.connect(url)


def _registered_chats(profile: str) -> set[str]:
    """chat_ids из telegram_chats для данного профиля."""
    # telegram_chats не имеет telegram_user_id колонки (см. inspect выше);
    # SSOT профиля живёт в telegram_messages_raw. Поэтому для bookkeeping
    # таблицы берём ВСЕ chat_ids — она per-database, не per-profile.
    conn = _supabase_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT chat_id FROM rick_messages_tasks.telegram_chats"
        )
        return {str(r[0]) for r in cur.fetchall()}
    finally:
        conn.close()


def _chats_with_messages(profile: str) -> set[str]:
    """chat_ids с ≥1 сообщением для данного профиля."""
    conn = _supabase_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT DISTINCT chat_id FROM rick_messages_tasks.telegram_messages_raw "
            "WHERE telegram_user_id=%s",
            (profile,),
        )
        return {str(r[0]) for r in cur.fetchall()}
    finally:
        conn.close()


def _chat_title_for_missing(chat_ids: set[str]) -> dict[str, dict[str, Any]]:
    """Подтянуть title/chat_type из telegram_chats для красивого вывода."""
    if not chat_ids:
        return {}
    conn = _supabase_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT chat_id, chat_title, chat_type, last_backfill_ts, "
            "backfill_completed FROM rick_messages_tasks.telegram_chats "
            "WHERE chat_id = ANY(%s)",
            (list(chat_ids),),
        )
        return {
            str(r[0]): {
                "title": r[1],
                "type": r[2],
                "last_backfill_ts": r[3],
                "backfill_completed": r[4],
            }
            for r in cur.fetchall()
        }
    finally:
        conn.close()


async def _enumerate_dialogs(client: Any) -> dict[str, dict[str, Any]]:
    """Все диалоги текущего профиля через iter_dialogs (без лимита)."""
    out: dict[str, dict[str, Any]] = {}
    async for dialog in client.iter_dialogs():
        chat_id = getattr(dialog, "id", None)
        if chat_id in (None, 0):
            continue
        entity = getattr(dialog, "entity", None)
        if entity is None:
            chat_type = "unknown"
        elif hasattr(entity, "broadcast"):
            chat_type = "channel" if entity.broadcast else "supergroup"
        elif getattr(entity, "megagroup", False):
            chat_type = "supergroup"
        elif hasattr(entity, "first_name"):
            chat_type = "private"
        else:
            chat_type = "group"
        title = getattr(dialog, "name", None) or getattr(dialog, "title", None) or ""
        out[str(chat_id)] = {"title": title, "type": chat_type}
    return out


async def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Audit Telegram→Supabase chat coverage. "
            "iter_dialogs(profile) vs telegram_chats vs telegram_messages_raw."
        )
    )
    parser.add_argument(
        "--profile",
        default=None,
        help="Telegram profile alias (TELEGRAM_USER env). "
        "Default: from env or 'ikrasinsky'.",
    )
    parser.add_argument(
        "--list-missing",
        type=int,
        default=50,
        help="How many missing chat_ids to print (default 50, 0 = none).",
    )
    parser.add_argument(
        "--list-zero",
        type=int,
        default=50,
        help="How many registered-but-zero chats to print (default 50, 0 = none).",
    )
    parser.add_argument(
        "--save-report",
        default=None,
        help="Save full JSON report to path (optional).",
    )
    args = parser.parse_args()

    profile = (
        args.profile
        or os.getenv("TELEGRAM_USER", "ikrasinsky")
    ).strip().lower()
    if profile in ("ik", "ilyakrasinsky"):
        profile = "ikrasinsky"
    os.environ["TELEGRAM_USER"] = profile

    creds = get_service_credentials("telegram")
    if not creds:
        logger.error("No Telegram credentials in Keychain for profile=%s", profile)
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

    logger.info("Profile=%s, connecting…", profile)
    client = TelegramClient(StringSession(session_str), api_id, api_hash)
    await client.start()
    me = await client.get_me()
    logger.info("Connected as %s (id=%s)", getattr(me, "username", "?"), getattr(me, "id", "?"))

    logger.info("Enumerating dialogs via iter_dialogs (this may take a minute)…")
    dialogs = await _enumerate_dialogs(client)
    await client.disconnect()
    logger.info("Found %d dialogs via Telegram API", len(dialogs))

    registered = _registered_chats(profile)
    with_msgs = _chats_with_messages(profile)

    dialog_ids = set(dialogs.keys())
    missing_from_bookkeeping = dialog_ids - registered
    registered_but_zero = registered - with_msgs
    dialogs_with_zero = dialog_ids - with_msgs
    coverage_pct = (
        (len(with_msgs & dialog_ids) / len(dialog_ids) * 100) if dialog_ids else 0.0
    )

    # --- print summary ---
    print()
    print(f"## Coverage audit · profile={profile}")
    print()
    print(f"| метрика | значение |")
    print(f"|---|---|")
    print(f"| total_dialogs (iter_dialogs) | {len(dialog_ids)} |")
    print(f"| registered_in_telegram_chats (вся таблица) | {len(registered)} |")
    print(f"| has_messages в telegram_messages_raw (profile={profile}) | {len(with_msgs)} |")
    print(f"| missing_from_bookkeeping (dialog не в chats) | {len(missing_from_bookkeeping)} |")
    print(f"| registered_but_zero (chats но 0 messages) | {len(registered_but_zero)} |")
    print(f"| dialogs_with_zero (видим в TG, нет сообщений в bronze) | {len(dialogs_with_zero)} |")
    print(f"| coverage_pct (dialogs∩with_msgs / dialogs) | {coverage_pct:.1f}% |")
    print()

    # --- list missing dialogs (видим в Telegram, не зарегистрированы) ---
    if args.list_missing > 0 and missing_from_bookkeeping:
        print(f"### Missing from bookkeeping (видим в Telegram но не в telegram_chats), top {args.list_missing}:")
        print()
        print(f"| chat_id | title | type |")
        print(f"|---|---|---|")
        for cid in list(missing_from_bookkeeping)[: args.list_missing]:
            d = dialogs.get(cid, {})
            t = (d.get("title") or "")[:60].replace("|", "/")
            print(f"| {cid} | {t} | {d.get('type', '?')} |")
        print()

    # --- list registered but zero (canonical Luis/Anna case) ---
    if args.list_zero > 0 and registered_but_zero:
        meta = _chat_title_for_missing(registered_but_zero)
        # Order: видимые в текущем iter_dialogs первыми (значит реально достижимы)
        visible_first = sorted(
            registered_but_zero,
            key=lambda c: (c not in dialog_ids, c),
        )
        print(f"### Registered but zero messages, top {args.list_zero}:")
        print()
        print(f"| chat_id | title | type | visible_now | last_backfill_ts |")
        print(f"|---|---|---|---|---|")
        for cid in visible_first[: args.list_zero]:
            m = meta.get(cid, {})
            t = (m.get("title") or "")[:60].replace("|", "/")
            visible = "yes" if cid in dialog_ids else "no"
            last_bf = m.get("last_backfill_ts") or "—"
            print(f"| {cid} | {t} | {m.get('type', '?')} | {visible} | {last_bf} |")
        print()

    # --- save full JSON if asked ---
    if args.save_report:
        import json

        report = {
            "profile": profile,
            "totals": {
                "dialogs": len(dialog_ids),
                "registered": len(registered),
                "with_messages": len(with_msgs),
                "missing_from_bookkeeping": len(missing_from_bookkeeping),
                "registered_but_zero": len(registered_but_zero),
                "dialogs_with_zero": len(dialogs_with_zero),
                "coverage_pct": round(coverage_pct, 2),
            },
            "missing_from_bookkeeping": sorted(missing_from_bookkeeping),
            "registered_but_zero": sorted(registered_but_zero),
            "dialogs_sample": [
                {"chat_id": cid, **info}
                for cid, info in list(dialogs.items())[:200]
            ],
        }
        Path(args.save_report).write_text(json.dumps(report, indent=2, default=str))
        logger.info("Full report saved to %s", args.save_report)

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
