#!/usr/bin/env python3
"""Find Telegram chats by Rick client alias via Supabase SQL (fast path).

JTBD: Когда нужно найти все чаты по клиенту (vipavenue-ru, elyts-ru) без долгого
Telegram MCP — выполнить SQL по rick_messages_tasks.telegram_chats (или rick_telegram_chats).
Опционально выгрузить последние N сообщений из telegram_messages_raw для извлечения запросов.

Usage:
  python -m heroes_platform.heroes_telegram_mcp.scripts.supabase_chats_by_client vipavenue-ru
  python -m heroes_platform.heroes_telegram_mcp.scripts.supabase_chats_by_client vipavenue-ru --messages 100
  python -m heroes_platform.heroes_telegram_mcp.scripts.supabase_chats_by_client vipavenue-ru --table telegram_chats

Output: JSON with chats[] and optionally messages_by_chat{}.

Runtime scope: monorepo-only. This helper depends on
``heroes_platform.rickai_mcp.supabase_postgres`` and is not a standalone
telegram-mcp deploy entrypoint.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import date, datetime
from pathlib import Path

script_dir = Path(__file__).resolve().parent
workspace_root = script_dir.parents[3]
sys.path.insert(0, str(workspace_root))

from heroes_platform.shared.import_setup import enable

enable(__file__)


RUNTIME_SCOPE = "monorepo-only"


def _search_key_from_alias(alias: str) -> str:
    """Convert a client alias to a separator-insensitive ASCII search key."""
    s = (alias or "").strip().lower()
    # Region suffixes are not normally present in Telegram chat titles.
    s = re.sub(r"[-.]ru$", "", s)
    return re.sub(r"[^a-z0-9]+", "", s)


def _sanitize_sql_literal(s: str) -> str:
    """Экранировать одинарные кавычки для подстановки в SQL-литерал."""
    if not s:
        return ""
    return s.replace("'", "''")


def _json_default(value: object) -> str:
    """Serialize PostgreSQL temporal values without hiding unsupported types."""
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _message_chat_ids(chats: list[dict]) -> list[str]:
    """Return registry IDs plus Telegram's -100 peer form for supergroups."""
    ids: list[str] = []
    for chat in chats:
        chat_id = str(chat.get("chat_id") or "")
        if not chat_id:
            continue
        ids.append(chat_id)
        if chat.get("chat_type") == "supergroup":
            if chat_id.startswith("-100") and chat_id[4:].isdigit():
                peer_id = chat_id
            elif chat_id.startswith("-") and chat_id[1:].isdigit():
                peer_id = f"-100{chat_id[1:]}"
            elif chat_id.isdigit():
                peer_id = f"-100{chat_id}"
            else:
                peer_id = ""
            if peer_id:
                ids.append(peer_id)
    return list(dict.fromkeys(ids))


def find_chats_by_client_alias(
    client_alias: str,
    *,
    limit: int = 50,
    table: str = "rick_telegram_chats",
) -> dict:
    """Найти чаты по client alias через Supabase SQL.

    Returns:
        {"success": bool, "chats": list[dict], "count": int} или {"success": False, "error": str}.
    """
    from heroes_platform.rickai_mcp.supabase_postgres import (
        get_supabase_postgres_url,
        run_sql_fetch,
    )

    search_key = _search_key_from_alias(client_alias)
    if not search_key:
        return {"success": False, "error": "Empty client alias", "chats": [], "count": 0}
    # Normalize both sides so fizikl-org also matches "fizikl.org" and
    # vipavenue-ru matches titles that contain only "VIPAvenue".
    like_val = "%" + search_key + "%"
    safe = _sanitize_sql_literal(like_val)
    schema_table = f"rick_messages_tasks.{table}"
    sql = f"""
SELECT chat_id, chat_title, chat_username, chat_type
FROM {schema_table}
WHERE regexp_replace(lower(coalesce(chat_title, '')), '[^a-z0-9]+', '', 'g') LIKE '{safe}'
   OR regexp_replace(lower(coalesce(chat_username, '')), '[^a-z0-9]+', '', 'g') LIKE '{safe}'
ORDER BY chat_title
LIMIT {max(1, min(limit, 200))}
"""
    out = run_sql_fetch(sql)
    if not out.get("success"):
        return {
            "success": False,
            "error": out.get("error", "Unknown error"),
            "chats": [],
            "count": 0,
        }
    rows = out.get("rows") or []
    return {"success": True, "chats": rows, "count": len(rows)}


def get_messages_for_chats(
    chat_ids: list[str],
    *,
    per_chat_limit: int = 100,
) -> dict:
    """Выгрузить последние сообщения по списку chat_id из telegram_messages_raw.

    Returns:
        {"success": bool, "messages_by_chat": {chat_id: [dict]}} или {"success": False, "error": str}.
    """
    from heroes_platform.rickai_mcp.supabase_postgres import run_sql_fetch

    if not chat_ids:
        return {"success": True, "messages_by_chat": {}}
    # Один запрос: все чаты, ORDER BY chat_id, message_ts DESC; затем разложить по chat_id
    ids_sql = ",".join(f"'{cid.replace(chr(39), chr(39)+chr(39))}'" for cid in chat_ids)
    sql = f"""
SELECT chat_id, message_id, sender_name, sender_username, message_ts, text
FROM rick_messages_tasks.telegram_messages_raw
WHERE chat_id IN ({ids_sql})
ORDER BY chat_id, message_ts DESC
LIMIT {len(chat_ids) * max(1, min(per_chat_limit, 500))}
"""
    out = run_sql_fetch(sql)
    if not out.get("success"):
        return {
            "success": False,
            "error": out.get("error", "Unknown error"),
            "messages_by_chat": {},
        }
    rows = out.get("rows") or []
    by_chat: dict[str, list] = {}
    for r in rows:
        cid = r.get("chat_id") or ""
        if cid not in by_chat:
            by_chat[cid] = []
        if len(by_chat[cid]) < per_chat_limit:
            by_chat[cid].append(r)
    return {"success": True, "messages_by_chat": by_chat}


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Find Telegram chats by Rick client alias (Supabase SQL)"
    )
    parser.add_argument("client_alias", help="Client alias, e.g. vipavenue-ru, elyts-ru")
    parser.add_argument("--limit", type=int, default=50, help="Max chats to return")
    parser.add_argument(
        "--table",
        default="rick_telegram_chats",
        choices=["rick_telegram_chats", "telegram_chats"],
        help="Table to search (rick_telegram_chats = only Rick.ai subset)",
    )
    parser.add_argument(
        "--messages",
        type=int,
        default=0,
        help="If >0, fetch last N messages per chat from telegram_messages_raw",
    )
    args = parser.parse_args()

    result = find_chats_by_client_alias(
        args.client_alias,
        limit=args.limit,
        table=args.table,
    )
    if not result.get("success"):
        print(json.dumps(result, ensure_ascii=False, indent=2), file=sys.stderr)
        return 1

    chats = result.get("chats") or []
    if args.messages > 0 and chats:
        chat_ids = _message_chat_ids(chats)
        msg_out = get_messages_for_chats(chat_ids, per_chat_limit=args.messages)
        if msg_out.get("success"):
            result["messages_by_chat"] = msg_out.get("messages_by_chat") or {}
        else:
            result["messages_error"] = msg_out.get("error")

    print(json.dumps(result, ensure_ascii=False, indent=2, default=_json_default))
    return 0


if __name__ == "__main__":
    sys.exit(main())
