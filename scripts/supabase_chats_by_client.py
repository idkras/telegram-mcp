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
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

script_dir = Path(__file__).resolve().parent
workspace_root = script_dir.parents[3]
sys.path.insert(0, str(workspace_root))

from heroes_platform.shared.import_setup import enable

enable(__file__)


def _search_pattern_from_alias(alias: str) -> str:
    """Из client alias (vipavenue-ru, elyts-ru) получить подстроку для ILIKE."""
    s = (alias or "").strip().lower()
    # убрать суффиксы -ru, .ru для поиска по названию
    s = re.sub(r"[-.]ru$", "", s)
    s = re.sub(r"[^a-z0-9_-]", "", s)
    return s or alias.lower()


def _sanitize_sql_literal(s: str) -> str:
    """Экранировать одинарные кавычки для подстановки в SQL-литерал."""
    if not s:
        return ""
    return s.replace("'", "''")


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

    pattern_raw = _search_pattern_from_alias(client_alias)
    if not pattern_raw:
        return {"success": False, "error": "Empty client alias", "chats": [], "count": 0}
    # для ILIKE: %vipavenue%
    like_val = "%" + pattern_raw + "%"
    safe = _sanitize_sql_literal(like_val)
    schema_table = f"rick_messages_tasks.{table}"
    sql = f"""
SELECT chat_id, chat_title, chat_username, chat_type
FROM {schema_table}
WHERE chat_title ILIKE '{safe}' OR chat_username ILIKE '{safe}'
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
        chat_ids = [c.get("chat_id") for c in chats if c.get("chat_id")]
        msg_out = get_messages_for_chats(chat_ids, per_chat_limit=args.messages)
        if msg_out.get("success"):
            result["messages_by_chat"] = msg_out.get("messages_by_chat") or {}
        else:
            result["messages_error"] = msg_out.get("error")

    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
