#!/usr/bin/env python3
"""Fill rick_telegram_chats from telegram_chats: only Rick.ai client chats, with segment.

JTBD: В таблице rick_telegram_chats только чаты Rick.ai с клиентами по индексу и типології:
rick.ai / rick-ai / rick в названии и segment ∈ {advising, pilot, dogovorennosti, na_soprovozhdenii, partners}.

Источник: rick_messages_tasks.telegram_chats (заполняется sync_telegram_chats_to_supabase).
Фильтр: chat_title/chat_username — «rick.ai», «rick-ai» или «rick» в контексте (advising, договор, сопровожд, report).
Оставляем только клиентские segment (исключены internal, community, bot_feedback, other).
Segment: по ключевым словам в chat_title (см. telegram-chats-index.md § 2.1–2.7).

Usage:
  python -m heroes_platform.heroes_telegram_mcp.scripts.sync_rick_telegram_chats_from_telegram_chats

Prerequisites:
  - SUPABASE_DB_URL или SUPABASE_RICK_DB_URL (или Keychain supabase_rick_db_url)
  - Миграции 20250215000000 и 20250216000001 применены
"""
from __future__ import annotations

import sys
from pathlib import Path

script_dir = Path(__file__).resolve().parent
workspace_root = script_dir.parents[3]
sys.path.insert(0, str(workspace_root))

from heroes_platform.shared.import_setup import enable

enable(__file__)

from heroes_platform.rickai_mcp.supabase_postgres import (
    get_supabase_postgres_url,
    run_sql_fetch,
    run_sql,
)

SCHEMA = "rick_messages_tasks"
SOURCE_TABLE = f"{SCHEMA}.telegram_chats"
TARGET_TABLE = f"{SCHEMA}.rick_telegram_chats"

# Ключевые слова для segment (по telegram-chats-index.md и README)
SEGMENT_KEYWORDS = [
    ("advising", ["advising", "flow.rick.ai", "анализ проекта"]),
    ("pilot", ["пилот", "vipavenue", "реаспект", "donplafon", "установочный"]),
    ("dogovorennosti", ["договоренност", "договорённост"]),
    ("na_soprovozhdenii", ["полезные отчеты", "сквозной", "helpful reports", "+ rick.ai]"]),
    ("internal", ["flow", "подстраховка", "pm care", "метрика", "errors"]),
    ("bot_feedback", ["bot", "feedback"]),
    ("community", ["heroes of", "hoc", "product heroes", "[ph]", "стрим"]),
    ("partners", ["партнер", "partner"]),
]

# Только чаты с клиентами Рика по индексу и типології (telegram-chats-index § 2.1–2.4, 2.7 partners)
CLIENT_SEGMENTS = ("advising", "pilot", "dogovorennosti", "na_soprovozhdenii", "partners")


def _is_rickai_chat(title: str | None, username: str | None) -> bool:
    """Оставить только чаты, относящиеся к Rick.ai (rick.ai, rick-ai, rick в контексте)."""
    t = (title or "").lower()
    u = (username or "").lower()
    return (
        "rick.ai" in t
        or "rick.ai" in u
        or "rick-ai" in t
        or "rick-ai" in u
        or (
            "rick" in t
            and ("advising" in t or "договор" in t or "сопровожд" in t or "report" in t)
        )
    )


def _segment_from_title(title: str | None) -> str:
    """Определить segment по chat_title (эвристика по telegram-chats-index.md)."""
    t = (title or "").lower()
    for segment, keywords in SEGMENT_KEYWORDS:
        if any(k in t for k in keywords):
            return segment
    return "other"


def main() -> int:
    url = get_supabase_postgres_url()
    if not url:
        print(
            "ERROR: Supabase Postgres URL not set. Set SUPABASE_DB_URL or SUPABASE_RICK_DB_URL, or Keychain supabase_rick_db_url."
        )
        return 1

    # Прочитать все чаты из источника
    sel = f"""SELECT chat_id, chat_type, chat_title, chat_username,
              last_backfill_message_id, last_backfill_ts, backfill_completed,
              last_seen_message_id, last_seen_ts, total_messages_count, is_active, last_error
              FROM {SOURCE_TABLE}"""
    out = run_sql_fetch(sel, postgres_url=url)
    if not out.get("success"):
        print("ERROR:", out.get("error", "SELECT failed"))
        return 1
    rows = out.get("rows") or []
    print(f"Read {len(rows)} rows from {SOURCE_TABLE}")

    # Фильтр: только Rick.ai (rick.ai / rick-ai / rick в контексте)
    rickai = [r for r in rows if _is_rickai_chat(r.get("chat_title"), r.get("chat_username"))]
    print(f"After filter (Rick.ai only): {len(rickai)} rows")

    # Только чаты с клиентами по индексу и типології (advising, pilot, dogovorennosti, na_soprovozhdenii, partners)
    rickai = [r for r in rickai if _segment_from_title(r.get("chat_title")) in CLIENT_SEGMENTS]
    print(f"After filter (client segments only): {len(rickai)} rows")

    if not rickai:
        print("No Rick.ai chats to write. Truncating rick_telegram_chats.")
        run_sql(f"TRUNCATE TABLE {TARGET_TABLE}", postgres_url=url)
        return 0

    # TRUNCATE и вставка
    run_sql(f"TRUNCATE TABLE {TARGET_TABLE}", postgres_url=url)

    import psycopg2

    conn = psycopg2.connect(url)
    conn.autocommit = False
    cur = conn.cursor()
    try:
        for r in rickai:
            seg = _segment_from_title(r.get("chat_title"))
            cur.execute(
                f"""INSERT INTO {TARGET_TABLE}
                    (chat_id, chat_type, chat_title, chat_username,
                     last_backfill_message_id, last_backfill_ts, backfill_completed,
                     last_seen_message_id, last_seen_ts, total_messages_count, is_active, last_error, segment)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (
                    r.get("chat_id"),
                    r.get("chat_type"),
                    r.get("chat_title"),
                    r.get("chat_username"),
                    r.get("last_backfill_message_id"),
                    r.get("last_backfill_ts"),
                    r.get("backfill_completed"),
                    r.get("last_seen_message_id"),
                    r.get("last_seen_ts"),
                    r.get("total_messages_count"),
                    r.get("is_active"),
                    r.get("last_error"),
                    seg,
                ),
            )
        conn.commit()
    except Exception as e:
        conn.rollback()
        print("ERROR on INSERT:", e)
        return 1
    finally:
        cur.close()
        conn.close()

    print(f"Inserted {len(rickai)} rows into {TARGET_TABLE} with segment.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
