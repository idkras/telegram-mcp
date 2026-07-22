#!/usr/bin/env python3
"""Backfill article/Instant View тел статей в telegram_articles.

JTBD: Когда исторический корпус (137k webpage-сообщений) лежит в
telegram_messages_raw без тел статей, хотим (1) мгновенно проиндексировать
title/description/уже имеющиеся cached_page из raw и (2) дозапросить
недостающие тела через Telethon, чтобы полнотекстовый поиск покрыл историю.

Modes:
    --mode from-raw   Без Telegram-логина: пройти webpage-сообщения в
                      telegram_messages_raw, извлечь title/description/
                      article_text (из существующих cached_page) и upsert в
                      telegram_articles. Безопасно запускать откуда угодно,
                      где есть доступ к Supabase PG (VPN/VPS).
    --mode fetch      Нужна живая Telethon-сессия (запускать на VPS/laba):
                      для строк telegram_articles без тела (has_page=false)
                      дозапросить GetWebPageRequest, извлечь текст, обновить
                      article_text + записать cached_page обратно в raw
                      сообщение (--update-raw).

Usage (на VPS/laba):
    python3 scripts/backfill_article_pages.py --mode from-raw
    python3 scripts/backfill_article_pages.py --mode fetch --only-telegraph --limit 500
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
PKG_DIR = SCRIPT_DIR.parent
sys.path.insert(0, str(PKG_DIR))
# workspace root (для heroes_platform.* импортов, когда доступны)
for _cand in (PKG_DIR.parent.parent, PKG_DIR.parent.parent.parent):
    if (_cand / "heroes_platform").is_dir():
        sys.path.insert(0, str(_cand))
        break

from article_enrichment import extract_article_text, fetch_cached_page  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("backfill_article_pages")

FLOOD_WAIT_MAX_SLEEP_BACKFILL = 120


def _postgres_url() -> str:
    url = os.getenv("SUPABASE_DB_URL")
    if url:
        return url
    try:
        from heroes_platform.shared.credentials_manager import credentials_manager

        result = credentials_manager.get_credential("supabase_rick_db_url")
        if result.success and result.value:
            return str(result.value)
    except Exception:  # noqa: BLE001
        pass
    try:  # канонический резолвер (тот же, что у SupabaseWriter/laba)
        from heroes_platform.rickai_mcp.supabase_postgres import get_supabase_postgres_url

        url = get_supabase_postgres_url()
        if url:
            return str(url)
    except Exception:  # noqa: BLE001
        pass
    raise SystemExit("No Supabase PG url: set SUPABASE_DB_URL or credentials registry")


def _connect(url: str) -> Any:
    import psycopg2  # type: ignore

    return psycopg2.connect(url)


UPSERT_SQL = """
INSERT INTO {schema}.telegram_articles
(chat_id, message_id, telegram_user_id, message_ts, url, title,
 description, article_text, has_page, fetched_at, updated_at)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s, CASE WHEN %s THEN now() END, now())
ON CONFLICT (chat_id, message_id) DO UPDATE SET
  url=EXCLUDED.url, title=EXCLUDED.title, description=EXCLUDED.description,
  message_ts=EXCLUDED.message_ts,
  article_text=CASE WHEN EXCLUDED.article_text <> ''
                    THEN EXCLUDED.article_text
                    ELSE {schema}.telegram_articles.article_text END,
  has_page={schema}.telegram_articles.has_page OR EXCLUDED.has_page,
  fetched_at=COALESCE(EXCLUDED.fetched_at, {schema}.telegram_articles.fetched_at),
  updated_at=now()
"""


def backfill_from_raw(
    conn: Any, schema: str, *, batch: int, limit: int | None, only_telegraph: bool, dry_run: bool
) -> dict[str, int]:
    """Извлечь карточки статей из существующего raw (без сети)."""
    where_url = "AND raw->'media'->'webpage'->>'url' ILIKE '%%telegra.ph%%'" if only_telegraph else ""
    select_sql = f"""
    SELECT chat_id, message_id, telegram_user_id, message_ts,
           raw->'media'->'webpage'->>'url'         AS url,
           raw->'media'->'webpage'->>'title'       AS title,
           raw->'media'->'webpage'->>'description' AS description,
           raw->'media'->'webpage'->'cached_page'  AS cached_page
    FROM {schema}.telegram_messages_raw
    WHERE raw->'media'->>'_' = 'MessageMediaWebPage'
      AND raw->'media'->'webpage'->>'url' IS NOT NULL
      {where_url}
      AND (chat_id, message_id) > (%s, %s)
    ORDER BY chat_id, message_id
    LIMIT %s
    """
    stats = {"scanned": 0, "upserted": 0, "with_body": 0}
    cursor_key: tuple[str, int] = ("", 0)
    while True:
        if limit is not None and stats["scanned"] >= limit:
            break
        page_size = batch if limit is None else min(batch, limit - stats["scanned"])
        cur = conn.cursor()
        cur.execute(select_sql, (*cursor_key, page_size))
        rows = cur.fetchall()
        cur.close()
        if not rows:
            break
        stats["scanned"] += len(rows)
        cursor_key = (rows[-1][0], rows[-1][1])
        articles = []
        for chat_id, message_id, tg_user, message_ts, url, title, description, cached_page in rows:
            page = cached_page
            if isinstance(page, str):
                try:
                    page = json.loads(page)
                except ValueError:
                    page = None
            body = extract_article_text(page) if isinstance(page, dict) else ""
            if body:
                stats["with_body"] += 1
            if not (title or description or body):
                continue
            articles.append(
                (chat_id, message_id, tg_user, message_ts, url, title or "",
                 description or "", body, bool(body), bool(body))
            )
        if articles and not dry_run:
            wcur = conn.cursor()
            try:
                wcur.executemany(UPSERT_SQL.format(schema=schema), articles)
                conn.commit()
                stats["upserted"] += len(articles)
            except Exception:
                conn.rollback()
                raise
            finally:
                wcur.close()
        elif articles:
            stats["upserted"] += len(articles)
        logger.info(
            "from-raw progress: scanned=%d upserted=%d with_body=%d (cursor=%s/%s)",
            stats["scanned"], stats["upserted"], stats["with_body"], *cursor_key,
        )
    return stats


async def backfill_fetch(
    conn: Any,
    schema: str,
    *,
    limit: int | None,
    only_telegraph: bool,
    sleep_seconds: float,
    update_raw: bool,
    dry_run: bool,
) -> dict[str, int]:
    """Дозапросить недостающие тела статей через Telethon (VPS-режим)."""
    from telethon import TelegramClient  # type: ignore
    from telethon.sessions import StringSession  # type: ignore

    from heroes_platform.shared.credentials_wrapper import get_service_credentials

    creds = get_service_credentials("telegram")
    if not creds:
        raise SystemExit("No Telegram credentials (get_service_credentials('telegram'))")
    api_id = int(creds.get("TELEGRAM_API_ID", 0))
    api_hash = creds.get("TELEGRAM_API_HASH", "")
    session_str = creds.get("TELEGRAM_SESSION_STRING", "")
    if not api_hash or api_id == 0 or not session_str:
        raise SystemExit("Invalid Telegram credentials (api_id/api_hash/session)")

    where_url = "AND url ILIKE '%%telegra.ph%%'" if only_telegraph else ""
    select_sql = f"""
    SELECT chat_id, message_id, url
    FROM {schema}.telegram_articles
    WHERE has_page = FALSE AND url IS NOT NULL {where_url}
    ORDER BY message_ts DESC NULLS LAST
    LIMIT %s
    """
    cur = conn.cursor()
    cur.execute(select_sql, (limit or 1000,))
    targets = cur.fetchall()
    cur.close()
    stats = {"targets": len(targets), "fetched": 0, "no_iv": 0, "errors": 0, "raw_updated": 0}
    if not targets:
        return stats

    client = TelegramClient(StringSession(session_str), api_id, api_hash)
    await client.start()
    try:
        for chat_id, message_id, url in targets:
            page_obj = await fetch_cached_page(
                client, url, flood_wait_max_sleep=FLOOD_WAIT_MAX_SLEEP_BACKFILL
            )
            if page_obj is None:
                stats["no_iv"] += 1
                await asyncio.sleep(sleep_seconds)
                continue
            try:
                page_dict = page_obj.to_dict()
            except Exception:  # noqa: BLE001
                stats["errors"] += 1
                continue
            body = extract_article_text(_json_safe(page_dict))
            if not body:
                stats["no_iv"] += 1
                await asyncio.sleep(sleep_seconds)
                continue
            stats["fetched"] += 1
            if not dry_run:
                wcur = conn.cursor()
                try:
                    wcur.execute(
                        f"""UPDATE {schema}.telegram_articles
                            SET article_text=%s, has_page=TRUE, fetched_at=now(), updated_at=now()
                            WHERE chat_id=%s AND message_id=%s""",
                        (body, chat_id, message_id),
                    )
                    if update_raw:
                        wcur.execute(
                            f"""UPDATE {schema}.telegram_messages_raw
                                SET raw = jsonb_set(raw, '{{media,webpage,cached_page}}', %s::jsonb, true)
                                WHERE chat_id=%s AND message_id=%s
                                  AND raw->'media'->>'_' = 'MessageMediaWebPage'""",
                            (json.dumps(_json_safe(page_dict)), chat_id, message_id),
                        )
                        stats["raw_updated"] += wcur.rowcount
                    conn.commit()
                except Exception:
                    conn.rollback()
                    raise
                finally:
                    wcur.close()
            logger.info("fetched %s (%s/%s): %d chars", url, chat_id, message_id, len(body))
            await asyncio.sleep(sleep_seconds)
    finally:
        await client.disconnect()
    return stats


def _json_safe(obj: Any) -> Any:
    """bytes/datetime из Telethon to_dict() → JSON-совместимые значения."""
    if isinstance(obj, dict):
        return {k: _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_json_safe(v) for v in obj]
    if isinstance(obj, bytes):
        return obj.decode("utf-8", errors="replace")
    if hasattr(obj, "isoformat"):
        return obj.isoformat()
    if isinstance(obj, (str, int, float, bool)) or obj is None:
        return obj
    return str(obj)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=["from-raw", "fetch"], required=True)
    parser.add_argument("--schema", default="rick_messages_tasks")
    parser.add_argument("--batch", type=int, default=500)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--only-telegraph", action="store_true")
    parser.add_argument("--sleep", type=float, default=2.0, help="fetch: пауза между запросами")
    parser.add_argument("--update-raw", action="store_true", help="fetch: писать cached_page в raw")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    started = time.time()
    conn = _connect(_postgres_url())
    try:
        if args.mode == "from-raw":
            stats = backfill_from_raw(
                conn, args.schema, batch=args.batch, limit=args.limit,
                only_telegraph=args.only_telegraph, dry_run=args.dry_run,
            )
        else:
            stats = asyncio.run(
                backfill_fetch(
                    conn, args.schema, limit=args.limit, only_telegraph=args.only_telegraph,
                    sleep_seconds=args.sleep, update_raw=args.update_raw, dry_run=args.dry_run,
                )
            )
    finally:
        conn.close()
    stats["elapsed_s"] = int(time.time() - started)
    print(json.dumps({"mode": args.mode, "dry_run": args.dry_run, **stats}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
