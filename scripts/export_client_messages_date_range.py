#!/usr/bin/env python3
"""Export Telegram messages for client chats over a date range to one Markdown file.

Usage:
  python -m heroes_platform.heroes_telegram_mcp.scripts.export_client_messages_date_range vipavenue-ru
  python -m heroes_platform.heroes_telegram_mcp.scripts.export_client_messages_date_range vipavenue-ru --from-date 2026-01-16 --to-date 2026-02-16
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

script_dir = Path(__file__).resolve().parent
workspace_root = script_dir.parents[3]
sys.path.insert(0, str(workspace_root))
from heroes_platform.shared.import_setup import enable

enable(__file__)
from heroes_platform.shared.credentials_wrapper import get_service_credentials
from heroes_platform.heroes_telegram_mcp.scripts.supabase_chats_by_client import (
    find_chats_by_client_alias,
)
from telethon import TelegramClient
from telethon.sessions import StringSession

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def _sender_name(msg):
    if not getattr(msg, "sender", None):
        return "Unknown"
    s = msg.sender
    if getattr(s, "title", None):
        return (s.title or "").strip() or "Unknown"
    first = getattr(s, "first_name", "") or ""
    last = getattr(s, "last_name", "") or ""
    return (first + " " + last).strip() or "Unknown"


def _safe_iterate(messages):
    if messages is None:
        return
    if isinstance(messages, list):
        for m in messages:
            yield m
    else:
        yield messages


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("client_alias", help="e.g. vipavenue-ru")
    parser.add_argument("--from-date", default=None, help="YYYY-MM-DD")
    parser.add_argument("--to-date", default=None, help="YYYY-MM-DD")
    parser.add_argument("--limit-per-chat", type=int, default=1500)
    parser.add_argument("--output", default=None, help="Output .md path")
    args = parser.parse_args()

    today = datetime.now(timezone.utc).date()
    from_date_s = args.from_date or (today - timedelta(days=30)).strftime("%Y-%m-%d")
    to_date_s = args.to_date or today.strftime("%Y-%m-%d")
    try:
        from_dt = datetime.strptime(from_date_s, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        to_dt = datetime.strptime(to_date_s, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        to_dt = to_dt + timedelta(days=1, microseconds=-1)
    except ValueError as e:
        logger.error("Invalid date: %s", e)
        return 1

    result = find_chats_by_client_alias(args.client_alias, limit=100)
    if not result.get("success") or not result.get("chats"):
        logger.error("No chats: %s", result.get("error", "empty"))
        return 1
    chats = result["chats"]
    logger.info("Found %d chats", len(chats))

    creds = get_service_credentials("telegram")
    if not creds:
        logger.error("No Telegram credentials")
        return 1
    api_id = int(creds.get("TELEGRAM_API_ID", 0))
    api_hash = creds.get("TELEGRAM_API_HASH", "")
    session_str = creds.get("TELEGRAM_SESSION_STRING", "")
    if not api_hash or api_id == 0:
        logger.error("Invalid Telegram credentials")
        return 1

    if session_str:
        client = TelegramClient(StringSession(session_str), api_id, api_hash)
    else:
        client = TelegramClient("telegram_session", api_id, api_hash)
    await client.start()

    out_lines = [
        "# Telegram export: " + args.client_alias,
        "Period: " + from_date_s + " to " + to_date_s,
        "",
    ]
    total_messages = 0

    for i, c in enumerate(chats, 1):
        chat_id = c.get("chat_id")
        title = (c.get("chat_title") or "").strip() or ("Chat " + str(chat_id))
        logger.info("[%d/%d] %s", i, len(chats), title[:50])
        try:
            try:
                entity = await client.get_entity(int(chat_id))
            except Exception as e1:
                if "PeerUser" in str(e1) and isinstance(chat_id, (int, str)):
                    # Supergroup/channel: try -100xxxxxxxxxx format (Telethon convention)
                    full_id = -(1000000000000 + int(chat_id))
                    entity = await client.get_entity(full_id)
                else:
                    raise
            messages = await client.get_messages(entity, limit=args.limit_per_chat)
            filtered = []
            for msg in _safe_iterate(messages):
                if not hasattr(msg, "date") or not msg.date:
                    continue
                if msg.date < from_dt or msg.date > to_dt:
                    continue
                filtered.append(msg)
            if not filtered:
                out_lines.append("## " + title + " (id=" + str(chat_id) + ")")
                out_lines.append("(no messages in range)")
                out_lines.append("")
                continue
            out_lines.append("## " + title + " (id=" + str(chat_id) + ")")
            out_lines.append("")
            for msg in sorted(filtered, key=lambda m: m.date):
                sender = _sender_name(msg)
                text = (msg.message or "").strip() or "[media/no text]"
                out_lines.append(
                    "- **"
                    + msg.date.strftime("%Y-%m-%d %H:%M")
                    + "** | "
                    + sender
                    + ": "
                    + text[:500]
                )
            out_lines.append("")
            total_messages += len(filtered)
        except Exception as e:
            logger.warning("  -> %s", e)
            out_lines.append("## " + title + " (id=" + str(chat_id) + ")")
            out_lines.append("(error: " + str(e) + ")")
            out_lines.append("")
        await asyncio.sleep(0.3)

    await client.disconnect()

    if args.output:
        out_path = Path(args.output)
    else:
        base = workspace_root / "[rick.ai]" / "clients" / "all-clients"
        alias_dir = args.client_alias.replace(".", "-").replace("_", "-")
        folder = base / alias_dir
        if not folder.exists():
            folder = (
                workspace_root / "heroes_platform" / "heroes_telegram_mcp" / "scripts" / "exports"
            )
            folder.mkdir(parents=True, exist_ok=True)
        out_path = folder / ("telegram-export-" + from_date_s + "-" + to_date_s + ".md")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(out_lines), encoding="utf-8")
    logger.info("Wrote %d messages to %s", total_messages, out_path)
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
