#!/usr/bin/env python3
"""Lookup Telegram chat_id by title or username from Supabase index.

JTBD: Когда агенту нужен chat_id по имени/username без вызова get_direct_chat_by_contact,
запустить этот скрипт после синхронизации чатов в Supabase (sync_telegram_chats_to_supabase).

Usage:
  python -m heroes_platform.heroes_telegram_mcp.scripts.lookup_telegram_chat "Krasinsky"
  python -m heroes_platform.heroes_telegram_mcp.scripts.lookup_telegram_chat "rick.ai" --limit 5

Output: JSON array of {chat_id, chat_title, chat_username, chat_type}.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

script_dir = Path(__file__).resolve().parent
workspace_root = script_dir.parents[3]
sys.path.insert(0, str(workspace_root))

from heroes_platform.shared.import_setup import enable

enable(__file__)

from heroes_platform.heroes_telegram_mcp.supabase_writer import SupabaseWriter


def main() -> int:
    parser = argparse.ArgumentParser(description="Lookup Telegram chat_id from Supabase index")
    parser.add_argument("query", help="Search in chat_title and chat_username")
    parser.add_argument("--limit", type=int, default=10, help="Max results")
    args = parser.parse_args()

    try:
        writer = SupabaseWriter()
        rows = writer.lookup_chats_by_query(args.query, limit=args.limit)
    except Exception as e:
        print(json.dumps({"error": str(e)}), file=sys.stderr)
        return 1

    print(json.dumps(rows, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
