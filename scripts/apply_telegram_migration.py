#!/usr/bin/env python3
"""Apply Telegram TDLib migration to Supabase via direct Postgres connection.

JTBD: Когда нужно применить миграцию 0.4 (таблицы telegram_chats, telegram_messages_raw,
telegram_ingest_runs) или segment (ik_telegram_chats, rick_telegram_chats) в Supabase,
я хочу запустить один скрипт с SUPABASE_DB_URL, чтобы агент или CI могли применить
DDL без ручного SQL Editor. Подключение и выполнение SQL — через rickai_mcp.supabase_postgres.

Использование:
- Задать в окружении SUPABASE_DB_URL (или Keychain supabase_rick_db_url).
- Запуск: python -m heroes_platform.heroes_telegram_mcp.scripts.apply_telegram_migration
- Опционально: APPLY_MIGRATION_FILE=/path/to/file.sql (по умолчанию — миграция 0.4).

Секреты: только в env/Keychain, не коммитить в репо.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

from heroes_platform.rickai_mcp.supabase_postgres import (
    apply_migration as supabase_apply_migration,
    get_supabase_postgres_url,
)

# Repo root for default migration path
REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent
DEFAULT_MIGRATION = (
    REPO_ROOT
    / "heroes_platform/heroes_telegram_mcp/sql_migrations/20250110000001_telegram_tdlib_tables.sql"
)


def main() -> int:
    db_url = get_supabase_postgres_url()
    if not db_url:
        print(
            "SUPABASE_DB_URL не задан. Задайте в env или Keychain (supabase_rick_db_url). "
            "Connection string: Supabase Dashboard → Settings → Database (Session/Transaction).",
            file=sys.stderr,
        )
        return 1

    migration_path = Path(os.getenv("APPLY_MIGRATION_FILE", str(DEFAULT_MIGRATION)))
    result = supabase_apply_migration(migration_path, postgres_url=db_url)
    if not result.get("success"):
        print(result.get("error", result.get("message", "Unknown error")), file=sys.stderr)
        return 1
    print(result.get("message", f"Миграция применена: {migration_path.name}"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
