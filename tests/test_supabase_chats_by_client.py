from __future__ import annotations

import json
from datetime import datetime, timezone

from heroes_platform.heroes_telegram_mcp.scripts import supabase_chats_by_client as subject


def test_helper_is_explicitly_monorepo_only() -> None:
    assert subject.RUNTIME_SCOPE == "monorepo-only"


def test_search_key_ignores_domain_and_title_separators() -> None:
    assert subject._search_key_from_alias("fizikl-org") == "fiziklorg"
    assert subject._search_key_from_alias("VIPAvenue-ru") == "vipavenue"
    assert subject._search_key_from_alias("elyts.ru") == "elyts"


def test_empty_alias_fails_before_database_call(monkeypatch) -> None:
    called = False

    def unexpected(_sql: str):
        nonlocal called
        called = True
        return {"success": True, "rows": []}

    monkeypatch.setattr(
        "heroes_platform.rickai_mcp.supabase_postgres.run_sql_fetch", unexpected
    )
    result = subject.find_chats_by_client_alias("---")
    assert result == {
        "success": False,
        "error": "Empty client alias",
        "chats": [],
        "count": 0,
    }
    assert not called


def test_query_normalizes_database_titles(monkeypatch) -> None:
    captured: dict[str, str] = {}
    row = {
        "chat_id": "3173965368",
        "chat_title": "[fizikl.org + rick.ai] полезные отчеты из сквозной аналитики",
        "chat_username": None,
        "chat_type": "supergroup",
    }

    def fake_run_sql_fetch(sql: str):
        captured["sql"] = sql
        return {"success": True, "rows": [row]}

    monkeypatch.setattr(
        "heroes_platform.rickai_mcp.supabase_postgres.run_sql_fetch", fake_run_sql_fetch
    )
    result = subject.find_chats_by_client_alias("fizikl-org")

    assert result == {"success": True, "chats": [row], "count": 1}
    assert "regexp_replace(lower(coalesce(chat_title" in captured["sql"]
    assert "LIKE '%fiziklorg%'" in captured["sql"]


def test_postgres_timestamp_serializes_as_iso8601() -> None:
    value = datetime(2026, 8, 4, 12, 3, 34, tzinfo=timezone.utc)

    rendered = json.dumps({"message_ts": value}, default=subject._json_default)

    assert rendered == '{"message_ts": "2026-08-04T12:03:34+00:00"}'


def test_message_ids_include_telegram_supergroup_peer_form() -> None:
    chats = [
        {"chat_id": "3702464665", "chat_type": "supergroup"},
        {"chat_id": "-3702464665", "chat_type": "supergroup"},
        {"chat_id": "5421923777", "chat_type": "group"},
        {"chat_id": "5421923777", "chat_type": "group"},
        {"chat_id": "-1002569706168", "chat_type": "supergroup"},
    ]

    assert subject._message_chat_ids(chats) == [
        "3702464665",
        "-1003702464665",
        "-3702464665",
        "5421923777",
        "-1002569706168",
    ]
    assert "-100-3702464665" not in subject._message_chat_ids(chats)
