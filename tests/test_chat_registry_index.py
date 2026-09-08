"""Unit tests for canonical chat registry indexing helpers."""

from heroes_platform.heroes_telegram_mcp.chat_registry_index import (
    build_family_rows,
    classify_slack_channel,
    classify_telegram_chat,
    extract_client_alias,
)


def test_extract_client_alias_from_rick_title():
    assert extract_client_alias("[vipavenue.ru + rick.ai] полезные отчеты", "") == "vipavenue-ru"


def test_extract_client_alias_ignores_reserved_product_domains():
    assert extract_client_alias("BuildStore & rick.ai", "") is None
    assert extract_client_alias("link https://youtu.be/example", "") is None


def test_classify_telegram_client_report_chat():
    result = classify_telegram_chat(
        title="[vipavenue.ru + rick.ai] полезные отчеты",
        username="",
        chat_type="supergroup",
    )
    assert result["entity_kind"] == "client"
    assert result["product_key"] == "rick-ai"
    assert result["project_key"] == "na_soprovozhdenii"
    assert result["client_alias"] == "vipavenue-ru"


def test_classify_telegram_dogovor_chat():
    result = classify_telegram_chat(
        title="askona.ru 🚀 rick.ai чат договорённостей",
        username="",
        chat_type="supergroup",
    )
    assert result["entity_kind"] == "client"
    assert result["project_key"] == "dogovorennosti"
    assert result["client_alias"] == "askona-ru"


def test_classify_slack_internal_channel():
    result = classify_slack_channel(channel_name="heroes-monitoring", team_id="T1", is_private=False)
    assert result["entity_kind"] == "internal"
    assert result["project_key"] == "monitoring"
    assert result["product_key"] == "heroes"


def test_build_family_rows_marks_canonical_and_duplicates():
    rows = [
        {
            "platform": "telegram",
            "chat_id": "1",
            "chat_title": "[vipavenue.ru + rick.ai] полезные отчеты",
            "entity_kind": "client",
            "segment": "na_soprovozhdenii",
            "product_key": "rick-ai",
            "project_key": "na_soprovozhdenii",
            "client_alias": "vipavenue-ru",
            "family_key": "telegram:vipavenue-ru:rick-ai:na_soprovozhdenii",
            "source_table": "telegram_chats",
            "source_row_updated_at": "2026-03-28T10:00:00+00:00",
            "source_last_seen_ts": "2026-03-28T10:00:00+00:00",
            "index_rule": "client-alias+product",
            "provenance_json": {"is_active": True},
        },
        {
            "platform": "telegram",
            "chat_id": "2",
            "chat_title": "[vipavenue.ru + rick.ai] полезные отчеты",
            "entity_kind": "client",
            "segment": "na_soprovozhdenii",
            "product_key": "rick-ai",
            "project_key": "na_soprovozhdenii",
            "client_alias": "vipavenue-ru",
            "family_key": "telegram:vipavenue-ru:rick-ai:na_soprovozhdenii",
            "source_table": "telegram_chats",
            "source_row_updated_at": "2026-03-20T10:00:00+00:00",
            "source_last_seen_ts": "2026-03-20T10:00:00+00:00",
            "index_rule": "client-alias+product",
            "provenance_json": {"is_active": True},
        },
    ]

    index_rows, family_rows = build_family_rows(rows)
    assert len(index_rows) == 2
    assert len(family_rows) == 1
    assert family_rows[0]["canonical_chat_id"] == "1"
    assert family_rows[0]["member_count"] == 2
    assert {row["duplicate_count"] for row in index_rows} == {2}
    assert sum(1 for row in index_rows if row["is_canonical"]) == 1
