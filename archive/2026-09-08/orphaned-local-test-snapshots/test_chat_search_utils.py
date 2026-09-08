#!/usr/bin/env python3
"""
Unit tests for chat_search_utils (max_dialogs_to_scan, limit).

JTBD: Guard against unbounded iter_dialogs() that caused Telegram MCP freeze (ai.incidents 15 Feb 2026).
"""
from __future__ import annotations

import pytest
from unittest.mock import MagicMock

from heroes_platform.heroes_telegram_mcp.chat_search_utils import (
    search_chats_by_keyword_impl,
    get_all_chats_list_impl,
    get_chat_type,
    format_chat_info,
)


def _make_entity(eid: int, title: str = "", first_name: str = "", username: str | None = None, is_user: bool = False):
    """Minimal entity-like object for tests."""
    entity = MagicMock()
    entity.id = eid
    entity.title = title
    entity.first_name = first_name
    entity.username = username
    if is_user:
        entity.__class__.__name__ = "User"
    return entity


def _make_dialog(entity, unread_count: int = 0):
    dialog = MagicMock()
    dialog.entity = entity
    dialog.unread_count = unread_count
    return dialog


async def _async_gen(items):
    for x in items:
        yield x


@pytest.mark.asyncio
async def test_search_chats_by_keyword_stops_at_max_dialogs_to_scan():
    """When max_dialogs_to_scan=5, iteration stops after 5 dialogs (no unbounded scan)."""
    # Build 20 fake dialogs; all have "test" in title so all would match
    dialogs = [
        _make_dialog(_make_entity(i, title=f"test chat {i}"))
        for i in range(20)
    ]
    client = MagicMock()
    client.iter_dialogs = lambda: _async_gen(dialogs)

    result = await search_chats_by_keyword_impl(
        client, "test", chat_type=None, limit=None, max_dialogs_to_scan=5
    )

    assert result["dialogs_scanned"] <= 5
    assert result["total_found"] <= 5
    assert "chats" in result


@pytest.mark.asyncio
async def test_search_chats_by_keyword_respects_limit():
    """When limit=2, at most 2 chats returned."""
    dialogs = [
        _make_dialog(_make_entity(i, title=f"foo {i}"))
        for i in range(10)
    ]
    client = MagicMock()
    client.iter_dialogs = lambda: _async_gen(dialogs)

    result = await search_chats_by_keyword_impl(
        client, "foo", chat_type=None, limit=2, max_dialogs_to_scan=100
    )

    assert result["total_found"] == 2
    assert len(result["chats"]) == 2


@pytest.mark.asyncio
async def test_get_all_chats_list_stops_at_max_dialogs_to_scan():
    """When max_dialogs_to_scan=5, get_all_chats_list stops after 5 dialogs."""
    dialogs = [_make_dialog(_make_entity(i, title=f"Chat {i}")) for i in range(20)]
    client = MagicMock()
    client.iter_dialogs = lambda: _async_gen(dialogs)

    result = await get_all_chats_list_impl(
        client, chat_type=None, limit=None, max_dialogs_to_scan=5
    )

    assert result["dialogs_scanned"] <= 5
    assert result["total_chats"] <= 5
    assert "chats" in result


@pytest.mark.asyncio
async def test_get_all_chats_list_respects_limit():
    """When limit=3, at most 3 chats returned."""
    dialogs = [_make_dialog(_make_entity(i, title=f"C {i}")) for i in range(10)]
    client = MagicMock()
    client.iter_dialogs = lambda: _async_gen(dialogs)

    result = await get_all_chats_list_impl(
        client, chat_type=None, limit=3, max_dialogs_to_scan=100
    )

    assert result["total_chats"] == 3
    assert len(result["chats"]) == 3


@pytest.mark.asyncio
async def test_search_chats_max_dialogs_none_can_scan_all():
    """When max_dialogs_to_scan=None, all given dialogs are scanned (no cap)."""
    dialogs = [_make_dialog(_make_entity(i, title=f"x {i}")) for i in range(7)]
    client = MagicMock()
    client.iter_dialogs = lambda: _async_gen(dialogs)

    result = await search_chats_by_keyword_impl(
        client, "x", chat_type=None, limit=None, max_dialogs_to_scan=None
    )

    assert result["dialogs_scanned"] == 7
    assert result["total_found"] == 7
    assert len(result["chats"]) == 7
