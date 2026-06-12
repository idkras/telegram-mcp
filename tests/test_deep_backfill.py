"""Tests for deep_backfill.py — backward-walk полной истории чатов.

JTBD: Verify that deep_backfill goes BACKWARD from floor, is resumable across
runs, marks backfill_completed when history is exhausted, isolates per-chat
failures, retries FloodWait, refuses to false-green on a dead session, and
stays universal (telegram_user_id-driven, no hardcode).

Data source: Unit tests with fake async client + fake writer (no live Telegram /
Supabase). Async tests run via asyncio.run() so they need no pytest-asyncio.
"""

from __future__ import annotations

import asyncio
import os
import sys

# Ensure heroes_platform is importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

from heroes_platform.heroes_telegram_mcp import deep_backfill as db  # noqa: E402
from heroes_platform.heroes_telegram_mcp.deep_backfill import (  # noqa: E402
    DeepBackfillChatResult,
    _flood_wait_seconds,
    _resolve_floor,
    deep_backfill_all_chats,
    deep_backfill_one_chat,
)


def run(coro):
    # Сброс module-level lock между прогонами (asyncio.Lock биндится на loop).
    db._deep_lock = None
    return asyncio.run(coro)


# ── fakes ─────────────────────────────────────────────────────────────────────
class FakeMsg:
    def __init__(self, mid: int):
        self.id = mid


class FloodWait(Exception):
    """Стенд-ин для telethon FloodWaitError (атрибут .seconds)."""

    def __init__(self, seconds):
        self.seconds = seconds
        super().__init__(f"flood {seconds}s")


class FakeClient:
    """История чата хранится как отсортированный по id list (descending in feeder).

    Telegram contract:
        iter_messages(entity, offset_id=F, limit=L) → возвращает сообщения с id < F
        в порядке убывания (свежее → старее). offset_id=0 → от newest.
    """

    def __init__(self, messages_by_chat=None, alive=True):
        self._messages = {
            int(k): sorted(list(v), key=lambda m: m.id, reverse=True)
            for k, v in (messages_by_chat or {}).items()
        }
        self._alive = alive

    async def is_user_authorized(self):
        return self._alive

    async def get_me(self):
        if not self._alive:
            raise RuntimeError("AUTHKEY_DUPLICATED")
        return object()

    async def iter_messages(self, entity, limit=None, offset_id=None, **_):
        msgs = list(self._messages.get(int(entity), []))
        # offset_id=0 OR None → от newest. offset_id=F>0 → только id < F.
        if offset_id and offset_id > 0:
            msgs = [m for m in msgs if m.id < int(offset_id)]
        if limit is not None:
            msgs = msgs[: int(limit)]
        for m in msgs:
            yield m


class FakeWriter:
    """Фейк SupabaseWriter с in-memory store для cursor state."""

    def __init__(
        self,
        cursors=None,
        telegram_user_id="ikrasinsky",
        fail_chats=None,
        flood_chats=None,
    ):
        self.telegram_user_id = telegram_user_id
        self.batch_size = 100
        self.schema = "rick_messages_tasks"
        self._cursors = {int(k): dict(v) for k, v in (cursors or {}).items()}
        self._fail_chats = set(fail_chats or [])
        # flood_chats: chat_id -> remaining_floods (decrement per call)
        self._flood_chats = dict(flood_chats or {})
        self.written_batches = []
        self.cursor_updates = []
        self.runtime_events = []

    async def get_chat_cursor(self, chat_id):
        return self._cursors.get(int(chat_id))

    async def write_messages_batch(self, batch, chat_id, chat_type):
        if int(chat_id) in self._fail_chats:
            raise RuntimeError("simulated write failure")
        if int(chat_id) in self._flood_chats and self._flood_chats[int(chat_id)] > 0:
            self._flood_chats[int(chat_id)] -= 1
            raise FloodWait(0)  # 0s flood → instant retry
        n = len(batch)
        self.written_batches.append((int(chat_id), n))
        return n

    async def update_chat_cursor(
        self,
        chat_id,
        last_seen_message_id=None,
        last_backfill_message_id=None,
        backfill_completed=None,
    ):
        self.cursor_updates.append(
            (int(chat_id), last_seen_message_id, last_backfill_message_id, backfill_completed)
        )
        # Apply to in-memory store (имитация UPSERT в chat_state)
        c = self._cursors.setdefault(int(chat_id), {})
        if last_backfill_message_id is not None:
            c["last_backfill_message_id"] = last_backfill_message_id
        if last_seen_message_id is not None:
            c["last_seen_message_id"] = last_seen_message_id
        if backfill_completed is not None:
            c["backfill_completed"] = backfill_completed
        return True

    async def record_runtime_event(self, mode, processed_chats=0, inserted_messages=0, error=None):
        self.runtime_events.append((mode, processed_chats, inserted_messages, error))


# ── unit tests for pure helpers ───────────────────────────────────────────────
def test_resolve_floor_prefers_last_backfill():
    cursor = {"last_backfill_message_id": 100, "last_seen_message_id": 500}
    assert _resolve_floor(cursor) == 100


def test_resolve_floor_falls_back_to_last_seen():
    cursor = {"last_backfill_message_id": None, "last_seen_message_id": 500}
    assert _resolve_floor(cursor) == 500


def test_resolve_floor_returns_zero_when_no_cursor():
    assert _resolve_floor(None) == 0
    assert _resolve_floor({}) == 0
    assert _resolve_floor({"last_seen_message_id": None}) == 0


def test_flood_wait_seconds_detects():
    assert _flood_wait_seconds(FloodWait(42)) == 42


def test_flood_wait_seconds_ignores_other_errors():
    assert _flood_wait_seconds(RuntimeError("other")) is None


# ── T1: backward-walk first pass от last_seen floor ──────────────────────────
def test_backward_walk_first_pass_from_last_seen():
    """Чат с last_seen=200, last_backfill=NULL. Должны взять msgs id<200."""
    msgs = [FakeMsg(i) for i in range(1, 250)]  # ids 1..249
    writer = FakeWriter(cursors={777: {"last_seen_message_id": 200}})
    client = FakeClient(messages_by_chat={777: msgs})
    res = run(
        deep_backfill_one_chat(
            client, writer, 777, "private", per_run_limit=50
        )
    )
    # Должны записать 50 сообщений от floor=200 вниз (ids 150..199 descending)
    assert res.written == 50
    assert res.floor_before == 200
    # min_id_seen = id 150
    assert res.min_id_seen == 150
    assert res.floor_after == 150
    assert res.completed is False  # история длиннее, ещё есть что догнать
    # Cursor update: floor moved to 150, not completed
    assert (777, None, 150, False) in writer.cursor_updates


# ── T2: resumable — второй проход продвигает floor ниже ───────────────────────
def test_resumable_second_pass_extends_floor_down():
    """Шаг 1 уехал до floor=150. Шаг 2 стартует с 150 → должен идти вниз ещё."""
    msgs = [FakeMsg(i) for i in range(1, 250)]
    writer = FakeWriter(
        cursors={
            777: {
                "last_seen_message_id": 200,
                "last_backfill_message_id": 150,  # после первого прохода
            }
        }
    )
    client = FakeClient(messages_by_chat={777: msgs})
    res = run(
        deep_backfill_one_chat(
            client, writer, 777, "private", per_run_limit=50
        )
    )
    # Floor=150 → берём msgs id<150, limit=50 → ids 100..149 → min=100
    assert res.written == 50
    assert res.floor_before == 150
    assert res.min_id_seen == 100
    assert res.floor_after == 100
    assert res.completed is False


# ── T3: backfill_completed=TRUE когда история исчерпана ──────────────────────
def test_backfill_completed_when_history_exhausted():
    """Чат с msgs 1..30, floor=20, limit=50. Получим 19 сообщений (1..19) <
    limit → terminal."""
    msgs = [FakeMsg(i) for i in range(1, 31)]
    writer = FakeWriter(cursors={888: {"last_backfill_message_id": 20}})
    client = FakeClient(messages_by_chat={888: msgs})
    res = run(
        deep_backfill_one_chat(
            client, writer, 888, "private", per_run_limit=50
        )
    )
    assert res.written == 19
    assert res.min_id_seen == 1
    assert res.completed is True
    assert (888, None, 1, True) in writer.cursor_updates


# ── T4: уже completed — пропускаем без работы ─────────────────────────────────
def test_skip_when_already_completed():
    writer = FakeWriter(cursors={999: {"backfill_completed": True}})
    client = FakeClient(messages_by_chat={999: [FakeMsg(1)]})
    res = run(deep_backfill_one_chat(client, writer, 999, "private", per_run_limit=50))
    assert res.completed is True
    assert res.written == 0
    assert writer.written_batches == []
    assert writer.cursor_updates == []  # ничего не делали


# ── T5: пустой чат — терминал сразу ───────────────────────────────────────────
def test_empty_chat_marks_completed():
    """Чат без сообщений (либо floor=last_seen уже на дне) → 0 seen → terminal."""
    writer = FakeWriter(cursors={1001: {"last_seen_message_id": 5}})
    client = FakeClient(messages_by_chat={1001: []})
    res = run(
        deep_backfill_one_chat(client, writer, 1001, "private", per_run_limit=50)
    )
    assert res.written == 0
    assert res.completed is True
    # cursor_update только с backfill_completed=True (без min_id)
    assert (1001, None, None, True) in writer.cursor_updates


# ── T6: FloodWait → sleep + retry проходит ────────────────────────────────────
def test_floodwait_retry_succeeds():
    """Одна FloodWait → retry → проход успешен."""
    msgs = [FakeMsg(i) for i in range(1, 11)]
    writer = FakeWriter(
        cursors={2002: {"last_backfill_message_id": 100}},
        flood_chats={2002: 1},  # 1 flood → retry success
    )
    client = FakeClient(messages_by_chat={2002: msgs})
    res = run(
        deep_backfill_one_chat(client, writer, 2002, "private", per_run_limit=50)
    )
    # 10 msgs < 50 limit → completed
    assert res.written == 10
    assert res.completed is True
    assert res.error is None


# ── T7: write failure → error filled, не падает ──────────────────────────────
def test_write_failure_is_isolated():
    msgs = [FakeMsg(i) for i in range(1, 11)]
    writer = FakeWriter(
        cursors={3003: {"last_backfill_message_id": 100}},
        fail_chats={3003},
    )
    client = FakeClient(messages_by_chat={3003: msgs})
    res = run(
        deep_backfill_one_chat(client, writer, 3003, "private", per_run_limit=50)
    )
    assert res.error is not None
    assert "simulated" in res.error
    assert res.completed is False


# ── T8: оркестратор с explicit_chats — обходит DB selection ──────────────────
def test_orchestrator_explicit_chats():
    writer = FakeWriter(
        cursors={
            1: {"last_seen_message_id": 50},
            2: {"last_seen_message_id": 100},
            3: {"backfill_completed": True},
        }
    )
    client = FakeClient(
        messages_by_chat={
            1: [FakeMsg(i) for i in range(1, 30)],
            2: [FakeMsg(i) for i in range(1, 60)],
            3: [FakeMsg(1)],
        }
    )
    res = run(
        deep_backfill_all_chats(
            client,
            writer,
            total_budget=1000,
            per_chat_limit=100,
            explicit_chats=[
                ("1", "private"),
                ("2", "private"),
                ("3", "private"),
            ],
        )
    )
    assert res.chats_processed == 3
    # chat 3 уже completed → 0 written; 1 и 2 завершены
    assert res.chats_completed == 3  # все терминальные
    assert res.chats_failed == 0
    assert res.messages_written == 29 + 59  # ids 1..29 и 1..59


# ── T9: budget exhaustion — честный partial marker ───────────────────────────
def test_budget_exhaustion_stops_orchestrator():
    writer = FakeWriter(
        cursors={
            10: {"last_seen_message_id": 50},
            20: {"last_seen_message_id": 50},
        }
    )
    client = FakeClient(
        messages_by_chat={
            10: [FakeMsg(i) for i in range(1, 50)],
            20: [FakeMsg(i) for i in range(1, 50)],
        }
    )
    res = run(
        deep_backfill_all_chats(
            client,
            writer,
            total_budget=20,  # очень мало
            per_chat_limit=100,
            explicit_chats=[("10", "private"), ("20", "private")],
        )
    )
    # После chat 10 (49 msgs) budget уже exhausted → chat 20 skipped
    assert res.budget_exhausted is True
    assert res.chats_processed == 1  # только chat 10
    assert res.messages_written == 49
    # Маркер — budget_exhausted
    assert any(
        ev[0] == "deep_backfill_budget_exhausted" for ev in writer.runtime_events
    )


# ── T10: DEAD session — session_dead маркер, не зелёный ──────────────────────
def test_dead_session_no_false_green():
    writer = FakeWriter(cursors={1: {"last_seen_message_id": 100}})
    client = FakeClient(
        messages_by_chat={1: [FakeMsg(i) for i in range(1, 50)]},
        alive=False,  # сессия мертва
    )
    res = run(
        deep_backfill_all_chats(
            client,
            writer,
            total_budget=1000,
            per_chat_limit=100,
            explicit_chats=[("1", "private")],
        )
    )
    assert res.session_dead is True
    assert res.messages_written == 0
    assert any(
        ev[0] == "deep_backfill_session_dead" for ev in writer.runtime_events
    )


# ── T11: no chats to backfill — no-op маркер ─────────────────────────────────
def test_no_chats_records_ok_marker():
    writer = FakeWriter()
    client = FakeClient(messages_by_chat={})
    res = run(
        deep_backfill_all_chats(
            client,
            writer,
            explicit_chats=[],
        )
    )
    assert res.chats_processed == 0
    assert res.messages_written == 0
    assert any(ev[0] == "deep_backfill_ok" for ev in writer.runtime_events)


# ── T12: marker_mode contract ─────────────────────────────────────────────────
def test_marker_mode_session_dead_takes_priority():
    r = DeepBackfillChatResult(chat_id="1", error="boom")  # noqa: F841
    from heroes_platform.heroes_telegram_mcp.deep_backfill import (
        DeepBackfillRunResult,
    )

    res = DeepBackfillRunResult(session_dead=True, chats_failed=1)
    assert res.marker_mode() == "deep_backfill_session_dead"
    res2 = DeepBackfillRunResult(chats_failed=1)
    assert res2.marker_mode() == "deep_backfill_partial"
    res3 = DeepBackfillRunResult(budget_exhausted=True)
    assert res3.marker_mode() == "deep_backfill_budget_exhausted"
    res4 = DeepBackfillRunResult()
    assert res4.marker_mode() == "deep_backfill_ok"
