"""Tests for startup_backfill.py — оркестрация догона Telegram -> Supabase.

JTBD: Verify that the startup/periodic backfill orchestrator catches up missed
messages across ALL chats, seeds cursor-less chats incrementally, isolates
per-chat failures, is idempotent, refuses to false-green on a dead session,
handles FloodWait, and stays universal (telegram_user_id-driven, no hardcode).

Data source: Unit tests with fake async client + fake writer (no live Telegram /
Supabase). Async tests run via asyncio.run() so they need no pytest-asyncio config.
"""

from __future__ import annotations

import asyncio
import os
import sys

# Ensure heroes_platform is importable (same pattern as test_supabase_writer.py)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

from heroes_platform.heroes_telegram_mcp import startup_backfill as sb  # noqa: E402
from heroes_platform.heroes_telegram_mcp.startup_backfill import (  # noqa: E402
    BackfillResult,
    _chat_type_from_dialog,
    _seed_recent,
    backfill_all_chats,
    backfill_one_chat,
    run_startup_backfill,
    schedule_backfill_tasks,
)


def run(coro):
    # Сброс module-level lock: каждый asyncio.run() создаёт новый loop, а
    # asyncio.Lock биндится лениво на первый loop — переиспользование падает.
    sb._backfill_lock = None
    return asyncio.run(coro)


# ── fakes ─────────────────────────────────────────────────────────────────────
class FakeMsg:
    def __init__(self, mid: int):
        self.id = mid


class FakeEntity:
    def __init__(self, **flags):
        for k, v in flags.items():
            setattr(self, k, v)


class FakeDialog:
    def __init__(self, did: int, entity=None):
        self.id = did
        self.entity = entity


class FloodWait(Exception):
    """Стенд-ин для telethon FloodWaitError (у него атрибут .seconds)."""

    def __init__(self, seconds):
        self.seconds = seconds
        super().__init__(f"flood {seconds}s")


class FakeClient:
    def __init__(self, dialogs, messages_by_chat=None, alive=True, dialogs_raise=None):
        self._dialogs = dialogs
        self._messages_by_chat = messages_by_chat or {}
        self._alive = alive
        self._dialogs_raise = dialogs_raise

    async def is_user_authorized(self):
        return self._alive

    async def get_me(self):
        if not self._alive:
            raise RuntimeError("AUTHKEY_DUPLICATED")
        return object()

    async def iter_dialogs(self):
        if self._dialogs_raise is not None:
            raise self._dialogs_raise
        for d in self._dialogs:
            yield d

    async def iter_messages(self, entity, limit=None, reverse=False):
        msgs = list(self._messages_by_chat.get(int(entity), []))
        if reverse:
            msgs = sorted(msgs, key=lambda m: m.id)
        if limit is not None:
            msgs = msgs[:limit]
        for m in msgs:
            yield m


class FakeWriter:
    def __init__(
        self,
        cursors=None,
        telegram_user_id="ikrasinsky",
        catch_up_returns=None,
        fail_chats=None,
        flood_chats=None,
    ):
        self.telegram_user_id = telegram_user_id
        self.batch_size = 2
        self._cursors = dict(cursors or {})
        self._catch_up_returns = catch_up_returns or {}
        self._fail_chats = set(fail_chats or [])
        self._flood_chats = dict(flood_chats or {})  # chat_id -> seconds (one-shot)
        self.written_batches = []
        self.cursor_updates = []
        self.catch_up_calls = []
        self.runtime_events = []  # (mode, processed_chats, inserted_messages)

    async def get_chat_cursor(self, chat_id):
        return self._cursors.get(int(chat_id))

    async def catch_up_recent(self, client, chat_id, chat_type="unknown", limit=1000):
        if int(chat_id) in self._fail_chats:
            raise RuntimeError("simulated catch_up failure")
        if int(chat_id) in self._flood_chats:
            secs = self._flood_chats.pop(int(chat_id))  # one-shot: retry succeeds
            raise FloodWait(secs)
        self.catch_up_calls.append((int(chat_id), chat_type, limit))
        return int(self._catch_up_returns.get(int(chat_id), 0))

    async def write_messages_batch(self, batch, chat_id, chat_type, chat_title=None):
        if int(chat_id) in self._fail_chats:
            raise RuntimeError("simulated write failure")
        n = len(batch)
        self.written_batches.append((int(chat_id), n, chat_title))
        return n

    async def update_chat_cursor(self, chat_id, last_seen_message_id=None, **kw):
        self.cursor_updates.append((int(chat_id), last_seen_message_id))

    async def record_runtime_event(self, mode, processed_chats=0, inserted_messages=0, error=None):
        self.runtime_events.append((mode, processed_chats, inserted_messages))


# ── T7: _chat_type_from_dialog (pure) ─────────────────────────────────────────
def test_chat_type_channel():
    assert _chat_type_from_dialog(FakeDialog(1, FakeEntity(broadcast=True))) == "channel"


def test_chat_type_supergroup_from_broadcast_false():
    assert _chat_type_from_dialog(FakeDialog(1, FakeEntity(broadcast=False))) == "supergroup"


def test_chat_type_supergroup_from_megagroup():
    assert _chat_type_from_dialog(FakeDialog(1, FakeEntity(megagroup=True))) == "supergroup"


def test_chat_type_group():
    assert _chat_type_from_dialog(FakeDialog(1, FakeEntity(participants_count=5))) == "group"


def test_chat_type_private():
    assert _chat_type_from_dialog(FakeDialog(1, FakeEntity(first_name="Bob"))) == "private"


def test_chat_type_unknown_when_no_entity():
    assert _chat_type_from_dialog(FakeDialog(1, None)) == "unknown"


# ── T1: chat WITH cursor → catch_up_recent ────────────────────────────────────
def test_chat_with_cursor_calls_catch_up():
    writer = FakeWriter(cursors={100: {"last_seen_message_id": 50}}, catch_up_returns={100: 7})
    client = FakeClient([])
    written, truncated = run(
        backfill_one_chat(client, writer, 100, "private", per_chat_limit=5000, seed_limit=500)
    )
    assert written == 7
    assert truncated is False
    assert writer.catch_up_calls == [(100, "private", 5000)]
    assert writer.written_batches == []


# ── T2: chat WITHOUT cursor → _seed_recent writes + INCREMENTAL cursor (B1) ───
def test_chat_without_cursor_seeds_incremental_cursor():
    msgs = [FakeMsg(1), FakeMsg(2), FakeMsg(3)]
    writer = FakeWriter(cursors={})
    client = FakeClient([], messages_by_chat={200: msgs})
    written, truncated = run(
        backfill_one_chat(client, writer, 200, "private", per_chat_limit=5000, seed_limit=500)
    )
    assert written == 3
    assert truncated is False
    # batch_size=2 → курсор двигается ИНКРЕМЕНТАЛЬНО: после батча [1,2]→cursor=2, после [3]→cursor=3
    assert (200, 2) in writer.cursor_updates
    assert (200, 3) in writer.cursor_updates


def test_seed_truncation_flag_when_limit_hit():
    msgs = [FakeMsg(i) for i in range(1, 11)]  # 10 сообщений
    writer = FakeWriter()
    client = FakeClient([], messages_by_chat={300: msgs})
    written, truncated = run(_seed_recent(client, writer, 300, "private", limit=4))
    assert written == 4
    assert truncated is True  # упёрлись в лимит → история обрезана, видимый сигнал


def test_seed_no_truncation_when_under_limit():
    msgs = [FakeMsg(1), FakeMsg(2)]
    writer = FakeWriter()
    client = FakeClient([], messages_by_chat={301: msgs})
    written, truncated = run(_seed_recent(client, writer, 301, "private", limit=100))
    assert written == 2
    assert truncated is False


def test_seed_partial_batch_does_not_advance_cursor_for_failed_batch():
    """Supabase #62: partial write count must not advance cursor across unwritten ids."""

    class PartialWriter(FakeWriter):
        async def write_messages_batch(self, batch, chat_id, chat_type, chat_title=None):
            self.written_batches.append((int(chat_id), len(batch), chat_title))
            return 1

    msgs = [FakeMsg(1), FakeMsg(2)]
    writer = PartialWriter()
    client = FakeClient([], messages_by_chat={302: msgs})

    written, truncated = run(_seed_recent(client, writer, 302, "private", limit=100))

    assert written == 1
    assert truncated is True
    assert writer.cursor_updates == []


# ── T3: N chats → all scanned, aggregate ──────────────────────────────────────
def test_all_chats_aggregate():
    dialogs = [
        FakeDialog(1, FakeEntity(first_name="A")),
        FakeDialog(2, FakeEntity(broadcast=True)),
    ]
    writer = FakeWriter(
        cursors={1: {"last_seen_message_id": 10}, 2: {"last_seen_message_id": 20}},
        catch_up_returns={1: 3, 2: 5},
    )
    res = run(backfill_all_chats(FakeClient(dialogs), writer))
    assert res.chats_scanned == 2
    assert res.chats_with_new == 2
    assert res.messages_written == 8
    assert res.chats_failed == 0


# ── T4: one chat fails → isolated, others continue ────────────────────────────
def test_per_chat_failure_isolated():
    dialogs = [FakeDialog(i, FakeEntity(first_name=str(i))) for i in (1, 2, 3)]
    writer = FakeWriter(
        cursors={
            1: {"last_seen_message_id": 1},
            2: {"last_seen_message_id": 1},
            3: {"last_seen_message_id": 1},
        },
        catch_up_returns={1: 2, 3: 4},
        fail_chats={2},
    )
    res = run(backfill_all_chats(FakeClient(dialogs), writer))
    assert res.chats_scanned == 3
    assert res.chats_failed == 1
    assert res.messages_written == 6
    assert any("chat=2" in e for e in res.errors)


# ── T5: idempotency ───────────────────────────────────────────────────────────
def test_idempotent_second_run_writes_zero():
    dialogs = [FakeDialog(1, FakeEntity(first_name="A"))]
    writer = FakeWriter(cursors={1: {"last_seen_message_id": 999}}, catch_up_returns={1: 0})
    res = run(backfill_all_chats(FakeClient(dialogs), writer))
    assert res.chats_scanned == 1
    assert res.chats_with_new == 0
    assert res.messages_written == 0


# ── T6: dialog_limit respected ────────────────────────────────────────────────
def test_dialog_limit():
    dialogs = [FakeDialog(i, FakeEntity(first_name=str(i))) for i in range(1, 6)]
    writer = FakeWriter(
        cursors={i: {"last_seen_message_id": 1} for i in range(1, 6)},
        catch_up_returns={i: 1 for i in range(1, 6)},
    )
    res = run(backfill_all_chats(FakeClient(dialogs), writer, dialog_limit=2))
    assert res.chats_scanned == 2


def test_skip_empty_chat_id():
    dialogs = [
        FakeDialog(0, FakeEntity(first_name="A")),
        FakeDialog(5, FakeEntity(first_name="B")),
    ]
    writer = FakeWriter(cursors={5: {"last_seen_message_id": 1}}, catch_up_returns={5: 3})
    res = run(backfill_all_chats(FakeClient(dialogs), writer))
    assert res.chats_scanned == 1  # chat_id=0 пропущен
    assert res.messages_written == 3


# ── FloodWait → sleep + retry succeeds ────────────────────────────────────────
def test_flood_wait_retries():
    dialogs = [FakeDialog(1, FakeEntity(first_name="A"))]
    writer = FakeWriter(
        cursors={1: {"last_seen_message_id": 1}},
        catch_up_returns={1: 5},
        flood_chats={1: 0},  # 0s flood (one-shot) → retry проходит
    )
    res = run(backfill_all_chats(FakeClient(dialogs), writer))
    assert res.chats_failed == 0
    assert res.messages_written == 5  # retry успешен


# ── DEAD SESSION → no false-green, session_dead marker (design fix) ───────────
def test_dead_session_no_false_green():
    dialogs = [FakeDialog(1, FakeEntity(first_name="A"))]
    writer = FakeWriter(cursors={1: {"last_seen_message_id": 1}}, catch_up_returns={1: 9})
    client = FakeClient(dialogs, alive=False)  # сессия мертва
    res = run(run_startup_backfill(client, writer))
    assert res.session_dead is True
    assert res.messages_written == 0  # ничего не догнали
    assert writer.catch_up_calls == []  # backfill_all_chats даже не вызывался
    # маркер ОТРИЦАТЕЛЬНЫЙ — не фальшивый зелёный
    modes = [ev[0] for ev in writer.runtime_events]
    assert "backfill_startup_session_dead" in modes
    assert "backfill_startup_ok" not in modes


def test_iter_dialogs_crash_marks_session_dead():
    writer = FakeWriter()
    client = FakeClient([], dialogs_raise=RuntimeError("disconnect mid-stream"))
    res = run(backfill_all_chats(client, writer))
    assert res.session_dead is True
    assert any("iter_dialogs" in e for e in res.errors)


# ── marker_mode logic ─────────────────────────────────────────────────────────
def test_marker_mode_ok():
    r = BackfillResult(chats_scanned=3, chats_with_new=2, messages_written=10)
    assert r.marker_mode("startup") == "backfill_startup_ok"


def test_marker_mode_partial():
    r = BackfillResult(chats_scanned=3, chats_failed=1)
    assert r.marker_mode("periodic") == "backfill_periodic_partial"


def test_marker_mode_session_dead():
    r = BackfillResult(session_dead=True)
    assert r.marker_mode("startup") == "backfill_startup_session_dead"


# ── T8: universal — telegram_user_id passthrough + ok-marker ──────────────────
def test_universal_user_id_passthrough():
    dialogs = [FakeDialog(1, FakeEntity(first_name="A"))]
    writer = FakeWriter(
        cursors={1: {"last_seen_message_id": 1}},
        catch_up_returns={1: 2},
        telegram_user_id="lisa",
    )
    res = run(run_startup_backfill(FakeClient(dialogs), writer))
    assert res.messages_written == 2
    assert any(ev[0] == "backfill_startup_ok" for ev in writer.runtime_events)
    assert writer.telegram_user_id == "lisa"


# ── schedule_backfill_tasks ───────────────────────────────────────────────────
class FakeLoop:
    def __init__(self):
        self.tasks = []

    def create_task(self, coro):
        coro.close()
        self.tasks.append(coro)
        return _FakeTask()


class _FakeTask:
    def add_done_callback(self, cb):
        pass


def test_schedule_creates_startup_and_periodic():
    os.environ["BACKFILL_ON_STARTUP"] = "true"
    os.environ["BACKFILL_PERIODIC_INTERVAL_SECONDS"] = "3600"
    tasks = schedule_backfill_tasks(FakeLoop(), FakeClient([]), FakeWriter())
    assert len(tasks) == 2
    os.environ.pop("BACKFILL_ON_STARTUP", None)
    os.environ.pop("BACKFILL_PERIODIC_INTERVAL_SECONDS", None)


def test_schedule_disables_periodic_when_zero():
    os.environ["BACKFILL_ON_STARTUP"] = "true"
    os.environ["BACKFILL_PERIODIC_INTERVAL_SECONDS"] = "0"
    tasks = schedule_backfill_tasks(FakeLoop(), FakeClient([]), FakeWriter())
    assert len(tasks) == 1
    os.environ.pop("BACKFILL_ON_STARTUP", None)
    os.environ.pop("BACKFILL_PERIODIC_INTERVAL_SECONDS", None)


def test_schedule_disables_startup_when_off():
    os.environ["BACKFILL_ON_STARTUP"] = "false"
    os.environ["BACKFILL_PERIODIC_INTERVAL_SECONDS"] = "3600"
    tasks = schedule_backfill_tasks(FakeLoop(), FakeClient([]), FakeWriter())
    assert len(tasks) == 1
    os.environ.pop("BACKFILL_ON_STARTUP", None)
    os.environ.pop("BACKFILL_PERIODIC_INTERVAL_SECONDS", None)


def test_backfill_result_helpers():
    r = BackfillResult()
    r.merge_chat(0)
    r.merge_chat(5)
    assert r.chats_scanned == 2
    assert r.chats_with_new == 1
    assert r.messages_written == 5
    assert r.chats_unchanged == 1
    r.merge_failure(42, RuntimeError("boom"))
    assert r.chats_failed == 1
    assert r.chats_scanned == 3
    assert any("chat=42" in e for e in r.errors)


def test_errors_bounded_at_100():
    r = BackfillResult()
    for i in range(150):
        r.merge_failure(i, RuntimeError("x"))
    assert len(r.errors) == 100  # bounded — не растёт бесконечно
    assert r.chats_failed == 150  # счётчик честный
