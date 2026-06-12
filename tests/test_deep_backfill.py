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
    """Чат с last_seen=200, last_backfill=NULL. Должны взять msgs id<200.

    После H1 incremental cursor: update_chat_cursor зовётся ПОСЛЕ КАЖДОГО
    flush. С batch_size=100 и limit=50 будет ровно один flush в конце цикла
    (collected 50 < batch_size 100) — поэтому ровно один cursor update без
    backfill_completed (seen=50 > 0)."""
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
    # H2 fix: completed остаётся False — short batch != confirmed bottom.
    # Подтверждение через seen==0 на следующем проходе.
    assert res.completed is False
    # Cursor update: floor moved to 150 incrementally (no backfill_completed
    # since seen > 0).
    assert (777, None, 150, None) in writer.cursor_updates


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


# ── T3: short batch НЕ метит completed (RCA H2 false-completion fix) ──────
def test_short_batch_does_not_mark_completed():
    """RCA H2: старый код метил completed=TRUE когда `seen < per_run_limit`.
    Это false-positive: transient short batch (rate-limit / pagination blip)
    ложно ставил completed=TRUE навсегда → следующий проход пропускал чат
    → undetected потеря истории.

    Новая семантика: completed=TRUE ТОЛЬКО когда seen==0 (реально дно).
    `seen < limit && seen > 0` оставляем для следующего прогона.

    Чат с msgs 1..30, floor=20, limit=50. Получим 19 сообщений → completed
    остаётся FALSE (следующий проход подтвердит через seen==0)."""
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
    # KEY ASSERTION: short batch ≠ completed.
    assert res.completed is False
    # Cursor продвинут до floor=1 (incremental cursor, H1), но НЕ completed.
    assert any(
        cu[0] == 888 and cu[2] == 1 and cu[3] is None
        for cu in writer.cursor_updates
    ), f"expected incremental cursor update to floor=1, got {writer.cursor_updates}"
    # Подтверждение completed произойдёт на следующем проходе через seen==0
    # (см. test_short_batch_completed_on_next_pass_through_seen_zero).


# ── T3b: confirm completed на следующем проходе через seen==0 ─────────────
def test_short_batch_completed_on_next_pass_through_seen_zero():
    """После T3 floor=1, msgs все на или выше floor=1 → следующий проход
    с floor=1 видит 0 сообщений старше floor=1 (только id=1 не пройдёт,
    т.к. iter возвращает id<floor=1 → пусто) → terminal через seen==0."""
    msgs = [FakeMsg(i) for i in range(1, 31)]
    # Имитируем состояние после первого short-batch прохода
    writer = FakeWriter(cursors={888: {"last_backfill_message_id": 1}})
    client = FakeClient(messages_by_chat={888: msgs})
    res = run(
        deep_backfill_one_chat(
            client, writer, 888, "private", per_run_limit=50
        )
    )
    assert res.written == 0
    assert res.completed is True
    # cursor_update с backfill_completed=True
    assert (888, None, None, True) in writer.cursor_updates


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
    """Одна FloodWait → retry → проход успешен.

    После C2 fix (counter inflation): result.written берётся из ВТОРОГО
    прохода (pass_state["written"] обнуляется в начале _do() ). Первый проход
    дописал 10 (на flush первой batch), но потом упал FloodWait → retry
    обнулил pass_state и дописал заново те же 10 (idempotent ON CONFLICT,
    видимый счётчик — только 10, не 20).

    После H2 fix: 10 seen > 0 и < 50 → completed остаётся False."""
    msgs = [FakeMsg(i) for i in range(1, 11)]
    writer = FakeWriter(
        cursors={2002: {"last_backfill_message_id": 100}},
        flood_chats={2002: 1},  # 1 flood → retry success
    )
    client = FakeClient(messages_by_chat={2002: msgs})
    res = run(
        deep_backfill_one_chat(client, writer, 2002, "private", per_run_limit=50)
    )
    # Counter не задвоен (C2 fix): только 10, а не 20.
    assert res.written == 10
    # H2 fix: 10 < 50 limit, но > 0 → НЕ completed.
    assert res.completed is False
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
    """После H2 fix completed выставляется только при seen==0.

    Chat 1 floor=50, msgs 1..29 → 29 seen (< 100 limit) → NOT completed,
    floor продвинут до 1.
    Chat 2 floor=100, msgs 1..59 → 59 seen → NOT completed, floor до 1.
    Chat 3 уже completed=True → skip без работы, completed=True.

    Total completed = 1 (только chat 3, который уже был completed)."""
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
    # chat 3 skip-completed; chats 1,2 short-batch → НЕ completed (H2 fix).
    assert res.chats_completed == 1
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
    res4 = DeepBackfillRunResult(stalled=True)
    assert res4.marker_mode() == "deep_backfill_stalled"
    res5 = DeepBackfillRunResult()
    assert res5.marker_mode() == "deep_backfill_ok"


# ──────────────────────────────────────────────────────────────────────────────
# Stage-7 rework tests (B1/C1/C2/C3/H1/H2 + design priority + stalled detector)
# ──────────────────────────────────────────────────────────────────────────────


# ── T13: C3 — msg.id=0 (MessageEmpty) НЕ участвует в min_id_seen ─────────────
def test_message_empty_id_zero_excluded_from_min_id():
    """Telegram отдаёт MessageEmpty (id=0) для удалённых сообщений в pagination.
    Раньше batch_min = min(0, real_min) = 0 → floor=0 → следующий проход
    стартовал от newest и тратил FloodWait-бюджет на already-written.

    Fix C3 (Stage-7): фильтруем msg.id > 0 ДО min(). batch_min должен быть
    минимальным РЕАЛЬНЫМ id, не 0."""
    msgs = [FakeMsg(0), FakeMsg(5), FakeMsg(10)]  # MessageEmpty + 2 real
    writer = FakeWriter(cursors={4040: {"last_backfill_message_id": 100}})
    client = FakeClient(messages_by_chat={4040: msgs})
    res = run(
        deep_backfill_one_chat(client, writer, 4040, "private", per_run_limit=50)
    )
    # written = 3 (Supabase ON CONFLICT может схлопнуть, но writer.batch — 3).
    assert res.written == 3
    # min_id_seen = 5, не 0 (id=0 отфильтрован).
    assert res.min_id_seen == 5
    assert res.floor_after == 5


# ── T14: H1 — incremental cursor update после каждого batch ──────────────────
def test_incremental_cursor_update_per_batch():
    """RCA H1: если процесс убьют между write и cursor-update, floor должен
    быть уже продвинут хотя бы до конца последнего успешно записанного batch.

    Имитируем batch_size=2 и 6 сообщений → 3 flush'а → 3 cursor update'а.
    Старый код делал ОДИН cursor update в конце цикла."""
    msgs = [FakeMsg(i) for i in range(1, 7)]  # ids 1..6
    writer = FakeWriter(cursors={5050: {"last_backfill_message_id": 100}})
    writer.batch_size = 2  # маленький batch → много flush'ей
    client = FakeClient(messages_by_chat={5050: msgs})
    res = run(
        deep_backfill_one_chat(client, writer, 5050, "private", per_run_limit=50)
    )
    assert res.written == 6
    # Должно быть >= 3 cursor update'ов (по одному на каждый flush).
    # Старый код давал ровно 1 update в конце.
    floor_updates = [
        cu for cu in writer.cursor_updates if cu[2] is not None
    ]
    assert len(floor_updates) >= 3, (
        f"expected ≥3 incremental cursor updates, got {len(floor_updates)}: "
        f"{writer.cursor_updates}"
    )
    # Финальный min_id = 1.
    assert res.min_id_seen == 1


# ── T15: C2 — FloodWait после успешного flush НЕ дублирует счётчик ──────────
def test_floodwait_after_successful_flush_does_not_double_count():
    """RCA C2: первый flush записал N, потом упал FloodWait, retry перезаписал
    те же N (ON CONFLICT идемпотентно). Счётчик должен показать N, не 2N.

    Имитация: batch_size=2, msgs 1..5, flood после ПЕРВОГО batch.
    Первый flush пишет 2 (ids 4,5 от newest вниз) → flood → retry _do()
    обнуляет pass_state → второй проход дописывает все 5."""

    class FloodAfterFirstBatchWriter(FakeWriter):
        """Падаем FloodWait на ВТОРОМ вызове write_messages_batch (после первого
        успешного flush)."""

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self._write_calls = 0

        async def write_messages_batch(self, batch, chat_id, chat_type):
            self._write_calls += 1
            # На 2-м вызове в первом проходе — flood; ретрай (вызовы 3+) проходит.
            if self._write_calls == 2:
                raise FloodWait(0)
            n = len(batch)
            self.written_batches.append((int(chat_id), n))
            return n

    writer = FloodAfterFirstBatchWriter(
        cursors={6060: {"last_backfill_message_id": 100}}
    )
    writer.batch_size = 2
    msgs = [FakeMsg(i) for i in range(1, 6)]  # ids 1..5
    client = FakeClient(messages_by_chat={6060: msgs})
    res = run(
        deep_backfill_one_chat(client, writer, 6060, "private", per_run_limit=50)
    )
    # Counter — только из второго прохода (5 msgs), не 2 + 5 = 7.
    assert res.written == 5, (
        f"expected 5 (retry-only count), got {res.written} — "
        f"counter inflation regression"
    )
    assert res.error is None


# ── T16: stalled marker когда non-completed чаты не пишут ничего ────────────
def test_stalled_marker_when_no_writes_but_chats_active():
    """Stage-7 detect-of-detector: если non-completed чаты обрабатываются и НЕ
    пишут НИЧЕГО → это stall, не green. Маркер `deep_backfill_stalled`."""
    writer = FakeWriter(cursors={7070: {"last_backfill_message_id": 100}})
    # Пустой чат, НО backfill_completed НЕ выставлен в cursor → non-completed.
    # iter_messages вернёт 0 → seen=0 → в данном случае completed=True.
    # Для имитации stall нам нужен чат который non-completed И не пишет.
    # Хитрее: сделаем чат с floor=10 и msgs только > 10 (значит iter с
    # offset_id=10 даст 0). seen=0 → completed=True → НЕ stall.
    # → stall случается когда iter возвращает 0 НО completed не выставляется.
    # Для теста используем chat 3 (completed=True уже) + chat 4 (тоже completed).
    # Но тогда non_completed_processed = 0 → НЕ stall. Правильно: нам нужен
    # чат который non-completed но не пишет. Это возможно при FloodWait
    # без retry success → result.error, но это тоже исключается через
    # chats_failed > 0. Реалистично: chat с уже completed=False, floor=1,
    # 1 msg id=1: seen=0 (filter id<floor=1 → пусто) → completed=True →
    # non_completed_processed=0.
    # Стабильный сценарий: используем пустой чат с floor=NULL и
    # last_seen_message_id=NULL → floor=0 → iter с no offset_id → пусто →
    # seen=0 → completed=True. Это не stall.
    # Финально, чтобы получить stall — нужен fake client который возвращает
    # 0 сообщений НО не updates cursor (mock-инъекция).
    class NoCompleteWriter(FakeWriter):
        """Игнорирует backfill_completed=True (имитация регрессии H2)."""

        async def update_chat_cursor(
            self,
            chat_id,
            last_seen_message_id=None,
            last_backfill_message_id=None,
            backfill_completed=None,
        ):
            # Сбрасываем completed обратно → имитация false-negative bug.
            return await super().update_chat_cursor(
                chat_id,
                last_seen_message_id,
                last_backfill_message_id,
                backfill_completed=None,
            )

    writer2 = NoCompleteWriter(cursors={7070: {"last_backfill_message_id": 5}})
    # Floor=5, msgs только id=10,20 — iter с offset_id=5 даст пусто.
    # seen=0 → in code completed=True пытается выставиться → NoCompleteWriter
    # игнорирует → chat_result.completed остаётся False (потому что мы не
    # cursor flag читаем, а возврат функции — completed=True всё равно
    # выставлен в `result.completed`).
    # Поэтому используем другой путь: completed=True на verification level
    # in DeepBackfillChatResult — игнорировать невозможно. Меняем подход:
    # имитируем chat где iter возвращает что-то, но writer.write_batch вернёт 0
    # (имитация all-already-written скенарий).
    class ZeroWriteWriter(FakeWriter):
        async def write_messages_batch(self, batch, chat_id, chat_type):
            return 0  # ON CONFLICT — все уже записаны

    writer3 = ZeroWriteWriter(cursors={7070: {"last_backfill_message_id": 100}})
    msgs = [FakeMsg(i) for i in range(1, 6)]
    client = FakeClient(messages_by_chat={7070: msgs})
    res = run(
        deep_backfill_all_chats(
            client,
            writer3,
            total_budget=1000,
            per_chat_limit=10,
            explicit_chats=[("7070", "private")],
        )
    )
    # 5 msgs < limit=10 → seen=5 > 0, но writer вернул 0 → messages_written=0.
    # completed остаётся False (H2 fix), failed=0 → stalled=True.
    assert res.messages_written == 0
    assert res.chats_failed == 0
    assert res.stalled is True, (
        f"expected stalled=True (non-completed chat writes 0), "
        f"got stalled={res.stalled}, marker={res.marker_mode()}"
    )
    assert any(
        ev[0] == "deep_backfill_stalled" for ev in writer3.runtime_events
    )


# ── T17: stalled НЕ выставляется когда все processed chats уже completed ────
def test_not_stalled_when_all_processed_already_completed():
    """No-op случай: все чаты уже completed=True → 0 writes — это валидный
    green, НЕ stall."""
    writer = FakeWriter(cursors={1: {"backfill_completed": True}})
    client = FakeClient(messages_by_chat={1: [FakeMsg(10)]})
    res = run(
        deep_backfill_all_chats(
            client,
            writer,
            total_budget=1000,
            per_chat_limit=10,
            explicit_chats=[("1", "private")],
        )
    )
    assert res.chats_processed == 1
    assert res.messages_written == 0
    assert res.stalled is False
    assert res.marker_mode() == "deep_backfill_ok"


# ── T18: priority_chat_ids передаётся в PG selection ────────────────────────
def test_priority_chat_ids_loaded_from_env(monkeypatch):
    """`DEEP_BACKFILL_PRIORITY_CHATS` env → set[str]. Универсально для CI/laba."""
    from heroes_platform.heroes_telegram_mcp.deep_backfill import (
        _load_priority_chat_ids,
    )

    monkeypatch.setenv(
        "DEEP_BACKFILL_PRIORITY_CHATS",
        "1001, 1002 ,1003",
    )
    monkeypatch.delenv("DEEP_BACKFILL_PRIORITY_FILE", raising=False)
    ids = _load_priority_chat_ids()
    assert ids == {"1001", "1002", "1003"}


def test_priority_chat_ids_empty_when_no_env(monkeypatch):
    monkeypatch.delenv("DEEP_BACKFILL_PRIORITY_CHATS", raising=False)
    monkeypatch.delenv("DEEP_BACKFILL_PRIORITY_FILE", raising=False)
    from heroes_platform.heroes_telegram_mcp.deep_backfill import (
        _load_priority_chat_ids,
    )

    assert _load_priority_chat_ids() == set()


def test_priority_chat_ids_from_file(monkeypatch, tmp_path):
    """JSON-файл с приоритетными chat_id — генерируется отдельно из
    advising-clients-registry.yaml; этот модуль — consumer."""
    import json as _json
    from heroes_platform.heroes_telegram_mcp.deep_backfill import (
        _load_priority_chat_ids,
    )

    f = tmp_path / "priority.json"
    f.write_text(_json.dumps([2001, "2002", {"chat_id": 2003}]))
    monkeypatch.delenv("DEEP_BACKFILL_PRIORITY_CHATS", raising=False)
    monkeypatch.setenv("DEEP_BACKFILL_PRIORITY_FILE", str(f))
    ids = _load_priority_chat_ids()
    assert ids == {"2001", "2002", "2003"}


# ── T19: PG SELECT с priority_chat_ids — клиенты идут первыми ───────────────
def test_pg_select_orders_priority_tier_first():
    """SELECT с priority_chat_ids: клиентский chat_id попадает в tier=0,
    остальные tier=1. ORDER BY priority_tier ASC → клиенты первые.

    Используем fake conn/cursor чтобы протестировать SQL без реального PG."""
    from heroes_platform.heroes_telegram_mcp.deep_backfill import (
        _select_chats_for_deep_backfill_pg,
    )

    captured_sql: dict = {}

    class FakeCursor:
        def __init__(self):
            self.description = None

        def execute(self, sql, params):
            captured_sql["sql"] = sql
            captured_sql["params"] = params
            # Имитируем 3 row: один клиент (id=100) + 2 обычных.
            # Должны вернуться в порядке priority_tier=0 первым.
            self._rows = [
                ("100", "private"),
                ("200", "supergroup"),
                ("300", "channel"),
            ]

        def fetchall(self):
            return self._rows

        def close(self):
            pass

    class FakeConn:
        def cursor(self):
            return FakeCursor()

    rows = _select_chats_for_deep_backfill_pg(
        FakeConn(),
        schema="rick_messages_tasks",
        telegram_user_id="ikrasinsky",
        limit=10,
        priority_chat_ids={"100"},
    )
    # SQL содержит priority_tier и ORDER BY priority_tier ASC.
    assert "priority_tier" in captured_sql["sql"]
    assert "ORDER BY priority_tier ASC" in captured_sql["sql"]
    # params[0] = priority_list — должна содержать "100".
    assert "100" in captured_sql["params"][0]
    # Возвращены rows (порядок задан SQL'ом, не Python).
    assert len(rows) == 3


# ── T20: C1 — supabase_writer last_backfill_message_id монотонно идёт вниз ──
def test_update_cursor_pg_uses_least_for_backfill_floor():
    """RCA C1: blind overwrite (старый SET last_backfill_message_id=%s)
    позволял race поднять floor вверх. Новый код использует
    LEAST(COALESCE(...), new) → монотонность вниз гарантирована.

    Тестируем напрямую SQL через mock conn — без реального PG."""
    from heroes_platform.heroes_telegram_mcp.supabase_writer import SupabaseWriter

    captured: list[tuple[str, tuple]] = []

    class FakeCursor:
        def execute(self, sql, params):
            captured.append((sql, params))

        def close(self):
            pass

    class FakeConn:
        def cursor(self):
            return FakeCursor()

        def commit(self):
            pass

        def rollback(self):
            pass

    # Создаём writer-like object с нужными атрибутами (минимум для _update_chat_cursor_pg).
    class FakeWriter2:
        schema = "tg_test"
        telegram_user_id = "test_user"

        def _ensure_chat_state_pg(self, conn, chat_id):
            pass

    # Bind method to FakeWriter2.
    fw = FakeWriter2()
    SupabaseWriter._update_chat_cursor_pg(
        fw,
        FakeConn(),
        chat_id="100",
        last_seen_message_id=None,
        last_backfill_message_id=42,
        backfill_completed=None,
    )
    # Должен быть один SQL UPDATE для last_backfill, содержащий LEAST.
    backfill_sql = [s for s, _p in captured if "last_backfill_message_id" in s]
    assert backfill_sql, "no UPDATE for last_backfill_message_id captured"
    sql = backfill_sql[0]
    assert "LEAST" in sql, (
        f"expected LEAST in UPDATE for backfill floor monotonicity, got: {sql}"
    )
    assert "COALESCE" in sql
