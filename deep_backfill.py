#!/usr/bin/env python3
"""System-wide deep backfill — backward-walk полной истории каждого Telegram чата.

JTBD: Когда нужна УВЕРЕННОСТЬ что в Supabase лежит ВСЯ история каждого чата
(не только последние N seed-сообщений и не только новее last_seen курсора),
я хочу системный oркестратор, который для каждого чата идёт ВНИЗ от известного
потолка (`last_seen_message_id` или `last_backfill_message_id`), пишет старые
сообщения батчами, продвигает `last_backfill_message_id` (нижний этаж) и
помечает `backfill_completed=TRUE` когда дошёл до начала чата. Между запусками
работа resumable: следующий проход стартует с уже продвинутого floor вниз.

Архитектура (§Wiring-first — переиспользуем существующие колонки + writer):

    Существующая инфраструктура (НЕ строим параллельно):
    - `telegram_chat_state.last_backfill_message_id` (BIGINT) — backward floor.
    - `telegram_chat_state.last_backfill_ts` (TIMESTAMPTZ) — отметка последнего прохода.
    - `telegram_chat_state.backfill_completed` (BOOLEAN) — terminal flag.
    - index `(backfill_completed, last_backfill_message_id)` — selection быстрый.
    - `SupabaseWriter.write_messages_batch()` — idempotent ON CONFLICT.
    - `SupabaseWriter.update_chat_cursor(..., last_backfill_message_id=..., backfill_completed=...)`
      — keyword args уже добавлены, монотонность курсора через GREATEST в `_update_chat_cursor_pg`.
    - `SupabaseWriter.get_chat_cursor()` возвращает все поля row из chat_state.

    Чего НЕ хватало (закрывает этот модуль):
    1. `deep_backfill_one_chat()` — backward-walk одного чата с правильным floor
       resolution и terminal detection.
    2. `deep_backfill_all_chats()` — bounded оркестратор по приоритету
       `last_backfill_ts ASC NULLS FIRST` (никогда не запускавшиеся → первыми).
    3. Универсальная CLI обёртка → один скрипт работает для любого профиля.
    4. launchd plist — раз в час (не пересекается с 5-мин periodic catch_up).

    КЛЮЧЕВОЕ ОТЛИЧИЕ от существующих путей:
    - `event_handlers.NewMessage` — пишет live (новейшие, +1 за раз).
    - `catch_up_recent` — `min_id=last_seen_message_id reverse=True` (только новее).
    - `_seed_recent` — последние seed_limit (truncated если история длиннее).
    - `backfill_chat` (legacy в writer) — есть backward, но не resumable между
      запусками: `max_id=cursor["last_backfill_message_id"]` НЕ trgrt `last_seen`
      если нижний этаж NULL (тянет с новейших — конкурирует с seed).
    - **deep_backfill_one_chat** — explicit floor resolution: сначала
      `last_backfill_message_id` → fallback `last_seen_message_id` → fallback `0`
      (от новейшего). Записывает min_id за проход. При исчерпании истории
      (получили <per_run_limit) → `backfill_completed=TRUE`.

Универсальность (Generalization-first §4 4×yes):
    1. Работает для всех профилей через `writer.telegram_user_id` (он же определяет
       schema). Новый профиль = новый контейнер с TELEGRAM_USER=<alias>.
    2. Никаких client-specific hardcodes (chat_id, профиль, schema).
    3. Все пути к данным через `writer.schema` (resolved в `SupabaseWriter.__init__`).
    4. Новый профиль = заводим credentials в Keychain + Supabase grants. Без
       правки этого файла.

Идемпотентность:
    - `write_messages_batch` уже ON CONFLICT (chat_id, message_id) — повторный
      проход не дублирует.
    - `update_chat_cursor(last_backfill_message_id=min_id)` — `_update_chat_cursor_pg`
      кладёт floor явным UPDATE; повторный проход с тем же floor не отбрасывает
      курсор (даже без GREATEST: следующий проход стартует с этого floor и идёт
      ниже — поэтому min_id будет ≤ floor).

Resume контракт:
    - Шаг N: floor=F → iter_messages(offset_id=F, limit=L) → min_id_seen=m → update.
    - Шаг N+1: floor=m → iter_messages(offset_id=m, limit=L) → продолжает с m-1.
    - Достижение начала чата: получено k<L сообщений → terminal `completed=TRUE`.

Защита от ложно-зелёного (Detect-of-detector):
    - PRE-FLIGHT liveness (`client.is_user_authorized` + `get_me`) — сессия мертва
      → пишем negative marker `deep_backfill_session_dead`, НЕ зелёный.
    - per-chat изоляция: ошибка одного чата → `chats_failed++`, проход
      продолжается.
    - FloodWaitError → sleep + один retry; повторный → проброс/skip.
    - Budget exhaust (total_budget messages reached) → честный `partial` маркер.

Non-goals (честно — чтобы владелец не считал что выгружено больше, чем есть):
    - Это НЕ архиватор медиа: пишутся id, текст, raw JSON Telegram-сообщения
      (как и существующий `write_messages_batch`). Скачивание файлов — отдельная
      задача (`chat_exporter`), не realtime ingestion.
    - Удалённые в Telegram сообщения могут уже быть в Supabase (raw bronze); soft
      delete — отдельный bead.
    - Backward-walk не возвращает сообщения, удалённые до прохода (Telegram их
      не отдаёт через `iter_messages` старее offset_id).

Конфиг (env, безопасные дефолты):
    DEEP_BACKFILL_TOTAL_BUDGET=10000      — макс. сообщений за один прогон оркестратора
    DEEP_BACKFILL_PER_CHAT_LIMIT=2000     — макс. сообщений за прогон одного чата
    BACKFILL_FLOOD_WAIT_MAX_SECONDS=300   — переиспользуем из startup_backfill
"""

from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

# Лок: deep backfill и periodic catch_up не должны идти параллельно по одному
# профилю — двойной iter_messages по 200+ чатам = умножение rate-limit hit'ов.
_deep_lock: asyncio.Lock | None = None


def _get_lock() -> asyncio.Lock:
    global _deep_lock
    if _deep_lock is None:
        _deep_lock = asyncio.Lock()
    return _deep_lock


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


# ── FloodWait (Telegram «остынь») — duck-type ─────────────────────────────────
def _flood_wait_seconds(exc: Exception) -> int | None:
    """Зеркало startup_backfill._flood_wait_seconds (один контракт для retry).

    Путь FloodWaitError менялся между версиями telethon (telethon.errors vs
    .rpcerrorlist), поэтому duck-type по имени класса + атрибуту.
    """
    if "FloodWait" in type(exc).__name__ and hasattr(exc, "seconds"):
        try:
            return int(exc.seconds)
        except (TypeError, ValueError):
            return None
    return None


@dataclass
class DeepBackfillChatResult:
    """Итог backward-walk одного чата."""

    chat_id: str
    written: int = 0
    min_id_seen: int | None = None
    floor_before: int | None = None
    floor_after: int | None = None
    completed: bool = False
    error: str | None = None


@dataclass
class DeepBackfillRunResult:
    """Итог одного прохода deep_backfill_all_chats."""

    chats_processed: int = 0
    chats_completed: int = 0
    chats_failed: int = 0
    messages_written: int = 0
    budget_exhausted: bool = False
    session_dead: bool = False
    per_chat: list[DeepBackfillChatResult] = field(default_factory=list)

    def marker_mode(self, phase: str = "deep_backfill") -> str:
        if self.session_dead:
            return f"{phase}_session_dead"
        if self.chats_failed > 0:
            return f"{phase}_partial"
        if self.budget_exhausted:
            return f"{phase}_budget_exhausted"
        return f"{phase}_ok"


# ── pre-flight liveness (зеркало startup_backfill._session_alive) ────────────
async def _session_alive(client: Any) -> bool:
    """Жива ли Telegram-сессия. Мёртвая → НЕ пишем false-green маркер."""
    try:
        if not await client.is_user_authorized():
            return False
        await client.get_me()  # реальный RPC — ловит revoked-после-connect
        return True
    except Exception as exc:  # noqa: BLE001
        logger.warning("Deep backfill session liveness probe failed: %s", exc)
        return False


def _resolve_floor(cursor: dict[str, Any] | None) -> int:
    """Решает с какого offset_id Telegram начинать backward-walk.

    Контракт `iter_messages(offset_id=N, reverse=False)` — возвращает сообщения
    СТАРШЕ N (id < N), от свежего к старому. offset_id=0 → от newest.

    Приоритет:
    1. `last_backfill_message_id` (нижний этаж предыдущего прохода) — главный
       resume-сигнал.
    2. `last_seen_message_id` (известный потолок, обновляется live-handler'ом).
    3. `0` (от newest — первый проход для совсем пустого чата).
    """
    if not cursor:
        return 0
    lb = cursor.get("last_backfill_message_id")
    if lb is not None:
        try:
            return int(lb)
        except (TypeError, ValueError):
            pass
    ls = cursor.get("last_seen_message_id")
    if ls is not None:
        try:
            return int(ls)
        except (TypeError, ValueError):
            pass
    return 0


# ── backward-walk одного чата ─────────────────────────────────────────────────
async def deep_backfill_one_chat(
    client: Any,
    writer: Any,
    chat_id: int | str,
    chat_type: str = "unknown",
    *,
    per_run_limit: int = 2000,
) -> DeepBackfillChatResult:
    """Один проход backward-walk для одного чата.

    Алгоритм:
        1. cursor = get_chat_cursor(chat_id)
        2. completed? → return (0, completed=True)
        3. floor = resolve_floor(cursor)  # last_backfill OR last_seen OR 0
        4. iter_messages(entity=chat_id, offset_id=floor, limit=per_run_limit,
           reverse=False) → старше floor (descending по id)
        5. Батчами пишем write_messages_batch (ON CONFLICT idempotent)
        6. Трекаем min_id_seen
        7. update_chat_cursor(last_backfill_message_id=min_id_seen,
                              last_backfill_ts=now)
        8. Если получено сообщений < per_run_limit → достигли начала чата →
           backfill_completed=TRUE (терминал)

    FloodWaitError → sleep + один retry (как backfill_one_chat). Повторный
    FloodWait → проброс наверх (caller увидит chats_failed++).
    """
    flood_max = _env_int("BACKFILL_FLOOD_WAIT_MAX_SECONDS", 300)
    cid_str = str(chat_id)
    cid_int = int(chat_id)
    result = DeepBackfillChatResult(chat_id=cid_str)

    cursor = await writer.get_chat_cursor(cid_int)
    if cursor and cursor.get("backfill_completed"):
        result.completed = True
        return result

    floor = _resolve_floor(cursor)
    result.floor_before = floor

    async def _do() -> DeepBackfillChatResult:
        batch: list[Any] = []
        seen = 0
        min_id_seen: int | None = None
        batch_size = int(getattr(writer, "batch_size", 100) or 100)

        async def _flush() -> None:
            nonlocal batch, min_id_seen
            if not batch:
                return
            n = await writer.write_messages_batch(batch, cid_int, chat_type)
            result.written += n
            batch_min = min(int(getattr(m, "id", 0) or 0) for m in batch)
            min_id_seen = (
                batch_min if min_id_seen is None else min(min_id_seen, batch_min)
            )
            batch = []

        # offset_id=0 — особый случай: Telegram трактует как "от newest";
        # для непустого floor он трактует "СТАРШЕ floor" (id < floor),
        # порядок DESC по id (свежее → старее).
        iter_kwargs: dict[str, Any] = {
            "entity": cid_int,
            "limit": per_run_limit,
        }
        if floor > 0:
            iter_kwargs["offset_id"] = floor

        async for msg in client.iter_messages(**iter_kwargs):
            batch.append(msg)
            seen += 1
            if len(batch) >= batch_size:
                await _flush()
        await _flush()

        if min_id_seen is not None and min_id_seen > 0:
            result.min_id_seen = min_id_seen
            result.floor_after = min_id_seen
            # Терминал: получили меньше чем просили = упёрлись в начало чата.
            # 0 сообщений = либо пустой чат, либо floor уже на самом дне.
            is_completed = seen < per_run_limit
            await writer.update_chat_cursor(
                cid_int,
                last_backfill_message_id=min_id_seen,
                backfill_completed=is_completed,
            )
            result.completed = is_completed
        elif seen == 0:
            # 0 сообщений старше floor (или пустой чат) → терминал.
            await writer.update_chat_cursor(
                cid_int,
                backfill_completed=True,
            )
            result.completed = True

        return result

    try:
        return await _do()
    except Exception as exc:  # noqa: BLE001
        wait = _flood_wait_seconds(exc)
        if wait is not None and 0 <= wait <= flood_max:
            logger.warning(
                "Deep backfill FloodWait %ss on chat %s — sleeping then retry",
                wait,
                cid_str,
            )
            await asyncio.sleep(wait)
            try:
                return await _do()
            except Exception as retry_exc:  # noqa: BLE001
                result.error = f"{type(retry_exc).__name__}: {retry_exc}"
                return result
        result.error = f"{type(exc).__name__}: {exc}"
        return result


# ── select chats by priority (PG) ─────────────────────────────────────────────
def _select_chats_for_deep_backfill_pg(
    conn: Any,
    schema: str,
    telegram_user_id: str,
    *,
    limit: int,
) -> list[tuple[str, str]]:
    """SELECT chats нуждающихся в deep backfill.

    Приоритет:
    1. backfill_completed = FALSE
    2. is_active = TRUE
    3. ORDER BY last_backfill_ts ASC NULLS FIRST  ← никогда не запускавшиеся первыми
    4. LIMIT N

    Возвращает [(chat_id, chat_type), ...]. chat_type подтягивается LEFT JOIN
    из `telegram_chats` (там есть `chat_type`); если отсутствует → 'unknown'.
    """
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            SELECT s.chat_id,
                   COALESCE(c.chat_type, 'unknown') AS chat_type
            FROM {schema}.telegram_chat_state s
            LEFT JOIN {schema}.telegram_chats c
                   ON c.chat_id = s.chat_id
            WHERE s.telegram_user_id = %s
              AND s.backfill_completed = FALSE
              AND s.is_active = TRUE
            ORDER BY s.last_backfill_ts ASC NULLS FIRST,
                     s.chat_id ASC
            LIMIT %s
            """,
            (telegram_user_id, limit),
        )
        return [(str(r[0]), str(r[1] or "unknown")) for r in cur.fetchall()]
    finally:
        cur.close()


async def _select_chats_for_deep_backfill(
    writer: Any,
    *,
    limit: int,
) -> list[tuple[str, str]]:
    """Async-обёртка вокруг PG SELECT. Если postgres_url не доступен (REST-only
    режим в тестах), возвращает пустой список — caller обязан передать chat_ids
    явно через CLI --chat-id.
    """
    pg_url = getattr(writer, "_postgres_url", None)
    if not pg_url:
        return []
    pg_conn_factory = getattr(writer, "_pg_conn", None)
    if pg_conn_factory is None:
        return []
    with pg_conn_factory() as conn:
        return _select_chats_for_deep_backfill_pg(
            conn,
            schema=writer.schema,
            telegram_user_id=writer.telegram_user_id,
            limit=limit,
        )


# ── оркестратор: все чаты с приоритетом по last_backfill_ts ──────────────────
async def deep_backfill_all_chats(
    client: Any,
    writer: Any,
    *,
    total_budget: int | None = None,
    per_chat_limit: int | None = None,
    chat_select_limit: int = 500,
    explicit_chats: list[tuple[str, str]] | None = None,
) -> DeepBackfillRunResult:
    """Bounded оркестратор: проходит по чатам с приоритетом last_backfill_ts
    ASC NULLS FIRST, для каждого вызывает deep_backfill_one_chat. Останавливается
    при достижении total_budget записанных сообщений.

    `explicit_chats=[(chat_id, chat_type), ...]` — bypass DB selection (для CLI
    `--chat-id` и для тестов без PG-доступа).
    """
    if total_budget is None:
        total_budget = _env_int("DEEP_BACKFILL_TOTAL_BUDGET", 10000)
    if per_chat_limit is None:
        per_chat_limit = _env_int("DEEP_BACKFILL_PER_CHAT_LIMIT", 2000)

    result = DeepBackfillRunResult()

    lock = _get_lock()
    if lock.locked():
        logger.warning("Deep backfill skipped: previous pass still running")
        return result

    async with lock:
        if not await _session_alive(client):
            logger.warning(
                "Deep backfill skipped: session DEAD (user=%s)",
                getattr(writer, "telegram_user_id", "?"),
            )
            result.session_dead = True
            try:
                await writer.record_runtime_event(
                    mode=result.marker_mode(),
                    processed_chats=0,
                    inserted_messages=0,
                    error="session_dead",
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("Failed to record session_dead marker: %s", exc)
            return result

        chats: list[tuple[str, str]]
        if explicit_chats is not None:
            chats = list(explicit_chats)
        else:
            chats = await _select_chats_for_deep_backfill(
                writer, limit=chat_select_limit
            )

        if not chats:
            logger.info(
                "Deep backfill: no chats need backward-walk (user=%s)",
                getattr(writer, "telegram_user_id", "?"),
            )
            try:
                await writer.record_runtime_event(
                    mode=result.marker_mode(),
                    processed_chats=0,
                    inserted_messages=0,
                    error=None,
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("Failed to record no-op marker: %s", exc)
            return result

        for chat_id, chat_type in chats:
            if result.messages_written >= total_budget:
                result.budget_exhausted = True
                logger.info(
                    "Deep backfill: budget %d exhausted after %d chats",
                    total_budget,
                    result.chats_processed,
                )
                break
            try:
                chat_result = await deep_backfill_one_chat(
                    client,
                    writer,
                    chat_id,
                    chat_type,
                    per_run_limit=per_chat_limit,
                )
            except Exception as exc:  # noqa: BLE001 — изоляция per-chat
                logger.warning(
                    "Deep backfill UNEXPECTED failure for chat %s: %s",
                    chat_id,
                    exc,
                )
                chat_result = DeepBackfillChatResult(
                    chat_id=chat_id,
                    error=f"{type(exc).__name__}: {exc}",
                )

            result.chats_processed += 1
            result.messages_written += chat_result.written
            if chat_result.error:
                result.chats_failed += 1
            if chat_result.completed:
                result.chats_completed += 1
            result.per_chat.append(chat_result)

        logger.info(
            "Deep backfill (user=%s): processed=%d completed=%d failed=%d "
            "written=%d budget_exhausted=%s",
            getattr(writer, "telegram_user_id", "?"),
            result.chats_processed,
            result.chats_completed,
            result.chats_failed,
            result.messages_written,
            result.budget_exhausted,
        )
        try:
            errs = [c.error for c in result.per_chat if c.error]
            await writer.record_runtime_event(
                mode=result.marker_mode(),
                processed_chats=result.chats_processed,
                inserted_messages=result.messages_written,
                error=("; ".join(errs[:5]) if errs else None),
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to record deep_backfill marker: %s", exc)

    return result
