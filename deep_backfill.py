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
    # Detect-of-detector (Stage-7 review): если несколько проходов подряд по
    # активным (не completed) чатам не записали НИЧЕГО — это не «всё ок», это
    # stall: либо все floors уже на дне, но completed=FALSE (false-negative
    # H2 регрессия), либо Telegram отдаёт пустые батчи (rate-limit / silent
    # API change). Маркер `*_stalled` отдельно — чтобы panel видела тренд
    # «coverage не растёт», а не ложный green.
    stalled: bool = False
    per_chat: list[DeepBackfillChatResult] = field(default_factory=list)

    def marker_mode(self, phase: str = "deep_backfill") -> str:
        if self.session_dead:
            return f"{phase}_session_dead"
        if self.chats_failed > 0:
            return f"{phase}_partial"
        if self.budget_exhausted:
            return f"{phase}_budget_exhausted"
        if self.stalled:
            return f"{phase}_stalled"
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

    # security-1 (pr-hero-x0p): resolve title once so deep-backfill honours the
    # title/username skip (iter_messages gives msg.chat=None). Closure _flush below
    # captures chat_title from this scope.
    from heroes_platform.heroes_telegram_mcp.supabase_writer import _resolve_chat_title
    chat_title = await _resolve_chat_title(client, cid_int)

    floor = _resolve_floor(cursor)
    result.floor_before = floor

    # Локальный аккумулятор written для этого ВЫЗОВА deep_backfill_one_chat
    # (snapshot перед FloodWait retry). RCA C2: если использовать `result.written`
    # как «истинный» счётчик и инкрементить на каждый _flush(), FloodWait retry
    # после нескольких успешных _flush() даст double-count (первый проход
    # дописал N, retry дописал N+M идемпотентно через ON CONFLICT, но счётчик
    # увидит 2*N+M). Локальный pass_written обнуляется в начале каждого _do(),
    # и переносится в result.written ТОЛЬКО ОДИН раз — после успешного выхода
    # из _do() (см. блок коммита после try).
    pass_state: dict[str, Any] = {"written": 0, "min_id_seen": None, "seen": 0}

    async def _do() -> DeepBackfillChatResult:
        batch: list[Any] = []
        batch_size = int(getattr(writer, "batch_size", 100) or 100)
        # Reset per-pass counters at START — FloodWait retry получит чистый старт.
        pass_state["written"] = 0
        pass_state["min_id_seen"] = None
        pass_state["seen"] = 0

        async def _flush() -> None:
            """Запись batch + продвижение cursor + reporter update.

            RCA B1 / C3 / H1: ВСЁ что меняет видимое состояние — здесь, после
            каждого batch, а не в конце цикла. Это даёт:
              - reporter (result.min_id_seen / floor_after) видит прогресс
                даже если процесс убьют между _flush() и концом цикла;
              - cursor продвигается инкрементально → idempotent ON CONFLICT
                защищает повторный проход с новым floor;
              - filter msg.id > 0: MessageEmpty / id=0 не должны участвовать в
                min(): иначе min=0 → floor=0 → следующий проход стартует от
                newest и пишет уже виденные сообщения (no-op по ON CONFLICT,
                но трата FloodWait-бюджета).
            """
            if not batch:
                return
            # Snapshot до записи: при partial failure внутри write_messages_batch
            # local accumulator уже обновится только после успешного await.
            valid_ids = [
                int(getattr(m, "id", 0) or 0) for m in batch if int(getattr(m, "id", 0) or 0) > 0
            ]
            n = await writer.write_messages_batch(batch, cid_int, chat_type, chat_title)
            pass_state["written"] += n
            if valid_ids:
                batch_min = min(valid_ids)
                prev = pass_state["min_id_seen"]
                pass_state["min_id_seen"] = batch_min if prev is None else min(prev, batch_min)
                # H1 incremental cursor: продвигаем floor после КАЖДОГО batch.
                # LEAST в _update_chat_cursor_pg гарантирует монотонность даже
                # при race с параллельным проходом. last_backfill_ts обновляется
                # только когда floor реально опустился (зеркало GREATEST).
                result.min_id_seen = pass_state["min_id_seen"]
                result.floor_after = pass_state["min_id_seen"]
                await writer.update_chat_cursor(
                    cid_int,
                    last_backfill_message_id=pass_state["min_id_seen"],
                )
            batch.clear()

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
            pass_state["seen"] += 1
            if len(batch) >= batch_size:
                await _flush()
        await _flush()

        # ── Completion semantics (RCA H2) ────────────────────────────────────
        # Терминал ТОЛЬКО когда `seen == 0` — это значит «реально дно: 0
        # сообщений старше floor». Раньше использовали `seen < per_run_limit`
        # — это false-positive: transient short batch (rate-limit / network
        # blip / gap в Telegram pagination) ложно метил completed=TRUE
        # навсегда; следующий проход видел backfill_completed=TRUE и
        # пропускал чат — undetected потеря истории.
        # При `seen < per_run_limit && seen > 0` НЕ ставим completed,
        # оставляем для следующего прогона (idempotent, безопасно).
        if pass_state["seen"] == 0:
            # 0 сообщений старше floor → реально упёрлись в начало чата.
            await writer.update_chat_cursor(
                cid_int,
                backfill_completed=True,
            )
            result.completed = True

        return result

    # ── Внешний try с FloodWait single-retry ─────────────────────────────────
    # RCA C2: result.written НЕ инкрементится в _flush() — иначе retry после
    # успешных _flush() задвоит счётчик. Здесь делаем единственный transfer
    # pass_state["written"] → result.written после успешного завершения _do().
    try:
        outcome = await _do()
        result.written = pass_state["written"]
        return outcome
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
                outcome = await _do()
                # _do() при retry сделал reset pass_state["written"]=0 → берём
                # значение второго прохода (ON CONFLICT защищает от дублей в БД,
                # но видимый счётчик — только второго прохода).
                result.written = pass_state["written"]
                return outcome
            except Exception as retry_exc:  # noqa: BLE001
                # Сохраняем то что успели записать в любом из проходов до сбоя.
                result.written = pass_state["written"]
                result.error = f"{type(retry_exc).__name__}: {retry_exc}"
                return result
        result.written = pass_state["written"]
        result.error = f"{type(exc).__name__}: {exc}"
        return result


# ── select chats by priority (PG) ─────────────────────────────────────────────
def _load_priority_chat_ids() -> set[str]:
    """Клиентские chat_id с tier=0 (highest priority).

    Источники (по убыванию):
      1. env `DEEP_BACKFILL_PRIORITY_CHATS` — comma-separated chat_ids (для
         тестов / быстрого override).
      2. env `DEEP_BACKFILL_PRIORITY_FILE` — путь к JSON со списком id (на
         laba монтируется из advising-clients-registry.yaml через отдельный
         генератор; здесь — только consumer, не парсим YAML напрямую → нет
         зависимости от структуры реестра в этом hot-path модуле).

    Universal: новый клиент = строка в реестре → регенерация JSON → авто-tier
    без правки этого файла. RCA design: «priority tier из списка клиентских
    chat_id» (Stage-7 review).

    Returns set[str]. Если ничего не найдено → пустое множество (FIFO
    fallback в SQL — сохраняет старое поведение).
    """
    raw = os.getenv("DEEP_BACKFILL_PRIORITY_CHATS", "").strip()
    out: set[str] = set()
    if raw:
        for token in raw.split(","):
            token = token.strip()
            if token:
                out.add(token)
    file_path = os.getenv("DEEP_BACKFILL_PRIORITY_FILE", "").strip()
    if file_path:
        try:
            import json as _json
            from pathlib import Path as _Path

            data = _json.loads(_Path(file_path).read_text())
            # Поддерживаем [int, ...] и [{"chat_id": ...}, ...] — не навязываем
            # одну форму потребителю.
            for entry in data if isinstance(data, list) else []:
                if isinstance(entry, (int, str)):
                    out.add(str(entry))
                elif isinstance(entry, dict) and "chat_id" in entry:
                    out.add(str(entry["chat_id"]))
        except (OSError, ValueError) as exc:
            logger.warning(
                "Deep backfill priority file unreadable (%s): %s — falling back to FIFO",
                file_path,
                exc,
            )
    return out


def _select_chats_for_deep_backfill_pg(
    conn: Any,
    schema: str,
    telegram_user_id: str,
    *,
    limit: int,
    priority_chat_ids: set[str] | None = None,
) -> list[tuple[str, str]]:
    """SELECT chats нуждающихся в deep backfill.

    Приоритет:
    1. backfill_completed = FALSE
    2. is_active = TRUE
    3. ORDER BY priority_tier ASC (0=клиент, 1=прочие) ← клиентские чаты ПЕРВЫМИ
    4. ORDER BY last_backfill_ts ASC NULLS FIRST  ← никогда не запускавшиеся
       первыми внутри своего tier
    5. LIMIT N

    Design (Stage-7 review): RCA — old FIFO не различал клиентов от системных
    чатов; долгий «холодный» проход догонял служебные диалоги, а клиентские
    выпадали в хвост. Tier из списка priority_chat_ids (см. `_load_priority_chat_ids`).

    Возвращает [(chat_id, chat_type), ...]. chat_type подтягивается LEFT JOIN
    из `telegram_chats` (там есть `chat_type`); если отсутствует → 'unknown'.
    """
    priority_chat_ids = priority_chat_ids or set()
    cur = conn.cursor()
    try:
        # ARRAY[…]::text[] перечисляем как параметр, чтобы избежать SQL-инъекции
        # (chat_id это BIGINT приходящий из реестра / env). Пустой массив =>
        # tier всегда 1 для всех — поведение совпадает со старым FIFO.
        priority_list = [str(x) for x in priority_chat_ids]
        cur.execute(
            f"""
            SELECT s.chat_id,
                   COALESCE(c.chat_type, 'unknown') AS chat_type,
                   CASE WHEN s.chat_id = ANY(%s::text[]) THEN 0 ELSE 1 END
                       AS priority_tier
            FROM {schema}.telegram_chat_state s
            LEFT JOIN {schema}.telegram_chats c
                   ON c.chat_id = s.chat_id
            WHERE s.telegram_user_id = %s
              AND s.backfill_completed = FALSE
              AND s.is_active = TRUE
            ORDER BY priority_tier ASC,
                     s.last_backfill_ts ASC NULLS FIRST,
                     s.chat_id ASC
            LIMIT %s
            """,
            (priority_list, telegram_user_id, limit),
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

    Priority chat_ids подтягиваются из env (см. `_load_priority_chat_ids`).
    Пустой список priority → tier всегда 1 → поведение совпадает со старым
    FIFO (backward-compat).
    """
    pg_url = getattr(writer, "_postgres_url", None)
    if not pg_url:
        return []
    pg_conn_factory = getattr(writer, "_pg_conn", None)
    if pg_conn_factory is None:
        return []
    priority = _load_priority_chat_ids()
    with pg_conn_factory() as conn:
        return _select_chats_for_deep_backfill_pg(
            conn,
            schema=writer.schema,
            telegram_user_id=writer.telegram_user_id,
            limit=limit,
            priority_chat_ids=priority,
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
            chats = await _select_chats_for_deep_backfill(writer, limit=chat_select_limit)

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

        # Detect-of-detector: stall = обрабатывали чаты, но НИЧЕГО не записали
        # И никто не failed И не было budget_exhausted. Это либо все floors
        # реально на дне (H2 регрессия: completed не выставлен), либо API
        # тихо отдаёт пустые батчи. В обоих случаях — НЕ молчаливо green.
        # Не считаем stall если все processed чаты были уже completed (валидный
        # no-op).
        non_completed_processed = sum(
            1 for c in result.per_chat if not c.completed and c.error is None
        )
        if (
            result.chats_processed > 0
            and result.messages_written == 0
            and result.chats_failed == 0
            and not result.budget_exhausted
            and non_completed_processed > 0
        ):
            result.stalled = True

        logger.info(
            "Deep backfill (user=%s): processed=%d completed=%d failed=%d "
            "written=%d budget_exhausted=%s stalled=%s",
            getattr(writer, "telegram_user_id", "?"),
            result.chats_processed,
            result.chats_completed,
            result.chats_failed,
            result.messages_written,
            result.budget_exhausted,
            result.stalled,
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
