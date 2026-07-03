#!/usr/bin/env python3
"""Startup + periodic backfill orchestration for Telegram -> Supabase ingestion.

JTBD: Когда laba telegram-mcp сервис поднят, я хочу чтобы он не только писал
новые сообщения в реальном времени (live NewMessage handler), но и ДОГОНЯЛ
пропущенное — на старте (после простоя / смерти сессии) и периодически (дрейф
live-handler), чтобы ни одно сообщение не терялось ни для одного клиента.

Архитектура (§Wiring-first — переиспользуем существующие методы SupabaseWriter,
не строим параллельную систему):
    - Реальное время: event_handlers.register_event_handlers (NewMessage) — уже есть.
    - Догон gap: SupabaseWriter.catch_up_recent(chat) — уже есть (min_id=cursor).
    - ЧЕГО НЕ ХВАТАЛО и что добавляет этот модуль:
        1. Оркестрация catch_up по ВСЕМ диалогам (iter_dialogs), а не per-chat.
        2. Seed чатов БЕЗ курсора (catch_up_recent их пропускал, return 0).
        3. Запуск на старте сервиса (background task, не блокирует live-ingestion).
        4. Периодический повтор (ловит сообщения, которые live-handler пропустил).

Защита от ложно-зелёного сигнала (design review 2026-06-09, §Detect-of-detector +
§Telegram session-per-endpoint — ровно класс, вызвавший 46-дневную заморозку):
    - PRE-FLIGHT liveness: перед backfill проверяем client.is_user_authorized().
      Сессия мертва → пишем ОТРИЦАТЕЛЬНЫЙ маркер backfill_*_session_dead и
      выходим — НЕ создаём фальшивый зелёный backfill_*_ok.
    - Раздельные маркеры: backfill_startup_ok / _session_dead / _partial — health
      consumer (SwiftBar) отличает «догнал» от «не смог даже залогиниться».
    - add_done_callback: silent смерть background-таска логируется (иначе exception
      улетал в asyncio default handler → никто не видел → молчаливая заморозка).

Универсальность: всё через writer.telegram_user_id (он же определяет schema —
rick_messages_tasks / tg_lisa / ...). Новый клиент = свой контейнер с
TELEGRAM_USER=<alias> + своя сессия. Кода править НЕ нужно.

Идемпотентность: write_messages_batch использует upsert ON CONFLICT
(chat_id, message_id); update_chat_cursor монотонен (GREATEST) — повторный
backfill поверх live-данных дублей и регрессии курсора не создаёт.

Изоляция: падение одного чата не валит проход (chats_failed++). FloodWaitError
от Telegram → sleep + один retry (это не «провал», а «остынь»).

Non-goals (честно — чтобы владелец не считал что выгружено больше, чем есть):
    - Это НЕ полный исторический архиватор. Для чата БЕЗ курсора засевается
      последние BACKFILL_NO_CURSOR_SEED_LIMIT сообщений (старше — не тянутся;
      если упёрлись в лимит → seed_truncated_chats++ + WARN). Полная история —
      отдельная задача (chat_exporter), не realtime-ingestion.
    - MessageDeleted не персистится здесь (live-handler только логирует) — Supabase
      может содержать сообщения, удалённые в Telegram. Soft-delete — отдельный bead.

Конфиг (env, дефолты безопасны для prod):
    BACKFILL_ON_STARTUP=true                  — гонять backfill при старте (LABA_MODE)
    BACKFILL_PER_CHAT_LIMIT=5000              — макс. сообщений за catch_up одного чата
    BACKFILL_NO_CURSOR_SEED_LIMIT=1000        — последних сообщений для нового чата
    BACKFILL_DIALOG_LIMIT=0                   — 0 = все диалоги; N>0 = staged rollout
    BACKFILL_PERIODIC_INTERVAL_SECONDS=3600   — повтор каждые N сек; 0 = выключить
    BACKFILL_FLOOD_WAIT_MAX_SECONDS=300       — макс. сон по FloodWaitError (иначе skip)
"""

from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

# Один backfill за раз на процесс: startup и periodic не должны идти параллельно
# (двойной iter_dialogs → удвоенный rate-limit hit → FloodWait). Lazy-init, т.к.
# Lock привязывается к running loop.
_backfill_lock: asyncio.Lock | None = None


def _get_lock() -> asyncio.Lock:
    global _backfill_lock
    if _backfill_lock is None:
        _backfill_lock = asyncio.Lock()
    return _backfill_lock


# ── config ──────────────────────────────────────────────────────────────────
def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


@dataclass
class BackfillResult:
    """Итог одного прохода backfill по всем чатам."""

    chats_scanned: int = 0
    chats_with_new: int = 0
    messages_written: int = 0
    chats_failed: int = 0
    seed_truncated_chats: int = 0  # новый чат, где упёрлись в seed_limit (история обрезана)
    session_dead: bool = False  # pre-flight liveness провалился — ничего не догнали
    errors: list[str] = field(default_factory=list)

    @property
    def chats_unchanged(self) -> int:
        return max(0, self.chats_scanned - self.chats_with_new - self.chats_failed)

    def marker_mode(self, phase: str) -> str:
        """Раздельный health-маркер: _session_dead / _partial / _ok."""
        if self.session_dead:
            return f"backfill_{phase}_session_dead"
        if self.chats_failed > 0:
            return f"backfill_{phase}_partial"
        return f"backfill_{phase}_ok"

    def merge_chat(self, written: int, *, truncated: bool = False) -> None:
        self.chats_scanned += 1
        if written > 0:
            self.chats_with_new += 1
            self.messages_written += written
        if truncated:
            self.seed_truncated_chats += 1

    def merge_failure(self, chat_id: Any, exc: Exception) -> None:
        self.chats_scanned += 1
        self.chats_failed += 1
        if len(self.errors) < 100:  # bounded — не растим список бесконечно на мёртвой сессии
            self.errors.append(f"chat={chat_id}: {type(exc).__name__}: {exc}")


# ── FloodWait (Telegram «остынь») — duck-type ─────────────────────────────────
def _flood_wait_seconds(exc: Exception) -> int | None:
    """Если это Telethon FloodWait* — вернуть .seconds, иначе None. Duck-type по
    имени класса + атрибуту: путь FloodWaitError менялся между версиями telethon
    (telethon.errors vs .rpcerrorlist), а isinstance к одному пути хрупок."""
    if "FloodWait" in type(exc).__name__ and hasattr(exc, "seconds"):
        try:
            return int(exc.seconds)
        except (TypeError, ValueError):
            return None
    return None


# ── chat type (зеркало event_handlers._get_chat_type, но из dialog.entity) ────
def _chat_type_from_dialog(dialog: Any) -> str:
    """Тип чата из Telethon dialog. Чистая функция (без сети) — юнит-тестируема."""
    entity = getattr(dialog, "entity", None)
    if entity is None:
        return "unknown"
    if hasattr(entity, "broadcast"):
        return "channel" if entity.broadcast else "supergroup"
    if getattr(entity, "megagroup", False):
        return "supergroup"
    if hasattr(entity, "participants_count"):
        return "group"
    if hasattr(entity, "first_name"):
        return "private"
    return "unknown"


# ── pre-flight liveness ───────────────────────────────────────────────────────
async def _session_alive(client: Any) -> bool:
    """Жива ли Telegram-сессия. Mертвая (AUTHKEY_DUPLICATED / REVOKED) → backfill
    не имеет смысла и НЕ должен писать фальшивый зелёный маркер."""
    try:
        if not await client.is_user_authorized():
            return False
        await client.get_me()  # реальный RPC — ловит revoked-после-connect
        return True
    except Exception as exc:  # noqa: BLE001
        logger.warning("Session liveness probe failed: %s", exc)
        return False


# ── seed чата без курсора (инкрементальный курсор — фикс B1) ──────────────────
async def _seed_recent(
    client: Any,
    writer: Any,
    chat_id: int | str,
    chat_type: str,
    limit: int,
) -> tuple[int, bool]:
    """Чат без курсора: тянем последние `limit` сообщений, пишем батчами и
    двигаем курсор ПОСЛЕ КАЖДОГО успешного батча (B1: если упадёт середина —
    курсор уже на max успешно записанного, повторный seed не дублирует всё).

    Возвращает (written, truncated): truncated=True если упёрлись в limit
    (значит история чата длиннее — часть осталась незасеяна, это видимый сигнал).
    """
    batch: list[Any] = []
    written = 0
    seen = 0
    batch_size = int(getattr(writer, "batch_size", 100) or 100)

    # security-1 (pr-hero-x0p): resolve title once so seed honours title/username
    # skip — iter_messages below gives msg.chat=None, so the guardian would miss a
    # code-relay chat not in id_tails on first seed without this.
    from heroes_platform.heroes_telegram_mcp.supabase_writer import _resolve_chat_title
    chat_title = await _resolve_chat_title(client, chat_id)

    async def _flush() -> None:
        nonlocal batch, written
        if not batch:
            return
        n = await writer.write_messages_batch(batch, chat_id, chat_type, chat_title)
        written += n
        max_id = max(int(getattr(m, "id", 0) or 0) for m in batch)
        if max_id > 0:
            await writer.update_chat_cursor(chat_id, last_seen_message_id=max_id)
        batch = []

    async for msg in client.iter_messages(int(chat_id), limit=limit, reverse=True):
        batch.append(msg)
        seen += 1
        if len(batch) >= batch_size:
            await _flush()
    await _flush()

    return written, (seen >= limit > 0)


# ── backfill одного чата (с FloodWait retry) ──────────────────────────────────
async def backfill_one_chat(
    client: Any,
    writer: Any,
    chat_id: int | str,
    chat_type: str,
    *,
    per_chat_limit: int,
    seed_limit: int,
) -> tuple[int, bool]:
    """Догнать один чат. Курсор есть → catch_up_recent (только новее курсора);
    нет → _seed_recent. FloodWaitError → sleep + один retry. Возвращает
    (written, truncated)."""
    flood_max = _env_int("BACKFILL_FLOOD_WAIT_MAX_SECONDS", 300)

    async def _do() -> tuple[int, bool]:
        cursor = await writer.get_chat_cursor(chat_id)
        if cursor and cursor.get("last_seen_message_id"):
            written = await writer.catch_up_recent(
                client, chat_id, chat_type, limit=per_chat_limit
            )
            return written, False
        return await _seed_recent(client, writer, chat_id, chat_type, limit=seed_limit)

    try:
        return await _do()
    except Exception as exc:  # noqa: BLE001
        wait = _flood_wait_seconds(exc)
        if wait is not None and 0 <= wait <= flood_max:
            logger.warning("FloodWait %ss on chat %s — sleeping then retry", wait, chat_id)
            await asyncio.sleep(wait)
            return await _do()  # один retry; повторный FloodWait/ошибка пробросится наверх
        raise


# ── backfill всех чатов ───────────────────────────────────────────────────────
async def backfill_all_chats(
    client: Any,
    writer: Any,
    *,
    dialog_limit: int = 0,
    per_chat_limit: int | None = None,
    seed_limit: int | None = None,
) -> BackfillResult:
    """Пройти по всем диалогам и догнать каждый. iter_dialogs обёрнут в try —
    мёртвая сессия/disconnect посреди прохода даёт partial result, а не silent
    crash. Ошибка одного чата изолирована."""
    if per_chat_limit is None:
        per_chat_limit = _env_int("BACKFILL_PER_CHAT_LIMIT", 5000)
    if seed_limit is None:
        seed_limit = _env_int("BACKFILL_NO_CURSOR_SEED_LIMIT", 1000)

    result = BackfillResult()
    scanned = 0
    try:
        async for dialog in client.iter_dialogs():
            if dialog_limit and scanned >= dialog_limit:
                break
            scanned += 1
            chat_id = getattr(dialog, "id", None)
            if chat_id in (None, 0):
                logger.warning("Skipping dialog with empty chat_id: %r", dialog)
                continue
            chat_type = _chat_type_from_dialog(dialog)
            try:
                written, truncated = await backfill_one_chat(
                    client,
                    writer,
                    chat_id,
                    chat_type,
                    per_chat_limit=per_chat_limit,
                    seed_limit=seed_limit,
                )
                result.merge_chat(written, truncated=truncated)
                if truncated:
                    logger.warning(
                        "Chat %s seeded to limit (%d) — older history NOT backfilled",
                        chat_id,
                        seed_limit,
                    )
            except Exception as exc:  # noqa: BLE001 — изоляция: один чат не валит проход
                logger.warning("Backfill failed for chat %s: %s", chat_id, exc)
                result.merge_failure(chat_id, exc)
    except Exception as exc:  # noqa: BLE001 — iter_dialogs сам упал (disconnect/dead)
        logger.warning("Dialog iteration aborted: %s", exc)
        result.errors.append(f"iter_dialogs: {type(exc).__name__}: {exc}")
        result.session_dead = True
    return result


# ── один проход с pre-flight + маркером (общий для startup и periodic) ────────
async def _run_one_pass(client: Any, writer: Any, phase: str) -> BackfillResult:
    """Pre-flight liveness → backfill_all_chats → раздельный health-маркер.
    Лок: не запускаем второй проход поверх ещё работающего."""
    lock = _get_lock()
    if lock.locked():
        logger.warning("Backfill (%s) skipped: previous pass still running", phase)
        return BackfillResult()
    async with lock:
        user = getattr(writer, "telegram_user_id", "?")
        if not await _session_alive(client):
            logger.warning("Backfill (%s) skipped: session DEAD (user=%s)", phase, user)
            result = BackfillResult(session_dead=True)
        else:
            result = await backfill_all_chats(client, writer)
        logger.info(
            "Backfill %s (user=%s): scanned=%d new=%d written=%d unchanged=%d failed=%d "
            "truncated=%d session_dead=%s",
            phase,
            user,
            result.chats_scanned,
            result.chats_with_new,
            result.messages_written,
            result.chats_unchanged,
            result.chats_failed,
            result.seed_truncated_chats,
            result.session_dead,
        )
        try:
            await writer.record_runtime_event(
                mode=result.marker_mode(phase),
                processed_chats=result.chats_scanned,
                inserted_messages=result.messages_written,
                error=("; ".join(result.errors[:5]) if result.errors else None),
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to record %s marker: %s", phase, exc)
        return result


# ── boot-time backfill ────────────────────────────────────────────────────────
async def run_startup_backfill(client: Any, writer: Any) -> BackfillResult:
    """Запуск на старте сервиса: pre-flight + догон gap по всем чатам + маркер."""
    return await _run_one_pass(client, writer, "startup")


# ── периодический backfill (ловит дрейф live-handler) ─────────────────────────
async def periodic_backfill_loop(
    client: Any,
    writer: Any,
    interval_seconds: int,
) -> None:
    """Каждые interval_seconds повторяет проход. interval<=0 → не запускается.
    Jitter ±10% против грозовой стаи нескольких контейнеров. Завершается только
    отменой таска (CancelledError пробрасывается)."""
    if interval_seconds <= 0:
        return
    # Детерминированный «джиттер» из user_id (без Math.random — он недоступен и
    # ломает тестируемость): сдвиг 0..10% интервала, стабильный на контейнер.
    user = str(getattr(writer, "telegram_user_id", ""))
    jitter = (sum(ord(c) for c in user) % max(1, interval_seconds // 10)) if user else 0
    while True:
        try:
            await asyncio.sleep(interval_seconds + jitter)
            await _run_one_pass(client, writer, "periodic")
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 — цикл не должен умирать от одной ошибки
            logger.warning("Periodic backfill iteration failed: %s", exc)


# ── wiring helper (вызывается из event_handlers.register_event_handlers) ──────
def _log_task_exception(task: Any) -> None:
    """add_done_callback: silent смерть background-таска → видимый лог (иначе
    exception улетал в asyncio default handler, владелец узнавал только из
    отсутствия данных)."""
    try:
        exc = task.exception()
    except asyncio.CancelledError:
        return
    except Exception:  # noqa: BLE001
        return
    if exc is not None:
        logger.error("Backfill background task died: %s", exc, exc_info=exc)


def schedule_backfill_tasks(loop: Any, client: Any, writer: Any) -> list[Any]:
    """Создать background-таски: startup backfill (если BACKFILL_ON_STARTUP) +
    периодик (если BACKFILL_PERIODIC_INTERVAL_SECONDS > 0). Не блокирует
    live-ingestion. Каждый таск получает done-callback для отлова silent death.
    Возвращает список созданных тасков (для тестов/отмены)."""
    tasks: list[Any] = []
    if _env_bool("BACKFILL_ON_STARTUP", True):
        t = loop.create_task(run_startup_backfill(client, writer))
        _attach_done_callback(t)
        tasks.append(t)
    interval = _env_int("BACKFILL_PERIODIC_INTERVAL_SECONDS", 3600)
    if interval > 0:
        t = loop.create_task(periodic_backfill_loop(client, writer, interval))
        _attach_done_callback(t)
        tasks.append(t)
    return tasks


def _attach_done_callback(task: Any) -> None:
    try:
        task.add_done_callback(_log_task_exception)
    except Exception:  # noqa: BLE001 — фейк-loop в тестах может не поддерживать
        pass
