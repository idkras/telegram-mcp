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
    BACKFILL_STARTUP_DIALOG_LIMIT=1000        — max dialogs checked after restart
    BACKFILL_PERIODIC_DIALOG_LIMIT=500        — max recent dialogs per hourly pass
    BACKFILL_RECENT_LOOKBACK_SECONDS=21600    — prove a six-hour recent window
    BACKFILL_PERIODIC_INTERVAL_SECONDS=3600   — повтор каждые N сек; 0 = выключить
    BACKFILL_FLOOD_WAIT_MAX_SECONDS=300       — макс. сон по FloodWaitError (иначе skip)
"""

from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
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
    dialog_limit: int = 0
    lookback_seconds: int = 0
    lookback_cutoff_reached: bool = False
    budget_exhausted: bool = False
    audit_available: bool = False
    audit_heads_checked: int = 0
    audit_heads_present: int = 0
    audit_heads_missing: int = 0
    audit_heads_skipped_policy: int = 0
    audit_heads_classification_errors: int = 0
    audit_accepted_checked: int = 0
    audit_accepted_present: int = 0
    audit_accepted_missing: int = 0
    audit_skip_reasons: dict[str, int] = field(default_factory=dict)
    audit_error: str = ""
    errors: list[str] = field(default_factory=list)

    @property
    def chats_unchanged(self) -> int:
        return max(0, self.chats_scanned - self.chats_with_new - self.chats_failed)

    def marker_mode(self, phase: str) -> str:
        """Раздельный health-маркер: _session_dead / _partial / _ok."""
        if self.session_dead:
            return f"backfill_{phase}_session_dead"
        if (
            self.chats_failed > 0
            or self.audit_heads_missing > 0
            or self.audit_accepted_missing > 0
            or self.audit_heads_classification_errors > 0
        ):
            return f"backfill_{phase}_partial"
        if self.lookback_seconds > 0 and not self.lookback_cutoff_reached:
            return f"backfill_{phase}_bounded"
        return f"backfill_{phase}_ok"

    @property
    def recent_scope_complete(self) -> bool:
        return self.lookback_seconds > 0 and self.lookback_cutoff_reached

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

    seed_cursor_hwm = 0  # high-water-mark — cursor only ever advances (path-agnostic)

    async def _flush() -> bool:
        nonlocal batch, written, seed_cursor_hwm
        if not batch:
            return True
        n = await writer.write_messages_batch(batch, chat_id, chat_type, chat_title)
        written += n
        if n < len(batch):
            logger.warning(
                "Seed partial write for chat %s: wrote %d/%d; cursor not advanced for this batch",
                chat_id,
                n,
                len(batch),
            )
            batch = []
            return False
        max_id = max(int(getattr(m, "id", 0) or 0) for m in batch)
        # Bug I2-REST (code+falsifier review pr-hero-1u1): with newest→oldest iteration
        # each batch has a DECREASING max_id. _update_chat_cursor_pg guards with GREATEST,
        # but the REST-only path (SUPABASE_TELEGRAM_USE_REST_ONLY=1) does a blind upsert
        # WITHOUT GREATEST → later (older) batches would regress the cursor to an old id →
        # forward catch-up re-fetches. Enforce monotonicity HERE (in-code high-water-mark)
        # so the cursor is path-agnostic: only advance, never send a value below the hwm.
        if max_id > seed_cursor_hwm:
            seed_cursor_hwm = max_id
            await writer.update_chat_cursor(chat_id, last_seen_message_id=seed_cursor_hwm)
        batch = []
        return True

    # Bug I2 (pr-hero-1u1): `reverse=True` at offset_id=0 walks from the OLDEST
    # message → this seeded the FIRST `limit` messages (years old) and parked
    # last_seen on an old id, so forward catch-up then crawled the whole history and
    # fresh messages arrived hours/days late (SSOT acceptance «max(message_ts)<6h»
    # broke). Default iter_messages (newest→oldest) seeds the LATEST `limit`; the first
    # (newest) batch sets the high-water-mark to the true max, older batches are in-code
    # no-ops (see _flush), so the cursor lands on the newest id on BOTH PG and REST paths.
    async for msg in client.iter_messages(int(chat_id), limit=limit):
        batch.append(msg)
        seen += 1
        if len(batch) >= batch_size:
            if not await _flush():
                return written, True
    if not await _flush():
        return written, True

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
    cursor: dict[str, Any] | None = None,
) -> tuple[int, bool]:
    """Догнать один чат. Курсор есть → catch_up_recent (только новее курсора);
    нет → _seed_recent. FloodWaitError → sleep + один retry. Возвращает
    (written, truncated)."""
    flood_max = _env_int("BACKFILL_FLOOD_WAIT_MAX_SECONDS", 300)

    async def _do() -> tuple[int, bool]:
        selected_cursor = cursor if cursor is not None else await writer.get_chat_cursor(chat_id)
        if selected_cursor and selected_cursor.get("last_seen_message_id"):
            written = await writer.catch_up_recent(
                client,
                chat_id,
                chat_type,
                limit=per_chat_limit,
                cursor=selected_cursor,
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
    lookback_seconds: int = 0,
) -> BackfillResult:
    """Пройти по всем диалогам и догнать каждый. iter_dialogs обёрнут в try —
    мёртвая сессия/disconnect посреди прохода даёт partial result, а не silent
    crash. Ошибка одного чата изолирована."""
    if per_chat_limit is None:
        per_chat_limit = _env_int("BACKFILL_PER_CHAT_LIMIT", 5000)
    if seed_limit is None:
        seed_limit = _env_int("BACKFILL_NO_CURSOR_SEED_LIMIT", 1000)

    dialog_limit = max(0, int(dialog_limit or 0))
    lookback_seconds = max(0, int(lookback_seconds or 0))
    result = BackfillResult(dialog_limit=dialog_limit, lookback_seconds=lookback_seconds)
    scanned = 0
    head_keys: list[tuple[str, int]] = []
    cutoff = (
        datetime.now(tz=timezone.utc) - timedelta(seconds=lookback_seconds)
        if lookback_seconds > 0
        else None
    )
    cursor_by_chat: dict[str, dict[str, Any]] | None = None
    bulk_loader = getattr(writer, "get_chat_cursors", None)
    if callable(bulk_loader):
        try:
            cursor_by_chat = await bulk_loader()
        except Exception as exc:  # noqa: BLE001
            logger.warning("Bulk cursor load failed; using safe per-chat fallback: %s", exc)
    try:
        async for dialog in client.iter_dialogs(limit=dialog_limit or None):
            if dialog_limit and scanned >= dialog_limit:
                break
            chat_id = getattr(dialog, "id", None)
            if chat_id in (None, 0):
                logger.warning("Skipping dialog with empty chat_id: %r", dialog)
                continue
            chat_type = _chat_type_from_dialog(dialog)
            cursor = cursor_by_chat.get(str(chat_id)) if cursor_by_chat is not None else None
            latest_message = getattr(dialog, "message", None)
            latest_message_id = getattr(latest_message, "id", None)
            latest_message_date = getattr(latest_message, "date", None)
            if (
                cutoff is not None
                and latest_message_date is not None
                and not bool(getattr(dialog, "pinned", False))
            ):
                if latest_message_date.tzinfo is None:
                    latest_message_date = latest_message_date.replace(tzinfo=timezone.utc)
                if latest_message_date < cutoff:
                    result.lookback_cutoff_reached = True
                    break
            scanned += 1
            if latest_message_id is not None:
                classifier = getattr(writer, "classify_message_for_ingest", None)
                classification = (
                    classifier(
                        latest_message,
                        chat_id,
                        getattr(dialog, "name", None),
                    )
                    if callable(classifier)
                    else {"eligible": True, "reason": "classifier_unavailable"}
                )
                if classification.get("eligible"):
                    head_keys.append((str(chat_id), int(latest_message_id)))
                else:
                    reason = "guard_error" if classification.get("classification_error") else "policy_skip"
                    result.audit_heads_skipped_policy += 1
                    result.audit_skip_reasons[reason] = (
                        result.audit_skip_reasons.get(reason, 0) + 1
                    )
                    if classification.get("classification_error"):
                        result.audit_heads_classification_errors += 1
            if (
                cursor
                and cursor.get("last_seen_message_id")
                and latest_message_id is not None
                and int(latest_message_id) <= int(cursor["last_seen_message_id"])
            ):
                result.merge_chat(0)
                continue
            try:
                written, truncated = await backfill_one_chat(
                    client,
                    writer,
                    chat_id,
                    chat_type,
                    per_chat_limit=per_chat_limit,
                    seed_limit=seed_limit,
                    cursor=cursor,
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
    result.budget_exhausted = bool(
        dialog_limit and scanned >= dialog_limit and not result.lookback_cutoff_reached
    )

    auditor = getattr(writer, "audit_message_keys", None)
    if callable(auditor):
        audit = await auditor(head_keys)
        result.audit_available = bool(audit.get("available"))
        result.audit_heads_checked = int(audit.get("checked") or 0)
        result.audit_heads_present = int(audit.get("present") or 0)
        result.audit_heads_missing = int(audit.get("missing") or 0)
        result.audit_error = str(audit.get("reason") or "")
        accepted_audit = {
            "available": True,
            "checked": 0,
            "present": 0,
            "missing": 0,
        }
        accepted_keys_reader = getattr(writer, "get_recent_accepted_keys", None)
        if callable(accepted_keys_reader):
            accepted_keys = accepted_keys_reader()
            accepted_audit = await auditor(accepted_keys, operation="reconcile_accepted")
            result.audit_accepted_checked = int(accepted_audit.get("checked") or 0)
            result.audit_accepted_present = int(accepted_audit.get("present") or 0)
            result.audit_accepted_missing = int(accepted_audit.get("missing") or 0)
        snapshot = {
            **audit,
            "dialog_limit": result.dialog_limit,
            "dialogs_scanned": result.chats_scanned,
            "lookback_seconds": result.lookback_seconds,
            "lookback_cutoff_reached": result.lookback_cutoff_reached,
            "budget_exhausted": result.budget_exhausted,
            "recent_scope_complete": result.recent_scope_complete,
            "skipped_policy": result.audit_heads_skipped_policy,
            "classification_errors": result.audit_heads_classification_errors,
            "accepted_checked": result.audit_accepted_checked,
            "accepted_present": result.audit_accepted_present,
            "accepted_missing": result.audit_accepted_missing,
            "accepted_audit_available": bool(accepted_audit.get("available")),
            "audit_key_scope": "dialog_heads_and_recent_accepted",
        }
        publisher = getattr(writer, "set_reconciliation_snapshot", None)
        if callable(publisher):
            publisher(snapshot)
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
            legacy_limit = _env_int("BACKFILL_DIALOG_LIMIT", 0)
            phase_limit = _env_int(
                "BACKFILL_STARTUP_DIALOG_LIMIT"
                if phase == "startup"
                else "BACKFILL_PERIODIC_DIALOG_LIMIT",
                1000 if phase == "startup" else 500,
            )
            result = await backfill_all_chats(
                client,
                writer,
                dialog_limit=legacy_limit or phase_limit,
                lookback_seconds=_env_int("BACKFILL_RECENT_LOOKBACK_SECONDS", 21600),
            )
        logger.info(
            "Backfill %s (user=%s): scanned=%d new=%d written=%d unchanged=%d failed=%d "
            "truncated=%d session_dead=%s budget_exhausted=%s lookback_complete=%s "
            "heads=%d/%d missing=%d accepted=%d/%d accepted_missing=%d "
            "skipped_policy=%d classification_errors=%d",
            phase,
            user,
            result.chats_scanned,
            result.chats_with_new,
            result.messages_written,
            result.chats_unchanged,
            result.chats_failed,
            result.seed_truncated_chats,
            result.session_dead,
            result.budget_exhausted,
            result.recent_scope_complete,
            result.audit_heads_present,
            result.audit_heads_checked,
            result.audit_heads_missing,
            result.audit_accepted_present,
            result.audit_accepted_checked,
            result.audit_accepted_missing,
            result.audit_heads_skipped_policy,
            result.audit_heads_classification_errors,
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


async def _run_deep_backfill_phase(client: Any, writer: Any) -> Any:
    """Run the bounded backward phase on the listener's existing client."""
    from heroes_platform.heroes_telegram_mcp.deep_backfill import deep_backfill_all_chats

    options = dict(
        total_budget=_env_int("DEEP_BACKFILL_TOTAL_BUDGET", 1000),
        per_chat_limit=_env_int("DEEP_BACKFILL_PER_CHAT_LIMIT", 250),
        chat_select_limit=_env_int("DEEP_BACKFILL_SELECT_LIMIT", 50),
    )
    max_passes = max(1, min(_env_int("DEEP_BACKFILL_STARTUP_MAX_PASSES", 20), 500))
    result = None
    for pass_number in range(1, max_passes + 1):
        result = await deep_backfill_all_chats(client, writer, **options)
        failures = [item for item in result.per_chat if item.error]
        if failures:
            if all(item.inactivated for item in failures):
                logger.info(
                    "Deep backfill continuing after inactivating %d inaccessible chats",
                    len(failures),
                )
                continue
            break
        unfinished = [item for item in result.per_chat if not item.completed]
        if result.messages_written > 0 and unfinished and pass_number < max_passes:
            logger.info(
                "Deep backfill continuing pass %d/%d with %d unfinished chats",
                pass_number + 1,
                max_passes,
                len(unfinished),
            )
            continue
        break
    return result


async def run_startup_cycle(client: Any, writer: Any) -> BackfillResult:
    """Prioritise bounded history, then optionally scan every chat forward."""
    if _env_bool("DEEP_BACKFILL_IN_LISTENER", False):
        await _run_deep_backfill_phase(client, writer)
    if _env_bool("BACKFILL_ON_STARTUP", True):
        return await run_startup_backfill(client, writer)
    return BackfillResult()


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
            if _env_bool("DEEP_BACKFILL_IN_LISTENER", False):
                await _run_deep_backfill_phase(client, writer)
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
    if _env_bool("BACKFILL_ON_STARTUP", True) or _env_bool("DEEP_BACKFILL_IN_LISTENER", False):
        t = loop.create_task(run_startup_cycle(client, writer))
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
