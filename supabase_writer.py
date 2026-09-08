#!/usr/bin/env python3
"""Supabase Writer for Telegram MCP Server.

JTBD: Когда Telegram MCP сервер получает новые сообщения через event handlers,
я хочу записывать их в Supabase таблицу telegram_messages_raw,
чтобы все сообщения из всех чатов сохранялись в централизованном хранилище.

Architecture:
    - Uses Supabase REST API via supabase-py client
    - Writes to telegram_messages_raw (bronze layer) with full raw JSONB
    - Manages telegram_chats registry plus telegram_chat_state per-user cursors
    - Logs ingest runs to telegram_ingest_runs with telegram_user_id scope
    - Dedup via unique index on (chat_id, message_id)

Credentials: registry-only API via credentials_registry (supabase_rick_api_key)
Migration: 20250110000001_telegram_tdlib_tables.sql (must be applied first)
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import socket
import threading
import time
import uuid
from collections import deque
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator

logger = logging.getLogger(__name__)

SUPABASE_URL = "https://supabase.rick.ai"

# ── Schema-per-profile resolution (RCA 2026-06-05, owner directive) ──
#
# Каждый Telegram-аккаунт пишет в СВОЮ схему — данные не смешиваются, и менеджерам
# выдаётся доступ одним грантом на схему Лизы (GRANT USAGE ON SCHEMA tg_lisa).
#
# Generalization-first gate (AGENTS.md): новый 3-й аккаунт = запись ключей в Keychain
# + TELEGRAM_USER=<имя> → схема tg_<slug>. Ноль правок Python-кода (Q4=YES).
#   1. Legacy ikrasinsky ОСТАЁТСЯ в rick_messages_tasks — туда смотрят все читатели
#      (n8n-бот V6jDG342xMRR5SwU, скилы 8-rick-clients-chats-supabase-search /
#      7-client-conversation-rag-first) + там уже лежат его данные. Переименование
#      сломало бы read-side по всему workspace.
#   2. Любой новый профиль → конвенция tg_{slug} (без правки кода).
# Канон slug совпадает с session_manager._slugify_profile (snake_case, latin-only).
_SCHEMA_PROFILE_OVERRIDES: dict[str, str] = {
    "ikrasinsky": "rick_messages_tasks",
    "ilyakrasinsky": "rick_messages_tasks",
    "ik": "rick_messages_tasks",
    "lisa": "tg_lisa",
}


# --- Index guardian (pr-hero-gcy): skip/redact sensitive chats+values on ingest ---
_GUARD_RULES: Any = None


def _guard_rules() -> Any:
    """Lazy-load index guardian rules once (SSOT: telegram_index_blacklist.yaml)."""
    global _GUARD_RULES
    if _GUARD_RULES is None:
        try:
            from . import index_guard  # type: ignore
        except ImportError:  # pragma: no cover — direct-run fallback
            import index_guard  # type: ignore
        _GUARD_RULES = (index_guard, index_guard.load_rules())
    return _GUARD_RULES


def _redact_raw_recursive(obj: Any, guard: Any, rules: Any) -> Any:
    """security-3 fix (pr-hero-x0p): mask sensitive values in EVERY string of the
    raw JSONB, not just raw["message"]. Telethon to_dict() puts text under varying
    keys (message/text/raw_text) and echoes it into fwd_from/quote/entities/
    reply_markup — a single-key redact left `SELECT raw->>'text'` leaking. Walks
    dict/list/str recursively and applies guard.redact_secrets to each string."""
    if isinstance(obj, str):
        red, _ = guard.redact_secrets(obj, rules)
        return red
    if isinstance(obj, dict):
        return {k: _redact_raw_recursive(v, guard, rules) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_redact_raw_recursive(v, guard, rules) for v in obj]
    return obj


async def _resolve_chat_title(telethon_client: Any, chat_id: Any) -> str | None:
    """security-1 fix (pr-hero-x0p): backfill uses iter_messages where msg.chat is
    NOT hydrated → derive-from-message fallback returns None → title_skip never fires
    → OTP relay leaks on backfill. Resolve the title once per chat via get_entity so
    backfill honours the same title/username skip as live NewMessage. Fallback chain
    title→first_name→username covers User/Bot entities (no .title). Returns None on
    any resolution error (writer still has content-redact + id_tail backstop)."""
    try:
        ent = await telethon_client.get_entity(int(chat_id))
    except Exception:  # noqa: BLE001 — resolution best-effort; None keeps id/content guards
        return None
    return (
        getattr(ent, "title", None)
        or getattr(ent, "first_name", None)
        or getattr(ent, "username", None)
    )


def _slugify_profile(profile: str) -> str:
    """Normalize profile/client name to a safe snake_case Postgres-identifier slug.

    Mirror of session_manager._slugify_profile so schema name и credential-имена
    выводятся из одного и того же правила.
    """
    import re

    slug = re.sub(r"[^a-z0-9]+", "_", (profile or "").strip().lower())
    return slug.strip("_")


def _schema_for_profile(profile: str) -> str:
    """Resolve Supabase schema name for a Telegram profile (universal, config-driven).

    Env SUPABASE_TELEGRAM_SCHEMA — hard override (operator знает, что задал per-process).
    Legacy профили → _SCHEMA_PROFILE_OVERRIDES (backward compatible).
    Любой новый профиль → tg_{slug}.

    Raises:
        ValueError: если slug пустой (whitespace/symbols/non-latin) — fail-fast
            вместо silent `tg_` namespace-collision.
    """
    env_override = os.getenv("SUPABASE_TELEGRAM_SCHEMA")
    if env_override:
        return env_override

    normalized = (profile or "").strip().lower()
    if normalized in _SCHEMA_PROFILE_OVERRIDES:
        return _SCHEMA_PROFILE_OVERRIDES[normalized]

    slug = _slugify_profile(profile)
    if not slug:
        raise ValueError(
            f"Cannot derive a safe Supabase schema slug from profile {profile!r} "
            "(empty after normalization). Use a latin-alphanumeric profile name "
            "or add an explicit _SCHEMA_PROFILE_OVERRIDES entry."
        )
    return f"tg_{slug}"


# Backward-compat module-level default (ikrasinsky). Per-instance schema всегда
# берётся из self.schema (см. SupabaseWriter.__init__) — НЕ из этой константы.
SUPABASE_SCHEMA = _schema_for_profile("ikrasinsky")
TABLE_MESSAGES = "telegram_messages_raw"
TABLE_ARTICLES = "telegram_articles"
TABLE_CHATS = "telegram_chats"
TABLE_CHAT_STATE = "telegram_chat_state"
TABLE_RUNS = "telegram_ingest_runs"
LISTENER_RUNTIME_MODES = ("listener_boot", "listener_heartbeat")


def _get_postgres_url() -> str | None:
    """Get Supabase Postgres connection URL (same as apply_telegram_migration / laba/n8n).

    When set, SupabaseWriter uses direct Postgres instead of REST — avoids PGRST106
    for schema rick_messages_tasks (Exposed schemas not required).
    """
    try:
        from credentials_registry import credentials_manager

        result = credentials_manager.get_credential("supabase_rick_db_url")
        if result.success and result.value:
            return result.value
    except ImportError:
        pass
    return None


def _get_supabase_client() -> Any:
    """Create Supabase client using Mac Keychain credentials.

    Returns:
        Supabase Client instance.

    Raises:
        RuntimeError: If credentials are missing or connection fails.
    """
    from supabase import create_client  # type: ignore

    api_key = None
    api_url = None
    try:
        from credentials_registry import credentials_manager

        result = credentials_manager.get_credential("supabase_rick_api_key")
        if result.success and result.value:
            api_key = result.value
        url_result = credentials_manager.get_credential("supabase_rick_api_url")
        if url_result.success and url_result.value:
            api_url = url_result.value
    except ImportError:
        pass

    if not api_key:
        raise RuntimeError(
            "Supabase API key not found through registry id 'supabase_rick_api_key'."
        )

    return create_client(api_url or SUPABASE_URL, api_key)


class SupabaseWriter:
    """Writes Telegram messages to Supabase.

    Handles batching, dedup, cursor management, and error handling.

    When postgres_url (or SUPABASE_DB_URL / Keychain supabase_rick_db_url) is set,
    uses direct PostgreSQL connection like laba/n8n — no REST, no Exposed schemas needed.
    """

    def __init__(
        self,
        telegram_user_id: str = "ikrasinsky",
        postgres_url: str | None = None,
    ) -> None:
        self._client: Any | None = None
        self._pg_pool: Any | None = None   # R3 D5: bounded connection pool (lazy)
        self._pg_pool_init_lock = threading.Lock()
        self.telegram_user_id = telegram_user_id
        # Schema-per-profile: каждый аккаунт пишет в свою схему (data не смешивается).
        self.schema = _schema_for_profile(telegram_user_id)
        self._batch: list[dict[str, Any]] = []
        self.batch_size = 50
        self._postgres_url = postgres_url or _get_postgres_url()
        self._ensure_db_load_state()
        # Direct Postgres (Keychain SUPABASE_DB_URL / supabase_rick_db_url) may be unreachable from some
        # networks while HTTPS REST to supabase.rick.ai works — force REST upserts for chat registry.
        if os.getenv("SUPABASE_TELEGRAM_USE_REST_ONLY", "").strip().lower() in (
            "1",
            "true",
            "yes",
        ):
            self._postgres_url = None

        # security-5 fix (pr-hero-x0p): fail-fast on a broken guardian YAML at
        # startup. A lazy load meant a corrupt telegram_index_blacklist.yaml would
        # blow up only at first ingest → fail-closed then drops EVERY message
        # silently (guardian dead, but the unit stays "active"). Loading here makes
        # the unit refuse to boot → visible in journald + doctor deploy_units, so a
        # bad deploy is caught immediately. Opt-out for tests: TELEGRAM_GUARD_LAZY=1.
        if os.getenv("TELEGRAM_GUARD_LAZY", "").strip().lower() not in ("1", "true", "yes"):
            try:
                _guard_rules()
            except Exception as exc:  # noqa: BLE001
                raise RuntimeError(
                    f"index guardian YAML failed to load ({exc}) — refusing to start; "
                    "a broken guardian would fail-closed-drop every message silently"
                ) from exc

    @property
    def client(self) -> Any:
        """Lazy-init Supabase client (only used when not using direct Postgres)."""
        if self._client is None:
            self._client = _get_supabase_client()
        return self._client

    def _table(self, name: str) -> Any:
        """Table reference in this profile's schema (schema-per-profile)."""
        return self.client.schema(self.schema).from_(name)

    async def ping(self) -> tuple[bool, str]:
        """Verify that the configured Supabase transport is reachable.

        Used by docker/health monitoring probes so we can validate the LABA ingest
        contour without creating synthetic rows every minute.
        """
        try:
            if self._postgres_url:
                with self._pg_conn(operation="ping") as conn:
                    cur = conn.cursor()
                    try:
                        cur.execute("SELECT 1")
                        cur.fetchone()
                    finally:
                        cur.close()
                return True, "Supabase Postgres reachable"

            self._table(TABLE_RUNS).select("run_id").limit(1).execute()
            return True, f"Supabase REST reachable ({self.schema}.{TABLE_RUNS})"
        except Exception as exc:
            return False, f"Supabase probe failed: {exc}"

    async def get_runtime_health(
        self,
        max_staleness_seconds: int | None = None,
    ) -> tuple[bool, str]:
        """Validate that the runtime contour is reachable and still alive."""
        ok, message = await self.ping()
        if not ok:
            return False, message

        staleness_seconds = max_staleness_seconds or int(
            os.getenv("TELEGRAM_RUNTIME_MAX_STALENESS_SECONDS", "180")
        )
        try:
            listener_event_at, latest_message_at = self._get_runtime_activity()
            return _evaluate_runtime_health(
                listener_event_at=listener_event_at,
                latest_message_at=latest_message_at,
                max_staleness_seconds=staleness_seconds,
                transport_message=message,
            )
        except Exception as exc:
            return False, f"Telegram LABA runtime probe failed: {exc}"

    async def get_monitoring_snapshot(self) -> dict[str, Any]:
        """Return owner-facing ingest/history evidence through this process's pool.

        Monitoring must reuse the listener's existing session-mode connection.
        Opening a third standalone Postgres client can itself exhaust a small
        Supabase pool and turn a healthy listener into a false orange signal.
        """
        return await asyncio.to_thread(self._get_monitoring_snapshot_sync)

    def _get_monitoring_snapshot_sync(self) -> dict[str, Any]:
        """Cache and singleflight the owner-facing DB probe.

        MCP smoke performs four HTTP requests and can be invoked concurrently by
        SwiftBar, operators and agents.  Only the first caller per TTL is allowed
        to hit Postgres; joiners reuse the same evidence.  A failed refresh never
        returns an old success as green.
        """
        self._ensure_db_load_state()
        with self._db_metrics_lock:
            self._db_metrics["monitoring_requests_total"] += 1
        ttl = max(1.0, float(os.getenv("TELEGRAM_MONITORING_CACHE_TTL_SECONDS", "60")))
        failure_ttl = max(
            1.0, float(os.getenv("TELEGRAM_MONITORING_FAILURE_TTL_SECONDS", "15"))
        )
        now = time.monotonic()
        if self._monitoring_cache is not None and now - self._monitoring_cache_at <= ttl:
            with self._db_metrics_lock:
                self._db_metrics["monitoring_cache_hits_total"] += 1
            cached = dict(self._monitoring_cache)
            cached.update(cache_hit=True, stale=False, cache_age_s=round(now - self._monitoring_cache_at, 3))
            cached["db_load"] = self.get_db_load_snapshot()
            cached["ingest_accounting"] = self.get_ingest_accounting_snapshot()
            return cached
        if (
            self._monitoring_failure_cache is not None
            and now - self._monitoring_failure_at <= failure_ttl
        ):
            failed = dict(self._monitoring_failure_cache)
            failed.update(cache_hit=True, stale=self._monitoring_cache is not None)
            failed["db_load"] = self.get_db_load_snapshot()
            failed["ingest_accounting"] = self.get_ingest_accounting_snapshot()
            return failed

        with self._monitoring_refresh_lock:
            now = time.monotonic()
            if self._monitoring_cache is not None and now - self._monitoring_cache_at <= ttl:
                with self._db_metrics_lock:
                    self._db_metrics["monitoring_cache_hits_total"] += 1
                cached = dict(self._monitoring_cache)
                cached.update(cache_hit=True, stale=False, cache_age_s=round(now - self._monitoring_cache_at, 3))
                cached["db_load"] = self.get_db_load_snapshot()
                cached["ingest_accounting"] = self.get_ingest_accounting_snapshot()
                return cached
            if (
                self._monitoring_failure_cache is not None
                and now - self._monitoring_failure_at <= failure_ttl
            ):
                failed = dict(self._monitoring_failure_cache)
                failed.update(cache_hit=True, stale=self._monitoring_cache is not None)
                failed["db_load"] = self.get_db_load_snapshot()
                failed["ingest_accounting"] = self.get_ingest_accounting_snapshot()
                return failed
            with self._db_metrics_lock:
                self._db_metrics["monitoring_refreshes_total"] += 1
            result = self._query_monitoring_snapshot_sync()
            observed_at = datetime.now(tz=timezone.utc).isoformat()
            if result.get("ok") is True:
                result = dict(result)
                result.update(
                    observed_at=observed_at,
                    cache_hit=False,
                    stale=False,
                    cache_age_s=0.0,
                )
                self._monitoring_cache = dict(result)
                self._monitoring_cache_at = time.monotonic()
                self._monitoring_failure_cache = None
                self._monitoring_failure_at = 0.0
                self._refresh_recent_accepted_audit_sync()
                result["db_load"] = self.get_db_load_snapshot()
                result["ingest_accounting"] = self.get_ingest_accounting_snapshot()
                self._monitoring_cache["db_load"] = result["db_load"]
                self._monitoring_cache["ingest_accounting"] = result["ingest_accounting"]
                return result

            failed = dict(result)
            failed.update(
                ok=False,
                observed_at=observed_at,
                cache_hit=False,
                stale=self._monitoring_cache is not None,
                db_load=self.get_db_load_snapshot(),
                ingest_accounting=self.get_ingest_accounting_snapshot(),
            )
            if self._monitoring_cache is not None:
                failed["last_good"] = dict(self._monitoring_cache)
                failed["last_good_age_s"] = round(time.monotonic() - self._monitoring_cache_at, 3)
            self._monitoring_failure_cache = dict(failed)
            self._monitoring_failure_at = time.monotonic()
            return failed

    def _query_monitoring_snapshot_sync(self) -> dict[str, Any]:
        if not self._postgres_url:
            return {"ok": False, "error": "direct Postgres monitoring is not configured"}
        started = time.monotonic()
        try:
            with self._pg_conn(operation="monitoring", timeout_seconds=2.0) as conn:
                cur = conn.cursor()
                try:
                    cur.execute(
                        f"""
                        SELECT
                          extract(epoch from (now() -
                            (SELECT created_at FROM {self.schema}.telegram_messages_raw
                             ORDER BY id DESC LIMIT 1)))::int AS age_s,
                          (SELECT count(*) FROM {self.schema}.telegram_chat_state
                           WHERE is_active=true) AS active_chats,
                          (SELECT count(*) FROM {self.schema}.telegram_chat_state
                           WHERE is_active=true AND backfill_completed=true) AS completed_chats,
                          (SELECT mode FROM {self.schema}.telegram_ingest_runs
                           WHERE mode LIKE 'deep_backfill_%'
                           ORDER BY started_at DESC LIMIT 1) AS deep_mode,
                          (SELECT extract(epoch from (now() - started_at))::int
                           FROM {self.schema}.telegram_ingest_runs
                           WHERE mode LIKE 'deep_backfill_%'
                           ORDER BY started_at DESC LIMIT 1) AS deep_age_s,
                          (SELECT processed_chats FROM {self.schema}.telegram_ingest_runs
                           WHERE mode LIKE 'deep_backfill_%'
                           ORDER BY started_at DESC LIMIT 1) AS deep_chats,
                          (SELECT inserted_messages FROM {self.schema}.telegram_ingest_runs
                           WHERE mode LIKE 'deep_backfill_%'
                           ORDER BY started_at DESC LIMIT 1) AS deep_messages,
                          (SELECT last_error FROM {self.schema}.telegram_ingest_runs
                           WHERE mode LIKE 'deep_backfill_%'
                           ORDER BY started_at DESC LIMIT 1) AS deep_error
                        """
                    )
                    row = cur.fetchone()
                finally:
                    cur.close()
            self._record_db_work("monitoring", duration_s=time.monotonic() - started)
            if not row:
                return {"ok": False, "error": "monitoring query returned no row"}
            return {
                "ok": True,
                "profile": self.telegram_user_id,
                "schema": self.schema,
                "age_s": int(row[0]) if row[0] is not None else None,
                "active_chats": int(row[1] or 0),
                "completed_chats": int(row[2] or 0),
                "deep_mode": row[3],
                "deep_age_s": int(row[4]) if row[4] is not None else None,
                "deep_chats": int(row[5] or 0),
                "deep_messages": int(row[6] or 0),
                "deep_error": str(row[7] or ""),
            }
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "error": f"{type(exc).__name__}: {exc}"[:240]}

    def _ensure_db_load_state(self) -> None:
        """Initialize pool admission, rolling counters and health singleflight.

        The lazy branch keeps old tests and recovery scripts that construct the
        writer through ``__new__`` compatible while normal instances initialize
        everything during ``__init__``.
        """
        if hasattr(self, "_db_metrics_lock"):
            return
        init_lock = self.__dict__.setdefault("_db_state_init_lock", threading.Lock())
        with init_lock:
            if hasattr(self, "_db_metrics_lock"):
                return
            self._pg_pool_init_lock = getattr(self, "_pg_pool_init_lock", threading.Lock())
            self._pg_pool_min = max(1, int(os.getenv("TELEGRAM_PG_POOL_MIN", "1")))
            self._pg_pool_max = max(self._pg_pool_min, int(os.getenv("TELEGRAM_PG_POOL_MAX", "8")))
            self._pg_slots = threading.BoundedSemaphore(self._pg_pool_max)
            profile = getattr(self, "telegram_user_id", "unknown")
            self._pg_application_name = f"telegram-mcp/{profile}/{socket.gethostname()}"[:63]
            self._pg_configured_conn_ids: set[int] = set()
            self._pg_config_lock = threading.Lock()
            self._db_metrics_lock = threading.Lock()
            self._db_metrics = {
                "pool_in_use": 0,
                "peak_in_use": 0,
                "pool_checkouts_total": 0,
                "waits_total": 0,
                "acquire_timeouts_total": 0,
                "discarded_connections_total": 0,
                "db_errors_total": 0,
                "sql_statements_total": 0,
                "rows_written_total": 0,
                "monitoring_requests_total": 0,
                "monitoring_cache_hits_total": 0,
                "monitoring_refreshes_total": 0,
            }
            self._db_operation_context = threading.local()
            self._db_operation_totals: dict[str, dict[str, float | int]] = {}
            self._db_work_events: deque[tuple[float, str, int, int, float]] = deque()
            self._ingest_metrics = {
                "observed_total": 0,
                "accepted_total": 0,
                "inserted_total": 0,
                "duplicate_total": 0,
                "accepted_unknown_total": 0,
                "skipped_total": 0,
                "failed_total": 0,
            }
            self._ingest_events: deque[tuple[float, dict[str, int]]] = deque()
            self._recent_accepted_keys: deque[tuple[str, int]] = deque(
                maxlen=max(10, int(os.getenv("TELEGRAM_RECONCILE_KEY_BUFFER", "500")))
            )
            self._reconciliation_snapshot: dict[str, Any] = {
                "available": False,
                "reason": "no reconciliation pass has completed in this process",
            }
            self._monitoring_refresh_lock = threading.Lock()
            self._monitoring_cache: dict[str, Any] | None = None
            self._monitoring_cache_at = 0.0
            self._monitoring_failure_cache: dict[str, Any] | None = None
            self._monitoring_failure_at = 0.0

    def _record_db_work(
        self,
        operation: str,
        *,
        statements: int = 0,
        rows: int = 0,
        duration_s: float = 0.0,
    ) -> None:
        self._ensure_db_load_state()
        operation = (operation or "other").strip()[:64] or "other"
        now = time.monotonic()
        with self._db_metrics_lock:
            self._db_metrics["sql_statements_total"] += max(0, statements)
            self._db_metrics["rows_written_total"] += max(0, rows)
            totals = self._db_operation_totals.setdefault(
                operation,
                {"sql_total": 0, "rows_total": 0, "duration_s_total": 0.0},
            )
            totals["sql_total"] = int(totals["sql_total"]) + max(0, statements)
            totals["rows_total"] = int(totals["rows_total"]) + max(0, rows)
            totals["duration_s_total"] = float(totals["duration_s_total"]) + max(
                0.0, duration_s
            )
            if statements or rows or duration_s:
                self._db_work_events.append(
                    (now, operation, max(0, statements), max(0, rows), max(0.0, duration_s))
                )
            cutoff = now - 300.0
            while self._db_work_events and self._db_work_events[0][0] < cutoff:
                self._db_work_events.popleft()

    def _current_db_operation(self) -> str:
        self._ensure_db_load_state()
        return str(getattr(self._db_operation_context, "operation", "other") or "other")

    def _record_ingest_outcome(self, **counts: int) -> None:
        """Record process-local message outcomes without storing message content."""
        self._ensure_db_load_state()
        allowed = {key: max(0, int(value or 0)) for key, value in counts.items() if f"{key}_total" in self._ingest_metrics}
        if not any(allowed.values()):
            return
        now = time.monotonic()
        with self._db_metrics_lock:
            for key, value in allowed.items():
                self._ingest_metrics[f"{key}_total"] += value
            self._ingest_events.append((now, allowed))
            cutoff = now - 300.0
            while self._ingest_events and self._ingest_events[0][0] < cutoff:
                self._ingest_events.popleft()

    def _record_accepted_keys(self, rows: list[dict[str, Any]]) -> None:
        """Keep a bounded, content-free receipt set for the next exact audit."""
        self._ensure_db_load_state()
        keys = [
            (str(row["chat_id"]), int(row["message_id"]))
            for row in rows
            if row.get("chat_id") is not None and row.get("message_id") is not None
        ]
        if not keys:
            return
        with self._db_metrics_lock:
            self._recent_accepted_keys.extend(keys)

    def get_recent_accepted_keys(self) -> list[tuple[str, int]]:
        """Return bounded exact keys accepted by this process, never message content."""
        self._ensure_db_load_state()
        with self._db_metrics_lock:
            return list(dict.fromkeys(self._recent_accepted_keys))

    def set_reconciliation_snapshot(self, snapshot: dict[str, Any]) -> None:
        """Publish the latest exact-key audit to health consumers."""
        self._ensure_db_load_state()
        with self._db_metrics_lock:
            self._reconciliation_snapshot = dict(snapshot)

    def get_ingest_accounting_snapshot(self) -> dict[str, Any]:
        self._ensure_db_load_state()
        now = time.monotonic()
        with self._db_metrics_lock:
            cutoff = now - 300.0
            while self._ingest_events and self._ingest_events[0][0] < cutoff:
                self._ingest_events.popleft()
            result = dict(self._ingest_metrics)
            for window_name, seconds in (("1m", 60.0), ("5m", 300.0)):
                events = [counts for at, counts in self._ingest_events if at >= now - seconds]
                for key in ("observed", "accepted", "inserted", "duplicate", "accepted_unknown", "skipped", "failed"):
                    result[f"{key}_{window_name}"] = sum(event.get(key, 0) for event in events)
            result["reconciliation"] = dict(self._reconciliation_snapshot)
            return result

    def get_db_load_snapshot(self) -> dict[str, Any]:
        """Return process-local admission and actual SQL counters for monitoring."""
        self._ensure_db_load_state()
        now = time.monotonic()
        with self._db_metrics_lock:
            cutoff = now - 300.0
            while self._db_work_events and self._db_work_events[0][0] < cutoff:
                self._db_work_events.popleft()
            one_minute = [event for event in self._db_work_events if event[0] >= now - 60.0]
            five_minutes = list(self._db_work_events)
            snapshot = dict(self._db_metrics)
            operations: dict[str, dict[str, int | float]] = {}
            for operation, totals in sorted(self._db_operation_totals.items()):
                one = [event for event in one_minute if event[1] == operation]
                five = [event for event in five_minutes if event[1] == operation]
                operations[operation] = {
                    **totals,
                    "sql_1m": sum(event[2] for event in one),
                    "sql_5m": sum(event[2] for event in five),
                    "rows_1m": sum(event[3] for event in one),
                    "rows_5m": sum(event[3] for event in five),
                }
            snapshot.update(
                pool_min=self._pg_pool_min,
                pool_max=self._pg_pool_max,
                sql_statements_1m=sum(event[2] for event in one_minute),
                sql_statements_5m=sum(event[2] for event in five_minutes),
                rows_written_1m=sum(event[3] for event in one_minute),
                rows_written_5m=sum(event[3] for event in five_minutes),
                operations=operations,
            )
            return snapshot

    def _get_pool(self) -> Any:
        """R3 D5 fix (pr-hero-1u1): lazily create a BOUNDED connection pool. Before,
        every _pg_conn() opened a fresh psycopg2.connect — 200 chats × N batches during
        backfill blew past Postgres max_connections → 'too many connections' → cursor
        stuck → lag grows geometrically. A ThreadedConnectionPool caps concurrency and
        reuses connections. Bounds via TELEGRAM_PG_POOL_MIN/MAX (default 1..8)."""
        self._ensure_db_load_state()
        if self._pg_pool is None:
            with self._pg_pool_init_lock:
                if self._pg_pool is None:
                    from psycopg2.extensions import cursor as PsycopgCursor
                    from psycopg2.pool import ThreadedConnectionPool

                    writer = self

                    class MetricsCursor(PsycopgCursor):
                        def execute(self, query: Any, vars: Any = None) -> Any:
                            try:
                                return super().execute(query, vars)
                            finally:
                                writer._record_db_work(
                                    writer._current_db_operation(), statements=1
                                )

                        def executemany(self, query: Any, vars_list: Any) -> Any:
                            values = list(vars_list)
                            try:
                                return super().executemany(query, values)
                            finally:
                                writer._record_db_work(
                                    writer._current_db_operation(), statements=len(values)
                                )

                    self._pg_pool = ThreadedConnectionPool(
                        self._pg_pool_min,
                        self._pg_pool_max,
                        self._postgres_url,
                        connect_timeout=10,
                        application_name=self._pg_application_name,
                        cursor_factory=MetricsCursor,
                    )
        return self._pg_pool

    def _configure_pg_connection(self, conn: Any) -> None:
        """Apply attribution/timeouts after Supavisor hands out the session.

        Supavisor may replace startup ``application_name`` and ignore libpq
        ``options``.  Session-level set_config is therefore the readbackable
        contract.  It runs once per physical client connection.
        """
        conn_id = id(conn)
        if conn_id in self._pg_configured_conn_ids:
            return
        with self._pg_config_lock:
            if conn_id in self._pg_configured_conn_ids:
                return
            statement_timeout_ms = max(
                1000, int(os.getenv("TELEGRAM_PG_STATEMENT_TIMEOUT_MS", "15000"))
            )
            idle_timeout_ms = max(
                1000, int(os.getenv("TELEGRAM_PG_IDLE_TX_TIMEOUT_MS", "5000"))
            )
            cur = conn.cursor()
            try:
                cur.execute(
                    "SELECT set_config('application_name', %s, false)",
                    (self._pg_application_name,),
                )
                cur.execute(
                    "SELECT set_config('statement_timeout', %s, false)",
                    (str(statement_timeout_ms),),
                )
                cur.execute(
                    "SELECT set_config('idle_in_transaction_session_timeout', %s, false)",
                    (str(idle_timeout_ms),),
                )
                conn.commit()
            finally:
                cur.close()
            self._pg_configured_conn_ids.add(conn_id)

    @contextmanager
    def _pg_conn(
        self,
        operation: str = "other",
        timeout_seconds: float | None = None,
    ) -> Iterator[Any]:
        """Yield a pooled psycopg2 connection (R3 D5). Caller must not use when
        _postgres_url is None. Connection is rolled back and returned to the pool on
        exit so an aborted transaction never poisons the next borrower."""
        self._ensure_db_load_state()
        pool = self._get_pool()
        if timeout_seconds is None:
            timeout_seconds = float(os.getenv("TELEGRAM_PG_ACQUIRE_TIMEOUT_SECONDS", "30"))
        wait_started = time.monotonic()
        acquired = self._pg_slots.acquire(timeout=max(0.0, timeout_seconds))
        waited_s = time.monotonic() - wait_started
        if waited_s >= 0.001:
            with self._db_metrics_lock:
                self._db_metrics["waits_total"] += 1
        if not acquired:
            with self._db_metrics_lock:
                self._db_metrics["acquire_timeouts_total"] += 1
            raise TimeoutError(
                f"Postgres pool admission timed out after {timeout_seconds:g}s "
                f"for {operation} (max={self._pg_pool_max})"
            )
        conn = None
        previous_operation = self._current_db_operation()
        try:
            for attempt in range(2):
                candidate = pool.getconn()
                try:
                    if bool(getattr(candidate, "closed", False)):
                        raise ConnectionError("pooled Postgres connection is already closed")
                    self._configure_pg_connection(candidate)
                    conn = candidate
                    break
                except Exception:
                    pool.putconn(candidate, close=True)
                    with self._db_metrics_lock:
                        self._db_metrics["discarded_connections_total"] += 1
                    with self._pg_config_lock:
                        self._pg_configured_conn_ids.discard(id(candidate))
                    if attempt == 1:
                        raise
            if conn is None:  # defensive: loop above either assigns or raises
                raise ConnectionError("no healthy Postgres connection available")
            self._db_operation_context.operation = operation
            with self._db_metrics_lock:
                self._db_metrics["pool_in_use"] += 1
                self._db_metrics["pool_checkouts_total"] += 1
                self._db_metrics["peak_in_use"] = max(
                    self._db_metrics["peak_in_use"], self._db_metrics["pool_in_use"]
                )
            yield conn
        except Exception:
            with self._db_metrics_lock:
                self._db_metrics["db_errors_total"] += 1
            raise
        finally:
            self._db_operation_context.operation = previous_operation
            try:
                if conn is not None:
                    discard = bool(getattr(conn, "closed", False))
                    try:
                        conn.rollback()   # clear any aborted txn before reuse
                    except Exception:  # noqa: BLE001
                        discard = True
                    try:
                        pool.putconn(conn, close=discard)
                    finally:
                        with self._db_metrics_lock:
                            self._db_metrics["pool_in_use"] = max(
                                0, self._db_metrics["pool_in_use"] - 1
                            )
                        if discard:
                            self._db_metrics["discarded_connections_total"] += 1
                            with self._pg_config_lock:
                                self._pg_configured_conn_ids.discard(id(conn))
            finally:
                self._pg_slots.release()

    def _get_runtime_activity(self) -> tuple[datetime | None, datetime | None]:
        """Return latest listener heartbeat timestamp and latest message timestamp."""
        if self._postgres_url:
            with self._pg_conn(operation="runtime_activity") as conn:
                return self._get_runtime_activity_pg(conn)
        return self._get_runtime_activity_rest()

    def _get_runtime_activity_pg(self, conn: Any) -> tuple[datetime | None, datetime | None]:
        cur = conn.cursor()
        try:
            cur.execute(
                f"""
                SELECT started_at
                FROM {self.schema}.telegram_ingest_runs
                WHERE telegram_user_id=%s
                  AND mode = ANY(%s)
                ORDER BY started_at DESC
                LIMIT 1
                """,
                (self.telegram_user_id, list(LISTENER_RUNTIME_MODES)),
            )
            runtime_row = cur.fetchone()
            listener_event_at = _coerce_datetime(runtime_row[0]) if runtime_row else None

            cur.execute(
                f"""
                SELECT created_at
                FROM {self.schema}.telegram_messages_raw
                WHERE telegram_user_id=%s
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (self.telegram_user_id,),
            )
            message_row = cur.fetchone()
            latest_message_at = _coerce_datetime(message_row[0]) if message_row else None
            return listener_event_at, latest_message_at
        finally:
            cur.close()

    def _get_runtime_activity_rest(self) -> tuple[datetime | None, datetime | None]:
        listener_response = (
            self._table(TABLE_RUNS)
            .select("started_at")
            .eq("telegram_user_id", self.telegram_user_id)
            .in_("mode", list(LISTENER_RUNTIME_MODES))
            .order("started_at", desc=True)
            .limit(1)
            .execute()
        )
        message_response = (
            self._table(TABLE_MESSAGES)
            .select("created_at")
            .eq("telegram_user_id", self.telegram_user_id)
            .order("created_at", desc=True)
            .limit(1)
            .execute()
        )
        listener_event_at = None
        latest_message_at = None
        if listener_response.data:
            listener_event_at = _coerce_datetime(listener_response.data[0].get("started_at"))
        if message_response.data:
            latest_message_at = _coerce_datetime(message_response.data[0].get("created_at"))
        return listener_event_at, latest_message_at

    # ------------------------------------------------------------------
    # Message writing
    # ------------------------------------------------------------------

    def _classify_message_guard(
        self,
        message: Any,
        chat_id: int | str,
        chat_title: str | None = None,
    ) -> tuple[Any, Any, Any, str | None, str]:
        """Run the canonical index guardian once for write and reconciliation.

        Reconciliation must apply the same allow/skip decision as the writer.
        Otherwise a deliberately excluded OTP/SMS relay is reported as a lost
        Supabase row even though the policy says that row must never be stored.
        """
        text_value = getattr(message, "text", "") or ""
        resolved_title = chat_title if isinstance(chat_title, str) and chat_title else None
        if resolved_title is None:
            message_chat = getattr(message, "chat", None)
            if message_chat is not None:
                for field in ("title", "first_name", "username"):
                    candidate = getattr(message_chat, field, None)
                    if isinstance(candidate, str) and candidate:
                        resolved_title = candidate
                        break
        guard, rules = _guard_rules()
        decision = guard.classify_message(chat_id, resolved_title, text_value, rules)
        return guard, rules, decision, resolved_title, text_value

    def classify_message_for_ingest(
        self,
        message: Any,
        chat_id: int | str,
        chat_title: str | None = None,
    ) -> dict[str, Any]:
        """Return a content-free guardian verdict for exact-key reconciliation."""
        try:
            _guard, _rules, decision, _title, _text = self._classify_message_guard(
                message,
                chat_id,
                chat_title,
            )
            return {
                "eligible": decision.action != "skip",
                "reason": "eligible" if decision.action != "skip" else "policy_skip",
                "classification_error": False,
            }
        except Exception as exc:  # noqa: BLE001 - mirror fail-closed write behavior
            logger.warning(
                "index guardian classification error on chat %s: %s — fail-closed skip",
                chat_id,
                exc,
            )
            return {
                "eligible": False,
                "reason": f"guard_error:{type(exc).__name__}",
                "classification_error": True,
            }

    def _telethon_message_to_row(
        self,
        message: Any,
        chat_id: int | str,
        chat_type: str = "unknown",
        chat_title: str | None = None,
    ) -> dict[str, Any] | None:
        """Convert a Telethon Message object to a Supabase row dict.

        Maps Telethon message fields to telegram_messages_raw schema.
        Stores full message as JSONB in ``raw`` field.

        Index guardian (pr-hero-gcy): returns ``None`` if the chat is blacklisted
        (pure code/SMS relay) so the caller skips the write; otherwise masks
        sensitive values (card / OTP / password / passport / SNILS) in ``text``.
        """
        sender_id = None
        sender_name = ""
        sender_username = ""

        if hasattr(message, "sender") and message.sender:
            sender = message.sender
            sender_id = str(getattr(sender, "id", ""))
            sender_name = getattr(sender, "first_name", "") or ""
            last_name = getattr(sender, "last_name", "") or ""
            if last_name:
                sender_name = f"{sender_name} {last_name}".strip()
            if not sender_name:
                sender_name = getattr(sender, "title", "") or ""
            sender_username = getattr(sender, "username", "") or ""
        elif hasattr(message, "sender_id") and message.sender_id:
            sender_id = str(message.sender_id)

        # Build raw JSONB from message.to_dict() if available
        raw_data: dict[str, Any] = {}
        if hasattr(message, "to_dict"):
            try:
                raw_data = message.to_dict()
            except Exception:
                raw_data = {"text": getattr(message, "text", ""), "id": message.id}
        else:
            raw_data = {"text": getattr(message, "text", ""), "id": message.id}

        # Ensure raw_data is JSON-serializable
        raw_data = _make_json_safe(raw_data)

        msg_date = getattr(message, "date", None)
        message_ts = msg_date.isoformat() if msg_date else None

        # Index guardian: skip blacklisted chats, mask sensitive values. The
        # helper is also used by reconciliation so policy skips and data loss
        # cannot be conflated in monitoring.
        try:
            guard, rules, decision, chat_title, text = self._classify_message_guard(
                message,
                chat_id,
                chat_title,
            )
            if decision.action == "skip":
                logger.info("guardian skip chat %s: %s", chat_id, decision.reason)
                return None
            if decision.categories:
                text = decision.text
            # security-3+I3 fix (edited-msg leak, security-reviewer squad 2026-07-03):
            # ALWAYS redact the raw JSONB, not only when `text` had a hit. An edited
            # message / media caption / poll can carry the secret in raw fields
            # (raw.message, fwd_from, entities, reply_markup) while `text` is empty or
            # None → decision.categories was empty → raw stayed unredacted → the OTP
            # leaked via `SELECT raw->>'message'`. Scanning raw independently every
            # time (chat not skipped) closes that path regardless of the text hit.
            raw_data = _redact_raw_recursive(raw_data, guard, rules)
        except Exception as guard_exc:
            # security-2 fix: fail CLOSED — on ANY guard error skip the message
            # entirely (return None). The previous "re-check blacklist" path called
            # the same failing _guard_rules() again → on a broken/missing YAML it
            # threw twice and the row leaked (fail-OPEN). Dropping the message is
            # safe; a lost non-secret message is cheaper than a leaked OTP.
            logger.warning("index guardian error on chat %s: %s — fail-closed skip", chat_id, guard_exc)
            return None

        return {
            "source": "telegram",
            "telegram_user_id": self.telegram_user_id,
            "chat_id": str(chat_id),
            "chat_type": chat_type,
            "message_id": message.id,
            "sender_user_id": sender_id,
            "sender_name": sender_name,
            "sender_username": sender_username,
            "message_ts": message_ts,
            "text": text,
            "raw": raw_data,
        }

    # ------------------------------------------------------------------
    # Article / Instant View index (telegram_articles)
    # ------------------------------------------------------------------

    def _article_rows(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Строки telegram_articles для webpage-постов из готовых message-rows.

        Работает по row["raw"] ПОСЛЕ redaction (см. _telethon_message_to_row) —
        текст статьи наследует маскирование секретов. Kill-switch:
        TELEGRAM_ARTICLE_INDEX=0.
        """
        if os.getenv("TELEGRAM_ARTICLE_INDEX", "1") != "1":
            return []
        try:
            from heroes_platform.heroes_telegram_mcp.article_enrichment import (
                article_row_from_message_row,
            )
        except ImportError:
            from article_enrichment import article_row_from_message_row  # type: ignore
        out: list[dict[str, Any]] = []
        for row in rows:
            try:
                article = article_row_from_message_row(row)
            except Exception as exc:  # noqa: BLE001 — индекс статей не должен ломать ingest
                logger.warning(
                    "article extract failed for %s/%s: %s",
                    row.get("chat_id"), row.get("message_id"), exc,
                )
                continue
            if article is not None:
                out.append(article)
        return out

    def _upsert_articles_pg(self, conn: Any, articles: list[dict[str, Any]]) -> int:
        """Upsert строк статей via direct Postgres. Fail-soft: warning, не исключение."""
        if not articles:
            return 0
        q = f"""
        INSERT INTO {self.schema}.telegram_articles
        (chat_id, message_id, telegram_user_id, message_ts, url, title,
         description, article_text, has_page, fetched_at, updated_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s, CASE WHEN %s THEN now() END, now())
        ON CONFLICT (chat_id, message_id) DO UPDATE SET
          url=EXCLUDED.url, title=EXCLUDED.title, description=EXCLUDED.description,
          message_ts=EXCLUDED.message_ts,
          -- не затирать ранее добытое тело пустым (edited-message без re-fetch)
          article_text=CASE WHEN EXCLUDED.article_text <> ''
                            THEN EXCLUDED.article_text
                            ELSE {self.schema}.telegram_articles.article_text END,
          has_page={self.schema}.telegram_articles.has_page OR EXCLUDED.has_page,
          fetched_at=COALESCE(EXCLUDED.fetched_at, {self.schema}.telegram_articles.fetched_at),
          updated_at=now()
        """
        cur = conn.cursor()
        try:
            params = [
                (
                    a["chat_id"], a["message_id"], a.get("telegram_user_id"),
                    a.get("message_ts"), a.get("url"), a.get("title"),
                    a.get("description"), a.get("article_text"),
                    a.get("has_page", False), a.get("has_page", False),
                )
                for a in articles
            ]
            try:
                from psycopg2.extras import execute_batch  # type: ignore

                execute_batch(cur, q, params, page_size=100)
            except ImportError:
                for row_params in params:
                    cur.execute(q, row_params)
            conn.commit()
            return len(articles)
        except Exception as exc:  # noqa: BLE001
            conn.rollback()
            # Migration 20260722000001 применяется per-schema; на профиле без неё
            # (tg_<slug>) глушим индекс на весь процесс вместо warning-шторма.
            if "does not exist" in str(exc) and "telegram_articles" in str(exc):
                self._articles_disabled = True
                logger.warning(
                    "%s.telegram_articles missing — article index disabled for "
                    "this process; apply migration 20260722000001 to this schema",
                    self.schema,
                )
            else:
                logger.warning("telegram_articles upsert failed (%d rows): %s", len(articles), exc)
            return 0
        finally:
            cur.close()

    def _index_articles(self, conn: Any | None, rows: list[dict[str, Any]]) -> None:
        """Best-effort индексация статей после успешной записи сообщений.

        Только PG-путь: REST .upsert() делает replace всей строки и терял бы
        merge-семантику (has_page OR, article_text CASE) — edited-message без
        cached_page затирал бы добытое тело (review MAJOR-1). Без _postgres_url
        индекс статей отключён (один warning на процесс).
        """
        if getattr(self, "_articles_disabled", False):
            return
        if conn is None:
            if not getattr(self, "_articles_rest_warned", False):
                self._articles_rest_warned = True
                logger.warning(
                    "telegram_articles index requires direct Postgres "
                    "(SUPABASE_DB_URL); REST mode — article index disabled"
                )
            return
        try:
            articles = self._article_rows(rows)
            if not articles:
                return
            self._upsert_articles_pg(conn, articles)
        except Exception as exc:  # noqa: BLE001 — индекс статей не должен ломать ingest
            logger.warning("telegram_articles index failed: %s", exc)

    def _write_message_pg(self, conn: Any, row: dict[str, Any]) -> bool:
        """Single message upsert via direct Postgres."""
        q = f"""
        INSERT INTO {self.schema}.telegram_messages_raw
        (source, telegram_user_id, chat_id, chat_type, message_id,
         sender_user_id, sender_name, sender_username, message_ts, text, raw)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
        ON CONFLICT (chat_id, message_id) DO UPDATE SET
          sender_user_id=EXCLUDED.sender_user_id, sender_name=EXCLUDED.sender_name,
          sender_username=EXCLUDED.sender_username, message_ts=EXCLUDED.message_ts,
          text=EXCLUDED.text, raw=EXCLUDED.raw
        RETURNING (xmax = 0) AS inserted
        """
        cur = conn.cursor()
        try:
            cur.execute(
                q,
                (
                    row["source"],
                    row["telegram_user_id"],
                    row["chat_id"],
                    row["chat_type"],
                    row["message_id"],
                    row.get("sender_user_id"),
                    row.get("sender_name"),
                    row.get("sender_username"),
                    row.get("message_ts"),
                    row.get("text"),
                    json.dumps(row["raw"]),
                ),
            )
            outcome = cur.fetchone() if hasattr(cur, "fetchone") else None
            conn.commit()
            self._record_db_work("live_message", rows=1)
            self._record_accepted_keys([row])
            if outcome is None:
                self._record_ingest_outcome(accepted=1, accepted_unknown=1)
            elif bool(outcome[0]):
                self._record_ingest_outcome(accepted=1, inserted=1)
            else:
                self._record_ingest_outcome(accepted=1, duplicate=1)
            return True
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()

    def _write_messages_batch_pg(self, conn: Any, rows: list[dict[str, Any]]) -> int:
        """Set-based batch upsert via one statement per page, not one per row."""
        q = f"""
        INSERT INTO {self.schema}.telegram_messages_raw
        (source, telegram_user_id, chat_id, chat_type, message_id,
         sender_user_id, sender_name, sender_username, message_ts, text, raw)
        VALUES %s
        ON CONFLICT (chat_id, message_id) DO UPDATE SET
          sender_user_id=EXCLUDED.sender_user_id, sender_name=EXCLUDED.sender_name,
          sender_username=EXCLUDED.sender_username, message_ts=EXCLUDED.message_ts,
          text=EXCLUDED.text, raw=EXCLUDED.raw
        RETURNING (xmax = 0) AS inserted
        """
        cur = conn.cursor()
        try:
            from psycopg2.extras import execute_values  # type: ignore

            values = [
                (
                    row["source"],
                    row["telegram_user_id"],
                    row["chat_id"],
                    row["chat_type"],
                    row["message_id"],
                    row.get("sender_user_id"),
                    row.get("sender_name"),
                    row.get("sender_username"),
                    row.get("message_ts"),
                    row.get("text"),
                    json.dumps(row["raw"]),
                )
                for row in rows
            ]
            outcomes = execute_values(
                cur,
                q,
                values,
                template="(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)",
                page_size=100,
                fetch=True,
            )
            conn.commit()
            self._record_db_work("message_batch", rows=len(rows))
            self._record_accepted_keys(rows)
            if outcomes is None:
                self._record_ingest_outcome(
                    accepted=len(rows), accepted_unknown=len(rows)
                )
            else:
                inserted = sum(1 for outcome in outcomes if bool(outcome[0]))
                self._record_ingest_outcome(
                    accepted=len(rows),
                    inserted=inserted,
                    duplicate=max(0, len(rows) - inserted),
                )
            return len(rows)
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()

    async def write_message(
        self,
        message: Any,
        chat_id: int | str,
        chat_type: str = "unknown",
        chat_title: str | None = None,
        *,
        _count_ingest: bool = True,
    ) -> bool:
        """Write a single Telethon message to Supabase.

        Uses upsert with ON CONFLICT to handle dedup.

        Returns:
            True if write succeeded, False otherwise.
        """
        if _count_ingest:
            self._record_ingest_outcome(observed=1)
        try:
            row = self._telethon_message_to_row(message, chat_id, chat_type, chat_title)
            if row is None:  # guardian skipped a blacklisted chat — handled, not written
                if _count_ingest:
                    self._record_ingest_outcome(skipped=1)
                return True
            if self._postgres_url:
                def _write() -> bool:
                    with self._pg_conn(operation="live_message") as conn:
                        ok = self._write_message_pg(conn, row)
                        if ok:
                            self._index_articles(conn, [row])
                        return ok

                result = await asyncio.to_thread(_write)
                return result
            self._table(TABLE_MESSAGES).upsert(
                row,
                on_conflict="chat_id,message_id",
            ).execute()
            self._record_accepted_keys([row])
            self._index_articles(None, [row])
            if _count_ingest:
                self._record_ingest_outcome(accepted=1, accepted_unknown=1)
            return True
        except Exception as exc:
            if _count_ingest:
                self._record_ingest_outcome(failed=1)
            logger.warning(
                "Failed to write message %s in chat %s: %s",
                getattr(message, "id", "?"),
                chat_id,
                exc,
            )
            return False

    async def write_messages_batch(
        self,
        messages: list[Any],
        chat_id: int | str,
        chat_type: str = "unknown",
        chat_title: str | None = None,
    ) -> int:
        """Write a batch of Telethon messages to Supabase.

        Returns:
            Number of successfully written messages.
        """
        if not messages:
            return 0

        self._record_ingest_outcome(observed=len(messages))

        rows = [
            r
            for m in messages
            if (r := self._telethon_message_to_row(m, chat_id, chat_type, chat_title)) is not None
        ]
        if not rows:  # all messages skipped by guardian (blacklisted chat)
            self._record_ingest_outcome(skipped=len(messages))
            return 0
        skipped = max(0, len(messages) - len(rows))
        if skipped:
            self._record_ingest_outcome(skipped=skipped)

        try:
            if self._postgres_url:
                def _write_batch() -> int:
                    with self._pg_conn(operation="message_batch") as conn:
                        written = self._write_messages_batch_pg(conn, rows)
                        if written:
                            self._index_articles(conn, rows)
                        return written

                return await asyncio.to_thread(_write_batch)
            self._table(TABLE_MESSAGES).upsert(
                rows,
                on_conflict="chat_id,message_id",
            ).execute()
            self._record_accepted_keys(rows)
            self._index_articles(None, rows)
            self._record_ingest_outcome(accepted=len(rows), accepted_unknown=len(rows))
            return len(rows)
        except Exception as exc:
            logger.warning(
                "Batch write failed for chat %s (%d msgs): %s",
                chat_id,
                len(rows),
                exc,
            )
            if self._postgres_url:
                self._record_ingest_outcome(failed=len(rows))
                return 0
            # Fall back to individual writes (REST only)
            ok = 0
            for msg in messages:
                if await self.write_message(
                    msg, chat_id, chat_type, chat_title, _count_ingest=False
                ):
                    ok += 1
            self._record_ingest_outcome(
                accepted=ok,
                accepted_unknown=ok,
                failed=max(0, len(rows) - ok),
            )
            return ok

    # ------------------------------------------------------------------
    # Chat registry management
    # ------------------------------------------------------------------

    def _upsert_chat_pg(
        self,
        conn: Any,
        chat_id: str,
        chat_type: str,
        chat_title: str,
        chat_username: str | None,
    ) -> bool:
        cur = conn.cursor()
        try:
            cur.execute(
                f"""
                INSERT INTO {self.schema}.telegram_chats
                (chat_id, chat_type, chat_title, chat_username, is_active)
                VALUES (%s,%s,%s,%s,TRUE)
                ON CONFLICT (chat_id) DO UPDATE SET
                  chat_type=EXCLUDED.chat_type, chat_title=EXCLUDED.chat_title,
                  chat_username=EXCLUDED.chat_username, is_active=EXCLUDED.is_active
                """,
                (chat_id, chat_type, chat_title, chat_username),
            )
            conn.commit()
            return True
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()

    async def upsert_chat(
        self,
        chat_id: int | str,
        chat_type: str = "unknown",
        chat_title: str = "",
        chat_username: str | None = None,
    ) -> bool:
        """Register or update a chat in telegram_chats global registry."""
        try:
            cid = str(chat_id)
            if self._postgres_url:
                with self._pg_conn(operation="chat_upsert") as conn:
                    return self._upsert_chat_pg(conn, cid, chat_type, chat_title, chat_username)
            row: dict[str, Any] = {
                "chat_id": cid,
                "chat_type": chat_type,
                "chat_title": chat_title,
                "is_active": True,
            }
            if chat_username:
                row["chat_username"] = chat_username
            self._table(TABLE_CHATS).upsert(row, on_conflict="chat_id").execute()
            return True
        except Exception as exc:
            logger.warning("Failed to upsert chat %s: %s", chat_id, exc)
            return False

    def _ensure_chat_state_pg(self, conn: Any, chat_id: str) -> None:
        cur = conn.cursor()
        try:
            cur.execute(
                f"""
                INSERT INTO {self.schema}.telegram_chat_state
                (telegram_user_id, chat_id, is_active)
                VALUES (%s,%s,TRUE)
                ON CONFLICT (telegram_user_id, chat_id) DO NOTHING
                """,
                (self.telegram_user_id, chat_id),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()

    def _update_chat_cursor_pg(
        self,
        conn: Any,
        chat_id: str,
        last_seen_message_id: int | None,
        last_backfill_message_id: int | None,
        backfill_completed: bool | None,
    ) -> bool:
        now = datetime.now(tz=timezone.utc)
        cur = conn.cursor()
        try:
            self._ensure_chat_state_pg(conn, chat_id)
            if last_seen_message_id is not None:
                # GREATEST → курсор монотонен: race между live NewMessage handler и
                # backfill не может откатить курсор назад (blind overwrite давал
                # регрессию качества данных). last_seen_ts двигаем только когда id вырос.
                cur.execute(
                    f"""
                    UPDATE {self.schema}.telegram_chat_state
                    SET last_seen_message_id=GREATEST(COALESCE(last_seen_message_id, 0), %s),
                        last_seen_ts=CASE
                            WHEN %s > COALESCE(last_seen_message_id, 0) THEN %s
                            ELSE last_seen_ts
                        END
                    WHERE telegram_user_id=%s AND chat_id=%s
                    """,
                    (
                        last_seen_message_id,
                        last_seen_message_id,
                        now,
                        self.telegram_user_id,
                        chat_id,
                    ),
                )
            if last_backfill_message_id is not None:
                # LEAST → backward-floor монотонно идёт ВНИЗ (зеркало GREATEST
                # для last_seen). Blind overwrite давал race: параллельный проход
                # с устаревшим (более высоким) min_id мог поднять floor вверх
                # → следующий проход начал бы с более высокого id, пропустив
                # уже обработанный участок. COALESCE покрывает первый проход
                # когда floor ещё NULL — берём явный new (LEAST(NULL, X)=NULL
                # в PG, поэтому COALESCE-вокруг столбца обязателен).
                # last_backfill_ts двигаем только когда floor реально опустился.
                cur.execute(
                    f"""
                    UPDATE {self.schema}.telegram_chat_state
                    SET last_backfill_message_id=LEAST(
                            COALESCE(last_backfill_message_id, %s),
                            %s
                        ),
                        last_backfill_ts=CASE
                            WHEN %s < COALESCE(last_backfill_message_id, %s)
                                OR last_backfill_message_id IS NULL THEN %s
                            ELSE last_backfill_ts
                        END
                    WHERE telegram_user_id=%s AND chat_id=%s
                    """,
                    (
                        last_backfill_message_id,
                        last_backfill_message_id,
                        last_backfill_message_id,
                        last_backfill_message_id,
                        now,
                        self.telegram_user_id,
                        chat_id,
                    ),
                )
            if backfill_completed is not None:
                cur.execute(
                    f"""
                    UPDATE {self.schema}.telegram_chat_state
                    SET backfill_completed=%s
                    WHERE telegram_user_id=%s AND chat_id=%s
                    """,
                    (backfill_completed, self.telegram_user_id, chat_id),
                )
            conn.commit()
            return True
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()

    async def update_chat_cursor(
        self,
        chat_id: int | str,
        last_seen_message_id: int | None = None,
        last_backfill_message_id: int | None = None,
        backfill_completed: bool | None = None,
    ) -> bool:
        """Update cursor fields for a user-scoped chat state row."""
        try:
            if (
                last_seen_message_id is None
                and last_backfill_message_id is None
                and backfill_completed is None
            ):
                return True
            cid = str(chat_id)
            if self._postgres_url:
                def _update() -> bool:
                    with self._pg_conn(operation="cursor_update") as conn:
                        return self._update_chat_cursor_pg(
                            conn,
                            cid,
                            last_seen_message_id,
                            last_backfill_message_id,
                            backfill_completed,
                        )

                return await asyncio.to_thread(_update)
            update: dict[str, Any] = {
                "telegram_user_id": self.telegram_user_id,
                "chat_id": cid,
            }
            now = datetime.now(tz=timezone.utc).isoformat()
            if last_seen_message_id is not None:
                update["last_seen_message_id"] = last_seen_message_id
                update["last_seen_ts"] = now
            if last_backfill_message_id is not None:
                update["last_backfill_message_id"] = last_backfill_message_id
                update["last_backfill_ts"] = now
            if backfill_completed is not None:
                update["backfill_completed"] = backfill_completed
            self._table(TABLE_CHAT_STATE).upsert(
                update,
                on_conflict="telegram_user_id,chat_id",
            ).execute()
            return True
        except Exception as exc:
            logger.warning("Failed to update cursor for chat %s: %s", chat_id, exc)
            return False

    async def touch_backfill_attempt(self, chat_id: int | str) -> bool:
        """Rotate a failed chat without claiming cursor progress or completion.

        ``last_backfill_ts`` is the scheduler's last-attempt timestamp.  Moving
        only that field keeps an inaccessible/left channel visible as a failed
        runtime event while preventing it from monopolising every oldest-first
        selection window.
        """
        cid = str(chat_id)
        now = datetime.now(tz=timezone.utc)
        try:
            if self._postgres_url:
                with self._pg_conn(operation="backfill_attempt") as conn:
                    self._ensure_chat_state_pg(conn, cid)
                    cur = conn.cursor()
                    try:
                        cur.execute(
                            f"""
                            UPDATE {self.schema}.telegram_chat_state
                            SET last_backfill_ts=%s
                            WHERE telegram_user_id=%s AND chat_id=%s
                            """,
                            (now, self.telegram_user_id, cid),
                        )
                        conn.commit()
                    finally:
                        cur.close()
                return True
            self._table(TABLE_CHAT_STATE).upsert(
                {
                    "telegram_user_id": self.telegram_user_id,
                    "chat_id": cid,
                    "last_backfill_ts": now.isoformat(),
                },
                on_conflict="telegram_user_id,chat_id",
            ).execute()
            return True
        except Exception as exc:
            logger.warning("Failed to touch backfill attempt for chat %s: %s", chat_id, exc)
            return False

    async def mark_chat_inactive(self, chat_id: int | str) -> bool:
        """Remove a proven inaccessible peer from the active history queue."""
        cid = str(chat_id)
        now = datetime.now(tz=timezone.utc)
        try:
            if self._postgres_url:
                with self._pg_conn(operation="chat_inactivate") as conn:
                    self._ensure_chat_state_pg(conn, cid)
                    cur = conn.cursor()
                    try:
                        cur.execute(
                            f"""
                            UPDATE {self.schema}.telegram_chat_state
                            SET is_active=FALSE, last_backfill_ts=%s
                            WHERE telegram_user_id=%s AND chat_id=%s
                            """,
                            (now, self.telegram_user_id, cid),
                        )
                        conn.commit()
                    finally:
                        cur.close()
                return True
            self._table(TABLE_CHAT_STATE).upsert(
                {
                    "telegram_user_id": self.telegram_user_id,
                    "chat_id": cid,
                    "is_active": False,
                    "last_backfill_ts": now.isoformat(),
                },
                on_conflict="telegram_user_id,chat_id",
            ).execute()
            return True
        except Exception as exc:
            logger.warning("Failed to mark inaccessible chat %s inactive: %s", chat_id, exc)
            return False

    def _get_chat_cursor_pg(self, conn: Any, chat_id: str) -> dict[str, Any] | None:
        cur = conn.cursor()
        try:
            cur.execute(
                f"""
                SELECT *
                FROM {self.schema}.telegram_chat_state
                WHERE telegram_user_id=%s AND chat_id=%s
                LIMIT 1
                """,
                (self.telegram_user_id, chat_id),
            )
            row = cur.fetchone()
            if not row:
                return None
            cols = [d[0] for d in cur.description]
            return dict(zip(cols, row))
        finally:
            cur.close()

    def _get_chat_cursors_pg(self, conn: Any) -> dict[str, dict[str, Any]]:
        """Load every current user-scoped cursor in one statement.

        Periodic reconciliation must not issue one SELECT per Telegram dialog.
        Missing rows still fall back to the legacy per-chat lookup in the
        caller, preserving compatibility during an incomplete migration.
        """
        cur = conn.cursor()
        try:
            cur.execute(
                f"""
                SELECT *
                FROM {self.schema}.telegram_chat_state
                WHERE telegram_user_id=%s
                """,
                (self.telegram_user_id,),
            )
            cols = [d[0] for d in cur.description]
            return {
                str(row[cols.index("chat_id")]): dict(zip(cols, row))
                for row in cur.fetchall()
            }
        finally:
            cur.close()

    async def get_chat_cursors(self) -> dict[str, dict[str, Any]] | None:
        """Return a bulk cursor map when direct Postgres is available.

        REST pagination differs between deployments, so the safe REST fallback
        remains the existing per-chat method instead of silently truncating the
        map at the API row limit.
        """
        if not self._postgres_url:
            return None
        try:
            with self._pg_conn(operation="cursor_bulk_read") as conn:
                return self._get_chat_cursors_pg(conn)
        except Exception as exc:
            logger.warning("Failed to bulk-load chat cursors: %s", exc)
            return None

    def _audit_message_keys_sync(
        self,
        keys: list[tuple[str, int]],
        *,
        operation: str = "reconcile_heads",
    ) -> dict[str, Any]:
        """Compare Telegram-observed exact keys with bronze in one SQL statement."""
        normalized = list(dict.fromkeys((str(chat_id), int(message_id)) for chat_id, message_id in keys))
        observed_at = datetime.now(tz=timezone.utc).isoformat()
        if not normalized:
            return {
                "available": True,
                "observed_at": observed_at,
                "checked": 0,
                "present": 0,
                "missing": 0,
                "missing_keys_omitted": 0,
            }
        if not self._postgres_url:
            return {
                "available": False,
                "observed_at": observed_at,
                "reason": "exact composite-key audit requires direct Postgres",
                "checked": len(normalized),
            }
        try:
            from psycopg2.extras import execute_values  # type: ignore

            with self._pg_conn(operation=operation, timeout_seconds=5.0) as conn:
                cur = conn.cursor()
                try:
                    expected = [
                        (self.telegram_user_id, chat_id, message_id)
                        for chat_id, message_id in normalized
                    ]
                    rows = execute_values(
                        cur,
                        f"""
                        WITH expected(telegram_user_id, chat_id, message_id) AS (VALUES %s)
                        SELECT expected.chat_id, expected.message_id
                        FROM expected
                        JOIN {self.schema}.telegram_messages_raw AS raw
                          ON raw.telegram_user_id=expected.telegram_user_id
                         AND raw.chat_id=expected.chat_id
                         AND raw.message_id=expected.message_id
                        """,
                        expected,
                        template="(%s::text,%s::text,%s::bigint)",
                        fetch=True,
                        page_size=max(1, min(1000, len(normalized))),
                    )
                finally:
                    cur.close()
            present_keys = {(str(row[0]), int(row[1])) for row in (rows or [])}
            missing_keys = [key for key in normalized if key not in present_keys]
            return {
                "available": True,
                "observed_at": observed_at,
                "checked": len(normalized),
                "present": len(present_keys),
                "missing": len(missing_keys),
                "missing_keys_omitted": len(missing_keys),
            }
        except Exception as exc:  # noqa: BLE001
            return {
                "available": False,
                "observed_at": observed_at,
                "reason": f"{type(exc).__name__}: {exc}"[:240],
                "checked": len(normalized),
            }

    async def audit_message_keys(
        self,
        keys: list[tuple[str, int]],
        *,
        operation: str = "reconcile_heads",
    ) -> dict[str, Any]:
        """Non-blocking owner-facing exact-key audit for a bounded Telegram window."""
        return await asyncio.to_thread(
            self._audit_message_keys_sync,
            keys,
            operation=operation,
        )

    def _refresh_recent_accepted_audit_sync(self) -> None:
        """Refresh bounded write receipts once per monitoring TTL, not per request."""
        audit = self._audit_message_keys_sync(
            self.get_recent_accepted_keys(),
            operation="reconcile_accepted",
        )
        self._ensure_db_load_state()
        with self._db_metrics_lock:
            snapshot = dict(self._reconciliation_snapshot)
            snapshot.update(
                accepted_checked=int(audit.get("checked") or 0),
                accepted_present=int(audit.get("present") or 0),
                accepted_missing=int(audit.get("missing") or 0),
                accepted_audit_available=bool(audit.get("available")),
                audit_key_scope="dialog_heads_and_recent_accepted",
            )
            self._reconciliation_snapshot = snapshot

    async def get_chat_cursor(self, chat_id: int | str) -> dict[str, Any] | None:
        """Get current user-scoped cursor state for a chat.

        Falls back to legacy telegram_chats cursors until the additive migration
        has been rolled through all environments.
        """
        try:
            cid = str(chat_id)
            if self._postgres_url:
                with self._pg_conn(operation="cursor_single_read") as conn:
                    state = self._get_chat_cursor_pg(conn, cid)
                    if state:
                        return state
                    return self._get_legacy_chat_cursor_pg(conn, cid)
            response = (
                self._table(TABLE_CHAT_STATE)
                .select("*")
                .eq("telegram_user_id", self.telegram_user_id)
                .eq("chat_id", cid)
                .limit(1)
                .execute()
            )
            if response.data:
                return response.data[0]
            legacy_response = (
                self._table(TABLE_CHATS).select("*").eq("chat_id", cid).limit(1).execute()
            )
            if legacy_response.data:
                return legacy_response.data[0]
            return None
        except Exception as exc:
            logger.warning("Failed to get cursor for chat %s: %s", chat_id, exc)
            return None

    def _get_legacy_chat_cursor_pg(self, conn: Any, chat_id: str) -> dict[str, Any] | None:
        cur = conn.cursor()
        try:
            cur.execute(
                f"SELECT * FROM {self.schema}.telegram_chats WHERE chat_id=%s LIMIT 1",
                (chat_id,),
            )
            row = cur.fetchone()
            if not row:
                return None
            cols = [d[0] for d in cur.description]
            return dict(zip(cols, row))
        finally:
            cur.close()

    def _lookup_chats_by_query_pg(
        self, conn: Any, pattern: str, limit: int
    ) -> list[dict[str, Any]]:
        cur = conn.cursor()
        try:
            cur.execute(
                f"""
                SELECT chat_id, chat_title, chat_username, chat_type
                FROM {self.schema}.telegram_chats
                WHERE chat_title ILIKE %s OR chat_username ILIKE %s
                LIMIT %s
                """,
                (pattern, pattern, limit),
            )
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
        finally:
            cur.close()

    def lookup_chats_by_query(self, query: str, limit: int = 10) -> list[dict[str, Any]]:
        """Lookup chat_id by title or username (avoid get_direct_chat_by_contact).

        Use after sync_telegram_chats_to_supabase has populated telegram_chats.
        """
        if not query or not query.strip():
            return []
        q = query.strip()
        pattern = f"%{q}%"
        try:
            if self._postgres_url:
                with self._pg_conn(operation="chat_lookup") as conn:
                    return self._lookup_chats_by_query_pg(conn, pattern, limit)
            seen: set[str] = set()
            out: list[dict[str, Any]] = []
            for col in ("chat_title", "chat_username"):
                try:
                    response = (
                        self._table(TABLE_CHATS)
                        .select("chat_id, chat_title, chat_username, chat_type")
                        .ilike(col, pattern)
                        .limit(limit)
                        .execute()
                    )
                    for row in response.data or []:
                        cid = row.get("chat_id")
                        if cid and cid not in seen:
                            seen.add(cid)
                            out.append(row)
                            if len(out) >= limit:
                                return out
                except Exception:
                    continue
            return out
        except Exception as exc:
            logger.warning("Lookup chats by query %r failed: %s", query, exc)
            return []

    # ------------------------------------------------------------------
    # Ingest run logging
    # ------------------------------------------------------------------

    def _start_ingest_run_pg(self, conn: Any, run_id: str, mode: str) -> None:
        """Записать старт прогона; переживает схему без telegram_user_id.

        RCA 2026-07-26: миграция 20260605000001 §A не была применена к
        rick_messages_tasks (таблица принадлежит supabase_admin, у писателя нет
        прав ALTER) → КАЖДЫЙ boot/heartbeat падал «column telegram_user_id does
        not exist», ingest_runs пустая с апреля, мониторинг считал живой
        листенер мёртвым. Пишем без скоуп-колонки, когда её нет: наблюдаемость
        важнее полноты поля, а `bd`-скоуп восстановим после ALTER.
        """
        now = datetime.now(tz=timezone.utc).isoformat()
        scoped = (
            f"INSERT INTO {self.schema}.telegram_ingest_runs "
            "(run_id, telegram_user_id, mode, started_at, status) "
            "VALUES (%s,%s,%s,%s,'running')"
        )
        legacy = (
            f"INSERT INTO {self.schema}.telegram_ingest_runs "
            "(run_id, mode, started_at, status) VALUES (%s,%s,%s,'running')"
        )
        cur = conn.cursor()
        try:
            if getattr(self, "_ingest_runs_unscoped", False):
                cur.execute(legacy, (run_id, mode, now))
            else:
                try:
                    cur.execute(scoped, (run_id, self.telegram_user_id, mode, now))
                except Exception as exc:  # noqa: BLE001
                    if "telegram_user_id" not in str(exc) or "does not exist" not in str(exc):
                        raise
                    conn.rollback()
                    self._ingest_runs_unscoped = True
                    logger.warning(
                        "%s.telegram_ingest_runs has no telegram_user_id column — "
                        "writing run markers unscoped; apply migration "
                        "20260605000001 §A to restore per-profile scope",
                        self.schema,
                    )
                    cur.execute(legacy, (run_id, mode, now))
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()

    async def start_ingest_run(self, mode: str = "poll_updates") -> str:
        """Start a new ingest run and return its run_id."""
        run_id = str(uuid.uuid4())
        if self._postgres_url:
            with self._pg_conn(operation="runtime_event_start") as conn:
                self._start_ingest_run_pg(conn, run_id, mode)
        else:
            self._table(TABLE_RUNS).insert(
                {
                    "run_id": run_id,
                    "telegram_user_id": self.telegram_user_id,
                    "mode": mode,
                    "started_at": datetime.now(tz=timezone.utc).isoformat(),
                    "status": "running",
                }
            ).execute()
        return run_id

    def _finish_ingest_run_pg(
        self,
        conn: Any,
        run_id: str,
        processed_chats: int,
        inserted_messages: int,
        error: str | None,
    ) -> None:
        status = "failed" if error else "success"
        finished_at = datetime.now(tz=timezone.utc).isoformat()
        last_error = (error[:500]) if error else None
        cur = conn.cursor()
        try:
            cur.execute(
                f"""
                UPDATE {self.schema}.telegram_ingest_runs
                SET finished_at=%s, processed_chats=%s, inserted_messages=%s,
                    last_error=%s, status=%s
                WHERE run_id=%s
                """,
                (finished_at, processed_chats, inserted_messages, last_error, status, run_id),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()

    async def finish_ingest_run(
        self,
        run_id: str,
        processed_chats: int = 0,
        inserted_messages: int = 0,
        error: str | None = None,
    ) -> None:
        """Mark an ingest run as finished."""
        if self._postgres_url:
            with self._pg_conn(operation="runtime_event_finish") as conn:
                self._finish_ingest_run_pg(conn, run_id, processed_chats, inserted_messages, error)
            return
        status = "failed" if error else "success"
        update: dict[str, Any] = {
            "finished_at": datetime.now(tz=timezone.utc).isoformat(),
            "processed_chats": processed_chats,
            "inserted_messages": inserted_messages,
            "status": status,
        }
        if error:
            update["last_error"] = error[:500]
        self._table(TABLE_RUNS).update(update).eq("run_id", run_id).execute()

    async def record_runtime_event(
        self,
        mode: str,
        processed_chats: int = 0,
        inserted_messages: int = 0,
        error: str | None = None,
    ) -> None:
        """Write a short runtime marker for health and freshness probes."""
        run_id = await self.start_ingest_run(mode=mode)
        await self.finish_ingest_run(
            run_id,
            processed_chats=processed_chats,
            inserted_messages=inserted_messages,
            error=error,
        )

    # ------------------------------------------------------------------
    # Backfill support
    # ------------------------------------------------------------------

    async def backfill_chat(
        self,
        telethon_client: Any,
        chat_id: int | str,
        chat_type: str = "unknown",
        limit: int = 5000,
    ) -> int:
        """Backfill history for a single chat.

        Reads messages from Telegram via Telethon and writes to Supabase.
        Uses cursor from telegram_chats to resume from where we left off.

        Returns:
            Number of messages written.
        """
        cursor = await self.get_chat_cursor(chat_id)
        max_id = None  # No limit for initial run
        if cursor and cursor.get("last_backfill_message_id"):
            # Resume: get messages OLDER than last_backfill (lower ids)
            max_id = cursor["last_backfill_message_id"]

        # security-1: resolve title once so backfill honours title/username skip
        chat_title = await _resolve_chat_title(telethon_client, chat_id)

        total_written = 0
        seen_count = 0
        partial_write = False
        batch: list[Any] = []
        min_id_seen = float("inf")

        try:
            iter_kwargs: dict[str, Any] = {"entity": int(chat_id), "limit": limit}
            if max_id is not None:
                iter_kwargs["max_id"] = max_id

            async for msg in telethon_client.iter_messages(**iter_kwargs):
                seen_count += 1
                batch.append(msg)
                if msg.id < min_id_seen:
                    min_id_seen = msg.id

                if len(batch) >= self.batch_size:
                    written = await self.write_messages_batch(
                        batch,
                        chat_id,
                        chat_type,
                        chat_title,
                    )
                    total_written += written
                    if written < len(batch):
                        partial_write = True
                        logger.warning(
                            "Backfill partial write for chat %s: wrote %d/%d; cursor not advanced",
                            chat_id,
                            written,
                            len(batch),
                        )
                        batch = []
                        break
                    batch = []

            # Write remaining
            if batch and not partial_write:
                written = await self.write_messages_batch(batch, chat_id, chat_type, chat_title)
                total_written += written
                if written < len(batch):
                    partial_write = True
                    logger.warning(
                        "Backfill partial write for chat %s: wrote %d/%d; cursor not advanced",
                        chat_id,
                        written,
                        len(batch),
                    )

            # Update cursor only when every seen message in this pass was handled.
            # If a batch reports partial success, we do not know which message ids
            # failed, so advancing the cursor would make unwritten rows invisible
            # to the next run (Supabase #62 false-green class).
            if not partial_write and total_written > 0 and min_id_seen < float("inf"):
                await self.update_chat_cursor(
                    chat_id,
                    last_backfill_message_id=int(min_id_seen),
                )
            elif not partial_write and seen_count == 0:
                await self.update_chat_cursor(chat_id, backfill_completed=True)

        except Exception as exc:
            logger.warning("Backfill error for chat %s: %s", chat_id, exc)
            raise

        return total_written

    async def catch_up_recent(
        self,
        telethon_client: Any,
        chat_id: int | str,
        chat_type: str = "unknown",
        limit: int = 1000,
        *,
        cursor: dict[str, Any] | None = None,
    ) -> int:
        """Backfill only messages newer than the last seen cursor."""
        if cursor is None:
            cursor = await self.get_chat_cursor(chat_id)
        if not cursor or not cursor.get("last_seen_message_id"):
            return 0

        last_seen_message_id = int(cursor["last_seen_message_id"])
        # security-1: resolve title once so catch-up honours title/username skip
        chat_title = await _resolve_chat_title(telethon_client, chat_id)
        total_written = 0
        partial_write = False
        batch: list[Any] = []
        max_id_seen = last_seen_message_id

        try:
            async for msg in telethon_client.iter_messages(
                entity=int(chat_id),
                min_id=last_seen_message_id,
                reverse=True,
                limit=limit,
            ):
                batch.append(msg)
                if msg.id > max_id_seen:
                    max_id_seen = msg.id

                if len(batch) >= self.batch_size:
                    written = await self.write_messages_batch(
                        batch,
                        chat_id,
                        chat_type,
                        chat_title,
                    )
                    total_written += written
                    if written < len(batch):
                        partial_write = True
                        logger.warning(
                            "Recent catch-up partial write for chat %s: wrote %d/%d; cursor not advanced",
                            chat_id,
                            written,
                            len(batch),
                        )
                        batch = []
                        break
                    batch = []

            if batch and not partial_write:
                written = await self.write_messages_batch(
                    batch,
                    chat_id,
                    chat_type,
                    chat_title,
                )
                total_written += written
                if written < len(batch):
                    partial_write = True
                    logger.warning(
                        "Recent catch-up partial write for chat %s: wrote %d/%d; cursor not advanced",
                        chat_id,
                        written,
                        len(batch),
                    )

            if not partial_write and max_id_seen > last_seen_message_id:
                await self.update_chat_cursor(
                    chat_id,
                    last_seen_message_id=max_id_seen,
                )
        except Exception as exc:
            logger.warning("Recent catch-up error for chat %s: %s", chat_id, exc)
            raise

        return total_written


# ---------------------------------------------------------------------------
# JSON safety helper
# ---------------------------------------------------------------------------


def _make_json_safe(obj: Any) -> Any:
    """Recursively make an object JSON-serializable.

    Converts bytes to hex strings, datetimes to ISO strings, etc.
    """
    if isinstance(obj, dict):
        return {k: _make_json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_make_json_safe(item) for item in obj]
    if isinstance(obj, bytes):
        return obj.hex()
    if isinstance(obj, datetime):
        return obj.isoformat()
    if hasattr(obj, "__dict__") and not isinstance(obj, (str, int, float, bool)):
        # Convert custom objects to dict
        try:
            return {
                k: _make_json_safe(v) for k, v in obj.__dict__.items() if not k.startswith("_")
            }
        except Exception:
            return str(obj)
    return obj


def _coerce_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value
    if isinstance(value, str):
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    return None


def _evaluate_runtime_health(
    *,
    listener_event_at: datetime | None,
    latest_message_at: datetime | None,
    max_staleness_seconds: int,
    transport_message: str,
    max_message_staleness_seconds: int | None = None,
) -> tuple[bool, str]:
    if listener_event_at is None:
        return (
            False,
            f"Telegram LABA runtime unhealthy: no listener heartbeat found; {transport_message}",
        )

    now = datetime.now(tz=timezone.utc)
    listener_age = now - listener_event_at
    if listener_age > timedelta(seconds=max_staleness_seconds):
        return (
            False,
            "Telegram LABA runtime unhealthy: listener heartbeat stale "
            f"({int(listener_age.total_seconds())}s old); {transport_message}",
        )

    details = [
        transport_message,
        f"listener heartbeat age={int(listener_age.total_seconds())}s",
    ]
    if latest_message_at is not None:
        latest_message_age = now - latest_message_at
        details.append(f"latest message age={int(latest_message_age.total_seconds())}s")
        # R3 D4 fix (pr-hero-1u1): heartbeat freshness ≠ ingest freshness. A live
        # listener that writes a heartbeat every 60s while NO message lands across
        # ALL chats for hours = silently stalled ingest (the "lisa 9 days" incident).
        # A separate, conservative message-staleness threshold catches it so the
        # unit's own healthcheck goes unhealthy — not just the external doctor.
        if max_message_staleness_seconds is None:
            max_message_staleness_seconds = int(
                os.getenv("TELEGRAM_MESSAGE_MAX_STALENESS_SECONDS", "21600")  # 6h
            )
        if max_message_staleness_seconds > 0 and latest_message_age > timedelta(
            seconds=max_message_staleness_seconds
        ):
            return (
                False,
                "Telegram LABA runtime unhealthy: INGEST STALLED — heartbeat live but "
                f"latest message is {int(latest_message_age.total_seconds())}s old "
                f"(> {max_message_staleness_seconds}s); {transport_message}",
            )
    else:
        details.append("latest message age=none yet")

    return True, "Telegram LABA runtime OK: " + "; ".join(details)
