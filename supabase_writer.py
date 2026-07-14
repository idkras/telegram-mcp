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

Credentials: registry-only API via heroes_platform.credentials (supabase_rick_api_key)
Migration: 20250110000001_telegram_tdlib_tables.sql (must be applied first)
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator

logger = logging.getLogger(__name__)

SUPABASE_URL = os.getenv("SUPABASE_URL", "https://supabase.rick.ai")

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
TABLE_CHATS = "telegram_chats"
TABLE_CHAT_STATE = "telegram_chat_state"
TABLE_RUNS = "telegram_ingest_runs"
LISTENER_RUNTIME_MODES = ("listener_boot", "listener_heartbeat")


def _get_postgres_url() -> str | None:
    """Get Supabase Postgres connection URL (same as apply_telegram_migration / laba/n8n).

    When set, SupabaseWriter uses direct Postgres instead of REST — avoids PGRST106
    for schema rick_messages_tasks (Exposed schemas not required).
    """
    url = os.getenv("SUPABASE_DB_URL")
    if url:
        return url
    try:
        from heroes_platform.credentials import credentials_manager

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

    # Try environment variable first (for laba deployment)
    api_key = os.getenv("SUPABASE_API_KEY")

    if not api_key:
        # Fall back to Mac Keychain (for local development)
        try:
            from heroes_platform.credentials import (
                credentials_manager,
            )

            result = credentials_manager.get_credential("supabase_rick_api_key")
            if result.success and result.value:
                api_key = result.value
        except ImportError:
            pass

    if not api_key:
        raise RuntimeError(
            "Supabase API key not found. Set SUPABASE_API_KEY env var "
            "or store 'supabase_rick_api_key' in Mac Keychain."
        )

    url = os.getenv("SUPABASE_URL", SUPABASE_URL)
    return create_client(url, api_key)


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
        self.telegram_user_id = telegram_user_id
        # Schema-per-profile: каждый аккаунт пишет в свою схему (data не смешивается).
        self.schema = _schema_for_profile(telegram_user_id)
        self._batch: list[dict[str, Any]] = []
        self.batch_size = 50
        self._postgres_url = postgres_url or _get_postgres_url()
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
                with self._pg_conn() as conn:
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

    def _get_pool(self) -> Any:
        """R3 D5 fix (pr-hero-1u1): lazily create a BOUNDED connection pool. Before,
        every _pg_conn() opened a fresh psycopg2.connect — 200 chats × N batches during
        backfill blew past Postgres max_connections → 'too many connections' → cursor
        stuck → lag grows geometrically. A ThreadedConnectionPool caps concurrency and
        reuses connections. Bounds via TELEGRAM_PG_POOL_MIN/MAX (default 1..8)."""
        if self._pg_pool is None:
            from psycopg2.pool import ThreadedConnectionPool

            minc = int(os.getenv("TELEGRAM_PG_POOL_MIN", "1"))
            maxc = int(os.getenv("TELEGRAM_PG_POOL_MAX", "8"))
            self._pg_pool = ThreadedConnectionPool(minc, maxc, self._postgres_url)
        return self._pg_pool

    @contextmanager
    def _pg_conn(self) -> Iterator[Any]:
        """Yield a pooled psycopg2 connection (R3 D5). Caller must not use when
        _postgres_url is None. Connection is rolled back and returned to the pool on
        exit so an aborted transaction never poisons the next borrower."""
        pool = self._get_pool()
        conn = pool.getconn()
        try:
            yield conn
        finally:
            try:
                conn.rollback()   # clear any aborted txn before reuse
            except Exception:  # noqa: BLE001
                pass
            pool.putconn(conn)

    def _get_runtime_activity(self) -> tuple[datetime | None, datetime | None]:
        """Return latest listener heartbeat timestamp and latest message timestamp."""
        if self._postgres_url:
            with self._pg_conn() as conn:
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

        text = getattr(message, "text", "") or ""

        # Index guardian: skip blacklisted chats, mask sensitive values.
        # D-core-1 fix: pass real chat_title so title_skip_regex fires (e.g. a
        # "sms Inbox" chat not in id_tails). Derive from message.chat when caller
        # didn't thread it. security-4 fix (pr-hero-x0p iter-2): User/Bot entities
        # have no .title (only .first_name/.username) → fall back so an OTP-bot
        # named "verify bot" is caught by title/username skip patterns.
        if chat_title is None:
            _mchat = getattr(message, "chat", None)
            if _mchat is not None:
                chat_title = (
                    getattr(_mchat, "title", None)
                    or getattr(_mchat, "first_name", None)
                    or getattr(_mchat, "username", None)
                )
        try:
            guard, rules = _guard_rules()
            decision = guard.classify_message(chat_id, chat_title, text, rules)
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
            conn.commit()
            return True
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()

    def _write_messages_batch_pg(self, conn: Any, rows: list[dict[str, Any]]) -> int:
        """Batch upsert via direct Postgres."""
        q = f"""
        INSERT INTO {self.schema}.telegram_messages_raw
        (source, telegram_user_id, chat_id, chat_type, message_id,
         sender_user_id, sender_name, sender_username, message_ts, text, raw)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
        ON CONFLICT (chat_id, message_id) DO UPDATE SET
          sender_user_id=EXCLUDED.sender_user_id, sender_name=EXCLUDED.sender_name,
          sender_username=EXCLUDED.sender_username, message_ts=EXCLUDED.message_ts,
          text=EXCLUDED.text, raw=EXCLUDED.raw
        """
        cur = conn.cursor()
        try:
            for row in rows:
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
            conn.commit()
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
    ) -> bool:
        """Write a single Telethon message to Supabase.

        Uses upsert with ON CONFLICT to handle dedup.

        Returns:
            True if write succeeded, False otherwise.
        """
        try:
            row = self._telethon_message_to_row(message, chat_id, chat_type, chat_title)
            if row is None:  # guardian skipped a blacklisted chat — handled, not written
                return True
            if self._postgres_url:
                with self._pg_conn() as conn:
                    return self._write_message_pg(conn, row)
            self._table(TABLE_MESSAGES).upsert(
                row,
                on_conflict="chat_id,message_id",
            ).execute()
            return True
        except Exception as exc:
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

        rows = [
            r
            for m in messages
            if (r := self._telethon_message_to_row(m, chat_id, chat_type, chat_title)) is not None
        ]
        if not rows:  # all messages skipped by guardian (blacklisted chat)
            return 0

        try:
            if self._postgres_url:
                with self._pg_conn() as conn:
                    return self._write_messages_batch_pg(conn, rows)
            self._table(TABLE_MESSAGES).upsert(
                rows,
                on_conflict="chat_id,message_id",
            ).execute()
            return len(rows)
        except Exception as exc:
            logger.warning(
                "Batch write failed for chat %s (%d msgs): %s",
                chat_id,
                len(rows),
                exc,
            )
            if self._postgres_url:
                return 0
            # Fall back to individual writes (REST only)
            ok = 0
            for msg in messages:
                if await self.write_message(msg, chat_id, chat_type):
                    ok += 1
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
                with self._pg_conn() as conn:
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
                with self._pg_conn() as conn:
                    return self._update_chat_cursor_pg(
                        conn,
                        cid,
                        last_seen_message_id,
                        last_backfill_message_id,
                        backfill_completed,
                    )
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

    async def get_chat_cursor(self, chat_id: int | str) -> dict[str, Any] | None:
        """Get current user-scoped cursor state for a chat.

        Falls back to legacy telegram_chats cursors until the additive migration
        has been rolled through all environments.
        """
        try:
            cid = str(chat_id)
            if self._postgres_url:
                with self._pg_conn() as conn:
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
                with self._pg_conn() as conn:
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
        now = datetime.now(tz=timezone.utc).isoformat()
        cur = conn.cursor()
        try:
            cur.execute(
                f"""
                INSERT INTO {self.schema}.telegram_ingest_runs
                (run_id, telegram_user_id, mode, started_at, status)
                VALUES (%s,%s,%s,%s,'running')
                """,
                (run_id, self.telegram_user_id, mode, now),
            )
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
            with self._pg_conn() as conn:
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
            with self._pg_conn() as conn:
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

        return total_written

    async def catch_up_recent(
        self,
        telethon_client: Any,
        chat_id: int | str,
        chat_type: str = "unknown",
        limit: int = 1000,
    ) -> int:
        """Backfill only messages newer than the last seen cursor."""
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
