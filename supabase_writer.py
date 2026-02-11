#!/usr/bin/env python3
"""Supabase Writer for Telegram MCP Server.

JTBD: Когда Telegram MCP сервер получает новые сообщения через event handlers,
я хочу записывать их в Supabase таблицу telegram_messages_raw,
чтобы все сообщения из всех чатов сохранялись в централизованном хранилище.

Architecture:
    - Uses Supabase REST API via supabase-py client
    - Writes to telegram_messages_raw (bronze layer) with full raw JSONB
    - Manages telegram_chats registry with cursors for backfill/updates
    - Logs ingest runs to telegram_ingest_runs
    - Dedup via unique index on (chat_id, message_id)

Credentials: Mac Keychain via credentials_manager (supabase_rick_api_key)
Migration: 20250110000001_telegram_tdlib_tables.sql (must be applied first)
"""
from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

SUPABASE_URL = os.getenv("SUPABASE_URL", "https://supabase.rick.ai")
TABLE_MESSAGES = "telegram_messages_raw"
TABLE_CHATS = "telegram_chats"
TABLE_RUNS = "telegram_ingest_runs"


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
            from heroes_platform.shared.credentials_manager import (
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
    """

    def __init__(self, telegram_user_id: str = "ikrasinsky") -> None:
        self._client: Any | None = None
        self.telegram_user_id = telegram_user_id
        self._batch: list[dict[str, Any]] = []
        self.batch_size = 50

    @property
    def client(self) -> Any:
        """Lazy-init Supabase client."""
        if self._client is None:
            self._client = _get_supabase_client()
        return self._client

    # ------------------------------------------------------------------
    # Message writing
    # ------------------------------------------------------------------

    def _telethon_message_to_row(
        self,
        message: Any,
        chat_id: int | str,
        chat_type: str = "unknown",
    ) -> dict[str, Any]:
        """Convert a Telethon Message object to a Supabase row dict.

        Maps Telethon message fields to telegram_messages_raw schema.
        Stores full message as JSONB in ``raw`` field.
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
            "text": getattr(message, "text", "") or "",
            "raw": raw_data,
        }

    async def write_message(
        self,
        message: Any,
        chat_id: int | str,
        chat_type: str = "unknown",
    ) -> bool:
        """Write a single Telethon message to Supabase.

        Uses upsert with ON CONFLICT to handle dedup.

        Returns:
            True if write succeeded, False otherwise.
        """
        try:
            row = self._telethon_message_to_row(message, chat_id, chat_type)
            self.client.table(TABLE_MESSAGES).upsert(
                row, on_conflict="chat_id,message_id",
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
    ) -> int:
        """Write a batch of Telethon messages to Supabase.

        Returns:
            Number of successfully written messages.
        """
        if not messages:
            return 0

        rows = [
            self._telethon_message_to_row(m, chat_id, chat_type) for m in messages
        ]

        try:
            self.client.table(TABLE_MESSAGES).upsert(
                rows, on_conflict="chat_id,message_id",
            ).execute()
            return len(rows)
        except Exception as exc:
            logger.warning(
                "Batch write failed for chat %s (%d msgs): %s",
                chat_id,
                len(rows),
                exc,
            )
            # Fall back to individual writes
            ok = 0
            for msg in messages:
                if await self.write_message(msg, chat_id, chat_type):
                    ok += 1
            return ok

    # ------------------------------------------------------------------
    # Chat registry management
    # ------------------------------------------------------------------

    async def upsert_chat(
        self,
        chat_id: int | str,
        chat_type: str = "unknown",
        chat_title: str = "",
        chat_username: str | None = None,
    ) -> bool:
        """Register or update a chat in telegram_chats."""
        try:
            row: dict[str, Any] = {
                "chat_id": str(chat_id),
                "chat_type": chat_type,
                "chat_title": chat_title,
                "is_active": True,
            }
            if chat_username:
                row["chat_username"] = chat_username

            self.client.table(TABLE_CHATS).upsert(
                row, on_conflict="chat_id",
            ).execute()
            return True
        except Exception as exc:
            logger.warning("Failed to upsert chat %s: %s", chat_id, exc)
            return False

    async def update_chat_cursor(
        self,
        chat_id: int | str,
        last_seen_message_id: int | None = None,
        last_backfill_message_id: int | None = None,
        backfill_completed: bool | None = None,
    ) -> bool:
        """Update cursor fields for a chat (for backfill/updates tracking)."""
        try:
            update: dict[str, Any] = {}
            now = datetime.now(tz=timezone.utc).isoformat()

            if last_seen_message_id is not None:
                update["last_seen_message_id"] = last_seen_message_id
                update["last_seen_ts"] = now

            if last_backfill_message_id is not None:
                update["last_backfill_message_id"] = last_backfill_message_id
                update["last_backfill_ts"] = now

            if backfill_completed is not None:
                update["backfill_completed"] = backfill_completed

            if not update:
                return True

            self.client.table(TABLE_CHATS).update(update).eq(
                "chat_id", str(chat_id),
            ).execute()
            return True
        except Exception as exc:
            logger.warning("Failed to update cursor for chat %s: %s", chat_id, exc)
            return False

    async def get_chat_cursor(self, chat_id: int | str) -> dict[str, Any] | None:
        """Get current cursor state for a chat."""
        try:
            response = (
                self.client.table(TABLE_CHATS)
                .select("*")
                .eq("chat_id", str(chat_id))
                .limit(1)
                .execute()
            )
            if response.data:
                return response.data[0]
            return None
        except Exception as exc:
            logger.warning("Failed to get cursor for chat %s: %s", chat_id, exc)
            return None

    # ------------------------------------------------------------------
    # Ingest run logging
    # ------------------------------------------------------------------

    async def start_ingest_run(self, mode: str = "poll_updates") -> str:
        """Start a new ingest run and return its run_id."""
        run_id = str(uuid.uuid4())
        try:
            self.client.table(TABLE_RUNS).insert({
                "run_id": run_id,
                "mode": mode,
                "started_at": datetime.now(tz=timezone.utc).isoformat(),
                "status": "running",
            }).execute()
        except Exception as exc:
            logger.warning("Failed to start ingest run: %s", exc)
        return run_id

    async def finish_ingest_run(
        self,
        run_id: str,
        processed_chats: int = 0,
        inserted_messages: int = 0,
        error: str | None = None,
    ) -> None:
        """Mark an ingest run as finished."""
        try:
            status = "failed" if error else "success"
            update: dict[str, Any] = {
                "finished_at": datetime.now(tz=timezone.utc).isoformat(),
                "processed_chats": processed_chats,
                "inserted_messages": inserted_messages,
                "status": status,
            }
            if error:
                update["last_error"] = error[:500]

            self.client.table(TABLE_RUNS).update(update).eq(
                "run_id", run_id,
            ).execute()
        except Exception as exc:
            logger.warning("Failed to finish ingest run %s: %s", run_id, exc)

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

        total_written = 0
        batch: list[Any] = []
        min_id_seen = float("inf")

        try:
            iter_kwargs: dict[str, Any] = {"entity": int(chat_id), "limit": limit}
            if max_id is not None:
                iter_kwargs["max_id"] = max_id

            async for msg in telethon_client.iter_messages(**iter_kwargs):
                batch.append(msg)
                if msg.id < min_id_seen:
                    min_id_seen = msg.id

                if len(batch) >= self.batch_size:
                    written = await self.write_messages_batch(
                        batch, chat_id, chat_type,
                    )
                    total_written += written
                    batch = []

            # Write remaining
            if batch:
                written = await self.write_messages_batch(batch, chat_id, chat_type)
                total_written += written

            # Update cursor
            if total_written > 0 and min_id_seen < float("inf"):
                await self.update_chat_cursor(
                    chat_id,
                    last_backfill_message_id=int(min_id_seen),
                    backfill_completed=(total_written < limit),
                )

        except Exception as exc:
            logger.warning("Backfill error for chat %s: %s", chat_id, exc)

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
            return {k: _make_json_safe(v) for k, v in obj.__dict__.items()
                    if not k.startswith("_")}
        except Exception:
            return str(obj)
    return obj
