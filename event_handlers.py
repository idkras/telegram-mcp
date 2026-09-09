#!/usr/bin/env python3
"""Telethon Event Handlers for real-time message ingestion to Supabase.

JTBD: Когда Telegram MCP сервер запущен на laba,
я хочу автоматически получать все новые сообщения из всех чатов,
чтобы записывать их в Supabase в реальном времени без потери данных.

Architecture:
    - Registers Telethon event handlers for NewMessage, MessageEdited, MessageDeleted
    - Writes messages to Supabase via SupabaseWriter
    - Enabled only when LABA_MODE=true (on laba deployment)
    - Does NOT interfere with normal MCP server operation

Usage:
    # In main.py, after client.start():
    from heroes_platform.heroes_telegram_mcp.event_handlers import register_event_handlers
    if os.getenv("LABA_MODE") == "true":
        register_event_handlers(client)
"""

from __future__ import annotations

import logging
import os
import asyncio
from typing import Any

logger = logging.getLogger(__name__)
HEARTBEAT_INTERVAL_SECONDS = int(os.getenv("TELEGRAM_LISTENER_HEARTBEAT_SECONDS", "60"))

try:
    from heroes_platform.heroes_telegram_mcp.article_enrichment import (
        enrich_message_with_page,
    )
except ImportError:  # плоский запуск с VPS (PYTHONPATH на пакет)
    from article_enrichment import enrich_message_with_page  # type: ignore

# Only import Supabase writer when actually used
_writer: Any = None
_PROFILE_ALIASES = {"ik": "ikrasinsky", "ilyakrasinsky": "ikrasinsky"}
_SUPPORTED_ENDPOINT_PROFILES = frozenset({"ikrasinsky", "lisa"})


def env_flag_enabled(name: str) -> bool:
    """Accept the conventional truthy spellings used by env files and systemd."""
    return os.getenv(name, "").strip().lower() in {"1", "true", "yes", "on"}


def _get_writer() -> Any:
    """Lazy-init SupabaseWriter."""
    global _writer
    if _writer is None:
        from heroes_platform.heroes_telegram_mcp.supabase_writer import SupabaseWriter

        telegram_user = os.getenv("TELEGRAM_USER", "ikrasinsky")
        _writer = SupabaseWriter(telegram_user_id=telegram_user)
    return _writer


def active_endpoint_profile() -> str:
    """Return the stable account identity assigned to this MCP endpoint."""
    active_profile = os.getenv("TELEGRAM_USER", "ikrasinsky").strip().lower()
    return _PROFILE_ALIASES.get(active_profile, active_profile)


def canonical_profile(profile: str | None) -> str:
    """Resolve ``current``/legacy aliases to the endpoint's stable identity."""
    normalized = (profile or "current").strip().lower()
    active_profile = active_endpoint_profile()
    if normalized in {"", "current", "default"}:
        return active_profile
    return _PROFILE_ALIASES.get(normalized, normalized)


def resolve_profile_request(profile: str | None) -> tuple[str, str]:
    """Return ``(active, requested)`` identities or reject invented profiles."""
    active_profile = active_endpoint_profile()
    requested_profile = canonical_profile(profile)
    if requested_profile not in _SUPPORTED_ENDPOINT_PROFILES:
        raise ValueError(
            f"Unknown Telegram profile {requested_profile!r}. Use 'current', "
            "'ikrasinsky' (or 'ik'), or 'lisa'."
        )
    return active_profile, requested_profile


def require_endpoint_profile(profile: str | None) -> str:
    """Return the endpoint identity, rejecting all cross-account routing."""
    active_profile, requested_profile = resolve_profile_request(profile)
    if requested_profile != active_profile:
        raise ValueError(
            f"This endpoint is pinned to profile={active_profile}; "
            f"use the telegram-mcp-{requested_profile} endpoint instead."
        )
    return active_profile


# Backward-compatible private name used by existing tests and local callers.
_canonical_profile = canonical_profile


def get_writer_for_profile(profile: str | None = "current") -> Any:
    """Return the schema-scoped writer for the account used by ``send_message``."""
    require_endpoint_profile(profile)
    return _get_writer()


async def persist_message_and_cursor(
    message: Any,
    chat_id: int | str,
    chat_type: str,
    chat_title: str | None,
    *,
    writer: Any | None = None,
) -> str | None:
    """Persist one Telegram message before advancing its monotonic cursor.

    Returns ``None`` on full success, otherwise the failed stage.  In particular,
    a failed write must never advance ``last_seen_message_id`` past a missing row.
    """
    target_writer = writer or _get_writer()
    written = await target_writer.write_message(
        message,
        chat_id,
        chat_type,
        chat_title,
    )
    if not written:
        return "message_write_failed"
    cursor_updated = await target_writer.update_chat_cursor(
        chat_id,
        last_seen_message_id=message.id,
    )
    if not cursor_updated:
        return "cursor_update_failed"
    return None


def _get_chat_type(chat: Any) -> str:
    """Determine chat type from Telethon entity."""
    if chat is None:
        return "unknown"
    # Channel with broadcast=True is a channel, otherwise supergroup
    if hasattr(chat, "broadcast"):
        return "channel" if chat.broadcast else "supergroup"
    if hasattr(chat, "megagroup") and chat.megagroup:
        return "supergroup"
    # Basic group
    if hasattr(chat, "participants_count"):
        return "group"
    # User (private chat)
    if hasattr(chat, "first_name"):
        return "private"
    return "unknown"


async def _maybe_fetch_article(client: Any, message: Any) -> None:
    """Дозапросить тело article/Instant View поста перед записью (fail-soft).

    Telegram редко присылает cached_page вместе с сообщением (4.6% корпуса),
    поэтому для webpage-постов без тела делаем GetWebPageRequest и прикладываем
    Page к message — писатель сохранит его в raw + telegram_articles.
    Kill-switch: TELEGRAM_ARTICLE_FETCH=0.
    """
    if os.getenv("TELEGRAM_ARTICLE_FETCH", "1") != "1":
        return
    try:
        await enrich_message_with_page(client, message)
    except Exception as exc:  # noqa: BLE001 — обогащение не должно ломать ingest
        logger.info("article enrichment failed: %s", exc)


def register_event_handlers(client: Any) -> None:
    """Register Telethon event handlers for real-time message ingestion.

    Call this AFTER client.start() and ONLY in LABA_MODE.

    Args:
        client: Connected TelegramClient instance.
    """
    from telethon import events  # type: ignore

    logger.info("Registering Telethon event handlers for Supabase ingestion")
    loop = asyncio.get_running_loop()

    @client.on(events.NewMessage)
    async def on_new_message(event: Any) -> None:
        """Handle new messages in all chats -> write to Supabase."""
        try:
            message = event.message
            await _maybe_fetch_article(client, message)
            chat = await event.get_chat()
            chat_id = event.chat_id or getattr(chat, "id", 0)
            chat_type = _get_chat_type(chat)
            chat_title = (  # D-core-1 + security-4: bots/User have no .title
                getattr(chat, "title", None)
                or getattr(chat, "first_name", None)
                or getattr(chat, "username", None)
            )

            failed_stage = await persist_message_and_cursor(
                message,
                chat_id,
                chat_type,
                chat_title,
            )
            if failed_stage:
                logger.error(
                    "NewMessage persistence incomplete for chat %s message %s: %s",
                    chat_id,
                    message.id,
                    failed_stage,
                )
        except Exception as exc:
            logger.error("Error handling new message: %s", exc, exc_info=True)

    @client.on(events.MessageEdited)
    async def on_message_edited(event: Any) -> None:
        """Handle edited messages -> update in Supabase (upsert)."""
        try:
            message = event.message
            await _maybe_fetch_article(client, message)
            chat = await event.get_chat()
            chat_id = event.chat_id or getattr(chat, "id", 0)
            chat_type = _get_chat_type(chat)
            chat_title = (  # D-core-1 + security-4: bots/User have no .title
                getattr(chat, "title", None)
                or getattr(chat, "first_name", None)
                or getattr(chat, "username", None)
            )

            writer = _get_writer()
            await writer.write_message(message, chat_id, chat_type, chat_title)
        except Exception as exc:
            logger.error("Error handling edited message: %s", exc, exc_info=True)

    @client.on(events.MessageDeleted)
    async def on_message_deleted(event: Any) -> None:
        """Handle deleted messages -> mark as deleted in Supabase.

        Note: telegram_messages_raw doesn't have is_deleted field,
        but the raw JSONB can be updated to track deletion.
        For now we log it; a future migration can add soft-delete support.
        """
        try:
            deleted_ids = event.deleted_ids
            chat_id = event.chat_id
            logger.info(
                "Messages deleted in chat %s: %s",
                chat_id,
                deleted_ids,
            )
            # Future: mark messages as deleted in Supabase
        except Exception as exc:
            logger.error("Error handling deleted message: %s", exc, exc_info=True)

    logger.info("Event handlers registered: NewMessage, MessageEdited, MessageDeleted")
    print(
        "✅ Supabase event handlers registered (LABA_MODE)",
        file=__import__("sys").stderr,
    )

    async def _record_runtime_event(mode: str) -> None:
        try:
            writer = _get_writer()
            # psycopg2 is synchronous.  A saturated session-mode pool can spend
            # tens of seconds in connect(); keep that wait off the Telethon/MCP
            # event loop so HTTP binds and live Telegram updates remain responsive.
            def _write_marker() -> None:
                asyncio.run(
                    writer.record_runtime_event(
                        mode=mode,
                        processed_chats=0,
                        inserted_messages=0,
                    )
                )

            await asyncio.to_thread(_write_marker)
        except Exception as exc:
            logger.warning("Failed to write %s ingest marker: %s", mode, exc)

    async def _heartbeat_loop() -> None:
        while True:
            try:
                await asyncio.sleep(HEARTBEAT_INTERVAL_SECONDS)
                await _record_runtime_event("listener_heartbeat")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("Listener heartbeat loop failed: %s", exc)

    loop.create_task(_record_runtime_event("listener_boot"))
    loop.create_task(_heartbeat_loop())

    # Backfill: догнать сообщения, пропущенные пока сервис был down / сессия мертва
    # (startup gap-recovery) + периодически (дрейф live-handler). Фоновые таски —
    # live-ingestion выше НЕ блокируется. Универсально по writer.telegram_user_id.
    # Первый backfill стартует на свежем connect: для чатов с курсором catch_up
    # догонит полностью; новые чаты засеются последними N (см. startup_backfill
    # Non-goals). ImportError/любой сбой здесь логируется как ERROR (не warning) —
    # тихая поломка backfill = §Always-green main observable signal (владелец узнал
    # бы только из отсутствия данных).
    try:
        from heroes_platform.heroes_telegram_mcp.startup_backfill import schedule_backfill_tasks

        schedule_backfill_tasks(loop, client, _get_writer())
    except Exception as exc:
        logger.error("Failed to schedule backfill tasks: %s", exc, exc_info=True)
        print(
            f"⚠️ Backfill NOT scheduled (live ingestion still on): {exc}",
            file=__import__("sys").stderr,
        )
