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

# Only import Supabase writer when actually used
_writer: Any = None


def _get_writer() -> Any:
    """Lazy-init SupabaseWriter."""
    global _writer
    if _writer is None:
        from heroes_platform.heroes_telegram_mcp.supabase_writer import SupabaseWriter

        telegram_user = os.getenv("TELEGRAM_USER", "ikrasinsky")
        _writer = SupabaseWriter(telegram_user_id=telegram_user)
    return _writer


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
            chat = await event.get_chat()
            chat_id = event.chat_id or getattr(chat, "id", 0)
            chat_type = _get_chat_type(chat)

            writer = _get_writer()
            success = await writer.write_message(message, chat_id, chat_type)

            if success:
                # Update last_seen cursor
                await writer.update_chat_cursor(
                    chat_id, last_seen_message_id=message.id,
                )
        except Exception as exc:
            logger.error("Error handling new message: %s", exc, exc_info=True)

    @client.on(events.MessageEdited)
    async def on_message_edited(event: Any) -> None:
        """Handle edited messages -> update in Supabase (upsert)."""
        try:
            message = event.message
            chat = await event.get_chat()
            chat_id = event.chat_id or getattr(chat, "id", 0)
            chat_type = _get_chat_type(chat)

            writer = _get_writer()
            await writer.write_message(message, chat_id, chat_type)
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
                "Messages deleted in chat %s: %s", chat_id, deleted_ids,
            )
            # Future: mark messages as deleted in Supabase
        except Exception as exc:
            logger.error("Error handling deleted message: %s", exc, exc_info=True)

    logger.info(
        "Event handlers registered: NewMessage, MessageEdited, MessageDeleted"
    )
    print(
        "✅ Supabase event handlers registered (LABA_MODE)",
        file=__import__("sys").stderr,
    )

    async def _record_listener_boot() -> None:
        try:
            writer = _get_writer()
            run_id = await writer.start_ingest_run(mode="listener_boot")
            await writer.finish_ingest_run(run_id, processed_chats=0, inserted_messages=0)
        except Exception as exc:
            logger.warning("Failed to write listener_boot ingest marker: %s", exc)

    loop.create_task(_record_listener_boot())
