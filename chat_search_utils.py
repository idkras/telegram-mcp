#!/usr/bin/env python3
"""
Universal chat search utilities for Telegram MCP server

Универсальные методы поиска чатов, которые можно использовать
как в MCP сервере, так и в скриптах.
"""

import json
from datetime import datetime
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from telethon import TelegramClient
    from telethon.tl.types import Channel, Chat, User
else:
    from telethon.tl.types import Channel, Chat, User


def json_serializer(obj):
    """Helper function to convert non-serializable objects for JSON serialization."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, bytes):
        return obj.decode("utf-8", errors="replace")
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


def get_chat_type(entity: Any) -> str:
    """Определить тип чата по entity"""
    if isinstance(entity, User):
        return "user"
    elif isinstance(entity, Chat):
        return "group"
    elif isinstance(entity, Channel):
        if getattr(entity, "broadcast", False):
            return "channel"
        else:
            return "supergroup"
    return "unknown"


def format_chat_info(entity: Any, dialog: Any = None) -> dict[str, Any]:
    """Форматировать информацию о чате в словарь"""
    title = getattr(entity, "title", None) or getattr(entity, "first_name", "")
    chat_type = get_chat_type(entity)
    
    chat_info = {
        "id": entity.id,
        "title": title,
        "type": chat_type,
        "username": getattr(entity, "username", None),
    }
    
    if dialog:
        chat_info["unread_count"] = getattr(dialog, "unread_count", 0)
    
    return chat_info


async def search_chats_by_keyword_impl(
    client: Any,
    keyword: str,
    chat_type: str | None = None,
    limit: int | None = None
) -> dict[str, Any]:
    """
    Универсальная реализация поиска чатов по ключевому слову.
    
    Args:
        client: TelegramClient instance
        keyword: Keyword to search for in chat titles
        chat_type: Filter by chat type ('user', 'group', 'channel', or None for all)
        limit: Maximum number of chats to return (None for all)
    
    Returns:
        Dictionary with search results
    """
    keyword_lower = keyword.lower()
    matching_chats = []
    count = 0
    
    async for dialog in client.iter_dialogs():
        entity = dialog.entity
        title = getattr(entity, "title", None) or getattr(entity, "first_name", "")
        
        # Check keyword match
        if not title or keyword_lower not in title.lower():
            continue
        
        # Determine chat type
        current_type = get_chat_type(entity)
        
        # Filter by type if requested
        if chat_type and current_type != chat_type.lower():
            continue
        
        chat_info = format_chat_info(entity, dialog)
        matching_chats.append(chat_info)
        count += 1
        
        # Apply limit if specified
        if limit and count >= limit:
            break
    
    return {
        "keyword": keyword,
        "chat_type_filter": chat_type,
        "total_found": len(matching_chats),
        "chats": matching_chats
    }


async def get_all_chats_list_impl(
    client: Any,
    chat_type: str | None = None,
    limit: int | None = None
) -> dict[str, Any]:
    """
    Универсальная реализация получения всех чатов.
    
    Args:
        client: TelegramClient instance
        chat_type: Filter by chat type ('user', 'group', 'channel', or None for all)
        limit: Maximum number of chats to return (None for all)
    
    Returns:
        Dictionary with all chats
    """
    all_chats = []
    count = 0
    
    async for dialog in client.iter_dialogs():
        entity = dialog.entity
        
        # Determine chat type
        current_type = get_chat_type(entity)
        
        # Filter by type if requested
        if chat_type and current_type != chat_type.lower():
            continue
        
        chat_info = format_chat_info(entity, dialog)
        all_chats.append(chat_info)
        count += 1
        
        # Apply limit if specified
        if limit and count >= limit:
            break
    
    return {
        "chat_type_filter": chat_type,
        "total_chats": len(all_chats),
        "chats": all_chats
    }


async def analyze_chat_messages_for_bots_impl(
    client: Any,
    chat_id: int,
    message_limit: int = 100
) -> dict[str, Any]:
    """
    Универсальная реализация анализа сообщений в чате.
    
    Args:
        client: TelegramClient instance
        chat_id: The ID of the chat to analyze
        message_limit: Maximum number of messages to analyze (default: 100)
    
    Returns:
        Dictionary with analysis results
    """
    entity = await client.get_entity(chat_id)
    
    bot_messages = 0
    user_messages = 0
    client_messages = 0
    total_analyzed = 0
    
    async for message in client.iter_messages(entity, limit=message_limit):
        # Skip service messages
        if hasattr(message, 'action') and message.action:
            continue
        
        total_analyzed += 1
        sender = await message.get_sender()
        
        if sender:
            if isinstance(sender, User) and sender.bot:
                bot_messages += 1
            else:
                user_messages += 1
                # Check if it's a client message (not a bot)
                if not (isinstance(sender, User) and sender.bot):
                    client_messages += 1
    
    return {
        "chat_id": chat_id,
        "total_messages_analyzed": total_analyzed,
        "bot_messages": bot_messages,
        "user_messages": user_messages,
        "client_messages": client_messages,
        "only_bots": bot_messages > 0 and client_messages == 0,
        "has_client_messages": client_messages > 0,
        "analysis_date": datetime.now().isoformat()
    }
