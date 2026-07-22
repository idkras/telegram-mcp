#!/usr/bin/env python3
"""Article / Instant View enrichment for Telegram messages.

JTBD: Когда в чат приходит пост-статья (ссылка с Instant View — telegra.ph,
СМИ, каналы), хотим сохранять в Supabase полное тело статьи, а не только
title/description превью, чтобы поиск по статьям работал полнотекстово.

Диагноз 2026-07-22: из 136 983 webpage-сообщений в telegram_messages_raw
только 6 369 (4.6%) имели непустой cached_page — Telegram присылает Page
вместе с сообщением редко, а писатель его не дозапрашивал.

Слои:
    - message_webpage_url(): у сообщения webpage-вложение без cached_page?
    - fetch_cached_page(): дозапрос Page через messages.GetWebPageRequest
      (телу нужна живая Telethon-сессия; вызывается из event_handlers и
      scripts/backfill_article_pages.py --mode fetch).
    - enrich_message_with_page(): прикрепить Page к message.media.webpage,
      чтобы обычный message.to_dict() в писателе сохранил его в raw.
    - extract_article_text(): plain-text из Page.blocks (рекурсивный обход
      Text*-узлов — работает на dict-представлении, без Telethon).
    - article_row_from_message_row(): строка telegram_articles из уже
      отредактированного (redacted) row писателя — редактирование секретов
      наследуется от raw, отдельного канала утечки нет.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

logger = logging.getLogger(__name__)

# Живой fetch в realtime-обработчике не должен задерживать ingest: сеть/DC
# Telegram может тупить, а NewMessage-хендлер держит очередь событий.
FETCH_TIMEOUT_SECONDS = 15
# FloodWait в realtime-пути не пересиживаем (сообщение уже пишется без тела
# статьи; backfill доберёт позже). В backfill-скрипте порог выше.
FLOOD_WAIT_MAX_SLEEP_REALTIME = 5


def _webpage_of(message: Any) -> Any | None:
    """Вернуть Telethon WebPage объекта сообщения, если media — webpage."""
    media = getattr(message, "media", None)
    if media is None:
        return None
    webpage = getattr(media, "webpage", None)
    if webpage is None:
        return None
    # WebPageEmpty / WebPagePending не имеют ни url-контента, ни cached_page
    if not getattr(webpage, "url", None):
        return None
    return webpage


def message_webpage_url(message: Any) -> str | None:
    """URL webpage-вложения, если тело статьи ещё не приложено сервером."""
    webpage = _webpage_of(message)
    if webpage is None:
        return None
    if getattr(webpage, "cached_page", None) is not None:
        return None  # тело уже есть — дозапрос не нужен
    return str(webpage.url)


async def fetch_cached_page(
    client: Any,
    url: str,
    *,
    flood_wait_max_sleep: int = FLOOD_WAIT_MAX_SLEEP_REALTIME,
) -> Any | None:
    """Дозапросить полную страницу (Instant View) по URL.

    Возвращает Telethon Page объект или None (нет IV-версии / ошибка).
    """
    try:
        from telethon.errors import FloodWaitError  # type: ignore
        from telethon.tl.functions.messages import GetWebPageRequest  # type: ignore
    except ImportError:  # pragma: no cover - telethon всегда есть на VPS
        logger.warning("telethon not importable — article fetch skipped")
        return None

    try:
        result = await client(GetWebPageRequest(url=url, hash=0))
    except FloodWaitError as exc:
        wait = int(getattr(exc, "seconds", 0) or 0)
        if wait <= flood_wait_max_sleep:
            await asyncio.sleep(wait + 1)
            try:
                result = await client(GetWebPageRequest(url=url, hash=0))
            except Exception as retry_exc:  # noqa: BLE001
                logger.info("article fetch retry failed for %s: %s", url, retry_exc)
                return None
        else:
            logger.info("article fetch FloodWait %ss > %ss for %s — skip", wait, flood_wait_max_sleep, url)
            return None
    except Exception as exc:  # noqa: BLE001 — любой RPC-сбой не должен ломать ingest
        logger.info("article fetch failed for %s: %s", url, exc)
        return None

    # Слои TL: messages.getWebPage → messages.WebPage{webpage} (новые) или WebPage (старые)
    webpage = getattr(result, "webpage", result)
    page = getattr(webpage, "cached_page", None)
    if page is None:
        return None
    return page


async def enrich_message_with_page(
    client: Any,
    message: Any,
    *,
    timeout: float = FETCH_TIMEOUT_SECONDS,
) -> bool:
    """Если у сообщения webpage без тела — дозапросить и приложить Page.

    Мутирует message.media.webpage.cached_page, чтобы дальнейший
    message.to_dict() в SupabaseWriter сохранил тело статьи в raw.
    Возвращает True, если Page приложен.
    """
    url = message_webpage_url(message)
    if not url:
        return False
    try:
        page = await asyncio.wait_for(fetch_cached_page(client, url), timeout=timeout)
    except asyncio.TimeoutError:
        logger.info("article fetch timeout (%ss) for %s", timeout, url)
        return False
    if page is None:
        return False
    try:
        message.media.webpage.cached_page = page
    except Exception as exc:  # noqa: BLE001 — экзотический media-тип не критичен
        logger.info("cannot attach cached_page for %s: %s", url, exc)
        return False
    return True


# ---------------------------------------------------------------------------
# Извлечение текста из Page.blocks (dict-представление, после to_dict())
# ---------------------------------------------------------------------------

# Блоки, содержимое которых — не читаемый текст статьи.
_SKIP_BLOCK_TYPES = {
    "PageBlockUnsupported",
    "PageBlockAnchor",
}


def _rich_text_to_str(node: Any) -> str:
    """Рекурсивно собрать строку из RichText-узла (TextPlain/TextBold/...)."""
    if node is None:
        return ""
    if isinstance(node, str):
        return node
    if isinstance(node, list):
        return "".join(_rich_text_to_str(x) for x in node)
    if isinstance(node, dict):
        t = node.get("_", "")
        if t == "TextPlain":
            return str(node.get("text", ""))
        if t == "TextEmpty":
            return ""
        if t == "TextConcat":
            return "".join(_rich_text_to_str(x) for x in node.get("texts", []))
        # TextBold/Italic/Underline/Strike/Fixed/Url/Email/Phone/Marked/
        # Subscript/Superscript/Anchor — обёртки вокруг вложенного text
        inner = node.get("text")
        if inner is not None:
            return _rich_text_to_str(inner)
    return ""


def _block_to_lines(block: Any, out: list[str]) -> None:
    """Достать читаемые строки из одного PageBlock* (рекурсивно)."""
    if not isinstance(block, dict):
        return
    btype = block.get("_", "")
    if btype in _SKIP_BLOCK_TYPES:
        return
    # Прямые rich-text поля блока
    for key in ("text", "caption", "credit", "footer", "title", "author", "h1", "h2"):
        val = block.get(key)
        if val is None:
            continue
        # caption бывает PageCaption{text, credit} — тоже dict с rich-text внутри
        if isinstance(val, dict) and val.get("_") == "PageCaption":
            for cap_key in ("text", "credit"):
                s = _rich_text_to_str(val.get(cap_key)).strip()
                if s:
                    out.append(s)
            continue
        s = _rich_text_to_str(val).strip()
        if s:
            out.append(s)
    # Списки: items — PageListItem* / PageListOrderedItem*
    for items_key in ("items",):
        for item in block.get(items_key) or []:
            if isinstance(item, dict):
                # PageListItemText{text} / PageListItemBlocks{blocks}
                s = _rich_text_to_str(item.get("text")).strip()
                if s:
                    out.append(s)
                for sub in item.get("blocks") or []:
                    _block_to_lines(sub, out)
            else:
                s = _rich_text_to_str(item).strip()
                if s:
                    out.append(s)
    # Таблицы: rows[].cells[].text
    for row in block.get("rows") or []:
        if isinstance(row, dict):
            cells: list[str] = []
            for cell in row.get("cells") or []:
                if isinstance(cell, dict):
                    s = _rich_text_to_str(cell.get("text")).strip()
                    if s:
                        cells.append(s)
            if cells:
                out.append(" | ".join(cells))
    # Вложенные блоки: cover/details/blockquote/slideshow/collage
    for sub_key in ("blocks", "cover", "page_blocks"):
        sub = block.get(sub_key)
        if isinstance(sub, dict):
            _block_to_lines(sub, out)
        elif isinstance(sub, list):
            for x in sub:
                _block_to_lines(x, out)


def extract_article_text(page: Any) -> str:
    """Plain-text статьи из dict-представления Page (raw JSONB).

    Порядок блоков сохраняется; пустые/служебные блоки пропускаются.
    """
    if not isinstance(page, dict):
        return ""
    lines: list[str] = []
    for block in page.get("blocks") or []:
        _block_to_lines(block, lines)
    return "\n".join(lines).strip()


def article_row_from_message_row(row: dict[str, Any]) -> dict[str, Any] | None:
    """Собрать строку telegram_articles из готового row писателя.

    Работает по row["raw"] ПОСЛЕ redaction — текст статьи наследует
    маскирование секретов. None, если сообщение не webpage-пост.
    """
    raw = row.get("raw") or {}
    media = raw.get("media") or {}
    if media.get("_") != "MessageMediaWebPage":
        return None
    webpage = media.get("webpage") or {}
    url = webpage.get("url")
    if not url:
        return None
    page = webpage.get("cached_page")
    article_text = extract_article_text(page) if page else ""
    title = webpage.get("title") or ""
    description = webpage.get("description") or ""
    if not (title or description or article_text):
        return None  # пустая карточка — индексировать нечего
    return {
        "chat_id": row["chat_id"],
        "message_id": row["message_id"],
        "telegram_user_id": row.get("telegram_user_id"),
        "message_ts": row.get("message_ts"),
        "url": url,
        "title": title,
        "description": description,
        "article_text": article_text,
        "has_page": bool(page),
    }
