"""Tests for article_enrichment: извлечение тела статьи и карточки для индекса.

Red-phase истина: до фикса писатель сохранял cached_page=null у 95.4% webpage-
сообщений и не имел таблицы telegram_articles вовсе — поиск по телу статьи был
невозможен. Эти тесты фиксируют контракт извлечения на dict-представлении Page
(как оно лежит в raw JSONB), без сети и без Telethon.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from article_enrichment import (  # noqa: E402
    article_row_from_message_row,
    extract_article_text,
    message_webpage_url,
)

# Реальная структура из Supabase raw (сообщение zerkalo.io, 2026): Page → blocks
SAMPLE_PAGE = {
    "_": "Page",
    "url": "https://news.example.io/world/128264.html",
    "blocks": [
        {"_": "PageBlockTitle", "text": {"_": "TextPlain", "text": "Заголовок статьи"}},
        {
            "_": "PageBlockAuthorDate",
            "author": {"_": "TextPlain", "text": "@author"},
            "published_date": "2026-05-28T18:04:00+00:00",
        },
        {
            "_": "PageBlockBlockquote",
            "text": {"_": "TextPlain", "text": "Цитата из статьи."},
            "caption": {"_": "TextPlain", "text": "Подпись цитаты"},
        },
        {
            "_": "PageBlockParagraph",
            "text": {
                "_": "TextConcat",
                "texts": [
                    {"_": "TextPlain", "text": "Первый абзац с "},
                    {"_": "TextBold", "text": {"_": "TextPlain", "text": "жирным"}},
                    {"_": "TextPlain", "text": " словом."},
                ],
            },
        },
        {
            "_": "PageBlockList",
            "items": [
                {"_": "PageListItemText", "text": {"_": "TextPlain", "text": "пункт один"}},
                {"_": "PageListItemText", "text": {"_": "TextPlain", "text": "пункт два"}},
            ],
        },
        {"_": "PageBlockUnsupported"},
    ],
}


class TestExtractArticleText:
    def test_extracts_all_readable_blocks_in_order(self):
        text = extract_article_text(SAMPLE_PAGE)
        assert "Заголовок статьи" in text
        assert "Цитата из статьи." in text
        assert "Первый абзац с жирным словом." in text
        assert "пункт один" in text and "пункт два" in text
        # порядок сохранён: заголовок раньше абзаца
        assert text.index("Заголовок статьи") < text.index("Первый абзац")

    def test_rich_text_wrappers_unwrapped(self):
        text = extract_article_text(SAMPLE_PAGE)
        # TextBold/TextConcat не оставляют служебных следов
        assert "TextBold" not in text and "{" not in text

    def test_null_and_garbage_page_safe(self):
        assert extract_article_text(None) == ""
        assert extract_article_text("null") == ""
        assert extract_article_text({}) == ""
        assert extract_article_text({"blocks": [None, 42, "str"]}) == ""


class TestArticleRowFromMessageRow:
    def _row(self, media: dict | None) -> dict:
        raw: dict = {"_": "Message", "id": 10}
        if media is not None:
            raw["media"] = media
        return {
            "chat_id": "-100123",
            "message_id": 10,
            "telegram_user_id": "ikrasinsky",
            "message_ts": "2026-07-22T10:00:00+00:00",
            "text": "смотрите статью",
            "raw": raw,
        }

    def test_webpage_with_body(self):
        media = {
            "_": "MessageMediaWebPage",
            "webpage": {
                "_": "WebPage",
                "url": "https://telegra.ph/x-01-01",
                "title": "Титул",
                "description": "Превью",
                "cached_page": SAMPLE_PAGE,
            },
        }
        row = article_row_from_message_row(self._row(media))
        assert row is not None
        assert row["url"] == "https://telegra.ph/x-01-01"
        assert row["title"] == "Титул"
        assert row["has_page"] is True
        assert "Первый абзац" in row["article_text"]

    def test_webpage_preview_only(self):
        media = {
            "_": "MessageMediaWebPage",
            "webpage": {"_": "WebPage", "url": "https://a.b/c", "title": "T", "cached_page": None},
        }
        row = article_row_from_message_row(self._row(media))
        assert row is not None
        assert row["has_page"] is False
        assert row["article_text"] == ""

    def test_non_webpage_returns_none(self):
        assert article_row_from_message_row(self._row(None)) is None
        assert article_row_from_message_row(self._row({"_": "MessageMediaPhoto"})) is None

    def test_empty_card_skipped(self):
        media = {"_": "MessageMediaWebPage", "webpage": {"_": "WebPage", "url": "https://a.b/c"}}
        assert article_row_from_message_row(self._row(media)) is None


class TestMessageWebpageUrl:
    class _Obj:
        def __init__(self, **kw):
            self.__dict__.update(kw)

    def test_needs_fetch_when_no_cached_page(self):
        msg = self._Obj(media=self._Obj(webpage=self._Obj(url="https://x.y/z", cached_page=None)))
        assert message_webpage_url(msg) == "https://x.y/z"

    def test_no_fetch_when_page_present(self):
        msg = self._Obj(media=self._Obj(webpage=self._Obj(url="https://x.y/z", cached_page={"_": "Page"})))
        assert message_webpage_url(msg) is None

    def test_no_media(self):
        assert message_webpage_url(self._Obj(media=None)) is None
        assert message_webpage_url(self._Obj(media=self._Obj(webpage=None))) is None
