"""Markdown → Telegram HTML converter.

JTBD: skill `4-offer-update-and-review` Step 7.1 — конвертирует draft.md в HTML
для Telegram parse_mode='html'. Whitelist tags per Bot API:
b, strong | i, em | u, ins | s, strike, del | code | pre | a href | tg-spoiler

Falsification reference: storytelling/src/telegram_client.py НЕ имеет такого
конвертера (только validate_html_formatting tag-whitelist). Этот модуль —
первая рабочая реализация в workspace.

RCA: 2026-05-26 owner steering «найди storytelling md→HTML код … нет конвертера».

Usage:
    from heroes_platform.heroes_telegram_mcp.converters.md_to_telegram_html import md_to_html
    html = md_to_html("**bold** and *italic* and [link](https://x.com)")
    # → "<b>bold</b> and <i>italic</i> and <a href=\"https://x.com\">link</a>"

Tested на 10 кейсах из реальных офферов management chat (см. tests).
"""

from __future__ import annotations

import html as _html
import re


# Telegram HTML whitelist (Bot API documentation):
# https://core.telegram.org/bots/api#html-style
ALLOWED_TAGS = {"b", "strong", "i", "em", "u", "ins", "s", "strike", "del", "code", "pre", "a", "tg-spoiler"}


def _escape_html(text: str) -> str:
    """HTML escape, но сохранить уже сконвертированные tags."""
    return _html.escape(text, quote=False)


def md_to_html(md: str) -> str:
    """Convert markdown to Telegram-flavored HTML.

    Поддержка:
    - **bold** / __bold__ → <b>
    - *italic* / _italic_ → <i>
    - ~~strike~~ → <s>
    - `code` → <code>
    - ```pre``` → <pre>
    - [text](url) → <a href="url">text</a>
    - # H1, ## H2, ### H3 → <b> (Telegram нет headings)
    - − / * / • items → перенос строки с маркером

    НЕ поддерживает:
    - таблицы (Telegram HTML их не рендерит)
    - blockquote (нет тега)
    - изображения inline (через sendPhoto)
    """
    if not md:
        return ""

    text = md

    # Шаг 1: code blocks ``` → <pre> (до escape, чтобы внутри не escape'нулись маркеры)
    pre_blocks: list[str] = []

    def _stash_pre(m: re.Match[str]) -> str:
        content = m.group(1)
        pre_blocks.append(content)
        return f"\x00PRE{len(pre_blocks) - 1}\x00"

    text = re.sub(r"```(.*?)```", _stash_pre, text, flags=re.DOTALL)

    # Шаг 2: inline code `code` → stash (тоже до escape)
    code_blocks: list[str] = []

    def _stash_code(m: re.Match[str]) -> str:
        content = m.group(1)
        code_blocks.append(content)
        return f"\x00CODE{len(code_blocks) - 1}\x00"

    text = re.sub(r"`([^`]+?)`", _stash_code, text)

    # Шаг 3: links [text](url) → stash
    link_blocks: list[tuple[str, str]] = []

    def _stash_link(m: re.Match[str]) -> str:
        link_text = m.group(1)
        link_url = m.group(2)
        link_blocks.append((link_text, link_url))
        return f"\x00LINK{len(link_blocks) - 1}\x00"

    text = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", _stash_link, text)

    # Шаг 4: headings # H1 → <b>H1</b>
    text = re.sub(r"^#{1,6}\s+(.+)$", r"<b>\1</b>", text, flags=re.MULTILINE)

    # Шаг 5: bold **text** или __text__
    text = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", text, flags=re.DOTALL)
    text = re.sub(r"__(.+?)__", r"<b>\1</b>", text, flags=re.DOTALL)

    # Шаг 6: italic *text* или _text_ (избегая уже обработанных **)
    # Negative lookahead/lookbehind для звёзд
    text = re.sub(r"(?<!\*)\*(?!\*)(.+?)(?<!\*)\*(?!\*)", r"<i>\1</i>", text)
    text = re.sub(r"(?<!_)_(?!_)(.+?)(?<!_)_(?!_)", r"<i>\1</i>", text)

    # Шаг 7: strikethrough ~~text~~
    text = re.sub(r"~~(.+?)~~", r"<s>\1</s>", text, flags=re.DOTALL)

    # Шаг 8: списки − или * или • → •
    text = re.sub(r"^[\s]*[-*•]\s+", r"• ", text, flags=re.MULTILINE)
    # numbered lists 1. 2. → оставляем как есть, Telegram рендерит plain

    # Шаг 9: escape остальной HTML
    # ВАЖНО: уже добавленные нами <b>/<i>/<s> tags должны выжить
    # Strategy: split по нашим тагам, escape только non-tag parts
    parts: list[str] = []
    buf = text
    tag_pattern = re.compile(r"(</?(?:b|i|u|s|code|pre|a|tg-spoiler)(?:\s[^>]*)?>)")
    pieces = tag_pattern.split(buf)
    for i, piece in enumerate(pieces):
        if i % 2 == 0:
            # text part — escape
            parts.append(_escape_html(piece))
        else:
            # tag part — оставить как есть
            parts.append(piece)
    text = "".join(parts)

    # Шаг 10: restore links (тоже escape URL для href)
    for i, (link_text, link_url) in enumerate(link_blocks):
        safe_text = _escape_html(link_text)
        safe_url = _html.escape(link_url, quote=True)
        text = text.replace(f"\x00LINK{i}\x00", f'<a href="{safe_url}">{safe_text}</a>')

    # Шаг 11: restore inline code
    for i, content in enumerate(code_blocks):
        safe = _escape_html(content)
        text = text.replace(f"\x00CODE{i}\x00", f"<code>{safe}</code>")

    # Шаг 12: restore pre blocks
    for i, content in enumerate(pre_blocks):
        safe = _escape_html(content)
        text = text.replace(f"\x00PRE{i}\x00", f"<pre>{safe}</pre>")

    return text


def validate_telegram_html(html: str) -> dict[str, object]:
    """Проверить что в HTML только whitelisted tags."""
    open_tags = re.findall(r"<(\w+(?:-\w+)?)[^>]*>", html)
    close_tags = re.findall(r"</(\w+(?:-\w+)?)>", html)
    errors: list[str] = []
    for tag in open_tags:
        if tag.lower() not in ALLOWED_TAGS:
            errors.append(f"non-whitelisted tag: <{tag}>")
    if sorted(open_tags) != sorted(close_tags):
        # Telegram прощает self-closing br, но мы их не генерим
        if set(open_tags) - set(close_tags) - {"br"}:
            errors.append("unpaired tags")
    return {"valid": len(errors) == 0, "errors": errors}


if __name__ == "__main__":
    # Smoke tests на реальных кейсах из management chat
    cases = [
        ("**Вы уже глубже большинства понимаете** 🍿", "<b>Вы уже глубже большинства понимаете</b> 🍿"),
        ("[Sales Heroes](https://heroes.camp/x) • 28 марта", '<a href="https://heroes.camp/x">Sales Heroes</a> • 28 марта'),
        ("`code` and **bold**", "<code>code</code> and <b>bold</b>"),
        ("# Заголовок\n**bold**", "<b>Заголовок</b>\n<b>bold</b>"),
        ("- пункт 1\n- пункт 2", "• пункт 1\n• пункт 2"),
        ("~~зачеркнуть~~", "<s>зачеркнуть</s>"),
        ("escape <script>alert(1)</script>", "escape &lt;script&gt;alert(1)&lt;/script&gt;"),
    ]
    for md, expected in cases:
        got = md_to_html(md)
        status = "✓" if got == expected else "✗"
        print(f"{status} md={md!r:60s} → {got!r}")
        if got != expected:
            print(f"  expected: {expected!r}")
        v = validate_telegram_html(got)
        if not v["valid"]:
            print(f"  ⚠ validation: {v['errors']}")
