#!/usr/bin/env python3
"""Telegram → Supabase index guardian (pr-hero-gcy).

Self-contained (submodule-local) so it ships with telegram-mcp to the VPS.
telegram-mcp calls `classify_message` on EVERY incoming message before writing
to Supabase and decides: skip whole chat / redact sensitive values / flag for
owner review.

SSOT: telegram_index_blacklist.yaml (same dir). Adding a chat/rule = editing
YAML, not code.

Public API:
    load_rules(path=None) -> dict
    normalize_id_tail(chat_id) -> str
    is_blacklisted(chat_id, title, rules) -> (bool, reason)
    redact_secrets(text, rules) -> (str, list[str])          # masked text + categories hit
    classify_message(chat_id, title, text, rules) -> GuardDecision
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

_DEFAULT_PATH = Path(__file__).with_name("telegram_index_blacklist.yaml")


@dataclass
class GuardDecision:
    action: str                         # "skip" | "save"
    text: str                           # possibly redacted text
    categories: list[str] = field(default_factory=list)  # what was masked
    review: bool = False                # flag for owner switchBar
    reason: str = ""


def load_rules(path: str | Path | None = None) -> dict[str, Any]:
    p = Path(path) if path else _DEFAULT_PATH
    with open(p, encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    data["_skip_tails"] = {str(c["id_tail"]) for c in (data.get("chats") or []) if c.get("action") == "skip"}
    data["_skip_re"] = [re.compile(r) for r in (data.get("title_skip_regex") or [])]
    data["_redact"] = [(c["name"], re.compile(c["regex"]), c["placeholder"]) for c in (data.get("content_redact") or [])]
    data["_title_review_re"] = [re.compile(r) for r in (data.get("title_review_regex") or [])]
    data["_content_review_re"] = [re.compile(r) for r in (data.get("content_review_regex") or [])]
    return data


def normalize_id_tail(chat_id: Any) -> str:
    s = str(chat_id).strip()
    if s.startswith("-100"):
        return s[4:]
    if s.startswith("-"):
        return s[1:]
    return s


def is_blacklisted(chat_id: Any, title: str | None, rules: dict[str, Any]) -> tuple[bool, str]:
    tail = normalize_id_tail(chat_id)
    if tail in rules.get("_skip_tails", set()):
        for c in rules.get("chats") or []:
            if str(c.get("id_tail")) == tail and c.get("action") == "skip":
                return True, f"blacklisted chat ({c.get('title')}): {c.get('reason')}"
        return True, "blacklisted chat"
    t = title or ""
    for rx in rules.get("_skip_re", []):
        if rx.search(t):
            return True, f"title skip-pattern: {rx.pattern}"
    return False, ""


_BARE_CODE_RE = re.compile(r"(?<!\d)\d{4,8}(?!\d)")


def redact_secrets(text: str | None, rules: dict[str, Any]) -> tuple[str, list[str]]:
    """Mask sensitive values (card / code / password / passport / snils). Keep message."""
    if not text:
        return text or "", []
    out = text
    hit: list[str] = []
    for name, rx, placeholder in rules.get("_redact", []):
        if rx.search(out):
            out = rx.sub(placeholder, out)
            hit.append(name)
    # bare-OTP heuristic (security-reviewer squad 2026-07-03): a keyword-gated regex
    # ("код 8241") misses a naked OTP ("8241", "8241 do not share"). Masking every
    # 4-8 digit run would corrupt prose (prices/years/counts), so gate on message
    # SHAPE: only when the whole (stripped) message is SHORT — i.e. it basically IS a
    # code, not prose with an embedded number. Threshold configurable; 0 disables.
    max_len = int(rules.get("bare_code_max_len", 40) or 0)
    if max_len and len(out.strip()) <= max_len and _BARE_CODE_RE.search(out):
        out = _BARE_CODE_RE.sub("[REDACTED-CODE]", out)
        if "bare_otp" not in hit:
            hit.append("bare_otp")
    return out, hit


def _needs_review(title: str | None, text: str | None, rules: dict[str, Any]) -> bool:
    t = title or ""
    for rx in rules.get("_title_review_re", []):
        if rx.search(t):
            return True
    body = text or ""
    for rx in rules.get("_content_review_re", []):
        if rx.search(body):
            return True
    return False


def classify_message(chat_id: Any, title: str | None, text: str | None,
                     rules: dict[str, Any]) -> GuardDecision:
    """The guardian: decide save/skip/redact for ONE incoming message."""
    blk, reason = is_blacklisted(chat_id, title, rules)
    if blk:
        return GuardDecision(action="skip", text="", reason=reason)
    redacted, cats = redact_secrets(text, rules)
    review = _needs_review(title, text, rules)
    return GuardDecision(action="save", text=redacted, categories=cats, review=review,
                         reason=("sensitive values masked" if cats else ""))


if __name__ == "__main__":
    import json
    import sys

    rules = load_rules()
    if len(sys.argv) >= 4:
        d = classify_message(sys.argv[1], sys.argv[2], sys.argv[3], rules)
        print(json.dumps({"action": d.action, "text": d.text, "categories": d.categories,
                          "review": d.review, "reason": d.reason}, ensure_ascii=False))
    else:
        print(f"loaded {len(rules.get('_skip_tails', set()))} skip-chats, "
              f"{len(rules.get('_skip_re', []))} skip-patterns, "
              f"{len(rules.get('_redact', []))} redact-rules, "
              f"{len(rules.get('_title_review_re', []))} review-patterns")
