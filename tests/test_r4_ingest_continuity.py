#!/usr/bin/env python3
"""R4 ingest-continuity tests (pr-hero-1u1) — «канал молча застревает 9 дней».

2-TDD: each test proved a real bug on baseline, then the fix flips it. Pure/AST
checks are used where a live Telegram/Supabase call would otherwise be required —
the bugs are structural (interactive start, wrong iter direction, missing heartbeat)
so the source itself is the falsifiable surface.
"""
from __future__ import annotations

import ast
import re
from pathlib import Path

_HERE = Path(__file__).resolve().parent
_ROOT = _HERE.parent
_CATCHUP = _ROOT / "scripts" / "catch_up_recent_telegram_to_supabase.py"
_STARTUP = _ROOT / "startup_backfill.py"


# ---------------------------------------------------------------------------
# I1 — catch_up must NOT call client.start() (interactive input() hang under launchd)
# ---------------------------------------------------------------------------
class TestI1NoInteractiveStart:
    def test_catchup_does_not_call_client_start(self):
        # AST — detect an ACTUAL `client.start()` call, not the substring inside the
        # explanatory comment (which legitimately names the removed pattern).
        tree = ast.parse(_CATCHUP.read_text())
        calls = [
            n for n in ast.walk(tree)
            if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
            and n.func.attr == "start"
            and isinstance(n.func.value, ast.Name) and n.func.value.id == "client"
        ]
        assert not calls, (
            "client.start() drops into interactive input() on a dead session → "
            "hangs forever under launchd (the 9-day lisa stall)"
        )

    def test_catchup_guards_authorization_before_use(self):
        src = _CATCHUP.read_text()
        assert "is_user_authorized()" in src, "must verify authorization, not blind start()"
        # a dead session must exit non-zero, not proceed
        assert re.search(r"is_user_authorized\(\).*\n(.*\n)*?\s*return 1", src) or \
               "return 1" in src.split("is_user_authorized")[1][:400], \
               "an unauthorized session must return non-zero, not hang or continue"

    def test_connect_precedes_any_start_semantics(self):
        # connect() never prompts; it must be the path used.
        src = _CATCHUP.read_text()
        assert "client.connect()" in src


# ---------------------------------------------------------------------------
# I2 — seed must fetch the LATEST messages, not the oldest (reverse=True bug)
# ---------------------------------------------------------------------------
class TestI2SeedLatestNotOldest:
    def test_seed_iter_messages_not_reverse(self):
        src = _STARTUP.read_text()
        # The seed loop must not use reverse=True (which walks oldest→newest at
        # offset 0, seeding years-old messages and parking the cursor on an old id).
        tree = ast.parse(src)
        offending = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                fn = node.func
                is_iter = isinstance(fn, ast.Attribute) and fn.attr == "iter_messages"
                if is_iter:
                    for kw in node.keywords:
                        if kw.arg == "reverse" and isinstance(kw.value, ast.Constant) and kw.value.value is True:
                            offending.append(ast.dump(node)[:60])
        assert not offending, f"seed still uses reverse=True (seeds oldest): {offending}"


# ---------------------------------------------------------------------------
# I5 — forward catch-up must emit a heartbeat (silent-fail detection)
# ---------------------------------------------------------------------------
class TestI5ForwardHeartbeat:
    def test_catchup_emits_runtime_event(self):
        src = _CATCHUP.read_text()
        assert src.count("record_runtime_event") >= 1, (
            "forward phase wrote NO runtime marker → 100% chats can fail and launchd "
            "still sees success (silent fail up to INGEST_STALE_HOURS)"
        )

    def test_heartbeat_boot_and_completion(self):
        src = _CATCHUP.read_text()
        assert "catchup_boot" in src, "need a boot heartbeat (cycle actually started)"
        assert "catchup_heartbeat" in src, "need a completion heartbeat (how much processed)"


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main([__file__, "-v"]))
