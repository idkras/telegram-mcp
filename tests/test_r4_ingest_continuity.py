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
    def test_catchup_emits_runtime_event_ast(self):
        # L1 (code review): AST — an ACTUAL record_runtime_event call with the boot
        # AND completion modes, not a substring in a comment (gameable).
        tree = ast.parse(_CATCHUP.read_text())
        modes = set()
        for n in ast.walk(tree):
            if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute) \
               and n.func.attr == "record_runtime_event":
                for kw in n.keywords:
                    if kw.arg == "mode" and isinstance(kw.value, ast.Constant):
                        modes.add(kw.value.value)
        assert "catchup_boot" in modes, "need a boot heartbeat call (cycle actually started)"
        assert "catchup_heartbeat" in modes, "need a completion heartbeat call (how much processed)"

    def test_dead_session_writes_explicit_marker(self):
        # I5-consumer: an unauthorized session must write catchup_session_dead so the
        # exit-1 loop is VISIBLE, not a silent absence of a fresh heartbeat.
        tree = ast.parse(_CATCHUP.read_text())
        modes = {
            kw.value.value
            for n in ast.walk(tree)
            if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
            and n.func.attr == "record_runtime_event"
            for kw in n.keywords
            if kw.arg == "mode" and isinstance(kw.value, ast.Constant)
        }
        assert "catchup_session_dead" in modes


# ---------------------------------------------------------------------------
# I1-network — connect() must be guarded (uncaught crash → not the graceful exit)
# ---------------------------------------------------------------------------
class TestI1NetworkGuard:
    def test_connect_wrapped_in_try(self):
        src = _CATCHUP.read_text()
        # connect() must sit inside a try so a DNS/sock error becomes return 1, not a crash.
        m = re.search(r"try:\s*\n\s*await client\.connect\(\)", src)
        assert m is not None, "client.connect() must be inside try/except → fail loud, not crash"


# ---------------------------------------------------------------------------
# I5-consumer — doctor must actually READ the catch-up markers (was theatre)
# ---------------------------------------------------------------------------
class TestI5DoctorConsumer:
    def test_doctor_has_catchup_freshness_check(self):
        src = (_ROOT / "telegram_mcp_doctor.py").read_text()
        assert "def check_catchup_freshness" in src, (
            "heartbeat markers are theatre without a doctor consumer (design+falsifier CRITICAL)"
        )
        assert "check_catchup_freshness" in src.split("CHECKS =")[1][:200], \
            "check_catchup_freshness must be wired into the CHECKS list, not just defined"

    def test_freshness_reads_ingest_runs_markers(self):
        src = (_ROOT / "telegram_mcp_doctor.py").read_text()
        assert "telegram_ingest_runs" in src and "catchup_boot" in src, \
            "consumer must query the ingest_runs table for the catch-up markers"


# ---------------------------------------------------------------------------
# I2-REST — seed cursor must be monotonic in-code (REST path has no GREATEST)
# ---------------------------------------------------------------------------
class TestI2RestMonotonic:
    def test_seed_uses_high_water_mark(self):
        src = _STARTUP.read_text()
        assert "seed_cursor_hwm" in src, (
            "newest→oldest batches would regress the cursor on the REST path (no GREATEST); "
            "an in-code high-water-mark keeps it monotonic on both paths"
        )
        # the cursor update must be gated by the hwm comparison
        assert re.search(r"if\s+max_id\s*>\s*seed_cursor_hwm", src), \
            "cursor must only advance when max_id exceeds the running high-water-mark"


_INSTALLER = _ROOT / "scripts" / "install_deep_backfill_launchd.sh"


# ---------------------------------------------------------------------------
# M4 — deprecated launchd installer must not register a disabled /bin/echo stub
# ---------------------------------------------------------------------------
class TestM4LaunchdStub:
    def test_installer_refuses_instead_of_registering_stub(self):
        src = _INSTALLER.read_text()
        # It must exit BEFORE the ACTUAL launchctl bootstrap COMMAND (not the mention
        # of «launchctl bootstrap» in the header comment).
        idx_exit = src.find("\nexit 2")
        m = re.search(r'^\s*launchctl bootstrap ', src, re.MULTILINE)
        assert idx_exit != -1, "installer must fail loudly (exit 2) with a redirect"
        assert m is None or idx_exit < m.start(), (
            "installer must exit BEFORE registering the disabled /bin/echo stub"
        )

    def test_installer_points_to_real_scheduler(self):
        assert "periodic-catchup" in _INSTALLER.read_text(), (
            "must redirect operator to the real scheduler, not silently no-op"
        )


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main([__file__, "-v"]))
