#!/usr/bin/env python3
"""R2 honest-detector tests (pr-hero-i5i) — fail-open detectors made fail-closed.

2-TDD contract: each test first proved a real bug on baseline code (fail-open /
tautology / green-when-blind), then the fix flips it. Pure-function checks are
preferred over mocks; DB/SSH/telethon are only stubbed where the function under
test genuinely calls them.

Subtasks covered:
  C1  classify_chats.coverage           — tautology 100% → real classified/total
  S5  telegram_mcp_doctor.check_ingest  — round(None) crash masks empty table
  S4  telegram_mcp_doctor verdict       — all-SKIP is INCONCLUSIVE, not «closed»
  S3  telegram_mcp_doctor CHECKS        — session_auth layer present
  S6  telegram_mcp_doctor collision     — INCONCLUSIVE ≠ green
  M2  main --list-tools                 — real registry count, not hardcoded
  M1  main --test                       — must verify authorization, not truthy client
  M3  main healthcheck                  — non-LABA endpoint not blindly OK
"""
from __future__ import annotations

import importlib.util
import re
import sys
from pathlib import Path

import pytest

_HERE = Path(__file__).resolve().parent
_ROOT = _HERE.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import classify_chats  # noqa: E402
import telegram_mcp_doctor as doctor  # noqa: E402


# ---------------------------------------------------------------------------
# C1 — classify coverage must be classified/total, not the total/total tautology
# ---------------------------------------------------------------------------
class TestClassifyCoverage:
    def test_mostly_unclassified_is_not_100_percent(self):
        # 9273 total, 9000 unclassified → only ~3% actually classified.
        res = {
            "total": 9273,
            "counts": {"unclassified": 9000, "client_chat": 200, "team_chat": 73},
        }
        cov = classify_chats.coverage(res)
        assert cov != 100.0, "tautology: coverage reported 100% while 9000/9273 unclassified"
        assert abs(cov - (100.0 * 273 / 9273)) < 0.01, f"expected ~2.94%, got {cov}"

    def test_all_classified_is_100(self):
        res = {"total": 100, "counts": {"client_chat": 100}}
        assert classify_chats.coverage(res) == 100.0

    def test_empty_total_is_zero_not_crash(self):
        assert classify_chats.coverage({"total": 0, "counts": {}}) == 0.0


# ---------------------------------------------------------------------------
# S5 — ingest: empty telegram_messages_raw (h is None) must be RED, not crash→SKIP
# ---------------------------------------------------------------------------
class TestIngestStale:
    def test_none_hours_is_stale_no_messages(self):
        # Empty raw table → max(message_ts)=NULL → h=None. round(None) crashed on
        # baseline → except → SKIP → masked «no messages».
        stale = doctor._ingest_stale({"ik": None}, threshold=6.0)
        assert "ik" in stale, "empty table (h=None) must be flagged stale/red"
        assert stale["ik"] == "no messages"

    def test_number_over_threshold_is_stale(self):
        stale = doctor._ingest_stale({"lisa": 220.0}, threshold=6.0)
        assert "lisa" in stale
        assert stale["lisa"] == 220.0

    def test_fresh_is_not_stale(self):
        assert doctor._ingest_stale({"ik": 1.2}, threshold=6.0) == {}

    def test_check_ingest_none_returns_ok_false(self, monkeypatch):
        # end-to-end: empty table must make check_ingest return ok=False (RED),
        # never ok=None (SKIP).
        class _Cur:
            def execute(self, *a, **k):
                pass

            def fetchone(self):
                return (None,)

        class _Conn:
            def cursor(self):
                return _Cur()

            def close(self):
                pass

        monkeypatch.setattr(doctor, "_pg", lambda: _Conn())
        monkeypatch.setattr(doctor, "_schemas", lambda s: {"ik": "rick_messages_tasks"})
        r = doctor.check_ingest({})
        assert r["ok"] is False, f"empty raw table must be RED, got ok={r['ok']} detail={r['detail']}"


# ---------------------------------------------------------------------------
# S4 — all-SKIP verdict is INCONCLUSIVE (exit != 0), not «contour closed»
# ---------------------------------------------------------------------------
class TestVerdictAllSkip:
    def test_all_skip_exits_nonzero(self, monkeypatch):
        monkeypatch.setattr(
            doctor, "run",
            lambda: [
                {"layer": "a", "ok": None, "detail": "SKIP"},
                {"layer": "b", "ok": None, "detail": "SKIP"},
            ],
        )
        rc = doctor.main([])
        assert rc != 0, "all layers SKIP means we verified NOTHING — must not be exit 0"

    def test_all_skip_json_exits_nonzero(self, monkeypatch):
        monkeypatch.setattr(
            doctor, "run",
            lambda: [{"layer": "a", "ok": None, "detail": "SKIP"}],
        )
        assert doctor.main(["--json"]) != 0

    def test_at_least_one_green_no_red_exits_zero(self, monkeypatch):
        monkeypatch.setattr(
            doctor, "run",
            lambda: [
                {"layer": "a", "ok": True, "detail": "OK"},
                {"layer": "b", "ok": None, "detail": "SKIP"},
            ],
        )
        assert doctor.main([]) == 0

    def test_any_red_exits_nonzero(self, monkeypatch):
        monkeypatch.setattr(
            doctor, "run",
            lambda: [
                {"layer": "a", "ok": True, "detail": "OK"},
                {"layer": "b", "ok": False, "detail": "RED"},
            ],
        )
        assert doctor.main([]) != 0


# ---------------------------------------------------------------------------
# S3 — session_auth must be one of the checked layers (6/8 → +session_auth)
# ---------------------------------------------------------------------------
class TestSessionAuthLayer:
    def test_session_auth_in_checks(self):
        names = {c.__name__ for c in doctor.CHECKS}
        assert "check_session_auth" in names, f"session_auth layer missing from {sorted(names)}"

    def test_check_session_auth_reports_its_layer(self, monkeypatch):
        # When the monitor script is unavailable the check should SKIP (ok=None),
        # not silently vanish — the point is the layer EXISTS and is labelled.
        monkeypatch.setattr(doctor, "_session_auth_probe", lambda s: (None, "monitor not found (SKIP)"))
        r = doctor.check_session_auth({})
        assert r["layer"] == "session_auth"

    def test_check_session_auth_dead_is_red(self, monkeypatch):
        monkeypatch.setattr(doctor, "_session_auth_probe", lambda s: (False, "lisa=REVOKED"))
        r = doctor.check_session_auth({})
        assert r["ok"] is False


# ---------------------------------------------------------------------------
# S6 — session_collision: validator INCONCLUSIVE (exit 0) must map to SKIP, not GREEN
# ---------------------------------------------------------------------------
class TestSessionCollisionInconclusive:
    def _fake_run(self, returncode, stdout="", stderr=""):
        class _R:
            pass

        r = _R()
        r.returncode = returncode
        r.stdout = stdout
        r.stderr = stderr
        return r

    def test_inconclusive_returncode0_is_skip_not_green(self, monkeypatch):
        monkeypatch.setattr(doctor.Path, "exists", lambda self: True)
        monkeypatch.setattr(
            doctor.subprocess, "run",
            lambda *a, **k: self._fake_run(0, stderr="session-per-endpoint: INCONCLUSIVE — нельзя перечислить"),
        )
        r = doctor.check_session_collision({})
        assert r["ok"] is None, f"INCONCLUSIVE must be SKIP (ok=None), got ok={r['ok']}"

    def test_clean_returncode0_is_green(self, monkeypatch):
        monkeypatch.setattr(doctor.Path, "exists", lambda self: True)
        monkeypatch.setattr(
            doctor.subprocess, "run",
            lambda *a, **k: self._fake_run(0, stdout="session-per-endpoint: OK — no collision"),
        )
        r = doctor.check_session_collision({})
        assert r["ok"] is True

    def test_collision_returncode2_is_red(self, monkeypatch):
        monkeypatch.setattr(doctor.Path, "exists", lambda self: True)
        monkeypatch.setattr(
            doctor.subprocess, "run",
            lambda *a, **k: self._fake_run(2, stderr="collision"),
        )
        r = doctor.check_session_collision({})
        assert r["ok"] is False


# ---------------------------------------------------------------------------
# main.py helpers (M1/M2/M3). main.py cannot be imported (credential bootstrap),
# so the honest logic is extracted into main_cli_helpers.py — a pure module with
# no telethon/credential imports — and main.py delegates to it.
# ---------------------------------------------------------------------------
def _load_helpers():
    spec = importlib.util.spec_from_file_location(
        "main_cli_helpers", str(_ROOT / "main_cli_helpers.py")
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ---------------------------------------------------------------------------
# M2 — --list-tools prints the REAL registry count, not a hardcoded «73+»
# ---------------------------------------------------------------------------
class TestListToolsRealCount:
    def _real_tool_count(self) -> int:
        # ground truth = number of @mcp.tool() decorators in main.py source
        src = (_ROOT / "main.py").read_text(encoding="utf-8")
        return len(re.findall(r"^@mcp\.tool\(\)", src, flags=re.MULTILINE))

    def test_helper_reads_registry_not_hardcoded(self):
        h = _load_helpers()

        class _FakeMgr:
            def __init__(self, names):
                self._names = names

            def list_tools(self):
                return [type("T", (), {"name": n})() for n in self._names]

        names = [f"tool_{i}" for i in range(78)]
        listing = h.list_tools_listing(_FakeMgr(names))
        assert listing["count"] == 78, "must reflect real registry size"
        assert listing["count"] != "73+", "hardcoded fallback string leaked"
        assert "tool_0" in listing["names"]

    def test_render_shows_real_number(self):
        h = _load_helpers()
        text = h.render_list_tools({"count": 78, "names": [f"t{i}" for i in range(78)]})
        assert "78" in text
        assert "73+" not in text

    def test_source_has_more_tools_than_old_hardcoded_claim(self):
        # regression anchor: hardcoded said «73+», real is higher.
        assert self._real_tool_count() >= 74, "baseline claim of 73+ was already stale"


# ---------------------------------------------------------------------------
# M1 — --test must verify authorization, not «client is not None»
# ---------------------------------------------------------------------------
class TestTestProbeAuthorization:
    def test_revoked_session_fails(self):
        h = _load_helpers()

        class _Client:
            def __init__(self, authed):
                self._authed = authed
                self._connected = False

            async def connect(self):
                self._connected = True

            async def is_user_authorized(self):
                return self._authed

            def is_connected(self):
                return self._connected

            async def disconnect(self):
                self._connected = False

        import asyncio

        ok, _msg = asyncio.run(h.run_test_probe(_Client(authed=False)))
        assert ok is False, "revoked (not authorized) session must fail --test"

    def test_authorized_session_passes(self):
        h = _load_helpers()

        class _Client:
            async def connect(self):
                pass

            async def is_user_authorized(self):
                return True

            def is_connected(self):
                return True

            async def disconnect(self):
                pass

        import asyncio

        ok, _msg = asyncio.run(h.run_test_probe(_Client()))
        assert ok is True

    def test_none_client_fails(self):
        h = _load_helpers()
        import asyncio

        ok, _msg = asyncio.run(h.run_test_probe(None))
        assert ok is False


# ---------------------------------------------------------------------------
# M3 — healthcheck on a non-LABA endpoint must not return blind (True, "OK")
# ---------------------------------------------------------------------------
class TestHealthcheckNonLaba:
    def test_non_laba_revoked_is_not_true_ok(self):
        h = _load_helpers()

        class _Client:
            async def connect(self):
                pass

            async def is_user_authorized(self):
                return False  # revoked

            def is_connected(self):
                return True

            async def disconnect(self):
                pass

        import asyncio

        ok, msg = asyncio.run(h.run_runtime_healthcheck(laba_mode=False, client=_Client()))
        assert not (ok is True and msg == "OK"), "non-LABA + revoked must NOT be a blind green OK"
        assert ok is not True, f"revoked session must not report healthy, got ok={ok} msg={msg}"

    def test_non_laba_authorized_is_healthy_or_inconclusive(self):
        h = _load_helpers()

        class _Client:
            async def connect(self):
                pass

            async def is_user_authorized(self):
                return True

            def is_connected(self):
                return True

            async def disconnect(self):
                pass

        import asyncio

        ok, _msg = asyncio.run(h.run_runtime_healthcheck(laba_mode=False, client=_Client()))
        # authorized session on local endpoint → healthy (True) is acceptable.
        assert ok is True


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
