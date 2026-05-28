"""Unit tests for scripts/session_health_monitor.py (pure functions).

Runtime monitor that surfaces a dying Telegram session BEFORE delivery fails
(RCA 2026-05-28). Tests cover the classification + dead-detection + rendering
contract without telethon / network / Keychain.

What is asserted:
  - classify() maps test_session diagnosis prefixes to canonical reason codes.
  - NETWORK is transient (NOT counted as a dead channel) — else flaky network
    would page the owner on every cron tick.
  - AUTHKEY_DUPLICATED / REVOKED / NO_SESSION / UNKNOWN are dead.
  - dead_profiles() filters to dead, non-OK, non-NETWORK rows.
  - render_table() marks the rows correctly.
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

_SCRIPTS = Path(__file__).resolve().parent.parent / "scripts"
_SPEC = importlib.util.spec_from_file_location(
    "session_health_monitor", _SCRIPTS / "session_health_monitor.py"
)
assert _SPEC and _SPEC.loader
shm = importlib.util.module_from_spec(_SPEC)
sys.modules["session_health_monitor"] = shm
_SPEC.loader.exec_module(shm)


# --- classify -------------------------------------------------------------------

def test_classify_authkey_duplicated():
    assert shm.classify("AUTHKEY_DUPLICATED: key used from 2 IPs") == "AUTHKEY_DUPLICATED"


def test_classify_revoked():
    assert shm.classify("REVOKED: session terminated by user") == "REVOKED"


def test_classify_network_is_transient():
    assert shm.classify("NETWORK: connection timeout") == "NETWORK"


def test_classify_ok():
    assert shm.classify("OK: connected as Lisa") == "OK"


def test_classify_no_session():
    assert shm.classify("NO_SESSION: keychain empty") == "NO_SESSION"


def test_classify_none_is_unknown():
    assert shm.classify(None) == "UNKNOWN"


def test_classify_unrecognized_prefix_is_unknown():
    assert shm.classify("WEIRD: something") == "UNKNOWN"


def test_classify_case_insensitive():
    assert shm.classify("authkey_duplicated: lower") == "AUTHKEY_DUPLICATED"


# --- dead_profiles --------------------------------------------------------------

def _r(profile, ok, code, detail=""):
    return {"profile": profile, "ok": ok, "code": code, "detail": detail}


def test_dead_profiles_filters_only_dead():
    results = [
        _r("lisa", False, "AUTHKEY_DUPLICATED"),
        _r("ik", True, "OK"),
        _r("default", False, "NETWORK"),       # transient — NOT dead
        _r("teammate", False, "REVOKED"),
    ]
    dead = shm.dead_profiles(results)
    names = {d["profile"] for d in dead}
    assert names == {"lisa", "teammate"}, "NETWORK must not count as dead; OK excluded"


def test_dead_profiles_empty_when_all_ok():
    results = [_r("lisa", True, "OK"), _r("ik", True, "OK")]
    assert shm.dead_profiles(results) == []


def test_dead_codes_contract():
    # Guard against accidental removal of a dead code from the contract.
    assert set(shm.DEAD_CODES) == {"REVOKED", "AUTHKEY_DUPLICATED", "NO_SESSION", "UNKNOWN"}


# --- render_table ---------------------------------------------------------------

def test_render_table_marks():
    results = [
        _r("lisa", False, "AUTHKEY_DUPLICATED", "key from 2 IPs"),
        _r("ik", True, "OK", "connected"),
        _r("default", False, "NETWORK", "timeout"),
    ]
    table = shm.render_table(results)
    assert "| profile | status | reason |" in table
    assert "AUTHKEY_DUPLICATED" in table
    assert "OK" in table
    assert "NETWORK" in table


def test_render_table_truncates_long_detail():
    long_detail = "x" * 200
    table = shm.render_table([_r("lisa", False, "UNKNOWN", long_detail)])
    # detail truncated to 90 chars in the rendered row
    assert "x" * 90 in table
    assert "x" * 91 not in table
