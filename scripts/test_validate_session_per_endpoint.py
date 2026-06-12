#!/usr/bin/env python3
"""Tests for validate_session_per_endpoint (RCA 2026-05-28 AuthKeyDuplicated)."""

from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
import tempfile
from pathlib import Path

HOOK = Path(__file__).resolve().parent / "validate_session_per_endpoint.py"
spec = importlib.util.spec_from_file_location("vspe", HOOK)
vspe = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(vspe)


def _write_env(session: str | None) -> str:
    fd, p = tempfile.mkstemp(suffix=".env")
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        fh.write("TELEGRAM_API_ID=12345\n")
        if session is not None:
            fh.write(f"TELEGRAM_SESSION_STRING={session}\n")
    return p


def test_detect_collision_when_env_matches_keychain():
    kc = {"lisa_tg_session": vspe._sha("SESSION_ABC"), "telegram_session": vspe._sha("OTHER")}
    env_sha = vspe._sha("SESSION_ABC")
    collision, account = vspe.detect_collision(env_sha, kc)
    assert collision is True and account == "lisa_tg_session"


def test_no_collision_when_distinct():
    kc = {"lisa_tg_session": vspe._sha("LOCAL_ONE")}
    env_sha = vspe._sha("LABA_SEPARATE")
    collision, account = vspe.detect_collision(env_sha, kc)
    assert collision is False and account == ""


def test_no_collision_when_env_none():
    kc = {"lisa_tg_session": vspe._sha("X")}
    assert vspe.detect_collision(None, kc) == (False, "")


def test_read_env_session_returns_sha_not_secret():
    p = _write_env("SUPERSECRETSTRING123")
    sha = vspe.read_env_session(p)
    assert sha == vspe._sha("SUPERSECRETSTRING123")
    assert "SUPERSECRETSTRING123" not in sha  # это хэш, не секрет
    os.unlink(p)


def test_read_env_missing_session_returns_none():
    p = _write_env(None)
    assert vspe.read_env_session(p) is None
    os.unlink(p)


def test_read_env_absent_file_returns_none():
    assert vspe.read_env_session("/nonexistent/.env.laba") is None


# ---------- end-to-end via subprocess ----------
def _run(env_path: str, env_extra: dict | None = None) -> tuple[int, str]:
    env = dict(os.environ)
    env.pop("TG_SESSION_REUSE_ACK", None)
    if env_extra:
        env.update(env_extra)
    proc = subprocess.run(
        [sys.executable, str(HOOK), "--env-path", env_path],
        capture_output=True,
        text=True,
        env=env,
    )
    return proc.returncode, proc.stderr


def test_e2e_no_session_in_env_passes():
    p = _write_env(None)
    rc, _ = _run(p)
    assert rc == 0
    os.unlink(p)


def test_e2e_secret_never_printed():
    # даже при collision/no-collision в stderr только sha-префикс, не секрет
    p = _write_env("PLAINTEXT_SECRET_THAT_MUST_NOT_LEAK")
    rc, err = _run(p)
    assert "PLAINTEXT_SECRET_THAT_MUST_NOT_LEAK" not in err
    os.unlink(p)


def test_e2e_ack_env_passes_even_on_collision(monkeypatch=None):
    # ACK всегда pass (exit 0) до сравнения
    p = _write_env("ANY")
    rc, _ = _run(p, {"TG_SESSION_REUSE_ACK": "intentional shared session for test rig"})
    assert rc == 0
    os.unlink(p)


# ---------- review-fix tests (RCA 2026-05-28 reviewer findings) ----------
def test_quote_symmetry_keychain_vs_env():
    # H2: quoted keychain value vs unquoted env value = same logical session
    assert vspe._sha('"SESSION_X"') == vspe._sha("SESSION_X")
    assert vspe._sha("'SESSION_X'") == vspe._sha("SESSION_X")


def test_trailing_newline_whitespace_normalized():
    assert vspe._sha("SESSION_X\n") == vspe._sha("SESSION_X")
    assert vspe._sha("  SESSION_X  ") == vspe._sha("SESSION_X")


def test_export_prefix_parsed():
    # H3: `export TELEGRAM_SESSION_STRING=...` must be read (manual .env edit)
    fd, p = tempfile.mkstemp(suffix=".env")
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        fh.write("export TELEGRAM_SESSION_STRING=SESSABC\n")
    assert vspe.read_env_session(p) == vspe._sha("SESSABC")
    os.unlink(p)


def test_inline_comment_stripped():
    # H3: `X=val # laba` — comment must not change the hash
    fd, p = tempfile.mkstemp(suffix=".env")
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        fh.write("TELEGRAM_SESSION_STRING=SESSABC # laba endpoint\n")
    assert vspe.read_env_session(p) == vspe._sha("SESSABC")
    os.unlink(p)


def test_collision_detected_despite_quote_asymmetry():
    # end-to-end of H2: env quoted, keychain-side unquoted → still collision
    kc = {"lisa_tg_session": vspe._sha("SESSABC")}
    env_sha = vspe.read_env_session(_write_env('"SESSABC"'))
    collision, account = vspe.detect_collision(env_sha, kc)
    assert collision is True and account == "lisa_tg_session"


# ---------- session_health_monitor classify ----------
def _load_monitor():
    mp = Path(__file__).resolve().parent / "session_health_monitor.py"
    s = importlib.util.spec_from_file_location("shm", mp)
    m = importlib.util.module_from_spec(s)
    s.loader.exec_module(m)
    return m


def test_monitor_classify_codes():
    m = _load_monitor()
    assert m.classify("AUTHKEY_DUPLICATED: ...") == "AUTHKEY_DUPLICATED"
    assert m.classify("REVOKED: logged out") == "REVOKED"
    assert m.classify("NETWORK: timeout") == "NETWORK"
    assert m.classify(None) == "UNKNOWN"
    assert m.classify("weird text no colon") == "UNKNOWN"


def test_monitor_dead_excludes_network():
    m = _load_monitor()
    results = [
        {"profile": "lisa", "ok": False, "code": "AUTHKEY_DUPLICATED", "detail": ""},
        {"profile": "ik", "ok": False, "code": "NETWORK", "detail": ""},
        {"profile": "default", "ok": True, "code": "OK", "detail": ""},
    ]
    dead = m.dead_profiles(results)
    assert [d["profile"] for d in dead] == ["lisa"]  # NETWORK не считается смертью


if __name__ == "__main__":
    sys.exit(subprocess.call([sys.executable, "-m", "pytest", __file__, "-v"]))
