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
            fh.write(f'TELEGRAM_SESSION_STRING={session}\n')
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
        capture_output=True, text=True, env=env,
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


if __name__ == "__main__":
    sys.exit(subprocess.call([sys.executable, "-m", "pytest", __file__, "-v"]))
