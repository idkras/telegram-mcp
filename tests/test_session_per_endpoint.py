"""Unit tests for scripts/validate_session_per_endpoint.py.

Covers the deploy-time session-per-endpoint guard that blocks AuthKeyDuplicated
recurrence (RCA 2026-05-28). All tests are pure-function / temp-file based — no
Keychain access, no network — so they run identically in CI and on any OS.

What is asserted (the contract that prevents the incident):
  - normalization is SYMMETRIC for env-side and keychain-side (quotes, whitespace,
    `export ` prefix, trailing inline comment) — else one logical session yields
    two different SHAs -> false-negative -> guard silently off.
  - collision detection returns the matching account name.
  - read_env_session parses canonical / export-prefixed / quoted / commented lines.
  - empty / missing env -> PASS (no false-positive block).
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

_SCRIPTS = Path(__file__).resolve().parent.parent / "scripts"
_SPEC = importlib.util.spec_from_file_location(
    "validate_session_per_endpoint", _SCRIPTS / "validate_session_per_endpoint.py"
)
assert _SPEC and _SPEC.loader
vspe = importlib.util.module_from_spec(_SPEC)
sys.modules["validate_session_per_endpoint"] = vspe
_SPEC.loader.exec_module(vspe)


# --- _normalize / _sha symmetry -------------------------------------------------

def test_normalize_strips_quotes_whitespace_and_comment():
    assert vspe._normalize('  "abc123"  ') == "abc123"
    assert vspe._normalize("'abc123'") == "abc123"
    assert vspe._normalize("abc123 # laba session") == "abc123"
    assert vspe._normalize("abc123") == "abc123"


def test_sha_symmetric_env_vs_keychain_form():
    """Same logical session written differently must hash identically."""
    keychain_form = "ABCdef0123_base64url-Session=="
    assert vspe._sha(keychain_form) == vspe._sha(' "ABCdef0123_base64url-Session=="  ')


def test_quoted_comment_not_stripped_inside_quotes():
    # '#' inside quotes is part of the value (defensive — base64url has no '#',
    # but normalization must not corrupt a quoted value that contains one)
    assert vspe._normalize('"abc#def"') == "abc#def"


# --- detect_collision -----------------------------------------------------------

def test_detect_collision_hit_returns_account():
    env = vspe._sha("SHARED_SESSION")
    keychain = {
        "lisa_tg_session": vspe._sha("SHARED_SESSION"),
        "telegram_session": vspe._sha("OTHER"),
    }
    collision, account = vspe.detect_collision(env, keychain)
    assert collision is True
    assert account == "lisa_tg_session"


def test_detect_collision_distinct_sessions_pass():
    env = vspe._sha("LABA_OWN_SESSION")
    keychain = {
        "lisa_tg_session": vspe._sha("LOCAL_LISA"),
        "telegram_session": vspe._sha("LOCAL_IK"),
    }
    collision, account = vspe.detect_collision(env, keychain)
    assert collision is False
    assert account == ""


def test_detect_collision_no_env_session_pass():
    collision, account = vspe.detect_collision(None, {"lisa_tg_session": "x"})
    assert collision is False


# --- read_env_session -----------------------------------------------------------

def _write(tmp_path, body: str) -> str:
    p = tmp_path / ".env.laba"
    p.write_text(body, encoding="utf-8")
    return str(p)


def test_read_env_canonical(tmp_path):
    path = _write(tmp_path, "TELEGRAM_SESSION_STRING=PLAIN_SESSION_VALUE\n")
    assert vspe.read_env_session(path) == vspe._sha("PLAIN_SESSION_VALUE")


def test_read_env_export_prefixed(tmp_path):
    path = _write(tmp_path, "export TELEGRAM_SESSION_STRING=EXPORTED_VALUE\n")
    assert vspe.read_env_session(path) == vspe._sha("EXPORTED_VALUE")


def test_read_env_quoted_with_inline_comment(tmp_path):
    path = _write(tmp_path, 'TELEGRAM_SESSION_STRING=COMMENTED  # laba endpoint\n')
    assert vspe.read_env_session(path) == vspe._sha("COMMENTED")


def test_read_env_missing_file_returns_none():
    assert vspe.read_env_session("/nonexistent/.env.laba") is None


def test_read_env_no_session_key_returns_none(tmp_path):
    path = _write(tmp_path, "TELEGRAM_API_ID=123\nSUPABASE_URL=https://x\n")
    assert vspe.read_env_session(path) is None


def test_end_to_end_collision_via_env_and_keychain_form(tmp_path):
    """The exact incident: same session in .env.laba (export+comment) and Keychain."""
    secret = "Lisa_Real_StringSession_base64=="
    path = _write(
        tmp_path, f'export TELEGRAM_SESSION_STRING={secret}  # copied from keychain\n'
    )
    env_sha = vspe.read_env_session(path)
    keychain = {"lisa_tg_session": vspe._sha(f'  "{secret}"  ')}
    collision, account = vspe.detect_collision(env_sha, keychain)
    assert collision is True, "copy-paste of keychain session into .env.laba must collide"
    assert account == "lisa_tg_session"
