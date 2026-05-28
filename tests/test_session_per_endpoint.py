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


# --- H1 fix: inline comment WITHOUT leading space (code-reviewer 2026-05-28) ----

def test_normalize_strips_comment_without_leading_space():
    # human-edited `.env` `KEY=val#c` must normalize to `val`, else false-negative
    assert vspe._normalize("abc123#laba") == "abc123"


def test_read_env_collision_with_no_space_comment(tmp_path):
    secret = "SHARED=="
    path = _write(tmp_path, f"TELEGRAM_SESSION_STRING={secret}#laba\n")
    env_sha = vspe.read_env_session(path)
    keychain = {"lisa_tg_session": vspe._sha(secret)}
    collision, account = vspe.detect_collision(env_sha, keychain)
    assert collision is True, "no-space inline comment must not hide a collision"


# --- D1/H2 fix: enumeration None => INCONCLUSIVE, never silent false-PASS -------

def test_detect_collision_none_keychain_is_not_collision():
    # None = couldn't enumerate; detect_collision must NOT crash and NOT claim collision
    collision, account = vspe.detect_collision(vspe._sha("X"), None)
    assert collision is False and account == ""


def test_main_inconclusive_when_keychain_unavailable(tmp_path, monkeypatch, capsys):
    """laba host (no macOS Keychain): enumerate -> None -> INCONCLUSIVE, NOT PASS."""
    path = _write(tmp_path, "TELEGRAM_SESSION_STRING=LABA_OWN==\n")
    monkeypatch.setattr(vspe, "enumerate_keychain_sessions", lambda: None)
    monkeypatch.setattr(sys, "argv", ["prog", "--env-path", path])
    monkeypatch.delenv("TG_SESSION_GUARD_STRICT", raising=False)
    monkeypatch.delenv("TG_SESSION_REUSE_ACK", raising=False)
    rc = vspe.main()
    err = capsys.readouterr().err
    assert rc == 0, "non-strict: INCONCLUSIVE must not block deploy"
    assert "INCONCLUSIVE" in err
    assert "PASS" not in err, "must NOT print a false PASS when enumeration impossible"


def test_main_inconclusive_strict_blocks(tmp_path, monkeypatch):
    path = _write(tmp_path, "TELEGRAM_SESSION_STRING=LABA_OWN==\n")
    monkeypatch.setattr(vspe, "enumerate_keychain_sessions", lambda: None)
    monkeypatch.setattr(sys, "argv", ["prog", "--env-path", path, "--strict"])
    monkeypatch.delenv("TG_SESSION_REUSE_ACK", raising=False)
    assert vspe.main() == 2, "strict mode: INCONCLUSIVE must fail-closed (exit 2)"


def test_main_collision_exits_2(tmp_path, monkeypatch):
    secret = "DUP=="
    path = _write(tmp_path, f"TELEGRAM_SESSION_STRING={secret}\n")
    monkeypatch.setattr(
        vspe, "enumerate_keychain_sessions",
        lambda: {"lisa_tg_session": vspe._sha(secret)},
    )
    monkeypatch.setattr(sys, "argv", ["prog", "--env-path", path])
    monkeypatch.delenv("TG_SESSION_REUSE_ACK", raising=False)
    monkeypatch.delenv("TG_SESSION_GUARD_STRICT", raising=False)
    assert vspe.main() == 2, "real collision must abort deploy (exit 2)"


def test_main_distinct_exits_0(tmp_path, monkeypatch):
    path = _write(tmp_path, "TELEGRAM_SESSION_STRING=LABA_OWN==\n")
    monkeypatch.setattr(
        vspe, "enumerate_keychain_sessions",
        lambda: {"lisa_tg_session": vspe._sha("LOCAL_DIFFERENT==")},
    )
    monkeypatch.setattr(sys, "argv", ["prog", "--env-path", path])
    monkeypatch.delenv("TG_SESSION_REUSE_ACK", raising=False)
    monkeypatch.delenv("TG_SESSION_GUARD_STRICT", raising=False)
    assert vspe.main() == 0, "distinct sessions pass"


def test_main_no_env_session_exits_0(tmp_path, monkeypatch):
    path = _write(tmp_path, "TELEGRAM_API_ID=1\n")
    monkeypatch.setattr(vspe, "enumerate_keychain_sessions", lambda: {"x": "y"})
    monkeypatch.setattr(sys, "argv", ["prog", "--env-path", path])
    monkeypatch.delenv("TG_SESSION_REUSE_ACK", raising=False)
    monkeypatch.delenv("TG_SESSION_GUARD_STRICT", raising=False)
    assert vspe.main() == 0


def test_main_ack_override_exits_0(tmp_path, monkeypatch):
    secret = "DUP=="
    path = _write(tmp_path, f"TELEGRAM_SESSION_STRING={secret}\n")
    monkeypatch.setattr(
        vspe, "enumerate_keychain_sessions",
        lambda: {"lisa_tg_session": vspe._sha(secret)},
    )
    monkeypatch.setattr(sys, "argv", ["prog", "--env-path", path])
    monkeypatch.setenv("TG_SESSION_REUSE_ACK", "intentional reuse for x reason")
    assert vspe.main() == 0, "ACK override bypasses even a real collision"
