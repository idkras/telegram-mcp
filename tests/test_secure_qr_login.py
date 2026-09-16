"""Contract tests for deploy/secure-qr-login.py (no Telegram, no root, no systemd)."""

from __future__ import annotations

import asyncio
import builtins
import importlib.util
import io
import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest


ROOT = Path(__file__).resolve().parents[1]
DEPLOY = ROOT / "deploy"
sys.path.insert(0, str(DEPLOY))

spec = importlib.util.spec_from_file_location("secure_qr_login", DEPLOY / "secure-qr-login.py")
assert spec and spec.loader
login = importlib.util.module_from_spec(spec)
spec.loader.exec_module(login)

MARKER = "test-only-session-marker-7f3a"


def _root_tty(**overrides):
    return {"geteuid": lambda: 0, "isatty": lambda fd: True, **overrides}


@pytest.fixture
def forbid_side_effects(monkeypatch):
    calls = []
    monkeypatch.setattr(login, "load_carried_values", lambda *a: calls.append("load") or pytest.fail("loaded"))
    monkeypatch.setattr(login, "qr_login", lambda *a, **k: pytest.fail("login attempted"))
    monkeypatch.setattr(login, "install_payload", lambda *a: pytest.fail("install attempted"))
    return calls


def test_refuses_without_tty(forbid_side_effects, capsys):
    assert login.main(["ikrasinsky"], geteuid=lambda: 0, isatty=lambda fd: fd != 0) == 2
    assert login.main(["ikrasinsky"], geteuid=lambda: 0, isatty=lambda fd: fd != 1) == 2
    assert "TTY required" in capsys.readouterr().err


def test_refuses_non_root(forbid_side_effects, capsys):
    assert login.main(["ikrasinsky"], **_root_tty(geteuid=lambda: 1000)) == 2
    assert "must run as root" in capsys.readouterr().err


@pytest.mark.parametrize("profile", ["mallory", "../lisa", "Lisa", "lisa;id", "selftest"])
def test_refuses_unknown_profile(forbid_side_effects, monkeypatch, profile):
    monkeypatch.setattr(login, "unit_state", lambda p: pytest.fail("unit queried"))
    assert login.main([profile], **_root_tty()) == 2


def test_selftest_profile_only_allowed_in_self_test_mode(forbid_side_effects, monkeypatch):
    monkeypatch.setattr(login, "unit_state", lambda p: "inactive")
    monkeypatch.setattr(login, "run_self_test", lambda: None)
    assert login.main(["selftest", "--self-test"], **_root_tty()) == 0
    assert login.main(["lisa", "--self-test"], **_root_tty()) == 2


@pytest.mark.parametrize("state", ["active", "activating", "reloading", "deactivating", "unknown"])
def test_refuses_when_unit_is_not_stopped(forbid_side_effects, monkeypatch, capsys, state):
    monkeypatch.setattr(login, "unit_state", lambda p: state)
    assert login.main(["lisa"], **_root_tty()) == 2
    assert f"telegram-mcp-lisa.service is {state}" in capsys.readouterr().err


class _FakeQr:
    def __init__(self, outcomes):
        self._outcomes = list(outcomes)
        self.url = "tg://login?token=first"
        self.expires = __import__("datetime").datetime(2026, 9, 16, 12, 0, 0)
        self.recreated = 0

    async def wait(self):
        outcome = self._outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    async def recreate(self):
        self.recreated += 1
        self.url = f"tg://login?token=recreated-{self.recreated}"


class _FakeClient:
    def __init__(self, qr):
        self.qr = qr
        self.session = SimpleNamespace(save=lambda: MARKER)
        self.passwords = []
        self.disconnected = False

    async def connect(self):
        pass

    async def qr_login(self):
        return self.qr

    async def sign_in(self, *, password):
        self.passwords.append(password)
        return SimpleNamespace(id=42, username="tester")

    async def disconnect(self):
        self.disconnected = True


def test_qr_is_rendered_to_terminal_only_and_never_written_to_a_file(monkeypatch, tmp_path):
    errors = pytest.importorskip("telethon.errors")
    monkeypatch.chdir(tmp_path)
    qr = _FakeQr([asyncio.TimeoutError(), errors.SessionPasswordNeededError(request=None)])
    client = _FakeClient(qr)
    rendered = []

    def _forbidden(*args, **kwargs):
        raise AssertionError("file write attempted during QR login")

    monkeypatch.setattr(builtins, "open", _forbidden)
    monkeypatch.setattr(os, "open", _forbidden)
    monkeypatch.setattr(Path, "write_bytes", _forbidden)
    monkeypatch.setattr(Path, "write_text", _forbidden)
    out = io.StringIO()
    session, user = asyncio.run(login.qr_login(
        1, "hash", client_factory=lambda: client,
        render=lambda url, stream: rendered.append((url, stream)),
        ask_password=lambda prompt: "test-only-2fa", out=out,
    ))
    monkeypatch.undo()

    assert session == MARKER and user.id == 42
    assert [url for url, _ in rendered] == ["tg://login?token=first", "tg://login?token=recreated-1"]
    assert all(stream is out for _, stream in rendered)
    assert client.passwords == ["test-only-2fa"] and client.disconnected
    assert "test-only-2fa" not in out.getvalue() and MARKER not in out.getvalue()
    assert out.getvalue().endswith(login.CLEAR_SCREEN)
    assert list(tmp_path.iterdir()) == []


def test_ascii_qr_refuses_non_terminal_stream():
    pytest.importorskip("qrcode")
    with pytest.raises(OSError, match="Not a tty"):
        login.render_qr("tg://login?token=x", io.StringIO())


def test_session_reaches_installer_only_through_stdin(monkeypatch, capsys):
    carried = {"TELEGRAM_API_ID": "1", "TELEGRAM_API_HASH": "test-only-hash",
               "SUPABASE_DB_URL": "postgresql://test-only"}
    monkeypatch.setattr(login, "unit_state", lambda p: "failed")
    monkeypatch.setattr(login, "load_carried_values", lambda p: (carried, "legacy:/test"))

    async def _fake_login(api_id, api_hash):
        return MARKER, SimpleNamespace(id=42, username=None)

    monkeypatch.setattr(login, "qr_login", _fake_login)
    runs = []

    def _fake_run(argv, **kwargs):
        runs.append((argv, kwargs))
        return subprocess.CompletedProcess(argv, 0, b"profile=ikrasinsky readback=match\n", b"")

    monkeypatch.setattr(login.subprocess, "run", _fake_run)
    assert login.main(["ikrasinsky"], **_root_tty()) == 0

    installs = [(argv, kw) for argv, kw in runs if str(login.INSTALLER) in argv]
    assert len(installs) == 1
    argv, kwargs = installs[0]
    assert argv[-2:] == ["--profile", "ikrasinsky"]
    assert MARKER not in " ".join(map(str, argv))
    assert set(kwargs) >= {"input"} and "env" not in kwargs
    assert f'TELEGRAM_SESSION_STRING="{MARKER}"'.encode() in kwargs["input"]
    output = capsys.readouterr()
    assert MARKER not in output.out + output.err
    assert "unit_restart=not-performed" in output.out
    assert not any(a[0] == "systemctl" and a[1] != "is-active" for a, _ in runs)


def test_inputs_come_from_encrypted_credential_without_legacy_fallback(monkeypatch, tmp_path):
    credstore, legacy = tmp_path / "credstore", tmp_path / "legacy"
    credstore.mkdir()
    legacy.mkdir()
    (credstore / "telegram-mcp-lisa.env.cred").write_bytes(b"blob")
    (legacy / "lisa.env").write_bytes(b"TELEGRAM_API_ID=2\nTELEGRAM_API_HASH=h\nSUPABASE_DB_URL=u\n")
    monkeypatch.setattr(login, "CREDSTORE", credstore)
    monkeypatch.setattr(login, "LEGACY_ENV_DIR", legacy)
    monkeypatch.setattr(login.subprocess, "run", lambda argv, **kw: subprocess.CompletedProcess(argv, 1, b"", b""))
    with pytest.raises(login.Refused, match="cannot decrypt"):
        login.load_carried_values("lisa")

    (credstore / "telegram-mcp-lisa.env.cred").unlink()
    carried, source = login.load_carried_values("lisa")
    assert source.startswith("legacy:") and tuple(carried) == login.REQUIRED_CARRIED_KEYS
    (legacy / "lisa.env").write_bytes(b"TELEGRAM_API_ID=2\nTELEGRAM_SESSION_STRING=old\n")
    with pytest.raises(login.Refused, match="missing keys TELEGRAM_API_HASH,SUPABASE_DB_URL"):
        login.load_carried_values("lisa")
