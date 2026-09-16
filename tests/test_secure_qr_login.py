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
        self.logged_out = False

    async def connect(self):
        pass

    async def qr_login(self):
        return self.qr

    async def sign_in(self, *, password):
        self.passwords.append(password)
        return SimpleNamespace(id=42, username="tester")

    async def log_out(self):
        self.logged_out = True

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
    installed = []
    rc, user = asyncio.run(login.qr_login(
        1, "hash", client_factory=lambda: client,
        render=lambda url, stream: rendered.append((url, stream)),
        ask_password=lambda prompt: "test-only-2fa", out=out,
        on_session=lambda session: installed.append((session, client.disconnected)) or 0,
    ))
    monkeypatch.undo()

    assert rc == 0 and user.id == 42
    assert installed == [(MARKER, False)]  # installed while the client is still connected
    assert [url for url, _ in rendered] == ["tg://login?token=first", "tg://login?token=recreated-1"]
    assert all(stream is out for _, stream in rendered)
    assert client.passwords == ["test-only-2fa"] and client.disconnected
    assert "test-only-2fa" not in out.getvalue() and MARKER not in out.getvalue()
    assert login.CLEAR_SCREEN in out.getvalue()
    assert list(tmp_path.iterdir()) == []


def test_ascii_qr_refuses_non_terminal_stream():
    pytest.importorskip("qrcode")
    with pytest.raises(OSError, match="Not a tty"):
        login.render_qr("tg://login?token=x", io.StringIO())


def test_session_reaches_installer_only_through_stdin(monkeypatch, capsys):
    carried = {"TELEGRAM_API_ID": "1", "TELEGRAM_API_HASH": "test-only-hash",
               "SUPABASE_DB_URL": "postgresql://test-only"}
    monkeypatch.setattr(login, "unit_state", lambda p: "failed")
    monkeypatch.setattr(login, "load_carried_values", lambda p: login.Inputs(carried, "legacy:/test", ()))

    async def _fake_login(api_id, api_hash, *, on_session, logout_on_failure):
        return on_session(MARKER), SimpleNamespace(id=42, username=None)

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
    assert argv[1:3] == ["-I", "-B"]
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
    (credstore / "telegram-mcp-ikrasinsky.env.cred").write_bytes(b"blob")
    (legacy / "ikrasinsky.env").write_bytes(b"TELEGRAM_API_ID=2\nTELEGRAM_API_HASH=h\nSUPABASE_DB_URL=u\n")
    monkeypatch.setattr(login, "CREDSTORE", credstore)
    monkeypatch.setattr(login, "LEGACY_ENV_DIR", legacy)
    monkeypatch.setattr(login.subprocess, "run", lambda argv, **kw: subprocess.CompletedProcess(argv, 1, b"", b""))
    with pytest.raises(login.Refused, match="cannot decrypt"):
        login.load_carried_values("ikrasinsky")

    (credstore / "telegram-mcp-ikrasinsky.env.cred").unlink()
    inputs = login.load_carried_values("ikrasinsky")
    assert inputs.source.startswith("legacy:")
    assert tuple(inputs.values) == ("TELEGRAM_API_ID", "TELEGRAM_API_HASH", "SUPABASE_DB_URL")
    (legacy / "ikrasinsky.env").write_bytes(b"TELEGRAM_API_ID=2\nTELEGRAM_SESSION_STRING=old\n")
    with pytest.raises(login.Refused,
                       match="missing required secret keys for profile ikrasinsky: TELEGRAM_API_HASH,SUPABASE_DB_URL"):
        login.load_carried_values("ikrasinsky")


# ---------------------------------------------------------------------------
# Profile key sets, --migrate-only, --dry-inputs, install retry, isolated wrapper
# ---------------------------------------------------------------------------

SECRET_VALUES = ("test-only-id-777", "test-only-hash-v", "test-only-db-url", "test-only-rick-k", "old-session-v")
LISA_LEGACY = (
    "TELEGRAM_USER=lisa\nLABA_MODE=1\nSUPABASE_URL=https://example.invalid\n"
    "LISA_TG_API_KEY=777\nLISA_TG_APP_HASH=test-only-hash-v\nLISA_TG_SESSION=old-session-v\n"
    "SUPABASE_DB_URL=test-only-db-url\nSUPABASE_RICK_API_KEY=test-only-rick-k\n"
    "TELEGRAM_API_ID=777\nTELEGRAM_API_HASH=test-only-hash-v\nTELEGRAM_SESSION_STRING=old-session-v\n"
).encode()
IK_LEGACY = (
    "TELEGRAM_USER=ikrasinsky\nLABA_MODE=1\nSUPABASE_URL=https://example.invalid\n"
    "TELEGRAM_API_ID=777\nTELEGRAM_API_HASH=test-only-hash-v\nTELEGRAM_SESSION_STRING=old-session-v\n"
    "SUPABASE_DB_URL=test-only-db-url\nSUPABASE_RICK_API_KEY=test-only-rick-k\n"
).encode()


@pytest.fixture
def stores(monkeypatch, tmp_path):
    credstore, legacy = tmp_path / "credstore", tmp_path / "legacy"
    credstore.mkdir()
    legacy.mkdir()
    (legacy / "lisa.env").write_bytes(LISA_LEGACY)
    (legacy / "ikrasinsky.env").write_bytes(IK_LEGACY)
    monkeypatch.setattr(login, "CREDSTORE", credstore)
    monkeypatch.setattr(login, "LEGACY_ENV_DIR", legacy)
    monkeypatch.setattr(login, "unit_state", lambda p: "active")  # lisa keeps running
    monkeypatch.setattr(login, "qr_login", lambda *a, **k: pytest.fail("Telegram contacted"))
    return SimpleNamespace(credstore=credstore, legacy=legacy)


def test_login_inputs_ikrasinsky_carry_rick_api_key_and_drop_nonsecret(stores):
    inputs = login.load_carried_values("ikrasinsky")
    assert tuple(inputs.values) == ("TELEGRAM_API_ID", "TELEGRAM_API_HASH", "SUPABASE_DB_URL",
                                    "SUPABASE_RICK_API_KEY")
    assert inputs.dropped_nonsecret == ("TELEGRAM_USER", "LABA_MODE", "SUPABASE_URL")


def test_login_inputs_lisa_carry_lisa_tg_keys_and_replace_both_session_names(stores):
    inputs = login.load_carried_values("lisa")
    assert {"LISA_TG_API_KEY", "LISA_TG_APP_HASH", "SUPABASE_RICK_API_KEY"} <= set(inputs.values)
    assert not {"LISA_TG_SESSION", "TELEGRAM_SESSION_STRING"} & set(inputs.values)
    payload = login.parse_dotenv(login.build_payload("lisa", inputs.values, MARKER))
    assert payload["LISA_TG_SESSION"] == payload["TELEGRAM_SESSION_STRING"] == MARKER


def test_unknown_key_in_source_refuses(stores):
    (stores.legacy / "ikrasinsky.env").write_bytes(IK_LEGACY + b"LISA_TG_SESSION=x\n")
    with pytest.raises(login.Refused, match="unknown secret keys for profile ikrasinsky: LISA_TG_SESSION"):
        login.load_carried_values("ikrasinsky")


def test_dry_inputs_prints_names_only_without_tty_telegram_or_install(stores, monkeypatch, capsys):
    monkeypatch.setattr(login, "install_payload", lambda *a: pytest.fail("install attempted"))
    for profile in ("ikrasinsky", "lisa"):
        assert login.main([profile, "--dry-inputs"], geteuid=lambda: 0, isatty=lambda fd: False) == 0
    out = capsys.readouterr()
    assert "carried_secret_keys=TELEGRAM_API_ID,TELEGRAM_API_HASH,SUPABASE_DB_URL,SUPABASE_RICK_API_KEY" in out.out
    assert "session_keys_replaced_by_login=LISA_TG_SESSION,TELEGRAM_SESSION_STRING" in out.out
    assert "telegram=not-contacted" in out.out
    for value in SECRET_VALUES:
        assert value not in out.out + out.err


def test_migrate_only_installs_legacy_without_telegram_and_refuses_existing(stores, monkeypatch, capsys):
    installs = []
    monkeypatch.setattr(login, "install_payload", lambda p, payload: installs.append((p, payload)) or 0)
    assert login.main(["lisa", "--migrate-only"], geteuid=lambda: 0, isatty=lambda fd: False) == 0
    (profile, payload), = installs
    values = login.parse_dotenv(payload)
    assert profile == "lisa" and values["LISA_TG_SESSION"] == "old-session-v"
    assert not {"TELEGRAM_USER", "LABA_MODE", "SUPABASE_URL"} & set(values)

    login.credential_path("lisa").write_bytes(b"existing")
    assert login.main(["lisa", "--migrate-only"], geteuid=lambda: 0, isatty=lambda fd: False) == 2
    assert "already exists; use --replace" in capsys.readouterr().err
    assert len(installs) == 1
    assert login.main(["lisa", "--migrate-only", "--replace"], geteuid=lambda: 0, isatty=lambda fd: False) == 0
    assert len(installs) == 2
    output = capsys.readouterr()
    for value in SECRET_VALUES:
        assert value not in output.out + output.err


def test_migrate_only_still_requires_root_and_rejects_replace_without_migrate(stores):
    assert login.main(["lisa", "--migrate-only"], geteuid=lambda: 1000, isatty=lambda fd: True) == 2
    with pytest.raises(SystemExit):
        login.main(["lisa", "--replace"], geteuid=lambda: 0, isatty=lambda fd: True)


def test_install_is_retried_once_from_memory(monkeypatch, capsys):
    monkeypatch.setattr(login, "unit_state", lambda p: "inactive")
    results = [1, 0]
    calls = []
    install = lambda p, payload: calls.append(payload) or results.pop(0)
    assert login.install_with_retry("ikrasinsky", b"payload", require_unit_stopped=True, install=install) == 0
    assert calls == [b"payload", b"payload"]
    assert "install_attempt=1/2 installer_rc=1" in capsys.readouterr().err


@pytest.mark.parametrize("logout_flag", [False, True])
def test_double_install_failure_reports_device_and_logs_out_only_by_flag(monkeypatch, capsys, logout_flag):
    errors = pytest.importorskip("telethon.errors")  # noqa: F841 - qr_login imports telethon.errors
    monkeypatch.setattr(login, "unit_state", lambda p: "inactive")
    client = _FakeClient(_FakeQr([SimpleNamespace(id=42, username=None)]))
    calls = []

    def on_session(session):
        return login.install_with_retry("ikrasinsky", session.encode(), require_unit_stopped=True,
                                        install=lambda p, payload: calls.append(client.disconnected) or 1)

    rc, _ = asyncio.run(login.qr_login(
        1, "hash", client_factory=lambda: client, render=lambda url, stream: None,
        out=io.StringIO(), on_session=on_session, logout_on_failure=logout_flag,
    ))
    err = capsys.readouterr().err
    assert rc != 0 and calls == [False, False] and client.disconnected
    assert client.logged_out is logout_flag
    if logout_flag:
        assert "session_logout=done" in err
    else:
        assert login.MANUAL_DEVICE_CLEANUP in err
    assert MARKER not in err


def test_wrapper_uses_root_venv_isolated_python_and_scrubbed_env():
    wrapper = (DEPLOY / "telegram-secure-qr-login").read_text()
    command = " ".join(line.rstrip("\\").strip() for line in wrapper.splitlines() if not line.startswith("#"))
    assert command.startswith("exec /usr/bin/env -i PATH=/usr/sbin:/usr/bin:/sbin:/bin ")
    assert "/usr/local/lib/telegram-secure-qr-login/venv/bin/python -I -B " in command
    assert "/home/" not in command
    requirements = (DEPLOY / "secure-qr-login-requirements.txt").read_text()
    assert "Telethon==1.44.0" in requirements and "qrcode==" in requirements


def test_isolated_interpreter_ignores_pythonpath_cwd_and_script_dir(tmp_path):
    probe = tmp_path / "probe.py"
    probe.write_text("import json, sys; print(json.dumps(sys.path))")
    evil = tmp_path / "home" / "idkras" / "evil"
    evil.mkdir(parents=True)
    result = subprocess.run(
        [sys.executable, "-I", "-B", str(probe)], cwd=evil, capture_output=True, text=True, check=True,
        env={**os.environ, "PYTHONPATH": str(evil), "PYTHONSTARTUP": str(evil / "x.py")},
    )
    paths = __import__("json").loads(result.stdout)
    assert str(evil) not in paths and str(tmp_path) not in paths and "" not in paths


def test_untrusted_sys_path_and_tree_detection(tmp_path):
    assert login.untrusted_sys_path_entries(
        ["/usr/lib/python3.12", "", "/home/idkras/telegram-mcp/.venv/lib", "rel", "/tmp/x"]
    ) == ["", "/home/idkras/telegram-mcp/.venv/lib", "rel", "/tmp/x"]
    if os.geteuid() != 0:
        (tmp_path / "x.pth").write_text("")
        assert str(tmp_path / "x.pth") in login.writable_by_non_root([tmp_path])
