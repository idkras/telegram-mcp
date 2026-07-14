"""R1 deploy-foundation experiment (pr-hero-xf6): prove the systemd deploy is valid
and idempotent BEFORE it runs on sandbox-ik. No live VPS needed — validates the
unit render, script syntax, and dry-run determinism."""
import re
import os
import subprocess
import sys
from pathlib import Path

import yaml

DEPLOY = Path(__file__).resolve().parents[1] / "deploy"
TEMPLATE = DEPLOY / "telegram-mcp.service.template"
SCRIPT = DEPLOY / "deploy-sandbox-ik.sh"


def _render(profile="ikrasinsky", user="idkras", app="/home/idkras/telegram-mcp"):
    t = TEMPLATE.read_text()
    return (t.replace("__PROFILE__", profile).replace("__USER__", user)
             .replace("__APP_DIR__", app))


def test_unit_renders_valid_systemd():
    u = _render("ikrasinsky")
    for section in ("[Unit]", "[Service]", "[Install]"):
        assert section in u, section
    assert "ExecStart=/home/idkras/telegram-mcp/.venv/bin/python listener.py" in u
    assert "EnvironmentFile=/etc/telegram-mcp/env.d/ikrasinsky.env" in u
    assert "Environment=PYTHONPATH=/home/idkras/telegram-mcp" in u
    assert "WantedBy=multi-user.target" in u
    assert "SyslogIdentifier=telegram-mcp-ikrasinsky" in u  # journald, not /app/logs
    assert "Restart=on-failure" in u
    assert "TimeoutStopSec" in u                             # graceful shutdown


def test_unit_no_unrendered_placeholders():
    u = _render("lisa")
    left = re.findall(r"__[A-Z_]+__", u)
    assert left == [], f"unrendered placeholders: {left}"
    assert "telegram-mcp-lisa" in u  # profile rendered
    # user is supplied via EnvironmentFile, not a hardcoded unit directive
    assert not re.search(r"^TELEGRAM_USER=", u, re.M)


def test_deploy_script_bash_syntax_ok():
    r = subprocess.run(["bash", "-n", str(SCRIPT)], capture_output=True, text=True)
    assert r.returncode == 0, r.stderr


def test_deploy_dry_run_is_idempotent_and_touches_nothing():
    """Dry-run twice → identical output (deterministic) and never calls sudo for real."""
    def dry():
        r = subprocess.run(["bash", str(SCRIPT), "--dry-run", "--profiles", "ikrasinsky,lisa"],
                           capture_output=True, text=True)
        return r.returncode, r.stdout
    rc1, out1 = dry()
    rc2, out2 = dry()
    assert rc1 == 0 and rc2 == 0
    assert out1 == out2, "dry-run not deterministic"
    # both profile units + both env skeletons appear
    assert "telegram-mcp-ikrasinsky.service" in out1
    assert "telegram-mcp-lisa.service" in out1
    assert "env.d/ikrasinsky.env" in out1 and "env.d/lisa.env" in out1
    # dry-run must not have executed a real systemctl start
    assert "DRY: sudo systemctl daemon-reload" in out1


def test_env_example_has_required_keys_and_no_secrets():
    ex = (DEPLOY / "env.d" / "profile.env.example").read_text()
    for k in ("TELEGRAM_USER", "LABA_MODE", "TELEGRAM_API_ID", "TELEGRAM_SESSION_STRING",
              "SUPABASE_DB_URL"):
        assert re.search(rf"^{k}=", ex, re.M), k
    # no actual secret values committed (all keys empty or a public URL)
    for line in ex.splitlines():
        if line.startswith("TELEGRAM_SESSION_STRING="):
            assert line.strip() == "TELEGRAM_SESSION_STRING="  # empty, filled on VPS


def test_deploy_installs_declared_dependencies_and_standalone_adapter():
    script = SCRIPT.read_text()
    assert "-r '$APP_DIR/requirements.txt' -r '$APP_DIR/requirements-laba.txt'" in script
    assert "deploy/standalone/heroes_platform" in script
    standalone = DEPLOY / "standalone"
    for rel in (
        "heroes_harness/credentials_registry.yaml",
        "heroes_platform/credentials/__init__.py",
        "heroes_platform/credentials/service_env.py",
        "heroes_platform/shared/import_setup.py",
        "heroes_platform/shared/logging_utils.py",
    ):
        assert (standalone / rel).is_file(), rel
    assert not (standalone / "heroes_platform/shared/credentials_wrapper.py").exists()

    assert "telegram-mcp-backfill@.service" in script
    assert "telegram-mcp-backfill@${p}.timer" in script


def test_standalone_registry_declares_every_listener_secret_id_and_rejects_unknown() -> None:
    registry = DEPLOY / "standalone/heroes_harness/credentials_registry.yaml"
    document = yaml.safe_load(registry.read_text(encoding="utf-8"))
    assert document["profile"] == "telegram-mcp-standalone"
    assert document["count"] == len(document["entries"])
    declared = {item["id"] for item in document["entries"]}
    assert {
        "telegram_api_id",
        "telegram_api_hash",
        "telegram_session",
        "supabase_rick_db_url",
        "supabase_rick_api_key",
    } <= declared
    aliases = {alias for item in document["entries"] for alias in item.get("env_aliases", [])}
    assert {"SUPABASE_DB_URL", "SUPABASE_API_KEY"} <= aliases
    standalone = DEPLOY / "standalone"
    env = os.environ.copy()
    env["PYTHONPATH"] = str(standalone)
    env["HEROES_CREDENTIALS_REGISTRY"] = str(registry)
    env["TELEGRAM_USER"] = "ikrasinsky"
    env["TELEGRAM_API_ID"] = "12345"
    env["TELEGRAM_API_HASH"] = "a" * 32
    env["TELEGRAM_SESSION_STRING"] = "test-session"
    probe = subprocess.run(
        [
            sys.executable,
            "-c",
            "from heroes_platform.credentials import credentials_manager; "
            "from heroes_platform.credentials.service_env import get_service_credentials; "
            "known=get_service_credentials('telegram'); "
            "r=credentials_manager.get_credential('undeclared_partner_secret'); "
            "raise SystemExit(0 if known['TELEGRAM_API_ID']=='12345' "
            "and known['TELEGRAM_SESSION_STRING']=='test-session' "
            "and not r.success and r.source is None else 1)",
        ],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert probe.returncode == 0, probe.stderr


def test_listener_entrypoint_is_noninteractive_and_long_lived():
    listener = (DEPLOY.parent / "listener.py").read_text()
    assert "run_until_disconnected" in listener
    assert "input(" not in listener
    assert "getpass" not in listener


def test_backfill_timer_is_bounded_and_resumable():
    service = (DEPLOY / "telegram-mcp-backfill@.service").read_text()
    timer = (DEPLOY / "telegram-mcp-backfill@.timer").read_text()
    assert "--budget 1000" in service
    assert "--profile %i" in service
    assert "EnvironmentFile=/etc/telegram-mcp/env.d/%i.env" in service
    assert "SuccessExitStatus=2" in service
    assert "OnUnitActiveSec=5min" in timer
    cli = (DEPLOY.parent / "scripts" / "deep_backfill_history.py").read_text()
    assert "get_dialogs(limit=None)" in cli


def test_rce_injection_via_profiles_refused():
    """squad code-reviewer RCE: crafted --profiles must be REFUSED before any eval (no exec)."""
    import os, tempfile
    marker = os.path.join(tempfile.gettempdir(), "rce_pwn_marker_xf6")
    if os.path.exists(marker): os.remove(marker)
    r = subprocess.run(["bash", str(SCRIPT), "--dry-run", "--profiles", f"ik;touch {marker}"],
                       capture_output=True, text=True)
    assert r.returncode == 2, r.stdout + r.stderr
    assert "REFUSED" in (r.stdout + r.stderr)
    assert not os.path.exists(marker)  # injection did NOT execute


def test_rce_injection_via_appdir_env_refused():
    import subprocess as sp
    r = sp.run(["bash", str(SCRIPT), "--dry-run"], capture_output=True, text=True,
               env={**__import__("os").environ, "TELEGRAM_MCP_APP_DIR": "/home/x;rm -rf ~"})
    assert r.returncode == 2 and "REFUSED" in (r.stdout + r.stderr)


if __name__ == "__main__":
    subprocess.run([sys.executable, "-m", "pytest", __file__, "-v"])
