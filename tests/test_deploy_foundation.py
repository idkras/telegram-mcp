"""R1 deploy-foundation experiment (pr-hero-xf6): prove the systemd deploy is valid
and idempotent BEFORE it runs on sandbox-ik. No live VPS needed — validates the
unit render, script syntax, and dry-run determinism."""
import re
import os
import subprocess
import sys
from pathlib import Path

DEPLOY = Path(__file__).resolve().parents[1] / "deploy"
TEMPLATE = DEPLOY / "telegram-mcp.service.template"
SCRIPT = DEPLOY / "deploy-sandbox-ik.sh"


def _render(
    profile="ikrasinsky",
    user="idkras",
    app="/home/idkras/telegram-mcp",
    mcp_port="8766",
):
    t = TEMPLATE.read_text()
    return (t.replace("__PROFILE__", profile).replace("__USER__", user)
             .replace("__APP_DIR__", app).replace("__MCP_PORT__", mcp_port))


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
    assert "TELEGRAM_MCP_TRANSPORT=streamable-http" in u
    assert "TELEGRAM_MCP_HOST=127.0.0.1" in u
    assert "TELEGRAM_MCP_PORT=8766" in u
    assert "DEEP_BACKFILL_IN_LISTENER=false" in u
    assert "BACKFILL_ON_STARTUP=false" in u
    assert "DEEP_BACKFILL_DEACTIVATE_UNRESOLVED=true" in u
    assert "DEEP_BACKFILL_STARTUP_MAX_PASSES=200" in u
    assert "TELEGRAM_PG_POOL_MIN=1" in u
    assert "TELEGRAM_PG_POOL_MAX=2" in u
    assert "Alias=telegram-mcp-ikrasinsky-remote.service" in u


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


def test_deploy_requires_canonical_harness_credential_runtime():
    script = SCRIPT.read_text()
    assert "-r '$APP_DIR/requirements.txt' -r '$APP_DIR/requirements-laba.txt'" in script
    assert "import credentials_registry" in script
    assert "deploy through Heroes Harness" in script
    standalone = DEPLOY / "standalone"
    for rel in (
        "heroes_harness/credentials_registry.yaml", "heroes_platform/credentials/__init__.py",
        "heroes_platform/credentials/service_env.py",
    ):
        assert not (standalone / rel).exists(), rel
    assert not (standalone / "heroes_platform/shared/credentials_wrapper.py").exists()

    assert "reset --hard" not in script
    assert "merge --ff-only origin/main" in script
    assert "disable --now telegram-mcp-backfill@${p}.timer" in script


def test_listener_entrypoint_is_noninteractive_and_long_lived():
    listener = (DEPLOY.parent / "listener.py").read_text()
    assert "run_until_disconnected" in listener
    assert "run_streamable_http_async" in listener
    assert "telegram_mcp.client = client" in listener
    assert "input(" not in listener
    assert "getpass" not in listener


def test_backfill_timer_is_bounded_and_resumable():
    service = (DEPLOY / "telegram-mcp-backfill@.service").read_text()
    timer = (DEPLOY / "telegram-mcp-backfill@.timer").read_text()
    assert "catch_up_recent_telegram_to_supabase.py" in service
    assert "--limit-messages 5000" in service
    assert "--deep-backfill-budget 1000" in service
    assert "--deep-backfill-per-chat 250" in service
    assert "--profile %i" in service
    assert "EnvironmentFile=/etc/telegram-mcp/env.d/%i.env" in service
    assert "HEROES_CREDENTIALS_REGISTRY=/home/idkras/telegram-mcp/heroes_harness/credentials_registry.yaml" in service
    assert "SuccessExitStatus=2" not in service
    assert "OnUnitActiveSec=5min" in timer
    cli = (DEPLOY.parent / "scripts" / "deep_backfill_history.py").read_text()
    assert "get_dialogs(limit=entity_cache_limit)" in cli
    assert '"--entity-cache-dialog-limit"' in cli


def test_main_http_endpoint_is_profile_pinned_and_loopback_by_default():
    main = (DEPLOY.parent / "main.py").read_text()
    assert 'os.getenv("TELEGRAM_MCP_HOST", "127.0.0.1")' in main
    assert 'os.getenv("TELEGRAM_MCP_SINGLE_PROFILE", "false")' in main
    assert "This endpoint is pinned to profile=" in main


def test_listener_boot_marker_cannot_block_mcp_event_loop():
    handlers = (DEPLOY.parent / "event_handlers.py").read_text()
    assert "await asyncio.to_thread(_write_marker)" in handlers


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
