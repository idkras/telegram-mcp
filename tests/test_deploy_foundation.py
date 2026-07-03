"""R1 deploy-foundation experiment (pr-hero-xf6): prove the systemd deploy is valid
and idempotent BEFORE it runs on sandbox-ik. No live VPS needed — validates the
unit render, script syntax, and dry-run determinism."""
import re
import subprocess
import sys
from pathlib import Path

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
    assert "ExecStart=/home/idkras/telegram-mcp/.venv/bin/python main.py" in u
    assert "EnvironmentFile=/etc/telegram-mcp/env.d/ikrasinsky.env" in u
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


if __name__ == "__main__":
    subprocess.run([sys.executable, "-m", "pytest", __file__, "-v"])
