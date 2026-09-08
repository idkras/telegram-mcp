"""Tests for telegram_mcp_doctor + lifecycle ssot (pr-hero-ku7) — no live deps."""
import sys
from pathlib import Path

import yaml

_HERE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_HERE))
import telegram_mcp_doctor as doc  # noqa: E402

SSOT = yaml.safe_load(open(_HERE / "telegram_mcp_workflow_ssot.yaml", encoding="utf-8"))


def test_ssot_has_lifecycle_8_stages():
    stages = [s["stage"] for s in SSOT["lifecycle"]]
    assert stages == ["provision_vps", "deploy_units", "session_auth", "session_collision_guard",
                      "ingest", "classify", "guardian_write", "monitor_surface"]


def test_every_stage_has_doctor_check_and_acceptance():
    for s in SSOT["lifecycle"]:
        assert s.get("doctor_check"), s["stage"]
        assert s.get("acceptance"), s["stage"]


def test_session_auth_is_owner_gated():
    auth = next(s for s in SSOT["lifecycle"] if s["stage"] == "session_auth")
    assert auth.get("owner_gated") is True


def test_code_relay_ids_extracted():
    ids = doc._code_relay_ids(SSOT)
    assert "684318338" in ids and "2809646231" in ids  # sms Inbox + once sms public
    assert len(ids) >= 5


def test_profiles_and_schemas():
    assert doc._profiles(SSOT) == ["ikrasinsky", "lisa"]
    assert doc._schemas(SSOT)["lisa"] == "tg_lisa"


def test_checks_registry_has_all_mechanical_layers():
    names = {c.__name__ for c in doc.CHECKS}
    assert names == {"check_deploy_units", "check_session_collision", "check_ingest",
                     "check_classify", "check_guardian_write", "check_monitor_surface"}


if __name__ == "__main__":
    import subprocess
    subprocess.run([sys.executable, "-m", "pytest", __file__, "-v"])
