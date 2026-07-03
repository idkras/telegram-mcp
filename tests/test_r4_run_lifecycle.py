"""R4 run-skill experiment (pr-hero-i5i): the lifecycle status renderer maps the
doctor verdict to an owner-facing status + STOP-flag + per-layer next action, so
`run-lifecycle.sh` is the single command that tells the owner работает/не работает."""
import importlib.util
import sys
from pathlib import Path

_MOD = Path(__file__).resolve().parents[1] / "deploy" / "render_lifecycle_status.py"
_spec = importlib.util.spec_from_file_location("render_lifecycle_status", _MOD)
r = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(r)


def _doctor(red_layers, all_layers=("deploy_units", "ingest", "guardian_write", "classify")):
    return {"results": [
        {"layer": L, "ok": (L not in red_layers), "detail": f"{L} detail"} for L in all_layers
    ], "red": list(red_layers)}


def test_open_contour_writes_stopflag_and_next_actions(tmp_path):
    flag = tmp_path / "stop"
    rc, out = r.render(_doctor(["deploy_units", "ingest"]), flag)
    assert rc == 1
    assert "CONTOUR OPEN" in out
    assert "deploy_units" in out and "SMS" in out            # owner next action surfaced
    assert flag.exists() and "deploy_units" in flag.read_text()  # SwitchBar STOP-flag


def test_closed_contour_clears_stopflag(tmp_path):
    flag = tmp_path / "stop"
    flag.write_text("deploy_units")                          # a stale flag from before
    rc, out = r.render(_doctor([]), flag)
    assert rc == 0
    assert "CONTOUR CLOSED" in out
    assert not flag.exists()                                 # cleared when green


def test_every_red_layer_has_a_next_action():
    for layer in ("deploy_units", "ingest", "guardian_write", "session_collision",
                  "classify", "monitor_surface"):
        assert layer in r.NEXT_ACTION and r.NEXT_ACTION[layer]


def test_deploy_units_next_action_is_owner_gated():
    # the ONE genuinely owner-gated step must name the SMS + the agent command
    a = r.NEXT_ACTION["deploy_units"]
    assert "deploy-sandbox-ik.sh" in a and "SMS" in a and "owner" in a


if __name__ == "__main__":
    import subprocess
    subprocess.run([sys.executable, "-m", "pytest", __file__, "-v"])
