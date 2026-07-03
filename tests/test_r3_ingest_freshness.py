"""R3 ingest-freshness experiment (pr-hero-1u1): prove the D4 fix — a live listener
heartbeat with stale messages (the "lisa 9 days silent stall") is now UNHEALTHY,
where before it returned OK. Tests _evaluate_runtime_health directly (no DB)."""
import datetime as _dt
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import supabase_writer as sw  # noqa: E402

_now = _dt.datetime.now(tz=_dt.timezone.utc)


def _ev(hb_age_s, msg_age_s, msg_max=21600):
    hb = _now - _dt.timedelta(seconds=hb_age_s)
    msg = None if msg_age_s is None else _now - _dt.timedelta(seconds=msg_age_s)
    return sw._evaluate_runtime_health(
        listener_event_at=hb, latest_message_at=msg,
        max_staleness_seconds=180, transport_message="pg ok",
        max_message_staleness_seconds=msg_max,
    )


def test_D4_heartbeat_live_but_messages_9days_is_UNHEALTHY():
    """THE fix: lisa scenario — heartbeat 60s fresh, latest message 9 days old."""
    ok, msg = _ev(hb_age_s=60, msg_age_s=9 * 24 * 3600)
    assert ok is False
    assert "INGEST STALLED" in msg


def test_D4_negative_control_old_logic_would_pass():
    """NEGATIVE CONTROL: with message-staleness DISABLED (msg_max=0, the old behaviour),
    the same stalled scenario returns OK → proves the check is what catches it."""
    ok, msg = _ev(hb_age_s=60, msg_age_s=9 * 24 * 3600, msg_max=0)
    assert ok is True   # regression proof: without the D4 threshold, stall is invisible


def test_fresh_heartbeat_fresh_messages_ok():
    ok, msg = _ev(hb_age_s=30, msg_age_s=120)
    assert ok is True and "OK" in msg


def test_fresh_heartbeat_no_messages_yet_ok():
    # a brand-new listener with no messages yet must NOT be flagged as stalled
    ok, msg = _ev(hb_age_s=30, msg_age_s=None)
    assert ok is True


def test_stale_heartbeat_still_unhealthy():
    # existing behaviour preserved: dead listener is unhealthy regardless
    ok, msg = _ev(hb_age_s=600, msg_age_s=60)
    assert ok is False and "heartbeat stale" in msg


def test_quiet_period_under_threshold_ok():
    # messages 3h old but threshold 6h → quiet, not stalled
    ok, msg = _ev(hb_age_s=30, msg_age_s=3 * 3600)
    assert ok is True


if __name__ == "__main__":
    import subprocess
    subprocess.run([sys.executable, "-m", "pytest", __file__, "-v"])
