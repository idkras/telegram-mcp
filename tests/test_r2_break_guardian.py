"""R2 break-the-system experiment as regression test (pr-hero-x0p).

Tests the ACTUAL writer path (_telethon_message_to_row), not just the guardian —
proves D-core-1 fix: a "sms Inbox" chat NOT in id_tails is skipped because the
real chat_title now reaches classify_message (was title=None → leaked).
"""
import datetime as _dt
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import supabase_writer as sw  # noqa: E402


class _Chat:
    def __init__(self, title):
        self.title = title


class _User:
    """Telethon User/Bot entity — has NO .title, only first_name/username (security-4)."""
    def __init__(self, first_name=None, username=None):
        self.first_name = first_name
        self.username = username
        self.title = None  # explicit: User has no title


class _Msg:
    def __init__(self, text, chat_title=None, mid=1):
        self.id = mid
        self.text = text
        self.date = _dt.datetime(2026, 7, 2, 12, 0, 0)
        self.chat = _Chat(chat_title) if chat_title else None

    def to_dict(self):
        return {"id": self.id, "message": self.text}


def _writer():
    w = sw.SupabaseWriter.__new__(sw.SupabaseWriter)   # bypass __init__ (no creds/DB)
    w.telegram_user_id = "test"
    w._postgres_url = None
    return w


def _row(writer, chat_id, title, text, pass_title):
    msg = _Msg(text, chat_title=title)
    ct = title if pass_title else None
    return writer._telethon_message_to_row(msg, chat_id, "group", ct)


# ── secret vectors: MUST be skipped (row is None) ──────────────────────────
def test_v1_code_relay_id_tail_skipped():
    assert _row(_writer(), "2809646231", "once sms public", "код 8241", True) is None


def test_v2_code_relay_minus100_prefix_skipped():
    assert _row(_writer(), "-1002809646231", "once sms public", "код 5591", True) is None


def test_v3_title_only_sms_inbox_skipped_WITH_title():
    # THE D-core-1 fix: chat NOT in id_tails but title matches → skip
    assert _row(_writer(), "999999001", "sms Inbox", "OTP 4417", True) is None


def test_v3_title_only_leaks_when_title_none_derives_from_message():
    # Even if caller passes None, writer derives title from message.chat → still skip
    assert _row(_writer(), "999999001", "sms Inbox", "OTP 4417", False) is None


def test_v4_code_relay_empty_text_skipped():
    assert _row(_writer(), "2809646231", "once sms public", "", True) is None


# ── redact / review / legit: MUST be saved (row not None) ──────────────────
def test_v5_personal_dm_card_redacted():
    row = _row(_writer(), "78126134", "Karina", "картой 4276 1234 5678 9010", True)
    assert row is not None
    assert "4276 1234 5678 9010" not in (row["text"] or "")   # masked


def test_v7_legit_client_analytics_saved():
    row = _row(_writer(), "2903143684", "[vipavenue.ru + rick.ai]", "конверсия +12%", True)
    assert row is not None and "конверсия" in row["text"]


# ── iter-2 security rework vectors (squad findings) ────────────────────────
def test_security4_bot_no_title_skipped_by_username():
    """BREAK#3/#4: a Bot has no .title — derive from first_name → title_skip fires."""
    w = _writer()
    msg = _Msg("OTP 4417", mid=1)
    msg.chat = _User(first_name="sms Inbox")   # bot-like, no title
    assert w._telethon_message_to_row(msg, "999999002", "private", None) is None


def test_security2_fail_closed_returns_none(monkeypatch):
    """BREAK#5/#6: any guard error → skip (return None), not fail-open leak."""
    import supabase_writer as _sw
    monkeypatch.setattr(_sw, "_guard_rules", lambda: (_ for _ in ()).throw(RuntimeError("broken YAML")))
    w = _writer()
    msg = _Msg("код 8241", chat_title="once sms public", mid=1)
    assert w._telethon_message_to_row(msg, "2809646231", "group", "once sms public") is None


def test_security3_raw_recursive_redact_no_card_anywhere():
    """BREAK#7: card must be masked in raw JSONB, not just text."""
    w = _writer()
    msg = _Msg("картой 4276 1234 5678 9010", chat_title="Karina", mid=1)
    # simulate Telethon to_dict with card echoed in nested fwd_from + entities
    msg.to_dict = lambda: {"id": 1, "message": "картой 4276 1234 5678 9010",
                           "fwd_from": {"from_name": "картой 4276 1234 5678 9010"}}
    row = w._telethon_message_to_row(msg, "78126134", "private", "Karina")
    import json as _json
    raw_str = _json.dumps(row["raw"], ensure_ascii=False)
    assert "4276 1234 5678 9010" not in raw_str   # nowhere in raw
    assert "4276 1234 5678 9010" not in (row["text"] or "")


if __name__ == "__main__":
    import subprocess
    subprocess.run([sys.executable, "-m", "pytest", __file__, "-v"])
