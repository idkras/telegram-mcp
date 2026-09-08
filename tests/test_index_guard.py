"""Tests for index_guard guardian (pr-hero-gcy)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import index_guard as g  # noqa: E402

R = g.load_rules()


def test_id_forms_normalize():
    assert g.normalize_id_tail("-1001866105830") == "1866105830"
    assert g.normalize_id_tail("-684318338") == "684318338"


def test_skip_sms_relay_all_forms():
    for cid in ("1866105830", "-1001866105830"):
        d = g.classify_message(cid, "sms Inbox", "код 4821", R)
        assert d.action == "skip"


def test_skip_rick_info_relay():
    d = g.classify_message("8513597149", "Rick info", "СМС от Yandex: код 567-305", R)
    assert d.action == "skip"


def test_skip_secretary_bot_by_title():
    d = g.classify_message("999", "Some Secretary Bot", "hi", R)
    assert d.action == "skip"


def test_business_bank_chat_saved_not_skipped():
    d = g.classify_message("-1001234567890", "Heroes. Орг фин вопросы", "оклад 150000 обсудили", R)
    assert d.action == "save"


def test_redact_card_number_keeps_message():
    d = g.classify_message("-1001268195334", "ИП Красинский. Документооборот",
                           "оплата с карты 4295 9412 3456 7890 прошла", R)
    assert d.action == "save"
    assert "4295" not in d.text
    assert "REDACTED-CARD" in d.text
    assert "card_number" in d.categories


def test_redact_otp_in_business_chat():
    d = g.classify_message("-100999", "рабочий чат", "код подтверждения 4821 введите", R)
    assert "4821" not in d.text
    assert "REDACTED-CODE" in d.text


def test_salary_chat_flagged_review():
    d = g.classify_message("-1001866298750", "Heroes. Орг, административные и фин. вопросы",
                           "зарплата за июнь начислена", R)
    assert d.action == "save"
    assert d.review is True


def test_plain_message_saved_untouched_no_review():
    d = g.classify_message("-100555", "Просто рабочий чат", "давай встретимся в 15:00", R)
    assert d.action == "save"
    assert d.text == "давай встретимся в 15:00"
    assert d.categories == []
    assert d.review is False


if __name__ == "__main__":
    import subprocess
    subprocess.run([sys.executable, "-m", "pytest", __file__, "-v"])
