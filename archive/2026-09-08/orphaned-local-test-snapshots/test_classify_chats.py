"""Tests for classify_chats (pr-hero-ei7) — pure classify(), no DB."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import classify_chats as cc  # noqa: E402

TYPES = cc.load_ssot()
MAP = {}


def _c(chat_id, title, chat_type="group", tags=None):
    return cc.classify(chat_id, title, chat_type, set(tags or []), MAP, TYPES)


def test_code_relay_by_id_and_title():
    assert _c("-1001866105830", "sms Inbox") == "code_relay"
    assert _c("8513597149", "Rick info", "private") == "code_relay"
    assert _c("999", "once sms public", "supergroup") == "code_relay"


def test_financial_type():
    assert _c("-100111", "R&H Биллинг, PNL, акты и сверки", "supergroup") == "financial"
    assert _c("-100222", "ИП Красинский. Документооборот") == "financial"


def test_client_analytics():
    assert _c("-100333", "[vipavenue.ru + rick.ai] полезные отчеты") == "client_analytics"


def test_personal_dm_private_fallthrough():
    # a plain private DM with a person name → personal_dm (redact)
    assert _c("78126134", "Karina", "private") == "personal_dm"


def test_subscribed_channel():
    assert _c("-100444", "Data Driven Decisions", "channel") == "subscribed_channel"


def test_community_by_tag():
    assert _c("-100555", "Some group", "supergroup", tags=["r-founders"]) == "community"


def test_bot_service_not_code_relay():
    assert _c("-100666", "Yandex Metrica Bot", "private") == "bot_service"


def test_fallback_unclassified_group():
    # a random group with no matching pattern → unclassified (100% coverage)
    assert _c("-100777", "Ремонт квартиры на Шмитовском", "supergroup") == "unclassified"


def test_every_type_has_policy():
    for t in TYPES:
        assert t["index_policy"] in ("skip", "redact", "review", "index"), t["name"]


def test_priority_financial_before_personal():
    # financial title wins over private chat_type
    assert _c("78126134", "Dasha оплата счёт", "private") == "financial"


if __name__ == "__main__":
    import subprocess
    subprocess.run([sys.executable, "-m", "pytest", __file__, "-v"])
