#!/usr/bin/env python3
"""Tests for universal (config-driven) Telegram profile → credential-name resolution.

JTBD: Когда команда добавляет НОВОГО клиента в telegram-mcp как laba-сервис,
мы хотим, чтобы это была правка config/Keychain, НЕ правка Python-кода
(Generalization-first gate Q4, AGENTS.md). При этом легаси-профили
(lisa / ikrasinsky / rick-coposlly-linkedinhero / default) обязаны
резолвиться в ТЕ ЖЕ credential-имена что и раньше (backward compatibility —
иначе протухнут существующие Keychain-записи).

Эти тесты — pure-function, без сети и без Keychain.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

_PKG_ROOT = Path(__file__).resolve().parent.parent.parent.parent
if str(_PKG_ROOT) not in sys.path:
    sys.path.insert(0, str(_PKG_ROOT))

from heroes_platform.heroes_telegram_mcp.session_manager import (  # noqa: E402
    get_profile_credential_names,
)


# ── Backward compatibility: легаси-профили резолвятся ДОСЛОВНО как раньше ──

def test_lisa_keys_unchanged():
    """Lisa использует исторически непоследовательные имена (api_key/app_hash) —
    они обязаны сохраниться, иначе живая сессия Лизы перестанет читаться."""
    names = get_profile_credential_names("lisa")
    assert names["api_id"] == "lisa_tg_api_key"
    assert names["api_hash"] == "lisa_tg_app_hash"
    assert names["session"] == "lisa_tg_session"
    assert names["phone"] == "lisa_tg_phone"
    assert names["2fa_password"] == "lisa_tg_2fa_password"


def test_ikrasinsky_keys_unchanged():
    names = get_profile_credential_names("ikrasinsky")
    assert names["api_id"] == "telegram_api_id"
    assert names["api_hash"] == "telegram_api_hash"
    assert names["session"] == "telegram_session"
    assert names["phone"] == "telegram_phone"


@pytest.mark.parametrize("alias", ["ik", "ilyakrasinsky", "IK", "IkRaSinSky"])
def test_ikrasinsky_aliases_resolve_to_same(alias):
    """ik / ilyakrasinsky / регистр — все резолвятся в ikrasinsky-профиль."""
    assert get_profile_credential_names(alias)["session"] == "telegram_session"


def test_rick_coposlly_keys_unchanged():
    names = get_profile_credential_names("rick-coposlly-linkedinhero")
    assert names["api_id"] == "rick_coposlly_linkedinhero_api_id"
    assert names["api_hash"] == "rick_coposlly_linkedinhero_api_hash"
    assert names["session"] == "rick_coposlly_linkedinhero_session"
    assert names["phone"] == "rick_coposlly_linkedinhero_phone"


@pytest.mark.parametrize("profile", ["default", "", "DEFAULT"])
def test_default_profile_uses_declared_registry_keys(profile):
    names = get_profile_credential_names(profile)
    assert names["api_id"] == "telegram_api_id"
    assert names["api_hash"] == "telegram_api_hash"
    assert names["session"] == "telegram_session"
    assert names["phone"] == "telegram_phone"


# ── Universal convention: НОВЫЙ клиент = zero code change ──

def test_new_client_requires_registry_declaration():
    with pytest.raises(ValueError, match="profile 'acme' is undeclared"):
        get_profile_credential_names("acme")


@pytest.mark.parametrize(
    "raw,slug",
    [
        ("my-client", "my_client"),
        ("Smokeway Co", "smokeway_co"),
        ("vipavenue.ru", "vipavenue_ru"),
        ("  Typhoon--Coffee  ", "typhoon_coffee"),
    ],
)
def test_new_client_slug_normalization(raw, slug):
    """Дефисы / точки / пробелы / регистр дают безопасный registry profile slug."""
    with pytest.raises(ValueError, match=rf"profile '{slug}' is undeclared"):
        get_profile_credential_names(raw)


def test_returns_fresh_dict_each_call():
    """Мутация результата не должна протекать в следующий вызов (no shared mutable state)."""
    a = get_profile_credential_names("lisa")
    a["session"] = "MUTATED"
    b = get_profile_credential_names("lisa")
    assert b["session"] == "lisa_tg_session"


# ── Edge cases / security (review 2026-06-01: empty-slug namespace collision) ──

@pytest.mark.parametrize("blank", ["   ", "\t", "\n  \n"])
def test_whitespace_only_profile_falls_back_to_default(blank):
    """'   ' (только пробелы) → default, НЕ silent '_tg_session' collision."""
    names = get_profile_credential_names(blank)
    assert names["session"] == "telegram_session"


@pytest.mark.parametrize("bad", ["---", "!!!", "...", "Лиса", "Студия", "🔥", "++--"])
def test_non_latin_or_symbols_only_raises(bad):
    """unicode-only / symbols-only → fail-fast ValueError, НЕ silent '_tg_*'
    (security finding: namespace squatting через пустой slug)."""
    with pytest.raises(ValueError):
        get_profile_credential_names(bad)


def test_registry_profile_source_not_mutated():
    """Мутация результата не должна затронуть registry profile source."""
    got = get_profile_credential_names("lisa")
    got["session"] = "HACKED"
    assert get_profile_credential_names("lisa")["session"] == "lisa_tg_session"


def test_slug_collision_is_by_design():
    for profile in ("my-client", "my_client", "My Client"):
        with pytest.raises(ValueError, match="profile 'my_client' is undeclared"):
            get_profile_credential_names(profile)


def test_reserved_override_name_cannot_be_new_client():
    """Новый клиент с именем 'lisa' получит ключи оператора Лизы (reserved name).
    Это by-design (override exclusive) — фиксируем как контракт, чтобы не было
    случайного переиспользования имени без миграции."""
    assert get_profile_credential_names("lisa")["session"] == "lisa_tg_session"
    # а вот 'lisa-skincare' (другое имя) уйдёт по конвенции — изоляция сохранена
    with pytest.raises(ValueError, match="profile 'lisa_skincare' is undeclared"):
        get_profile_credential_names("lisa-skincare")


def test_every_builtin_override_logical_id_is_in_canonical_registry():
    from credentials_registry import CredentialsManager
    from credentials_registry.registry import load_service_profiles

    registered = set(CredentialsManager()._configs)
    generated = {
        name for profile, data in load_service_profiles().items()
        if profile.startswith("telegram:")
        for name in data.get("credential_roles", {}).values() if name
    }
    assert generated <= registered
