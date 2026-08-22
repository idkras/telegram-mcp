"""Registry-only credential resolution for the Telegram Supabase writer."""

from __future__ import annotations

import sys
from types import SimpleNamespace

from credentials_registry import credentials_manager
from heroes_platform.heroes_telegram_mcp import supabase_writer


def test_supabase_client_resolves_url_and_key_through_registry(monkeypatch) -> None:
    requested: list[str] = []

    def fake_get_credential(credential_id: str) -> SimpleNamespace:
        requested.append(credential_id)
        values = {
            "supabase_rick_api_key": "registry-test-key",
            "supabase_rick_api_url": "https://registry.example.test",
        }
        return SimpleNamespace(success=True, value=values[credential_id])

    created: list[tuple[str, str]] = []
    monkeypatch.setattr(credentials_manager, "get_credential", fake_get_credential)
    monkeypatch.setitem(
        sys.modules,
        "supabase",
        SimpleNamespace(create_client=lambda url, key: created.append((url, key)) or object()),
    )

    assert supabase_writer._get_supabase_client() is not None
    assert requested == ["supabase_rick_api_key", "supabase_rick_api_url"]
    assert created == [("https://registry.example.test", "registry-test-key")]
