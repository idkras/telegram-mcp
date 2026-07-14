"""Telegram service mapping for the standalone partner registry subset."""

from __future__ import annotations

import os

from heroes_platform.credentials import credentials_manager


def _logical_names(profile: str) -> tuple[str, str, str]:
    normalized = profile.strip().lower()
    if normalized == "lisa":
        return "lisa_tg_api_key", "lisa_tg_app_hash", "lisa_tg_session"
    if normalized in {"ik", "ikrasinsky", "ilyakrasinsky"}:
        return "ik_tg_api_id", "ik_tg_api_hash", "ik_tg_session"
    return "telegram_api_id", "telegram_api_hash", "telegram_session"


def get_service_credentials(service: str) -> dict[str, str]:
    if service != "telegram":
        return {}
    profile = os.getenv("TELEGRAM_USER", "default")
    logical = _logical_names(profile)
    result: dict[str, str] = {"TELEGRAM_USER": profile}
    for logical_id, env_name in zip(
        logical,
        ("TELEGRAM_API_ID", "TELEGRAM_API_HASH", "TELEGRAM_SESSION_STRING"),
        strict=True,
    ):
        resolved = credentials_manager.get_credential(logical_id)
        if resolved.success and resolved.value:
            result[env_name] = resolved.value
    return result

