"""Environment-only credential adapter for a root-only systemd EnvironmentFile."""

from __future__ import annotations

import os


def get_service_credentials(service: str) -> dict[str, str]:
    if service != "telegram":
        return {}
    return {
        "TELEGRAM_API_ID": os.getenv("TELEGRAM_API_ID", ""),
        "TELEGRAM_API_HASH": os.getenv("TELEGRAM_API_HASH", ""),
        "TELEGRAM_SESSION_STRING": os.getenv("TELEGRAM_SESSION_STRING", ""),
    }
