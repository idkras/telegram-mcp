"""Registry-backed credential subset for standalone Linux deployment.

This generated-compatible adapter deliberately accepts only logical ids declared
in the bundled partner registry. Values remain in the systemd EnvironmentFile;
the registry controls which environment aliases may be read.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import yaml


@dataclass
class CredentialResult:
    success: bool
    value: str | None = None
    source: str | None = None
    error: str | None = None


class CredentialsManager:
    def __init__(self) -> None:
        configured = os.getenv("HEROES_CREDENTIALS_REGISTRY")
        if not configured:
            raise RuntimeError("HEROES_CREDENTIALS_REGISTRY is required in standalone mode")
        path = Path(configured)
        document = yaml.safe_load(path.read_text(encoding="utf-8"))
        if document.get("catalog_scope") != "partner_subset":
            raise RuntimeError("Standalone registry must declare catalog_scope: partner_subset")
        self._configs = {
            item["key"]: item
            for item in document.get("credentials", [])
            if isinstance(item, dict) and isinstance(item.get("key"), str)
        }
        if not self._configs:
            raise RuntimeError("Standalone credential registry is empty")

    def get_credential(self, credential_name: str) -> CredentialResult:
        config = self._configs.get(credential_name)
        if config is None:
            return CredentialResult(False, error=f"Unknown credential: {credential_name}")
        for alias in config.get("env_aliases", []):
            value = os.getenv(alias)
            if value:
                return CredentialResult(True, value=value, source="env")
        return CredentialResult(False, error=f"Credential unavailable: {credential_name}")

    def store_credential(self, credential_name: str, value: str, source: str = "env") -> bool:
        config = self._configs.get(credential_name)
        aliases = config.get("env_aliases", []) if config else []
        if source != "env" or not aliases:
            return False
        os.environ[aliases[0]] = value
        return True


credentials_manager = CredentialsManager()

