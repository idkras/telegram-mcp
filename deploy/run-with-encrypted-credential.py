#!/usr/bin/env python3
"""Exec a command with environment loaded from one systemd credential.

Unlike ``source``/``EnvironmentFile``, dotenv values are data, never shell code.
The credential is expected at ``$CREDENTIALS_DIRECTORY/telegram_env`` and is
normally materialized by systemd under ``/run/credentials/<unit>/``.
Only the secret key names declared for the unit's ``TELEGRAM_USER`` profile are
exported; any other name (for example ``PYTHONPATH`` or ``LD_PRELOAD``) refuses.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:  # python -I does not add the script directory
    sys.path.insert(0, str(HERE))

from encrypted_credential import (  # noqa: E402
    BASE_REQUIRED_SECRET_KEYS,
    RUNTIME_PROFILES,
    PayloadError,
    parse_dotenv,
    validate_profile_secrets,
)


CREDENTIAL_NAME = "telegram_env"
REQUIRED_KEYS = BASE_REQUIRED_SECRET_KEYS  # lisa additionally requires LISA_TG_*


def build_exec_environment(credential_dir: Path, base: dict[str, str]) -> dict[str, str]:
    profile = base.get("TELEGRAM_USER", "")
    if profile not in RUNTIME_PROFILES:
        raise RuntimeError("TELEGRAM_USER must name a runtime profile with a declared key set")
    directory = credential_dir.resolve(strict=True)
    credential = directory / CREDENTIAL_NAME
    if credential.is_symlink() or not credential.is_file():
        raise RuntimeError("telegram_env credential is missing or not a regular file")
    try:
        values = parse_dotenv(credential.read_bytes())
        secrets_only = validate_profile_secrets(values, profile, allow_legacy_nonsecret=False)
    except PayloadError as exc:
        raise RuntimeError(f"telegram_env is invalid: {exc}") from exc
    result = dict(base)
    result.update(secrets_only)
    return result


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if not args:
        print("usage: run-with-encrypted-credential.py COMMAND [ARG ...]", file=sys.stderr)
        return 2
    credential_dir = os.environ.get("CREDENTIALS_DIRECTORY")
    if not credential_dir:
        print("CREDENTIALS_DIRECTORY is not set", file=sys.stderr)
        return 1
    try:
        environment = build_exec_environment(Path(credential_dir), dict(os.environ))
    except (OSError, RuntimeError) as exc:
        print(f"encrypted credential refused: {exc}", file=sys.stderr)
        return 1
    os.execvpe(args[0], args, environment)
    return 127  # pragma: no cover - os.execvpe only returns by raising


if __name__ == "__main__":
    raise SystemExit(main())
