#!/usr/bin/env python3
"""Exec a command with environment loaded from one systemd credential.

Unlike ``source``/``EnvironmentFile``, dotenv values are data, never shell code.
The credential is expected at ``$CREDENTIALS_DIRECTORY/telegram_env`` and is
normally materialized by systemd under ``/run/credentials/<unit>/``.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from encrypted_credential import PayloadError, parse_dotenv


CREDENTIAL_NAME = "telegram_env"
REQUIRED_KEYS = (
    "TELEGRAM_API_ID",
    "TELEGRAM_API_HASH",
    "TELEGRAM_SESSION_STRING",
    "SUPABASE_DB_URL",
)


def build_exec_environment(credential_dir: Path, base: dict[str, str]) -> dict[str, str]:
    directory = credential_dir.resolve(strict=True)
    credential = directory / CREDENTIAL_NAME
    if credential.is_symlink() or not credential.is_file():
        raise RuntimeError("telegram_env credential is missing or not a regular file")
    try:
        values = parse_dotenv(credential.read_bytes())
    except PayloadError as exc:
        raise RuntimeError(str(exc)) from exc
    missing = [key for key in REQUIRED_KEYS if not values.get(key)]
    if missing:
        raise RuntimeError("telegram_env is missing required keys: " + ",".join(missing))
    result = dict(base)
    result.update(values)
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
