#!/usr/bin/env python3
"""Install a Telegram profile payload as a host-encrypted systemd credential.

The plaintext payload is accepted through stdin (or an explicit legacy source
path during migration), is held in memory only, and is never printed.  The
installed blob is bound to the systemd credential name ``telegram_env`` and is
independently decrypted in memory before the atomic replace is accepted.
"""

from __future__ import annotations

import argparse
import hmac
import os
import re
import subprocess
import sys
import uuid
from pathlib import Path

from encrypted_credential import PayloadError, parse_dotenv, serialize_dotenv


CREDENTIAL_NAME = "telegram_env"
REQUIRED_SECRET_KEYS = (
    "TELEGRAM_API_ID",
    "TELEGRAM_API_HASH",
    "TELEGRAM_SESSION_STRING",
    "SUPABASE_DB_URL",
)
OPTIONAL_SECRET_KEYS = (
    "SUPABASE_API_KEY",
    "SUPABASE_SERVICE_ROLE_KEY",
)
ALLOWED_SECRET_KEYS = frozenset(REQUIRED_SECRET_KEYS + OPTIONAL_SECRET_KEYS)
# Accepted only as migration input and intentionally omitted from the encrypted
# secret payload.  Stable endpoint identity/runtime tuning belongs to the unit.
LEGACY_NONSECRET_KEYS = frozenset(("TELEGRAM_USER", "LABA_MODE", "SUPABASE_URL"))
PROFILE_RE = re.compile(r"^[a-z0-9_]+$")


class CredentialInstallError(RuntimeError):
    """Safe, value-blind installation failure."""


def canonicalize_payload(raw: bytes) -> bytes:
    """Validate a dotenv payload and return deterministic secret-only bytes."""

    try:
        values = parse_dotenv(raw)
    except PayloadError as exc:
        raise CredentialInstallError(str(exc)) from exc
    unknown = sorted(set(values) - ALLOWED_SECRET_KEYS - LEGACY_NONSECRET_KEYS)
    if unknown:
        raise CredentialInstallError("unknown secret keys: " + ",".join(unknown))
    missing = sorted(
        key for key in REQUIRED_SECRET_KEYS if not isinstance(values.get(key), str) or not values[key]
    )
    if missing:
        raise CredentialInstallError("missing required secret keys: " + ",".join(missing))

    ordered = REQUIRED_SECRET_KEYS + OPTIONAL_SECRET_KEYS
    return serialize_dotenv(values, ordered)


def _run_systemd_creds(
    binary: str,
    action: str,
    source: str,
    destination: str,
    *,
    payload: bytes | None = None,
) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(
        [binary, action, f"--name={CREDENTIAL_NAME}", source, destination],
        input=payload,
        stdout=subprocess.PIPE if destination == "-" else subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )


def install_credential(
    profile: str,
    raw: bytes,
    output: Path,
    *,
    systemd_creds: str = "systemd-creds",
) -> tuple[Path, Path | None]:
    """Encrypt, verify, and atomically install one profile credential."""

    if not PROFILE_RE.fullmatch(profile):
        raise CredentialInstallError("profile must match [a-z0-9_]+")
    canonical = canonicalize_payload(raw)
    output = output.resolve()
    output.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(output.parent, 0o700)

    temporary = output.with_name(f".{output.name}.{uuid.uuid4().hex}.new")
    rollback = output.with_name(f"{output.name}.rollback")
    had_previous = output.exists()
    try:
        encrypted = _run_systemd_creds(
            systemd_creds, "encrypt", "-", str(temporary), payload=canonical
        )
        if encrypted.returncode != 0 or not temporary.is_file():
            raise CredentialInstallError("systemd-creds encrypt failed")
        os.chmod(temporary, 0o600)

        decrypted = _run_systemd_creds(
            systemd_creds, "decrypt", str(temporary), "-"
        )
        if decrypted.returncode != 0 or not hmac.compare_digest(decrypted.stdout, canonical):
            raise CredentialInstallError("encrypted credential readback mismatch")

        if had_previous:
            os.replace(output, rollback)
            os.chmod(rollback, 0o600)
        try:
            os.replace(temporary, output)
        except BaseException:
            if had_previous and rollback.exists() and not output.exists():
                os.replace(rollback, output)
            raise
        os.chmod(output, 0o600)
        directory_fd = os.open(output.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
        return output, rollback if had_previous else None
    finally:
        if temporary.exists():
            temporary.unlink()


def _default_output(profile: str) -> Path:
    return Path(f"/etc/credstore.encrypted/telegram-mcp-{profile}.env.cred")


def verify_encrypted_credential(
    source: Path,
    *,
    systemd_creds: str = "systemd-creds",
) -> tuple[str, ...]:
    """Decrypt and validate an installed blob without printing secret values."""

    source = source.resolve()
    if not source.is_file():
        raise CredentialInstallError("encrypted credential is missing")
    decrypted = _run_systemd_creds(systemd_creds, "decrypt", str(source), "-")
    if decrypted.returncode != 0:
        raise CredentialInstallError("encrypted credential decrypt failed")
    canonical = canonicalize_payload(decrypted.stdout)
    if not hmac.compare_digest(canonical, decrypted.stdout):
        raise CredentialInstallError("encrypted credential payload is not canonical")
    values = parse_dotenv(canonical)
    return tuple(values)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    selector = parser.add_mutually_exclusive_group(required=True)
    selector.add_argument("--profile")
    selector.add_argument("--verify-encrypted", type=Path)
    parser.add_argument(
        "--source-env",
        type=Path,
        help="explicit root-only legacy env path; default reads payload from stdin",
    )
    parser.add_argument("--output", type=Path)
    parser.add_argument("--systemd-creds", default="systemd-creds")
    args = parser.parse_args(argv)

    if args.profile and not PROFILE_RE.fullmatch(args.profile):
        parser.error("profile must match [a-z0-9_]+")
    try:
        if args.verify_encrypted:
            keys = verify_encrypted_credential(
                args.verify_encrypted,
                systemd_creds=args.systemd_creds,
            )
            print(
                f"encrypted_credential={args.verify_encrypted.resolve()} "
                f"readback=match keys={','.join(keys)}"
            )
            return 0
        raw = args.source_env.read_bytes() if args.source_env else sys.stdin.buffer.read()
        output, rollback = install_credential(
            args.profile,
            raw,
            args.output or _default_output(args.profile),
            systemd_creds=args.systemd_creds,
        )
    except (OSError, CredentialInstallError) as exc:
        print(f"credential install refused: {exc}", file=sys.stderr)
        return 1

    keys = ",".join(REQUIRED_SECRET_KEYS)
    print(
        f"profile={args.profile} encrypted_credential={output} "
        f"readback=match required_keys={keys} "
        f"rollback={'available' if rollback else 'not-created'}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
