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

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:  # python -I does not add the script directory
    sys.path.insert(0, str(HERE))

from encrypted_credential import (  # noqa: E402
    BASE_REQUIRED_SECRET_KEYS,
    PayloadError,
    parse_dotenv,
    profile_key_set,
    serialize_dotenv,
    validate_profile_secrets,
)


CREDENTIAL_NAME = "telegram_env"
# Required by every profile; lisa additionally requires LISA_TG_* (see
# encrypted_credential.PROFILE_KEY_SETS).  Kept for existing importers.
REQUIRED_SECRET_KEYS = BASE_REQUIRED_SECRET_KEYS
PROFILE_RE = re.compile(r"^[a-z0-9_]+$")
CREDENTIAL_FILE_RE = re.compile(r"^telegram-mcp-([a-z0-9_]+)\.env\.cred$")


class CredentialInstallError(RuntimeError):
    """Safe, value-blind installation failure."""


def canonicalize_payload(raw: bytes, profile: str) -> bytes:
    """Validate a dotenv payload against the profile key set; return secret-only bytes."""

    try:
        values = parse_dotenv(raw)
        keyset = profile_key_set(profile)
        secrets_only = validate_profile_secrets(values, profile, allow_legacy_nonsecret=True)
    except PayloadError as exc:
        raise CredentialInstallError(str(exc)) from exc
    return serialize_dotenv(secrets_only, keyset.ordered)


def profile_from_credential_path(path: Path) -> str:
    match = CREDENTIAL_FILE_RE.fullmatch(path.name)
    if not match:
        raise CredentialInstallError("cannot infer profile from credential file name")
    return match.group(1)


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
    canonical = canonicalize_payload(raw, profile)
    output = output.resolve()
    named = CREDENTIAL_FILE_RE.fullmatch(output.name)
    if named and named.group(1) != profile:
        raise CredentialInstallError("credential file name belongs to another profile")
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
    profile: str | None = None,
    systemd_creds: str = "systemd-creds",
) -> tuple[str, ...]:
    """Decrypt and validate an installed blob without printing secret values."""

    source = source.resolve()
    profile = profile or profile_from_credential_path(source)
    if not source.is_file():
        raise CredentialInstallError("encrypted credential is missing")
    decrypted = _run_systemd_creds(systemd_creds, "decrypt", str(source), "-")
    if decrypted.returncode != 0:
        raise CredentialInstallError("encrypted credential decrypt failed")
    canonical = canonicalize_payload(decrypted.stdout, profile)
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
                f"profile={profile_from_credential_path(args.verify_encrypted.resolve())} "
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

    keys = ",".join(profile_key_set(args.profile).required)
    print(
        f"profile={args.profile} encrypted_credential={output} "
        f"readback=match required_keys={keys} "
        f"rollback={'available' if rollback else 'not-created'}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
