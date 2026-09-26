#!/usr/bin/env python3
"""Create a fresh Telegram StringSession for any registered profile via QR.

The historical default remains ``lisa`` for backwards compatibility.  Pass
``--profile ikrasinsky`` to mint an independent IK session.  The QR login URL
is rendered only as an image; secret session material is stored through the
canonical credential registry and is never printed.
"""

import argparse
import asyncio
import getpass
import hmac
import re
import sys
from pathlib import Path

import qrcode  # type: ignore
import telethon.errors.rpcerrorlist  # type: ignore
from telethon import TelegramClient  # type: ignore
from telethon.sessions import StringSession  # type: ignore

# Add workspace root so heroes_platform imports resolve.
heroes_platform_path = Path(__file__).parent.parent.parent.parent
if str(heroes_platform_path) not in sys.path:
    sys.path.insert(0, str(heroes_platform_path))

from heroes_platform.heroes_telegram_mcp.session_manager import (  # type: ignore  # noqa: E402
    get_profile_credential_names,
)
from credentials_registry import credentials_manager  # type: ignore  # noqa: E402


def _require_credential(key: str) -> str:
    result = credentials_manager.get_credential(key)
    if not result.success or not result.value:
        raise RuntimeError(f"Missing required credential: {key}")
    return str(result.value)


def _qr_output_path(profile: str) -> Path:
    logs_dir = heroes_platform_path / "heroes_platform" / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    safe_profile = re.sub(r"[^a-z0-9_-]+", "-", profile.strip().lower()).strip("-")
    if not safe_profile:
        raise ValueError("Telegram profile cannot be empty")
    return logs_dir / f"{safe_profile}_telegram_qr_login.png"


def _store_session_and_verify(credential_name: str, session_string: str) -> str:
    """Persist a Telegram session and independently read it back.

    The credential value is deliberately excluded from every error and log.
    """

    if not credentials_manager.store_credential(
        credential_name, session_string, "keychain"
    ):
        raise RuntimeError(
            f"Failed to save {credential_name} through the credential registry"
        )

    credentials_manager.clear_credentials_cache()
    readback = credentials_manager.get_credential(credential_name)
    if (
        not readback.success
        or readback.source not in {"keychain", "keyring"}
        or not readback.value
        or not hmac.compare_digest(str(readback.value), session_string)
    ):
        raise RuntimeError(
            f"Saved {credential_name}, but independent Keychain readback did not match"
        )
    return str(readback.source)


async def update_session_via_qr(
    profile: str,
    *,
    qr_path: Path | None = None,
    max_attempts: int = 12,
) -> None:
    names = get_profile_credential_names(profile)
    api_id = int(_require_credential(names["api_id"]))
    api_hash = _require_credential(names["api_hash"])

    print(f"=== TELEGRAM QR SESSION UPDATER: {profile} ===")
    print(
        "INFO: Using Telegram profile "
        f"{profile} with credential keys "
        f"api_id={names['api_id']}, api_hash={names['api_hash']}, "
        f"session={names['session']}"
    )

    client = TelegramClient(StringSession(), api_id, api_hash)
    await client.connect()
    try:
        if await client.is_user_authorized():
            session_string = client.session.save()
            source = _store_session_and_verify(names["session"], session_string)
            print(
                "INFO: Fresh client was already authorized; session saved and verified "
                f"through {source}."
            )
            return

        qr_login = await client.qr_login()
        qr_path = qr_path or _qr_output_path(profile)
        image = qrcode.make(qr_login.url)
        image.save(qr_path)

        print(f"INFO: QR login image saved to: {qr_path}")
        print(f"INFO: QR token expires at: {qr_login.expires.isoformat()}")
        print(
            "INFO: Open Telegram for this profile, go to "
            "Settings -> Devices -> Link Desktop Device, and scan this QR."
        )

        try:
            import subprocess

            subprocess.run(["open", str(qr_path)], check=False)
        except Exception:
            pass

        # Refresh loop — Telegram QR token TTL ~30s, без recreate() любой скан после expiry = fail.
        # RCA 2026-04-17: первый прогон без recreate() выдавал "лажовый QR" — owner не успевал отсканировать до expiry.
        user = None
        for attempt in range(max_attempts):
            try:
                user = await asyncio.wait_for(qr_login.wait(), timeout=30)
                break
            except asyncio.TimeoutError:
                await qr_login.recreate()
                image = qrcode.make(qr_login.url)
                image.save(qr_path)
                print(
                    f"INFO: QR refreshed (attempt {attempt + 2}/{max_attempts}), "
                    f"expires at {qr_login.expires.isoformat()}. Открой PNG заново и сканируй."
                )
            except telethon.errors.rpcerrorlist.SessionPasswordNeededError:
                print("INFO: 2FA cloud password required.")
                # Try Keychain first (works in background/nohup without stdin).
                password = None
                twofa_key = names.get("2fa_password")
                if twofa_key:
                    try:
                        password = _require_credential(twofa_key)
                        print(
                            f"INFO: 2FA password read from Keychain key '{twofa_key}'."
                        )
                    except RuntimeError:
                        print(
                            f"INFO: 2FA password not in Keychain (key '{twofa_key}' missing)."
                        )
                # Fallback to hidden interactive input (foreground Terminal only).
                for password_attempt in range(3):
                    if not password:
                        try:
                            password = getpass.getpass(
                                f"Enter {profile} 2FA password: "
                            )
                        except EOFError as exc:
                            raise RuntimeError(
                                "2FA password required, no registered credential, and stdin "
                                "is unavailable. Run this command in an interactive Terminal."
                            ) from exc
                    try:
                        user = await client.sign_in(password=password)
                        print("INFO: 2FA password accepted, login complete.")
                        break
                    except telethon.errors.rpcerrorlist.PasswordHashInvalidError:
                        password = None
                        print(
                            "ERROR: Telegram rejected the 2FA password "
                            f"(attempt {password_attempt + 1}/3)."
                        )
                if user is None:
                    raise RuntimeError("Telegram rejected the 2FA password three times")
                break
        if user is None:
            raise RuntimeError(
                f"QR login timed out after {max_attempts * 30}s без сканирования. "
                "Запусти скрипт заново и сканируй сразу после того как PNG откроется."
            )

        session_string = client.session.save()
        source = _store_session_and_verify(names["session"], session_string)

        username = (
            f"@{user.username}" if getattr(user, "username", None) else "no-username"
        )
        full_name = " ".join(
            part
            for part in [
                getattr(user, "first_name", None),
                getattr(user, "last_name", None),
            ]
            if part
        )
        print(
            f"SUCCESS: {profile} session updated for "
            f"{username} ({full_name or 'no-name'}, id={user.id})"
        )
        print(
            f"INFO: Saved and verified new session through {source} "
            f"for Keychain key {names['session']}"
        )
    finally:
        await client.disconnect()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--profile",
        default="lisa",
        help="Registered Telegram profile (for example: lisa or ikrasinsky).",
    )
    parser.add_argument("--qr-out", type=Path, help="Optional QR PNG output path.")
    parser.add_argument("--max-attempts", type=int, default=12)
    args = parser.parse_args()
    if args.max_attempts < 1:
        parser.error("--max-attempts must be at least 1")
    asyncio.run(
        update_session_via_qr(
            args.profile,
            qr_path=args.qr_out,
            max_attempts=args.max_attempts,
        )
    )


if __name__ == "__main__":
    main()
