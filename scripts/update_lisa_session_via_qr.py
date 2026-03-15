#!/usr/bin/env python3
"""
QR-based Telegram session updater for Lisa profile.

Creates a fresh Telegram login token, renders a QR image, waits for scan from an
already authorized Lisa device, and saves the resulting StringSession to Keychain.
"""

import asyncio
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

from heroes_platform.heroes_telegram_mcp.session_manager import (  # type: ignore
    _mask_phone,
    get_profile_credential_names,
)
from heroes_platform.shared.credentials_manager import credentials_manager  # type: ignore


def _require_credential(key: str) -> str:
    result = credentials_manager.get_credential(key)
    if not result.success or not result.value:
        raise RuntimeError(f"Missing required credential: {key}")
    return str(result.value)


def _qr_output_path() -> Path:
    logs_dir = heroes_platform_path / "heroes_platform" / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    return logs_dir / "lisa_telegram_qr_login.png"


async def main() -> None:
    profile = "lisa"
    names = get_profile_credential_names(profile)
    api_id = int(_require_credential(names["api_id"]))
    api_hash = _require_credential(names["api_hash"])
    phone = _require_credential(names["phone"])

    print("=== LISA TELEGRAM QR SESSION UPDATER ===")
    print(
        "INFO: Using Telegram profile "
        f"{profile} with credential keys "
        f"api_id={names['api_id']}, api_hash={names['api_hash']}, "
        f"session={names['session']}, phone={names['phone']}"
    )
    print(f"INFO: Loaded phone from credential {names['phone']}: {_mask_phone(phone)}")

    client = TelegramClient(StringSession(), api_id, api_hash)
    await client.connect()
    try:
        if await client.is_user_authorized():
            session_string = client.session.save()
            credentials_manager.store_credential(names["session"], session_string, "keychain")
            print("INFO: Fresh client was already authorized; session saved without QR flow.")
            return

        qr_login = await client.qr_login()
        qr_path = _qr_output_path()
        image = qrcode.make(qr_login.url)
        image.save(qr_path)
        url_path = qr_path.with_suffix(".txt")
        url_path.write_text(qr_login.url, encoding="utf-8")

        print(f"INFO: QR login image saved to: {qr_path}")
        print(f"INFO: Raw tg:// login URL saved to: {url_path}")
        print(f"INFO: Raw tg:// login URL: {qr_login.url}")
        print(f"INFO: QR token expires at: {qr_login.expires.isoformat()}")
        print("INFO: Open Telegram on Lisa's already logged-in device, go to Settings -> Devices -> Link Desktop Device, and scan this QR.")
        print("INFO: Alternative: open the raw tg:// URL on a device where Lisa Telegram is already logged in.")

        try:
            import subprocess

            subprocess.run(["open", str(qr_path)], check=False)
        except Exception:
            pass

        try:
            user = await qr_login.wait()
        except telethon.errors.rpcerrorlist.SessionPasswordNeededError:
            password = input("Enter Lisa 2FA password: ")
            user = await client.sign_in(password=password)

        session_string = client.session.save()
        saved = credentials_manager.store_credential(names["session"], session_string, "keychain")
        if not saved:
            raise RuntimeError("QR login succeeded, but failed to save lisa_tg_session to Keychain")

        username = f"@{user.username}" if getattr(user, "username", None) else "no-username"
        full_name = " ".join(part for part in [getattr(user, "first_name", None), getattr(user, "last_name", None)] if part)
        print(f"SUCCESS: Lisa session updated for {username} ({full_name or 'no-name'}, id={user.id})")
        print(f"INFO: Saved new session to Keychain key {names['session']}")
    finally:
        await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
