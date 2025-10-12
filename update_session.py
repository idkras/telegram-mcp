#!/usr/bin/env python3
"""
Telegram Session Token Updater
Создает новый session token для telegram-mcp

JTBD: Как пользователь, я хочу обновить telegram session token,
чтобы telegram-mcp работал с полной функциональностью.
"""

import asyncio
import sys
from pathlib import Path

from shared.credentials_manager import credentials_manager  # type: ignore
from telethon import TelegramClient  # type: ignore
from telethon.sessions import StringSession  # type: ignore

# Add the heroes_platform directory to Python path
heroes_platform_path = Path(__file__).parent.parent
if str(heroes_platform_path) not in sys.path:
    sys.path.insert(0, str(heroes_platform_path))


async def create_new_session():
    """Create a new Telegram session token"""

    print("INFO: Starting Telegram session token update...")

    # Get credentials
    api_id_result = credentials_manager.get_credential("telegram_api_id")
    api_hash_result = credentials_manager.get_credential("telegram_api_hash")

    if not api_id_result.success or not api_hash_result.success:
        print("ERROR: Failed to get API credentials")
        return False

    api_id = int(api_id_result.value)
    api_hash = api_hash_result.value

    print(f"INFO: Using API_ID: {api_id}", file=sys.stderr)
    print(f"INFO: Using API_HASH: {api_hash[:10]}...", file=sys.stderr)

    # Create a new session
    session = StringSession()
    client = TelegramClient(session, api_id, api_hash)

    try:
        print("INFO: Connecting to Telegram...")
        await client.connect()

        if not await client.is_user_authorized():
            print("INFO: User not authorized. Starting authorization...")
            print("INFO: Please check your phone for the verification code.")

            # Start authorization
            phone = input("Enter your phone number (with country code, e.g., +1234567890): ")
            await client.send_code_request(phone)

            code = input("Enter the verification code: ")

            try:
                await client.sign_in(phone, code)
                print("SUCCESS: Authorization successful!")
            except Exception as e:
                if "password" in str(e).lower():
                    password = input("Enter your 2FA password: ")
                    await client.sign_in(password=password)
                    print("SUCCESS: Authorization with 2FA successful!")
                else:
                    print(f"ERROR: Authorization failed: {e}")
                    return False
        else:
            print("SUCCESS: User already authorized!")

        # Get the session string
        session_string = client.session.save()
        print("SUCCESS: New session token created!")
        print(f"INFO: Session token: {session_string[:50]}...")

        # Update credentials
        print("INFO: Updating credentials in keychain...")
        success = credentials_manager.store_credential("telegram_session", session_string, "keychain")

        if success:
            print("SUCCESS: Session token updated in keychain!")
            return True
        else:
            print("ERROR: Failed to update session token in keychain")
            return False

    except Exception as e:
        print(f"ERROR: Failed to create session: {e}")
        return False
    finally:
        await client.disconnect()


async def main():
    """Main function"""
    print("=== TELEGRAM SESSION TOKEN UPDATER ===")
    print("This script will create a new Telegram session token")
    print("and update it in the keychain.")
    print()

    success = await create_new_session()

    if success:
        print()
        print("SUCCESS: Session token updated successfully!")
        print("INFO: You can now restart Cursor to use the new session.")
    else:
        print()
        print("ERROR: Failed to update session token.")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
