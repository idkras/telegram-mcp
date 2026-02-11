#!/usr/bin/env python3
"""
Telegram Session Token Updater
Создает новый session token для telegram-mcp (default profile: ikrasinsky)

📚 CODEBASE REFERENCES:
- heroes_platform/telegram_mcp/session_manager.py - универсальный модуль для создания сессий
- heroes_platform/telegram_mcp/connect_rick_coposlly_linkedinhero.py - пример для другого профиля
- heroes_platform/shared/credentials_manager.py - управление credentials
- heroes_platform/shared/credentials_wrapper.py - маппинг профилей на credential names

JTBD: Как пользователь, я хочу обновить telegram session token,
чтобы telegram-mcp работал с полной функциональностью.
"""

import asyncio
import sys
from pathlib import Path

# Add the heroes_platform directory to Python path
heroes_platform_path = Path(__file__).parent.parent.parent.parent
if str(heroes_platform_path) not in sys.path:
    sys.path.insert(0, str(heroes_platform_path))

from heroes_platform.telegram_mcp.session_manager import create_telegram_session  # type: ignore


async def create_new_session():
    """Create a new Telegram session token for default profile (ikrasinsky)"""
    print("INFO: Starting Telegram session token update...")
    
    # Use default profile (ikrasinsky)
    success, session_string, error = await create_telegram_session("ikrasinsky")
    
    if success:
        print("SUCCESS: Session token created and saved!")
        print(f"INFO: Session token: {session_string[:50] if session_string else 'N/A'}...")
        return True
    else:
        print(f"ERROR: {error}")
        return False


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
