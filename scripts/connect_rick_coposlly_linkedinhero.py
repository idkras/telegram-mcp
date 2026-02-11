#!/usr/bin/env python3
"""
Telegram Session Creator for Rick Coposlly LinkedinHero
Создает session token для пользователя rick-coposlly-linkedinhero

📚 CODEBASE REFERENCES:
- heroes_platform/telegram_mcp/session_manager.py - универсальный модуль для создания сессий
- heroes_platform/telegram_mcp/update_session.py - пример для default профиля
- heroes_platform/shared/credentials_manager.py - управление credentials
- heroes_platform/shared/credentials_wrapper.py - маппинг профилей

JTBD: Как пользователь, я хочу подключиться к Telegram под аккаунтом rick-coposlly-linkedinhero,
чтобы получить active session для работы через Telegram MCP.
"""

import asyncio
import sys
from pathlib import Path

# Add the heroes_platform directory to Python path
heroes_platform_path = Path(__file__).parent.parent.parent.parent
if str(heroes_platform_path) not in sys.path:
    sys.path.insert(0, str(heroes_platform_path))

from heroes_platform.telegram_mcp.session_manager import create_telegram_session  # type: ignore


async def create_session_for_rick_coposlly_linkedinhero():
    """Create a new Telegram session token for rick-coposlly-linkedinhero"""
    print("INFO: Starting Telegram session creation for rick-coposlly-linkedinhero...")
    
    success, session_string, error = await create_telegram_session("rick-coposlly-linkedinhero")
    
    if success:
        print("SUCCESS: Session token created and saved!")
        print(f"INFO: Session token: {session_string[:50] if session_string else 'N/A'}...")
        return True
    else:
        print(f"ERROR: {error}")
        return False


async def main():
    """Main function"""
    print("=== TELEGRAM SESSION CREATOR FOR RICK COPOSLLY LINKEDINHERO ===")
    print("This script will create a new Telegram session token")
    print("and update it in the keychain.")
    print()

    success = await create_session_for_rick_coposlly_linkedinhero()

    if success:
        print()
        print("SUCCESS: Session token created and saved successfully!")
        print("INFO: You can now use TELEGRAM_USER=rick-coposlly-linkedinhero in .cursor/mcp.json")
    else:
        print()
        print("ERROR: Failed to create session token.")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
