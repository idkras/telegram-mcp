#!/usr/bin/env python3
"""
Telegram Session Token Updater for Lisa profile.

Creates a new session token for the Lisa Telegram profile and saves it to Keychain.
"""

import asyncio
import sys
from pathlib import Path

# Add the workspace root so heroes_platform imports resolve.
heroes_platform_path = Path(__file__).parent.parent.parent.parent
if str(heroes_platform_path) not in sys.path:
    sys.path.insert(0, str(heroes_platform_path))

from heroes_platform.heroes_telegram_mcp.session_manager import create_telegram_session  # type: ignore


async def create_new_session() -> bool:
    """Create a new Telegram session token for Lisa profile."""
    print("INFO: Starting Telegram session token update for Lisa...")

    success, session_string, error = await create_telegram_session("lisa")

    if success:
        print("SUCCESS: Lisa session token created and saved!")
        print(f"INFO: Session token: {session_string[:50] if session_string else 'N/A'}...")
        return True

    print(f"ERROR: {error}")
    return False


async def main() -> None:
    """Main function."""
    print("=== LISA TELEGRAM SESSION TOKEN UPDATER ===")
    print("This script will create a new Telegram session token for Lisa")
    print("and update it in the keychain.")
    print()

    success = await create_new_session()

    if success:
        print()
        print("SUCCESS: Lisa session token updated successfully!")
        print("INFO: You can now rerun Telegram auth check.")
        return

    print()
    print("ERROR: Failed to update Lisa session token.")
    sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
