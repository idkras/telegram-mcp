#!/usr/bin/env python3
"""
Telegram Session String Generator

This script generates a session string that can be used for Telegram authentication
with the Telegram MCP server. The session string allows for portable authentication
without storing session files.

Usage:
    python session_string_generator.py

The API credentials and generated session are handled by the canonical
registry-only credential API. Secret values are never printed or written to
``.env``.

Note on ID Formats:
When using the MCP server, please be aware that all `chat_id` and `user_id`
parameters support integer IDs, string representations of IDs (e.g., "123456"),
and usernames (e.g., "@mychannel").
"""

import asyncio
import sys

from heroes_platform.heroes_telegram_mcp.session_manager import create_telegram_session


def main() -> None:
    profile = input("Telegram profile [default]: ").strip() or "default"
    success, _session, error = asyncio.run(create_telegram_session(profile))
    if not success:
        print(f"Session creation failed: {error}", file=sys.stderr)
        sys.exit(1)
    print("Authentication succeeded; session stored through the credential registry.")


if __name__ == "__main__":
    main()
