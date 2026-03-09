#!/usr/bin/env python3
"""
Telegram MCP Server
Basic implementation for MCP protocol
"""

import json
import sys


def main():
    """Main entry point for Telegram MCP server."""
    print("Telegram MCP Server starting...")

    # Basic MCP protocol implementation
    try:
        while True:
            line = sys.stdin.readline()
            if not line:
                break

            # Parse MCP message
            try:
                message = json.loads(line.strip())
                print(f"Received: {message}", file=sys.stderr)

                # Echo back for now
                response = {
                    "jsonrpc": "2.0",
                    "id": message.get("id"),
                    "result": {"status": "ok"},
                }
                print(json.dumps(response))
                sys.stdout.flush()

            except json.JSONDecodeError:
                print(f"Invalid JSON: {line}", file=sys.stderr)
                continue

    except KeyboardInterrupt:
        print("Telegram MCP Server stopped", file=sys.stderr)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
