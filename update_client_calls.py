#!/usr/bin/env python3
"""
Script to update all client calls to use ensure_client_initialized()
"""
import re


def update_client_calls():
    """Update all direct client calls to use ensure_client_initialized()"""

    with open("main.py") as f:
        content = f.read()

    # Pattern to match: await client.something()
    pattern = r"await client\."
    replacement = "await (await ensure_client_initialized())."

    # Replace all occurrences
    updated_content = re.sub(pattern, replacement, content)

    # Also handle cases where client is used without await
    pattern2 = r"(?<!await )client\."
    replacement2 = r"(await ensure_client_initialized())."

    # But be careful not to double-replace
    updated_content = re.sub(pattern2, replacement2, updated_content)

    # Fix double await issues
    updated_content = re.sub(
        r"await \(await ensure_client_initialized\(\)\)\.",
        "await (await ensure_client_initialized()).",
        updated_content,
    )

    with open("main.py", "w") as f:
        f.write(updated_content)

    print("✅ Updated all client calls to use ensure_client_initialized()")


if __name__ == "__main__":
    update_client_calls()
