#!/usr/bin/env python3
"""Honest CLI-probe helpers for main.py (pr-hero-i5i, R2 fail-open fixes).

main.py cannot be imported in a test (it bootstraps heroes_platform + real
Telegram credentials at import time). The *logic* of the fail-open CLI probes is
therefore extracted here into a dependency-free, pure module that main.py imports
and delegates to. Kept import-light on purpose: NO telethon, NO credential
bootstrap — so it is importable from tests and from main.py's __main__ block
without side effects.

Fixes:
  M2  list_tools_listing / render_list_tools — read the real FastMCP registry,
      print the real count + names, instead of a hardcoded «73+».
  M1  run_test_probe — --test must connect + is_user_authorized(), not just
      «client is not None» (which is truthy even on a revoked session).
  M3  run_runtime_healthcheck — a non-LABA endpoint must still verify the session,
      not blindly return (True, "OK").
"""
from __future__ import annotations

from typing import Any, Optional, Tuple


# ---------------------------------------------------------------------------
# M2 — --list-tools reads the real registry
# ---------------------------------------------------------------------------
def _extract_tool_names(tool_manager: Any) -> list[str]:
    """Best-effort extraction of tool names from a FastMCP tool manager.

    Supports the two shapes FastMCP has shipped: `.list_tools()` returning objects
    with `.name`, and a `._tools` dict keyed by name. Returns [] if neither works.
    """
    # Preferred: list_tools()
    lister = getattr(tool_manager, "list_tools", None)
    if callable(lister):
        try:
            tools = lister()
            names = []
            for t in tools:
                name = getattr(t, "name", None)
                if name is None and isinstance(t, dict):
                    name = t.get("name")
                if name:
                    names.append(str(name))
            if names:
                return names
        except Exception:  # noqa: BLE001 — fall through to dict form
            pass
    # Fallback: internal ._tools mapping
    tools_map = getattr(tool_manager, "_tools", None)
    if isinstance(tools_map, dict):
        return [str(k) for k in tools_map.keys()]
    return []


def list_tools_listing(tool_manager: Any) -> dict:
    """Return {count, names} from the live registry. Bug M2: the old --list-tools
    printed a hardcoded ~12 names + «Total: 73+», which drifted from the real 78
    @mcp.tool() registrations — a false, self-congratulatory count. This reads the
    actual registry so the number can never lie."""
    names = _extract_tool_names(tool_manager)
    return {"count": len(names), "names": sorted(names)}


def render_list_tools(listing: dict) -> str:
    names = listing.get("names", [])
    count = listing.get("count", 0)
    lines = ["Available Telegram MCP Tools:", "=" * 50]
    for i, name in enumerate(names, 1):
        lines.append(f"{i:>3}. {name}")
    lines.append("=" * 50)
    lines.append(f"Total: {count} tools available")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# M1 — --test must verify authorization
# ---------------------------------------------------------------------------
async def run_test_probe(client: Any) -> Tuple[bool, str]:
    """--test smoke check. Bug M1: the old probe only checked `client is not None`
    — the client object is constructed unconditionally at import, so --test printed
    OK even on a revoked/expired session. Now it connects and calls
    is_user_authorized(); a non-authorized session fails (exit 1)."""
    if client is None:
        return False, "Telegram MCP client not initialized"
    try:
        await client.connect()
        try:
            authorized = await client.is_user_authorized()
        except Exception as exc:  # noqa: BLE001
            return False, f"Telegram MCP --test: is_user_authorized() raised {exc}"
        if not authorized:
            return False, "Telegram session present but NOT authorized (revoked/expired/AuthKeyDuplicated)"
        return True, "OK"
    except Exception as exc:  # noqa: BLE001
        return False, f"Telegram MCP test failed: {exc}"
    finally:
        try:
            if client.is_connected():
                await client.disconnect()
        except Exception:  # noqa: BLE001 — disconnect best-effort
            pass


# ---------------------------------------------------------------------------
# M3 — healthcheck on a non-LABA endpoint must still verify the session
# ---------------------------------------------------------------------------
async def run_runtime_healthcheck(
    laba_mode: bool,
    client: Any = None,
    laba_probe: Optional[Any] = None,
) -> Tuple[Any, str]:
    """Runtime healthcheck.

    Bug M3: the old code short-circuited to (True, "Telegram runtime OK (LABA_MODE
    disabled)") whenever LABA_MODE != "true" — i.e. every local endpoint reported
    healthy even with a dead session. Now a non-LABA endpoint still verifies the
    session (connect + is_user_authorized): revoked → (False, ...); authorized →
    (True, ...); unable to probe → (None, INCONCLUSIVE) rather than a blind OK.

    laba_mode=True delegates to `laba_probe()` (the existing Supabase reachability
    coroutine), preserving prior behaviour.
    """
    if laba_mode:
        if laba_probe is None:
            return None, "LABA runtime probe not wired (INCONCLUSIVE)"
        return await laba_probe()

    # non-LABA: verify the session instead of blindly returning OK
    if client is None:
        return None, "no client to probe (INCONCLUSIVE)"
    try:
        await client.connect()
        try:
            authorized = await client.is_user_authorized()
        except Exception as exc:  # noqa: BLE001
            return None, f"session probe INCONCLUSIVE: is_user_authorized() raised {exc}"
        if not authorized:
            return False, "session NOT authorized (revoked/expired/AuthKeyDuplicated)"
        return True, "OK"
    except Exception as exc:  # noqa: BLE001
        return None, f"session probe INCONCLUSIVE: {exc}"
    finally:
        try:
            if client.is_connected():
                await client.disconnect()
        except Exception:  # noqa: BLE001
            pass
