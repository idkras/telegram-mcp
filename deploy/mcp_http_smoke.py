#!/usr/bin/env python3
"""Protocol-level smoke check for a loopback Streamable HTTP MCP endpoint."""

from __future__ import annotations

import argparse
import asyncio
import json
import time
from typing import Any

import httpx


def _payload(response: httpx.Response) -> dict[str, Any]:
    content_type = response.headers.get("content-type", "")
    if "text/event-stream" not in content_type:
        return response.json()
    for line in response.text.splitlines():
        if line.startswith("data:"):
            return json.loads(line.removeprefix("data:").strip())
    raise ValueError("SSE response contained no data event")


async def smoke(url: str, required_tools: set[str], timeout: float) -> dict[str, Any]:
    started = time.monotonic()
    headers = {
        "accept": "application/json, text/event-stream",
        "content-type": "application/json",
    }
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            initialized = await client.post(
                url,
                headers=headers,
                json={
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "initialize",
                    "params": {
                        "protocolVersion": "2025-03-26",
                        "capabilities": {},
                        "clientInfo": {"name": "telegram-mcp-smoke", "version": "1"},
                    },
                },
            )
            initialized.raise_for_status()
            init_body = _payload(initialized)
            session_id = initialized.headers.get("mcp-session-id")
            if init_body.get("error") or not session_id:
                raise RuntimeError(f"initialize failed: {init_body.get('error') or 'missing session id'}")
            session_headers = {**headers, "mcp-session-id": session_id}
            notice = await client.post(
                url,
                headers=session_headers,
                json={"jsonrpc": "2.0", "method": "notifications/initialized"},
            )
            notice.raise_for_status()
            listed = await client.post(
                url,
                headers=session_headers,
                json={"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}},
            )
            listed.raise_for_status()
            list_body = _payload(listed)
            tools = {
                str(item.get("name"))
                for item in (list_body.get("result") or {}).get("tools", [])
                if item.get("name")
            }
            missing = sorted(required_tools - tools)
            health = None
            health_error = ""
            if "get_ingest_health" in tools:
                called = await client.post(
                    url,
                    headers=session_headers,
                    json={
                        "jsonrpc": "2.0",
                        "id": 3,
                        "method": "tools/call",
                        "params": {"name": "get_ingest_health", "arguments": {}},
                    },
                )
                called.raise_for_status()
                call_body = _payload(called)
                result = call_body.get("result") or {}
                health = result.get("structuredContent")
                if not isinstance(health, dict):
                    for item in result.get("content") or []:
                        if item.get("type") == "text":
                            try:
                                parsed = json.loads(item.get("text") or "")
                            except (TypeError, ValueError):
                                continue
                            if isinstance(parsed, dict):
                                health = parsed
                                break
                if not isinstance(health, dict):
                    health_error = "get_ingest_health returned no structured JSON"
            return {
                "ok": not missing and bool(tools),
                "initialize": "ok",
                "tools_count": len(tools),
                "missing_tools": missing,
                "health": health,
                "health_error": health_error,
                "latency_ms": round((time.monotonic() - started) * 1000),
            }
    except Exception as exc:  # noqa: BLE001
        return {
            "ok": False,
            "initialize": "failed",
            "tools_count": 0,
            "missing_tools": sorted(required_tools),
            "error": f"{type(exc).__name__}: {exc}"[:240],
            "latency_ms": round((time.monotonic() - started) * 1000),
        }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True)
    parser.add_argument("--required-tool", action="append", default=[])
    parser.add_argument("--timeout", type=float, default=8.0)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    result = asyncio.run(smoke(args.url, set(args.required_tool), args.timeout))
    print(json.dumps(result, ensure_ascii=False) if args.json else result)
    return 0 if result["ok"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
