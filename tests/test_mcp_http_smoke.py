from __future__ import annotations

import importlib.util
from pathlib import Path

import httpx
import pytest


MODULE_PATH = Path(__file__).resolve().parents[1] / "deploy" / "mcp_http_smoke.py"
SPEC = importlib.util.spec_from_file_location("mcp_http_smoke", MODULE_PATH)
assert SPEC and SPEC.loader
SMOKE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SMOKE)


def test_payload_parses_json_and_sse():
    request = httpx.Request("POST", "http://127.0.0.1/mcp")
    json_response = httpx.Response(200, json={"result": {"tools": []}}, request=request)
    sse_response = httpx.Response(
        200,
        text='event: message\ndata: {"result":{"tools":[]}}\n\n',
        headers={"content-type": "text/event-stream"},
        request=request,
    )

    assert SMOKE._payload(json_response) == {"result": {"tools": []}}
    assert SMOKE._payload(sse_response) == {"result": {"tools": []}}


def test_payload_rejects_sse_without_data():
    response = httpx.Response(
        200,
        text="event: ping\n\n",
        headers={"content-type": "text/event-stream"},
        request=httpx.Request("POST", "http://127.0.0.1/mcp"),
    )
    with pytest.raises(ValueError, match="no data event"):
        SMOKE._payload(response)
