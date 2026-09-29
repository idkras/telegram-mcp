"""MCP image delivery must not mistake a server path for a client image."""

import base64
import os
from types import SimpleNamespace

import pytest

os.environ.setdefault("TELEGRAM_API_ID", "12345")
os.environ.setdefault("TELEGRAM_API_HASH", "dummy_hash")

import main


class FakeClient:
    def __init__(self, message, data=b"\xff\xd8\xff\xd9"):
        self.message = message
        self.data = data
        self.download_calls = 0

    async def get_messages(self, entity, ids):
        return self.message

    async def download_media(self, message, file):
        assert file is bytes
        self.download_calls += 1
        return self.data


async def fake_resolve(chat_id, tg_client):
    return chat_id


@pytest.mark.asyncio
async def test_photo_is_delivered_as_mcp_image_not_server_path(monkeypatch):
    message = SimpleNamespace(
        media=object(),
        photo=SimpleNamespace(sizes=[SimpleNamespace(size=4)]),
        document=None,
    )
    fake = FakeClient(message)
    monkeypatch.setattr(main, "client", fake)
    monkeypatch.setattr(main, "_resolve_chat_entity", fake_resolve)

    blocks = await main.mcp.call_tool(
        "get_media_image", {"chat_id": 1827468065, "message_id": 12424}
    )

    assert len(blocks) == 1
    assert blocks[0].type == "image"
    assert blocks[0].mimeType == "image/jpeg"
    assert base64.b64decode(blocks[0].data) == fake.data
    assert fake.download_calls == 1


@pytest.mark.asyncio
async def test_non_image_and_oversize_do_not_download(monkeypatch):
    monkeypatch.setattr(main, "_resolve_chat_entity", fake_resolve)
    non_image = FakeClient(SimpleNamespace(media=object(), photo=None, document=None))
    monkeypatch.setattr(main, "client", non_image)
    blocks = await main.mcp.call_tool(
        "get_media_image", {"chat_id": 1, "message_id": 2}
    )
    assert blocks[0].type == "text"
    assert "does not contain a supported image" in blocks[0].text
    assert non_image.download_calls == 0

    oversize = FakeClient(
        SimpleNamespace(
            media=object(),
            photo=SimpleNamespace(
                sizes=[SimpleNamespace(size=main.MAX_INLINE_IMAGE_BYTES + 1)]
            ),
            document=None,
        )
    )
    monkeypatch.setattr(main, "client", oversize)
    blocks = await main.mcp.call_tool(
        "get_media_image", {"chat_id": 1, "message_id": 3}
    )
    assert blocks[0].type == "text"
    assert "inline limit" in blocks[0].text
    assert oversize.download_calls == 0


@pytest.mark.asyncio
async def test_mislabeled_download_is_not_returned_as_image(monkeypatch):
    message = SimpleNamespace(
        media=object(),
        photo=None,
        document=SimpleNamespace(mime_type="image/png", size=16),
    )
    fake = FakeClient(message, data=b"not-an-image")
    monkeypatch.setattr(main, "client", fake)
    monkeypatch.setattr(main, "_resolve_chat_entity", fake_resolve)

    blocks = await main.mcp.call_tool(
        "get_media_image", {"chat_id": 1, "message_id": 4}
    )

    assert blocks[0].type == "text"
    assert "not a supported image" in blocks[0].text
