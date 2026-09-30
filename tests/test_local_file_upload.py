"""Byte transfer and send boundary; no Telegram credentials or network needed."""

import asyncio
import base64
import hashlib
import json
import os
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from heroes_platform.heroes_telegram_mcp.local_file_upload import (
    UploadStore,
    send_verified_upload,
)


DATA = b"%PDF-1.4\nlocal-client-file\n"


def _begin(store: UploadStore, data: bytes = DATA, sha256: str | None = None) -> str:
    return store.begin("offer.pdf", len(data), sha256 or hashlib.sha256(data).hexdigest())["upload_id"]


def _append(store: UploadStore, upload_id: str, data: bytes = DATA) -> None:
    store.append(upload_id, 0, base64.b64encode(data).decode("ascii"))


class FakeTelegram:
    def __init__(self, *, fail_send: bool = False, wrong_readback: bool = False) -> None:
        self.calls = 0
        self.fail_send = fail_send
        self.wrong_readback = wrong_readback

    async def send_file(self, entity, path, *, caption, force_document):
        self.calls += 1
        assert entity == "chat-42"
        assert Path(path).name == "offer.pdf"
        assert Path(path).read_bytes() == DATA
        assert caption == "invoice"
        assert force_document is True
        if self.fail_send:
            raise RuntimeError("ambiguous Telegram failure")
        return SimpleNamespace(id=71)

    async def get_messages(self, entity, *, ids):
        assert entity == "chat-42" and ids == 71
        name = "wrong.pdf" if self.wrong_readback else "offer.pdf"
        return SimpleNamespace(id=71, file=SimpleNamespace(name=name, size=len(DATA)))


async def _resolve(chat_id, *, tg_client):
    assert chat_id == 42
    return "chat-42"


def _send(store: UploadStore, telegram: FakeTelegram, upload_id: str, chat_id: int = 42):
    return asyncio.run(send_verified_upload(
        store=store,
        telegram_client=telegram,
        resolve_chat=_resolve,
        chat_id=chat_id,
        upload_id=upload_id,
        caption="invoice",
        profile="lisa",
    ))


def test_upload_send_readback_and_retry_return_same_receipt(tmp_path):
    store = UploadStore(tmp_path / "uploads")
    upload_id = _begin(store)
    _append(store, upload_id)
    verified = store.complete(upload_id)
    assert verified["sha256"] == hashlib.sha256(DATA).hexdigest()
    telegram = FakeTelegram()
    receipt = _send(store, telegram, upload_id)
    assert receipt["chat_id"] == 42
    assert receipt["message_id"] == 71
    assert receipt["readback_confirmed"] is True
    assert store.status(upload_id)["receipt"] == receipt
    assert not (tmp_path / "uploads" / upload_id / "offer.pdf").exists()
    assert _send(store, telegram, upload_id) == receipt
    assert telegram.calls == 1


def test_incomplete_and_hash_mismatch_cannot_send(tmp_path):
    store = UploadStore(tmp_path / "uploads")
    incomplete = _begin(store)
    with pytest.raises(ValueError, match="incomplete"):
        store.complete(incomplete)
    with pytest.raises(ValueError, match="uploading"):
        _send(store, FakeTelegram(), incomplete)

    bad_hash = _begin(store, sha256="0" * 64)
    _append(store, bad_hash)
    with pytest.raises(ValueError, match="sha256 mismatch"):
        store.complete(bad_hash)
    assert store.status(bad_hash)["state"] == "rejected"
    with pytest.raises(ValueError, match="rejected"):
        _send(store, FakeTelegram(), bad_hash)


def test_wrong_offset_filename_and_ambiguous_failure(tmp_path):
    store = UploadStore(tmp_path / "uploads")
    with pytest.raises(ValueError, match="safe basename"):
        store.begin("../offer.pdf", len(DATA), hashlib.sha256(DATA).hexdigest())
    for reserved in ("meta.json", "payload.part", ".lock"):
        with pytest.raises(ValueError, match="safe basename"):
            store.begin(reserved, len(DATA), hashlib.sha256(DATA).hexdigest())
    upload_id = _begin(store)
    with pytest.raises(ValueError, match="offset"):
        store.append(upload_id, 1, base64.b64encode(DATA).decode())
    _append(store, upload_id)
    store.complete(upload_id)
    telegram = FakeTelegram(fail_send=True)
    with pytest.raises(RuntimeError, match="ambiguous"):
        _send(store, telegram, upload_id)
    assert store.status(upload_id)["state"] == "uncertain"
    with pytest.raises(ValueError, match="uncertain"):
        _send(store, telegram, upload_id)
    assert telegram.calls == 1


def test_wrong_telegram_readback_is_reported_not_confirmed(tmp_path):
    store = UploadStore(tmp_path / "uploads")
    upload_id = _begin(store)
    _append(store, upload_id)
    store.complete(upload_id)
    receipt = _send(store, FakeTelegram(wrong_readback=True), upload_id)
    assert receipt["message_id"] == 71
    assert receipt["readback_confirmed"] is False
    assert receipt["readback_filename"] == "wrong.pdf"


def test_old_incomplete_directory_is_collected_on_next_upload(tmp_path):
    store = UploadStore(tmp_path / "uploads")
    orphan = store.root / ("a" * 32)
    orphan.mkdir(mode=0o700)
    old = time.time() - 7200
    os.utime(orphan, (old, old))
    _begin(store)
    assert not orphan.exists()


def test_cleanup_uses_last_chunk_time_not_upload_start(tmp_path):
    store = UploadStore(tmp_path / "uploads")
    upload_id = _begin(store)
    directory = store.root / upload_id
    meta_path = directory / "meta.json"
    meta = json.loads(meta_path.read_text())
    meta["created_at"] = time.time() - 7200
    meta_path.write_text(json.dumps(meta))
    store.cleanup_expired()
    assert directory.exists()  # a recent chunk/creation keeps it alive
    meta["updated_at"] = time.time() - 7200
    meta_path.write_text(json.dumps(meta))
    store.cleanup_expired()
    assert not directory.exists()
