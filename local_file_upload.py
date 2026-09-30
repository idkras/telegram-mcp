"""Private, bounded staging for files uploaded through the Telegram MCP endpoint.

Only the caller's bytes cross the MCP transport.  A client-side path is never
interpreted on the server.  Each upload has a random ID and a private directory.
"""

from __future__ import annotations

import base64
import binascii
import fcntl
import hashlib
import json
import os
import re
import shutil
import stat
import tempfile
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


MAX_FILE_BYTES = 64 * 1024 * 1024
MAX_CHUNK_BYTES = 256 * 1024
MAX_ACTIVE_UPLOADS = 16
UPLOAD_TTL_SECONDS = 60 * 60
SENDING_TTL_SECONDS = 24 * 60 * 60
_ID = re.compile(r"[0-9a-f]{32}\Z")
_SHA256 = re.compile(r"[0-9a-fA-F]{64}\Z")


class UploadStore:
    def __init__(self, root: Path | None = None) -> None:
        self.root = root or Path(
            os.environ.get(
                "TELEGRAM_MCP_UPLOAD_DIR",
                str(Path.home() / ".cache" / "telegram-mcp" / "uploads"),
            )
        )
        if self.root.is_symlink():
            raise RuntimeError("Upload directory must not be a symlink")
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        if stat.S_IMODE(self.root.stat().st_mode) != 0o700:
            raise RuntimeError("Upload directory must have mode 0700")

    @staticmethod
    def _filename(name: str) -> str:
        if (
            not name
            or name in {".", ".."}
            or name in {"meta.json", "payload.part", ".lock"}
            or name.startswith(".")
            or "/" in name
            or "\\" in name
            or any(ord(char) < 32 or ord(char) == 127 for char in name)
            or len(name.encode("utf-8")) > 255
        ):
            raise ValueError("filename must be a safe basename")
        return name

    def _directory(self, upload_id: str) -> Path:
        if not _ID.fullmatch(upload_id):
            raise ValueError("invalid upload_id")
        directory = self.root / upload_id
        if not directory.is_dir() or directory.is_symlink():
            raise ValueError("unknown upload_id")
        return directory

    @staticmethod
    @contextmanager
    def _lock(path: Path) -> Iterator[None]:
        fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            yield
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    @staticmethod
    def _read(directory: Path) -> dict:
        with (directory / "meta.json").open(encoding="utf-8") as stream:
            return json.load(stream)

    @staticmethod
    def _write(directory: Path, meta: dict) -> None:
        fd, name = tempfile.mkstemp(prefix=".meta-", dir=directory)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as stream:
                json.dump(meta, stream, ensure_ascii=False, sort_keys=True)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(name, directory / "meta.json")
        finally:
            if os.path.exists(name):
                os.unlink(name)

    def cleanup_expired(self) -> None:
        """Remove expired payloads and receipts, including crash orphans."""
        now = time.time()
        for directory in self.root.iterdir():
            if not directory.is_dir() or directory.is_symlink() or not _ID.fullmatch(directory.name):
                continue
            directory_age = now - directory.stat().st_mtime
            with self._lock(directory / ".lock"):
                try:
                    meta = self._read(directory)
                except (OSError, ValueError):
                    if directory_age > UPLOAD_TTL_SECONDS:
                        shutil.rmtree(directory)
                    continue
                ttl = SENDING_TTL_SECONDS if meta["state"] in {"sending", "uncertain"} else UPLOAD_TTL_SECONDS
                if now - meta.get("updated_at", meta["created_at"]) > ttl:
                    shutil.rmtree(directory)

    def begin(self, filename: str, size_bytes: int, sha256: str) -> dict:
        filename = self._filename(filename)
        if not isinstance(size_bytes, int) or isinstance(size_bytes, bool) or not 0 < size_bytes <= MAX_FILE_BYTES:
            raise ValueError(f"size_bytes must be between 1 and {MAX_FILE_BYTES}")
        if not _SHA256.fullmatch(sha256):
            raise ValueError("sha256 must be 64 hexadecimal characters")
        with self._lock(self.root / ".global.lock"):
            self.cleanup_expired()
            active = 0
            for directory in self.root.iterdir():
                if not directory.is_dir() or not _ID.fullmatch(directory.name):
                    continue
                try:
                    state = self._read(directory).get("state")
                except (OSError, ValueError):
                    state = "uploading"  # a fresh crash orphan still consumes quota
                if state in {"uploading", "ready", "sending", "uncertain"}:
                    active += 1
            if active >= MAX_ACTIVE_UPLOADS:
                raise ValueError("too many active uploads")
            upload_id = uuid.uuid4().hex
            directory = self.root / upload_id
            directory.mkdir(mode=0o700)
            fd = os.open(directory / "payload.part", os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            os.close(fd)
            self._write(directory, {
                "upload_id": upload_id,
                "filename": filename,
                "size_bytes": size_bytes,
                "sha256": sha256.lower(),
                "created_at": time.time(),
                "updated_at": time.time(),
                "state": "uploading",
            })
        return {"upload_id": upload_id, "max_chunk_bytes": MAX_CHUNK_BYTES, "size_bytes": size_bytes}

    def append(self, upload_id: str, offset: int, chunk_base64: str) -> dict:
        try:
            chunk = base64.b64decode(chunk_base64, validate=True)
        except (binascii.Error, ValueError) as exc:
            raise ValueError("chunk_base64 is invalid") from exc
        if not chunk or len(chunk) > MAX_CHUNK_BYTES:
            raise ValueError(f"chunk must contain 1..{MAX_CHUNK_BYTES} bytes")
        directory = self._directory(upload_id)
        with self._lock(directory / ".lock"):
            meta = self._read(directory)
            if meta["state"] != "uploading":
                raise ValueError(f"upload is {meta['state']}")
            part = directory / "payload.part"
            current = part.stat().st_size
            if offset != current or current + len(chunk) > meta["size_bytes"]:
                raise ValueError(f"invalid offset or size; received={current}")
            with part.open("ab") as stream:
                stream.write(chunk)
                stream.flush()
                os.fsync(stream.fileno())
            meta["updated_at"] = time.time()
            self._write(directory, meta)
            return {"upload_id": upload_id, "received_bytes": current + len(chunk)}

    def complete(self, upload_id: str) -> dict:
        directory = self._directory(upload_id)
        with self._lock(directory / ".lock"):
            meta = self._read(directory)
            if meta["state"] != "uploading":
                raise ValueError(f"upload is {meta['state']}")
            part = directory / "payload.part"
            if part.stat().st_size != meta["size_bytes"]:
                raise ValueError(f"incomplete upload; received={part.stat().st_size}")
            digest = hashlib.sha256()
            with part.open("rb") as stream:
                for block in iter(lambda: stream.read(1024 * 1024), b""):
                    digest.update(block)
            if digest.hexdigest() != meta["sha256"]:
                meta["state"] = "rejected"
                meta["updated_at"] = time.time()
                self._write(directory, meta)
                part.unlink()
                raise ValueError("sha256 mismatch; upload rejected")
            os.replace(part, directory / meta["filename"])
            meta["state"] = "ready"
            meta["updated_at"] = time.time()
            self._write(directory, meta)
            return {key: meta[key] for key in ("upload_id", "filename", "size_bytes", "sha256", "state")}

    def prepare_send(self, upload_id: str) -> tuple[Path | None, dict]:
        directory = self._directory(upload_id)
        with self._lock(directory / ".lock"):
            meta = self._read(directory)
            if meta["state"] == "sent":
                return None, meta
            if meta["state"] != "ready":
                raise ValueError(f"upload is {meta['state']}; send refused")
            path = directory / meta["filename"]
            if not path.is_file() or path.stat().st_size != meta["size_bytes"]:
                raise ValueError("staged file is missing or changed")
            meta["state"] = "sending"
            meta["updated_at"] = time.time()
            self._write(directory, meta)
            return path, meta

    def record_sent(self, upload_id: str, receipt: dict) -> dict:
        directory = self._directory(upload_id)
        with self._lock(directory / ".lock"):
            meta = self._read(directory)
            if meta["state"] != "sending":
                raise ValueError(f"upload is {meta['state']}")
            meta["state"] = "sent"
            meta["receipt"] = receipt
            meta["updated_at"] = time.time()
            self._write(directory, meta)
            (directory / meta["filename"]).unlink(missing_ok=True)
            return receipt

    def record_uncertain(self, upload_id: str) -> None:
        directory = self._directory(upload_id)
        with self._lock(directory / ".lock"):
            meta = self._read(directory)
            if meta["state"] == "sending":
                meta["state"] = "uncertain"
                meta["updated_at"] = time.time()
                self._write(directory, meta)

    def status(self, upload_id: str) -> dict:
        directory = self._directory(upload_id)
        with self._lock(directory / ".lock"):
            meta = self._read(directory)
            result = {key: meta[key] for key in ("upload_id", "filename", "size_bytes", "sha256", "state")}
            if "receipt" in meta:
                result["receipt"] = meta["receipt"]
            return result


async def send_verified_upload(
    *,
    store: UploadStore,
    telegram_client: object,
    resolve_chat: object,
    chat_id: int,
    upload_id: str,
    caption: str | None,
    profile: str,
) -> dict:
    """Send one verified document and preserve an unambiguous retry boundary."""
    entity = await resolve_chat(chat_id, tg_client=telegram_client)
    path, meta = store.prepare_send(upload_id)
    if path is None:
        receipt = meta["receipt"]
        if receipt["chat_id"] != chat_id or receipt["profile"] != profile:
            raise ValueError("upload was already sent to a different destination")
        return receipt
    try:
        sent = await telegram_client.send_file(entity, str(path), caption=caption, force_document=True)
        message_id = getattr(sent, "id", None)
        if not isinstance(message_id, int):
            raise RuntimeError("Telegram did not return a message ID; delivery is uncertain")
    except Exception:
        store.record_uncertain(upload_id)
        raise

    observed_name = None
    observed_size = None
    try:
        readback = await telegram_client.get_messages(entity, ids=message_id)
        remote_file = getattr(readback, "file", None)
        observed_name = getattr(remote_file, "name", None)
        observed_size = getattr(remote_file, "size", None)
        confirmed = (
            getattr(readback, "id", None) == message_id
            and observed_name == meta["filename"]
            and observed_size == meta["size_bytes"]
        )
    except Exception:
        confirmed = False
    receipt = {
        "chat_id": chat_id,
        "message_id": message_id,
        "profile": profile,
        "upload_id": upload_id,
        "filename": meta["filename"],
        "size_bytes": meta["size_bytes"],
        "sha256_before_send": meta["sha256"],
        "readback_confirmed": confirmed,
        "readback_filename": observed_name,
        "readback_size_bytes": observed_size,
    }
    return store.record_sent(upload_id, receipt)
