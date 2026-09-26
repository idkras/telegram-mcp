#!/usr/bin/env python3
"""Canonicalize a legacy Telegram env into the secret-only archive payload."""

from __future__ import annotations

import argparse
import importlib.util
import os
import sys
import uuid
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
INSTALLER_PATH = ROOT / "deploy" / "install-encrypted-credential.py"
sys.path.insert(0, str(INSTALLER_PATH.parent))


def _load_installer():
    spec = importlib.util.spec_from_file_location("telegram_credential_installer", INSTALLER_PATH)
    if not spec or not spec.loader:
        raise RuntimeError("credential installer module cannot be loaded")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def normalize_in_place(path: Path) -> tuple[str, ...]:
    installer = _load_installer()
    canonical = installer.canonicalize_payload(path.read_bytes())
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.new")
    try:
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(canonical)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        os.chmod(path, 0o600)
    finally:
        if temporary.exists():
            temporary.unlink()
    return installer.REQUIRED_SECRET_KEYS


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("path", type=Path)
    args = parser.parse_args()
    try:
        keys = normalize_in_place(args.path)
    except (OSError, RuntimeError, ValueError) as exc:
        print(f"secret normalization refused: {exc}", file=sys.stderr)
        return 1
    print(f"normalized=secret-only required_keys={','.join(keys)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
