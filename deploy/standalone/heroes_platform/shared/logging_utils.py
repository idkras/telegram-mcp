"""Small logging adapter used by the standalone Telegram MCP checkout."""

from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path


def add_rotating_file_handler(
    logger: logging.Logger,
    file_path: str | Path,
    *,
    level: int = logging.INFO,
    max_bytes: int = 5 * 1024 * 1024,
    backup_count: int = 5,
    formatter: logging.Formatter | None = None,
    encoding: str = "utf-8",
) -> RotatingFileHandler:
    target = Path(file_path).resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    handler = RotatingFileHandler(
        target,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding=encoding,
    )
    handler.setLevel(level)
    handler.setFormatter(formatter or logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s"))
    logger.addHandler(handler)
    return handler
