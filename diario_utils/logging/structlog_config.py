from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

# Keep handlers small and safe by default.
_MAX_BYTES = 10_485_760  # 10 MB
_BACKUP_COUNT = 3
_CONFIGURED = False


class BoundLogger:
    """Lightweight bound logger that accepts keyword fields."""

    def __init__(self, logger: logging.Logger, bindings: dict[str, object]):
        self.logger = logger
        self.bindings = bindings

    def bind(self, **bindings: object) -> "BoundLogger":  # pragma: no cover - convenience
        merged = {**self.bindings, **bindings}
        return BoundLogger(self.logger, merged)

    def debug(self, event: str, **fields: object) -> None:
        self._log(logging.DEBUG, event, fields)

    def info(self, event: str, **fields: object) -> None:
        self._log(logging.INFO, event, fields)

    def warning(self, event: str, **fields: object) -> None:
        self._log(logging.WARNING, event, fields)

    def error(self, event: str, **fields: object) -> None:
        self._log(logging.ERROR, event, fields)

    def exception(self, event: str, **fields: object) -> None:
        fields.setdefault("exc_info", True)
        self._log(logging.ERROR, event, fields)

    def _log(self, level: int, event: str, fields: dict[str, object]) -> None:
        payload = {**self.bindings, **fields}
        self.logger.log(level, event, extra={"structured": payload})


def _coerce_level(level: str | int) -> int:
    if isinstance(level, int):
        return level
    return logging._nameToLevel.get(level.upper(), logging.INFO)


def _has_handler_for_path(root: logging.Logger, path: Path) -> bool:
    for handler in root.handlers:
        if hasattr(handler, "baseFilename") and Path(handler.baseFilename) == path:
            return True
    return False


class _JsonFormatter(logging.Formatter):
    """Minimal JSON formatter to mimic structlog output for tests."""

    def format(self, record: logging.LogRecord) -> str:  # noqa: D401
        payload: dict[str, Any] = {
            "timestamp": datetime.now(tz=timezone.utc).isoformat(),
            "level": record.levelname.lower(),
            "event": record.getMessage(),
        }
        structured = getattr(record, "structured", None)
        if isinstance(structured, dict):
            payload.update(structured)
        return json.dumps(payload, ensure_ascii=False)


def _attach_file_handler(root: logging.Logger, log_file: Path, formatter: logging.Formatter, level: int) -> None:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    if _has_handler_for_path(root, log_file):
        return
    file_handler = RotatingFileHandler(log_file, maxBytes=_MAX_BYTES, backupCount=_BACKUP_COUNT)
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)


def configure_structlog(level: str | int = "INFO", log_file: str | Path | None = None) -> None:
    """Configure JSON logging to stdout and optional file. Idempotent."""

    global _CONFIGURED
    level_value = _coerce_level(level)
    formatter = _JsonFormatter()
    root = logging.getLogger()

    if not _CONFIGURED:
        root.handlers.clear()
        console = logging.StreamHandler(sys.stdout)
        console.setLevel(level_value)
        console.setFormatter(formatter)
        root.addHandler(console)
        root.setLevel(level_value)
        _CONFIGURED = True

    if log_file:
        _attach_file_handler(root, Path(log_file), formatter, level_value)


def get_logger(**bindings: object) -> logging.LoggerAdapter:
    """Return a bound logger that accepts keyword fields."""

    if not _CONFIGURED:
        configure_structlog()
    base_logger = logging.getLogger()
    return BoundLogger(base_logger, bindings)
