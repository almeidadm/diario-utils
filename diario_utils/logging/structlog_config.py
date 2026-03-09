from __future__ import annotations

import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

import structlog

# Keep handlers small and safe by default.
_MAX_BYTES = 10_485_760  # 10 MB
_BACKUP_COUNT = 3


def _coerce_level(level: str | int) -> int:
    """Normalize level strings/ints to logging level numbers."""
    if isinstance(level, int):
        return level
    return logging._nameToLevel.get(level.upper(), logging.INFO)


def _clear_root_handlers(root: logging.Logger) -> None:
    """Remove existing handlers to avoid duplicate emission."""
    for handler in list(root.handlers):
        root.removeHandler(handler)
        handler.close()


def _build_formatter() -> structlog.stdlib.ProcessorFormatter:
    """Create a ProcessorFormatter that renders JSON lines."""
    processor_chain = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]
    return structlog.stdlib.ProcessorFormatter(
        processor=structlog.processors.JSONRenderer(),
        foreign_pre_chain=processor_chain,
    )


def _has_handler_for_path(root: logging.Logger, path: Path) -> bool:
    """Return True when a logging handler already writes to the given path."""
    for handler in root.handlers:
        if hasattr(handler, "baseFilename") and Path(handler.baseFilename) == path:
            return True
    return False


def _attach_file_handler(
    root: logging.Logger, log_file: Path, formatter: logging.Formatter, level: int
) -> Path:
    """Add a rotating file handler when absent; return the path used."""
    log_file.parent.mkdir(parents=True, exist_ok=True)
    if _has_handler_for_path(root, log_file):
        return log_file
    file_handler = RotatingFileHandler(
        log_file, maxBytes=_MAX_BYTES, backupCount=_BACKUP_COUNT
    )
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)
    return log_file


def configure_structlog(level: str | int = "INFO", log_file: str | Path | None = None) -> None:
    """
    Configure structlog + stdlib logging for JSON output to stdout and optional file.

    Idempotent: calling multiple times will not duplicate handlers. If structlog
    is already configured by a host application, this function will only attach an
    optional file handler (once) and return.
    """
    level_value = _coerce_level(level)
    formatter = _build_formatter()
    root = logging.getLogger()

    # Respect existing configuration from a host application.
    if structlog.is_configured():
        if log_file:
            _attach_file_handler(root, Path(log_file), formatter, level_value)
        return

    _clear_root_handlers(root)
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(level_value)
    console.setFormatter(formatter)
    root.addHandler(console)

    file_handler_path: Path | None = None
    if log_file:
        file_handler_path = _attach_file_handler(root, Path(log_file), formatter, level_value)

    root.setLevel(level_value)

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.stdlib.filter_by_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    configure_structlog._file_handler_path = file_handler_path  # type: ignore[attr-defined]


def get_logger(**bindings: object) -> structlog.stdlib.BoundLogger:
    """
    Return a bound structlog logger.

    If structlog has not been configured yet, it is configured with defaults
    (INFO, stdout only).
    """
    if not structlog.is_configured():
        configure_structlog()
    return structlog.get_logger(**bindings)
