"""Small helpers to configure and access project logging."""

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path


def setup_logging(
    level: str = "INFO",
    log_file: str | Path | None = None,
    fmt: str | None = None,
) -> None:
    """Configure root logging with console output and optional rotating file.

    Parameters
    ----------
    level:
        Logging level string understood by ``logging`` (e.g., ``"INFO"``).
    log_file:
        Optional path to a log file; a rotating handler is created when provided.
    fmt:
        Optional log format; defaults to timestamp/name/level/message.
    """
    if fmt is None:
        fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    formatter = logging.Formatter(fmt, datefmt="%Y-%m-%d %H:%M:%S")

    root = logging.getLogger()
    root.setLevel(level)
    for handler in list(root.handlers):
        root.removeHandler(handler)
        handler.close()

    console = logging.StreamHandler()
    console.setLevel(level)
    console.setFormatter(formatter)
    handlers = [console]

    if log_file:
        path = Path(log_file)
        path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = RotatingFileHandler(path, maxBytes=10_485_760, backupCount=3)
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        handlers.append(file_handler)

    for handler in handlers:
        root.addHandler(handler)


def get_logger(name: str) -> logging.Logger:
    """Return a logger configured via :func:`setup_logging`."""
    return logging.getLogger(name)
