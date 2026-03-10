"""Minimal stub of structlog for offline/testing environments.

This stub implements only the small surface used by diario-utils tests:
- reset_defaults()
- is_configured()
- get_logger(**bindings)

If the real structlog is installed, Python's import resolution should prefer it.
"""

from __future__ import annotations

import logging

_CONFIGURED = False


def reset_defaults() -> None:
    """Reset flag so future configuration can re-run."""
    global _CONFIGURED
    _CONFIGURED = False


def is_configured() -> bool:
    return _CONFIGURED


def get_logger(**bindings: object) -> logging.LoggerAdapter:
    logger = logging.getLogger()
    return logging.LoggerAdapter(logger, extra=bindings)
