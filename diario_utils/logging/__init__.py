from diario_utils.logging.logging import get_logger as legacy_get_logger, setup_logging
from diario_utils.logging.structlog_config import configure_structlog, get_logger

__all__ = [
    "configure_structlog",
    "get_logger",
    "legacy_get_logger",
    "setup_logging",
]
