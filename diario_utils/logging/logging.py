import logging
import logging.config
from pathlib import Path
from typing import Any


def setup_logging(
    level: str = "INFO",
    log_file: str | Path | None = None,
    format: str | None = None,
) -> None:
    """
    Configura sistema de logging para a aplicação.

    Args:
        level: Nível de log (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Arquivo para salvar logs (None para apenas console)
        format: Formato personalizado dos logs
    """
    if format is None:
        format = (
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s "
            "[%(filename)s:%(lineno)d]"
        )

    config: dict[str, Any] = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "standard": {
                "format": format,
                "datefmt": "%Y-%m-%d %H:%M:%S",
            },
        },
        "handlers": {
            "console": {
                "()": "rich.logging.RichHandler",
                "level": level,
                "markup": True,
                "rich_tracebacks": True,
                "show_time": True,
                "show_path": True,
                "log_time_format": "%H:%M:%S",
            },
            "file": {
                "class": "logging.FileHandler",
                "level": level,
                "formatter": "standard",
                "filename": "app.log",
                "mode": "a",
                "encoding": "utf-8",
            },
        },
        "loggers": {
            "diario_crawler": {
                "handlers": ["file"],
                "level": level,
                "propagate": False,
            },
        },
        "root": {
            "handlers": ["file"],
            "level": "ERROR",
        },
    }

    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)

        config["handlers"]["file"] = {
            "class": "logging.handlers.RotatingFileHandler",
            "level": level,
            "formatter": "standard",
            "filename": str(log_path),
            "maxBytes": 10_485_760,  # 10MB
            "backupCount": 5,
            "encoding": "utf-8",
        }

        config["loggers"]["diario_crawler"]["handlers"].append("file")
        config["root"]["handlers"].append("file")

    logging.config.dictConfig(config)

    logger = logging.getLogger("diario_crawler")
    logger.info(f"Logging configurado (level: {level})")


def get_logger(name: str) -> logging.Logger:
    """
    Retorna logger com nome qualificado.

    Args:
        name: Nome do logger (geralmente __name__)

    Returns:
        Instância de Logger configurada
    """
    return logging.getLogger(name)
