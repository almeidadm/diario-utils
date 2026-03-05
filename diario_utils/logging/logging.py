import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path


def _build_console_handler(
    level: str,
    formatter: logging.Formatter,
) -> logging.Handler:
    try:
        from rich.logging import RichHandler
    except ImportError:
        handler: logging.Handler = logging.StreamHandler()
    else:
        handler = RichHandler(
            markup=True,
            rich_tracebacks=True,
            show_time=True,
            show_path=True,
            log_time_format="%H:%M:%S",
        )

    handler.setLevel(level)
    handler.setFormatter(formatter)
    return handler


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

    formatter = logging.Formatter(format, datefmt="%Y-%m-%d %H:%M:%S")

    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    for handler in list(root_logger.handlers):
        root_logger.removeHandler(handler)
        handler.close()

    handlers = [_build_console_handler(level=level, formatter=formatter)]

    if log_file is not None:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)

        file_handler = RotatingFileHandler(
            filename=str(log_path),
            maxBytes=10_485_760,  # 10MB
            backupCount=5,
            encoding="utf-8",
        )
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        handlers.append(file_handler)

    for handler in handlers:
        root_logger.addHandler(handler)

    logger = logging.getLogger("diario_crawler")
    logger.setLevel(level)
    logger.propagate = True
    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        handler.close()

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
