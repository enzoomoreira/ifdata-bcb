import sys
from datetime import datetime
from pathlib import Path
from typing import Any

_configured: bool = False
_logger_instance = None


def configure_logging(
    level: str = "WARNING",
    enable_file: bool = True,
    file_level: str = "DEBUG",
) -> None:
    """
    Configura loguru com dual output (console + arquivo).

    Idempotente - chamadas subsequentes sao ignoradas.
    Console: WARNING+ por padrao. Arquivo: DEBUG+ em Logs/ ao lado do cache efetivo.
    """
    global _configured, _logger_instance

    if _configured:
        return

    from loguru import logger

    logger.remove()

    logger.add(
        sys.stderr,
        level=level,
        format="<level>{level: <8}</level> | {message}",
        colorize=True,
    )

    if enable_file:
        from ifdata_bcb.infra.config import get_settings

        try:
            log_path = get_settings().logs_path
            today = datetime.now().strftime("%Y-%m-%d")
            log_file = log_path / f"ifdata_{today}.log"

            logger.add(
                log_file,
                format="[{time:YYYY-MM-DD HH:mm:ss}] {level: <8} [{name}] {message}",
                level=file_level,
                rotation="10 MB",
                retention="30 days",
                encoding="utf-8",
            )
        except OSError:
            # Ambiente restrito: mantem apenas sink de console.
            pass

    _logger_instance = logger
    _configured = True


def get_logger(name: str = "ifdata_bcb") -> Any:
    configure_logging()
    return _logger_instance.bind(name=name)


def set_log_level(level: str) -> None:
    global _configured, _logger_instance
    if _logger_instance:
        _logger_instance.remove()
    _configured = False
    configure_logging(level=level)


def get_log_path() -> Path:
    from ifdata_bcb.infra.config import get_settings

    return get_settings().logs_path
