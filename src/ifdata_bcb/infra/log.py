"""
Configuracao de logging com loguru para ifdata-bcb.

Fornece dual logging:
- Console (stderr): Mensagens de warning/error para usuario
- Arquivo: Logs tecnicos completos para debugging

Logs sao salvos em: AppData/Local/py-bacen/Logs/ifdata_YYYY-MM-DD.log
"""

import sys
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from loguru import Logger

_configured: bool = False
_logger_instance = None


def _get_log_path() -> Path:
    """
    Retorna caminho para diretorio de logs.

    Returns:
        Path do diretorio de logs.
    """
    from ifdata_bcb.infra.config import get_logs_path

    return get_logs_path()


def configure_logging(
    level: str = "WARNING",
    enable_file: bool = True,
    file_level: str = "DEBUG",
) -> None:
    """
    Configura loguru com dual output (console + arquivo).

    Esta funcao e idempotente - chamadas subsequentes sao ignoradas.

    Args:
        level: Nivel minimo para console (WARNING por padrao).
        enable_file: Se True, habilita logging em arquivo.
        file_level: Nivel minimo para arquivo (DEBUG por padrao).
    """
    global _configured, _logger_instance

    if _configured:
        return

    from loguru import logger

    # Remove handler padrao
    logger.remove()

    # Console handler (apenas warnings e erros por padrao)
    # Formato simplificado para nao poluir o terminal
    logger.add(
        sys.stderr,
        level=level,
        format="<level>{level: <8}</level> | {message}",
        colorize=True,
    )

    # File handler (logs completos para debugging)
    if enable_file:
        log_path = _get_log_path()
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

    _logger_instance = logger
    _configured = True


def get_logger(name: str = "ifdata_bcb") -> Any:
    """
    Retorna logger configurado com contexto de nome.

    Configura o logger na primeira chamada (lazy initialization).
    Logs de WARNING+ vao para console.
    Todos os logs (DEBUG+) vao para arquivo em AppData/Local/py-bacen/Logs/.

    Args:
        name: Nome do modulo para contexto (geralmente __name__).

    Returns:
        Logger loguru com binding de nome.
    """
    configure_logging()
    return _logger_instance.bind(name=name)


def set_log_level(level: str) -> None:
    """
    Altera o nivel de logging do console.

    Note: O nivel do arquivo permanece DEBUG para capturar tudo.

    Args:
        level: Nivel (DEBUG, INFO, WARNING, ERROR, CRITICAL).
    """
    global _configured
    _configured = False
    configure_logging(level=level)


def get_log_path() -> Path:
    """
    Retorna caminho para diretorio de logs.

    Util para usuario verificar onde os logs estao sendo salvos.

    Returns:
        Path do diretorio de logs.
    """
    return _get_log_path()
