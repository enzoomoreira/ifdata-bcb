"""
Logging interno da biblioteca.

Desativado por padrao via `logger.disable("ifdata_bcb")` em ifdata_bcb/__init__.py,
conforme o padrao que a documentacao do loguru prescreve para bibliotecas: uma lib
nao pode adicionar nem remover sinks do logger global, porque esse logger pertence
a aplicacao consumidora.

Para ativar, o consumidor chama `enable_logging()`. Sem sinks proprios
(to_stderr=False, to_file=False), as mensagens passam a fluir para os sinks que a
aplicacao ja tiver configurado.
"""

import contextlib
import sys
import warnings
from pathlib import Path
from typing import Any

from loguru import logger

# Ids dos sinks criados por enable_logging(). Só estes podem ser removidos --
# remover por id evita destruir sinks da aplicacao consumidora.
_sink_ids: list[int] = []

_PACKAGE = "ifdata_bcb"


def _remove_own_sinks() -> None:
    global _sink_ids
    for sink_id in _sink_ids:
        # ValueError: sink ja removido pela aplicacao consumidora
        with contextlib.suppress(ValueError):
            logger.remove(sink_id)
    _sink_ids = []


def enable_logging(
    level: str = "WARNING",
    to_stderr: bool = True,
    to_file: bool = False,
    file_level: str = "DEBUG",
) -> None:
    """
    Ativa o logging interno da biblioteca.

    Args:
        level: nivel minimo do sink de console.
        to_stderr: adiciona sink proprio em stderr.
        to_file: adiciona sink proprio em arquivo, em `get_log_path()`.
        file_level: nivel minimo do sink de arquivo.

    Com to_stderr=False e to_file=False, nenhum sink e criado e as mensagens
    vao para os sinks ja configurados pela aplicacao consumidora.
    """
    _remove_own_sinks()
    logger.enable(_PACKAGE)

    if to_stderr:
        _sink_ids.append(
            logger.add(
                sys.stderr,
                level=level,
                format="<level>{level: <8}</level> | {message}",
                colorize=True,
                filter=_PACKAGE,
            )
        )

    if to_file:
        from ifdata_bcb.infra.config import get_settings

        try:
            log_file = get_settings().logs_path / "ifdata_{time:YYYY-MM-DD}.log"
            _sink_ids.append(
                logger.add(
                    log_file,
                    format="[{time:YYYY-MM-DD HH:mm:ss}] {level: <8} [{name}] {message}",
                    level=file_level,
                    rotation="00:00",
                    retention="30 days",
                    encoding="utf-8",
                    filter=_PACKAGE,
                )
            )
        except OSError:
            # Ambiente restrito: segue sem sink de arquivo.
            pass


def disable_logging() -> None:
    """Desativa o logging interno e remove os sinks criados por enable_logging()."""
    _remove_own_sinks()
    logger.disable(_PACKAGE)


def get_logger(name: str = "ifdata_bcb") -> Any:
    return logger.bind(name=name)


def emit_user_warning(
    warning: str | Warning,
    category: type[Warning] = UserWarning,
    stacklevel: int = 2,
) -> None:
    """Emite warning para o usuario E registra no log interno."""
    if isinstance(warning, Warning):
        warnings.warn(warning, type(warning), stacklevel=stacklevel + 1)
        msg = str(warning)
        cat_name = type(warning).__name__
    else:
        warnings.warn(warning, category, stacklevel=stacklevel + 1)
        msg = warning
        cat_name = category.__name__
    get_logger("ifdata_bcb.warnings").debug(f"[{cat_name}] {msg}")


def get_log_path() -> Path:
    from ifdata_bcb.infra.config import get_settings

    return get_settings().logs_path
