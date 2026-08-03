import json
import random
import time
from collections.abc import Callable
from typing import Any

import httpx
from tenacity import (
    RetryCallState,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
    wait_random_exponential,
)
from tenacity import (
    retry as tenacity_retry,
)

DEFAULT_RETRY_ATTEMPTS = 3
DEFAULT_RETRY_DELAY = 1.0
DEFAULT_BACKOFF_FACTOR = 2.0
DEFAULT_REQUEST_TIMEOUT = 240
DEFAULT_PARALLEL_STAGGER = 0.5

# Logger lazy - so carrega quando usado
_logger = None


def _get_logger() -> Any:
    global _logger
    if _logger is None:
        from ifdata_bcb.infra.log import get_logger

        _logger = get_logger("ifdata_bcb.infra.resilience")
    return _logger


def _before_sleep_log(retry_state: RetryCallState) -> None:
    # Loga em DEBUG para nao poluir terminal
    if retry_state.outcome is None:
        return

    exception = retry_state.outcome.exception()
    _get_logger().debug(
        f"Tentativa {retry_state.attempt_number} falhou para {retry_state.fn.__name__}. "
        f"Retry em {retry_state.upcoming_sleep:.1f}s. Erro: {exception}"
    )


def _log_final_failure(retry_state: RetryCallState) -> None:
    # Re-levanta excecao original para o caller tratar
    exception = retry_state.outcome.exception()
    _get_logger().debug(
        f"Funcao {retry_state.fn.__name__} falhou apos "
        f"{retry_state.attempt_number} tentativas. Erro: {exception}"
    )
    raise retry_state.outcome.result()


# Status HTTP que justificam nova tentativa: sobrecarga e falha de servidor.
# 4xx (404 de periodo inexistente, 403, 400) sao definitivos -- retentar so
# desperdicaria requisicoes contra a API do BCB.
RETRYABLE_STATUS_CODES = frozenset({429})


def is_retryable(exc: BaseException) -> bool:
    """Decide se uma excecao justifica nova tentativa."""
    if isinstance(exc, httpx.HTTPStatusError):
        status = exc.response.status_code
        return status in RETRYABLE_STATUS_CODES or 500 <= status < 600

    # TransportError cobre ConnectError, TimeoutException, ReadError etc.
    # A base httpx.HTTPError fica de fora de proposito: HTTPStatusError herda
    # dela e seria retentada em qualquer status.
    if isinstance(exc, httpx.TransportError):
        return True

    # Resposta invalida/truncada da API pode ser transiente
    if isinstance(exc, json.JSONDecodeError):
        return True

    # OSError cobre ConnectionError e TimeoutError (subclasses desde 3.3/3.10)
    return isinstance(exc, OSError)


def retry(
    max_attempts: int = DEFAULT_RETRY_ATTEMPTS,
    delay: float = DEFAULT_RETRY_DELAY,
    backoff_factor: float = DEFAULT_BACKOFF_FACTOR,
    retry_on: Callable[[BaseException], bool] = is_retryable,
    jitter: bool = True,
):
    """Decorator para retry com exponential backoff. Jitter evita thundering herd."""
    # Calcula delay maximo baseado nos parametros
    # Com 3 tentativas e backoff 2.0: delays podem ser 1, 2, 4 -> max ~4s
    max_delay = delay * (backoff_factor ** (max_attempts - 1))

    # Seleciona estrategia de wait baseado em jitter
    if jitter:
        wait_strategy = wait_random_exponential(multiplier=delay, max=max_delay)
    else:
        wait_strategy = wait_exponential(multiplier=delay, max=max_delay)

    return tenacity_retry(
        stop=stop_after_attempt(max_attempts),
        wait=wait_strategy,
        retry=retry_if_exception(retry_on),
        before_sleep=_before_sleep_log,
        retry_error_callback=_log_final_failure,
        reraise=True,  # Re-levanta excecao original apos todas tentativas
    )


def staggered_delay(index: int, base_delay: float = DEFAULT_PARALLEL_STAGGER) -> None:
    """
    Delay escalonado para workers paralelos (evita thundering herd).

    Worker 0 nao espera. Worker N espera N * base_delay + jitter.
    """
    if index == 0:
        return  # Primeiro worker nao espera

    # Delay = index * base + jitter aleatorio (0-50% do base)
    jitter = random.uniform(0, base_delay * 0.5)
    delay = (index * base_delay) + jitter
    time.sleep(delay)
