import json
import random
import time
from typing import Tuple, Type

import requests
import urllib3
from tenacity import (
    RetryCallState,
    retry as tenacity_retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
    wait_random_exponential,
)

DEFAULT_RETRY_ATTEMPTS = 3
DEFAULT_RETRY_DELAY = 1.0
DEFAULT_BACKOFF_FACTOR = 2.0
DEFAULT_REQUEST_TIMEOUT = 240
DEFAULT_PARALLEL_STAGGER = 0.5

# Logger lazy - so carrega quando usado
_logger = None


def _get_logger():
    global _logger
    if _logger is None:
        from ifdata_bcb.infra.log import get_logger

        _logger = get_logger("ifdata_bcb.infra.resilience")
    return _logger


def _before_sleep_log(retry_state: RetryCallState):
    # Loga em DEBUG para nao poluir terminal
    if retry_state.outcome is None:
        return

    exception = retry_state.outcome.exception()
    _get_logger().debug(
        f"Tentativa {retry_state.attempt_number} falhou para {retry_state.fn.__name__}. "
        f"Retry em {retry_state.upcoming_sleep:.1f}s. Erro: {exception}"
    )


def _log_final_failure(retry_state: RetryCallState):
    # Re-levanta excecao original para o caller tratar
    exception = retry_state.outcome.exception()
    _get_logger().debug(
        f"Funcao {retry_state.fn.__name__} falhou apos "
        f"{retry_state.attempt_number} tentativas. Erro: {exception}"
    )
    raise retry_state.outcome.result()


# Excecoes transientes que justificam retry (rede, parsing, APIs instaveis)
TRANSIENT_EXCEPTIONS: Tuple[Type[Exception], ...] = (
    # Rede/HTTP
    requests.RequestException,
    requests.ConnectionError,
    requests.Timeout,
    urllib3.exceptions.HTTPError,
    ConnectionError,
    TimeoutError,
    OSError,  # Inclui socket errors
    # Parsing (APIs que retornam resposta invalida/vazia)
    json.JSONDecodeError,
    ValueError,
)


def retry(
    max_attempts: int = DEFAULT_RETRY_ATTEMPTS,
    delay: float = DEFAULT_RETRY_DELAY,
    backoff_factor: float = DEFAULT_BACKOFF_FACTOR,
    exceptions: Tuple[Type[Exception], ...] = TRANSIENT_EXCEPTIONS,
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
        retry=retry_if_exception_type(exceptions),
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
