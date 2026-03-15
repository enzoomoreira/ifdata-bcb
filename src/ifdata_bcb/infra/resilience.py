"""
Utilitarios de resiliencia: retry, backoff, tratamento de erros.

Fornece decorators para lidar com falhas transientes em APIs externas.
Usa tenacity para implementacao robusta de retry com exponential backoff.
"""

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

# Constantes de resiliencia
DEFAULT_RETRY_ATTEMPTS = 3
DEFAULT_RETRY_DELAY = 1.0
DEFAULT_BACKOFF_FACTOR = 2.0
DEFAULT_REQUEST_TIMEOUT = 240
DEFAULT_PARALLEL_STAGGER = 0.5  # Delay entre starts de workers paralelos

# Logger lazy - so carrega quando usado
_logger = None


def _get_logger():
    """Logger lazy - so carrega quando usado."""
    global _logger
    if _logger is None:
        from ifdata_bcb.infra.log import get_logger

        _logger = get_logger("ifdata_bcb.infra.resilience")
    return _logger


def _before_sleep_log(retry_state: RetryCallState):
    """
    Callback para logar antes de dormir entre tentativas.

    Usa DEBUG para nao poluir terminal do usuario. Detalhes completos
    ficam apenas no arquivo de log. O collector mostra mensagem limpa
    via Display quando a falha final ocorre.
    """
    if retry_state.outcome is None:
        return

    exception = retry_state.outcome.exception()
    _get_logger().debug(
        f"Tentativa {retry_state.attempt_number} falhou para {retry_state.fn.__name__}. "
        f"Retry em {retry_state.upcoming_sleep:.1f}s. Erro: {exception}"
    )


def _log_final_failure(retry_state: RetryCallState):
    """
    Callback quando todas tentativas falharam.

    Usa DEBUG para nao poluir terminal do usuario. O caller (collector)
    e responsavel por mostrar mensagem amigavel via Display.
    Re-levanta a excecao original para o caller tratar.
    """
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
    """
    Decorator para retry com exponential backoff e jitter.

    Usa tenacity internamente para implementacao robusta.

    Args:
        max_attempts: Numero maximo de tentativas.
        delay: Delay inicial em segundos.
        backoff_factor: Multiplicador do delay apos cada falha.
        exceptions: Tupla de excecoes para capturar (rede, parsing, etc).
        jitter: Se True, adiciona variacao aleatoria ao delay (evita thundering herd).

    Returns:
        Funcao decorada.

    Example:
        @retry(max_attempts=3, delay=1.0)
        def fetch_data():
            return requests.get(url, timeout=30)
    """
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
    Adiciona delay escalonado para workers paralelos.

    Evita que todos workers iniciem simultaneamente, reduzindo
    pressao sobre APIs publicas com rate limiting.

    O delay inclui jitter aleatorio para evitar sincronizacao.

    Args:
        index: Indice do worker (0, 1, 2, ...).
        base_delay: Delay base em segundos entre cada worker.

    Example:
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = {
                executor.submit(process_with_stagger, i, item): item
                for i, item in enumerate(items)
            }

        def process_with_stagger(index, item):
            staggered_delay(index)  # Worker 0: 0s, Worker 1: ~0.5s, etc.
            return process(item)
    """
    if index == 0:
        return  # Primeiro worker nao espera

    # Delay = index * base + jitter aleatorio (0-50% do base)
    jitter = random.uniform(0, base_delay * 0.5)
    delay = (index * base_delay) + jitter
    time.sleep(delay)
