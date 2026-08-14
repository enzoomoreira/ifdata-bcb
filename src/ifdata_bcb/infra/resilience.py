import json
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
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

# Teto de requisicoes HTTP simultaneas ao BCB, valido para o processo inteiro.
MAX_CONCURRENT_REQUESTS = 4
_request_semaphore = threading.BoundedSemaphore(MAX_CONCURRENT_REQUESTS)

# Logger lazy - so carrega quando usado
_logger = None


def _get_logger() -> Any:
    global _logger
    if _logger is None:
        from ifdata_bcb.infra.log import get_logger

        _logger = get_logger("ifdata_bcb.infra.resilience")
    return _logger


def _fn_name(retry_state: RetryCallState) -> str:
    return retry_state.fn.__name__ if retry_state.fn is not None else "<desconhecida>"


def _before_sleep_log(retry_state: RetryCallState) -> None:
    # Loga em DEBUG para nao poluir terminal
    if retry_state.outcome is None:
        return

    exception = retry_state.outcome.exception()
    _get_logger().debug(
        f"Tentativa {retry_state.attempt_number} falhou para {_fn_name(retry_state)}. "
        f"Retry em {retry_state.upcoming_sleep:.1f}s. Erro: {exception}"
    )


def _log_final_failure(retry_state: RetryCallState) -> None:
    """Loga o esgotamento das tentativas e re-levanta a excecao original.

    Este callback e quem propaga o erro: com retry_error_callback definido, o
    tenacity ignora reraise=True e usa o retorno daqui.
    """
    # O callback so roda quando a ultima tentativa falhou, entao ha outcome com
    # excecao -- o Optional vem da assinatura generica de RetryCallState.
    assert retry_state.outcome is not None
    exception = retry_state.outcome.exception()
    assert exception is not None
    _get_logger().debug(
        f"Funcao {_fn_name(retry_state)} falhou apos "
        f"{retry_state.attempt_number} tentativas. Erro: {exception}"
    )
    raise exception


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


@contextmanager
def request_slot() -> Iterator[None]:
    """
    Limita as requisicoes HTTP simultaneas ao BCB.

    O semaforo e global ao processo, entao o teto vale mesmo com pools
    aninhados -- a coleta do IFDATA roda um pool de 3 tipos de instituicao
    dentro do pool de periodos, o que permitia 12 conexoes simultaneas contra
    uma API publica.
    """
    with _request_semaphore:
        yield
