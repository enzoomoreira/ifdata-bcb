"""Testes para ifdata_bcb.infra.resilience."""

from unittest.mock import patch

import httpx
import pytest
from ifdata_bcb.domain.exceptions import PeriodUnavailableError
from ifdata_bcb.infra.resilience import (
    DEFAULT_PARALLEL_STAGGER,
    is_retryable,
    retry,
    staggered_delay,
)


def _status_error(status_code: int) -> httpx.HTTPStatusError:
    request = httpx.Request("GET", "https://exemplo.bcb.gov.br/dado")
    response = httpx.Response(status_code, request=request)
    return httpx.HTTPStatusError(
        f"HTTP {status_code}", request=request, response=response
    )


class TestRetrySuccess:
    """retry: funcao decora e retorna valor correto quando nao ha falha."""

    def test_returns_value_on_first_attempt(self) -> None:
        @retry(delay=0.01, jitter=False)
        def always_ok() -> str:
            return "ok"

        assert always_ok() == "ok"

    def test_succeeds_after_transient_failures(self) -> None:
        counter = {"calls": 0}

        @retry(max_attempts=3, delay=0.01, jitter=False)
        def fail_twice_then_ok() -> str:
            counter["calls"] += 1
            if counter["calls"] < 3:
                raise httpx.ConnectError("transient")
            return "recovered"

        result = fail_twice_then_ok()
        assert result == "recovered"
        assert counter["calls"] == 3

    def test_succeeds_after_one_failure(self) -> None:
        counter = {"calls": 0}

        @retry(max_attempts=3, delay=0.01, jitter=False)
        def fail_once() -> int:
            counter["calls"] += 1
            if counter["calls"] == 1:
                raise ConnectionError("lost")
            return 42

        assert fail_once() == 42
        assert counter["calls"] == 2


class TestRetryExhaustion:
    """retry: esgota tentativas e re-lanca a excecao original."""

    def test_raises_after_max_attempts(self) -> None:
        counter = {"calls": 0}

        @retry(max_attempts=3, delay=0.01, jitter=False)
        def always_fail() -> None:
            counter["calls"] += 1
            raise httpx.ConnectError("persistent")

        with pytest.raises(httpx.ConnectError, match="persistent"):
            always_fail()

        assert counter["calls"] == 3

    def test_preserves_timeout_error_type(self) -> None:
        @retry(max_attempts=2, delay=0.01, jitter=False)
        def timeout_func() -> None:
            raise TimeoutError("timed out")

        with pytest.raises(TimeoutError, match="timed out"):
            timeout_func()

    def test_preserves_os_error_type(self) -> None:
        @retry(max_attempts=2, delay=0.01, jitter=False)
        def os_error_func() -> None:
            raise OSError("disk fail")

        with pytest.raises(OSError, match="disk fail"):
            os_error_func()


class TestRetryTransientExceptions:
    """retry: valida que excecoes transientes sao retentadas."""

    @pytest.mark.parametrize(
        "exc_class",
        [
            httpx.ConnectError,
            httpx.TimeoutException,
            httpx.ReadError,
            ConnectionError,
            TimeoutError,
            OSError,
        ],
    )
    def test_transient_exception_is_retried(self, exc_class: type) -> None:
        counter = {"calls": 0}

        @retry(max_attempts=2, delay=0.01, jitter=False)
        def fail_then_ok() -> str:
            counter["calls"] += 1
            if counter["calls"] == 1:
                raise exc_class("transient")
            return "ok"

        assert fail_then_ok() == "ok"
        assert counter["calls"] == 2


class TestRetryNonTransientExceptions:
    """retry: excecoes nao-transientes nao sao retentadas."""

    def test_period_unavailable_error_not_retried(self) -> None:
        counter = {"calls": 0}

        @retry(max_attempts=3, delay=0.01, jitter=False)
        def raise_period_error() -> None:
            counter["calls"] += 1
            raise PeriodUnavailableError(period=202301)

        with pytest.raises(PeriodUnavailableError):
            raise_period_error()

        assert counter["calls"] == 1

    def test_key_error_not_retried(self) -> None:
        counter = {"calls": 0}

        @retry(max_attempts=3, delay=0.01, jitter=False)
        def raise_key_error() -> None:
            counter["calls"] += 1
            raise KeyError("missing")

        with pytest.raises(KeyError):
            raise_key_error()

        assert counter["calls"] == 1

    def test_type_error_not_retried(self) -> None:
        counter = {"calls": 0}

        @retry(max_attempts=3, delay=0.01, jitter=False)
        def raise_type_error() -> None:
            counter["calls"] += 1
            raise TypeError("wrong type")

        with pytest.raises(TypeError):
            raise_type_error()

        assert counter["calls"] == 1


class TestRetryJitter:
    """retry: parametro jitter alterna entre estrategias de espera."""

    def test_jitter_true_does_not_raise(self) -> None:
        @retry(max_attempts=1, delay=0.01, jitter=True)
        def ok_func() -> str:
            return "ok"

        assert ok_func() == "ok"

    def test_jitter_false_does_not_raise(self) -> None:
        @retry(max_attempts=1, delay=0.01, jitter=False)
        def ok_func() -> str:
            return "ok"

        assert ok_func() == "ok"


class TestIsRetryable:
    """is_retryable: so erro de transporte e resposta 5xx/429 justificam retry."""

    @pytest.mark.parametrize("status", [500, 502, 503, 504, 429])
    def test_server_errors_and_throttling_are_retryable(self, status: int) -> None:
        assert is_retryable(_status_error(status)) is True

    @pytest.mark.parametrize("status", [400, 401, 403, 404, 410, 422])
    def test_client_errors_are_not_retryable(self, status: int) -> None:
        assert is_retryable(_status_error(status)) is False

    def test_transport_errors_are_retryable(self) -> None:
        assert is_retryable(httpx.ConnectError("sem rede")) is True
        assert is_retryable(httpx.ReadTimeout("lento")) is True

    def test_builtin_network_errors_are_retryable(self) -> None:
        assert is_retryable(ConnectionError("perdeu")) is True
        assert is_retryable(TimeoutError("estourou")) is True
        assert is_retryable(OSError("socket")) is True

    def test_domain_exceptions_are_not_retryable(self) -> None:
        assert is_retryable(PeriodUnavailableError(period=202301)) is False

    def test_generic_value_error_is_not_retryable(self) -> None:
        """ValueError amplo mascarava bugs de logica com 3 tentativas."""
        assert is_retryable(ValueError("bug de logica")) is False

    def test_json_decode_error_is_retryable(self) -> None:
        import json

        assert is_retryable(json.JSONDecodeError("invalido", "", 0)) is True


class TestHttpStatusRetryBehaviour:
    """Comportamento ponta a ponta do decorator para respostas HTTP."""

    def test_404_is_not_retried(self) -> None:
        counter = {"calls": 0}

        @retry(max_attempts=3, delay=0.01, jitter=False)
        def not_found() -> None:
            counter["calls"] += 1
            raise _status_error(404)

        with pytest.raises(httpx.HTTPStatusError):
            not_found()

        assert counter["calls"] == 1

    def test_503_is_retried(self) -> None:
        counter = {"calls": 0}

        @retry(max_attempts=3, delay=0.01, jitter=False)
        def unavailable() -> None:
            counter["calls"] += 1
            raise _status_error(503)

        with pytest.raises(httpx.HTTPStatusError):
            unavailable()

        assert counter["calls"] == 3


class TestStaggeredDelay:
    """staggered_delay: atraso escalonado para requisicoes paralelas."""

    @patch("ifdata_bcb.infra.resilience.time.sleep")
    @patch("ifdata_bcb.infra.resilience.random.uniform", return_value=0.1)
    def test_index_zero_does_not_sleep(
        self, mock_uniform: object, mock_sleep: object
    ) -> None:
        staggered_delay(0)
        mock_sleep.assert_not_called()  # type: ignore[union-attr]

    @patch("ifdata_bcb.infra.resilience.time.sleep")
    @patch("ifdata_bcb.infra.resilience.random.uniform", return_value=0.1)
    def test_index_one_sleeps_correct_amount(
        self, mock_uniform: object, mock_sleep: object
    ) -> None:
        staggered_delay(1, base_delay=0.5)
        mock_uniform.assert_called_once_with(0, 0.5 * 0.5)  # type: ignore[union-attr]
        expected_delay = (1 * 0.5) + 0.1
        mock_sleep.assert_called_once_with(expected_delay)  # type: ignore[union-attr]

    @patch("ifdata_bcb.infra.resilience.time.sleep")
    @patch("ifdata_bcb.infra.resilience.random.uniform", return_value=0.2)
    def test_index_three_scales_linearly(
        self, mock_uniform: object, mock_sleep: object
    ) -> None:
        staggered_delay(3, base_delay=1.0)
        expected_delay = (3 * 1.0) + 0.2
        mock_sleep.assert_called_once_with(expected_delay)  # type: ignore[union-attr]

    @patch("ifdata_bcb.infra.resilience.time.sleep")
    @patch("ifdata_bcb.infra.resilience.random.uniform", return_value=0.0)
    def test_uses_default_base_delay(
        self, mock_uniform: object, mock_sleep: object
    ) -> None:
        staggered_delay(2)
        mock_uniform.assert_called_once_with(0, DEFAULT_PARALLEL_STAGGER * 0.5)  # type: ignore[union-attr]
        expected_delay = (2 * DEFAULT_PARALLEL_STAGGER) + 0.0
        mock_sleep.assert_called_once_with(expected_delay)  # type: ignore[union-attr]
