"""Testes para o logging de ifdata_bcb.infra.log."""

import contextlib
import sys

import pytest
from loguru import logger

import ifdata_bcb.infra.log as log_module


@pytest.fixture
def consumer_sink():
    """Simula um sink que a aplicacao consumidora configurou no loguru."""
    received: list[str] = []
    sink_id = logger.add(received.append, format="{message}", level="DEBUG")
    yield received
    with contextlib.suppress(ValueError):
        logger.remove(sink_id)


@pytest.fixture(autouse=True)
def restore_library_state():
    yield
    log_module.disable_logging()


class TestConsumerSinksPreserved:
    """A lib nao pode remover sinks que nao criou."""

    def test_get_logger_does_not_touch_consumer_sinks(self, consumer_sink) -> None:
        log_module.get_logger("qualquer")

        logger.warning("mensagem do consumidor")

        assert any("mensagem do consumidor" in m for m in consumer_sink)

    def test_enable_logging_does_not_remove_consumer_sinks(self, consumer_sink) -> None:
        log_module.enable_logging(to_stderr=False)

        logger.warning("mensagem do consumidor")

        assert any("mensagem do consumidor" in m for m in consumer_sink)

    def test_disable_logging_does_not_remove_consumer_sinks(
        self, consumer_sink
    ) -> None:
        log_module.enable_logging(to_stderr=True)
        log_module.disable_logging()

        logger.warning("mensagem do consumidor")

        assert any("mensagem do consumidor" in m for m in consumer_sink)


class TestEnableDisable:
    def test_enable_logging_removes_only_its_own_sinks_on_reentry(self) -> None:
        log_module.enable_logging(to_stderr=True)
        first_ids = list(log_module._sink_ids)

        log_module.enable_logging(to_stderr=True)

        assert first_ids != log_module._sink_ids
        assert len(log_module._sink_ids) == 1

    def test_disable_logging_clears_own_sinks(self) -> None:
        log_module.enable_logging(to_stderr=True)
        assert log_module._sink_ids

        log_module.disable_logging()

        assert log_module._sink_ids == []

    def test_enable_logging_without_sinks_creates_none(self) -> None:
        log_module.enable_logging(to_stderr=False, to_file=False)

        assert log_module._sink_ids == []

    def test_falls_back_to_console_when_file_sink_fails(self, monkeypatch) -> None:
        calls: list[object] = []
        real_add = logger.add

        def fake_add(sink, *args, **kwargs) -> int:
            calls.append(sink)
            if sink is sys.stderr:
                return real_add(sink, *args, **kwargs)
            raise PermissionError("blocked")

        monkeypatch.setattr(logger, "add", fake_add)

        log_module.enable_logging(to_stderr=True, to_file=True)

        assert calls[0] is sys.stderr
        assert len(calls) == 2
        assert len(log_module._sink_ids) == 1
