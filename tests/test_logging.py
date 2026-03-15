"""Testes para fallback de logging em ifdata_bcb.infra.log."""

import sys

from loguru import logger

import ifdata_bcb.infra.log as log_module


def _reset_logging_state() -> None:
    log_module._configured = False
    log_module._logger_instance = None


def test_configure_logging_falls_back_to_console_when_file_sink_fails(
    monkeypatch,
) -> None:
    calls: list[object] = []

    def fake_remove(*args, **kwargs) -> None:
        return None

    def fake_add(sink, *args, **kwargs) -> int:
        calls.append(sink)
        if sink is sys.stderr:
            return 1
        raise PermissionError("blocked")

    _reset_logging_state()
    monkeypatch.setattr(logger, "remove", fake_remove)
    monkeypatch.setattr(logger, "add", fake_add)

    try:
        log_module.configure_logging()
        configured = log_module._configured
        bound_logger = log_module.get_logger("tests")
    finally:
        _reset_logging_state()

    assert bound_logger is not None
    assert configured is True
    assert calls[0] is sys.stderr
    assert len(calls) == 2
