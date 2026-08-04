"""Testes para infra/config.py -- Settings e variaveis de ambiente."""

from pathlib import Path

import pytest
from ifdata_bcb.infra.config import Settings, get_settings


@pytest.fixture(autouse=True)
def clear_settings_cache():
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


class TestDefaults:
    def test_data_dir_has_default(self) -> None:
        assert Settings().data_dir is not None

    def test_operational_defaults(self) -> None:
        s = Settings()

        assert s.max_workers == 4
        assert s.request_timeout == 240.0
        assert s.connect_timeout == 10.0
        assert s.fuzzy_threshold == 78


class TestEnvOverrides:
    def test_data_dir_from_env(self, monkeypatch, tmp_path: Path) -> None:
        monkeypatch.setenv("BACEN_DATA_DIR", str(tmp_path))

        assert get_settings().data_dir == tmp_path

    def test_max_workers_from_env(self, monkeypatch) -> None:
        monkeypatch.setenv("BACEN_MAX_WORKERS", "2")

        assert get_settings().max_workers == 2

    def test_timeouts_from_env(self, monkeypatch) -> None:
        monkeypatch.setenv("BACEN_REQUEST_TIMEOUT", "30")
        monkeypatch.setenv("BACEN_CONNECT_TIMEOUT", "5")

        s = get_settings()

        assert s.request_timeout == 30.0
        assert s.connect_timeout == 5.0

    def test_fuzzy_threshold_from_env(self, monkeypatch) -> None:
        monkeypatch.setenv("BACEN_FUZZY_THRESHOLD", "90")

        assert get_settings().fuzzy_threshold == 90


class TestCacheInvalidation:
    """O singleton anterior lia o ambiente uma unica vez e nunca reavaliava."""

    def test_cache_clear_picks_up_new_env(self, monkeypatch, tmp_path: Path) -> None:
        monkeypatch.setenv("BACEN_DATA_DIR", str(tmp_path / "primeiro"))
        assert get_settings().data_dir == tmp_path / "primeiro"

        monkeypatch.setenv("BACEN_DATA_DIR", str(tmp_path / "segundo"))
        get_settings.cache_clear()

        assert get_settings().data_dir == tmp_path / "segundo"

    def test_same_instance_without_clear(self) -> None:
        assert get_settings() is get_settings()


class TestPaths:
    def test_cache_path_is_data_dir(self, monkeypatch, tmp_path: Path) -> None:
        monkeypatch.setenv("BACEN_DATA_DIR", str(tmp_path))

        assert get_settings().cache_path == tmp_path

    def test_logs_path_stays_inside_data_dir(self, monkeypatch, tmp_path: Path) -> None:
        """
        Com data_dir.parent, um BACEN_DATA_DIR=/dados/bcb jogava os logs em
        /dados/Logs -- fora do diretorio que o usuario configurou.
        """
        data_dir = tmp_path / "bcb"
        monkeypatch.setenv("BACEN_DATA_DIR", str(data_dir))

        logs = get_settings().logs_path

        assert logs.is_relative_to(data_dir)
        assert logs == data_dir / "logs"

    def test_logs_path_does_not_create_directory(
        self, monkeypatch, tmp_path: Path
    ) -> None:
        """Ler um atributo nao deve ter efeito colateral no disco."""
        data_dir = tmp_path / "bcb"
        monkeypatch.setenv("BACEN_DATA_DIR", str(data_dir))

        logs = get_settings().logs_path

        assert not logs.exists()
