"""Testes unitarios para infra/storage.py."""

from pathlib import Path

import pandas as pd
import pytest

from ifdata_bcb.infra.storage import DataManager, cleanup_partial_writes


@pytest.fixture
def dm(tmp_path: Path) -> DataManager:
    return DataManager(base_path=tmp_path)


@pytest.fixture
def df() -> pd.DataFrame:
    return pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})


class TestSave:
    def test_creates_parquet(self, dm: DataManager, df: pd.DataFrame) -> None:
        path = dm.save(df, "arq", "sub")

        assert path.exists()
        assert path.name == "arq.parquet"
        assert len(pd.read_parquet(path)) == 3

    def test_overwrites_existing(self, dm: DataManager, df: pd.DataFrame) -> None:
        dm.save(df, "arq", "sub")
        path = dm.save(df.head(1), "arq", "sub")

        assert len(pd.read_parquet(path)) == 1

    def test_leaves_no_tmp_behind(self, dm: DataManager, df: pd.DataFrame) -> None:
        path = dm.save(df, "arq", "sub")

        assert list(path.parent.glob("*.tmp")) == []


class TestSaveIsAtomic:
    """Escrita interrompida nao pode deixar .parquet truncado com nome final."""

    def test_failure_leaves_no_destination_file(
        self, dm: DataManager, df: pd.DataFrame, monkeypatch
    ) -> None:
        def boom(*args, **kwargs):
            raise OSError("disco cheio")

        monkeypatch.setattr(pd.DataFrame, "to_parquet", boom)

        with pytest.raises(OSError):
            dm.save(df, "arq", "sub")

        assert not (dm.cache_path / "sub" / "arq.parquet").exists()

    def test_failure_removes_tmp(
        self, dm: DataManager, df: pd.DataFrame, monkeypatch
    ) -> None:
        def boom(*args, **kwargs):
            raise OSError("disco cheio")

        monkeypatch.setattr(pd.DataFrame, "to_parquet", boom)

        with pytest.raises(OSError):
            dm.save(df, "arq", "sub")

        assert list((dm.cache_path / "sub").glob("*.tmp")) == []

    def test_failure_preserves_previous_version(
        self, dm: DataManager, df: pd.DataFrame, monkeypatch
    ) -> None:
        """O dado antigo continua legivel se a re-escrita falhar."""
        path = dm.save(df, "arq", "sub")

        def boom(*args, **kwargs):
            raise OSError("disco cheio")

        monkeypatch.setattr(pd.DataFrame, "to_parquet", boom)

        with pytest.raises(OSError):
            dm.save(df.head(1), "arq", "sub")

        assert len(pd.read_parquet(path)) == 3

    def test_keyboard_interrupt_leaves_no_destination_file(
        self, dm: DataManager, df: pd.DataFrame, monkeypatch
    ) -> None:
        """BaseException (Ctrl+C) tambem precisa limpar o .tmp."""

        def boom(*args, **kwargs):
            raise KeyboardInterrupt()

        monkeypatch.setattr(pd.DataFrame, "to_parquet", boom)

        with pytest.raises(KeyboardInterrupt):
            dm.save(df, "arq", "sub")

        assert not (dm.cache_path / "sub" / "arq.parquet").exists()
        assert list((dm.cache_path / "sub").glob("*.tmp")) == []


class TestCleanupPartialWrites:
    def test_removes_orphan_tmp(self, tmp_path: Path) -> None:
        subdir = tmp_path / "sub"
        subdir.mkdir()
        (subdir / "arq.parquet.tmp").write_bytes(b"lixo")

        removed = cleanup_partial_writes("sub", tmp_path)

        assert removed == 1
        assert list(subdir.glob("*.tmp")) == []

    def test_preserves_valid_parquet(
        self, dm: DataManager, df: pd.DataFrame, tmp_path: Path
    ) -> None:
        path = dm.save(df, "arq", "sub")
        (tmp_path / "sub" / "outro.parquet.tmp").write_bytes(b"lixo")

        removed = cleanup_partial_writes("sub", tmp_path)

        assert removed == 1
        assert path.exists()

    def test_missing_dir_returns_zero(self, tmp_path: Path) -> None:
        assert cleanup_partial_writes("nao_existe", tmp_path) == 0


class TestPeriodDiscovery:
    def test_tmp_file_is_not_counted_as_collected(
        self, dm: DataManager, tmp_path: Path
    ) -> None:
        """Um .tmp orfao nao pode marcar o periodo como ja coletado."""
        subdir = tmp_path / "sub"
        subdir.mkdir()
        (subdir / "pref_202412.parquet.tmp").write_bytes(b"lixo")

        assert dm.get_periodos_disponiveis("pref", "sub") == []

    def test_saved_period_is_discovered(
        self, dm: DataManager, df: pd.DataFrame
    ) -> None:
        dm.save(df, "pref_202412", "sub")

        assert dm.get_periodos_disponiveis("pref", "sub") == [(2024, 12)]
