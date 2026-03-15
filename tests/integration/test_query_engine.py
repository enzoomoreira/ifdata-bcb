"""Testes de integracao -- QueryEngine."""

from pathlib import Path

from ifdata_bcb.infra.query import QueryEngine
from tests.conftest import BANCO_A_CNPJ


class TestQueryEngine:
    def test_read_glob_returns_data(self, populated_cache: Path) -> None:
        qe = QueryEngine(base_path=populated_cache)
        df = qe.read_glob(pattern="cosif_ind_*.parquet", subdir="cosif/individual")
        assert not df.empty
        assert "DATA_BASE" in df.columns

    def test_read_glob_with_where(self, populated_cache: Path) -> None:
        qe = QueryEngine(base_path=populated_cache)
        df = qe.read_glob(
            pattern="cosif_ind_*.parquet",
            subdir="cosif/individual",
            where=f"CNPJ_8 = '{BANCO_A_CNPJ}'",
        )
        assert not df.empty
        assert all(df["CNPJ_8"] == BANCO_A_CNPJ)

    def test_read_glob_empty_dir_returns_empty(self, tmp_cache_dir: Path) -> None:
        qe = QueryEngine(base_path=tmp_cache_dir)
        df = qe.read_glob(pattern="*.parquet", subdir="nonexistent")
        assert df.empty

    def test_sql_with_cache_placeholder(self, populated_cache: Path) -> None:
        qe = QueryEngine(base_path=populated_cache)
        df = qe.sql(
            "SELECT COUNT(*) as total FROM "
            "'{cache}/cosif/individual/cosif_ind_202303.parquet'"
        )
        assert df["total"].iloc[0] > 0
