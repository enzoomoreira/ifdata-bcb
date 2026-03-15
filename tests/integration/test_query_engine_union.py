"""Testes para union_by_name no QueryEngine.read_glob."""

from pathlib import Path

import pandas as pd

from ifdata_bcb.infra.query import QueryEngine


# =========================================================================
# union_by_name behavior
# =========================================================================


class TestReadGlobUnionByName:
    """Verifica que read_glob lida com schemas heterogeneos via union_by_name."""

    def test_extra_column_filled_with_null(self, tmp_cache_dir: Path) -> None:
        """Parquet com coluna extra deve ter NULL para arquivos que nao tem."""
        subdir = "test"
        target = tmp_cache_dir / subdir
        target.mkdir()

        df1 = pd.DataFrame({"A": [1], "B": ["x"]})
        df1.to_parquet(target / "f_1.parquet", index=False)

        df2 = pd.DataFrame({"A": [2], "B": ["y"], "C": [99]})
        df2.to_parquet(target / "f_2.parquet", index=False)

        qe = QueryEngine(base_path=tmp_cache_dir)
        result = qe.read_glob("f_*.parquet", subdir)

        assert len(result) == 2
        assert "C" in result.columns
        assert pd.isna(result.loc[result["A"] == 1, "C"].iloc[0])
        assert result.loc[result["A"] == 2, "C"].iloc[0] == 99

    def test_identical_schemas_work_normally(self, tmp_cache_dir: Path) -> None:
        subdir = "test"
        target = tmp_cache_dir / subdir
        target.mkdir()

        for i in range(3):
            df = pd.DataFrame({"X": [i], "Y": [f"val_{i}"]})
            df.to_parquet(target / f"f_{i}.parquet", index=False)

        qe = QueryEngine(base_path=tmp_cache_dir)
        result = qe.read_glob("f_*.parquet", subdir)

        assert len(result) == 3
        assert list(result.columns) == ["X", "Y"]

    def test_column_subset_selection_with_heterogeneous_schemas(
        self, tmp_cache_dir: Path
    ) -> None:
        """SELECT de coluna especifica funciona mesmo com schemas diferentes."""
        subdir = "test"
        target = tmp_cache_dir / subdir
        target.mkdir()

        df1 = pd.DataFrame({"A": [1], "B": ["x"]})
        df1.to_parquet(target / "f_1.parquet", index=False)

        df2 = pd.DataFrame({"A": [2], "B": ["y"], "C": [99]})
        df2.to_parquet(target / "f_2.parquet", index=False)

        qe = QueryEngine(base_path=tmp_cache_dir)
        result = qe.read_glob("f_*.parquet", subdir, columns=["A"])

        assert len(result) == 2
        assert list(result.columns) == ["A"]

    def test_where_clause_with_union(self, tmp_cache_dir: Path) -> None:
        subdir = "test"
        target = tmp_cache_dir / subdir
        target.mkdir()

        df1 = pd.DataFrame({"A": [1, 2], "B": ["x", "y"]})
        df1.to_parquet(target / "f_1.parquet", index=False)

        df2 = pd.DataFrame({"A": [3], "B": ["z"], "C": [99]})
        df2.to_parquet(target / "f_2.parquet", index=False)

        qe = QueryEngine(base_path=tmp_cache_dir)
        result = qe.read_glob("f_*.parquet", subdir, where="A > 1")

        assert len(result) == 2
        assert set(result["A"]) == {2, 3}

    def test_no_files_returns_empty(self, tmp_cache_dir: Path) -> None:
        subdir = "empty"
        (tmp_cache_dir / subdir).mkdir()

        qe = QueryEngine(base_path=tmp_cache_dir)
        result = qe.read_glob("*.parquet", subdir)

        assert result.empty

    def test_nonexistent_dir_returns_empty(self, tmp_cache_dir: Path) -> None:
        qe = QueryEngine(base_path=tmp_cache_dir)
        result = qe.read_glob("*.parquet", "nao_existe")

        assert result.empty
