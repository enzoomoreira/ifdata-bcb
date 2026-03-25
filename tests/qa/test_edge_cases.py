"""QA: edge cases de dados e infraestrutura."""

import io
from pathlib import Path

import pandas as pd
import pytest

from ifdata_bcb.core.entity import EntityLookup
from ifdata_bcb.infra.query import QueryEngine
from ifdata_bcb.infra.storage import (
    DataManager,
    get_parquet_metadata,
    list_parquet_files,
)
from ifdata_bcb.providers.cosif.explorer import COSIFExplorer
from ifdata_bcb.ui.display import Display
from tests.conftest import _save_parquet


class TestQueryEngineEdgeCases:
    def test_glob_no_match(self, tmp_cache_dir: Path) -> None:
        qe = QueryEngine(base_path=tmp_cache_dir)
        df = qe.read_glob(pattern="nonexistent_*.parquet", subdir="cosif/individual")
        assert df.empty

    def test_glob_invalid_where(self, populated_cache: Path) -> None:
        qe = QueryEngine(base_path=populated_cache)
        df = qe.read_glob(
            pattern="cosif_ind_*.parquet",
            subdir="cosif/individual",
            where="WHERE = = =",
        )
        assert df.empty

    def test_sql_syntax_error(self, populated_cache: Path) -> None:
        qe = QueryEngine(base_path=populated_cache)
        with pytest.raises(Exception):
            qe.sql("SELECT FROM WHERE INVALID")

    def test_multiple_engines_same_dir(self, populated_cache: Path) -> None:
        engines = [QueryEngine(base_path=populated_cache) for _ in range(5)]
        dfs = [
            e.read_glob(pattern="cosif_ind_*.parquet", subdir="cosif/individual")
            for e in engines
        ]
        assert all(len(d) == len(dfs[0]) for d in dfs)


class TestDataEdgeCases:
    def test_parquet_zero_rows(self, workspace_tmp_dir: Path) -> None:
        (workspace_tmp_dir / "cosif/individual").mkdir(parents=True)
        pd.DataFrame(
            {
                "DATA_BASE": pd.array([], dtype="Int64"),
                "CNPJ_8": pd.array([], dtype="str"),
                "NOME_INSTITUICAO": pd.array([], dtype="str"),
                "DOCUMENTO": pd.array([], dtype="str"),
                "CONTA": pd.array([], dtype="str"),
                "NOME_CONTA": pd.array([], dtype="str"),
                "SALDO": pd.array([], dtype="float64"),
            }
        ).to_parquet(
            workspace_tmp_dir / "cosif/individual/cosif_ind_202303.parquet",
            index=False,
        )
        qe = QueryEngine(base_path=workspace_tmp_dir)
        el = EntityLookup(query_engine=qe)
        cosif = COSIFExplorer(query_engine=qe, entity_lookup=el)
        df = cosif.read(instituicao="60872504", start="2023-03")
        assert df.empty

    def test_saldo_nan_preserved(self, workspace_tmp_dir: Path) -> None:
        _save_parquet(
            pd.DataFrame(
                {
                    "DATA_BASE": pd.array([202303], dtype="Int64"),
                    "CNPJ_8": ["60872504"],
                    "NOME_INSTITUICAO": ["BANCO X"],
                    "DOCUMENTO": ["D1"],
                    "CONTA": ["10100"],
                    "NOME_CONTA": ["ATIVO"],
                    "SALDO": [float("nan")],
                }
            ),
            workspace_tmp_dir,
            "cosif/individual",
            "cosif_ind_202303",
        )
        qe = QueryEngine(base_path=workspace_tmp_dir)
        cosif = COSIFExplorer(
            query_engine=qe, entity_lookup=EntityLookup(query_engine=qe)
        )
        df = cosif.read(instituicao="60872504", start="2023-03")
        assert not df.empty
        assert df["VALOR"].isna().any()

    def test_saldo_inf_accepted(self, workspace_tmp_dir: Path) -> None:
        _save_parquet(
            pd.DataFrame(
                {
                    "DATA_BASE": pd.array([202303, 202303], dtype="Int64"),
                    "CNPJ_8": ["60872504"] * 2,
                    "NOME_INSTITUICAO": ["BANCO X"] * 2,
                    "DOCUMENTO": ["D1", "D2"],
                    "CONTA": ["10100", "20200"],
                    "NOME_CONTA": ["ATIVO", "PASSIVO"],
                    "SALDO": [float("inf"), float("-inf")],
                }
            ),
            workspace_tmp_dir,
            "cosif/individual",
            "cosif_ind_202303",
        )
        qe = QueryEngine(base_path=workspace_tmp_dir)
        cosif = COSIFExplorer(
            query_engine=qe, entity_lookup=EntityLookup(query_engine=qe)
        )
        df = cosif.read(instituicao="60872504", start="2023-03")
        assert len(df) == 2

    def test_nome_10k_chars(self, workspace_tmp_dir: Path) -> None:
        _save_parquet(
            pd.DataFrame(
                {
                    "DATA_BASE": pd.array([202303], dtype="Int64"),
                    "CNPJ_8": ["60872504"],
                    "NOME_INSTITUICAO": ["X" * 10000],
                    "DOCUMENTO": ["D1"],
                    "CONTA": ["10100"],
                    "NOME_CONTA": ["ATIVO"],
                    "SALDO": [1000.0],
                }
            ),
            workspace_tmp_dir,
            "cosif/individual",
            "cosif_ind_202303",
        )
        qe = QueryEngine(base_path=workspace_tmp_dir)
        cosif = COSIFExplorer(
            query_engine=qe, entity_lookup=EntityLookup(query_engine=qe)
        )
        df = cosif.read(instituicao="60872504", start="2023-03")
        assert len(df) == 1

    def test_cadastro_empty_enrichment(self, workspace_tmp_dir: Path) -> None:
        _save_parquet(
            pd.DataFrame(
                {
                    "DATA_BASE": pd.array([202303], dtype="Int64"),
                    "CNPJ_8": ["60872504"],
                    "NOME_INSTITUICAO": ["BANCO X"],
                    "DOCUMENTO": ["D1"],
                    "CONTA": ["10100"],
                    "NOME_CONTA": ["ATIVO"],
                    "SALDO": [1000.0],
                }
            ),
            workspace_tmp_dir,
            "cosif/individual",
            "cosif_ind_202303",
        )
        qe = QueryEngine(base_path=workspace_tmp_dir)
        cosif = COSIFExplorer(
            query_engine=qe, entity_lookup=EntityLookup(query_engine=qe)
        )
        df = cosif.read(
            instituicao="60872504", start="2023-03", cadastro=["SEGMENTO", "UF"]
        )
        assert "SEGMENTO" in df.columns
        assert "UF" in df.columns
        assert df["SEGMENTO"].isna().all()


class TestEntityLookupEdgeCases:
    def test_entity_5_periods_returns_newest(self, workspace_tmp_dir: Path) -> None:
        (workspace_tmp_dir / "ifdata/cadastro").mkdir(parents=True)
        for p in [202103, 202106, 202109, 202112, 202203]:
            _save_parquet(
                pd.DataFrame(
                    {
                        "Data": pd.array([p], dtype="Int64"),
                        "CodInst": ["60872504"],
                        "CNPJ_8": ["60872504"],
                        "NomeInstituicao": [f"BANCO V{p}"],
                        "SegmentoTb": ["S1"],
                        "CodConglomeradoPrudencial": [None],
                        "CodConglomeradoFinanceiro": [None],
                        "CNPJ_LIDER_8": [None],
                        "Situacao": ["A"],
                        "Atividade": ["001"],
                        "Tcb": ["0001"],
                        "Td": ["01"],
                        "Tc": ["1"],
                        "Uf": ["SP"],
                        "Municipio": ["Sao Paulo"],
                        "Sr": ["01"],
                        "DataInicioAtividade": ["19900101"],
                    }
                ),
                workspace_tmp_dir,
                "ifdata/cadastro",
                f"ifdata_cad_{p}",
            )

        qe = QueryEngine(base_path=workspace_tmp_dir)
        el = EntityLookup(query_engine=qe)
        info = el.get_entity_identifiers("60872504")
        assert info["nome_entidade"] == "BANCO V202203"

    def test_lider_cnpj_nonexistent(self, workspace_tmp_dir: Path) -> None:
        _save_parquet(
            pd.DataFrame(
                {
                    "Data": pd.array([202303], dtype="Int64"),
                    "CodInst": ["60872504"],
                    "CNPJ_8": ["60872504"],
                    "NomeInstituicao": ["BANCO X"],
                    "SegmentoTb": ["S1"],
                    "CodConglomeradoPrudencial": ["40"],
                    "CodConglomeradoFinanceiro": [None],
                    "CNPJ_LIDER_8": ["99999999"],
                    "Situacao": ["A"],
                    "Atividade": ["001"],
                    "Tcb": ["0001"],
                    "Td": ["01"],
                    "Tc": ["1"],
                    "Uf": ["SP"],
                    "Municipio": ["Sao Paulo"],
                    "Sr": ["01"],
                    "DataInicioAtividade": ["19900101"],
                }
            ),
            workspace_tmp_dir,
            "ifdata/cadastro",
            "ifdata_cad_202303",
        )
        qe = QueryEngine(base_path=workspace_tmp_dir)
        el = EntityLookup(query_engine=qe)
        info = el.get_entity_identifiers("60872504")
        assert info["nome_entidade"] == "BANCO X"


class TestStorageEdgeCases:
    def test_corrupted_parquet_metadata(self, workspace_tmp_dir: Path) -> None:
        subdir = "test"
        (workspace_tmp_dir / subdir).mkdir()
        (workspace_tmp_dir / subdir / "corrupt.parquet").write_bytes(
            b"not a parquet file"
        )
        meta = get_parquet_metadata("corrupt", subdir, base_path=workspace_tmp_dir)
        assert meta is not None
        assert "Erro" in str(meta["status"])

    def test_mixed_files_filtered(self, workspace_tmp_dir: Path) -> None:
        subdir = "mixed"
        (workspace_tmp_dir / subdir).mkdir()
        (workspace_tmp_dir / subdir / "data_202303.parquet").write_bytes(b"fake")
        (workspace_tmp_dir / subdir / "readme.txt").write_text("hello")
        files = list_parquet_files(subdir, base_path=workspace_tmp_dir)
        assert "data_202303" in files
        assert not any(f.endswith(".txt") for f in files)

    def test_save_zero_rows(self, workspace_tmp_dir: Path) -> None:
        dm = DataManager(base_path=workspace_tmp_dir)
        path = dm.save(
            pd.DataFrame({"col1": pd.array([], dtype="str")}), "empty", "subdir"
        )
        assert path.exists()


class TestDisplayEdgeCases:
    def test_verbose_false_no_output(self) -> None:
        buf = io.StringIO()
        d = Display(stream=buf)
        d.banner("test", verbose=False)
        d.end_banner(verbose=False)
        d.separator(verbose=False)
        d.fetch_start("x", verbose=False)
        d.fetch_result(100, verbose=False)
        d.print_warning("w", verbose=False)
        d.print_info("i", verbose=False)
        d.print_success("s", verbose=False)
        assert buf.getvalue() == ""

    def test_progress_empty_iterable(self) -> None:
        d = Display(stream=io.StringIO())
        for _ in d.progress([], total=0, verbose=False):
            pass
