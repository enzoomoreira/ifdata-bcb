"""Testes para ifdata_bcb.providers.ifdata.collector (processamento CSV -> DataFrame)."""

from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd

from ifdata_bcb.providers.ifdata.valores.collector import IFDATAValoresCollector
from ifdata_bcb.providers.ifdata.cadastro.collector import IFDATACadastroCollector


# =========================================================================
# Helpers para escrita de CSVs
# =========================================================================

VALORES_HEADER = (
    "AnoMes,CodInst,TipoInstituicao,Conta,NomeColuna,Saldo,NomeRelatorio,Grupo"
)
CADASTRO_HEADER = (
    "Data,CodInst,NomeInstituicao,SegmentoTb,CodConglomeradoPrudencial,"
    "CodConglomeradoFinanceiro,CnpjInstituicaoLider,Situacao,Atividade,"
    "Tcb,Td,Tc,Uf,Municipio,Sr,DataInicioAtividade"
)


def _write_valores_csv(path: Path, rows: list[str]) -> Path:
    """Escreve CSV de IFDATA Valores com header e linhas fornecidas."""
    content = VALORES_HEADER + "\n" + "\n".join(rows) + "\n"
    path.write_text(content, encoding="utf-8")
    return path


def _write_cadastro_csv(path: Path, rows: list[str]) -> Path:
    """Escreve CSV de IFDATA Cadastro com header e linhas fornecidas."""
    content = CADASTRO_HEADER + "\n" + "\n".join(rows) + "\n"
    path.write_text(content, encoding="utf-8")
    return path


def _make_valores_collector() -> IFDATAValoresCollector:
    dm = MagicMock()
    return IFDATAValoresCollector(data_manager=dm)


def _make_cadastro_collector() -> IFDATACadastroCollector:
    dm = MagicMock()
    return IFDATACadastroCollector(data_manager=dm)


# =========================================================================
# IFDATAValoresCollector._process_to_parquet
# =========================================================================


class TestValoresProcessToParquet:
    """IFDATAValoresCollector._process_to_parquet: CSV dir -> DataFrame."""

    def test_valid_csv_returns_correct_columns(self, workspace_tmp_dir: Path) -> None:
        collector = _make_valores_collector()
        _write_valores_csv(
            workspace_tmp_dir / "ifdata_val_202303_3.csv",
            ["202303,60872504,3,10100,ATIVO TOTAL,1000000.50,Resumo,Balanco"],
        )
        df = collector._process_to_parquet(workspace_tmp_dir, 202303)

        assert df is not None
        expected_cols = [
            "AnoMes",
            "CodInst",
            "TipoInstituicao",
            "Conta",
            "NomeColuna",
            "Saldo",
            "NomeRelatorio",
            "Grupo",
        ]
        assert list(df.columns) == expected_cols

    def test_multiple_csvs_concatenated(self, workspace_tmp_dir: Path) -> None:
        collector = _make_valores_collector()
        _write_valores_csv(
            workspace_tmp_dir / "ifdata_val_202303_1.csv",
            ["202303,40,1,10100,ATIVO TOTAL,1500000.00,Resumo,Balanco"],
        )
        _write_valores_csv(
            workspace_tmp_dir / "ifdata_val_202303_3.csv",
            ["202303,60872504,3,20200,PASSIVO TOTAL,800000.25,Resumo,Balanco"],
        )
        df = collector._process_to_parquet(workspace_tmp_dir, 202303)

        assert df is not None
        assert len(df) == 2

    def test_null_strings_replaced_with_none(self, workspace_tmp_dir: Path) -> None:
        collector = _make_valores_collector()
        _write_valores_csv(
            workspace_tmp_dir / "ifdata_val_202303_3.csv",
            ["202303,60872504,3,10100,null,1000.00,null,null"],
        )
        df = collector._process_to_parquet(workspace_tmp_dir, 202303)

        assert df is not None
        row = df.iloc[0]
        assert pd.isna(row["NomeColuna"])
        assert row["NomeColuna"] != "null"
        assert pd.isna(row["NomeRelatorio"])
        assert pd.isna(row["Grupo"])

    def test_anomes_coerced_to_int64(self, workspace_tmp_dir: Path) -> None:
        collector = _make_valores_collector()
        _write_valores_csv(
            workspace_tmp_dir / "ifdata_val_202303_3.csv",
            ["202303,60872504,3,10100,ATIVO TOTAL,1000.00,Resumo,Balanco"],
        )
        df = collector._process_to_parquet(workspace_tmp_dir, 202303)

        assert df is not None
        assert df["AnoMes"].dtype == pd.Int64Dtype()

    def test_small_csv_skipped(self, workspace_tmp_dir: Path) -> None:
        collector = _make_valores_collector()
        # Arquivo com <= 100 bytes (apenas header curto)
        tiny = workspace_tmp_dir / "ifdata_val_202303_3.csv"
        tiny.write_text("AnoMes,CodInst\n", encoding="utf-8")
        assert tiny.stat().st_size <= 100

        df = collector._process_to_parquet(workspace_tmp_dir, 202303)
        assert df is None

    def test_empty_directory_returns_none(self, workspace_tmp_dir: Path) -> None:
        collector = _make_valores_collector()
        df = collector._process_to_parquet(workspace_tmp_dir, 202303)
        assert df is None


# =========================================================================
# IFDATACadastroCollector._process_to_parquet
# =========================================================================


class TestCadastroProcessToParquet:
    """IFDATACadastroCollector._process_to_parquet: CSV file -> DataFrame."""

    def _default_row(self, **overrides: str) -> str:
        """Gera linha CSV de cadastro com valores default, aplicando overrides."""
        defaults = {
            "Data": "202303",
            "CodInst": "60872504",
            "NomeInstituicao": "BANCO ALFA S.A.",
            "SegmentoTb": "S1",
            "CodConglomeradoPrudencial": "40",
            "CodConglomeradoFinanceiro": "50",
            "CnpjInstituicaoLider": "60872504000170",
            "Situacao": "A",
            "Atividade": "001",
            "Tcb": "0001",
            "Td": "01",
            "Tc": "1",
            "Uf": "SP",
            "Municipio": "Sao Paulo",
            "Sr": "01",
            "DataInicioAtividade": "19900101",
        }
        defaults.update(overrides)
        return ",".join(defaults.values())

    def test_valid_csv_has_expected_columns(self, workspace_tmp_dir: Path) -> None:
        collector = _make_cadastro_collector()
        csv_path = _write_cadastro_csv(
            workspace_tmp_dir / "ifdata_cad_202303.csv",
            [self._default_row()],
        )
        df = collector._process_to_parquet(csv_path, 202303)

        assert df is not None
        expected_cols = [
            "Data",
            "CodInst",
            "CNPJ_8",
            "NomeInstituicao",
            "SegmentoTb",
            "CodConglomeradoPrudencial",
            "CodConglomeradoFinanceiro",
            "CNPJ_LIDER_8",
            "Situacao",
            "Atividade",
            "Tcb",
            "Td",
            "Tc",
            "Uf",
            "Municipio",
            "Sr",
            "DataInicioAtividade",
        ]
        assert list(df.columns) == expected_cols

    def test_numeric_codinst_produces_cnpj8(self, workspace_tmp_dir: Path) -> None:
        collector = _make_cadastro_collector()
        csv_path = _write_cadastro_csv(
            workspace_tmp_dir / "ifdata_cad_202303.csv",
            [self._default_row(CodInst="60872504")],
        )
        df = collector._process_to_parquet(csv_path, 202303)

        assert df is not None
        assert df.iloc[0]["CNPJ_8"] == "60872504"

    def test_non_numeric_codinst_produces_null_cnpj8(
        self, workspace_tmp_dir: Path
    ) -> None:
        collector = _make_cadastro_collector()
        csv_path = _write_cadastro_csv(
            workspace_tmp_dir / "ifdata_cad_202303.csv",
            [self._default_row(CodInst="PRUD_ALIAS")],
        )
        df = collector._process_to_parquet(csv_path, 202303)

        assert df is not None
        assert df.iloc[0]["CNPJ_8"] is None

    def test_cnpj_instituicao_lider_standardized_to_8_digits(
        self, workspace_tmp_dir: Path
    ) -> None:
        collector = _make_cadastro_collector()
        csv_path = _write_cadastro_csv(
            workspace_tmp_dir / "ifdata_cad_202303.csv",
            [self._default_row(CnpjInstituicaoLider="60872504000170")],
        )
        df = collector._process_to_parquet(csv_path, 202303)

        assert df is not None
        assert df.iloc[0]["CNPJ_LIDER_8"] == "60872504"
        assert "CnpjInstituicaoLider" not in df.columns

    def test_null_cnpj_lider_produces_none(self, workspace_tmp_dir: Path) -> None:
        collector = _make_cadastro_collector()
        csv_path = _write_cadastro_csv(
            workspace_tmp_dir / "ifdata_cad_202303.csv",
            [self._default_row(CnpjInstituicaoLider="null")],
        )
        df = collector._process_to_parquet(csv_path, 202303)

        assert df is not None
        assert df.iloc[0]["CNPJ_LIDER_8"] is None

    def test_data_coerced_to_int64(self, workspace_tmp_dir: Path) -> None:
        collector = _make_cadastro_collector()
        csv_path = _write_cadastro_csv(
            workspace_tmp_dir / "ifdata_cad_202303.csv",
            [self._default_row()],
        )
        df = collector._process_to_parquet(csv_path, 202303)

        assert df is not None
        assert df["Data"].dtype == pd.Int64Dtype()
        assert df.iloc[0]["Data"] == 202303
