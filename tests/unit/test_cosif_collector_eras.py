"""Testes para processamento multi-era no COSIFCollector."""

from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

from ifdata_bcb.providers.cosif.collector import COSIFCollector


# =========================================================================
# Helpers
# =========================================================================

ERA_1_METADATA = [
    "Balancete Patrimonial (Codigo documento 4010)",
    "Data de geracao dos dados: 14/12/2009",
    "Fonte: Instituicoes financeiras",
]

ERA_1_HEADER = "DATA;CNPJ;NOME INSTITUICAO;ATRIBUTO;DOCUMENTO;CONTA;NOME CONTA;SALDO"

ERA_2_HEADER = "#DATA_BASE;DOCUMENTO;CNPJ;AGENCIA;NOME_INSTITUICAO;COD_CONGL;NOME_CONGL;TAXONOMIA;CONTA;NOME_CONTA;SALDO"


def _write_era1_csv(path: Path, rows: list[str]) -> Path:
    lines = ERA_1_METADATA + [ERA_1_HEADER] + rows
    path.write_text("\n".join(lines), encoding="CP1252")
    return path


def _write_era2_csv(path: Path, rows: list[str], encoding: str = "CP1252") -> Path:
    lines = ERA_1_METADATA + [ERA_2_HEADER] + rows
    path.write_text("\n".join(lines), encoding=encoding)
    return path


def _make_collector() -> COSIFCollector:
    return COSIFCollector("individual", data_manager=MagicMock())


# =========================================================================
# Era 1 processing
# =========================================================================


class TestEra1Processing:
    """Testes de _process_to_parquet com CSVs no formato Era 1 (pre-201010)."""

    def test_era1_csv_returns_correct_columns(self, workspace_tmp_dir: Path) -> None:
        csv = _write_era1_csv(
            workspace_tmp_dir / "e1.csv",
            [
                "200901;00000000;BCO DO BRASIL S.A.;L;4010;0010000007;CIRCULANTE E REALIZAVEL;1000000,50"
            ],
        )
        collector = _make_collector()
        df = collector._process_to_parquet(csv, 200901)

        assert df is not None
        expected = [
            "DATA_BASE",
            "CNPJ_8",
            "NOME_INSTITUICAO",
            "DOCUMENTO",
            "CONTA",
            "NOME_CONTA",
            "SALDO",
        ]
        assert list(df.columns) == expected

    def test_era1_strips_leading_zeros_from_conta(
        self, workspace_tmp_dir: Path
    ) -> None:
        """CONTA 0010000007 deve virar 10000007 (int64)."""
        csv = _write_era1_csv(
            workspace_tmp_dir / "e1.csv",
            ["200901;00000000;BANCO X;L;4010;0010000007;CONTA;100,00"],
        )
        collector = _make_collector()
        df = collector._process_to_parquet(csv, 200901)

        assert df is not None
        assert df["CONTA"].iloc[0] == 10000007

    def test_era1_nome_conta_uppercased(self, workspace_tmp_dir: Path) -> None:
        csv = _write_era1_csv(
            workspace_tmp_dir / "e1.csv",
            ["200901;00000000;BANCO X;L;4010;0010000007;Disponibilidades;100,00"],
        )
        collector = _make_collector()
        df = collector._process_to_parquet(csv, 200901)

        assert df is not None
        assert df["NOME_CONTA"].iloc[0] == "DISPONIBILIDADES"

    def test_era1_cnpj_standardized(self, workspace_tmp_dir: Path) -> None:
        csv = _write_era1_csv(
            workspace_tmp_dir / "e1.csv",
            ["200901;60.872.504/0001-34;BANCO ALFA;L;4010;0010000007;CONTA;100,00"],
        )
        collector = _make_collector()
        df = collector._process_to_parquet(csv, 200901)

        assert df is not None
        assert df["CNPJ_8"].iloc[0] == "60872504"

    def test_era1_saldo_comma_decimal(self, workspace_tmp_dir: Path) -> None:
        csv = _write_era1_csv(
            workspace_tmp_dir / "e1.csv",
            ["200901;00000000;BANCO X;L;4010;0010000007;CONTA;1234567,89"],
        )
        collector = _make_collector()
        df = collector._process_to_parquet(csv, 200901)

        assert df is not None
        assert df["SALDO"].iloc[0] == pytest.approx(1234567.89)

    def test_era1_data_base_as_int64(self, workspace_tmp_dir: Path) -> None:
        csv = _write_era1_csv(
            workspace_tmp_dir / "e1.csv",
            ["200901;00000000;BANCO X;L;4010;0010000007;CONTA;100,00"],
        )
        collector = _make_collector()
        df = collector._process_to_parquet(csv, 200901)

        assert df is not None
        assert df["DATA_BASE"].dtype == pd.Int64Dtype()
        assert df["DATA_BASE"].iloc[0] == 200901

    def test_era1_multiple_rows(self, workspace_tmp_dir: Path) -> None:
        csv = _write_era1_csv(
            workspace_tmp_dir / "e1.csv",
            [
                "200901;00000000;BB;L;4010;0010000007;ATIVO;1000,00",
                "200901;00000000;BB;L;4010;0011000006;DISPONIBILIDADES;500,00",
                "200901;12345678;OUTRO;L;4010;0010000007;ATIVO;200,00",
            ],
        )
        collector = _make_collector()
        df = collector._process_to_parquet(csv, 200901)

        assert df is not None
        assert len(df) == 3
        assert set(df["CNPJ_8"]) == {"00000000", "12345678"}


# =========================================================================
# Era 2/3 NOME_CONTA normalization
# =========================================================================


class TestNomeContaNormalization:
    """Verifica que NOME_CONTA e normalizado para UPPER em todas as eras."""

    def test_era2_nome_conta_already_upper(self, workspace_tmp_dir: Path) -> None:
        csv = _write_era2_csv(
            workspace_tmp_dir / "e2.csv",
            ["202412;4010;00000000;;BANCO X;;;;10000007;ATIVO REALIZAVEL;1000000,50"],
        )
        collector = _make_collector()
        df = collector._process_to_parquet(csv, 202412)

        assert df is not None
        assert df["NOME_CONTA"].iloc[0] == "ATIVO REALIZAVEL"

    def test_era3_title_case_uppercased(self, workspace_tmp_dir: Path) -> None:
        """Era 3 vem em Title Case -- deve ser normalizado para UPPER."""
        csv = _write_era2_csv(
            workspace_tmp_dir / "e3.csv",
            ["202501;4010;00000000;;BANCO X;;;;1000000009;Ativo Realizavel;2000000,00"],
        )
        collector = _make_collector()
        df = collector._process_to_parquet(csv, 202501)

        assert df is not None
        assert df["NOME_CONTA"].iloc[0] == "ATIVO REALIZAVEL"

    def test_era2_and_era1_produce_same_schema(self, workspace_tmp_dir: Path) -> None:
        """Parquets de qualquer era devem ter o mesmo schema."""
        csv1 = _write_era1_csv(
            workspace_tmp_dir / "e1.csv",
            ["200901;00000000;BANCO X;L;4010;0010000007;ATIVO;100,00"],
        )
        csv2 = _write_era2_csv(
            workspace_tmp_dir / "e2.csv",
            ["202412;4010;00000000;;BANCO X;;;;10000007;ATIVO;200,00"],
        )
        collector = _make_collector()
        df1 = collector._process_to_parquet(csv1, 200901)
        df2 = collector._process_to_parquet(csv2, 202412)

        assert df1 is not None and df2 is not None
        assert list(df1.columns) == list(df2.columns)


# =========================================================================
# _download_single inheritance
# =========================================================================


class TestDownloadSingleInheritance:
    """Verifica que _download_single do BaseCollector e herdado pelos IFDATA collectors."""

    def test_ifdata_valores_inherits_download_single(self) -> None:
        from ifdata_bcb.providers.base_collector import BaseCollector
        from ifdata_bcb.providers.ifdata.collector import IFDATAValoresCollector

        collector = IFDATAValoresCollector(data_manager=MagicMock())
        # Deve usar o metodo da base, nao ter override proprio
        assert type(collector)._download_single is BaseCollector._download_single

    def test_ifdata_cadastro_inherits_download_single(self) -> None:
        from ifdata_bcb.providers.base_collector import BaseCollector
        from ifdata_bcb.providers.ifdata.collector import IFDATACadastroCollector

        collector = IFDATACadastroCollector(data_manager=MagicMock())
        assert type(collector)._download_single is BaseCollector._download_single

    def test_cosif_overrides_download_single(self) -> None:
        """COSIF tem override com param extra (period) para PeriodUnavailableError."""
        from ifdata_bcb.providers.base_collector import BaseCollector
        from ifdata_bcb.providers.cosif.collector import COSIFCollector

        collector = COSIFCollector("individual", data_manager=MagicMock())
        assert type(collector)._download_single is not BaseCollector._download_single
