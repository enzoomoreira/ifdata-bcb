"""Testes para ifdata_bcb.providers.cosif.collector.COSIFCollector."""

import zipfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from ifdata_bcb.domain.exceptions import PeriodUnavailableError
from ifdata_bcb.providers.cosif.collector import COSIFCollector


def _write_cosif_csv(
    path: Path, rows: list[list[str]], encoding: str = "utf-8"
) -> Path:
    """Escreve CSV no formato COSIF: 3 linhas de lixo, header, dados."""
    lines = [
        "linha ignorada 1",
        "linha ignorada 2",
        "linha ignorada 3",
        "#DATA_BASE;CNPJ;NOME_INSTITUICAO;DOCUMENTO;CONTA;NOME_CONTA;SALDO",
    ]
    for row in rows:
        lines.append(";".join(row))
    path.write_text("\n".join(lines), encoding=encoding)
    return path


def _make_collector() -> COSIFCollector:
    return COSIFCollector("individual", data_manager=MagicMock())


# =========================================================================
# _process_to_parquet
# =========================================================================


class TestProcessToParquet:
    """Testes de _process_to_parquet com CSVs reais em disco."""

    def test_valid_csv_returns_correct_columns_and_types(
        self, workspace_tmp_dir: Path
    ) -> None:
        csv_path = _write_cosif_csv(
            workspace_tmp_dir / "test.csv",
            [
                [
                    "202303",
                    "60.872.504/0001-34",
                    "BANCO ALFA S.A.",
                    "D1",
                    "10100",
                    "ATIVO TOTAL",
                    "1000000,50",
                ],
            ],
        )
        collector = _make_collector()
        df = collector._process_to_parquet(csv_path, 202303)

        assert df is not None
        expected_cols = [
            "DATA_BASE",
            "CNPJ_8",
            "NOME_INSTITUICAO",
            "DOCUMENTO",
            "CONTA",
            "NOME_CONTA",
            "SALDO",
        ]
        assert list(df.columns) == expected_cols
        assert len(df) == 1

    def test_saldo_comma_decimal_converted(self, workspace_tmp_dir: Path) -> None:
        csv_path = _write_cosif_csv(
            workspace_tmp_dir / "test.csv",
            [
                [
                    "202303",
                    "12345678",
                    "BANCO X",
                    "D1",
                    "10100",
                    "CONTA",
                    "1234,56",
                ],
            ],
        )
        collector = _make_collector()
        df = collector._process_to_parquet(csv_path, 202303)

        assert df is not None
        assert df["SALDO"].iloc[0] == pytest.approx(1234.56)

    def test_cnpj_standardized_to_8_digits(self, workspace_tmp_dir: Path) -> None:
        csv_path = _write_cosif_csv(
            workspace_tmp_dir / "test.csv",
            [
                [
                    "202303",
                    "60.872.504/0001-34",
                    "BANCO ALFA",
                    "D1",
                    "10100",
                    "CONTA",
                    "100,00",
                ],
            ],
        )
        collector = _make_collector()
        df = collector._process_to_parquet(csv_path, 202303)

        assert df is not None
        assert df["CNPJ_8"].iloc[0] == "60872504"

    def test_data_base_coerced_to_int64(self, workspace_tmp_dir: Path) -> None:
        csv_path = _write_cosif_csv(
            workspace_tmp_dir / "test.csv",
            [
                [
                    "202303",
                    "12345678",
                    "BANCO X",
                    "D1",
                    "10100",
                    "CONTA",
                    "100,00",
                ],
            ],
        )
        collector = _make_collector()
        df = collector._process_to_parquet(csv_path, 202303)

        assert df is not None
        assert df["DATA_BASE"].dtype == pd.Int64Dtype()
        assert df["DATA_BASE"].iloc[0] == 202303

    def test_empty_csv_returns_none(self, workspace_tmp_dir: Path) -> None:
        csv_path = _write_cosif_csv(workspace_tmp_dir / "test.csv", [])
        collector = _make_collector()
        result = collector._process_to_parquet(csv_path, 202303)

        assert result is None

    def test_non_numeric_saldo_becomes_nan(self, workspace_tmp_dir: Path) -> None:
        csv_path = _write_cosif_csv(
            workspace_tmp_dir / "test.csv",
            [
                [
                    "202303",
                    "12345678",
                    "BANCO X",
                    "D1",
                    "10100",
                    "CONTA",
                    "N/D",
                ],
            ],
        )
        collector = _make_collector()
        df = collector._process_to_parquet(csv_path, 202303)

        assert df is not None
        assert pd.isna(df["SALDO"].iloc[0])


# =========================================================================
# _download_period
# =========================================================================


class TestDownloadPeriod:
    """Testes de _download_period com _download_single mockado."""

    @patch.object(COSIFCollector, "_download_single")
    def test_first_suffix_succeeds_returns_csv_path(
        self, mock_dl: MagicMock, workspace_tmp_dir: Path
    ) -> None:
        """Primeiro suffix e um .csv.zip que funciona."""
        collector = _make_collector()
        period = 202303

        zip_path = workspace_tmp_dir / f"{period}BANCOS.csv.zip"
        csv_name = f"{period}BANCOS.csv"
        csv_content = "header\ndata"

        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr(csv_name, csv_content)

        def side_effect(url: str, output_path: Path, period: int = 0) -> bool:
            if url.endswith("BANCOS.csv.zip"):
                output_path.write_bytes(zip_path.read_bytes())
                return True
            raise PeriodUnavailableError(period)

        mock_dl.side_effect = side_effect

        result = collector._download_period(period, workspace_tmp_dir)

        assert result is not None
        assert result.exists()
        assert result.name == csv_name

    @patch.object(COSIFCollector, "_download_single")
    def test_first_404_second_succeeds(
        self, mock_dl: MagicMock, workspace_tmp_dir: Path
    ) -> None:
        """Primeiro suffix da 404, segundo (.zip) funciona."""
        collector = _make_collector()
        period = 202303

        zip_path = workspace_tmp_dir / f"{period}BANCOS.zip"
        csv_name = f"{period}BANCOS.csv"
        csv_content = "header\ndata"

        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr(csv_name, csv_content)

        def side_effect(url: str, output_path: Path, period: int = 0) -> bool:
            if url.endswith("BANCOS.csv.zip"):
                raise PeriodUnavailableError(period)
            if url.endswith("BANCOS.zip"):
                output_path.write_bytes(zip_path.read_bytes())
                return True
            raise PeriodUnavailableError(period)

        mock_dl.side_effect = side_effect

        result = collector._download_period(period, workspace_tmp_dir)

        assert result is not None
        assert result.exists()

    @patch.object(COSIFCollector, "_download_single")
    def test_all_404_raises_period_unavailable(
        self, mock_dl: MagicMock, workspace_tmp_dir: Path
    ) -> None:
        collector = _make_collector()
        period = 202303

        mock_dl.side_effect = PeriodUnavailableError(period)

        with pytest.raises(PeriodUnavailableError):
            collector._download_period(period, workspace_tmp_dir)

    @patch.object(COSIFCollector, "_download_single")
    def test_bad_zip_continues_to_next_suffix(
        self, mock_dl: MagicMock, workspace_tmp_dir: Path
    ) -> None:
        """BadZipFile no primeiro suffix, segundo (.zip) funciona."""
        collector = _make_collector()
        period = 202303

        good_zip_path = workspace_tmp_dir / "good.zip"
        csv_name = f"{period}BANCOS.csv"

        with zipfile.ZipFile(good_zip_path, "w") as zf:
            zf.writestr(csv_name, "header\ndata")

        def side_effect(url: str, output_path: Path, period: int = 0) -> bool:
            if url.endswith("BANCOS.csv.zip"):
                output_path.write_bytes(b"not a zip file")
                return True
            if url.endswith("BANCOS.zip"):
                output_path.write_bytes(good_zip_path.read_bytes())
                return True
            raise PeriodUnavailableError(period)

        mock_dl.side_effect = side_effect

        result = collector._download_period(period, workspace_tmp_dir)

        assert result is not None
        assert result.exists()

    @patch.object(COSIFCollector, "_download_single")
    def test_zip_with_no_matching_csv_returns_none(
        self, mock_dl: MagicMock, workspace_tmp_dir: Path
    ) -> None:
        """ZIP valido mas sem CSV correspondente ao file_pattern."""
        collector = _make_collector()
        period = 202303

        zip_with_wrong_csv = workspace_tmp_dir / "wrong.zip"
        with zipfile.ZipFile(zip_with_wrong_csv, "w") as zf:
            zf.writestr("OUTROS.csv", "some data")

        def side_effect(url: str, output_path: Path, period: int = 0) -> bool:
            if "zip" in url.lower():
                output_path.write_bytes(zip_with_wrong_csv.read_bytes())
                return True
            raise PeriodUnavailableError(period)

        mock_dl.side_effect = side_effect

        result = collector._download_period(period, workspace_tmp_dir)

        assert result is None
