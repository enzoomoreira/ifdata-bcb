"""Testes para classificacao de status em ifdata_bcb.providers.base_collector."""

from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd

from ifdata_bcb.domain.exceptions import DataProcessingError, PeriodUnavailableError
from ifdata_bcb.providers.base_collector import BaseCollector
from ifdata_bcb.providers.collector_models import CollectStatus


class StubCollector(BaseCollector):
    def __init__(
        self,
        download_result: Path | Exception | None,
        process_result: pd.DataFrame | None = None,
        process_error: Exception | None = None,
    ):
        super().__init__(data_manager=MagicMock())
        self._download_result = download_result
        self._process_result = process_result
        self._process_error = process_error

    def _get_file_prefix(self) -> str:
        return "stub"

    def _get_subdir(self) -> str:
        return "stub"

    def _download_period(self, period: int, work_dir: Path) -> Path | None:
        if isinstance(self._download_result, Exception):
            raise self._download_result
        return self._download_result

    def _process_to_parquet(self, csv_path: Path, period: int) -> pd.DataFrame | None:
        if self._process_error is not None:
            raise self._process_error
        return self._process_result


# =========================================================================
# Testes: _filter_by_availability (cutoff dates)
# =========================================================================


class TestFilterByAvailability:
    """Filtragem de periodos anteriores ao primeiro disponivel na fonte."""

    def test_known_prefix_filters_old_periods(self) -> None:
        """Periodos anteriores ao cutoff sao removidos."""
        collector = StubCollector(download_result=None)
        collector._get_file_prefix = lambda: "cosif_prud"  # type: ignore[method-assign]

        periods = [201301, 201306, 201407, 201412, 201501]
        result = collector._filter_by_availability(periods)

        assert result == [201407, 201412, 201501]

    def test_known_prefix_keeps_all_when_after_cutoff(self) -> None:
        """Se todos os periodos sao apos o cutoff, nenhum e removido."""
        collector = StubCollector(download_result=None)
        collector._get_file_prefix = lambda: "cosif_prud"  # type: ignore[method-assign]

        periods = [202001, 202006, 202012]
        result = collector._filter_by_availability(periods)

        assert result == periods

    def test_unknown_prefix_keeps_all(self) -> None:
        """Prefix desconhecido nao filtra nada."""
        collector = StubCollector(download_result=None)
        # StubCollector retorna "stub" como prefix (nao esta no registry)

        periods = [190001, 200001, 202001]
        result = collector._filter_by_availability(periods)

        assert result == periods

    def test_empty_input(self) -> None:
        collector = StubCollector(download_result=None)
        collector._get_file_prefix = lambda: "cosif_prud"  # type: ignore[method-assign]

        assert collector._filter_by_availability([]) == []

    def test_all_periods_before_cutoff(self) -> None:
        """Se todos sao anteriores, retorna lista vazia."""
        collector = StubCollector(download_result=None)
        collector._get_file_prefix = lambda: "cosif_prud"  # type: ignore[method-assign]

        periods = [201301, 201306, 201312]
        result = collector._filter_by_availability(periods)

        assert result == []

    def test_cutoff_period_itself_is_included(self) -> None:
        """O proprio periodo de cutoff (>=) deve ser incluido."""
        collector = StubCollector(download_result=None)
        collector._get_file_prefix = lambda: "cosif_prud"  # type: ignore[method-assign]

        result = collector._filter_by_availability([201407])
        assert result == [201407]


class TestGeneratePeriodsWithCutoff:
    """_generate_periods integra o filtro de availability."""

    def test_monthly_filters_before_cutoff(self) -> None:
        collector = StubCollector(download_result=None)
        collector._get_file_prefix = lambda: "cosif_prud"  # type: ignore[method-assign]
        collector._PERIOD_TYPE = "monthly"

        periods = collector._generate_periods("2014-01", "2014-12")

        assert periods[0] == 201407
        assert len(periods) == 6  # jul-dez

    def test_quarterly_filters_before_cutoff(self) -> None:
        collector = StubCollector(download_result=None)
        collector._get_file_prefix = lambda: "ifdata_val"  # type: ignore[method-assign]
        collector._PERIOD_TYPE = "quarterly"

        periods = collector._generate_periods("2002-01", "2004-12")

        assert periods[0] == 200303
        assert all(p >= 200303 for p in periods)


# =========================================================================
# Testes: _process_single_period (status classification)
# =========================================================================


def test_process_single_period_marks_unavailable_when_period_is_missing() -> None:
    collector = StubCollector(download_result=PeriodUnavailableError(202412))

    count, status, error = collector._process_single_period(202412)

    assert count == 0
    assert status is CollectStatus.UNAVAILABLE
    assert error is None


def test_process_single_period_marks_failed_on_processing_error(
    workspace_tmp_dir: Path,
) -> None:
    csv_path = workspace_tmp_dir / "period.csv"
    csv_path.write_text("x\n", encoding="utf-8")
    collector = StubCollector(
        download_result=csv_path,
        process_error=DataProcessingError("stub", "bad schema"),
    )

    count, status, error = collector._process_single_period(202412)

    assert count == 0
    assert status is CollectStatus.FAILED
    assert "bad schema" in error


def test_process_single_period_keeps_empty_dataframe_as_unavailable(
    workspace_tmp_dir: Path,
) -> None:
    csv_path = workspace_tmp_dir / "period.csv"
    csv_path.write_text("x\n", encoding="utf-8")
    collector = StubCollector(download_result=csv_path, process_result=pd.DataFrame())

    count, status, error = collector._process_single_period(202412)

    assert count == 0
    assert status is CollectStatus.UNAVAILABLE
    assert error is None


def test_process_single_period_saves_successful_result(workspace_tmp_dir: Path) -> None:
    csv_path = workspace_tmp_dir / "period.csv"
    csv_path.write_text("x\n", encoding="utf-8")
    df = pd.DataFrame({"value": [1, 2, 3]})
    collector = StubCollector(download_result=csv_path, process_result=df)

    count, status, error = collector._process_single_period(202412)

    assert count == 3
    assert status is CollectStatus.SUCCESS
    assert error is None
    collector.dm.save.assert_called_once()
