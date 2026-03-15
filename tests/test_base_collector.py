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
