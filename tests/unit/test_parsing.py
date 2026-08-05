"""Testes para providers/parsing.py -- deteccao de perda silenciosa de dados."""

import warnings
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from ifdata_bcb.domain.exceptions import PartialDataWarning
from ifdata_bcb.providers.parsing import (
    count_parseable_rows,
    warn_if_rows_dropped,
    warn_if_values_nulled,
)


@pytest.fixture
def cursor():
    con = duckdb.connect(":memory:")
    yield con
    con.close()


class TestCountParseableRows:
    def test_counts_all_rows(self, cursor, workspace_tmp_dir: Path) -> None:
        path = workspace_tmp_dir / "dados.csv"
        path.write_text("a,b\n1,2\n3,4\n5,6\n", encoding="utf-8")

        assert count_parseable_rows(cursor, path, delim=",") == 3

    def test_respects_skip(self, cursor, workspace_tmp_dir: Path) -> None:
        path = workspace_tmp_dir / "meta.csv"
        path.write_text("lixo\nlixo\nlixo\na;b\n1;2\n", encoding="utf-8")

        assert count_parseable_rows(cursor, path, delim=";", skip=3) == 1

    def test_missing_file_returns_none(self, cursor, workspace_tmp_dir: Path) -> None:
        """Diagnostico nunca pode quebrar a coleta."""
        assert (
            count_parseable_rows(cursor, workspace_tmp_dir / "sumiu.csv", ",") is None
        )


class TestWarnIfRowsDropped:
    def test_warns_above_threshold(self) -> None:
        with pytest.warns(PartialDataWarning, match="descartadas"):
            warn_if_rows_dropped("fonte", rows_read=50, rows_expected=100)

    def test_silent_below_threshold(self) -> None:
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            warn_if_rows_dropped("fonte", rows_read=9999, rows_expected=10000)

    def test_silent_when_nothing_dropped(self) -> None:
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            warn_if_rows_dropped("fonte", rows_read=100, rows_expected=100)

    def test_silent_when_expected_unknown(self) -> None:
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            warn_if_rows_dropped("fonte", rows_read=100, rows_expected=None)

    def test_silent_when_read_exceeds_expected(self) -> None:
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            warn_if_rows_dropped("fonte", rows_read=120, rows_expected=100)


class TestWarnIfValuesNulled:
    def test_warns_when_values_lost(self) -> None:
        raw = pd.Series(["1,50", "2,30", "3,10", "4,20"])
        parsed = pd.Series([None, None, None, 4.20])

        with pytest.warns(PartialDataWarning, match="Saldo"):
            warn_if_values_nulled("fonte", "Saldo", raw, parsed)

    def test_decimal_separator_change_is_detected(self) -> None:
        """O cenario que motiva a checagem: todos os saldos zerados em silencio."""
        raw = pd.Series(["1.234,56"] * 10)
        parsed = pd.Series([None] * 10)

        with pytest.warns(PartialDataWarning, match="100.0%"):
            warn_if_values_nulled("fonte", "Saldo", raw, parsed)

    def test_null_tokens_are_not_counted_as_loss(self) -> None:
        """'null' vindo do BCB e ausencia legitima, nao falha de conversao."""
        raw = pd.Series(["null", "NULL", "", None, "nan"])
        parsed = pd.Series([None] * 5)

        with warnings.catch_warnings():
            warnings.simplefilter("error")
            warn_if_values_nulled("fonte", "Saldo", raw, parsed)

    def test_silent_when_all_parsed(self) -> None:
        raw = pd.Series(["1.5", "2.5"])
        parsed = pd.Series([1.5, 2.5])

        with warnings.catch_warnings():
            warnings.simplefilter("error")
            warn_if_values_nulled("fonte", "Saldo", raw, parsed)

    def test_silent_on_empty_series(self) -> None:
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            warn_if_values_nulled(
                "fonte",
                "Saldo",
                pd.Series([], dtype="object"),
                pd.Series([], dtype="float64"),
            )
