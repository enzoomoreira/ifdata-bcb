"""Testes para ifdata_bcb.utils.date."""

from datetime import date, datetime

import pandas as pd
import pytest

from ifdata_bcb.domain.exceptions import InvalidDateFormatError
from ifdata_bcb.utils.date import (
    _parse_date_input,
    generate_month_range,
    generate_quarter_range,
    normalize_date_to_int,
)


class TestParseDateInput:
    """_parse_date_input: converte varios formatos para date."""

    def test_int_yyyymm(self) -> None:
        assert _parse_date_input(202412) == date(2024, 12, 1)

    def test_str_yyyymm(self) -> None:
        assert _parse_date_input("202412") == date(2024, 12, 1)

    def test_str_yyyy_mm(self) -> None:
        assert _parse_date_input("2024-12") == date(2024, 12, 1)

    def test_str_yyyy_mm_dd(self) -> None:
        assert _parse_date_input("2024-12-15") == date(2024, 12, 15)

    def test_str_with_whitespace(self) -> None:
        assert _parse_date_input("  202412  ") == date(2024, 12, 1)

    def test_invalid_format_raises(self) -> None:
        with pytest.raises(InvalidDateFormatError):
            _parse_date_input("not-a-date")

    def test_invalid_type_raises(self) -> None:
        with pytest.raises(InvalidDateFormatError):
            _parse_date_input([2024, 12])

    def test_int_month_zero_raises(self) -> None:
        # divmod(202400, 100) -> (2024, 0) -> date(2024, 0, 1) -> ValueError
        with pytest.raises(ValueError):
            _parse_date_input(202400)

    def test_int_month_13_raises(self) -> None:
        with pytest.raises(ValueError):
            _parse_date_input(202413)

    def test_str_5_digits_invalid(self) -> None:
        # "20241" tem 5 chars, nao bate com nenhum pattern
        with pytest.raises(InvalidDateFormatError):
            _parse_date_input("20241")

    # --- Novos tipos: date, datetime, pd.Timestamp ---

    def test_date_object(self) -> None:
        assert _parse_date_input(date(2024, 12, 1)) == date(2024, 12, 1)

    def test_date_preserves_day(self) -> None:
        assert _parse_date_input(date(2024, 3, 15)) == date(2024, 3, 15)

    def test_datetime_object(self) -> None:
        assert _parse_date_input(datetime(2024, 12, 15, 10, 30)) == date(2024, 12, 15)

    def test_datetime_extracts_date_only(self) -> None:
        dt = datetime(2024, 3, 1, 23, 59, 59)
        assert _parse_date_input(dt) == date(2024, 3, 1)

    def test_pd_timestamp(self) -> None:
        ts = pd.Timestamp("2024-12-01")
        assert _parse_date_input(ts) == date(2024, 12, 1)

    def test_pd_timestamp_with_time(self) -> None:
        ts = pd.Timestamp("2024-03-15 10:30:00")
        assert _parse_date_input(ts) == date(2024, 3, 15)

    def test_pd_nat_raises(self) -> None:
        with pytest.raises(InvalidDateFormatError, match="NaT"):
            _parse_date_input(pd.NaT)

    def test_none_raises(self) -> None:
        with pytest.raises((InvalidDateFormatError, TypeError)):
            _parse_date_input(None)  # type: ignore[arg-type]


class TestNormalizeDateToInt:
    """normalize_date_to_int: converte para YYYYMM int."""

    def test_int_passthrough(self) -> None:
        assert normalize_date_to_int(202412) == 202412

    def test_int_invalid_month_raises(self) -> None:
        with pytest.raises(InvalidDateFormatError):
            normalize_date_to_int(202413)

    def test_int_month_zero_raises(self) -> None:
        with pytest.raises(InvalidDateFormatError):
            normalize_date_to_int(202400)

    def test_str_yyyymm(self) -> None:
        assert normalize_date_to_int("202403") == 202403

    def test_str_yyyy_mm(self) -> None:
        assert normalize_date_to_int("2024-03") == 202403

    def test_str_yyyy_mm_dd_drops_day(self) -> None:
        assert normalize_date_to_int("2024-03-15") == 202403

    # --- Novos tipos: date, datetime, pd.Timestamp ---

    def test_date_object(self) -> None:
        assert normalize_date_to_int(date(2024, 3, 15)) == 202403

    def test_datetime_object(self) -> None:
        assert normalize_date_to_int(datetime(2024, 12, 25, 10, 30)) == 202412

    def test_pd_timestamp(self) -> None:
        assert normalize_date_to_int(pd.Timestamp("2024-06-15")) == 202406

    def test_pd_timestamp_month_boundary(self) -> None:
        assert normalize_date_to_int(pd.Timestamp("2024-01-31")) == 202401

    def test_pd_nat_raises(self) -> None:
        with pytest.raises(InvalidDateFormatError, match="NaT"):
            normalize_date_to_int(pd.NaT)


class TestGenerateMonthRange:
    """generate_month_range: gera lista YYYYMM entre start e end."""

    def test_same_month(self) -> None:
        assert generate_month_range(202401, 202401) == [202401]

    def test_three_months(self) -> None:
        assert generate_month_range(202410, 202412) == [202410, 202411, 202412]

    def test_crosses_year_boundary(self) -> None:
        result = generate_month_range(202411, 202502)
        assert result == [202411, 202412, 202501, 202502]

    def test_start_after_end_returns_empty(self) -> None:
        assert generate_month_range(202412, 202401) == []

    def test_full_year(self) -> None:
        result = generate_month_range(202401, 202412)
        assert len(result) == 12
        assert result[0] == 202401
        assert result[-1] == 202412

    def test_accepts_string_formats(self) -> None:
        result = generate_month_range("2024-01", "2024-03")
        assert result == [202401, 202402, 202403]

    def test_multi_year_span(self) -> None:
        result = generate_month_range(202301, 202501)
        assert len(result) == 25  # 24 meses + 1


class TestGenerateQuarterRange:
    """generate_quarter_range: gera fins de trimestre entre start e end."""

    def test_single_quarter(self) -> None:
        assert generate_quarter_range(202401, 202403) == [202403]

    def test_full_year_quarters(self) -> None:
        result = generate_quarter_range(202401, 202412)
        assert result == [202403, 202406, 202409, 202412]

    def test_start_mid_quarter(self) -> None:
        # Start em fevereiro, primeiro trimestre e marco
        result = generate_quarter_range(202402, 202406)
        assert result == [202403, 202406]

    def test_start_after_end_returns_empty(self) -> None:
        assert generate_quarter_range(202412, 202401) == []

    def test_crosses_year(self) -> None:
        result = generate_quarter_range(202410, 202506)
        assert result == [202412, 202503, 202506]

    def test_start_at_quarter_end(self) -> None:
        result = generate_quarter_range(202403, 202409)
        assert result == [202403, 202406, 202409]

    def test_start_equals_end_at_quarter(self) -> None:
        assert generate_quarter_range(202403, 202403) == [202403]

    def test_start_equals_end_not_quarter(self) -> None:
        # start=end=202402, primeiro trimestre=202403 que e > end
        assert generate_quarter_range(202402, 202402) == []
