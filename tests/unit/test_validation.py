"""Testes unitarios para validators Pydantic em domain/validation.py."""

import pytest

from ifdata_bcb.domain.exceptions import InvalidDateFormatError, InvalidIdentifierError
from ifdata_bcb.domain.validation import (
    AccountList,
    InstitutionList,
    NormalizedDates,
    ValidatedCnpj8,
)


class TestNormalizedDates:
    def test_single_int(self) -> None:
        assert NormalizedDates(values=202403).values == [202403]

    def test_single_str_dash(self) -> None:
        assert NormalizedDates(values="2024-03").values == [202403]

    def test_single_str_compact(self) -> None:
        assert NormalizedDates(values="202403").values == [202403]

    def test_list_of_ints(self) -> None:
        result = NormalizedDates(values=[202401, 202402, 202403]).values
        assert result == [202401, 202402, 202403]

    def test_list_mixed_types(self) -> None:
        result = NormalizedDates(values=[202401, "2024-02", 202403]).values
        assert result == [202401, 202402, 202403]

    def test_invalid_string_raises(self) -> None:
        with pytest.raises(InvalidDateFormatError):
            NormalizedDates(values="abc")

    def test_month_13_raises(self) -> None:
        with pytest.raises(InvalidDateFormatError):
            NormalizedDates(values=202413)

    def test_month_0_raises(self) -> None:
        with pytest.raises(InvalidDateFormatError):
            NormalizedDates(values=202400)

    def test_1000_valid_dates(self) -> None:
        datas = [y * 100 + m for y in range(2000, 2084) for m in range(1, 13)][:1000]
        result = NormalizedDates(values=datas)
        assert len(result.values) == 1000


class TestValidatedCnpj8:
    def test_valid_cnpj(self) -> None:
        assert ValidatedCnpj8(value="60872504").value == "60872504"

    def test_strips_whitespace(self) -> None:
        assert ValidatedCnpj8(value="  60872504  ").value == "60872504"

    def test_7_digits_raises(self) -> None:
        with pytest.raises(InvalidIdentifierError):
            ValidatedCnpj8(value="1234567")

    def test_9_digits_raises(self) -> None:
        with pytest.raises(InvalidIdentifierError):
            ValidatedCnpj8(value="123456789")

    def test_letters_raises(self) -> None:
        with pytest.raises(InvalidIdentifierError):
            ValidatedCnpj8(value="abcdefgh")

    def test_fullwidth_unicode_digits_raises(self) -> None:
        fullwidth = "".join(chr(0xFF10 + i) for i in range(1, 9))
        with pytest.raises(InvalidIdentifierError):
            ValidatedCnpj8(value=fullwidth)

    def test_int_input_raises(self) -> None:
        with pytest.raises(InvalidIdentifierError):
            ValidatedCnpj8(value=60872504)


class TestInstitutionList:
    def test_single_string_wraps(self) -> None:
        assert InstitutionList(values="60872504").values == ["60872504"]

    def test_valid_list(self) -> None:
        result = InstitutionList(values=["60872504", "90400888"]).values
        assert result == ["60872504", "90400888"]

    def test_invalid_in_list_raises(self) -> None:
        with pytest.raises(InvalidIdentifierError):
            InstitutionList(values=["60872504", "abc"])

    def test_empty_list_accepted(self) -> None:
        assert InstitutionList(values=[]).values == []


class TestAccountList:
    def test_single_string_wraps(self) -> None:
        assert AccountList(values="ATIVO TOTAL").values == ["ATIVO TOTAL"]

    def test_list_passthrough(self) -> None:
        result = AccountList(values=["ATIVO", "PASSIVO"]).values
        assert result == ["ATIVO", "PASSIVO"]

    def test_strings_with_newlines(self) -> None:
        result = AccountList(values=["ATIVO\nTOTAL", "PASSIVO\tTOTAL"]).values
        assert len(result) == 2
