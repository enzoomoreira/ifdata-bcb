"""Testes para ifdata_bcb.domain.exceptions."""

import pytest

from ifdata_bcb.domain.exceptions import (
    BacenAnalysisError,
    DataUnavailableError,
    InvalidDateFormatError,
    InvalidDateRangeError,
    InvalidIdentifierError,
    InvalidScopeError,
    MissingRequiredParameterError,
    PeriodUnavailableError,
)


class TestExceptionHierarchy:
    """Todas as exceptions devem herdar de BacenAnalysisError."""

    @pytest.mark.parametrize(
        "exc_class",
        [
            InvalidScopeError,
            DataUnavailableError,
            InvalidIdentifierError,
            MissingRequiredParameterError,
            InvalidDateRangeError,
            InvalidDateFormatError,
            PeriodUnavailableError,
        ],
    )
    def test_inherits_from_base(self, exc_class: type) -> None:
        assert issubclass(exc_class, BacenAnalysisError)


class TestInvalidScopeError:
    def test_message_contains_valid_values(self) -> None:
        err = InvalidScopeError("escopo", "xyz", ["individual", "prudencial"])
        assert "xyz" in str(err)
        assert "individual" in str(err)
        assert "prudencial" in str(err)

    def test_attributes(self) -> None:
        err = InvalidScopeError("escopo", "xyz", ["a", "b"])
        assert err.scope == "escopo"
        assert err.value == "xyz"
        assert err.valid_values == ["a", "b"]


class TestDataUnavailableError:
    def test_message_with_reason(self) -> None:
        err = DataUnavailableError("12345678", "prudencial", "Sem conglomerado.")
        assert "12345678" in str(err)
        assert "prudencial" in str(err)
        assert "Sem conglomerado." in str(err)

    def test_message_without_reason(self) -> None:
        err = DataUnavailableError("12345678", "cosif")
        assert "12345678" in str(err)
        assert "cosif" in str(err)


class TestInvalidDateFormatError:
    def test_with_detail(self) -> None:
        err = InvalidDateFormatError("abc", "mes invalido")
        assert "abc" in str(err)
        assert "mes invalido" in str(err)

    def test_without_detail(self) -> None:
        err = InvalidDateFormatError("abc")
        assert "abc" in str(err)
