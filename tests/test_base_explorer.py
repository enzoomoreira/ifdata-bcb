"""Testes para metodos puros de ifdata_bcb.core.base_explorer."""

from unittest.mock import MagicMock

import pandas as pd
import pytest

from ifdata_bcb.core.base_explorer import BaseExplorer
from ifdata_bcb.domain.exceptions import (
    InvalidDateFormatError,
    InvalidDateRangeError,
    InvalidIdentifierError,
    MissingRequiredParameterError,
)


class ConcreteExplorer(BaseExplorer):
    """Implementacao concreta minima para testes."""

    _COLUMN_MAP = {"DATA_BASE": "DATA", "NOME_INST": "INSTITUICAO"}

    def _get_subdir(self) -> str:
        return "test/data"

    def _get_file_prefix(self) -> str:
        return "test_prefix"


@pytest.fixture
def explorer() -> ConcreteExplorer:
    """Explorer com QueryEngine mockado."""
    mock_qe = MagicMock()
    mock_qe.cache_path = "/fake/path"
    return ConcreteExplorer(query_engine=mock_qe)


class TestNormalizeDates:
    """_normalize_dates: aceita int, str, ou lista."""

    def test_single_int(self, explorer: ConcreteExplorer) -> None:
        assert explorer._normalize_dates(202412) == [202412]

    def test_single_str_yyyymm(self, explorer: ConcreteExplorer) -> None:
        assert explorer._normalize_dates("202412") == [202412]

    def test_single_str_yyyy_mm(self, explorer: ConcreteExplorer) -> None:
        assert explorer._normalize_dates("2024-12") == [202412]

    def test_list_of_ints(self, explorer: ConcreteExplorer) -> None:
        assert explorer._normalize_dates([202401, 202402]) == [202401, 202402]

    def test_list_mixed(self, explorer: ConcreteExplorer) -> None:
        result = explorer._normalize_dates([202401, "2024-06"])
        assert result == [202401, 202406]

    def test_invalid_type_raises(self, explorer: ConcreteExplorer) -> None:
        with pytest.raises(InvalidDateFormatError):
            explorer._normalize_dates([None])


class TestResolveEntity:
    """_resolve_entity: valida CNPJ de 8 digitos."""

    def test_valid_cnpj(self, explorer: ConcreteExplorer) -> None:
        assert explorer._resolve_entity("12345678") == "12345678"

    def test_strips_whitespace(self, explorer: ConcreteExplorer) -> None:
        assert explorer._resolve_entity("  12345678  ") == "12345678"

    def test_too_short_raises(self, explorer: ConcreteExplorer) -> None:
        with pytest.raises(InvalidIdentifierError):
            explorer._resolve_entity("1234567")

    def test_too_long_raises(self, explorer: ConcreteExplorer) -> None:
        with pytest.raises(InvalidIdentifierError):
            explorer._resolve_entity("123456789")

    def test_letters_raise(self, explorer: ConcreteExplorer) -> None:
        with pytest.raises(InvalidIdentifierError):
            explorer._resolve_entity("1234567a")

    def test_formatted_cnpj_raises(self, explorer: ConcreteExplorer) -> None:
        with pytest.raises(InvalidIdentifierError):
            explorer._resolve_entity("12.345.678")

    def test_empty_raises(self, explorer: ConcreteExplorer) -> None:
        with pytest.raises(InvalidIdentifierError):
            explorer._resolve_entity("")


class TestBuildStringCondition:
    """_build_string_condition: gera SQL WHERE para strings."""

    def test_single_value(self, explorer: ConcreteExplorer) -> None:
        result = explorer._build_string_condition("col", ["abc"])
        assert result == "col = 'abc'"

    def test_multiple_values(self, explorer: ConcreteExplorer) -> None:
        result = explorer._build_string_condition("col", ["a", "b", "c"])
        assert result == "col IN ('a', 'b', 'c')"

    def test_escapes_single_quotes(self, explorer: ConcreteExplorer) -> None:
        result = explorer._build_string_condition("col", ["O'Brien"])
        assert result == "col = 'O''Brien'"

    def test_case_insensitive(self, explorer: ConcreteExplorer) -> None:
        result = explorer._build_string_condition("col", ["abc"], case_insensitive=True)
        assert "UPPER(col)" in result
        assert "ABC" in result

    def test_strips_whitespace(self, explorer: ConcreteExplorer) -> None:
        result = explorer._build_string_condition("col", ["  abc  "])
        assert result == "col = 'abc'"

    def test_accent_insensitive(self, explorer: ConcreteExplorer) -> None:
        result = explorer._build_string_condition(
            "col", ["caf\u00e9"], accent_insensitive=True
        )
        assert "strip_accents(col)" in result
        assert "cafe" in result

    def test_case_and_accent_insensitive(self, explorer: ConcreteExplorer) -> None:
        result = explorer._build_string_condition(
            "col",
            ["Patr\u00edmonio L\u00edquido"],
            case_insensitive=True,
            accent_insensitive=True,
        )
        assert "UPPER(strip_accents(col))" in result
        assert "PATRIMONIO LIQUIDO" in result

    def test_accent_insensitive_multiple(self, explorer: ConcreteExplorer) -> None:
        result = explorer._build_string_condition(
            "col",
            ["caf\u00e9", "a\u00e7\u00e3o"],
            accent_insensitive=True,
        )
        assert "strip_accents(col)" in result
        assert "'cafe'" in result
        assert "'acao'" in result


class TestBuildIntCondition:
    """_build_int_condition: gera SQL WHERE para inteiros."""

    def test_single_value(self, explorer: ConcreteExplorer) -> None:
        assert explorer._build_int_condition("col", [202412]) == "col = 202412"

    def test_multiple_values(self, explorer: ConcreteExplorer) -> None:
        result = explorer._build_int_condition("col", [1, 2, 3])
        assert result == "col IN (1, 2, 3)"


class TestJoinConditions:
    """_join_conditions: combina condicoes com AND."""

    def test_multiple_conditions(self, explorer: ConcreteExplorer) -> None:
        result = explorer._join_conditions(["a = 1", "b = 2"])
        assert result == "a = 1 AND b = 2"

    def test_filters_none(self, explorer: ConcreteExplorer) -> None:
        result = explorer._join_conditions([None, "a = 1", None, "b = 2"])
        assert result == "a = 1 AND b = 2"

    def test_all_none_returns_none(self, explorer: ConcreteExplorer) -> None:
        assert explorer._join_conditions([None, None]) is None

    def test_empty_list_returns_none(self, explorer: ConcreteExplorer) -> None:
        assert explorer._join_conditions([]) is None


class TestValidateRequiredParams:
    """_validate_required_params: valida parametros obrigatorios."""

    def test_both_present(self, explorer: ConcreteExplorer) -> None:
        explorer._validate_required_params("12345678", "2024-01")

    def test_missing_instituicao_raises(self, explorer: ConcreteExplorer) -> None:
        with pytest.raises(MissingRequiredParameterError) as exc_info:
            explorer._validate_required_params(None, "2024-01")
        assert exc_info.value.param_name == "instituicao"

    def test_missing_start_raises(self, explorer: ConcreteExplorer) -> None:
        with pytest.raises(MissingRequiredParameterError) as exc_info:
            explorer._validate_required_params("12345678", None)
        assert exc_info.value.param_name == "start"


class TestColumnMapping:
    """_COLUMN_MAP e metodos auxiliares de mapeamento."""

    def test_storage_col_mapped(self, explorer: ConcreteExplorer) -> None:
        assert explorer._storage_col("DATA") == "DATA_BASE"

    def test_storage_col_unmapped(self, explorer: ConcreteExplorer) -> None:
        assert explorer._storage_col("UNKNOWN") == "UNKNOWN"

    def test_reverse_map(self, explorer: ConcreteExplorer) -> None:
        reverse = explorer._reverse_column_map
        assert reverse["DATA"] == "DATA_BASE"
        assert reverse["INSTITUICAO"] == "NOME_INST"

    def test_translate_columns_none(self, explorer: ConcreteExplorer) -> None:
        assert explorer._translate_columns(None) is None

    def test_translate_columns_presentation_names(
        self, explorer: ConcreteExplorer
    ) -> None:
        result = explorer._translate_columns(["DATA", "INSTITUICAO"])
        assert result == ["DATA_BASE", "NOME_INST"]

    def test_translate_columns_storage_names_passthrough(
        self, explorer: ConcreteExplorer
    ) -> None:
        result = explorer._translate_columns(["DATA_BASE", "CNPJ_8"])
        assert result == ["DATA_BASE", "CNPJ_8"]

    def test_translate_columns_mixed(self, explorer: ConcreteExplorer) -> None:
        result = explorer._translate_columns(["DATA", "CNPJ_8"])
        assert result == ["DATA_BASE", "CNPJ_8"]

    def test_apply_column_mapping_empty_df(self, explorer: ConcreteExplorer) -> None:
        """DataFrames vazios devem ter colunas renomeadas."""
        df = pd.DataFrame(columns=["DATA_BASE", "NOME_INST"])
        result = explorer._apply_column_mapping(df)
        assert list(result.columns) == ["DATA", "INSTITUICAO"]
        assert result.empty


class TestResolveDateRange:
    """_resolve_date_range: converte start/end para lista de periodos."""

    def test_none_returns_none(self, explorer: ConcreteExplorer) -> None:
        assert explorer._resolve_date_range(None, None) is None

    def test_start_only_single_date(self, explorer: ConcreteExplorer) -> None:
        result = explorer._resolve_date_range("2024-06", None)
        assert result == [202406]

    def test_start_end_range(self, explorer: ConcreteExplorer) -> None:
        result = explorer._resolve_date_range("2024-01", "2024-03")
        assert result == [202401, 202402, 202403]

    def test_start_after_end_raises(self, explorer: ConcreteExplorer) -> None:
        with pytest.raises(InvalidDateRangeError):
            explorer._resolve_date_range("2024-12", "2024-01")

    def test_trimestral(self, explorer: ConcreteExplorer) -> None:
        result = explorer._resolve_date_range("2024-01", "2024-12", trimestral=True)
        assert result == [202403, 202406, 202409, 202412]
