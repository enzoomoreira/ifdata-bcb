"""Testes para metodos puros de ifdata_bcb.providers.base_explorer."""

from unittest.mock import MagicMock

import pandas as pd
import pytest

from ifdata_bcb.providers.base_explorer import BaseExplorer
from ifdata_bcb.domain.exceptions import (
    InvalidDateFormatError,
    InvalidDateRangeError,
    InvalidIdentifierError,
    InvalidScopeError,
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
    """_normalize_datas: aceita int, str, ou lista."""

    def test_single_int(self, explorer: ConcreteExplorer) -> None:
        assert explorer._normalize_datas(202412) == [202412]

    def test_single_str_yyyymm(self, explorer: ConcreteExplorer) -> None:
        assert explorer._normalize_datas("202412") == [202412]

    def test_single_str_yyyy_mm(self, explorer: ConcreteExplorer) -> None:
        assert explorer._normalize_datas("2024-12") == [202412]

    def test_list_of_ints(self, explorer: ConcreteExplorer) -> None:
        assert explorer._normalize_datas([202401, 202402]) == [202401, 202402]

    def test_list_mixed(self, explorer: ConcreteExplorer) -> None:
        result = explorer._normalize_datas([202401, "2024-06"])
        assert result == [202401, 202406]

    def test_invalid_type_raises(self, explorer: ConcreteExplorer) -> None:
        with pytest.raises(InvalidDateFormatError):
            explorer._normalize_datas([None])


class TestResolveEntity:
    """_resolve_entidade: valida CNPJ de 8 digitos."""

    def test_valid_cnpj(self, explorer: ConcreteExplorer) -> None:
        assert explorer._resolve_entidade("12345678") == "12345678"

    def test_strips_whitespace(self, explorer: ConcreteExplorer) -> None:
        assert explorer._resolve_entidade("  12345678  ") == "12345678"

    def test_too_short_raises(self, explorer: ConcreteExplorer) -> None:
        with pytest.raises(InvalidIdentifierError):
            explorer._resolve_entidade("1234567")

    def test_too_long_raises(self, explorer: ConcreteExplorer) -> None:
        with pytest.raises(InvalidIdentifierError):
            explorer._resolve_entidade("123456789")

    def test_letters_raise(self, explorer: ConcreteExplorer) -> None:
        with pytest.raises(InvalidIdentifierError):
            explorer._resolve_entidade("1234567a")

    def test_formatted_cnpj_raises(self, explorer: ConcreteExplorer) -> None:
        with pytest.raises(InvalidIdentifierError):
            explorer._resolve_entidade("12.345.678")

    def test_empty_raises(self, explorer: ConcreteExplorer) -> None:
        with pytest.raises(InvalidIdentifierError):
            explorer._resolve_entidade("")


class TestBuildDateCondition:
    """_build_date_condition: gera condicao SQL para datas via storage col."""

    def test_returns_none_when_no_dates(self, explorer: ConcreteExplorer) -> None:
        assert explorer._build_date_condition(None, None) is None

    def test_single_date_uses_storage_col(self, explorer: ConcreteExplorer) -> None:
        result = explorer._build_date_condition("2024-06", None)
        assert "DATA_BASE" in result
        assert "202406" in result

    def test_date_range(self, explorer: ConcreteExplorer) -> None:
        result = explorer._build_date_condition("2024-01", "2024-03")
        assert "DATA_BASE" in result
        assert "IN" in result


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


# =========================================================================
# Explorer com _DERIVED_COLUMNS e _VALID_ESCOPOS para testes adversariais
# =========================================================================


class DerivedExplorer(BaseExplorer):
    """Explorer com colunas derivadas e escopos para testes adversariais."""

    _COLUMN_MAP = {"AnoMes": "DATA", "CodInst": "COD_INST", "NomeColuna": "CONTA"}
    _DERIVED_COLUMNS: set[str] = {"CNPJ_8", "INSTITUICAO", "ESCOPO"}
    _DROP_COLUMNS = ["TipoInstituicao"]
    _COLUMN_ORDER = ["DATA", "CNPJ_8", "ESCOPO", "COD_INST", "CONTA"]
    _VALID_ESCOPOS = ["individual", "prudencial"]

    def _get_subdir(self) -> str:
        return "test/data"

    def _get_file_prefix(self) -> str:
        return "test_prefix"


@pytest.fixture
def derived_explorer() -> DerivedExplorer:
    mock_qe = MagicMock()
    mock_qe.cache_path = "/fake/path"
    return DerivedExplorer(query_engine=mock_qe)


class TestStorageColumnsForQuery:
    """_storage_columns_for_query: filtra derivadas e garante required."""

    def test_none_returns_none(self, derived_explorer: DerivedExplorer) -> None:
        assert derived_explorer._storage_columns_for_query(None) is None

    def test_all_derived_with_required_returns_required_only(
        self, derived_explorer: DerivedExplorer
    ) -> None:
        result = derived_explorer._storage_columns_for_query(
            ["CNPJ_8", "ESCOPO"], required=["CodInst", "TipoInstituicao"]
        )
        assert result == ["CodInst", "TipoInstituicao"]

    def test_all_derived_no_required_returns_none(
        self, derived_explorer: DerivedExplorer
    ) -> None:
        result = derived_explorer._storage_columns_for_query(["CNPJ_8", "ESCOPO"])
        assert result is None

    def test_mix_derived_storage_with_required(
        self, derived_explorer: DerivedExplorer
    ) -> None:
        result = derived_explorer._storage_columns_for_query(
            ["DATA", "CNPJ_8"], required=["CodInst"]
        )
        assert "AnoMes" in result  # DATA -> AnoMes
        assert "CodInst" in result

    def test_required_not_duplicated(self, derived_explorer: DerivedExplorer) -> None:
        result = derived_explorer._storage_columns_for_query(
            ["COD_INST"], required=["CodInst"]
        )
        assert result.count("CodInst") == 1


class TestValidateColumnsEdgeCases:
    """_validate_columns com inputs de borda."""

    def test_empty_list_warns_and_returns_none(
        self, derived_explorer: DerivedExplorer
    ) -> None:
        """Lista vazia emite EmptyFilterWarning e retorna None."""
        import warnings

        from ifdata_bcb.domain.exceptions import EmptyFilterWarning

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = derived_explorer._validate_columns([])
        assert result is None
        empty_warnings = [x for x in w if issubclass(x.category, EmptyFilterWarning)]
        assert len(empty_warnings) == 1

    def test_derived_column_accepted(self, derived_explorer: DerivedExplorer) -> None:
        derived_explorer._validate_columns(["CNPJ_8", "ESCOPO"])

    def test_unknown_column_raises_with_suggestions(
        self, derived_explorer: DerivedExplorer
    ) -> None:
        with pytest.raises(InvalidScopeError, match="COLUNA_FAKE"):
            derived_explorer._validate_columns(["DATA", "COLUNA_FAKE"])


class TestFilterColumnsEdgeCases:
    """_filter_columns com inputs de borda."""

    def test_empty_df_returns_empty(self, derived_explorer: DerivedExplorer) -> None:
        df = pd.DataFrame(columns=["DATA", "CNPJ_8"])
        result = derived_explorer._filter_columns(df, ["DATA"])
        assert result.empty

    def test_presentation_name_after_mapping(
        self, derived_explorer: DerivedExplorer
    ) -> None:
        """Coluna ja renomeada para apresentacao e encontrada corretamente."""
        df = pd.DataFrame({"DATA": [202303], "COD_INST": ["X"]})
        result = derived_explorer._filter_columns(df, ["DATA", "COD_INST"])
        assert list(result.columns) == ["DATA", "COD_INST"]


class TestFinalizeReadEdgeCases:
    """_finalize_read com inputs de borda."""

    def test_empty_df_returns_early(self, derived_explorer: DerivedExplorer) -> None:
        df = pd.DataFrame({"AnoMes": pd.Series([], dtype="Int64")})
        df = derived_explorer._apply_column_mapping(df)
        result = derived_explorer._finalize_read(df)
        assert result.empty

    def test_without_data_column_skips_conversion(
        self, derived_explorer: DerivedExplorer
    ) -> None:
        df = pd.DataFrame({"VALOR": [100.0, 200.0]})
        result = derived_explorer._finalize_read(df)
        assert "DATA" not in result.columns
        assert len(result) == 2


class TestValidateEscopo:
    """_validate_escopo: normaliza e valida."""

    def test_valid_returns_lowercase(self, derived_explorer: DerivedExplorer) -> None:
        assert derived_explorer._validate_escopo("INDIVIDUAL") == "individual"

    def test_invalid_raises(self, derived_explorer: DerivedExplorer) -> None:
        with pytest.raises(InvalidScopeError):
            derived_explorer._validate_escopo("financeiro")

    def test_empty_valid_list_accepts_all(self, explorer: ConcreteExplorer) -> None:
        """ConcreteExplorer tem _VALID_ESCOPOS=[], aceita qualquer escopo."""
        assert explorer._validate_escopo("qualquer_coisa") == "qualquer_coisa"
