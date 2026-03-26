"""Testes unitarios para providers/enrichment.py."""

import pandas as pd
import pytest

from ifdata_bcb.domain.exceptions import InvalidScopeError
from ifdata_bcb.providers.enrichment import (
    VALID_CADASTRO_COLUMNS,
    enrich_with_cadastro,
    validate_cadastro_columns,
)


# =========================================================================
# validate_cadastro_columns
# =========================================================================


class TestValidateCadastroColumns:
    def test_none_passes(self) -> None:
        validate_cadastro_columns(None)

    def test_valid_columns_pass(self) -> None:
        validate_cadastro_columns(["SEGMENTO", "UF"])

    def test_invalid_column_raises(self) -> None:
        with pytest.raises(InvalidScopeError, match="INEXISTENTE"):
            validate_cadastro_columns(["INEXISTENTE"])

    def test_mix_valid_invalid_reports_invalid(self) -> None:
        with pytest.raises(InvalidScopeError, match="INEXISTENTE"):
            validate_cadastro_columns(["SEGMENTO", "INEXISTENTE"])

    def test_empty_list_passes(self) -> None:
        validate_cadastro_columns([])

    def test_all_valid_columns_accepted(self) -> None:
        validate_cadastro_columns(list(VALID_CADASTRO_COLUMNS))


# =========================================================================
# enrich_with_cadastro -- edge cases com DataFrames inline
# =========================================================================


class TestEnrichWithCadastroEdgeCases:
    def test_empty_financial_df_returns_empty(self) -> None:
        df = pd.DataFrame(columns=["DATA", "CNPJ_8", "VALOR"])
        result = enrich_with_cadastro(
            df, ["SEGMENTO"], query_engine=None, entity_lookup=None
        )
        assert result.empty

    def test_cadastro_empty_adds_na_columns(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Se cadastro nao retorna dados, colunas solicitadas ficam NA."""
        df = pd.DataFrame(
            {
                "DATA": pd.to_datetime(["2023-03-31"]),
                "CNPJ_8": ["60872504"],
                "VALOR": [100.0],
            }
        )

        import ifdata_bcb.providers.ifdata.cadastro.explorer as cad_mod

        class FakeCadastro:
            def __init__(self, **kw):
                pass

            def read(self, *args, **kw):
                return pd.DataFrame()

        monkeypatch.setattr(cad_mod, "CadastroExplorer", FakeCadastro)

        result = enrich_with_cadastro(
            df, ["SEGMENTO", "UF"], query_engine=None, entity_lookup=None
        )
        assert "SEGMENTO" in result.columns
        assert "UF" in result.columns
        assert result["SEGMENTO"].isna().all()

    def test_single_period_uses_simple_merge(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Com data unica, merge e feito por CNPJ_8 sem merge_asof."""
        df = pd.DataFrame(
            {
                "DATA": pd.to_datetime(["2023-03-31", "2023-03-31"]),
                "CNPJ_8": ["60872504", "90400888"],
                "VALOR": [100.0, 200.0],
            }
        )

        cad_df = pd.DataFrame(
            {
                "DATA": pd.to_datetime(["2023-03-31", "2023-03-31"]),
                "CNPJ_8": ["60872504", "90400888"],
                "SEGMENTO": ["S1", "S2"],
            }
        )

        import ifdata_bcb.providers.ifdata.cadastro.explorer as cad_mod

        class FakeCadastro:
            def __init__(self, **kw):
                pass

            def read(self, *args, **kw):
                return cad_df

        monkeypatch.setattr(cad_mod, "CadastroExplorer", FakeCadastro)

        result = enrich_with_cadastro(
            df, ["SEGMENTO"], query_engine=None, entity_lookup=None
        )
        assert not result.empty
        assert "SEGMENTO" in result.columns
        assert set(result["SEGMENTO"]) == {"S1", "S2"}
