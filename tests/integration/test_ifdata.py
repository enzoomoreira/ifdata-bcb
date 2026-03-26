"""Testes de integracao -- IFDATA read() e list methods."""

import warnings

import pytest

from ifdata_bcb.providers.cosif.explorer import COSIFExplorer
from ifdata_bcb.providers.ifdata.cadastro.explorer import CadastroExplorer
from ifdata_bcb.providers.ifdata.valores.explorer import IFDATAExplorer
from tests.conftest import BANCO_A_CNPJ, BANCO_B_CNPJ


class TestIFDATARead:
    def test_read_individual_returns_data(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        df = explorers[1].read(
            instituicao=BANCO_A_CNPJ, start="2023-03", escopo="individual"
        )
        assert not df.empty
        for col in ("DATA", "CNPJ_8", "VALOR"):
            assert col in df.columns

    def test_read_individual_filters_by_institution(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        df = explorers[1].read(
            instituicao=BANCO_A_CNPJ, start="2023-03", escopo="individual"
        )
        assert all(df["CNPJ_8"] == BANCO_A_CNPJ)

    def test_read_prudencial_resolves_conglomerate(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        df = explorers[1].read(
            instituicao=BANCO_A_CNPJ, start="2023-03", escopo="prudencial"
        )
        assert not df.empty
        assert BANCO_A_CNPJ in df["CNPJ_8"].values

    def test_read_no_data_returns_empty(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        df = explorers[1].read(
            instituicao="99999999", start="2023-03", escopo="individual"
        )
        assert df.empty
        assert "DATA" in df.columns

    def test_read_includes_cod_conta(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        df = explorers[1].read(
            instituicao=BANCO_A_CNPJ, start="2023-03", escopo="individual"
        )
        assert "COD_CONTA" in df.columns

    def test_read_filters_by_account_code(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        df = explorers[1].read(
            instituicao=BANCO_A_CNPJ,
            start="2023-03",
            conta="10100",
            escopo="individual",
        )
        assert not df.empty
        assert all(df["COD_CONTA"] == "10100")


class TestIFDATAListMethods:
    def test_list_periodos(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        assert 202303 in explorers[1].list_periodos()

    def test_has_data(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        assert explorers[1].has_data() is True

    def test_list_contas_includes_relatorio_and_grupo(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        df = explorers[1].list_contas()
        assert "RELATORIO" in df.columns
        assert "GRUPO" in df.columns

    def test_list_contas_filters_by_relatorio(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        df = explorers[1].list_contas(relatorio="Resumo")
        assert not df.empty
        assert all(df["RELATORIO"].str.upper().str.contains("RESUMO"))

    def test_list_contas_filters_by_period(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        df = explorers[1].list_contas(start="2023-03")
        assert not df.empty
        assert "COD_CONTA" in df.columns


class TestIFDATAReadRelatorio:
    """read() com filtro relatorio= (padrao real de uso)."""

    def test_read_with_relatorio_filter(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        df = explorers[1].read(
            instituicao=BANCO_A_CNPJ,
            start="2023-03",
            escopo="individual",
            relatorio="Resumo",
        )
        assert not df.empty
        assert (df["RELATORIO"] == "Resumo").all()

    def test_read_with_relatorio_and_conta_combined(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        df = explorers[1].read(
            instituicao=BANCO_A_CNPJ,
            start="2023-03",
            escopo="individual",
            relatorio="Resumo",
            conta="ATIVO TOTAL",
        )
        assert not df.empty
        assert (df["RELATORIO"] == "Resumo").all()
        assert (df["CONTA"] == "ATIVO TOTAL").all()


class TestIFDATAListMethodsExtended:
    """Metodos list/describe que nao tinham cobertura."""

    def test_mapeamento_returns_mapping(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        df = explorers[1].mapeamento()
        assert not df.empty
        for col in ("COD_INST", "TIPO_INST", "ESCOPO", "CNPJ_8"):
            assert col in df.columns

    def test_describe_returns_info(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        desc = explorers[1].describe()
        assert isinstance(desc, dict)
        assert desc["has_data"] is True
        assert desc["period_count"] > 0
        assert 202303 in desc["periods"]


# =========================================================================
# columns= parameter (early _validate_columns + _filter_columns)
# =========================================================================


class TestIFDATAColumns:
    def test_columns_storage_only(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        """Solicitar apenas colunas de storage (nomes do parquet) funciona."""
        df = explorers[1].read(
            instituicao=BANCO_A_CNPJ,
            start="2023-03",
            escopo="individual",
            columns=["DATA", "VALOR"],
        )
        assert not df.empty
        assert "DATA" in df.columns
        assert "VALOR" in df.columns
        # Colunas nao solicitadas nao devem estar presentes
        assert "COD_CONTA" not in df.columns

    def test_columns_derived_only(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        """Solicitar apenas colunas derivadas (CNPJ_8, ESCOPO, INSTITUICAO)."""
        df = explorers[1].read(
            instituicao=BANCO_A_CNPJ,
            start="2023-03",
            escopo="individual",
            columns=["CNPJ_8", "ESCOPO"],
        )
        assert not df.empty
        assert "CNPJ_8" in df.columns
        assert "ESCOPO" in df.columns

    def test_columns_mix_storage_and_derived(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        """Mix de colunas de storage e derivadas."""
        df = explorers[1].read(
            instituicao=BANCO_A_CNPJ,
            start="2023-03",
            escopo="individual",
            columns=["DATA", "CNPJ_8", "VALOR", "ESCOPO"],
        )
        assert not df.empty
        assert set(df.columns) == {"DATA", "CNPJ_8", "VALOR", "ESCOPO"}

    def test_columns_unknown_raises_early(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        """Coluna desconhecida levanta InvalidScopeError antes da query."""
        from ifdata_bcb.domain.exceptions import InvalidScopeError

        with pytest.raises(InvalidScopeError, match="COLUNA_INEXISTENTE"):
            explorers[1].read(
                instituicao=BANCO_A_CNPJ,
                start="2023-03",
                escopo="individual",
                columns=["DATA", "COLUNA_INEXISTENTE"],
            )

    def test_columns_none_returns_all(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        """columns=None retorna todas as colunas padrao."""
        df = explorers[1].read(
            instituicao=BANCO_A_CNPJ,
            start="2023-03",
            escopo="individual",
            columns=None,
        )
        assert not df.empty
        # Deve conter colunas padrao
        for col in ("DATA", "CNPJ_8", "VALOR", "COD_CONTA", "CONTA"):
            assert col in df.columns


# =========================================================================
# escopo financeiro
# =========================================================================


class TestIFDATAFinanceiro:
    """Testes end-to-end para escopo financeiro."""

    def test_read_financeiro_returns_data(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        df = explorers[1].read(
            instituicao=BANCO_A_CNPJ,
            start="2023-03",
            escopo="financeiro",
        )
        assert not df.empty
        assert "ESCOPO" in df.columns
        assert (df["ESCOPO"] == "financeiro").all()

    def test_read_financeiro_entity_without_congl_returns_empty(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            df = explorers[1].read(
                instituicao=BANCO_B_CNPJ,
                start="2023-03",
                escopo="financeiro",
            )
        assert df.empty


# =========================================================================
# LIKE ESCAPE contra DuckDB real
# =========================================================================


class TestLikeEscapeIntegration:
    def test_list_contas_with_underscore_in_term(
        self,
        explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer],
    ) -> None:
        """Busca com _ no termo nao trata como wildcard."""
        df = explorers[1].list_contas(termo="ATIVO_TOTAL")
        assert df.empty

    def test_list_contas_normal_term_still_works(
        self,
        explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer],
    ) -> None:
        """Busca normal continua funcionando com ESCAPE clause."""
        df = explorers[1].list_contas(termo="ATIVO")
        assert not df.empty


# =========================================================================
# enrich_with_cadastro multi-periodo (merge_asof)
# =========================================================================


class TestEnrichmentMultiPeriod:
    def test_cadastro_enrichment_multi_period(
        self,
        temporal_explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer],
    ) -> None:
        """Enriquecimento cadastral funciona com multiplos periodos."""
        df = temporal_explorers[1].read(
            instituicao=BANCO_A_CNPJ,
            start="2023-03",
            end="2023-06",
            escopo="individual",
            cadastro=["SEGMENTO"],
        )
        assert not df.empty
        assert "SEGMENTO" in df.columns
        assert df["SEGMENTO"].notna().any()


# =========================================================================
# Adversarial -- columns e inputs de borda
# =========================================================================


class TestIFDATAColumnsAdversarial:
    def test_columns_all_derived_returns_data(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        """Pedir apenas colunas derivadas retorna DataFrame com essas colunas."""
        df = explorers[1].read(
            instituicao=BANCO_A_CNPJ,
            start="2023-03",
            escopo="individual",
            columns=["CNPJ_8", "ESCOPO"],
        )
        assert not df.empty
        assert "CNPJ_8" in df.columns
        assert "ESCOPO" in df.columns

    def test_columns_empty_list_warns_and_returns_all(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        """columns=[] emite warning e retorna todas as colunas (tratado como None)."""
        import warnings

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            df = explorers[1].read(
                instituicao=BANCO_A_CNPJ,
                start="2023-03",
                escopo="individual",
                columns=[],
            )
        # EmptyFilterWarning emitido sobre filtro vazio
        from ifdata_bcb.domain.exceptions import EmptyFilterWarning

        empty_col_warnings = [
            x for x in w if issubclass(x.category, EmptyFilterWarning)
        ]
        assert len(empty_col_warnings) == 1
        # Retorna todas as colunas (mesmo comportamento que columns=None)
        assert not df.empty
        for col in ("DATA", "CNPJ_8", "VALOR", "COD_CONTA", "CONTA"):
            assert col in df.columns


class TestIFDATAReadAdversarial:
    def test_read_multiple_institutions_batch(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        """Lista de CNPJs retorna dados de multiplas entidades."""
        df = explorers[1].read(
            instituicao=[BANCO_A_CNPJ, BANCO_B_CNPJ],
            start="2023-03",
            escopo="individual",
        )
        assert not df.empty
        cnpjs = df["CNPJ_8"].unique()
        assert BANCO_A_CNPJ in cnpjs

    def test_read_nonexistent_account_returns_empty(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        """Conta inexistente com instituicao valida retorna vazio."""
        df = explorers[1].read(
            instituicao=BANCO_A_CNPJ,
            start="2023-03",
            escopo="individual",
            conta="99999_INEXISTENTE",
        )
        assert df.empty
