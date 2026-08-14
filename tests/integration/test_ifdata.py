"""Testes de integracao -- IFDATA read() e list methods."""

import warnings

import pandas as pd
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
        assert df.index.name == "date"
        for col in ("cnpj_8", "valor"):
            assert col in df.columns

    def test_read_individual_filters_by_institution(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        df = explorers[1].read(
            instituicao=BANCO_A_CNPJ, start="2023-03", escopo="individual"
        )
        assert all(df["cnpj_8"] == BANCO_A_CNPJ)

    def test_read_prudencial_resolves_conglomerate(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        df = explorers[1].read(
            instituicao=BANCO_A_CNPJ, start="2023-03", escopo="prudencial"
        )
        assert not df.empty
        assert BANCO_A_CNPJ in df["cnpj_8"].values

    def test_read_no_data_returns_empty(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        df = explorers[1].read(
            instituicao="99999999", start="2023-03", escopo="individual"
        )
        assert df.empty
        assert isinstance(df.index, pd.DatetimeIndex)
        assert "cnpj_8" in df.columns

    def test_read_includes_cod_conta(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        df = explorers[1].read(
            instituicao=BANCO_A_CNPJ, start="2023-03", escopo="individual"
        )
        assert "cod_conta" in df.columns

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
        assert all(df["cod_conta"] == "10100")


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
        assert "relatorio" in df.columns
        assert "grupo" in df.columns

    def test_list_contas_filters_by_relatorio(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        df = explorers[1].list_contas(relatorio="Resumo")
        assert not df.empty
        assert all(df["relatorio"].str.upper().str.contains("RESUMO"))

    def test_list_contas_filters_by_period(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        df = explorers[1].list_contas(start="2023-03")
        assert not df.empty
        assert "cod_conta" in df.columns


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
        assert (df["relatorio"] == "Resumo").all()

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
        assert (df["relatorio"] == "Resumo").all()
        assert (df["conta"] == "ATIVO TOTAL").all()


class TestIFDATAListMethodsExtended:
    """Metodos list/describe que nao tinham cobertura."""

    def test_mapeamento_returns_mapping(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        df = explorers[1].mapeamento()
        assert not df.empty
        for col in ("cod_inst", "tipo_inst", "escopo", "cnpj_8"):
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
        """Solicitar apenas colunas vindas do storage (nao-derivadas) funciona."""
        df = explorers[1].read(
            instituicao=BANCO_A_CNPJ,
            start="2023-03",
            escopo="individual",
            columns=["data", "valor"],
        )
        assert not df.empty
        # data pedida em columns= vira o DatetimeIndex, nao uma coluna
        assert df.index.name == "date"
        assert list(df.columns) == ["valor"]

    def test_columns_derived_only(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        """Solicitar apenas colunas derivadas (cnpj_8, escopo, instituicao)."""
        df = explorers[1].read(
            instituicao=BANCO_A_CNPJ,
            start="2023-03",
            escopo="individual",
            columns=["cnpj_8", "escopo"],
        )
        assert not df.empty
        assert "cnpj_8" in df.columns
        assert "escopo" in df.columns

    def test_columns_mix_storage_and_derived(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        """Mix de colunas de storage e derivadas."""
        df = explorers[1].read(
            instituicao=BANCO_A_CNPJ,
            start="2023-03",
            escopo="individual",
            columns=["data", "cnpj_8", "valor", "escopo"],
        )
        assert not df.empty
        assert df.index.name == "date"
        assert set(df.columns) == {"cnpj_8", "valor", "escopo"}

    def test_columns_unknown_raises_early(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        """Coluna desconhecida levanta InvalidColumnError antes da query."""
        from ifdata_bcb.domain.exceptions import InvalidColumnError

        with pytest.raises(InvalidColumnError, match="COLUNA_INEXISTENTE"):
            explorers[1].read(
                instituicao=BANCO_A_CNPJ,
                start="2023-03",
                escopo="individual",
                columns=["data", "COLUNA_INEXISTENTE"],
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
        # Deve conter colunas padrao (data vira o index)
        assert df.index.name == "date"
        for col in ("cnpj_8", "valor", "cod_conta", "conta"):
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
        assert "escopo" in df.columns
        assert (df["escopo"] == "financeiro").all()

    def test_read_financeiro_entity_without_congl_returns_empty(
        self, explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]
    ) -> None:
        with warnings.catch_warnings(record=True):
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
            cadastro=["segmento"],
        )
        assert not df.empty
        assert "segmento" in df.columns
        assert df["segmento"].notna().any()


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
            columns=["cnpj_8", "escopo"],
        )
        assert not df.empty
        assert "cnpj_8" in df.columns
        assert "escopo" in df.columns

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
        assert df.index.name == "date"
        for col in ("cnpj_8", "valor", "cod_conta", "conta"):
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
        cnpjs = df["cnpj_8"].unique()
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
