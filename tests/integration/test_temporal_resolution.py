"""Testes de integracao -- resolucao temporal de conglomerados IFDATA."""

import warnings

from ifdata_bcb.domain.exceptions import PartialDataWarning, ScopeUnavailableWarning
from ifdata_bcb.providers.cosif.explorer import COSIFExplorer
from ifdata_bcb.providers.ifdata.cadastro_explorer import CadastroExplorer
from ifdata_bcb.providers.ifdata.valores_explorer import IFDATAExplorer
from tests.conftest import (
    BANCO_A_CNPJ,
    BANCO_B_CNPJ,
    BANCO_C_CNPJ,
    COD_CONGL_PRUD,
    COD_CONGL_PRUD_NEW,
    COD_CONGL_PRUD_OLD,
)


# =========================================================================
# Resolucao temporal basica
# =========================================================================


class TestTemporalResolutionBasic:
    """Entidade com codigo estavel retorna grupo unico."""

    def test_stable_code_single_group(
        self,
        temporal_explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer],
    ) -> None:
        ifdata = temporal_explorers[1]
        groups, unavailable = ifdata._temporal.resolve(
            [BANCO_A_CNPJ], "prudencial", [202303, 202306]
        )
        assert len(groups) == 1
        assert groups[0].cod_inst == COD_CONGL_PRUD
        assert sorted(groups[0].periodos) == [202303, 202306]
        assert unavailable == []

    def test_individual_does_not_query_cadastro(
        self,
        temporal_explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer],
    ) -> None:
        """Escopo individual retorna grupo trivial sem consultar cadastro."""
        ifdata = temporal_explorers[1]
        groups, unavailable = ifdata._temporal.resolve(
            [BANCO_A_CNPJ], "individual", [202303, 202306]
        )
        assert len(groups) == 1
        assert groups[0].cod_inst == "_individual_"
        assert groups[0].tipo_inst == 3
        assert sorted(groups[0].periodos) == [202303, 202306]
        assert unavailable == []


# =========================================================================
# Mudanca de conglomerado
# =========================================================================


class TestConglomerateChange:
    """Entidade que muda de conglomerado entre periodos."""

    def test_entity_with_code_change_returns_two_groups(
        self,
        temporal_explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer],
    ) -> None:
        ifdata = temporal_explorers[1]
        groups, _ = ifdata._temporal.resolve(
            [BANCO_C_CNPJ], "prudencial", [202303, 202306]
        )
        cod_insts = {g.cod_inst for g in groups}
        assert COD_CONGL_PRUD_OLD in cod_insts
        assert COD_CONGL_PRUD_NEW in cod_insts

    def test_correct_periods_per_group(
        self,
        temporal_explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer],
    ) -> None:
        ifdata = temporal_explorers[1]
        groups, _ = ifdata._temporal.resolve(
            [BANCO_C_CNPJ], "prudencial", [202303, 202306]
        )
        group_map = {g.cod_inst: g.periodos for g in groups}
        assert group_map[COD_CONGL_PRUD_OLD] == [202303]
        assert group_map[COD_CONGL_PRUD_NEW] == [202306]

    def test_read_prudencial_returns_both_periods(
        self,
        temporal_explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer],
    ) -> None:
        ifdata = temporal_explorers[1]
        df = ifdata.read(
            instituicao=BANCO_C_CNPJ,
            start="2023-03",
            end="2023-06",
            escopo="prudencial",
        )
        assert not df.empty
        # Deve ter dados de ambos os periodos
        datas = df["DATA"].dt.to_period("Q").unique()
        assert len(datas) == 2


# =========================================================================
# Backfill / Forward fill
# =========================================================================


class TestBackfillForwardFill:
    def test_backfill_uses_previous_cadastro(
        self,
        temporal_explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer],
    ) -> None:
        """Periodo sem cadastro exato usa ultimo cadastro anterior (backfill)."""
        ifdata = temporal_explorers[1]
        # 202309 nao tem cadastro, backfill de 202306 -> COD_CONGL_PRUD_NEW
        groups, _ = ifdata._temporal.resolve([BANCO_C_CNPJ], "prudencial", [202309])
        assert len(groups) == 1
        assert groups[0].cod_inst == COD_CONGL_PRUD_NEW

    def test_forward_fill_uses_first_cadastro(
        self,
        temporal_explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer],
    ) -> None:
        """Periodo anterior ao primeiro cadastro usa forward fill."""
        ifdata = temporal_explorers[1]
        # 202212 anterior a qualquer cadastro -> forward fill do primeiro (202303)
        groups, _ = ifdata._temporal.resolve([BANCO_C_CNPJ], "prudencial", [202212])
        assert len(groups) == 1
        assert groups[0].cod_inst == COD_CONGL_PRUD_OLD


# =========================================================================
# Batch e warnings
# =========================================================================


class TestBatchAndWarnings:
    def test_batch_with_unavailable_entity_returns_unavailable(
        self,
        temporal_explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer],
    ) -> None:
        """Entidade sem conglomerado prudencial retorna na lista de indisponiveis."""
        ifdata = temporal_explorers[1]
        groups, unavailable = ifdata._temporal.resolve(
            [BANCO_B_CNPJ], "prudencial", [202303]
        )
        assert groups == []
        assert BANCO_B_CNPJ in unavailable

    def test_empty_result_warns(
        self,
        temporal_explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer],
    ) -> None:
        """Read com entidade inexistente retorna vazio e emite warning."""
        ifdata = temporal_explorers[1]
        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            df = ifdata.read(
                instituicao="99999999",
                start="2023-03",
                escopo="individual",
            )
            assert df.empty

    def test_successful_read_no_scope_warning(
        self,
        temporal_explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer],
    ) -> None:
        """Leitura com dados disponiveis nao emite ScopeUnavailableWarning."""
        ifdata = temporal_explorers[1]
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            df = ifdata.read(
                instituicao=BANCO_A_CNPJ,
                start="2023-03",
                escopo="individual",
            )
            scope_warnings = [
                x for x in w if issubclass(x.category, ScopeUnavailableWarning)
            ]
            assert len(scope_warnings) == 0
        assert not df.empty


# =========================================================================
# Falha de query no temporal resolver
# =========================================================================


class TestTemporalResolutionFailure:
    def test_cadastro_query_failure_warns_user(
        self,
        temporal_explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer],
    ) -> None:
        """Falha na query do cadastro emite PartialDataWarning."""
        ifdata = temporal_explorers[1]
        from ifdata_bcb.providers.ifdata import temporal

        original = temporal._ESCOPO_TO_COD_COL.copy()
        temporal._ESCOPO_TO_COD_COL["prudencial"] = "ColunaInexistente"
        try:
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                groups, unavailable = ifdata._temporal.resolve(
                    [BANCO_A_CNPJ], "prudencial", [202303]
                )
            assert groups == []
            partial_warnings = [
                x for x in w if issubclass(x.category, PartialDataWarning)
            ]
            assert len(partial_warnings) >= 1
        finally:
            temporal._ESCOPO_TO_COD_COL.update(original)


# =========================================================================
# Adversarial -- inputs vazios e duplicados
# =========================================================================


class TestTemporalResolverAdversarial:
    def test_resolve_empty_cnpj_list(
        self,
        temporal_explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer],
    ) -> None:
        """Lista vazia de CNPJs retorna tupla vazia sem crashear."""
        ifdata = temporal_explorers[1]
        groups, unavailable = ifdata._temporal.resolve([], "prudencial", [202303])
        assert groups == []
        assert unavailable == []

    def test_resolve_empty_periodos(
        self,
        temporal_explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer],
    ) -> None:
        """Lista vazia de periodos retorna tupla vazia."""
        ifdata = temporal_explorers[1]
        groups, unavailable = ifdata._temporal.resolve([BANCO_A_CNPJ], "prudencial", [])
        assert groups == []
        assert unavailable == []

    def test_resolve_duplicate_cnpjs_no_duplicate_groups(
        self,
        temporal_explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer],
    ) -> None:
        """CNPJs duplicados no input nao geram grupos duplicados."""
        ifdata = temporal_explorers[1]
        groups, _ = ifdata._temporal.resolve(
            [BANCO_A_CNPJ, BANCO_A_CNPJ], "prudencial", [202303]
        )
        cod_insts = [g.cod_inst for g in groups]
        assert len(cod_insts) == len(set(cod_insts))
