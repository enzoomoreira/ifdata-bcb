"""Testes de integracao -- schemas heterogeneos entre periodos (union_by_name)."""

from ifdata_bcb.providers.cosif.explorer import COSIFExplorer
from ifdata_bcb.providers.ifdata.cadastro_explorer import CadastroExplorer
from ifdata_bcb.providers.ifdata.valores_explorer import IFDATAExplorer
from tests.conftest import BANCO_A_CNPJ, COD_CONGL_PRUD


class TestHeterogeneousSchemas:
    """Parquets com tipos conflitantes (DOUBLE vs VARCHAR) para mesma coluna."""

    def test_resolve_temporal_with_mixed_types(
        self,
        heterogeneous_explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer],
    ) -> None:
        """_resolve_temporal nao falha com CodConglomeradoPrudencial heterogeneo."""
        ifdata = heterogeneous_explorers[1]
        groups, _ = ifdata._temporal.resolve(
            [BANCO_A_CNPJ], "prudencial", [202303, 202306]
        )
        # Deve resolver apesar do tipo misto (DuckDB faz cast automatico)
        assert len(groups) >= 1
        # Todos os periodos devem estar cobertos
        all_periodos = set()
        for g in groups:
            all_periodos.update(g.periodos)
        assert 202303 in all_periodos
        assert 202306 in all_periodos

    def test_get_entity_identifiers_with_mixed_types(
        self,
        heterogeneous_explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer],
    ) -> None:
        """get_entity_identifiers retorna cod_congl_prud mesmo com tipos mistos."""
        el = heterogeneous_explorers[1].resolver
        info = el.get_entity_identifiers(BANCO_A_CNPJ)
        assert info["cod_congl_prud"] is not None
        # O valor pode ser "40" ou "40.0" dependendo do cast, mas nao deve ser None
        assert COD_CONGL_PRUD in str(info["cod_congl_prud"])

    def test_canonical_names_with_mixed_types(
        self,
        heterogeneous_explorers: tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer],
    ) -> None:
        """get_canonical_names_for_cnpjs funciona com schema heterogeneo."""
        el = heterogeneous_explorers[1].resolver
        names = el.get_canonical_names_for_cnpjs([BANCO_A_CNPJ])
        assert names[BANCO_A_CNPJ] == "BANCO ALFA S.A."
