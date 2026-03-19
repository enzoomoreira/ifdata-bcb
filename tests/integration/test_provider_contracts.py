"""Testes de contrato publico para explorers em cache vazio."""

import pytest

from ifdata_bcb.domain.exceptions import InvalidScopeError
from ifdata_bcb.infra.query import QueryEngine
from ifdata_bcb.providers.cosif.explorer import COSIFExplorer
from ifdata_bcb.providers.ifdata.cadastro.explorer import CadastroExplorer
from ifdata_bcb.providers.ifdata.valores.explorer import IFDATAExplorer


@pytest.fixture
def query_engine(tmp_cache_dir) -> QueryEngine:
    return QueryEngine(base_path=tmp_cache_dir)


def test_ifdata_read_invalid_scope_raises(query_engine: QueryEngine) -> None:
    explorer = IFDATAExplorer(query_engine=query_engine)

    with pytest.raises(InvalidScopeError):
        explorer.read("2024-12", instituicao="12345678", escopo="invalido")


def test_ifdata_list_methods_return_empty_when_cache_is_missing(
    query_engine: QueryEngine,
) -> None:
    explorer = IFDATAExplorer(query_engine=query_engine)

    accounts = explorer.list_contas()
    institutions = explorer.list_instituicoes()
    reporters = explorer.list_mapeamento()

    assert accounts.empty
    assert list(accounts.columns) == ["COD_CONTA", "CONTA", "RELATORIO", "GRUPO"]
    assert institutions.empty
    assert list(institutions.columns) == [
        "CNPJ_8",
        "INSTITUICAO",
        "TEM_INDIVIDUAL",
        "TEM_PRUDENCIAL",
        "TEM_FINANCEIRO",
        "COD_INST_INDIVIDUAL",
        "COD_INST_PRUDENCIAL",
        "COD_INST_FINANCEIRO",
    ]
    assert reporters.empty
    assert list(reporters.columns) == [
        "COD_INST",
        "TIPO_INST",
        "ESCOPO",
        "REPORT_KEY_TYPE",
        "CNPJ_8",
        "INSTITUICAO",
    ]
    assert explorer.list_relatorios() == []


def test_cosif_list_methods_return_empty_when_cache_is_missing(
    query_engine: QueryEngine,
) -> None:
    explorer = COSIFExplorer(query_engine=query_engine)

    accounts = explorer.list_contas()
    institutions = explorer.list_instituicoes()
    scoped_accounts = explorer.list_contas(escopo="individual")
    scoped_institutions = explorer.list_instituicoes(escopo="prudencial")

    assert accounts.empty
    assert list(accounts.columns) == ["COD_CONTA", "CONTA", "ESCOPOS"]
    assert institutions.empty
    assert list(institutions.columns) == [
        "CNPJ_8",
        "INSTITUICAO",
        "TEM_INDIVIDUAL",
        "TEM_PRUDENCIAL",
    ]
    assert scoped_accounts.empty
    assert list(scoped_accounts.columns) == ["COD_CONTA", "CONTA"]
    assert scoped_institutions.empty
    assert list(scoped_institutions.columns) == ["CNPJ_8", "INSTITUICAO"]


def test_cadastro_list_methods_return_empty_when_cache_is_missing(
    query_engine: QueryEngine,
) -> None:
    explorer = CadastroExplorer(query_engine=query_engine)

    assert explorer.list_segmentos() == []
    assert explorer.list_ufs() == []
