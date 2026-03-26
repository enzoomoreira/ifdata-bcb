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
    reporters = explorer.mapeamento()

    assert accounts.empty
    assert list(accounts.columns) == ["COD_CONTA", "CONTA", "RELATORIO", "GRUPO"]
    assert reporters.empty
    assert list(reporters.columns) == [
        "COD_INST",
        "TIPO_INST",
        "ESCOPO",
        "REPORT_KEY_TYPE",
        "CNPJ_8",
        "INSTITUICAO",
    ]


def test_cosif_list_methods_return_empty_when_cache_is_missing(
    query_engine: QueryEngine,
) -> None:
    explorer = COSIFExplorer(query_engine=query_engine)

    accounts = explorer.list_contas()
    scoped_accounts = explorer.list_contas(escopo="individual")

    assert accounts.empty
    assert list(accounts.columns) == ["COD_CONTA", "CONTA", "ESCOPOS"]
    assert scoped_accounts.empty
    assert list(scoped_accounts.columns) == ["COD_CONTA", "CONTA"]


def test_cadastro_list_empty_when_cache_is_missing(
    query_engine: QueryEngine,
) -> None:
    explorer = CadastroExplorer(query_engine=query_engine)

    # list() retorna DataFrame vazio com colunas corretas
    df = explorer.list(["SEGMENTO"])
    assert df.empty
    assert list(df.columns) == ["SEGMENTO"]
