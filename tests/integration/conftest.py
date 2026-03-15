"""Fixtures para testes de integracao."""

from pathlib import Path

import pytest

from ifdata_bcb.core.entity_lookup import EntityLookup
from ifdata_bcb.infra.query import QueryEngine
from ifdata_bcb.providers.cosif.explorer import COSIFExplorer
from ifdata_bcb.providers.ifdata.cadastro_explorer import CadastroExplorer
from ifdata_bcb.providers.ifdata.explorer import IFDATAExplorer


@pytest.fixture
def explorers(
    populated_cache: Path,
) -> tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]:
    qe = QueryEngine(base_path=populated_cache)
    el = EntityLookup(query_engine=qe)
    return (
        COSIFExplorer(query_engine=qe, entity_lookup=el),
        IFDATAExplorer(query_engine=qe, entity_lookup=el),
        CadastroExplorer(query_engine=qe, entity_lookup=el),
    )
