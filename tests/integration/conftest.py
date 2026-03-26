"""Fixtures para testes de integracao."""

from pathlib import Path

import pytest

from ifdata_bcb.core.entity import EntityLookup
from ifdata_bcb.infra.query import QueryEngine
from ifdata_bcb.providers.cosif.explorer import COSIFExplorer
from ifdata_bcb.providers.ifdata.cadastro.explorer import CadastroExplorer
from ifdata_bcb.providers.ifdata.valores.explorer import IFDATAExplorer


def _build_explorers(
    cache_dir: Path,
) -> tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]:
    qe = QueryEngine(base_path=cache_dir)
    el = EntityLookup(query_engine=qe)
    return (
        COSIFExplorer(query_engine=qe, entity_lookup=el),
        IFDATAExplorer(query_engine=qe, entity_lookup=el),
        CadastroExplorer(query_engine=qe, entity_lookup=el),
    )


@pytest.fixture
def explorers(
    populated_cache: Path,
) -> tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]:
    return _build_explorers(populated_cache)


@pytest.fixture
def temporal_explorers(
    populated_cache_temporal: Path,
) -> tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]:
    return _build_explorers(populated_cache_temporal)


@pytest.fixture
def heterogeneous_explorers(
    heterogeneous_cache: Path,
) -> tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]:
    return _build_explorers(heterogeneous_cache)


@pytest.fixture
def fin_disappeared_explorers(
    fin_disappeared_cache: Path,
) -> tuple[COSIFExplorer, IFDATAExplorer, CadastroExplorer]:
    return _build_explorers(fin_disappeared_cache)
