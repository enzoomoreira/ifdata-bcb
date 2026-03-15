"""Fixtures para testes QA -- cache auto-contido com dados sinteticos."""

from pathlib import Path

import pandas as pd
import pytest

from ifdata_bcb.core.entity_lookup import EntityLookup
from ifdata_bcb.infra.query import QueryEngine
from ifdata_bcb.providers.cosif.explorer import COSIFExplorer
from ifdata_bcb.providers.ifdata.cadastro_explorer import CadastroExplorer
from ifdata_bcb.providers.ifdata.explorer import IFDATAExplorer

QA_CNPJ = "60872504"
QA_CNPJ_B = "90400888"


def _build_qa_cache(base: Path) -> Path:
    """Constroi cache sintetico completo em base."""
    (base / "cosif/individual").mkdir(parents=True)
    (base / "cosif/prudencial").mkdir(parents=True)
    (base / "ifdata/cadastro").mkdir(parents=True)
    (base / "ifdata/valores").mkdir(parents=True)

    pd.DataFrame(
        {
            "Data": pd.array([202303, 202303], dtype="Int64"),
            "CodInst": [QA_CNPJ, QA_CNPJ_B],
            "CNPJ_8": [QA_CNPJ, QA_CNPJ_B],
            "NomeInstituicao": ["BANCO ALFA S.A.", "BANCO BETA S.A."],
            "SegmentoTb": ["S1", "S2"],
            "CodConglomeradoPrudencial": ["40", None],
            "CodConglomeradoFinanceiro": ["50", None],
            "CNPJ_LIDER_8": [QA_CNPJ, None],
            "Situacao": ["A", "A"],
            "Atividade": ["001", "002"],
            "Tcb": ["0001", "0002"],
            "Td": ["01", "02"],
            "Tc": ["1", "2"],
            "Uf": ["SP", "RJ"],
            "Municipio": ["Sao Paulo", "Rio de Janeiro"],
            "Sr": ["01", "02"],
            "DataInicioAtividade": ["19900101", "19950601"],
        }
    ).to_parquet(base / "ifdata/cadastro/ifdata_cad_202303.parquet", index=False)

    pd.DataFrame(
        {
            "DATA_BASE": pd.array([202303] * 4, dtype="Int64"),
            "CNPJ_8": [QA_CNPJ, QA_CNPJ, QA_CNPJ_B, QA_CNPJ_B],
            "NOME_INSTITUICAO": ["BANCO ALFA S.A."] * 2 + ["BANCO BETA S.A."] * 2,
            "DOCUMENTO": ["D1", "D2", "D1", "D2"],
            "CONTA": ["10100", "20200", "10100", "20200"],
            "NOME_CONTA": ["ATIVO TOTAL", "PASSIVO TOTAL"] * 2,
            "SALDO": [1000000.5, 800000.25, 500000.75, 400000.0],
        }
    ).to_parquet(base / "cosif/individual/cosif_ind_202303.parquet", index=False)

    pd.DataFrame(
        {
            "DATA_BASE": pd.array([202303] * 2, dtype="Int64"),
            "CNPJ_8": [QA_CNPJ] * 2,
            "NOME_INSTITUICAO": ["CONGL PRUD ALFA"] * 2,
            "DOCUMENTO": ["D1", "D2"],
            "CONTA": ["10100", "20200"],
            "NOME_CONTA": ["ATIVO TOTAL", "PASSIVO TOTAL"],
            "SALDO": [1500000.0, 1200000.0],
        }
    ).to_parquet(base / "cosif/prudencial/cosif_prud_202303.parquet", index=False)

    pd.DataFrame(
        {
            "AnoMes": pd.array([202303] * 6, dtype="Int64"),
            "CodInst": [QA_CNPJ, QA_CNPJ, "40", "40", "50", "50"],
            "TipoInstituicao": pd.array([3, 3, 1, 1, 2, 2], dtype="Int64"),
            "Conta": ["10100", "20200"] * 3,
            "NomeColuna": ["ATIVO TOTAL", "PASSIVO TOTAL"] * 3,
            "Saldo": [1e6, 8e5, 1.5e6, 1.2e6, 1.6e6, 1.3e6],
            "NomeRelatorio": ["Resumo"] * 6,
            "Grupo": ["Balanco"] * 6,
        }
    ).to_parquet(base / "ifdata/valores/ifdata_val_202303.parquet", index=False)

    return base


@pytest.fixture
def qa_cache(workspace_tmp_dir: Path) -> Path:
    return _build_qa_cache(workspace_tmp_dir)


@pytest.fixture
def qa_cosif(qa_cache: Path) -> COSIFExplorer:
    qe = QueryEngine(base_path=qa_cache)
    el = EntityLookup(query_engine=qe)
    return COSIFExplorer(query_engine=qe, entity_lookup=el)


@pytest.fixture
def qa_ifdata(qa_cache: Path) -> IFDATAExplorer:
    qe = QueryEngine(base_path=qa_cache)
    el = EntityLookup(query_engine=qe)
    return IFDATAExplorer(query_engine=qe, entity_lookup=el)


@pytest.fixture
def qa_cadastro(qa_cache: Path) -> CadastroExplorer:
    qe = QueryEngine(base_path=qa_cache)
    el = EntityLookup(query_engine=qe)
    return CadastroExplorer(query_engine=qe, entity_lookup=el)


@pytest.fixture
def qa_lookup(qa_cache: Path) -> EntityLookup:
    qe = QueryEngine(base_path=qa_cache)
    return EntityLookup(query_engine=qe)
