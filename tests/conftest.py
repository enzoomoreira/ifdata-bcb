"""Fixtures compartilhadas para a suite de testes."""

from collections.abc import Generator
from pathlib import Path

import pandas as pd
import pytest

from ifdata_bcb.infra.paths import temp_dir


@pytest.fixture
def workspace_tmp_dir() -> Generator[Path, None, None]:
    """Diretorio temporario em %TEMP% com cleanup automatico."""
    with temp_dir(prefix="test") as path:
        yield path


@pytest.fixture
def tmp_cache_dir(workspace_tmp_dir: Path) -> Path:
    """Diretorio temporario para simular cache de dados."""
    cache_dir = workspace_tmp_dir / "cache"
    cache_dir.mkdir()
    return cache_dir


# =========================================================================
# Fixtures de dados amostrais para testes de integracao
# =========================================================================

# Entidades de teste
BANCO_A_CNPJ = "60872504"  # Entidade individual com conglomerado
BANCO_B_CNPJ = "90400888"  # Entidade individual sem conglomerado
LIDER_CNPJ = "60872504"  # Lider do conglomerado
COD_CONGL_PRUD = "40"
COD_CONGL_FIN = "50"


def _make_cadastro_df() -> pd.DataFrame:
    """Cria DataFrame amostral de cadastro com entidades reais e alias."""
    return pd.DataFrame(
        {
            "Data": pd.array([202303, 202303, 202303, 202306, 202306], dtype="Int64"),
            "CodInst": [
                BANCO_A_CNPJ,
                BANCO_B_CNPJ,
                "PRUD_ALIAS",
                BANCO_A_CNPJ,
                BANCO_B_CNPJ,
            ],
            "CNPJ_8": [BANCO_A_CNPJ, BANCO_B_CNPJ, None, BANCO_A_CNPJ, BANCO_B_CNPJ],
            "NomeInstituicao": [
                "BANCO ALFA S.A.",
                "BANCO BETA S.A.",
                "CONGL PRUDENCIAL ALFA",
                "BANCO ALFA S.A.",
                "BANCO BETA S.A.",
            ],
            "SegmentoTb": ["S1", "S2", None, "S1", "S2"],
            "CodConglomeradoPrudencial": [
                COD_CONGL_PRUD,
                None,
                COD_CONGL_PRUD,
                COD_CONGL_PRUD,
                None,
            ],
            "CodConglomeradoFinanceiro": [
                COD_CONGL_FIN,
                None,
                COD_CONGL_FIN,
                COD_CONGL_FIN,
                None,
            ],
            "CNPJ_LIDER_8": [
                LIDER_CNPJ,
                None,
                LIDER_CNPJ,
                LIDER_CNPJ,
                None,
            ],
            "Situacao": ["A", "A", None, "A", "A"],
            "Atividade": ["001", "002", None, "001", "002"],
            "Tcb": ["0001", "0002", None, "0001", "0002"],
            "Td": ["01", "02", None, "01", "02"],
            "Tc": ["1", "2", None, "1", "2"],
            "Uf": ["SP", "RJ", None, "SP", "RJ"],
            "Municipio": [
                "Sao Paulo",
                "Rio de Janeiro",
                None,
                "Sao Paulo",
                "Rio de Janeiro",
            ],
            "Sr": ["01", "02", None, "01", "02"],
            "DataInicioAtividade": [
                "19900101",
                "19950601",
                None,
                "19900101",
                "19950601",
            ],
        }
    )


def _make_cosif_individual_df() -> pd.DataFrame:
    """Cria DataFrame amostral COSIF individual."""
    return pd.DataFrame(
        {
            "DATA_BASE": pd.array([202303, 202303, 202303, 202303], dtype="Int64"),
            "CNPJ_8": [BANCO_A_CNPJ, BANCO_A_CNPJ, BANCO_B_CNPJ, BANCO_B_CNPJ],
            "NOME_INSTITUICAO": [
                "BANCO ALFA S.A.",
                "BANCO ALFA S.A.",
                "BANCO BETA S.A.",
                "BANCO BETA S.A.",
            ],
            "DOCUMENTO": ["D1", "D2", "D1", "D2"],
            "CONTA": ["10100", "20200", "10100", "20200"],
            "NOME_CONTA": [
                "ATIVO TOTAL",
                "PASSIVO TOTAL",
                "ATIVO TOTAL",
                "PASSIVO TOTAL",
            ],
            "SALDO": [1000000.50, 800000.25, 500000.75, 400000.00],
        }
    )


def _make_cosif_prudencial_df() -> pd.DataFrame:
    """Cria DataFrame amostral COSIF prudencial."""
    return pd.DataFrame(
        {
            "DATA_BASE": pd.array([202303, 202303], dtype="Int64"),
            "CNPJ_8": [LIDER_CNPJ, LIDER_CNPJ],
            "NOME_INSTITUICAO": [
                "CONGL PRUDENCIAL ALFA",
                "CONGL PRUDENCIAL ALFA",
            ],
            "DOCUMENTO": ["D1", "D2"],
            "CONTA": ["10100", "20200"],
            "NOME_CONTA": ["ATIVO TOTAL", "PASSIVO TOTAL"],
            "SALDO": [1500000.00, 1200000.00],
        }
    )


def _make_ifdata_valores_df() -> pd.DataFrame:
    """Cria DataFrame amostral IFDATA Valores com 3 tipos de instituicao."""
    return pd.DataFrame(
        {
            "AnoMes": pd.array(
                [202303, 202303, 202303, 202303, 202303, 202303], dtype="Int64"
            ),
            "CodInst": [
                BANCO_A_CNPJ,
                BANCO_A_CNPJ,
                COD_CONGL_PRUD,
                COD_CONGL_PRUD,
                COD_CONGL_FIN,
                COD_CONGL_FIN,
            ],
            "TipoInstituicao": pd.array([3, 3, 1, 1, 2, 2], dtype="Int64"),
            "Conta": ["10100", "20200", "10100", "20200", "10100", "20200"],
            "NomeColuna": [
                "ATIVO TOTAL",
                "PASSIVO TOTAL",
                "ATIVO TOTAL",
                "PASSIVO TOTAL",
                "ATIVO TOTAL",
                "PASSIVO TOTAL",
            ],
            "Saldo": [
                1000000.50,
                800000.25,
                1500000.00,
                1200000.00,
                1600000.00,
                1300000.00,
            ],
            "NomeRelatorio": ["Resumo"] * 6,
            "Grupo": ["Balanco"] * 6,
        }
    )


def _save_parquet(
    df: pd.DataFrame, cache_dir: Path, subdir: str, filename: str
) -> Path:
    """Salva DataFrame como Parquet no cache."""
    target_dir = cache_dir / subdir
    target_dir.mkdir(parents=True, exist_ok=True)
    filepath = target_dir / f"{filename}.parquet"
    df.to_parquet(filepath, engine="pyarrow", index=False)
    return filepath


@pytest.fixture
def populated_cache(tmp_cache_dir: Path) -> Path:
    """Cache com dados amostrais de todas as fontes (cosif, ifdata, cadastro)."""
    _save_parquet(
        _make_cadastro_df(), tmp_cache_dir, "ifdata/cadastro", "ifdata_cad_202303"
    )
    _save_parquet(
        _make_cadastro_df()[_make_cadastro_df()["Data"] == 202306],
        tmp_cache_dir,
        "ifdata/cadastro",
        "ifdata_cad_202306",
    )
    _save_parquet(
        _make_cosif_individual_df(),
        tmp_cache_dir,
        "cosif/individual",
        "cosif_ind_202303",
    )
    _save_parquet(
        _make_cosif_prudencial_df(),
        tmp_cache_dir,
        "cosif/prudencial",
        "cosif_prud_202303",
    )
    _save_parquet(
        _make_ifdata_valores_df(), tmp_cache_dir, "ifdata/valores", "ifdata_val_202303"
    )
    return tmp_cache_dir
