"""Valida a classificacao de era contra os parquets reais do cache local.

A deteccao de era e derivada do dado, entao a rede de seguranca contra uma
mudanca futura do BCB e comparar o veredito atual com o que foi medido na
transicao 202412 -> 202503. Se o BCB reclassificar um relatorio, este teste
falha e a mudanca vira decisao consciente em vez de silencio.

Depende do cache do usuario: skipa quando os periodos nao estao baixados.
"""

from pathlib import Path

import duckdb
import pandas as pd
import pytest

from ifdata_bcb.core.eras import IFDATA_ERA_BOUNDARY, diagnose_eras
from ifdata_bcb.infra.config import get_settings

PRE, POST = 202412, 202503

# Medido em 2026-08 sobre os parquets do BCB. status + overlap por relatorio.
ESPERADO: dict[str, tuple[str, float]] = {
    "Ativo": ("renumerado", 0.0),
    "Passivo": ("renumerado", 0.0),
    "Demonstracao de Resultado": ("renumerado", 0.0),
    "Resumo": ("renumerado", 30.0),
    "Segmentacao": ("renumerado", 60.0),
    "Informacoes de Capital": ("estavel", 95.8),
    "Carteira de credito ativa - por indexador": ("estavel", 100.0),
    "Carteira de credito ativa - por regiao geografica": ("estavel", 100.0),
    "Carteira de credito ativa - quantidade de clientes e de operacoes": (
        "estavel",
        100.0,
    ),
    "Carteira de credito ativa Pessoa Fisica - modalidade e prazo de vencimento": (
        "estavel",
        100.0,
    ),
    "Carteira de credito ativa Pessoa Juridica - modalidade e prazo de vencimento": (
        "estavel",
        100.0,
    ),
    "Carteira de credito ativa Pessoa Juridica - por atividade economica (CNAE)": (
        "estavel",
        100.0,
    ),
    "Carteira de credito ativa Pessoa Juridica - por porte do tomador": (
        "estavel",
        100.0,
    ),
    "Carteira de credito ativa - por nivel de risco da operacao": ("so_pre", 0.0),
    "Carteira de credito ativa - por carteiras de instrumentos financeiros": (
        "so_post",
        0.0,
    ),
}


def _parquet(periodo: int) -> Path:
    return (
        get_settings().cache_path
        / "ifdata"
        / "valores"
        / f"ifdata_val_{periodo}.parquet"
    )


pytestmark = pytest.mark.skipif(
    not (_parquet(PRE).exists() and _parquet(POST).exists()),
    reason=f"cache local sem ifdata_val_{PRE}/{POST}",
)


@pytest.fixture(scope="module")
def diagnostico() -> dict:
    paths = [
        str(_parquet(PRE)).replace("\\", "/"),
        str(_parquet(POST)).replace("\\", "/"),
    ]
    df = (
        duckdb.connect()
        .sql(f"""
        SELECT AnoMes, NomeRelatorio AS RELATORIO, Conta AS COD_CONTA
        FROM read_parquet(['{paths[0]}', '{paths[1]}'])
    """)
        .df()
    )
    df["DATA"] = pd.to_datetime(df["AnoMes"].astype(str) + "01", format="%Y%m%d")
    return diagnose_eras(
        df,
        boundary=IFDATA_ERA_BOUNDARY,
        source="IFDATA",
        periodos_solicitados=[PRE, POST],
        group_col="RELATORIO",
    )


def _normalizar(grupos: dict) -> dict:
    from ifdata_bcb.core.eras import _normalize_report_name

    return {_normalize_report_name(k): v for k, v in grupos.items()}


@pytest.mark.parametrize("relatorio", sorted(ESPERADO))
def test_classificacao_por_relatorio(relatorio: str, diagnostico: dict) -> None:
    from ifdata_bcb.core.eras import _normalize_report_name

    grupo = _normalizar(diagnostico["grupos"]).get(_normalize_report_name(relatorio))
    assert grupo is not None, f"'{relatorio}' sumiu dos dados do BCB"

    status_esperado, pct_esperado = ESPERADO[relatorio]
    assert grupo["status"] == status_esperado, (
        f"'{relatorio}': status virou {grupo['status']} "
        f"({grupo['pct_overlap']}% de overlap)"
    )
    assert grupo["pct_overlap"] == pytest.approx(pct_esperado, abs=0.1)


def test_nenhum_relatorio_novo_sem_classificacao(diagnostico: dict) -> None:
    """Relatorio que o BCB adicionar depois desta medicao aparece aqui."""
    from ifdata_bcb.core.eras import _normalize_report_name

    conhecidos = {_normalize_report_name(k) for k in ESPERADO}
    novos = sorted(set(_normalizar(diagnostico["grupos"])) - conhecidos)
    assert not novos, f"relatorios nao classificados na tabela de referencia: {novos}"
