"""Enriquecimento de DataFrames financeiros com dados cadastrais."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

from ifdata_bcb.core.entity_lookup import EntityLookup
from ifdata_bcb.domain.exceptions import InvalidScopeError
from ifdata_bcb.infra.query import QueryEngine

if TYPE_CHECKING:
    from ifdata_bcb.providers.ifdata.cadastro.explorer import CadastroExplorer

VALID_CADASTRO_COLUMNS = {
    "SEGMENTO",
    "COD_CONGL_PRUD",
    "COD_CONGL_FIN",
    "SITUACAO",
    "ATIVIDADE",
    "TCB",
    "TD",
    "TC",
    "UF",
    "MUNICIPIO",
    "SR",
    "DATA_INICIO_ATIVIDADE",
    "NOME_CONGL_PRUD",
}


def validate_cadastro_columns(columns: list[str] | None) -> None:
    """Valida nomes de colunas cadastrais."""
    if columns is None:
        return
    invalid = set(columns) - VALID_CADASTRO_COLUMNS
    if invalid:
        raise InvalidScopeError(
            "cadastro",
            str(sorted(invalid)),
            sorted(VALID_CADASTRO_COLUMNS),
        )


def _subtract_months(year: int, month: int, months: int) -> tuple[int, int]:
    """Subtrai meses de uma data (year, month). Retorna (year, month)."""
    month -= months
    while month <= 0:
        month += 12
        year -= 1
    return year, month


def _derive_nome_congl_prud(
    df_cad: pd.DataFrame,
    cadastro_explorer: CadastroExplorer,
    start_str: str,
    end_str: str,
    query_engine: QueryEngine,
) -> pd.DataFrame:
    """Deriva NOME_CONGL_PRUD: nome da instituicao lider do conglomerado prudencial.

    Para cada COD_CONGL_PRUD, encontra a instituicao onde CNPJ_8 == CNPJ_LIDER_8
    (a lider do conglomerado) e usa seu nome (INSTITUICAO) como NOME_CONGL_PRUD.
    """
    required = {"COD_CONGL_PRUD", "CNPJ_LIDER_8", "INSTITUICAO", "CNPJ_8"}
    if not required.issubset(df_cad.columns):
        df_cad["NOME_CONGL_PRUD"] = None
        return df_cad

    # Buscar lideres ausentes no dataset atual
    lider_cnpjs = set(df_cad["CNPJ_LIDER_8"].dropna().unique())
    cnpjs_presentes = set(df_cad["CNPJ_8"].dropna().unique())
    ausentes = lider_cnpjs - cnpjs_presentes

    if ausentes:
        df_lider = cadastro_explorer.read(
            start_str, end_str, instituicao=list(ausentes)
        )
        df_lookup = pd.concat([df_cad, df_lider], ignore_index=True)
    else:
        df_lookup = df_cad

    sql = """
        SELECT c.*, CAST(l.NOME_CONGL_PRUD AS VARCHAR) AS NOME_CONGL_PRUD
        FROM _cadastro c
        LEFT JOIN (
            SELECT DATA, COD_CONGL_PRUD, INSTITUICAO as NOME_CONGL_PRUD
            FROM _lookup
            WHERE CNPJ_8 = CNPJ_LIDER_8
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY DATA, COD_CONGL_PRUD ORDER BY ROWID
            ) = 1
        ) l USING (DATA, COD_CONGL_PRUD)
    """
    try:
        return query_engine.sql_with_df(sql, _cadastro=df_cad, _lookup=df_lookup)
    except Exception:
        df_cad["NOME_CONGL_PRUD"] = pd.Series([None] * len(df_cad), dtype="string")
        return df_cad


def enrich_with_cadastro(
    df: pd.DataFrame,
    cadastro_columns: list[str],
    query_engine: QueryEngine,
    entity_lookup: EntityLookup,
) -> pd.DataFrame:
    """Enriquece DataFrame financeiro com colunas cadastrais.

    Usa ASOF JOIN no DuckDB para alinhamento temporal backward-looking:
    cada linha financeira recebe os atributos cadastrais do trimestre
    mais recente <= sua data.

    Suporta coluna derivada NOME_CONGL_PRUD: nome da instituicao lider
    do conglomerado prudencial, resolvida internamente via lookup no cadastro.
    """
    if df.empty:
        return df

    from ifdata_bcb.providers.ifdata.cadastro.explorer import CadastroExplorer

    if query_engine is None:
        query_engine = QueryEngine()

    cadastro_explorer = CadastroExplorer(
        query_engine=query_engine, entity_lookup=entity_lookup
    )

    cnpjs = df["CNPJ_8"].unique().tolist()
    min_date = df["DATA"].min()
    max_date = df["DATA"].max()

    # Buscar cadastro com 1 trimestre de margem anterior (aritmetica Python pura)
    min_y, min_m = min_date.year, min_date.month
    start_y, start_m = _subtract_months(min_y, min_m, 3)
    start_str = f"{start_y}-{start_m:02d}"
    end_str = max_date.strftime("%Y-%m")

    df_cad = cadastro_explorer.read(
        start_str,
        end_str,
        instituicao=cnpjs,
    )

    if df_cad.empty:
        for col in cadastro_columns:
            df[col] = pd.Series([None] * len(df), dtype="string")
        return df

    # Derivar NOME_CONGL_PRUD antes de filtrar colunas
    if "NOME_CONGL_PRUD" in cadastro_columns:
        df_cad = _derive_nome_congl_prud(
            df_cad, cadastro_explorer, start_str, end_str, query_engine
        )

    cad_cols = ["CNPJ_8", "DATA"] + cadastro_columns
    df_cad = df_cad[[c for c in cad_cols if c in df_cad.columns]]

    # Colunas de cadastro presentes para o SELECT
    merge_cols = [c for c in cadastro_columns if c in df_cad.columns]
    cad_select = ", ".join(f"c.{col}" for col in merge_cols)
    if not cad_select:
        return df

    # Caso data unica: LEFT JOIN com ROW_NUMBER para pegar registro mais recente
    if df["DATA"].nunique() == 1:
        sql = f"""
            SELECT f.*, {cad_select}
            FROM _financial f
            LEFT JOIN (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY CNPJ_8 ORDER BY DATA DESC
                ) as _rn
                FROM _cadastro
            ) c ON f.CNPJ_8 = c.CNPJ_8 AND c._rn = 1
        """
        return query_engine.sql_with_df(sql, _financial=df, _cadastro=df_cad)

    # Time-series: ASOF LEFT JOIN para alinhamento temporal backward-looking
    sql = f"""
        SELECT f.*, {cad_select}
        FROM _financial f
        ASOF LEFT JOIN _cadastro c
            ON f.CNPJ_8 = c.CNPJ_8
            AND f.DATA >= c.DATA
        ORDER BY f.DATA
    """
    return query_engine.sql_with_df(sql, _financial=df, _cadastro=df_cad).reset_index(
        drop=True
    )
