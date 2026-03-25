"""Enriquecimento de DataFrames financeiros com dados cadastrais."""

from __future__ import annotations

import pandas as pd

from ifdata_bcb.core.constants import get_pattern, get_subdir
from ifdata_bcb.core.entity import EntityLookup
from ifdata_bcb.domain.exceptions import InvalidScopeError, PartialDataWarning
from ifdata_bcb.infra.log import emit_user_warning, get_logger
from ifdata_bcb.infra.query import QueryEngine
from ifdata_bcb.infra.sql import build_in_clause

logger = get_logger(__name__)

VALID_CADASTRO_COLUMNS = {
    "SEGMENTO",
    "COD_CONGL_PRUD",
    "COD_CONGL_FIN",
    "CNPJ_LIDER_8",
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
    query_engine: QueryEngine,
) -> pd.DataFrame:
    """Deriva NOME_CONGL_PRUD a partir das alias rows do cadastro.

    O BCB armazena os nomes oficiais dos conglomerados em linhas com CodInst
    nao-numerico (ex: C0080714 -> "GOLDMAN SACHS - PRUDENCIAL"). Essas linhas
    sao filtradas pelo real_entity_condition nas queries normais, mas contem
    o nome correto do conglomerado.
    """
    if "COD_CONGL_PRUD" not in df_cad.columns:
        df_cad["NOME_CONGL_PRUD"] = None
        return df_cad

    cod_pruds = [str(c) for c in df_cad["COD_CONGL_PRUD"].dropna().unique()]
    if not cod_pruds:
        df_cad["NOME_CONGL_PRUD"] = None
        return df_cad

    cadastro_path = (
        query_engine.cache_path / get_subdir("cadastro") / get_pattern("cadastro")
    )
    cod_str = build_in_clause(cod_pruds)

    sql = f"""
    SELECT COD_CONGL_PRUD, NOME_CONGL_PRUD
    FROM (
        SELECT CodInst AS COD_CONGL_PRUD,
               NomeInstituicao AS NOME_CONGL_PRUD,
               ROW_NUMBER() OVER (
                   PARTITION BY CodInst ORDER BY Data DESC
               ) AS rn
        FROM read_parquet('{cadastro_path}', union_by_name=true)
        WHERE CodInst IN ({cod_str})
          AND NomeInstituicao IS NOT NULL
    )
    WHERE rn = 1
    """

    try:
        df_names = query_engine.sql(sql)
        if df_names.empty:
            df_cad["NOME_CONGL_PRUD"] = None
            return df_cad

        nome_map = dict(
            zip(
                df_names["COD_CONGL_PRUD"].astype(str).values,
                df_names["NOME_CONGL_PRUD"].astype(str).values,
            )
        )
        df_cad = df_cad.copy()
        df_cad["NOME_CONGL_PRUD"] = df_cad["COD_CONGL_PRUD"].map(nome_map)

        resolved = df_cad["NOME_CONGL_PRUD"].notna().sum()
        logger.debug(
            "enrichment NOME_CONGL_PRUD: %d/%d resolvidos", resolved, len(df_cad)
        )
        return df_cad
    except Exception as e:
        emit_user_warning(
            PartialDataWarning(
                f"Falha ao derivar NOME_CONGL_PRUD: {e}. Coluna preenchida com NULL.",
                reason="enrichment_derivation_failed",
            )
        )
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

    Suporta coluna derivada NOME_CONGL_PRUD: nome oficial do conglomerado
    prudencial, resolvido a partir das alias rows do cadastro.
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
        df_cad = _derive_nome_congl_prud(df_cad, query_engine)

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
