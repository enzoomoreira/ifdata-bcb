"""Enriquecimento de DataFrames financeiros com dados cadastrais."""

import pandas as pd

from ifdata_bcb.core.entity_lookup import EntityLookup
from ifdata_bcb.domain.exceptions import InvalidScopeError
from ifdata_bcb.infra.query import QueryEngine

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


def enrich_with_cadastro(
    df: pd.DataFrame,
    cadastro_columns: list[str],
    query_engine: QueryEngine,
    entity_lookup: EntityLookup,
) -> pd.DataFrame:
    """Enriquece DataFrame financeiro com colunas cadastrais.

    Usa merge temporal backward-looking: cada linha financeira recebe
    os atributos cadastrais do trimestre mais recente <= sua data.
    """
    if df.empty:
        return df

    from ifdata_bcb.providers.ifdata.cadastro_explorer import CadastroExplorer

    cadastro_explorer = CadastroExplorer(
        query_engine=query_engine, entity_lookup=entity_lookup
    )

    cnpjs = df["CNPJ_8"].unique().tolist()
    min_date = df["DATA"].min()
    max_date = df["DATA"].max()

    # Buscar cadastro com 1 trimestre de margem anterior
    start_str = (min_date - pd.DateOffset(months=3)).strftime("%Y-%m")
    end_str = max_date.strftime("%Y-%m")

    df_cad = cadastro_explorer.read(
        instituicao=cnpjs,
        start=start_str,
        end=end_str,
    )

    if df_cad.empty:
        for col in cadastro_columns:
            df[col] = pd.NA
        return df

    cad_cols = ["CNPJ_8", "DATA"] + cadastro_columns
    df_cad = df_cad[[c for c in cad_cols if c in df_cad.columns]]

    # Caso data unica: merge simples por CNPJ_8
    if df["DATA"].nunique() == 1:
        df_cad_latest = df_cad.sort_values("DATA").drop_duplicates(
            subset=["CNPJ_8"], keep="last"
        )
        merge_cols = [c for c in cadastro_columns if c in df_cad_latest.columns]
        return df.merge(
            df_cad_latest[["CNPJ_8"] + merge_cols],
            on="CNPJ_8",
            how="left",
        )

    # Time-series: merge_asof para alinhamento temporal
    df_sorted = df.sort_values("DATA")
    df_cad_sorted = df_cad.sort_values("DATA")

    merge_cols = [c for c in cadastro_columns if c in df_cad_sorted.columns]
    result = pd.merge_asof(
        df_sorted,
        df_cad_sorted[["CNPJ_8", "DATA"] + merge_cols],
        on="DATA",
        by="CNPJ_8",
        direction="backward",
    )

    return result.sort_values("DATA").reset_index(drop=True)
