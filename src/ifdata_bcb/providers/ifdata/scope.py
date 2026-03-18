"""Resolucao de escopo IFDATA a partir de CNPJ."""

import pandas as pd

from ifdata_bcb.core.constants import TIPO_INST_MAP, get_pattern, get_subdir
from ifdata_bcb.core.entity_lookup import EntityLookup
from ifdata_bcb.domain.exceptions import DataUnavailableError, InvalidScopeError
from ifdata_bcb.domain.models import ScopeResolution
from ifdata_bcb.infra.sql import build_string_condition


def resolve_ifdata_escopo(
    entity_lookup: EntityLookup,
    cnpj_8: str,
    escopo: str,
) -> ScopeResolution:
    """Resolve CNPJ para codigo IFDATA baseado no escopo.

    Raises:
        DataUnavailableError: Entidade nao tem dados para o escopo.
        InvalidScopeError: Escopo invalido.
    """
    escopo_lower = escopo.lower()
    valid_scopes = ["individual", "prudencial", "financeiro"]

    if escopo_lower not in valid_scopes:
        raise InvalidScopeError("escopo", escopo, valid_scopes)

    if escopo_lower == "individual":
        return ScopeResolution(
            cod_inst=cnpj_8,
            tipo_inst=TIPO_INST_MAP["individual"],
            cnpj_original=cnpj_8,
            escopo="individual",
        )

    info = entity_lookup.get_entity_identifiers(cnpj_8)

    if escopo_lower == "prudencial":
        cod_congl = info.get("cod_congl_prud")
        if not cod_congl:
            reason = "Entidade nao pertence a conglomerado prudencial."
            if info.get("nome_entidade") is None:
                reason += (
                    " Cadastro pode nao estar coletado"
                    " -- execute cadastro.collect() primeiro."
                )
            raise DataUnavailableError(cnpj_8, "prudencial", reason)
        return ScopeResolution(
            cod_inst=cod_congl,
            tipo_inst=TIPO_INST_MAP["prudencial"],
            cnpj_original=cnpj_8,
            escopo="prudencial",
        )

    # financeiro: tenta cod_congl primeiro, depois cnpj_8
    qe = entity_lookup.query_engine
    cod_congl = info.get("cod_congl_fin")
    candidates = [c for c in [cod_congl, cnpj_8] if c]
    ifdata_path = f"{qe.cache_path}/{get_subdir('ifdata_valores')}/{get_pattern('ifdata_valores')}"
    tipo_fin = TIPO_INST_MAP["financeiro"]
    candidates_cond = build_string_condition("CodInst", candidates)
    sql_fin = f"""
    SELECT DISTINCT CodInst
    FROM '{ifdata_path}'
    WHERE TipoInstituicao = {tipo_fin}
      AND {candidates_cond}
    """
    try:
        df_fin = qe.sql(sql_fin)
    except Exception as e:
        from ifdata_bcb.infra.log import get_logger

        get_logger(__name__).warning(f"Financeiro scope query failed for {cnpj_8}: {e}")
        df_fin = pd.DataFrame()
    # Priorizar cod_congl sobre cnpj_8
    if not df_fin.empty and cod_congl:
        congl_match = df_fin[df_fin["CodInst"].astype(str) == cod_congl]
        if not congl_match.empty:
            df_fin = congl_match
    if df_fin.empty:
        reason = "Entidade nao possui dados no escopo financeiro."
        if info.get("nome_entidade") is None:
            reason += (
                " Cadastro pode nao estar coletado"
                " -- execute cadastro.collect() primeiro."
            )
        raise DataUnavailableError(cnpj_8, "financeiro", reason)
    return ScopeResolution(
        cod_inst=str(df_fin["CodInst"].iloc[0]),
        tipo_inst=TIPO_INST_MAP["financeiro"],
        cnpj_original=cnpj_8,
        escopo="financeiro",
    )
