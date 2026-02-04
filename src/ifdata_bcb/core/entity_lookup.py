from typing import Optional

import pandas as pd

from ifdata_bcb.core.constants import TIPO_INST_MAP, get_pattern, get_subdir
from ifdata_bcb.domain.exceptions import (
    DataUnavailableError,
    InvalidScopeError,
)
from ifdata_bcb.domain.models import ScopeResolution
from ifdata_bcb.infra.cache import cached
from ifdata_bcb.infra.log import get_logger
from ifdata_bcb.infra.query import QueryEngine
from ifdata_bcb.utils.fuzzy import FuzzyMatcher
from ifdata_bcb.utils.text import normalize_accents


class EntityLookup:
    """
    Resolve e busca entidades usando queries DuckDB.

    Toda a logica de busca (exata, parcial, fuzzy) e feita via SQL
    sempre que possivel, carregando dados em memoria apenas quando
    necessario (fuzzy matching).
    """

    def __init__(
        self,
        query_engine: Optional[QueryEngine] = None,
        fuzzy_threshold_auto: int = 85,
        fuzzy_threshold_suggest: int = 70,
    ):
        self._qe = query_engine or QueryEngine()
        self._logger = get_logger(__name__)
        self._fuzzy = FuzzyMatcher(
            threshold_auto=fuzzy_threshold_auto,
            threshold_suggest=fuzzy_threshold_suggest,
        )

    def _get_source_path(self, subdir: str, pattern: str) -> str:
        """Retorna path completo para glob de arquivos."""
        return f"{self._qe.cache_path}/{subdir}/{pattern}"

    def _build_entity_union_sql(
        self,
        select_cols: str = "CNPJ_8, NOME, NOME_NORM, FONTE",
        where: Optional[str] = None,
    ) -> str:
        """
        Gera SQL que une cadastro + cosif_ind + cosif_prud.

        Colunas disponiveis: CNPJ_8, NOME, NOME_NORM (sem acentos, upper), FONTE
        """
        cadastro_path = self._get_source_path(
            get_subdir("cadastro"), get_pattern("cadastro")
        )
        cosif_ind_path = self._get_source_path(
            get_subdir("cosif_individual"), get_pattern("cosif_individual")
        )
        cosif_prud_path = self._get_source_path(
            get_subdir("cosif_prudencial"), get_pattern("cosif_prudencial")
        )

        where_clause = f"WHERE {where}" if where else ""

        sql = f"""
        SELECT DISTINCT {select_cols}
        FROM (
            SELECT
                CNPJ_8,
                NomeInstituicao AS NOME,
                strip_accents(UPPER(NomeInstituicao)) AS NOME_NORM,
                'cadastro' AS FONTE
            FROM '{cadastro_path}'
            WHERE NomeInstituicao IS NOT NULL

            UNION ALL

            SELECT
                CNPJ_8,
                NOME_INSTITUICAO AS NOME,
                strip_accents(UPPER(NOME_INSTITUICAO)) AS NOME_NORM,
                'cosif_ind' AS FONTE
            FROM '{cosif_ind_path}'
            WHERE NOME_INSTITUICAO IS NOT NULL

            UNION ALL

            SELECT
                CNPJ_8,
                NOME_INSTITUICAO AS NOME,
                strip_accents(UPPER(NOME_INSTITUICAO)) AS NOME_NORM,
                'cosif_prud' AS FONTE
            FROM '{cosif_prud_path}'
            WHERE NOME_INSTITUICAO IS NOT NULL
        ) AS entity_union
        {where_clause}
        """
        return sql

    def _escape_sql_string(self, value: str) -> str:
        """Escapa aspas simples para SQL."""
        return value.replace("'", "''")

    def _get_data_sources_for_cnpjs(self, cnpjs: list[str]) -> dict[str, set[str]]:
        """
        Verifica quais fontes de dados estao disponiveis para cada CNPJ.

        Retorna dict {cnpj: {fontes}} onde fontes pode ser 'cosif' e/ou 'ifdata'.
        """
        result: dict[str, set[str]] = {cnpj: set() for cnpj in cnpjs}

        # CNPJs presentes no COSIF
        cosif_ind_path = self._get_source_path(
            get_subdir("cosif_individual"), get_pattern("cosif_individual")
        )
        cosif_prud_path = self._get_source_path(
            get_subdir("cosif_prudencial"), get_pattern("cosif_prudencial")
        )

        cnpjs_str = ", ".join(f"'{c}'" for c in cnpjs)
        sql_cosif = f"""
        SELECT DISTINCT CNPJ_8 FROM (
            SELECT CNPJ_8 FROM '{cosif_ind_path}' WHERE CNPJ_8 IN ({cnpjs_str})
            UNION
            SELECT CNPJ_8 FROM '{cosif_prud_path}' WHERE CNPJ_8 IN ({cnpjs_str})
        )
        """
        try:
            df_cosif = self._qe.sql(sql_cosif)
            for cnpj in df_cosif["CNPJ_8"].astype(str):
                result[cnpj].add("cosif")
        except Exception:
            pass

        # Para IFDATA, precisamos verificar:
        # 1. Individual: CNPJ aparece como CodInst com TipoInstituicao=individual
        # 2. Prudencial/Financeiro: codigo do conglomerado aparece como CodInst

        ifdata_path = self._get_source_path(
            get_subdir("ifdata_valores"), get_pattern("ifdata_valores")
        )

        # Verificar individual (CNPJ direto)
        sql_ifdata_ind = f"""
        SELECT DISTINCT CodInst FROM '{ifdata_path}'
        WHERE TipoInstituicao = {TIPO_INST_MAP["individual"]} AND CodInst IN ({cnpjs_str})
        """
        try:
            df_ifdata = self._qe.sql(sql_ifdata_ind)
            for cnpj in df_ifdata["CodInst"].astype(str):
                result[cnpj].add("ifdata")
        except Exception:
            pass

        # Verificar prudencial/financeiro via codigos de conglomerado
        cadastro_path = self._get_source_path(
            get_subdir("cadastro"), get_pattern("cadastro")
        )
        sql_congl = f"""
        SELECT DISTINCT
            CNPJ_8,
            CodConglomeradoPrudencial as cod_prud,
            CodConglomeradoFinanceiro as cod_fin
        FROM '{cadastro_path}'
        WHERE CNPJ_8 IN ({cnpjs_str})
          AND (CodConglomeradoPrudencial IS NOT NULL
               OR CodConglomeradoFinanceiro IS NOT NULL)
        """
        try:
            df_congl = self._qe.sql(sql_congl)
            if not df_congl.empty:
                # Coletar todos os codigos de conglomerado
                cod_to_cnpjs: dict[str, list[str]] = {}
                for _, row in df_congl.iterrows():
                    cnpj = str(row["CNPJ_8"])
                    for col in ["cod_prud", "cod_fin"]:
                        cod = row[col]
                        if pd.notna(cod):
                            cod_str = str(cod)
                            cod_to_cnpjs.setdefault(cod_str, []).append(cnpj)

                if cod_to_cnpjs:
                    cods_str = ", ".join(f"'{c}'" for c in cod_to_cnpjs.keys())
                    sql_ifdata_congl = f"""
                    SELECT DISTINCT CodInst FROM '{ifdata_path}'
                    WHERE CodInst IN ({cods_str})
                    """
                    df_ifdata_congl = self._qe.sql(sql_ifdata_congl)
                    for cod in df_ifdata_congl["CodInst"].astype(str):
                        for cnpj in cod_to_cnpjs.get(cod, []):
                            result[cnpj].add("ifdata")
        except Exception:
            pass

        return result

    def _get_latest_situacao(self, cnpjs: list[str]) -> dict[str, str]:
        """Retorna situacao mais recente de cada CNPJ (A=Ativa, I=Inativa)."""
        if not cnpjs:
            return {}

        cadastro_path = self._get_source_path(
            get_subdir("cadastro"), get_pattern("cadastro")
        )
        cnpjs_str = ", ".join(f"'{c}'" for c in cnpjs)

        sql = f"""
        SELECT CNPJ_8, Situacao
        FROM (
            SELECT CNPJ_8, Situacao, ROW_NUMBER() OVER (
                PARTITION BY CNPJ_8 ORDER BY Data DESC
            ) as rn
            FROM '{cadastro_path}'
            WHERE CNPJ_8 IN ({cnpjs_str})
        )
        WHERE rn = 1
        """

        try:
            df = self._qe.sql(sql)
            return {str(row["CNPJ_8"]): str(row["Situacao"]) for _, row in df.iterrows()}
        except Exception:
            return {}

    def search(self, termo: str, limit: int = 10) -> pd.DataFrame:
        """
        Busca entidades por nome com fuzzy matching.

        Retorna DataFrame com CNPJ_8, INSTITUICAO, SITUACAO, FONTES, SCORE.
        Ordenado por ativas primeiro, depois por score.
        FONTES indica onde ha dados disponiveis: 'cosif', 'ifdata'.
        """
        termo_norm = normalize_accents(termo.strip().upper())

        # Carrega nomes do cadastro para fuzzy
        cadastro_path = self._get_source_path(
            get_subdir("cadastro"), get_pattern("cadastro")
        )
        sql = f"""
        SELECT DISTINCT
            CNPJ_8,
            NomeInstituicao AS NOME,
            strip_accents(UPPER(NomeInstituicao)) AS NOME_NORM
        FROM '{cadastro_path}'
        WHERE NomeInstituicao IS NOT NULL
        """

        empty_df = pd.DataFrame(
            columns=["CNPJ_8", "INSTITUICAO", "SITUACAO", "FONTES", "SCORE"]
        )

        try:
            df = self._qe.sql(sql)
        except Exception as e:
            self._logger.warning(f"Search query failed: {e}")
            return empty_df

        if df.empty:
            return empty_df

        # Agrupa por CNPJ (pode ter multiplos nomes)
        cnpj_data: dict[str, dict] = {}
        for _, row in df.iterrows():
            cnpj = str(row["CNPJ_8"])
            if cnpj not in cnpj_data:
                cnpj_data[cnpj] = {
                    "nome": str(row["NOME"]),
                    "nome_norm": str(row["NOME_NORM"]),
                }

        # Monta dict para fuzzy: {nome_norm: cnpj}
        nome_to_cnpj: dict[str, str] = {}
        for cnpj, data in cnpj_data.items():
            nome_norm = data["nome_norm"]
            if nome_norm not in nome_to_cnpj:
                nome_to_cnpj[nome_norm] = cnpj

        # Fuzzy search
        matches = self._fuzzy.search(
            query=termo_norm,
            choices=nome_to_cnpj,
            limit=limit,
            score_cutoff=50,
        )

        if not matches:
            return empty_df

        # Coleta CNPJs unicos dos matches
        matched_cnpjs: list[str] = []
        seen_cnpjs: set[str] = set()
        for nome_norm, _ in matches:
            cnpj = nome_to_cnpj[nome_norm]
            if cnpj not in seen_cnpjs:
                seen_cnpjs.add(cnpj)
                matched_cnpjs.append(cnpj)

        # Verifica fontes de dados e situacao
        cnpj_sources = self._get_data_sources_for_cnpjs(matched_cnpjs)
        cnpj_situacao = self._get_latest_situacao(matched_cnpjs)

        # Monta resultado
        results = []
        seen_cnpjs = set()
        for nome_norm, score in matches:
            cnpj = nome_to_cnpj[nome_norm]
            if cnpj in seen_cnpjs:
                continue
            seen_cnpjs.add(cnpj)

            data = cnpj_data[cnpj]
            fontes = cnpj_sources.get(cnpj, set())
            situacao = cnpj_situacao.get(cnpj, "")
            results.append({
                "CNPJ_8": cnpj,
                "INSTITUICAO": data["nome"],
                "SITUACAO": situacao,
                "FONTES": ",".join(sorted(fontes)) if fontes else "",
                "SCORE": score,
            })

        result_df = pd.DataFrame(results)

        # Ordena: ativas primeiro (A < I), depois por score desc
        result_df = result_df.sort_values(
            by=["SITUACAO", "SCORE"],
            ascending=[True, False],
        ).reset_index(drop=True)

        return result_df[["CNPJ_8", "INSTITUICAO", "SITUACAO", "FONTES", "SCORE"]]

    @cached(maxsize=256)
    def get_entity_identifiers(self, cnpj_8: str) -> dict[str, Optional[str]]:
        """
        Retorna identificadores da entidade a partir do cadastro.

        Retorna dict com:
        - cnpj_interesse: CNPJ consultado
        - cnpj_reporte_cosif: CNPJ do lider do conglomerado (ou o proprio)
        - cod_congl_prud: Codigo do conglomerado prudencial
        - cod_congl_fin: Codigo do conglomerado financeiro
        - nome_entidade: Nome da instituicao
        """
        if not cnpj_8:
            return {
                "cnpj_interesse": cnpj_8,
                "cnpj_reporte_cosif": cnpj_8,
                "cod_congl_prud": None,
                "cod_congl_fin": None,
                "nome_entidade": None,
            }

        cadastro_path = self._get_source_path(
            get_subdir("cadastro"), get_pattern("cadastro")
        )

        # Query principal - dados da entidade
        sql = f"""
        SELECT
            NomeInstituicao,
            CodConglomeradoPrudencial,
            CodConglomeradoFinanceiro,
            CNPJ_LIDER_8
        FROM '{cadastro_path}'
        WHERE CNPJ_8 = '{cnpj_8}'
        ORDER BY Data DESC
        LIMIT 1
        """

        try:
            df = self._qe.sql(sql)
        except Exception as e:
            self._logger.debug(f"get_entity_identifiers query failed: {e}")
            df = pd.DataFrame()

        if df.empty:
            return {
                "cnpj_interesse": cnpj_8,
                "cnpj_reporte_cosif": cnpj_8,
                "cod_congl_prud": None,
                "cod_congl_fin": None,
                "nome_entidade": None,
            }

        row = df.iloc[0]
        nome = str(row["NomeInstituicao"]) if pd.notna(row["NomeInstituicao"]) else None
        cod_prud = (
            str(row["CodConglomeradoPrudencial"])
            if pd.notna(row["CodConglomeradoPrudencial"])
            else None
        )
        cod_fin = (
            str(row["CodConglomeradoFinanceiro"])
            if pd.notna(row["CodConglomeradoFinanceiro"])
            else None
        )

        # Determina CNPJ de reporte (lider do conglomerado)
        cnpj_reporte = cnpj_8
        if cod_prud:
            # Busca lider do conglomerado
            sql_lider = f"""
            SELECT CNPJ_LIDER_8
            FROM '{cadastro_path}'
            WHERE CodConglomeradoPrudencial = '{cod_prud}'
              AND CNPJ_LIDER_8 IS NOT NULL
            ORDER BY Data DESC
            LIMIT 1
            """
            try:
                df_lider = self._qe.sql(sql_lider)
                if not df_lider.empty:
                    lider = df_lider["CNPJ_LIDER_8"].iloc[0]
                    if pd.notna(lider):
                        cnpj_reporte = str(lider)
            except Exception:
                pass

        return {
            "cnpj_interesse": cnpj_8,
            "cnpj_reporte_cosif": cnpj_reporte,
            "cod_congl_prud": cod_prud,
            "cod_congl_fin": cod_fin,
            "nome_entidade": nome,
        }

    def resolve_ifdata_scope(self, cnpj_8: str, escopo: str) -> ScopeResolution:
        """
        Resolve CNPJ para codigo IFDATA baseado no escopo.

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

        info = self.get_entity_identifiers(cnpj_8)

        if escopo_lower == "prudencial":
            cod_congl = info.get("cod_congl_prud")
            if not cod_congl:
                raise DataUnavailableError(
                    cnpj_8,
                    "prudencial",
                    "Entidade nao pertence a conglomerado prudencial.",
                )
            return ScopeResolution(
                cod_inst=cod_congl,
                tipo_inst=TIPO_INST_MAP["prudencial"],
                cnpj_original=cnpj_8,
                escopo="prudencial",
            )

        # financeiro
        cod_congl = info.get("cod_congl_fin")
        if not cod_congl:
            raise DataUnavailableError(
                cnpj_8,
                "financeiro",
                "Entidade nao pertence a conglomerado financeiro.",
            )
        return ScopeResolution(
            cod_inst=cod_congl,
            tipo_inst=TIPO_INST_MAP["financeiro"],
            cnpj_original=cnpj_8,
            escopo="financeiro",
        )

    def get_names_for_cnpjs(self, cnpjs: list[str]) -> dict[str, str]:
        """
        Retorna mapeamento {cnpj: nome} para lista de CNPJs.
        CNPJs nao encontrados terao string vazia.
        """
        if not cnpjs:
            return {}

        # Escapa e monta IN clause
        cnpjs_escaped = [self._escape_sql_string(c) for c in cnpjs]
        cnpjs_str = ", ".join(f"'{c}'" for c in cnpjs_escaped)

        sql = self._build_entity_union_sql(
            select_cols="CNPJ_8, NOME",
            where=f"CNPJ_8 IN ({cnpjs_str})",
        )

        try:
            df = self._qe.sql(sql)
        except Exception as e:
            self._logger.warning(f"get_names_for_cnpjs query failed: {e}")
            return {cnpj: "" for cnpj in cnpjs}

        # Monta mapeamento (primeiro nome encontrado para cada CNPJ)
        cnpj_to_name: dict[str, str] = {}
        for _, row in df.iterrows():
            cnpj = str(row["CNPJ_8"])
            if cnpj not in cnpj_to_name:
                cnpj_to_name[cnpj] = str(row["NOME"])

        # Retorna com string vazia para CNPJs nao encontrados
        return {cnpj: cnpj_to_name.get(cnpj, "") for cnpj in cnpjs}

    def clear_cache(self) -> None:
        """Limpa caches LRU."""
        self.get_entity_identifiers.cache_clear()
