import re
from typing import Optional

import pandas as pd

from ifdata_bcb.domain.exceptions import (
    AmbiguousIdentifierError,
    DataUnavailableError,
    EntityNotFoundError,
    InvalidScopeError,
)
from ifdata_bcb.domain.models import ScopeResolution
from ifdata_bcb.infra.cache import cached
from ifdata_bcb.infra.log import get_logger
from ifdata_bcb.infra.query import QueryEngine
from ifdata_bcb.utils.fuzzy import FuzzyMatcher
from ifdata_bcb.utils.text import normalize_accents


# Subdiretorios das fontes de dados
CADASTRO_SUBDIR = "ifdata/cadastro"
COSIF_IND_SUBDIR = "cosif/individual"
COSIF_PRUD_SUBDIR = "cosif/prudencial"

# Patterns dos arquivos
CADASTRO_PATTERN = "ifdata_cad_*.parquet"
COSIF_IND_PATTERN = "cosif_ind_*.parquet"
COSIF_PRUD_PATTERN = "cosif_prud_*.parquet"


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
        cadastro_path = self._get_source_path(CADASTRO_SUBDIR, CADASTRO_PATTERN)
        cosif_ind_path = self._get_source_path(COSIF_IND_SUBDIR, COSIF_IND_PATTERN)
        cosif_prud_path = self._get_source_path(COSIF_PRUD_SUBDIR, COSIF_PRUD_PATTERN)

        where_clause = f"WHERE {where}" if where else ""

        sql = f"""
        SELECT DISTINCT {select_cols}
        FROM (
            SELECT
                CNPJ_8,
                NomeInstituicao AS NOME,
                UPPER(NomeInstituicao) AS NOME_NORM,
                'cadastro' AS FONTE
            FROM '{cadastro_path}'
            WHERE NomeInstituicao IS NOT NULL

            UNION ALL

            SELECT
                CNPJ_8,
                NOME_INSTITUICAO AS NOME,
                UPPER(NOME_INSTITUICAO) AS NOME_NORM,
                'cosif_ind' AS FONTE
            FROM '{cosif_ind_path}'
            WHERE NOME_INSTITUICAO IS NOT NULL

            UNION ALL

            SELECT
                CNPJ_8,
                NOME_INSTITUICAO AS NOME,
                UPPER(NOME_INSTITUICAO) AS NOME_NORM,
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

    def _find_exact_match(self, termo_norm: str) -> Optional[str]:
        """Busca exata por nome (SQL puro). Retorna CNPJ ou None."""
        termo_escaped = self._escape_sql_string(termo_norm)
        sql = self._build_entity_union_sql(
            select_cols="CNPJ_8",
            where=f"NOME_NORM = '{termo_escaped}'",
        )
        sql += " LIMIT 1"

        try:
            df = self._qe.sql(sql)
            if not df.empty:
                return str(df["CNPJ_8"].iloc[0])
        except Exception as e:
            self._logger.debug(f"Exact match query failed: {e}")
        return None

    def _find_contains_matches(
        self, termo_norm: str, limit: int = 10
    ) -> list[tuple[str, str]]:
        """Busca parcial (contains) por nome. Retorna [(cnpj, nome), ...]."""
        termo_escaped = self._escape_sql_string(termo_norm)
        sql = self._build_entity_union_sql(
            select_cols="CNPJ_8, NOME_NORM",
            where=f"NOME_NORM LIKE '%{termo_escaped}%'",
        )
        sql += f" LIMIT {limit}"

        try:
            df = self._qe.sql(sql)
            if not df.empty:
                return list(zip(df["CNPJ_8"].astype(str), df["NOME_NORM"].astype(str)))
        except Exception as e:
            self._logger.debug(f"Contains match query failed: {e}")
        return []

    def _find_fuzzy_matches(
        self, termo_norm: str, limit: int = 5
    ) -> list[tuple[str, int]]:
        """
        Busca fuzzy carregando apenas CNPJ + NOME.
        Retorna [(nome, score), ...] ordenado por score desc.
        """
        sql = self._build_entity_union_sql(select_cols="CNPJ_8, NOME_NORM")

        try:
            df = self._qe.sql(sql)
            if df.empty:
                return []

            # Monta dict {nome_norm: cnpj} para fuzzy
            nome_to_cnpj: dict[str, str] = {}
            for _, row in df.iterrows():
                nome = str(row["NOME_NORM"])
                if nome not in nome_to_cnpj:
                    nome_to_cnpj[nome] = str(row["CNPJ_8"])

            # Fuzzy search
            matches = self._fuzzy.search(
                query=termo_norm,
                choices=nome_to_cnpj,
                limit=limit,
            )
            return matches

        except Exception as e:
            self._logger.debug(f"Fuzzy match query failed: {e}")
        return []

    @cached(maxsize=256)
    def find_cnpj(self, identificador: str) -> str:
        """
        Encontra CNPJ_8 a partir de nome ou CNPJ.

        Busca em ordem: CNPJ direto -> exato -> contains -> fuzzy.
        Se fuzzy score >= threshold_auto, aceita automaticamente.

        Raises:
            EntityNotFoundError: Identificador nao encontrado.
            AmbiguousIdentifierError: Multiplos matches encontrados.
        """
        identificador = identificador.strip()

        # Se ja e CNPJ de 8 digitos, retorna direto
        if re.fullmatch(r"\d{8}", identificador):
            return identificador

        # Normaliza para busca (upper, sem acentos)
        termo_norm = normalize_accents(identificador.upper())

        # 1. Busca exata
        cnpj = self._find_exact_match(termo_norm)
        if cnpj:
            self._logger.debug(f"Exact match: {identificador} -> {cnpj}")
            return cnpj

        # 2. Busca contains
        contains_matches = self._find_contains_matches(termo_norm, limit=10)
        if len(contains_matches) == 1:
            cnpj = contains_matches[0][0]
            self._logger.debug(f"Contains match: {identificador} -> {cnpj}")
            return cnpj
        elif len(contains_matches) > 1:
            nomes = [nome for _, nome in contains_matches[:5]]
            raise AmbiguousIdentifierError(identificador, nomes)

        # 3. Busca fuzzy
        fuzzy_matches = self._find_fuzzy_matches(termo_norm, limit=5)
        if fuzzy_matches:
            best_nome, best_score = fuzzy_matches[0]

            # Auto-aceita se score >= threshold_auto
            if best_score >= self._fuzzy.threshold_auto:
                # Precisa buscar o CNPJ correspondente
                cnpj = self._find_exact_match(best_nome)
                if cnpj:
                    self._logger.debug(
                        f"Fuzzy auto-match ({best_score}%): {identificador} -> {cnpj}"
                    )
                    return cnpj

            # Sugere se score >= threshold_suggest
            if best_score >= self._fuzzy.threshold_suggest:
                suggestions = [f"{nome} ({score}%)" for nome, score in fuzzy_matches[:3]]
                raise AmbiguousIdentifierError(identificador, suggestions)

        raise EntityNotFoundError(identificador)

    def search(self, termo: str, limit: int = 10) -> pd.DataFrame:
        """
        Busca entidades por nome com fuzzy matching.

        Retorna DataFrame com CNPJ_8, INSTITUICAO, FONTES, SCORE.
        """
        termo_norm = normalize_accents(termo.strip().upper())

        # Carrega dados para fuzzy (apenas CNPJ, NOME, FONTE)
        sql = self._build_entity_union_sql(select_cols="CNPJ_8, NOME, NOME_NORM, FONTE")

        try:
            df = self._qe.sql(sql)
        except Exception as e:
            self._logger.warning(f"Search query failed: {e}")
            return pd.DataFrame(columns=["CNPJ_8", "INSTITUICAO", "FONTES", "SCORE"])

        if df.empty:
            return pd.DataFrame(columns=["CNPJ_8", "INSTITUICAO", "FONTES", "SCORE"])

        # Agrupa fontes por CNPJ
        cnpj_data: dict[str, dict] = {}
        for _, row in df.iterrows():
            cnpj = str(row["CNPJ_8"])
            if cnpj not in cnpj_data:
                cnpj_data[cnpj] = {
                    "nome": str(row["NOME"]),
                    "nome_norm": str(row["NOME_NORM"]),
                    "fontes": set(),
                }
            cnpj_data[cnpj]["fontes"].add(row["FONTE"])

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

        # Monta resultado
        results = []
        seen_cnpjs: set[str] = set()
        for nome_norm, score in matches:
            cnpj = nome_to_cnpj[nome_norm]
            if cnpj in seen_cnpjs:
                continue
            seen_cnpjs.add(cnpj)

            data = cnpj_data[cnpj]
            results.append({
                "CNPJ_8": cnpj,
                "INSTITUICAO": data["nome"],
                "FONTES": ",".join(sorted(data["fontes"])),
                "SCORE": score,
            })

        if not results:
            return pd.DataFrame(columns=["CNPJ_8", "INSTITUICAO", "FONTES", "SCORE"])

        return pd.DataFrame(results)[["CNPJ_8", "INSTITUICAO", "FONTES", "SCORE"]]

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

        cadastro_path = self._get_source_path(CADASTRO_SUBDIR, CADASTRO_PATTERN)

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
                tipo_inst=3,
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
                tipo_inst=1,
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
            tipo_inst=2,
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
        self.find_cnpj.cache_clear()
        self.get_entity_identifiers.cache_clear()
