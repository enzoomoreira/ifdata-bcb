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
        query_engine: QueryEngine | None = None,
        fuzzy_threshold_suggest: int = 78,
    ):
        self._qe = query_engine or QueryEngine()
        self._logger = get_logger(__name__)
        self._fuzzy = FuzzyMatcher(threshold_suggest=fuzzy_threshold_suggest)

    def _get_source_path(self, subdir: str, pattern: str) -> str:
        """Retorna path completo para glob de arquivos."""
        return f"{self._qe.cache_path}/{subdir}/{pattern}"

    def _escape_sql_string(self, value: str) -> str:
        """Escapa aspas simples para SQL."""
        return value.replace("'", "''")

    @cached(maxsize=1)
    def _cadastro_has_codinst(self) -> bool:
        """Indica se o cache de cadastro ja preserva CodInst bruto."""
        cadastro_path = self._get_source_path(
            get_subdir("cadastro"), get_pattern("cadastro")
        )
        try:
            df = self._qe.sql(f"DESCRIBE SELECT * FROM '{cadastro_path}' LIMIT 1")
        except Exception:
            return False
        return "CodInst" in df["column_name"].astype(str).tolist()

    def _legacy_alias_condition(
        self,
        cnpj_col: str = "CNPJ_8",
        cnpj_lider_col: str = "CNPJ_LIDER_8",
        name_col: str = "NomeInstituicao",
    ) -> str:
        """
        Heuristica para caches legados sem CodInst.

        Mantem entidades reais com CNPJ_8, inclusive membros de conglomerado,
        e tenta excluir aliases prudenciais/financeiros mais obvios.
        """
        name_norm = f"strip_accents(UPPER(COALESCE({name_col}, '')))"
        return (
            f"(({cnpj_lider_col} IS NOT NULL AND {cnpj_col} <> {cnpj_lider_col} "
            f"AND {name_norm} LIKE '%PRUDENCIAL%') "
            f"OR {name_norm} = 'MASTER')"
        )

    def real_entity_condition(
        self,
        cnpj_col: str = "CNPJ_8",
        cnpj_lider_col: str = "CNPJ_LIDER_8",
        name_col: str = "NomeInstituicao",
        cod_inst_col: str = "CodInst",
    ) -> str:
        """
        Filtra apenas linhas que representam entidades reais.

        Regra canônica:
        - toda linha com CNPJ_8 e CodInst numerico representa uma entidade
        - aliases prudenciais/financeiros sao identificados pelo CodInst bruto

        Para caches legados sem CodInst, usa heuristica por nome.
        """
        if self._cadastro_has_codinst():
            return (
                f"{cnpj_col} IS NOT NULL AND "
                f"regexp_matches(COALESCE({cod_inst_col}, ''), '^[0-9]+$')"
            )
        return (
            f"{cnpj_col} IS NOT NULL AND "
            f"NOT {self._legacy_alias_condition(cnpj_col, cnpj_lider_col, name_col)}"
        )

    def resolved_entity_cnpj_expr(
        self,
        cnpj_col: str = "CNPJ_8",
        cnpj_lider_col: str = "CNPJ_LIDER_8",
        name_col: str = "NomeInstituicao",
        cod_inst_col: str = "CodInst",
    ) -> str:
        """Resolve aliases prudenciais para o CNPJ da entidade lider."""
        entity_condition = self.real_entity_condition(
            cnpj_col=cnpj_col,
            cnpj_lider_col=cnpj_lider_col,
            name_col=name_col,
            cod_inst_col=cod_inst_col,
        )
        return (
            f"CASE WHEN {entity_condition} THEN {cnpj_col} "
            f"ELSE COALESCE({cnpj_lider_col}, {cnpj_col}) END"
        )

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

        cnpjs_escaped = [self._escape_sql_string(c) for c in cnpjs]
        cnpjs_str = ", ".join(f"'{c}'" for c in cnpjs_escaped)
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
        # 2. Prudencial/Financeiro: a chave de reporte pode ser conglomerado
        #    ou o proprio CNPJ, dependendo do escopo/fonte

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
          AND {self.real_entity_condition()}
          AND (CodConglomeradoPrudencial IS NOT NULL
               OR CodConglomeradoFinanceiro IS NOT NULL)
        """
        try:
            df_congl = self._qe.sql(sql_congl)
            if not df_congl.empty:
                # Coletar todos os codigos de conglomerado
                cod_to_cnpjs: dict[str, list[str]] = {}
                cnpjs_col = df_congl["CNPJ_8"].astype(str).values
                prud_col = df_congl["cod_prud"].values
                fin_col = df_congl["cod_fin"].values
                for cnpj, cod_prud, cod_fin in zip(cnpjs_col, prud_col, fin_col):
                    for cod in (cod_prud, cod_fin):
                        if pd.notna(cod):
                            cod_to_cnpjs.setdefault(str(cod), []).append(cnpj)

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
        cnpjs_escaped = [self._escape_sql_string(c) for c in cnpjs]
        cnpjs_str = ", ".join(f"'{c}'" for c in cnpjs_escaped)

        sql = f"""
        SELECT CNPJ_8, Situacao
        FROM (
            SELECT CNPJ_8, Situacao, ROW_NUMBER() OVER (
                PARTITION BY CNPJ_8 ORDER BY Data DESC
            ) as rn
            FROM '{cadastro_path}'
            WHERE CNPJ_8 IN ({cnpjs_str})
              AND {self.real_entity_condition()}
        )
        WHERE rn = 1
        """

        try:
            df = self._qe.sql(sql)
            return dict(
                zip(
                    df["CNPJ_8"].astype(str).values,
                    df["Situacao"].astype(str).values,
                )
            )
        except Exception:
            return {}

    def search(self, termo: str, limit: int = 10) -> pd.DataFrame:
        """
        Busca entidades por nome com fuzzy matching.

        Retorna DataFrame com CNPJ_8, INSTITUICAO, SITUACAO, FONTES, SCORE.
        Ordenado por ativas primeiro, depois por score.
        FONTES indica onde ha dados disponiveis: 'cosif', 'ifdata'.
        """
        _RESULT_COLUMNS = [
            "CNPJ_8",
            "INSTITUICAO",
            "SITUACAO",
            "FONTES",
            "SCORE",
        ]

        if limit <= 0:
            raise ValueError(f"limit deve ser > 0, recebido: {limit}")

        termo_norm = normalize_accents(termo.strip().upper())

        if not termo_norm:
            return pd.DataFrame(columns=_RESULT_COLUMNS)

        # Entidades reais para exibicao deterministica dos resultados
        cadastro_path = self._get_source_path(
            get_subdir("cadastro"), get_pattern("cadastro")
        )
        sql_entities = f"""
        SELECT CNPJ_8, NOME, strip_accents(UPPER(NOME)) AS NOME_NORM
        FROM (
            SELECT
                CNPJ_8,
                NomeInstituicao AS NOME,
                ROW_NUMBER() OVER (PARTITION BY CNPJ_8 ORDER BY Data DESC) as rn
            FROM '{cadastro_path}'
            WHERE NomeInstituicao IS NOT NULL
              AND {self.real_entity_condition()}
        )
        WHERE rn = 1
        """

        # Aliases do cadastro, incluindo linhas prudenciais/financeiras
        # resolvidas para o CNPJ da entidade lider.
        resolved_cnpj = self.resolved_entity_cnpj_expr()
        sql_aliases = f"""
        SELECT DISTINCT
            {resolved_cnpj} AS CNPJ_8,
            NomeInstituicao AS NOME,
            strip_accents(UPPER(NomeInstituicao)) AS NOME_NORM
        FROM '{cadastro_path}'
        WHERE NomeInstituicao IS NOT NULL
          AND {resolved_cnpj} IS NOT NULL
        """

        empty_df = pd.DataFrame(
            columns=["CNPJ_8", "INSTITUICAO", "SITUACAO", "FONTES", "SCORE"]
        )

        try:
            df_entities = self._qe.sql(sql_entities)
            df_aliases = self._qe.sql(sql_aliases)
        except Exception as e:
            self._logger.warning(f"Search query failed: {e}")
            return empty_df

        if df_entities.empty:
            return empty_df

        # Dados oficiais para exibicao do resultado
        cnpj_data: dict[str, dict] = {}
        cnpjs_arr = df_entities["CNPJ_8"].astype(str).values
        nomes_arr = df_entities["NOME"].astype(str).values
        nomes_norm_arr = df_entities["NOME_NORM"].astype(str).values
        for cnpj, nome, nome_norm in zip(cnpjs_arr, nomes_arr, nomes_norm_arr):
            if cnpj not in cnpj_data:
                cnpj_data[cnpj] = {"nome": nome, "nome_norm": nome_norm}

        # Aliases pesquisaveis -> CNPJ real
        nome_to_cnpj: dict[str, str] = {}
        alias_cnpjs = df_aliases["CNPJ_8"].astype(str).values
        alias_norms = df_aliases["NOME_NORM"].astype(str).values
        for cnpj, nome_norm in zip(alias_cnpjs, alias_norms):
            if cnpj in cnpj_data and nome_norm not in nome_to_cnpj:
                nome_to_cnpj[nome_norm] = cnpj

        # Fuzzy search (sem limit aqui, aplicado apos ordenar por situacao)
        matches = self._fuzzy.search(
            query=termo_norm,
            choices=nome_to_cnpj,
            score_cutoff=self._fuzzy.threshold_suggest,
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
            results.append(
                {
                    "CNPJ_8": cnpj,
                    "INSTITUICAO": data["nome"],
                    "SITUACAO": situacao,
                    "FONTES": ",".join(sorted(fontes)) if fontes else "",
                    "SCORE": score,
                }
            )

        result_df = pd.DataFrame(results)

        # Quando houver entidades com dados no repositorio, prioriza apenas essas
        # para evitar sugestoes nao acionaveis no fluxo search -> read.
        if not result_df.empty and (result_df["FONTES"] != "").any():
            result_df = result_df[result_df["FONTES"] != ""].copy()

        # Ordena: ativas primeiro (A < I), depois por score desc, depois alfabetico
        result_df = result_df.sort_values(
            by=["SITUACAO", "SCORE", "INSTITUICAO"],
            ascending=[True, False, True],
        ).reset_index(drop=True)

        return result_df.head(limit)[
            ["CNPJ_8", "INSTITUICAO", "SITUACAO", "FONTES", "SCORE"]
        ].reset_index(drop=True)

    @cached(maxsize=256)
    def get_entity_identifiers(self, cnpj_8: str) -> dict[str, str | None]:
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
          AND {self.real_entity_condition()}
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
                reason = "Entidade nao pertence a conglomerado prudencial."
                if info.get("nome_entidade") is None:
                    reason += (
                        " Cadastro pode nao estar coletado"
                        " -- execute cadastro.collect() primeiro."
                    )
                raise DataUnavailableError(
                    cnpj_8,
                    "prudencial",
                    reason,
                )
            return ScopeResolution(
                cod_inst=cod_congl,
                tipo_inst=TIPO_INST_MAP["prudencial"],
                cnpj_original=cnpj_8,
                escopo="prudencial",
            )

        # financeiro: a chave de reporte observada pode ser o proprio CNPJ
        # ou um codigo de conglomerado financeiro.
        cod_congl = info.get("cod_congl_fin")
        candidates = [c for c in [cod_congl, cnpj_8] if c]
        ifdata_path = self._get_source_path(
            get_subdir("ifdata_valores"), get_pattern("ifdata_valores")
        )
        candidates_str = ", ".join(
            f"'{self._escape_sql_string(candidate)}'" for candidate in candidates
        )
        sql_fin = f"""
        SELECT DISTINCT CodInst
        FROM '{ifdata_path}'
        WHERE TipoInstituicao = {TIPO_INST_MAP["financeiro"]}
          AND CodInst IN ({candidates_str})
        ORDER BY CASE
            WHEN CodInst = '{self._escape_sql_string(cod_congl or "")}' THEN 0
            WHEN CodInst = '{self._escape_sql_string(cnpj_8)}' THEN 1
            ELSE 2
        END,
        CodInst
        LIMIT 1
        """
        try:
            df_fin = self._qe.sql(sql_fin)
        except Exception:
            df_fin = pd.DataFrame()
        if df_fin.empty:
            reason = "Entidade nao possui dados no escopo financeiro."
            if info.get("nome_entidade") is None:
                reason += (
                    " Cadastro pode nao estar coletado"
                    " -- execute cadastro.collect() primeiro."
                )
            raise DataUnavailableError(
                cnpj_8,
                "financeiro",
                reason,
            )
        return ScopeResolution(
            cod_inst=str(df_fin["CodInst"].iloc[0]),
            tipo_inst=TIPO_INST_MAP["financeiro"],
            cnpj_original=cnpj_8,
            escopo="financeiro",
        )

    def get_canonical_names_for_cnpjs(self, cnpjs: list[str]) -> dict[str, str]:
        """
        Retorna nomes canônicos a partir do cadastro mais recente.

        O cadastro e a fonte mestra para nomes de entidades nas leituras
        analiticas. Se um CNPJ nao existir no cadastro, retorna string vazia.
        """
        if not cnpjs:
            return {}

        cnpjs_escaped = [self._escape_sql_string(c) for c in cnpjs]
        cnpjs_str = ", ".join(f"'{c}'" for c in cnpjs_escaped)
        cadastro_path = self._get_source_path(
            get_subdir("cadastro"), get_pattern("cadastro")
        )

        sql = f"""
        SELECT CNPJ_8, NOME
        FROM (
            SELECT
                CNPJ_8,
                NomeInstituicao AS NOME,
                ROW_NUMBER() OVER (
                    PARTITION BY CNPJ_8
                    ORDER BY Data DESC
                ) as rn
            FROM '{cadastro_path}'
            WHERE NomeInstituicao IS NOT NULL
              AND CNPJ_8 IN ({cnpjs_str})
              AND {self.real_entity_condition()}
        )
        WHERE rn = 1
        """

        try:
            df = self._qe.sql(sql)
        except Exception as e:
            self._logger.warning(f"get_canonical_names_for_cnpjs query failed: {e}")
            return {cnpj: "" for cnpj in cnpjs}

        cnpj_to_name = dict(
            zip(df["CNPJ_8"].astype(str).values, df["NOME"].astype(str).values)
        )
        return {cnpj: cnpj_to_name.get(cnpj, "") for cnpj in cnpjs}

    def clear_cache(self) -> None:
        """Limpa caches LRU."""
        self._cadastro_has_codinst.cache_clear()
        self.get_entity_identifiers.cache_clear()
