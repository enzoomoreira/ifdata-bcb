"""Resolucao de entidades e metadados via queries DuckDB."""

import pandas as pd

from ifdata_bcb.core.constants import TIPO_INST_MAP, get_pattern, get_subdir
from ifdata_bcb.infra.cache import cached
from ifdata_bcb.infra.log import get_logger
from ifdata_bcb.infra.query import QueryEngine
from ifdata_bcb.infra.sql import build_in_clause, build_string_condition


class EntityLookup:
    """
    Resolve entidades e consulta metadados via queries DuckDB.

    Responsabilidades:
    - Verificacao de disponibilidade por fonte (cosif/ifdata)
    - Resolucao de identificadores (CNPJ, conglomerado, lider)
    - Nomes canonicos a partir do cadastro
    """

    def __init__(
        self,
        query_engine: QueryEngine | None = None,
    ):
        self._qe = query_engine or QueryEngine()
        self._logger = get_logger(__name__)
        self._name_cache: dict[str, str] = {}

    @property
    def query_engine(self) -> QueryEngine:
        """QueryEngine usada para consultas."""
        return self._qe

    def _source_path(self, source_key: str) -> str:
        """Retorna path completo para glob de arquivos de uma fonte."""
        return (
            f"{self._qe.cache_path}/{get_subdir(source_key)}/{get_pattern(source_key)}"
        )

    @staticmethod
    def real_entity_condition(
        cnpj_col: str = "CNPJ_8",
        cod_inst_col: str = "CodInst",
    ) -> str:
        """Filtra apenas linhas que representam entidades reais.

        Regra canonica: toda linha com CNPJ_8 e CodInst numerico
        representa uma entidade. Aliases prudenciais/financeiros
        sao identificados pelo CodInst nao-numerico.
        """
        return (
            f"{cnpj_col} IS NOT NULL AND "
            f"regexp_matches(COALESCE({cod_inst_col}, ''), '^[0-9]+$')"
        )

    def resolved_entity_cnpj_expr(
        self,
        cnpj_col: str = "CNPJ_8",
        cnpj_lider_col: str = "CNPJ_LIDER_8",
        cod_inst_col: str = "CodInst",
    ) -> str:
        """Resolve aliases prudenciais para o CNPJ da entidade lider."""
        entity_condition = self.real_entity_condition(
            cnpj_col=cnpj_col,
            cod_inst_col=cod_inst_col,
        )
        return (
            f"CASE WHEN {entity_condition} THEN {cnpj_col} "
            f"ELSE COALESCE({cnpj_lider_col}, {cnpj_col}) END"
        )

    def _latest_cadastro_sql(
        self,
        inner_cols: str,
        outer_cols: str,
        extra_where: str = "",
    ) -> str:
        """SQL para linha mais recente por CNPJ do cadastro (ROW_NUMBER)."""
        cadastro = self._source_path("cadastro")
        where = self.real_entity_condition()
        if extra_where:
            where += f" AND {extra_where}"
        return f"""
        SELECT {outer_cols}
        FROM (
            SELECT {inner_cols},
                ROW_NUMBER() OVER (PARTITION BY CNPJ_8 ORDER BY Data DESC) as rn
            FROM read_parquet('{cadastro}', union_by_name=true)
            WHERE {where}
        )
        WHERE rn = 1
        """

    def _get_data_sources_for_cnpjs(self, cnpjs: list[str]) -> dict[str, set[str]]:
        """
        Verifica quais fontes de dados estao disponiveis para cada CNPJ.

        Retorna dict {cnpj: {fontes}} onde fontes pode ser 'cosif' e/ou 'ifdata'.
        """
        result: dict[str, set[str]] = {cnpj: set() for cnpj in cnpjs}
        cnpjs_str = build_in_clause(cnpjs)
        self._check_cosif_sources(cnpjs_str, result)
        self._check_ifdata_individual_sources(cnpjs_str, result)
        self._check_ifdata_conglomerate_sources(cnpjs, cnpjs_str, result)
        return result

    def _check_cosif_sources(self, cnpjs_str: str, result: dict[str, set[str]]) -> None:
        cosif_ind_path = self._source_path("cosif_individual")
        cosif_prud_path = self._source_path("cosif_prudencial")
        sql = f"""
        SELECT DISTINCT CNPJ_8 FROM (
            SELECT CNPJ_8 FROM '{cosif_ind_path}' WHERE CNPJ_8 IN ({cnpjs_str})
            UNION
            SELECT CNPJ_8 FROM '{cosif_prud_path}' WHERE CNPJ_8 IN ({cnpjs_str})
        )
        """
        try:
            df = self._qe.sql(sql)
            for cnpj in df["CNPJ_8"].astype(str):
                result[cnpj].add("cosif")
        except Exception as e:
            self._logger.warning(f"Data source check failed (cosif): {e}")

    def _check_ifdata_individual_sources(
        self, cnpjs_str: str, result: dict[str, set[str]]
    ) -> None:
        ifdata_path = self._source_path("ifdata_valores")
        sql = f"""
        SELECT DISTINCT CodInst FROM '{ifdata_path}'
        WHERE TipoInstituicao = {TIPO_INST_MAP["individual"]} AND CodInst IN ({cnpjs_str})
        """
        try:
            df = self._qe.sql(sql)
            for cnpj in df["CodInst"].astype(str):
                result[cnpj].add("ifdata")
        except Exception as e:
            self._logger.warning(f"Data source check failed (ifdata): {e}")

    def _check_ifdata_conglomerate_sources(
        self,
        cnpjs: list[str],
        cnpjs_str: str,
        result: dict[str, set[str]],
    ) -> None:
        cadastro_path = self._source_path("cadastro")
        ifdata_path = self._source_path("ifdata_valores")
        sql = f"""
        SELECT DISTINCT
            CNPJ_8,
            CodConglomeradoPrudencial as cod_prud,
            CodConglomeradoFinanceiro as cod_fin
        FROM read_parquet('{cadastro_path}', union_by_name=true)
        WHERE CNPJ_8 IN ({cnpjs_str})
          AND {self.real_entity_condition()}
          AND (CodConglomeradoPrudencial IS NOT NULL
               OR CodConglomeradoFinanceiro IS NOT NULL)
        """
        try:
            df_congl = self._qe.sql(sql)
            if not df_congl.empty:
                cod_to_cnpjs: dict[str, list[str]] = {}
                cnpjs_col = df_congl["CNPJ_8"].astype(str).values
                prud_col = df_congl["cod_prud"].values
                fin_col = df_congl["cod_fin"].values
                for cnpj, cod_prud, cod_fin in zip(cnpjs_col, prud_col, fin_col):
                    for cod in (cod_prud, cod_fin):
                        if pd.notna(cod):
                            cod_to_cnpjs.setdefault(str(cod), []).append(cnpj)

                if cod_to_cnpjs:
                    cods_str = build_in_clause(list(cod_to_cnpjs.keys()))
                    sql_ifdata = f"""
                    SELECT DISTINCT CodInst FROM '{ifdata_path}'
                    WHERE CodInst IN ({cods_str})
                    """
                    df_ifdata = self._qe.sql(sql_ifdata)
                    for cod in df_ifdata["CodInst"].astype(str):
                        for cnpj in cod_to_cnpjs.get(cod, []):
                            result[cnpj].add("ifdata")
        except Exception as e:
            self._logger.warning(f"Data source check failed (conglomerate): {e}")

    def _get_latest_situacao(self, cnpjs: list[str]) -> dict[str, str]:
        """Retorna situacao mais recente de cada CNPJ (A=Ativa, I=Inativa)."""
        if not cnpjs:
            return {}

        cnpjs_str = build_in_clause(cnpjs)
        sql = self._latest_cadastro_sql(
            inner_cols="CNPJ_8, Situacao",
            outer_cols="CNPJ_8, Situacao",
            extra_where=f"CNPJ_8 IN ({cnpjs_str})",
        )

        try:
            df = self._qe.sql(sql)
            return dict(
                zip(
                    df["CNPJ_8"].astype(str).values,
                    df["Situacao"].astype(str).values,
                )
            )
        except Exception as e:
            self._logger.warning(f"Latest situacao query failed: {e}")
            return {}

    @cached(maxsize=256)
    def get_entity_identifiers(self, cnpj_8: str) -> dict[str, str | None]:
        """
        Retorna identificadores da entidade a partir do cadastro.

        Usa o valor mais recente nao-nulo de cada campo, o que protege contra
        periodos em que o BCB deixou de popular determinados campos.
        """
        if not cnpj_8:
            return {
                "cnpj_interesse": cnpj_8,
                "cnpj_reporte_cosif": cnpj_8,
                "cod_congl_prud": None,
                "cod_congl_fin": None,
                "nome_entidade": None,
            }

        cadastro_path = self._source_path("cadastro")
        cnpj_condition = build_string_condition("CNPJ_8", [cnpj_8])

        sql = f"""
        SELECT
            FIRST(NomeInstituicao ORDER BY Data DESC)
                as NomeInstituicao,
            FIRST(CodConglomeradoPrudencial ORDER BY Data DESC)
                FILTER (WHERE CodConglomeradoPrudencial IS NOT NULL)
                as CodConglomeradoPrudencial,
            FIRST(CodConglomeradoFinanceiro ORDER BY Data DESC)
                FILTER (WHERE CodConglomeradoFinanceiro IS NOT NULL)
                as CodConglomeradoFinanceiro,
            FIRST(CNPJ_LIDER_8 ORDER BY Data DESC)
                FILTER (WHERE CNPJ_LIDER_8 IS NOT NULL)
                as CNPJ_LIDER_8
        FROM read_parquet('{cadastro_path}', union_by_name=true)
        WHERE {cnpj_condition}
          AND {self.real_entity_condition()}
        """

        try:
            df = self._qe.sql(sql)
        except Exception as e:
            self._logger.warning(
                f"get_entity_identifiers query failed for {cnpj_8}: {e}"
            )
            df = pd.DataFrame()

        if df.empty or pd.isna(df.iloc[0]["NomeInstituicao"]):
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

        cnpj_reporte = cnpj_8
        if cod_prud:
            prud_condition = build_string_condition(
                "CodConglomeradoPrudencial", [cod_prud]
            )
            sql_lider = f"""
            SELECT CNPJ_LIDER_8
            FROM read_parquet('{cadastro_path}', union_by_name=true)
            WHERE {prud_condition}
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
            except Exception as e:
                self._logger.warning(
                    f"CNPJ_LIDER lookup failed for cod_prud={cod_prud}, "
                    f"using cnpj_8={cnpj_8} as cnpj_reporte_cosif: {e}"
                )

        return {
            "cnpj_interesse": cnpj_8,
            "cnpj_reporte_cosif": cnpj_reporte,
            "cod_congl_prud": cod_prud,
            "cod_congl_fin": cod_fin,
            "nome_entidade": nome,
        }

    def get_canonical_names_for_cnpjs(self, cnpjs: list[str]) -> dict[str, str]:
        """
        Retorna nomes canonicos a partir do cadastro mais recente.

        O cadastro e a fonte mestra para nomes de entidades nas leituras
        analiticas. Se um CNPJ nao existir no cadastro, retorna string vazia.
        Resultados sao cacheados por sessao para evitar queries repetidas.
        """
        if not cnpjs:
            return {}

        missing = [c for c in cnpjs if c not in self._name_cache]

        if missing:
            cnpjs_str = build_in_clause(missing)
            sql = self._latest_cadastro_sql(
                inner_cols="CNPJ_8, NomeInstituicao AS NOME",
                outer_cols="CNPJ_8, NOME",
                extra_where=f"NomeInstituicao IS NOT NULL AND CNPJ_8 IN ({cnpjs_str})",
            )

            try:
                df = self._qe.sql(sql)
            except Exception as e:
                self._logger.warning(f"get_canonical_names_for_cnpjs query failed: {e}")
                for cnpj in missing:
                    self._name_cache[cnpj] = ""
                return {cnpj: self._name_cache.get(cnpj, "") for cnpj in cnpjs}

            fetched = dict(
                zip(df["CNPJ_8"].astype(str).values, df["NOME"].astype(str).values)
            )
            for cnpj in missing:
                self._name_cache[cnpj] = fetched.get(cnpj, "")

        return {cnpj: self._name_cache.get(cnpj, "") for cnpj in cnpjs}

    def clear_cache(self) -> None:
        """Limpa caches LRU e cache de nomes."""
        self.get_entity_identifiers.cache_clear()
        self._name_cache.clear()
