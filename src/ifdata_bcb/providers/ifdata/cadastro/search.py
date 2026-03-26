"""Busca avancada de instituicoes com filtros por fonte e escopo."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

from ifdata_bcb.core.constants import TIPO_INST_MAP
from ifdata_bcb.core.entity.lookup import EntityLookup
from ifdata_bcb.domain.exceptions import InvalidScopeError
from ifdata_bcb.infra.log import get_logger
from ifdata_bcb.infra.sql import build_in_clause
from ifdata_bcb.utils.date import normalize_date_to_int

if TYPE_CHECKING:
    from ifdata_bcb.core.entity.search import EntitySearch
    from ifdata_bcb.infra.query import QueryEngine


class CadastroSearch:
    """
    Busca avancada de instituicoes com filtros por fonte e escopo.

    Camada sobre EntitySearch que adiciona:
    - Filtragem por fonte de dados (cosif/ifdata)
    - Filtragem por escopo (individual/prudencial/financeiro)
    - Listagem de todas as instituicoes com dados disponiveis
    """

    _VALID_FONTES = ("ifdata", "cosif")
    _COSIF_ESCOPOS = ("individual", "prudencial")
    _IFDATA_ESCOPOS = ("individual", "prudencial", "financeiro")

    _SEARCH_COLUMNS = ["CNPJ_8", "INSTITUICAO", "SITUACAO", "FONTES"]
    _SEARCH_COLUMNS_WITH_SCORE = [
        "CNPJ_8",
        "INSTITUICAO",
        "SITUACAO",
        "FONTES",
        "SCORE",
    ]

    def __init__(
        self,
        query_engine: QueryEngine,
        entity_lookup: EntityLookup,
        entity_search: EntitySearch,
    ):
        self._qe = query_engine
        self._lookup = entity_lookup
        self._search = entity_search
        self._logger = get_logger(__name__)

    @staticmethod
    def _resolve_date_range(
        start: str | None,
        end: str | None,
    ) -> tuple[int, int] | None:
        """Converte start/end em tupla (min_yyyymm, max_yyyymm) para filtro."""
        if start is None:
            return None
        s = normalize_date_to_int(start)
        e = normalize_date_to_int(end) if end is not None else s
        return (s, e)

    def search(
        self,
        termo: str | None = None,
        *,
        fonte: str | None = None,
        escopo: str | None = None,
        start: str | None = None,
        end: str | None = None,
        limit: int = 100,
    ) -> pd.DataFrame:
        """Busca instituicoes por nome ou lista todas com dados disponiveis.

        Args:
            termo: Termo de busca (fuzzy matching). Se None, lista todas.
            fonte: Filtra por fonte de dados ("ifdata", "cosif", ou None=todas).
            escopo: Filtra por escopo disponivel na fonte.
            start: Periodo inicial para verificacao de disponibilidade (YYYYMM
                ou YYYY-MM). Retorna apenas instituicoes com dados neste
                intervalo.
            end: Periodo final. Se None com start, filtra periodo unico.
            limit: Maximo de resultados.

        Raises:
            InvalidScopeError: Se fonte ou escopo invalidos.
        """
        if limit <= 0:
            raise ValueError(f"limit deve ser > 0, recebido: {limit}")

        self._validate_search_params(fonte, escopo)
        date_range = self._resolve_date_range(start, end)

        if termo is not None:
            return self._search_with_termo(
                termo,
                fonte=fonte,
                escopo=escopo,
                limit=limit,
                date_range=date_range,
            )
        return self._search_without_termo(
            fonte=fonte,
            escopo=escopo,
            limit=limit,
            date_range=date_range,
        )

    def _validate_search_params(self, fonte: str | None, escopo: str | None) -> None:
        """Valida combinacao fonte/escopo para search()."""
        if fonte is not None:
            fonte_lower = fonte.lower()
            if fonte_lower not in self._VALID_FONTES:
                raise InvalidScopeError("fonte", fonte, list(self._VALID_FONTES))

        if escopo is not None:
            escopo_lower = escopo.lower()
            if fonte is not None and fonte.lower() == "cosif":
                if escopo_lower not in self._COSIF_ESCOPOS:
                    raise InvalidScopeError("escopo", escopo, list(self._COSIF_ESCOPOS))
            else:
                if escopo_lower not in self._IFDATA_ESCOPOS:
                    raise InvalidScopeError(
                        "escopo", escopo, list(self._IFDATA_ESCOPOS)
                    )

    def _search_with_termo(
        self,
        termo: str,
        *,
        fonte: str | None,
        escopo: str | None,
        limit: int,
        date_range: tuple[int, int] | None = None,
    ) -> pd.DataFrame:
        """Busca com fuzzy matching, filtrando por fonte/escopo/periodo."""
        empty = pd.DataFrame(columns=self._SEARCH_COLUMNS_WITH_SCORE)

        fetch_limit = limit * 5 if (fonte or escopo or date_range) else limit
        df = self._search.search(termo, limit=fetch_limit, date_range=date_range)

        if df.empty:
            return empty

        df = self._apply_fonte_filter(df, fonte)
        df = self._apply_escopo_filter(df, fonte, escopo, date_range)

        df = df.head(limit).reset_index(drop=True)
        return df[self._SEARCH_COLUMNS_WITH_SCORE] if not df.empty else empty

    def _search_without_termo(
        self,
        *,
        fonte: str | None,
        escopo: str | None,
        limit: int,
        date_range: tuple[int, int] | None = None,
    ) -> pd.DataFrame:
        """Lista todas as instituicoes com dados, sem fuzzy matching."""
        empty = pd.DataFrame(columns=self._SEARCH_COLUMNS)

        all_entities = self._get_all_entities()
        if all_entities.empty:
            return empty

        cnpjs = all_entities["CNPJ_8"].tolist()
        cnpj_sources = self._lookup._get_data_sources_for_cnpjs(
            cnpjs, date_range=date_range
        )

        rows: list[dict[str, str]] = []
        cnpjs_arr = all_entities["CNPJ_8"].values
        insts_arr = all_entities["INSTITUICAO"].values
        sits_arr = all_entities["SITUACAO"].values
        for cnpj, inst, sit in zip(cnpjs_arr, insts_arr, sits_arr):
            fontes = cnpj_sources.get(cnpj, set())
            if not fontes:
                continue
            rows.append(
                {
                    "CNPJ_8": cnpj,
                    "INSTITUICAO": inst,
                    "SITUACAO": sit,
                    "FONTES": ",".join(sorted(fontes)),
                }
            )

        if not rows:
            return empty

        df = pd.DataFrame(rows)
        df = self._apply_fonte_filter(df, fonte)
        df = self._apply_escopo_filter(df, fonte, escopo, date_range)

        df = (
            df.sort_values(by=["SITUACAO", "INSTITUICAO"], ascending=[True, True])
            .reset_index(drop=True)
            .head(limit)
            .reset_index(drop=True)
        )

        return df[self._SEARCH_COLUMNS] if not df.empty else empty

    def _get_all_entities(self) -> pd.DataFrame:
        """Retorna todas as entidades do cadastro (linha mais recente por CNPJ)."""
        sql = self._lookup._latest_cadastro_sql(
            inner_cols="CNPJ_8, NomeInstituicao AS INSTITUICAO, Situacao AS SITUACAO",
            outer_cols="CNPJ_8, INSTITUICAO, SITUACAO",
            extra_where="NomeInstituicao IS NOT NULL",
        )
        try:
            df = self._qe.sql(sql)
            if not df.empty:
                df["CNPJ_8"] = df["CNPJ_8"].astype(str)
                df["INSTITUICAO"] = df["INSTITUICAO"].astype(str)
                df["SITUACAO"] = df["SITUACAO"].astype(str)
            return df
        except Exception as e:
            self._logger.warning(f"search: failed to query cadastro: {e}")
            return pd.DataFrame(columns=["CNPJ_8", "INSTITUICAO", "SITUACAO"])

    def _apply_fonte_filter(self, df: pd.DataFrame, fonte: str | None) -> pd.DataFrame:
        """Filtra DataFrame de resultados por fonte de dados."""
        if fonte is None or df.empty:
            return df
        fonte_lower = fonte.lower()
        mask = df["FONTES"].str.contains(fonte_lower, na=False)
        return df[mask].copy()

    def _apply_escopo_filter(
        self,
        df: pd.DataFrame,
        fonte: str | None,
        escopo: str | None,
        date_range: tuple[int, int] | None = None,
    ) -> pd.DataFrame:
        """Filtra por escopo verificando disponibilidade real nos dados."""
        if escopo is None or df.empty:
            return df

        escopo_lower = escopo.lower()
        cnpjs = df["CNPJ_8"].tolist()

        if fonte is not None and fonte.lower() == "cosif":
            valid_cnpjs = self._get_cnpjs_with_cosif_escopo(
                cnpjs, escopo_lower, date_range
            )
        else:
            valid_cnpjs = self._get_cnpjs_with_ifdata_escopo(
                cnpjs, escopo_lower, date_range
            )

        return df[df["CNPJ_8"].isin(valid_cnpjs)].copy()

    def _get_cnpjs_with_cosif_escopo(
        self,
        cnpjs: list[str],
        escopo: str,
        date_range: tuple[int, int] | None = None,
    ) -> set[str]:
        """Retorna CNPJs que tem dados no COSIF para o escopo especificado."""
        source_key = f"cosif_{escopo}"
        path = self._lookup._source_path(source_key)
        cnpjs_str = build_in_clause(cnpjs)
        date_cond = EntityLookup._date_filter("DATA_BASE", date_range)
        sql = f"""
        SELECT DISTINCT CNPJ_8 FROM '{path}'
        WHERE CNPJ_8 IN ({cnpjs_str}){date_cond}
        """
        try:
            df = self._qe.sql(sql)
            return set(df["CNPJ_8"].astype(str))
        except Exception as e:
            self._logger.warning(f"search: COSIF escopo check failed ({escopo}): {e}")
            return set()

    def _get_cnpjs_with_ifdata_escopo(
        self,
        cnpjs: list[str],
        escopo: str,
        date_range: tuple[int, int] | None = None,
    ) -> set[str]:
        """Retorna CNPJs que tem dados no IFDATA para o escopo especificado."""
        tipo_inst = TIPO_INST_MAP.get(escopo)
        if tipo_inst is None:
            return set()

        if escopo == "individual":
            return self._get_cnpjs_ifdata_individual(cnpjs, tipo_inst, date_range)

        return self._get_cnpjs_ifdata_conglomerate(cnpjs, escopo, date_range)

    def _get_cnpjs_ifdata_individual(
        self,
        cnpjs: list[str],
        tipo_inst: int,
        date_range: tuple[int, int] | None = None,
    ) -> set[str]:
        """Verifica quais CNPJs tem dados individuais no IFDATA."""
        ifdata_path = self._lookup._source_path("ifdata_valores")
        cnpjs_str = build_in_clause(cnpjs)
        date_cond = EntityLookup._date_filter("AnoMes", date_range)
        sql = f"""
        SELECT DISTINCT CodInst FROM '{ifdata_path}'
        WHERE TipoInstituicao = {tipo_inst}
          AND CodInst IN ({cnpjs_str}){date_cond}
        """
        try:
            df = self._qe.sql(sql)
            return set(df["CodInst"].astype(str))
        except Exception as e:
            self._logger.warning(f"search: IFDATA individual escopo check failed: {e}")
            return set()

    def _get_cnpjs_ifdata_conglomerate(
        self,
        cnpjs: list[str],
        escopo: str,
        date_range: tuple[int, int] | None = None,
    ) -> set[str]:
        """Verifica quais CNPJs tem dados prudenciais/financeiros no IFDATA."""
        cadastro_path = self._lookup._source_path("cadastro")
        ifdata_path = self._lookup._source_path("ifdata_valores")
        cnpjs_str = build_in_clause(cnpjs)

        cod_col = (
            "CodConglomeradoPrudencial"
            if escopo == "prudencial"
            else "CodConglomeradoFinanceiro"
        )

        sql = f"""
        SELECT DISTINCT CNPJ_8, {cod_col} AS cod_congl
        FROM read_parquet('{cadastro_path}', union_by_name=true)
        WHERE CNPJ_8 IN ({cnpjs_str})
          AND {self._lookup.real_entity_condition()}
          AND {cod_col} IS NOT NULL
        """
        try:
            df_congl = self._qe.sql(sql)
            if df_congl.empty:
                return set()

            cod_to_cnpjs: dict[str, list[str]] = {}
            for cnpj, cod in zip(
                df_congl["CNPJ_8"].astype(str).values,
                df_congl["cod_congl"].astype(str).values,
            ):
                cod_to_cnpjs.setdefault(cod, []).append(cnpj)

            cods_str = build_in_clause(list(cod_to_cnpjs.keys()))
            date_cond = EntityLookup._date_filter("AnoMes", date_range)
            sql_ifdata = f"""
            SELECT DISTINCT CodInst FROM '{ifdata_path}'
            WHERE CodInst IN ({cods_str}){date_cond}
            """
            df_ifdata = self._qe.sql(sql_ifdata)

            result: set[str] = set()
            for cod in df_ifdata["CodInst"].astype(str):
                result.update(cod_to_cnpjs.get(cod, []))
            return result
        except Exception as e:
            self._logger.warning(
                f"search: IFDATA conglomerate escopo check failed ({escopo}): {e}"
            )
            return set()
