"""Busca fuzzy de entidades por nome."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

from ifdata_bcb.infra.log import get_logger
from ifdata_bcb.utils.fuzzy import FuzzyMatcher
from ifdata_bcb.utils.text import normalize_accents

if TYPE_CHECKING:
    from ifdata_bcb.core.entity.lookup import EntityLookup


class EntitySearch:
    """
    Busca entidades por nome com fuzzy matching.

    Responsabilidades:
    - Construcao do corpus de busca (entidades + aliases)
    - Fuzzy matching via FuzzyMatcher
    - Match exato por CNPJ de 8 digitos
    - Montagem de resultados com dedup, fontes e situacao
    """

    _SEARCH_RESULT_COLUMNS: list[str] = [
        "CNPJ_8",
        "INSTITUICAO",
        "SITUACAO",
        "FONTES",
        "SCORE",
    ]

    def __init__(
        self,
        lookup: EntityLookup,
        fuzzy_threshold_suggest: int = 78,
    ):
        self._lookup = lookup
        self._qe = lookup.query_engine
        self._logger = get_logger(__name__)
        self._fuzzy = FuzzyMatcher(threshold_suggest=fuzzy_threshold_suggest)

    def search(
        self,
        termo: str,
        limit: int = 10,
        date_range: tuple[int, int] | None = None,
    ) -> pd.DataFrame:
        """
        Busca entidades por nome com fuzzy matching.

        Args:
            termo: Termo de busca.
            limit: Maximo de resultados.
            date_range: Tupla (min_yyyymm, max_yyyymm) para restringir
                a verificacao de disponibilidade de dados. Se None,
                verifica todos os periodos.

        Retorna DataFrame com CNPJ_8, INSTITUICAO, SITUACAO, FONTES, SCORE.
        Ordenado por ativas primeiro, depois por score.
        FONTES indica onde ha dados disponiveis: 'cosif', 'ifdata'.
        """
        if limit <= 0:
            raise ValueError(f"limit deve ser > 0, recebido: {limit}")

        termo_norm = normalize_accents(termo.strip().upper())
        empty = pd.DataFrame(columns=self._SEARCH_RESULT_COLUMNS)

        if not termo_norm:
            return empty

        corpus = self._build_search_corpus()
        if corpus is None:
            return empty
        cnpj_data, nome_to_cnpj = corpus

        exact = self._search_exact_cnpj(termo_norm, cnpj_data, date_range)
        if exact is not None:
            return exact

        matches = self._fuzzy.search(
            query=termo_norm,
            choices=nome_to_cnpj,
            score_cutoff=self._fuzzy.threshold_suggest,
        )
        if not matches:
            return empty

        return self._assemble_search_results(
            matches, nome_to_cnpj, cnpj_data, limit, date_range
        )

    def _build_search_corpus(
        self,
    ) -> tuple[dict[str, dict], dict[str, str]] | None:
        """Constroi corpus de busca: cnpj_data e nome_to_cnpj.

        Retorna None se query falhar ou cadastro vazio.
        """
        sql_entities = self._lookup._latest_cadastro_sql(
            inner_cols="CNPJ_8, NomeInstituicao AS NOME",
            outer_cols="CNPJ_8, NOME, strip_accents(UPPER(NOME)) AS NOME_NORM",
            extra_where="NomeInstituicao IS NOT NULL",
        )

        cadastro_path = self._lookup._source_path("cadastro")
        resolved_cnpj = self._lookup.resolved_entity_cnpj_expr()
        sql_aliases = f"""
        SELECT DISTINCT
            {resolved_cnpj} AS CNPJ_8,
            NomeInstituicao AS NOME,
            strip_accents(UPPER(NomeInstituicao)) AS NOME_NORM
        FROM read_parquet('{cadastro_path}', union_by_name=true)
        WHERE NomeInstituicao IS NOT NULL
          AND {resolved_cnpj} IS NOT NULL
        """

        try:
            df_entities = self._qe.sql(sql_entities)
            df_aliases = self._qe.sql(sql_aliases)
        except Exception as e:
            self._logger.warning(f"Search query failed: {e}")
            return None

        if df_entities.empty:
            return None

        cnpj_data: dict[str, dict] = {}
        cnpjs_arr = df_entities["CNPJ_8"].astype(str).values
        nomes_arr = df_entities["NOME"].astype(str).values
        nomes_norm_arr = df_entities["NOME_NORM"].astype(str).values
        for cnpj, nome, nome_norm in zip(cnpjs_arr, nomes_arr, nomes_norm_arr):
            if cnpj not in cnpj_data:
                cnpj_data[cnpj] = {"nome": nome, "nome_norm": nome_norm}

        nome_to_cnpj: dict[str, str] = {}
        alias_cnpjs = df_aliases["CNPJ_8"].astype(str).values
        alias_norms = df_aliases["NOME_NORM"].astype(str).values
        for cnpj, nome_norm in zip(alias_cnpjs, alias_norms):
            if cnpj in cnpj_data and nome_norm not in nome_to_cnpj:
                nome_to_cnpj[nome_norm] = cnpj

        return cnpj_data, nome_to_cnpj

    def _search_exact_cnpj(
        self,
        termo_norm: str,
        cnpj_data: dict[str, dict],
        date_range: tuple[int, int] | None = None,
    ) -> pd.DataFrame | None:
        """Match exato por CNPJ de 8 digitos. Retorna None se nao aplicavel."""
        if len(termo_norm) != 8 or not termo_norm.isdigit():
            return None
        if termo_norm not in cnpj_data:
            return None

        cnpj_sources = self._lookup._get_data_sources_for_cnpjs(
            [termo_norm], date_range=date_range
        )
        fontes = cnpj_sources.get(termo_norm, set())
        if not fontes:
            return pd.DataFrame(columns=self._SEARCH_RESULT_COLUMNS)

        cnpj_situacao = self._lookup._get_latest_situacao([termo_norm])
        data = cnpj_data[termo_norm]
        return pd.DataFrame(
            [
                {
                    "CNPJ_8": termo_norm,
                    "INSTITUICAO": data["nome"],
                    "SITUACAO": cnpj_situacao.get(termo_norm, ""),
                    "FONTES": ",".join(sorted(fontes)),
                    "SCORE": 100,
                }
            ]
        )

    def _assemble_search_results(
        self,
        matches: list[tuple[str, int]],
        nome_to_cnpj: dict[str, str],
        cnpj_data: dict[str, dict],
        limit: int,
        date_range: tuple[int, int] | None = None,
    ) -> pd.DataFrame:
        """Dedup, enriquecimento (sources/situacao), filtragem e sort."""
        cnpj_scores: dict[str, int] = {}
        for nome_norm, score in matches:
            cnpj = nome_to_cnpj[nome_norm]
            if cnpj not in cnpj_scores:
                cnpj_scores[cnpj] = score

        cnpj_list = list(cnpj_scores)
        cnpj_sources = self._lookup._get_data_sources_for_cnpjs(
            cnpj_list, date_range=date_range
        )
        cnpj_situacao = self._lookup._get_latest_situacao(cnpj_list)

        results = []
        for cnpj, score in cnpj_scores.items():
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

        if date_range is not None:
            result_df = result_df[result_df["FONTES"] != ""].copy()
        elif not result_df.empty and (result_df["FONTES"] != "").any():
            result_df = result_df[result_df["FONTES"] != ""].copy()

        result_df = result_df.sort_values(
            by=["SITUACAO", "SCORE", "INSTITUICAO"],
            ascending=[True, False, True],
        ).reset_index(drop=True)

        return result_df.head(limit)[self._SEARCH_RESULT_COLUMNS].reset_index(drop=True)
