"""Explorer para dados cadastrais IFDATA (trimestrais)."""

from __future__ import annotations

import pandas as pd

from ifdata_bcb.core.constants import DATA_SOURCES, TIPO_INST_MAP, get_subdir
from ifdata_bcb.core.entity_lookup import EntityLookup
from ifdata_bcb.domain.exceptions import InvalidScopeError
from ifdata_bcb.domain.types import InstitutionInput
from ifdata_bcb.infra.query import QueryEngine
from ifdata_bcb.infra.sql import (
    build_in_clause,
    build_string_condition,
    join_conditions,
)
from ifdata_bcb.providers.base_explorer import BaseExplorer
from ifdata_bcb.providers.ifdata.cadastro.collector import IFDATACadastroCollector


class CadastroExplorer(BaseExplorer):
    """Explorer para dados cadastrais IFDATA (trimestrais)."""

    _DROP_COLUMNS = ["CodInst"]
    _PASSTHROUGH_COLUMNS: set[str] = {"CNPJ_8", "CNPJ_LIDER_8"}

    _DATE_COLUMN = "Data"

    _COLUMN_MAP = {
        "Data": "DATA",
        "NomeInstituicao": "INSTITUICAO",
        "SegmentoTb": "SEGMENTO",
        "CodConglomeradoPrudencial": "COD_CONGL_PRUD",
        "CodConglomeradoFinanceiro": "COD_CONGL_FIN",
        "Situacao": "SITUACAO",
        "Atividade": "ATIVIDADE",
        "Tcb": "TCB",
        "Td": "TD",
        "Tc": "TC",
        "Uf": "UF",
        "Municipio": "MUNICIPIO",
        "Sr": "SR",
        "DataInicioAtividade": "DATA_INICIO_ATIVIDADE",
    }

    _COLUMN_ORDER = [
        "DATA",
        "CNPJ_8",
        "INSTITUICAO",
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
    ]

    _LIST_COLUMNS: dict[str, str] = {
        "DATA": "Data",
        "SEGMENTO": "SegmentoTb",
        "UF": "Uf",
        "SITUACAO": "Situacao",
        "ATIVIDADE": "Atividade",
        "TCB": "Tcb",
        "TD": "Td",
        "TC": "Tc",
        "SR": "Sr",
        "MUNICIPIO": "Municipio",
    }

    _BLOCKED_COLUMNS: dict[str, str] = {
        "CNPJ_8": "Use cadastro.search() para buscar instituicoes.",
        "INSTITUICAO": "Use cadastro.search() para buscar instituicoes.",
    }

    def __init__(
        self,
        query_engine: QueryEngine | None = None,
        entity_lookup: EntityLookup | None = None,
    ):
        super().__init__(query_engine, entity_lookup)
        self._collector: IFDATACadastroCollector | None = None

    def _get_subdir(self) -> str:
        return get_subdir("cadastro")

    def _get_file_prefix(self) -> str:
        return DATA_SOURCES["cadastro"]["prefix"]

    def _get_collector(self) -> IFDATACadastroCollector:
        if self._collector is None:
            self._collector = IFDATACadastroCollector()
        return self._collector

    def _build_real_entidade_condition(self) -> str:
        return self._resolver.real_entity_condition()

    def collect(
        self, start: str, end: str, force: bool = False, verbose: bool = True
    ) -> None:
        """Coleta dados cadastrais IFDATA do BCB (trimestral)."""
        self._get_collector().collect(start, end, force=force, verbose=verbose)

    def read(
        self,
        start: str,
        end: str | None = None,
        *,
        instituicao: InstitutionInput | None = None,
        segmento: str | None = None,
        uf: str | None = None,
        situacao: str | None = None,
        atividade: str | None = None,
        tcb: str | None = None,
        td: str | None = None,
        tc: str | int | None = None,
        sr: str | None = None,
        municipio: str | None = None,
        columns: list[str] | None = None,
    ) -> pd.DataFrame:
        """
        Le dados cadastrais com filtros.

        Args:
            start: Periodo inicial (obrigatorio). Formato: '2024-12' ou '202412'.
            end: Periodo final. Se None, retorna apenas start.
            instituicao: CNPJ de 8 digitos. Se None, retorna todas.
            segmento: Filtro por segmento (case/accent insensitive).
            uf: Filtro por UF (case/accent insensitive).
            situacao: Filtro por situacao (case/accent insensitive).
            atividade: Filtro por atividade (case/accent insensitive).
            tcb: Filtro por TCB (case/accent insensitive).
            td: Filtro por TD (case/accent insensitive).
            tc: Filtro por TC (aceita str ou int).
            sr: Filtro por SR (case/accent insensitive).
            municipio: Filtro por municipio (case/accent insensitive).
            columns: Colunas a retornar. Se None, retorna todas.

        Raises:
            MissingRequiredParameterError: Se start nao fornecido.
            InvalidDateRangeError: Se start > end.
        """
        self._validate_required_params(start)
        columns = self._validate_columns(columns)
        self._logger.debug(
            f"Cadastro read: instituicao={instituicao}, segmento={segmento}"
        )

        conditions = [
            self._build_cnpj_condition(instituicao),
            self._build_date_condition(start, end, trimestral=True),
            self._build_real_entidade_condition(),
        ]

        filter_params: dict[str, str | int | None] = {
            "SEGMENTO": segmento,
            "UF": uf,
            "SITUACAO": situacao,
            "ATIVIDADE": atividade,
            "TCB": tcb,
            "TD": td,
            "TC": tc,
            "SR": sr,
            "MUNICIPIO": municipio,
        }

        for col_name, value in filter_params.items():
            if value is not None:
                conditions.append(
                    build_string_condition(
                        self._storage_col(col_name),
                        [str(value)],
                        case_insensitive=True,
                        accent_insensitive=True,
                    )
                )

        df = self._read_glob(
            pattern=self._get_pattern(),
            subdir=self._get_subdir(),
            columns=self._translate_columns(columns),
            where=join_conditions(conditions),
        )
        return self._finalize_read(df)

    def list(
        self,
        columns: list[str],
        *,
        start: str | None = None,
        end: str | None = None,
        segmento: str | None = None,
        uf: str | None = None,
        situacao: str | None = None,
        atividade: str | None = None,
        tcb: str | None = None,
        td: str | None = None,
        tc: str | int | None = None,
        sr: str | None = None,
        municipio: str | None = None,
        limit: int = 100,
    ) -> pd.DataFrame:
        """Lista valores distintos para as colunas solicitadas.

        Args:
            columns: Colunas a listar (DATA, SEGMENTO, UF, SITUACAO, ATIVIDADE,
                     TCB, TD, TC, SR, MUNICIPIO).
            start: Periodo inicial (opcional).
            end: Periodo final (opcional).
            segmento: Filtro por segmento (case/accent insensitive).
            uf: Filtro por UF (case/accent insensitive).
            situacao: Filtro por situacao (case/accent insensitive).
            atividade: Filtro por atividade (case/accent insensitive).
            tcb: Filtro por TCB (case/accent insensitive).
            td: Filtro por TD (case/accent insensitive).
            tc: Filtro por TC (aceita str ou int).
            sr: Filtro por SR (case/accent insensitive).
            municipio: Filtro por municipio (case/accent insensitive).
            limit: Maximo de resultados.

        Raises:
            InvalidColumnError: Se coluna invalida.
        """
        return self._base_list(
            columns,
            start=start,
            end=end,
            limit=limit,
            segmento=segmento,
            uf=uf,
            situacao=situacao,
            atividade=atividade,
            tcb=tcb,
            td=td,
            tc=tc,
            sr=sr,
            municipio=municipio,
        )

    def _build_list_conditions(
        self,
        start: str | None = None,
        end: str | None = None,
        **filters: object,
    ) -> list[str | None]:
        conditions: list[str | None] = []

        # Date filter (trimestral)
        conditions.append(self._build_date_condition(start, end, trimestral=True))

        # Real entity filter (exclude alias rows)
        conditions.append(self._build_real_entidade_condition())

        # Categorical filters -- same pattern as read()
        filter_map: dict[str, str] = {
            "segmento": "SegmentoTb",
            "uf": "Uf",
            "situacao": "Situacao",
            "atividade": "Atividade",
            "tcb": "Tcb",
            "td": "Td",
            "tc": "Tc",
            "sr": "Sr",
            "municipio": "Municipio",
        }

        for param_name, storage_col in filter_map.items():
            value = filters.get(param_name)
            if value is not None:
                conditions.append(
                    build_string_condition(
                        storage_col,
                        [str(value)],
                        case_insensitive=True,
                        accent_insensitive=True,
                    )
                )

        return conditions

    # ------------------------------------------------------------------
    # search()
    # ------------------------------------------------------------------

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
            start: Periodo inicial para verificacao de disponibilidade.
            end: Periodo final para verificacao de disponibilidade.
            limit: Maximo de resultados.

        Raises:
            InvalidScopeError: Se fonte ou escopo invalidos.
        """
        if limit <= 0:
            raise ValueError(f"limit deve ser > 0, recebido: {limit}")

        self._validate_search_params(fonte, escopo)

        if termo is not None:
            return self._search_with_termo(
                termo, fonte=fonte, escopo=escopo, limit=limit
            )
        return self._search_without_termo(fonte=fonte, escopo=escopo, limit=limit)

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
                # fonte=None or fonte="ifdata": use IFDATA escopos
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
    ) -> pd.DataFrame:
        """Busca com fuzzy matching, filtrando por fonte/escopo."""
        empty = pd.DataFrame(columns=self._SEARCH_COLUMNS_WITH_SCORE)

        # Fetch more than needed so filtering doesn't exhaust results
        fetch_limit = limit * 5 if (fonte or escopo) else limit
        df = self._resolver.search(termo, limit=fetch_limit)

        if df.empty:
            return empty

        df = self._apply_fonte_filter(df, fonte)
        df = self._apply_escopo_filter(df, fonte, escopo)

        df = df.head(limit).reset_index(drop=True)
        return df[self._SEARCH_COLUMNS_WITH_SCORE] if not df.empty else empty

    def _search_without_termo(
        self,
        *,
        fonte: str | None,
        escopo: str | None,
        limit: int,
    ) -> pd.DataFrame:
        """Lista todas as instituicoes com dados, sem fuzzy matching."""
        empty = pd.DataFrame(columns=self._SEARCH_COLUMNS)

        all_entities = self._get_all_entities()
        if all_entities.empty:
            return empty

        cnpjs = all_entities["CNPJ_8"].tolist()
        cnpj_sources = self._resolver._get_data_sources_for_cnpjs(cnpjs)

        rows: list[dict[str, str]] = []
        for _, row in all_entities.iterrows():
            cnpj = row["CNPJ_8"]
            fontes = cnpj_sources.get(cnpj, set())
            if not fontes:
                continue
            rows.append(
                {
                    "CNPJ_8": cnpj,
                    "INSTITUICAO": row["INSTITUICAO"],
                    "SITUACAO": row["SITUACAO"],
                    "FONTES": ",".join(sorted(fontes)),
                }
            )

        if not rows:
            return empty

        df = pd.DataFrame(rows)
        df = self._apply_fonte_filter(df, fonte)
        df = self._apply_escopo_filter(df, fonte, escopo)

        df = (
            df.sort_values(by=["SITUACAO", "INSTITUICAO"], ascending=[True, True])
            .reset_index(drop=True)
            .head(limit)
            .reset_index(drop=True)
        )

        return df[self._SEARCH_COLUMNS] if not df.empty else empty

    def _get_all_entities(self) -> pd.DataFrame:
        """Retorna todas as entidades do cadastro (linha mais recente por CNPJ)."""
        sql = self._resolver._latest_cadastro_sql(
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
    ) -> pd.DataFrame:
        """Filtra por escopo verificando disponibilidade real nos dados."""
        if escopo is None or df.empty:
            return df

        escopo_lower = escopo.lower()
        cnpjs = df["CNPJ_8"].tolist()

        if fonte is not None and fonte.lower() == "cosif":
            valid_cnpjs = self._get_cnpjs_with_cosif_escopo(cnpjs, escopo_lower)
        else:
            valid_cnpjs = self._get_cnpjs_with_ifdata_escopo(cnpjs, escopo_lower)

        return df[df["CNPJ_8"].isin(valid_cnpjs)].copy()

    def _get_cnpjs_with_cosif_escopo(self, cnpjs: list[str], escopo: str) -> set[str]:
        """Retorna CNPJs que tem dados no COSIF para o escopo especificado."""
        source_key = f"cosif_{escopo}"
        path = self._resolver._source_path(source_key)
        cnpjs_str = build_in_clause(cnpjs)
        sql = f"""
        SELECT DISTINCT CNPJ_8 FROM '{path}'
        WHERE CNPJ_8 IN ({cnpjs_str})
        """
        try:
            df = self._qe.sql(sql)
            return set(df["CNPJ_8"].astype(str))
        except Exception as e:
            self._logger.warning(f"search: COSIF escopo check failed ({escopo}): {e}")
            return set()

    def _get_cnpjs_with_ifdata_escopo(self, cnpjs: list[str], escopo: str) -> set[str]:
        """Retorna CNPJs que tem dados no IFDATA para o escopo especificado."""
        tipo_inst = TIPO_INST_MAP.get(escopo)
        if tipo_inst is None:
            return set()

        if escopo == "individual":
            return self._get_cnpjs_ifdata_individual(cnpjs, tipo_inst)

        return self._get_cnpjs_ifdata_conglomerate(cnpjs, escopo)

    def _get_cnpjs_ifdata_individual(
        self, cnpjs: list[str], tipo_inst: int
    ) -> set[str]:
        """Verifica quais CNPJs tem dados individuais no IFDATA."""
        ifdata_path = self._resolver._source_path("ifdata_valores")
        cnpjs_str = build_in_clause(cnpjs)
        sql = f"""
        SELECT DISTINCT CodInst FROM '{ifdata_path}'
        WHERE TipoInstituicao = {tipo_inst}
          AND CodInst IN ({cnpjs_str})
        """
        try:
            df = self._qe.sql(sql)
            return set(df["CodInst"].astype(str))
        except Exception as e:
            self._logger.warning(f"search: IFDATA individual escopo check failed: {e}")
            return set()

    def _get_cnpjs_ifdata_conglomerate(self, cnpjs: list[str], escopo: str) -> set[str]:
        """Verifica quais CNPJs tem dados prudenciais/financeiros no IFDATA."""
        cadastro_path = self._resolver._source_path("cadastro")
        ifdata_path = self._resolver._source_path("ifdata_valores")
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
          AND {self._resolver.real_entity_condition()}
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
            sql_ifdata = f"""
            SELECT DISTINCT CodInst FROM '{ifdata_path}'
            WHERE CodInst IN ({cods_str})
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
