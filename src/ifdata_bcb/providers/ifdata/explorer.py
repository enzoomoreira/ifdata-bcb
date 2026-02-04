from typing import Literal, Optional

import pandas as pd

from ifdata_bcb.core.base_explorer import BaseExplorer
from ifdata_bcb.core.entity_lookup import EntityLookup
from ifdata_bcb.domain.exceptions import BacenAnalysisError
from ifdata_bcb.domain.models import ScopeResolution
from ifdata_bcb.domain.types import AccountInput, InstitutionInput
from ifdata_bcb.infra.query import QueryEngine
from ifdata_bcb.providers.ifdata.collector import IFDATAValoresCollector


EscopoIFDATA = Literal["individual", "prudencial", "financeiro"]

# Colunas padrao para retorno vazio
_EMPTY_COLUMNS = [
    "DATA",
    "CNPJ_8",
    "INSTITUICAO",
    "ESCOPO",
    "COD_INST",
    "TIPO_INST",
    "COD_CONTA",
    "CONTA",
    "VALOR",
    "RELATORIO",
    "GRUPO",
]


class IFDATAExplorer(BaseExplorer):
    """
    Explorer para dados IFDATA Valores (trimestrais).

    Para dados cadastrais, use CadastroExplorer.
    """

    _COLUMN_MAP = {
        "AnoMes": "DATA",
        "CodInst": "COD_INST",
        "TipoInstituicao": "TIPO_INST",
        "Conta": "COD_CONTA",
        "NomeColuna": "CONTA",
        "Saldo": "VALOR",
        "NomeRelatorio": "RELATORIO",
        "Grupo": "GRUPO",
    }

    _COLUMN_ORDER = [
        "DATA",
        "CNPJ_8",
        "INSTITUICAO",
        "ESCOPO",
        "COD_INST",
        "TIPO_INST",
        "COD_CONTA",
        "CONTA",
        "VALOR",
        "RELATORIO",
        "GRUPO",
    ]

    def __init__(
        self,
        query_engine: Optional[QueryEngine] = None,
        entity_lookup: Optional[EntityLookup] = None,
    ):
        super().__init__(query_engine, entity_lookup)
        self._collector: Optional[IFDATAValoresCollector] = None

    def _get_subdir(self) -> str:
        return "ifdata/valores"

    def _get_file_prefix(self) -> str:
        return "ifdata_val"

    def _get_collector(self) -> IFDATAValoresCollector:
        if self._collector is None:
            self._collector = IFDATAValoresCollector()
        return self._collector

    def _get_pattern(self) -> str:
        return f"{self._get_file_prefix()}_*.parquet"

    def _reorder_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        existing = [c for c in self._COLUMN_ORDER if c in df.columns]
        remaining = [c for c in df.columns if c not in existing]
        return df[existing + remaining]

    def _add_institution_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Adiciona coluna INSTITUICAO a partir de CNPJ_8."""
        if df.empty or "CNPJ_8" not in df.columns:
            return df
        cnpjs = df["CNPJ_8"].unique().tolist()
        nomes = self._resolver.get_names_for_cnpjs(cnpjs)
        df["INSTITUICAO"] = df["CNPJ_8"].map(nomes)
        return df

    def _add_cnpj_mapping(
        self,
        df: pd.DataFrame,
        cnpj_map: dict[str, list[str]],
    ) -> pd.DataFrame:
        """Adiciona CNPJ_8 via merge com mapa cod_inst -> cnpjs."""
        if df.empty:
            return df

        cod_inst_col = self._storage_col("COD_INST")

        if not cnpj_map:
            df["CNPJ_8"] = df[cod_inst_col]
            return df

        rows = [
            {cod_inst_col: cod, "CNPJ_8": cnpj}
            for cod, cnpjs in cnpj_map.items()
            for cnpj in cnpjs
        ]
        df_map = pd.DataFrame(rows)
        return df.merge(df_map, on=cod_inst_col, how="left")

    def _resolve_institutions_with_scope(
        self,
        instituicoes: InstitutionInput,
        escopo: str,
    ) -> list[ScopeResolution]:
        if isinstance(instituicoes, str):
            instituicoes = [instituicoes]
        return [
            self._resolver.resolve_ifdata_scope(self._resolve_entity(i), escopo)
            for i in instituicoes
        ]

    def _read_single_scope(
        self,
        instituicao: InstitutionInput,
        escopo: str,
        start: str,
        end: Optional[str],
        conta: Optional[AccountInput],
        relatorio: Optional[str],
        columns: Optional[list[str]],
    ) -> Optional[pd.DataFrame]:
        """Le dados de um escopo especifico. Retorna None se falhar."""
        try:
            resolutions = self._resolve_institutions_with_scope(instituicao, escopo)
        except BacenAnalysisError:
            return None

        if not resolutions:
            return None

        # Mapa cod_inst -> cnpjs originais
        cnpj_map: dict[str, list[str]] = {}
        for r in resolutions:
            cnpj_map.setdefault(r.cod_inst, []).append(r.cnpj_original)

        tipo_inst = resolutions[0].tipo_inst
        codes = list(set(r.cod_inst for r in resolutions))

        # Construir WHERE
        conditions = [
            self._build_string_condition(self._storage_col("COD_INST"), codes),
            self._build_int_condition(self._storage_col("TIPO_INST"), [tipo_inst]),
            self._build_date_condition(start, end, trimestral=True),
        ]

        if conta:
            contas = self._normalize_accounts(conta)
            if contas:
                conditions.append(
                    self._build_string_condition(
                        self._storage_col("CONTA"), contas, case_insensitive=True
                    )
                )

        if relatorio:
            conditions.append(
                self._build_string_condition(
                    self._storage_col("RELATORIO"), [relatorio], case_insensitive=True
                )
            )

        where = self._join_conditions(conditions)

        df = self._qe.read_glob(
            pattern=self._get_pattern(),
            subdir=self._get_subdir(),
            columns=columns,
            where=where,
        )

        if df.empty:
            return None

        df = df.copy()
        df["ESCOPO"] = escopo
        df = self._add_cnpj_mapping(df, cnpj_map)
        return df

    def collect(
        self, start: str, end: str, force: bool = False, verbose: bool = True
    ) -> None:
        """Coleta dados IFDATA Valores do BCB (trimestral)."""
        self._get_collector().collect(start, end, force=force, verbose=verbose)

    def read(
        self,
        instituicao: InstitutionInput,
        start: str,
        end: Optional[str] = None,
        conta: Optional[AccountInput] = None,
        columns: Optional[list[str]] = None,
        escopo: Optional[EscopoIFDATA] = None,
        relatorio: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Le dados IFDATA Valores com filtros.

        Raises:
            MissingRequiredParameterError: Se instituicao ou start nao fornecidos.
            InvalidDateRangeError: Se start > end.
        """
        self._validate_required_params(instituicao, start)
        self._logger.debug(f"IFDATA read: instituicao={instituicao}, escopo={escopo}")

        escopos = [escopo] if escopo else ["individual", "prudencial", "financeiro"]
        results = []

        for esc in escopos:
            df = self._read_single_scope(
                instituicao, esc, start, end, conta, relatorio, columns
            )
            if df is not None:
                results.append(df)

        if not results:
            return pd.DataFrame(columns=_EMPTY_COLUMNS)

        df = pd.concat(results, ignore_index=True)
        df = self._add_institution_names(df)
        df = self._reorder_columns(df)
        self._logger.debug(f"IFDATA result: {len(df)} rows")
        return self._finalize_read(df)

    def list_accounts(
        self,
        termo: Optional[str] = None,
        escopo: Optional[EscopoIFDATA] = None,
        limit: int = 100,
    ) -> pd.DataFrame:
        """
        Lista contas disponiveis.

        Args:
            termo: Filtro por nome (case-insensitive).
            escopo: Filtro por escopo. Se None, busca em todos.
            limit: Maximo de resultados.
        """
        path = self._qe.cache_path / self._get_subdir() / self._get_pattern()

        conditions = []
        if termo:
            termo_clean = termo.strip().replace("'", "''").upper()
            conditions.append(f"UPPER(NomeColuna) LIKE '%{termo_clean}%'")

        if escopo:
            tipo_inst = {"individual": 3, "prudencial": 1, "financeiro": 2}[escopo]
            conditions.append(f"TipoInstituicao = {tipo_inst}")

        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

        query = f"""
            SELECT DISTINCT Conta as COD_CONTA, NomeColuna as CONTA
            FROM '{path}'
            {where}
            ORDER BY CONTA
            LIMIT {limit}
        """
        return self._qe.sql(query)

    def list_institutions(
        self,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> pd.DataFrame:
        """Lista instituicoes disponiveis."""
        path = self._qe.cache_path / self._get_subdir() / self._get_pattern()

        where = ""
        date_cond = self._build_date_condition(start, end, trimestral=True)
        if date_cond:
            # Precisa usar nome de storage na query SQL direta
            where = f"WHERE {date_cond.replace(self._storage_col('DATA'), 'AnoMes')}"

        query = f"""
            SELECT DISTINCT CodInst as COD_INST, TipoInstituicao as TIPO_INST
            FROM '{path}'
            {where}
            ORDER BY COD_INST
        """

        df = self._qe.sql(query)

        if not df.empty:
            mask = df["TIPO_INST"] == 3
            cnpjs = df.loc[mask, "COD_INST"].tolist()
            if cnpjs:
                nomes = self._resolver.get_names_for_cnpjs(cnpjs)

                def get_nome(row):
                    if row["TIPO_INST"] == 3:
                        return nomes.get(row["COD_INST"], "")
                    return ""

                df["INSTITUICAO"] = df.apply(get_nome, axis=1)
            else:
                df["INSTITUICAO"] = ""

        return df

    def list_reports(
        self,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> list[str]:
        """Lista relatorios disponiveis."""
        path = self._qe.cache_path / self._get_subdir() / self._get_pattern()

        where = ""
        date_cond = self._build_date_condition(start, end, trimestral=True)
        if date_cond:
            where = f"WHERE {date_cond.replace(self._storage_col('DATA'), 'AnoMes')}"

        query = f"""
            SELECT DISTINCT NomeRelatorio as RELATORIO
            FROM '{path}'
            {where}
            ORDER BY RELATORIO
        """

        df = self._qe.sql(query)
        return df["RELATORIO"].tolist() if not df.empty else []
