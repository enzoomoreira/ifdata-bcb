from typing import Literal, Optional

import pandas as pd

from ifdata_bcb.core.base_explorer import BaseExplorer
from ifdata_bcb.core.constants import (
    DATA_SOURCES,
    TIPO_INST_MAP,
    get_pattern,
    get_subdir,
)
from ifdata_bcb.core.entity_lookup import EntityLookup
from ifdata_bcb.domain.exceptions import DataUnavailableError, InvalidScopeError
from ifdata_bcb.utils.text import normalize_accents
from ifdata_bcb.domain.models import ScopeResolution
from ifdata_bcb.domain.types import AccountInput, InstitutionInput
from ifdata_bcb.infra.query import QueryEngine
from ifdata_bcb.providers.ifdata.collector import IFDATAValoresCollector


EscopoIFDATA = Literal["individual", "prudencial", "financeiro"]
_TIPO_INST_REVERSE = {v: k for k, v in TIPO_INST_MAP.items()}

# Colunas padrao para retorno vazio
_EMPTY_COLUMNS = [
    "DATA",
    "CNPJ_8",
    "INSTITUICAO",
    "ESCOPO",
    "COD_INST",
    "CONTA",
    "VALOR",
    "RELATORIO",
    "GRUPO",
]
_EMPTY_ACCOUNT_COLUMNS = ["COD_CONTA", "CONTA"]
_EMPTY_INSTITUTION_COLUMNS = [
    "CNPJ_8",
    "INSTITUICAO",
    "TEM_INDIVIDUAL",
    "TEM_PRUDENCIAL",
    "TEM_FINANCEIRO",
    "COD_INST_INDIVIDUAL",
    "COD_INST_PRUDENCIAL",
    "COD_INST_FINANCEIRO",
]
_EMPTY_REPORTER_COLUMNS = [
    "COD_INST",
    "TIPO_INST",
    "ESCOPO",
    "REPORT_KEY_TYPE",
    "CNPJ_8",
    "INSTITUICAO",
]


class IFDATAExplorer(BaseExplorer):
    """
    Explorer para dados IFDATA Valores (trimestrais).

    Para dados cadastrais, use CadastroExplorer.
    """

    _COLUMN_MAP = {
        "AnoMes": "DATA",
        "CodInst": "COD_INST",
        "NomeColuna": "CONTA",
        "Saldo": "VALOR",
        "NomeRelatorio": "RELATORIO",
        "Grupo": "GRUPO",
    }

    # Colunas a remover do resultado final
    _DROP_COLUMNS = ["TipoInstituicao", "Conta"]

    _COLUMN_ORDER = [
        "DATA",
        "CNPJ_8",
        "INSTITUICAO",
        "ESCOPO",
        "COD_INST",
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
        return get_subdir("ifdata_valores")

    def _get_file_prefix(self) -> str:
        return DATA_SOURCES["ifdata_valores"]["prefix"]

    def _get_collector(self) -> IFDATAValoresCollector:
        if self._collector is None:
            self._collector = IFDATAValoresCollector()
        return self._collector

    def _get_pattern(self) -> str:
        return f"{self._get_file_prefix()}_*.parquet"

    def _validate_escopo(self, escopo: str) -> EscopoIFDATA:
        escopo_lower = escopo.lower()
        valid_scopes = list(TIPO_INST_MAP.keys())
        if escopo_lower not in TIPO_INST_MAP:
            raise InvalidScopeError("escopo", escopo, valid_scopes)
        return escopo_lower  # type: ignore[return-value]

    def _reorder_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        existing = [c for c in self._COLUMN_ORDER if c in df.columns]
        remaining = [c for c in df.columns if c not in existing]
        return df[existing + remaining]

    def _finalize_read(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove colunas internas, aplica mapeamento e reordena."""
        # Remove colunas internas antes do mapeamento
        drop_cols = [c for c in self._DROP_COLUMNS if c in df.columns]
        if drop_cols:
            df = df.drop(columns=drop_cols)
        # Aplica processamento padrao (rename, conversao de DATA)
        df = super()._finalize_read(df)
        # Reordena colunas apos rename
        return self._reorder_columns(df)

    def _add_institution_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Adiciona coluna INSTITUICAO a partir do cadastro canônico."""
        if df.empty or "CNPJ_8" not in df.columns:
            return df
        cnpjs = df["CNPJ_8"].unique().tolist()
        nomes = self._resolver.get_canonical_names_for_cnpjs(cnpjs)
        df["INSTITUICAO"] = df["CNPJ_8"].map(nomes)
        return df

    def _build_ifdata_date_where(
        self,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> str:
        date_cond = self._build_date_condition(start, end, trimestral=True)
        if not date_cond:
            return ""
        return f"WHERE {date_cond.replace(self._storage_col('DATA'), 'AnoMes')}"

    def _load_reporter_rows(
        self,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> pd.DataFrame:
        if not self._qe.has_glob(self._get_pattern(), self._get_subdir()):
            return pd.DataFrame(columns=["COD_INST", "TIPO_INST", "ESCOPO"])

        path = self._qe.cache_path / self._get_subdir() / self._get_pattern()
        where = self._build_ifdata_date_where(start, end)
        query = f"""
            SELECT DISTINCT CodInst as COD_INST, TipoInstituicao as TIPO_INST
            FROM '{path}'
            {where}
            ORDER BY COD_INST, TIPO_INST
        """
        df = self._qe.sql(query)
        if df.empty:
            return df
        df["ESCOPO"] = df["TIPO_INST"].map(_TIPO_INST_REVERSE)
        return df

    def _load_cadastro_entities(
        self,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> pd.DataFrame:
        cadastro_pattern = get_pattern("cadastro")
        cadastro_subdir = get_subdir("cadastro")
        if not self._qe.has_glob(cadastro_pattern, cadastro_subdir):
            return pd.DataFrame(
                columns=["CNPJ_8", "INSTITUICAO", "COD_CONGL_PRUD", "COD_CONGL_FIN"]
            )

        path = self._qe.cache_path / cadastro_subdir / cadastro_pattern
        where_parts = [self._resolver.real_entity_condition()]
        date_cond = self._build_date_condition(start, end, trimestral=True)
        if date_cond:
            where_parts.append(date_cond.replace(self._storage_col("DATA"), "Data"))
        where = " AND ".join(where_parts)
        query = f"""
            SELECT
                CNPJ_8,
                NomeInstituicao as INSTITUICAO,
                CodConglomeradoPrudencial as COD_CONGL_PRUD,
                CodConglomeradoFinanceiro as COD_CONGL_FIN
            FROM (
                SELECT
                    CNPJ_8,
                    NomeInstituicao,
                    CodConglomeradoPrudencial,
                    CodConglomeradoFinanceiro,
                    ROW_NUMBER() OVER (
                        PARTITION BY CNPJ_8
                        ORDER BY Data DESC
                    ) as rn
                FROM '{path}'
                WHERE {where}
            )
            WHERE rn = 1
        """
        return self._qe.sql(query)

    def _resolve_reporter_mappings(
        self,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> pd.DataFrame:
        reporters = self._load_reporter_rows(start, end)
        if reporters.empty:
            return pd.DataFrame(columns=_EMPTY_REPORTER_COLUMNS)

        cadastro = self._load_cadastro_entities(start, end)
        frames: list[pd.DataFrame] = []

        individual = reporters[
            reporters["TIPO_INST"] == TIPO_INST_MAP["individual"]
        ].copy()
        if not individual.empty:
            individual["CNPJ_8"] = individual["COD_INST"]
            individual["REPORT_KEY_TYPE"] = "cnpj"
            frames.append(individual)

        for escopo, tipo_inst, cod_col in [
            ("prudencial", TIPO_INST_MAP["prudencial"], "COD_CONGL_PRUD"),
            ("financeiro", TIPO_INST_MAP["financeiro"], "COD_CONGL_FIN"),
        ]:
            subset = reporters[reporters["TIPO_INST"] == tipo_inst].copy()
            if subset.empty:
                continue

            lookup = pd.concat(
                [
                    cadastro[["CNPJ_8"]].assign(COD_INST=cadastro["CNPJ_8"]),
                    cadastro[["CNPJ_8", cod_col]]
                    .rename(columns={cod_col: "COD_INST"})
                    .dropna(subset=["COD_INST"]),
                ],
                ignore_index=True,
            ).drop_duplicates()

            merged = subset.merge(lookup, on="COD_INST", how="left")
            merged = merged.dropna(subset=["CNPJ_8"])
            if merged.empty:
                continue
            merged["REPORT_KEY_TYPE"] = (
                merged["COD_INST"]
                .astype(str)
                .eq(merged["CNPJ_8"].astype(str))
                .map({True: "cnpj", False: escopo})
            )
            frames.append(merged)

        if not frames:
            return pd.DataFrame(columns=_EMPTY_REPORTER_COLUMNS)

        df = pd.concat(frames, ignore_index=True)
        df = self._add_institution_names(df)
        return (
            df[
                [
                    "COD_INST",
                    "TIPO_INST",
                    "ESCOPO",
                    "REPORT_KEY_TYPE",
                    "CNPJ_8",
                    "INSTITUICAO",
                ]
            ]
            .drop_duplicates()
            .sort_values(["COD_INST", "TIPO_INST", "CNPJ_8"])
            .reset_index(drop=True)
        )

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
        except DataUnavailableError:
            return None

        if not resolutions:
            return None

        # Mapa cod_inst -> cnpjs originais
        cnpj_map: dict[str, list[str]] = {}
        for r in resolutions:
            cnpj_map.setdefault(r.cod_inst, []).append(r.cnpj_original)

        tipo_inst = resolutions[0].tipo_inst
        codes = list(set(r.cod_inst for r in resolutions))

        # Construir WHERE (nomes de storage)
        conditions = [
            self._build_string_condition("CodInst", codes),
            self._build_int_condition("TipoInstituicao", [tipo_inst]),
            self._build_date_condition(start, end, trimestral=True),
        ]

        if conta:
            contas = self._normalize_accounts(conta)
            if contas:
                conditions.append(
                    self._build_string_condition(
                        self._storage_col("CONTA"),
                        contas,
                        case_insensitive=True,
                        accent_insensitive=True,
                    )
                )

        if relatorio:
            conditions.append(
                self._build_string_condition(
                    self._storage_col("RELATORIO"),
                    [relatorio],
                    case_insensitive=True,
                    accent_insensitive=True,
                )
            )

        where = self._join_conditions(conditions)

        # CodInst e TipoInstituicao sao necessarios internamente
        # para merge de CNPJ e drop posterior - garantir inclusao
        storage_columns = self._translate_columns(columns)
        if storage_columns is not None:
            for required in ["CodInst", "TipoInstituicao"]:
                if required not in storage_columns:
                    storage_columns.append(required)

        df = self._qe.read_glob(
            pattern=self._get_pattern(),
            subdir=self._get_subdir(),
            columns=storage_columns,
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
        cadastro: Optional[list[str]] = None,
    ) -> pd.DataFrame:
        """
        Le dados IFDATA Valores com filtros.

        Args:
            cadastro: Colunas cadastrais para enriquecer o resultado
                (ex: ["TCB", "SEGMENTO"]). Se None, nao enriquece.

        Raises:
            MissingRequiredParameterError: Se instituicao ou start nao fornecidos.
            InvalidDateRangeError: Se start > end.
        """
        self._validate_required_params(instituicao, start)
        self._logger.debug(f"IFDATA read: instituicao={instituicao}, escopo={escopo}")

        escopos = (
            [self._validate_escopo(escopo)]
            if escopo
            else ["individual", "prudencial", "financeiro"]
        )
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
        self._logger.debug(f"IFDATA result: {len(df)} rows")
        df = self._finalize_read(df)

        if cadastro is not None:
            df = self._enrich_with_cadastro(df, cadastro)

        return df

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
        if not self._qe.has_glob(self._get_pattern(), self._get_subdir()):
            return pd.DataFrame(columns=_EMPTY_ACCOUNT_COLUMNS)

        path = self._qe.cache_path / self._get_subdir() / self._get_pattern()

        conditions = []
        if termo:
            termo_clean = normalize_accents(termo.strip().replace("'", "''")).upper()
            conditions.append(
                f"strip_accents(UPPER(NomeColuna)) LIKE '%{termo_clean}%'"
            )

        if escopo:
            tipo_inst = TIPO_INST_MAP[self._validate_escopo(escopo)]
            conditions.append(f"TipoInstituicao = {tipo_inst}")

        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

        query = f"""
            SELECT DISTINCT Conta as COD_CONTA, NomeColuna as CONTA
            FROM '{path}'
            {where}
            ORDER BY CONTA, COD_CONTA
            LIMIT {limit}
        """
        return self._qe.sql(query)

    def list_institutions(
        self,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> pd.DataFrame:
        """Lista entidades analíticas com disponibilidade por escopo."""
        df = self._resolve_reporter_mappings(start, end)
        if df.empty:
            return pd.DataFrame(columns=_EMPTY_INSTITUTION_COLUMNS)

        def join_codes(series: pd.Series) -> str:
            values = sorted({str(v) for v in series.dropna().astype(str) if str(v)})
            return ", ".join(values)

        rows = []
        for cnpj, group in df.groupby("CNPJ_8", sort=True):
            rows.append(
                {
                    "CNPJ_8": cnpj,
                    "INSTITUICAO": group["INSTITUICAO"].dropna().astype(str).iloc[0]
                    if not group["INSTITUICAO"].dropna().empty
                    else "",
                    "TEM_INDIVIDUAL": bool((group["ESCOPO"] == "individual").any()),
                    "TEM_PRUDENCIAL": bool((group["ESCOPO"] == "prudencial").any()),
                    "TEM_FINANCEIRO": bool((group["ESCOPO"] == "financeiro").any()),
                    "COD_INST_INDIVIDUAL": join_codes(
                        group.loc[group["ESCOPO"] == "individual", "COD_INST"]
                    ),
                    "COD_INST_PRUDENCIAL": join_codes(
                        group.loc[group["ESCOPO"] == "prudencial", "COD_INST"]
                    ),
                    "COD_INST_FINANCEIRO": join_codes(
                        group.loc[group["ESCOPO"] == "financeiro", "COD_INST"]
                    ),
                }
            )

        return (
            pd.DataFrame(rows)
            .sort_values(["INSTITUICAO", "CNPJ_8"])
            .reset_index(drop=True)
        )

    def list_reporters(
        self,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> pd.DataFrame:
        """Lista chaves operacionais de reporte do IFDATA por entidade e escopo."""
        return self._resolve_reporter_mappings(start, end)

    def list_reports(
        self,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> list[str]:
        """Lista relatorios disponiveis."""
        if not self._qe.has_glob(self._get_pattern(), self._get_subdir()):
            return []

        path = self._qe.cache_path / self._get_subdir() / self._get_pattern()
        where = self._build_ifdata_date_where(start, end)

        query = f"""
            SELECT DISTINCT NomeRelatorio as RELATORIO
            FROM '{path}'
            {where}
            ORDER BY RELATORIO
        """

        df = self._qe.sql(query)
        return df["RELATORIO"].tolist() if not df.empty else []
