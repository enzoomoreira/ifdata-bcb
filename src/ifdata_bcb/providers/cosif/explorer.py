from typing import Literal, Optional

import pandas as pd

from ifdata_bcb.core.base_explorer import BaseExplorer
from ifdata_bcb.core.constants import DATA_SOURCES, get_subdir
from ifdata_bcb.core.entity_lookup import EntityLookup
from ifdata_bcb.domain.exceptions import InvalidScopeError
from ifdata_bcb.domain.types import AccountInput, InstitutionInput
from ifdata_bcb.infra.query import QueryEngine
from ifdata_bcb.utils.text import normalize_accents
from ifdata_bcb.providers.cosif.collector import COSIFCollector
from ifdata_bcb.ui.display import get_display


EscopoCOSIF = Literal["individual", "prudencial"]

_EMPTY_COLUMNS = [
    "DATA",
    "CNPJ_8",
    "INSTITUICAO",
    "ESCOPO",
    "CONTA",
    "VALOR",
]
_EMPTY_ACCOUNT_COLUMNS = ["COD_CONTA", "CONTA"]
_EMPTY_ACCOUNT_COLUMNS_ALL = ["COD_CONTA", "CONTA", "ESCOPO"]
_EMPTY_INSTITUTION_COLUMNS = ["CNPJ_8", "INSTITUICAO"]
_EMPTY_INSTITUTION_COLUMNS_ALL = ["CNPJ_8", "INSTITUICAO", "ESCOPO"]


class COSIFExplorer(BaseExplorer):
    """
    Explorer para dados COSIF (mensais).

    Multi-source: escopos 'individual' e 'prudencial' (mesmo schema).
    """

    _COLUMN_MAP = {
        "DATA_BASE": "DATA",
        "NOME_INSTITUICAO": "INSTITUICAO",
        "NOME_CONTA": "CONTA",
        "SALDO": "VALOR",
    }

    # Colunas a remover do resultado final (internas/redundantes)
    _DROP_COLUMNS = ["CONTA", "DOCUMENTO"]

    _COLUMN_ORDER = [
        "DATA",
        "CNPJ_8",
        "INSTITUICAO",
        "ESCOPO",
        "CONTA",
        "VALOR",
    ]

    _ESCOPOS: dict[str, dict[str, str]] = {
        "individual": {
            "subdir": get_subdir("cosif_individual"),
            "prefix": DATA_SOURCES["cosif_individual"]["prefix"],
        },
        "prudencial": {
            "subdir": get_subdir("cosif_prudencial"),
            "prefix": DATA_SOURCES["cosif_prudencial"]["prefix"],
        },
    }

    def __init__(
        self,
        query_engine: Optional[QueryEngine] = None,
        entity_lookup: Optional[EntityLookup] = None,
    ):
        super().__init__(query_engine, entity_lookup)

    def _get_sources(self) -> dict[str, dict[str, str]]:
        return self._ESCOPOS

    def _get_subdir(self) -> str:
        return self._ESCOPOS["individual"]["subdir"]

    def _get_file_prefix(self) -> str:
        return self._ESCOPOS["individual"]["prefix"]

    def _get_escopo_config(self, escopo: EscopoCOSIF) -> dict[str, str]:
        return self._ESCOPOS[escopo]

    def _get_pattern(self, escopo: EscopoCOSIF) -> str:
        return f"{self._get_escopo_config(escopo)['prefix']}_*.parquet"

    def _validate_escopo(self, escopo: str) -> EscopoCOSIF:
        escopo_lower = escopo.lower()
        if escopo_lower not in self._ESCOPOS:
            raise InvalidScopeError("escopo", escopo, list(self._ESCOPOS.keys()))
        return escopo_lower  # type: ignore

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

    def _apply_canonical_institution_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Substitui aliases do COSIF por nomes canônicos do cadastro."""
        if df.empty or "CNPJ_8" not in df.columns or "INSTITUICAO" not in df.columns:
            return df

        cnpjs = df["CNPJ_8"].dropna().astype(str).unique().tolist()
        if not cnpjs:
            return df

        nomes = self._resolver.get_canonical_names_for_cnpjs(cnpjs)
        df = df.copy()
        canonical = df["CNPJ_8"].astype(str).map(nomes)
        mask = canonical.notna() & (canonical != "")
        df.loc[mask, "INSTITUICAO"] = canonical[mask]
        return df

    def _read_single_scope(
        self,
        escopo: EscopoCOSIF,
        instituicao: InstitutionInput,
        start: str,
        end: Optional[str],
        conta: Optional[AccountInput],
        columns: Optional[list[str]],
    ) -> pd.DataFrame:
        """Le dados de um escopo especifico."""
        contas = self._normalize_accounts(conta) if conta else None

        conditions = [
            self._build_cnpj_condition(instituicao),
            self._build_date_condition(start, end, trimestral=False),
        ]

        if contas:
            conditions.append(
                self._build_string_condition(
                    self._storage_col("CONTA"),
                    contas,
                    case_insensitive=True,
                    accent_insensitive=True,
                )
            )

        return self._qe.read_glob(
            pattern=self._get_pattern(escopo),
            subdir=self._get_escopo_config(escopo)["subdir"],
            columns=self._translate_columns(columns),
            where=self._join_conditions(conditions),
        )

    def collect(
        self,
        start: str,
        end: str,
        escopo: Optional[EscopoCOSIF] = None,
        force: bool = False,
        verbose: bool = True,
    ) -> None:
        """Coleta dados COSIF do BCB. Se escopo=None, coleta ambos."""
        if escopo is not None:
            escopo = self._validate_escopo(escopo)
            COSIFCollector(escopo).collect(start, end, force=force, verbose=verbose)
        else:
            self._collect_all_escopos(start, end, force=force, verbose=verbose)

    def _collect_all_escopos(
        self,
        start: str,
        end: str,
        force: bool = False,
        verbose: bool = True,
    ) -> None:
        display = get_display()

        collectors_info: list[tuple[str, COSIFCollector, int]] = []
        total_periodos = 0

        for esc in self._ESCOPOS:
            collector = COSIFCollector(esc)
            periods = (
                collector._generate_periods(start, end)
                if force
                else collector._get_missing_periods(start, end)
            )
            if periods:
                collectors_info.append((esc, collector, len(periods)))
                total_periodos += len(periods)

        if not collectors_info:
            display.print_info("COSIF: Dados ja atualizados", verbose=verbose)
            return

        escopos_str = " + ".join(esc.capitalize() for esc, _, _ in collectors_info)
        display.banner(
            f"Coletando COSIF ({escopos_str})",
            indicator_count=total_periodos,
            verbose=verbose,
        )

        total_registros = 0
        total_falhas = 0
        total_indisponiveis = 0
        periodos_ok = 0

        for esc, collector, _ in collectors_info:
            registros, ok, falhas, indisponiveis = collector.collect(
                start,
                end,
                force=force,
                verbose=verbose,
                progress_desc=f"  {esc.capitalize()}",
                _show_banners=False,
            )
            total_registros += registros
            total_falhas += falhas
            total_indisponiveis += indisponiveis
            periodos_ok += ok

        display.end_banner(
            total=total_registros if total_registros > 0 else None,
            periodos=periodos_ok,
            falhas=total_falhas if total_falhas > 0 else None,
            indisponiveis=total_indisponiveis if total_indisponiveis > 0 else None,
            verbose=verbose,
        )

    def read(
        self,
        instituicao: InstitutionInput,
        start: str,
        end: Optional[str] = None,
        conta: Optional[AccountInput] = None,
        escopo: Optional[EscopoCOSIF] = None,
        columns: Optional[list[str]] = None,
        cadastro: Optional[list[str]] = None,
    ) -> pd.DataFrame:
        """
        Le dados COSIF com filtros.

        Args:
            cadastro: Colunas cadastrais para enriquecer o resultado
                (ex: ["TCB", "SEGMENTO"]). Se None, nao enriquece.

        Raises:
            MissingRequiredParameterError: Se instituicao ou start nao fornecidos.
            InvalidDateRangeError: Se start > end.
        """
        self._validate_required_params(instituicao, start)
        self._validate_cadastro_columns(cadastro)
        self._logger.debug(f"COSIF read: escopo={escopo}, instituicao={instituicao}")

        escopos = (
            [self._validate_escopo(escopo)] if escopo else list(self._ESCOPOS.keys())
        )

        results = []
        for esc in escopos:
            df = self._read_single_scope(esc, instituicao, start, end, conta, columns)
            if not df.empty:
                df = df.copy()
                df["ESCOPO"] = esc
                results.append(df)

        if not results:
            return pd.DataFrame(columns=_EMPTY_COLUMNS)

        df = pd.concat(results, ignore_index=True)
        self._logger.debug(f"COSIF result: {len(df)} rows")
        df = self._finalize_read(df)
        df = self._apply_canonical_institution_names(df)

        if cadastro is not None:
            df = self._enrich_with_cadastro(df, cadastro)

        return df

    def list_accounts(
        self,
        termo: Optional[str] = None,
        escopo: Optional[EscopoCOSIF] = None,
        limit: int = 100,
    ) -> pd.DataFrame:
        """
        Lista contas disponiveis.

        Args:
            termo: Filtro por nome (case-insensitive, sem acentos).
            escopo: Filtro por escopo. Se None, busca em ambos.
            limit: Maximo de resultados.
        """
        if escopo is not None:
            escopo = self._validate_escopo(escopo)
            return self._list_accounts_single(escopo, termo, limit)

        dfs = []
        for esc in self._ESCOPOS:
            df = self._list_accounts_single(esc, termo, limit)
            if df.empty:
                continue
            df["ESCOPO"] = esc
            dfs.append(df)
        if not dfs:
            return pd.DataFrame(columns=_EMPTY_ACCOUNT_COLUMNS_ALL)
        return pd.concat(dfs, ignore_index=True)

    def _list_accounts_single(
        self, escopo: EscopoCOSIF, termo: Optional[str], limit: int
    ) -> pd.DataFrame:
        cfg = self._get_escopo_config(escopo)
        if not self._qe.has_glob(self._get_pattern(escopo), cfg["subdir"]):
            return pd.DataFrame(columns=_EMPTY_ACCOUNT_COLUMNS)

        path = self._qe.cache_path / cfg["subdir"] / self._get_pattern(escopo)

        where = ""
        if termo:
            termo_clean = normalize_accents(termo.strip().replace("'", "''")).upper()
            where = f"WHERE strip_accents(UPPER(NOME_CONTA)) LIKE '%{termo_clean}%'"

        query = f"""
            SELECT DISTINCT CONTA as COD_CONTA, NOME_CONTA as CONTA
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
        escopo: Optional[EscopoCOSIF] = None,
    ) -> pd.DataFrame:
        """Lista instituicoes disponiveis."""
        if escopo is not None:
            escopo = self._validate_escopo(escopo)
            return self._list_institutions_single(escopo, start, end)

        dfs = []
        for esc in self._ESCOPOS:
            df = self._list_institutions_single(esc, start, end)
            if df.empty:
                continue
            df["ESCOPO"] = esc
            dfs.append(df)
        if not dfs:
            return pd.DataFrame(columns=_EMPTY_INSTITUTION_COLUMNS_ALL)
        return pd.concat(dfs, ignore_index=True)

    def _list_institutions_single(
        self,
        escopo: EscopoCOSIF,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> pd.DataFrame:
        cfg = self._get_escopo_config(escopo)
        if not self._qe.has_glob(self._get_pattern(escopo), cfg["subdir"]):
            return pd.DataFrame(columns=_EMPTY_INSTITUTION_COLUMNS)

        path = self._qe.cache_path / cfg["subdir"] / self._get_pattern(escopo)

        where = ""
        datas = self._resolve_date_range(start, end, trimestral=False)
        if datas:
            cond = self._build_int_condition("DATA_BASE", datas)
            where = f"WHERE {cond}"

        query = f"""
            SELECT DISTINCT CNPJ_8, NOME_INSTITUICAO as INSTITUICAO
            FROM '{path}'
            {where}
            ORDER BY INSTITUICAO
        """
        df = self._qe.sql(query)
        return self._apply_canonical_institution_names(df)
