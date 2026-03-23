"""Explorer para dados COSIF (mensais)."""

from __future__ import annotations

from typing import Literal, cast

import pandas as pd

from ifdata_bcb.core.constants import DATA_SOURCES, get_subdir
from ifdata_bcb.core.entity_lookup import EntityLookup
from ifdata_bcb.domain.exceptions import InvalidScopeError
from ifdata_bcb.domain.types import AccountInput, InstitutionInput
from ifdata_bcb.infra.query import QueryEngine
from ifdata_bcb.infra.sql import build_int_condition, build_like_condition
from ifdata_bcb.utils.text import stem_ptbr
from ifdata_bcb.providers.base_explorer import BaseExplorer
from ifdata_bcb.providers.cosif.collector import COSIFCollector
from ifdata_bcb.providers.enrichment import (
    enrich_with_cadastro,
    validate_cadastro_columns,
)
from ifdata_bcb.ui.display import get_display


EscopoCOSIF = Literal["individual", "prudencial"]

_EMPTY_COLUMNS = [
    "DATA",
    "CNPJ_8",
    "INSTITUICAO",
    "ESCOPO",
    "COD_CONTA",
    "CONTA",
    "DOCUMENTO",
    "VALOR",
]
_EMPTY_ACCOUNT_COLUMNS = ["COD_CONTA", "CONTA"]
_EMPTY_ACCOUNT_COLUMNS_ALL = ["COD_CONTA", "CONTA", "ESCOPOS"]


class COSIFExplorer(BaseExplorer):
    """
    Explorer para dados COSIF (mensais).

    Multi-source: escopos 'individual' e 'prudencial' (mesmo schema).
    """

    _COLUMN_MAP = {
        "DATA_BASE": "DATA",
        "NOME_INSTITUICAO": "INSTITUICAO",
        "NOME_CONTA": "CONTA",
        "CONTA": "COD_CONTA",
        "SALDO": "VALOR",
    }

    _DERIVED_COLUMNS: set[str] = {"ESCOPO"}
    _PASSTHROUGH_COLUMNS: set[str] = {"CNPJ_8", "DOCUMENTO"}

    _DATE_COLUMN = "DATA_BASE"

    _COLUMN_ORDER = [
        "DATA",
        "CNPJ_8",
        "INSTITUICAO",
        "ESCOPO",
        "COD_CONTA",
        "CONTA",
        "DOCUMENTO",
        "VALOR",
    ]

    _VALID_ESCOPOS = ["individual", "prudencial"]

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

    _LIST_COLUMNS: dict[str, str] = {
        "DATA": "DATA_BASE",
        "ESCOPO": "ESCOPO",
        "DOCUMENTO": "DOCUMENTO",
    }

    _BLOCKED_COLUMNS: dict[str, str] = {
        "CONTA": "Use list_contas(termo='...') para buscar contas.",
        "COD_CONTA": "Use list_contas(termo='...') para buscar contas.",
        "CNPJ_8": "Use cadastro.search() para buscar instituicoes.",
        "INSTITUICAO": "Use cadastro.search() para buscar instituicoes.",
        "VALOR": "VALOR e uma metrica continua, nao listavel.",
        "SALDO": "VALOR e uma metrica continua, nao listavel.",
    }

    def __init__(
        self,
        query_engine: QueryEngine | None = None,
        entity_lookup: EntityLookup | None = None,
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

    def _get_pattern_for_escopo(self, escopo: EscopoCOSIF) -> str:
        return f"{self._get_escopo_config(escopo)['prefix']}_*.parquet"

    def _read_single_escopo(
        self,
        escopo: EscopoCOSIF,
        instituicao: InstitutionInput,
        start: str,
        end: str | None,
        conta: AccountInput | None,
        columns: list[str] | None,
        documento: str | list[str] | None = None,
    ) -> pd.DataFrame:
        from ifdata_bcb.infra.sql import build_account_condition

        contas = self._normalize_contas(conta) if conta else None

        conditions = [
            self._build_cnpj_condition(instituicao),
            self._build_date_condition(start, end, trimestral=False),
        ]

        if contas:
            conditions.append(
                build_account_condition(
                    self._storage_col("CONTA"),
                    self._storage_col("COD_CONTA"),
                    contas,
                )
            )

        if documento:
            conditions.append(self._build_documento_condition(documento))

        from ifdata_bcb.infra.sql import join_conditions as jc

        return self._read_glob(
            pattern=self._get_pattern_for_escopo(escopo),
            subdir=self._get_escopo_config(escopo)["subdir"],
            columns=self._storage_columns_for_query(columns),
            where=jc(conditions),
        )

    def collect(
        self,
        start: str,
        end: str,
        escopo: EscopoCOSIF | None = None,
        force: bool = False,
        verbose: bool = True,
    ) -> None:
        """Coleta dados COSIF do BCB. Se escopo=None, coleta ambos."""
        if escopo is not None:
            escopo = self._validate_escopo(escopo)  # type: ignore[assignment]
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
        start: str,
        end: str | None = None,
        *,
        instituicao: InstitutionInput | None = None,
        escopo: EscopoCOSIF | None = None,
        conta: AccountInput | None = None,
        documento: str | list[str] | None = None,
        columns: list[str] | None = None,
        cadastro: list[str] | None = None,
    ) -> pd.DataFrame:
        """
        Le dados COSIF com filtros.

        Args:
            start: Periodo inicial (obrigatorio). Formato: '2024-12' ou '202412'.
            end: Periodo final. Se None, retorna apenas start.
            instituicao: CNPJ de 8 digitos. Se None, retorna todas (bulk).
            escopo: Filtro por escopo. Se None, busca em ambos.
            conta: Filtro por conta (nome ou codigo).
            documento: Filtro por documento COSIF.
            columns: Colunas a retornar. Se None, retorna todas.
            cadastro: Colunas cadastrais para enriquecer o resultado.

        Raises:
            MissingRequiredParameterError: Se start nao fornecido.
            InvalidDateRangeError: Se start > end.
        """
        self._validate_required_params(start)
        columns = self._validate_columns(columns)
        validate_cadastro_columns(cadastro)

        from ifdata_bcb.core.eras import COSIF_ERA_BOUNDARY, check_era_boundary

        check_era_boundary(
            self._resolve_date_range(start, end), COSIF_ERA_BOUNDARY, "COSIF"
        )
        self._logger.debug(f"COSIF read: escopo={escopo}, instituicao={instituicao}")

        escopos: list[EscopoCOSIF] = (
            [self._validate_escopo(escopo)]  # type: ignore[misc]
            if escopo
            else cast(list[EscopoCOSIF], list(self._ESCOPOS.keys()))
        )

        results = []
        for esc in escopos:
            df = self._read_single_escopo(
                esc, instituicao, start, end, conta, columns, documento
            )
            if not df.empty:
                df = df.copy()
                df["ESCOPO"] = esc
                results.append(df)

        if not results:
            self._diagnose_empty_result(
                source_name="COSIF",
                has_files=any(
                    self._qe.has_glob(f"{cfg['prefix']}_*.parquet", cfg["subdir"])
                    for cfg in self._get_sources().values()
                ),
                had_conta_filter=conta is not None,
                had_institution_filter=instituicao is not None,
            )
            return pd.DataFrame(columns=_EMPTY_COLUMNS)

        df = pd.concat(results, ignore_index=True)
        self._logger.debug(f"COSIF result: {len(df)} rows")
        df = self._finalize_read(df)
        self._check_null_value_instituicoes(df)
        df = self._apply_canonical_names(df)

        if cadastro is not None:
            df = enrich_with_cadastro(df, cadastro, self._qe, self._resolver)

        df = self._filter_columns(df, columns)
        return df

    def list(
        self,
        columns: list[str],
        *,
        start: str | None = None,
        end: str | None = None,
        escopo: EscopoCOSIF | None = None,
        documento: str | list[str] | None = None,
        limit: int = 100,
    ) -> pd.DataFrame:
        """Lista valores distintos para as colunas solicitadas.

        Args:
            columns: Colunas a listar (DATA, ESCOPO, DOCUMENTO).
            start: Periodo inicial (opcional).
            end: Periodo final (opcional).
            escopo: Filtro por escopo.
            documento: Filtro por documento COSIF.
            limit: Maximo de resultados.

        Raises:
            InvalidColumnError: Se coluna invalida.
        """
        return self._base_list(
            columns,
            start=start,
            end=end,
            limit=limit,
            escopo=escopo,
            documento=documento,
        )

    def _get_list_source(
        self,
        columns: list[str],
        start: str | None = None,
        end: str | None = None,
        **filters: object,
    ) -> str | None:
        """Monta UNION ALL de escopos disponiveis com coluna ESCOPO literal."""
        escopo_filter = filters.get("escopo")
        if escopo_filter is not None:
            escopos_to_check: list[str] = [self._validate_escopo(str(escopo_filter))]
        else:
            escopos_to_check = list(self._ESCOPOS.keys())

        union_parts: list[str] = []
        for esc in escopos_to_check:
            cfg = self._get_escopo_config(esc)  # type: ignore[arg-type]
            pattern = self._get_pattern_for_escopo(esc)  # type: ignore[arg-type]
            if not self._qe.has_glob(pattern, cfg["subdir"]):
                continue
            path = self._qe.cache_path / cfg["subdir"] / pattern
            union_parts.append(
                f"SELECT *, '{esc}' AS ESCOPO "
                f"FROM read_parquet('{path}', union_by_name=true)"
            )

        if not union_parts:
            return None

        if len(union_parts) == 1:
            return f"({union_parts[0]})"
        return f"({' UNION ALL '.join(union_parts)})"

    def _build_documento_condition(self, documento: str | list[str]) -> str:
        """Valida e converte documento para condicao SQL."""
        docs = [documento] if isinstance(documento, str) else documento
        try:
            docs_int = [int(d) for d in docs]
        except (ValueError, TypeError):
            raise InvalidScopeError(
                "documento", str(documento), "valores numericos (ex: 4010, 4016)"
            )
        return build_int_condition("DOCUMENTO", docs_int)

    def _build_list_conditions(
        self,
        start: str | None = None,
        end: str | None = None,
        **filters: object,
    ) -> list[str | None]:
        conditions: list[str | None] = []

        # Date filter (mensal)
        conditions.append(self._build_date_condition(start, end, trimestral=False))

        # Escopo filter (already handled by _get_list_source UNION selection,
        # but also add WHERE for safety when both escopos in UNION)
        escopo = filters.get("escopo")
        if escopo is not None:
            esc_val = self._validate_escopo(str(escopo))
            conditions.append(f"ESCOPO = '{esc_val}'")

        # Documento filter
        documento = filters.get("documento")
        if documento is not None:
            conditions.append(self._build_documento_condition(documento))

        return conditions

    def list_contas(
        self,
        termo: str | None = None,
        escopo: EscopoCOSIF | None = None,
        start: str | None = None,
        end: str | None = None,
        limit: int = 100,
    ) -> pd.DataFrame:
        """
        Lista contas disponiveis.

        Args:
            termo: Filtro por nome (case-insensitive, sem acentos).
            escopo: Filtro por escopo. Se None, busca em ambos.
            start: Periodo inicial (filtra contas que existem no periodo).
            end: Periodo final. Se None com start, filtra data unica.
            limit: Maximo de resultados.
        """
        if limit <= 0:
            raise ValueError(f"limit deve ser > 0, recebido: {limit}")
        datas = self._resolve_date_range(start, end, trimestral=False)

        if escopo is not None:
            escopo = self._validate_escopo(escopo)  # type: ignore[assignment]
            return self._list_contas_single(escopo, termo, datas, limit)

        dfs = []
        for esc in cast(list[EscopoCOSIF], list(self._ESCOPOS.keys())):
            df = self._list_contas_single(esc, termo, datas, limit=None)
            if df.empty:
                continue
            df["_escopo"] = esc
            dfs.append(df)
        if not dfs:
            return pd.DataFrame(columns=_EMPTY_ACCOUNT_COLUMNS_ALL)

        combined = pd.concat(dfs, ignore_index=True)
        result = (
            combined.groupby(["COD_CONTA", "CONTA"], sort=False)
            .agg(ESCOPOS=("_escopo", lambda x: ", ".join(sorted(x.unique()))))
            .reset_index()
            .sort_values(["CONTA", "COD_CONTA"])
            .reset_index(drop=True)
        )
        if limit is not None:
            result = result.head(limit)
        return result

    def _list_contas_single(
        self,
        escopo: EscopoCOSIF,
        termo: str | None,
        datas: list[int] | None,
        limit: int | None,
    ) -> pd.DataFrame:
        cfg = self._get_escopo_config(escopo)
        pattern = self._get_pattern_for_escopo(escopo)
        if not self._qe.has_glob(pattern, cfg["subdir"]):
            return pd.DataFrame(columns=_EMPTY_ACCOUNT_COLUMNS)

        path = self._qe.cache_path / cfg["subdir"] / pattern

        # Base conditions for the CTE (date filter + non-null names)
        base_conditions: list[str] = ["NOME_CONTA IS NOT NULL"]
        if datas:
            base_conditions.append(build_int_condition("DATA_BASE", datas))

        base_where = f"WHERE {' AND '.join(base_conditions)}"

        # Outer conditions (applied after dedup): term filter
        outer_conditions: list[str] = ["rn = 1"]
        if termo:
            outer_conditions.append(
                build_like_condition("NOME_CONTA", stem_ptbr(termo))
            )

        outer_where = f"WHERE {' AND '.join(outer_conditions)}"
        limit_clause = f"LIMIT {limit}" if limit is not None else ""

        query = f"""
            WITH deduped AS (
                SELECT CONTA, NOME_CONTA, DATA_BASE,
                       ROW_NUMBER() OVER (
                           PARTITION BY CONTA ORDER BY DATA_BASE DESC
                       ) AS rn
                FROM '{path}'
                {base_where}
            )
            SELECT DISTINCT CONTA AS COD_CONTA, NOME_CONTA AS CONTA
            FROM deduped
            {outer_where}
            ORDER BY CONTA, COD_CONTA
            {limit_clause}
        """
        return self._qe.sql(query)
