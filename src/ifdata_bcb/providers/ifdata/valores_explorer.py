"""Explorer para dados IFDATA Valores (trimestrais)."""

from typing import Literal

import pandas as pd

from ifdata_bcb.core.constants import (
    DATA_SOURCES,
    TIPO_INST_MAP,
    get_subdir,
)
from ifdata_bcb.core.entity_lookup import EntityLookup
from ifdata_bcb.domain.exceptions import ScopeUnavailableWarning
from ifdata_bcb.domain.types import AccountInput, InstitutionInput
from ifdata_bcb.infra.log import emit_user_warning
from ifdata_bcb.infra.query import QueryEngine
from ifdata_bcb.infra.sql import (
    build_account_condition,
    build_int_condition,
    build_like_condition,
    build_string_condition,
    join_conditions,
)
from ifdata_bcb.providers.base_explorer import BaseExplorer
from ifdata_bcb.providers.enrichment import (
    enrich_with_cadastro,
    validate_cadastro_columns,
)
from ifdata_bcb.providers.ifdata.collector import IFDATAValoresCollector
from ifdata_bcb.providers.ifdata.temporal import TemporalResolver


EscopoIFDATA = Literal["individual", "prudencial", "financeiro"]

# Colunas padrao para retorno vazio
_EMPTY_COLUMNS = [
    "DATA",
    "CNPJ_8",
    "INSTITUICAO",
    "ESCOPO",
    "COD_INST",
    "COD_CONTA",
    "CONTA",
    "VALOR",
    "RELATORIO",
    "GRUPO",
]
_EMPTY_ACCOUNT_COLUMNS = ["COD_CONTA", "CONTA", "RELATORIO", "GRUPO"]
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


class IFDATAExplorer(BaseExplorer):
    """
    Explorer para dados IFDATA Valores (trimestrais).

    Para dados cadastrais, use CadastroExplorer.
    """

    _COLUMN_MAP = {
        "AnoMes": "DATA",
        "CodInst": "COD_INST",
        "NomeColuna": "CONTA",
        "Conta": "COD_CONTA",
        "Saldo": "VALOR",
        "NomeRelatorio": "RELATORIO",
        "Grupo": "GRUPO",
    }

    _DERIVED_COLUMNS: set[str] = {"CNPJ_8", "INSTITUICAO", "ESCOPO"}

    _DROP_COLUMNS = ["TipoInstituicao"]

    _DATE_COLUMN = "AnoMes"

    _COLUMN_ORDER = [
        "DATA",
        "CNPJ_8",
        "INSTITUICAO",
        "ESCOPO",
        "COD_INST",
        "COD_CONTA",
        "CONTA",
        "VALOR",
        "RELATORIO",
        "GRUPO",
    ]

    _VALID_ESCOPOS = ["individual", "prudencial", "financeiro"]

    def __init__(
        self,
        query_engine: QueryEngine | None = None,
        entity_lookup: EntityLookup | None = None,
    ):
        super().__init__(query_engine, entity_lookup)
        self._collector: IFDATAValoresCollector | None = None
        self._temporal = TemporalResolver(
            query_engine=self._qe,
            entity_lookup=self._resolver,
            valores_subdir=self._get_subdir(),
            valores_pattern=self._get_pattern(),
        )

    def _get_subdir(self) -> str:
        return get_subdir("ifdata_valores")

    def _get_file_prefix(self) -> str:
        return DATA_SOURCES["ifdata_valores"]["prefix"]

    def _get_collector(self) -> IFDATAValoresCollector:
        if self._collector is None:
            self._collector = IFDATAValoresCollector()
        return self._collector

    def _warn_escopo_unavailable(
        self,
        cnpjs: list[str],
        escopo: str,
        periodos: list[int],
    ) -> None:
        """Emite warning consolidado para entidades sem escopo."""
        nomes = self._resolver.get_canonical_names_for_cnpjs(cnpjs)
        entities_labels = []
        for cnpj in cnpjs:
            nome = nomes.get(cnpj, "")
            label = f"{cnpj} ({nome})" if nome else cnpj
            entities_labels.append(label)

        period_range = (
            f"{min(periodos)}-{max(periodos)}"
            if len(periodos) > 1
            else str(periodos[0])
        )
        if len(entities_labels) <= 5:
            entity_str = ", ".join(entities_labels)
        else:
            entity_str = f"{len(entities_labels)} entidades"
        emit_user_warning(
            ScopeUnavailableWarning(
                f"Escopo {escopo} indisponivel para {entity_str} "
                f"nos periodos {period_range}.",
                entities=cnpjs,
                escopo=escopo,
                periodos=periodos,
            ),
            stacklevel=4,
        )

    def _read_single_escopo(
        self,
        instituicao: InstitutionInput,
        escopo: str,
        start: str,
        end: str | None,
        conta: AccountInput | None,
        relatorio: str | None,
        grupo: str | None,
        columns: list[str] | None,
    ) -> pd.DataFrame | None:
        """Le dados de um escopo especifico com resolucao temporal."""
        if isinstance(instituicao, str):
            instituicao = [instituicao]

        cnpjs = [self._resolve_entidade(i) for i in instituicao]
        periodos = self._resolve_date_range(start, end, trimestral=True) or []

        if not periodos:
            return None

        groups, unavailable = self._temporal.resolve(cnpjs, escopo, periodos)
        if unavailable:
            self._warn_escopo_unavailable(unavailable, escopo, periodos)
        if not groups:
            return None

        extra_conditions = self._build_common_conditions(conta, relatorio, grupo)

        storage_columns = self._storage_columns_for_query(columns, required=["CodInst"])

        frames: list[pd.DataFrame] = []
        tipo_inst = TIPO_INST_MAP[escopo]

        for group in groups:
            if escopo == "individual":
                codes = cnpjs
            else:
                codes = [group.cod_inst]

            conditions = [
                build_string_condition("CodInst", codes),
                build_int_condition("TipoInstituicao", [tipo_inst]),
                build_int_condition("AnoMes", group.periodos),
            ] + extra_conditions

            where = join_conditions(conditions)

            df = self._read_glob(
                pattern=self._get_pattern(),
                subdir=self._get_subdir(),
                columns=storage_columns,
                where=where,
            )
            if df.empty:
                continue

            df = df.copy()
            df["ESCOPO"] = escopo
            df = TemporalResolver.add_cnpj_mapping(df, group.cnpj_map)
            frames.append(df)

        if not frames:
            return None

        return pd.concat(frames, ignore_index=True)

    def collect(
        self, start: str, end: str, force: bool = False, verbose: bool = True
    ) -> None:
        """Coleta dados IFDATA Valores do BCB (trimestral)."""
        self._get_collector().collect(start, end, force=force, verbose=verbose)

    def _build_common_conditions(
        self,
        conta: AccountInput | None,
        relatorio: str | None,
        grupo: str | None,
    ) -> list[str]:
        """Condicoes SQL compartilhadas entre read com instituicao e bulk."""
        conditions: list[str] = []
        if conta:
            contas = self._normalize_contas(conta)
            if contas:
                conditions.append(
                    build_account_condition(
                        self._storage_col("CONTA"),
                        self._storage_col("COD_CONTA"),
                        contas,
                    )
                )
        if relatorio:
            conditions.append(
                build_string_condition(
                    self._storage_col("RELATORIO"),
                    [relatorio],
                    case_insensitive=True,
                    accent_insensitive=True,
                )
            )
        if grupo:
            conditions.append(
                build_string_condition(
                    self._storage_col("GRUPO"),
                    [grupo],
                    case_insensitive=True,
                    accent_insensitive=True,
                )
            )
        return conditions

    def _read_bulk(
        self,
        start: str,
        end: str | None,
        escopo: EscopoIFDATA | None,
        conta: AccountInput | None,
        relatorio: str | None,
        grupo: str | None,
        columns: list[str] | None,
        cadastro: list[str] | None,
    ) -> pd.DataFrame:
        """Le dados IFDATA sem resolucao de entidade (acesso direto ao parquet)."""
        from ifdata_bcb.domain.exceptions import PartialDataWarning

        periodos = self._resolve_date_range(start, end, trimestral=True)
        if not periodos:
            return pd.DataFrame(columns=_EMPTY_COLUMNS)

        escopos = (
            [self._validate_escopo(escopo)]
            if escopo
            else ["individual", "prudencial", "financeiro"]
        )

        extra_conditions = self._build_common_conditions(conta, relatorio, grupo)
        storage_columns = self._storage_columns_for_query(columns, required=["CodInst"])

        frames: list[pd.DataFrame] = []
        for esc in escopos:
            tipo_inst = TIPO_INST_MAP[esc]
            conditions = [
                build_int_condition("TipoInstituicao", [tipo_inst]),
                build_int_condition("AnoMes", periodos),
            ] + extra_conditions

            where = join_conditions(conditions)

            df = self._read_glob(
                pattern=self._get_pattern(),
                subdir=self._get_subdir(),
                columns=storage_columns,
                where=where,
            )
            if df.empty:
                continue

            df = df.copy()
            df["ESCOPO"] = esc
            if esc == "individual":
                df["CNPJ_8"] = df["CodInst"]
            frames.append(df)

        if not frames:
            self._diagnose_empty_result(
                source_name="IFDATA",
                has_files=self._ensure_data_exists(),
                had_conta_filter=conta is not None,
                had_institution_filter=False,
            )
            return pd.DataFrame(columns=_EMPTY_COLUMNS)

        df = pd.concat(frames, ignore_index=True)
        df = self._apply_canonical_names(df)
        df = self._finalize_read(df)
        self._check_null_value_instituicoes(df)

        if cadastro is not None:
            if "CNPJ_8" in df.columns and df["CNPJ_8"].notna().any():
                df = enrich_with_cadastro(df, cadastro, self._qe, self._resolver)
            else:
                emit_user_warning(
                    PartialDataWarning(
                        "Enrichment cadastral nao disponivel para bulk "
                        "prudencial/financeiro (sem CNPJ_8 no resultado). "
                        "Use instituicao= ou escopo='individual' para ativar.",
                        reason="no_cnpj_for_enrichment",
                    ),
                    stacklevel=2,
                )

        return self._filter_columns(df, columns)

    def read(
        self,
        start: str,
        end: str | None = None,
        *,
        instituicao: InstitutionInput | None = None,
        escopo: EscopoIFDATA | None = None,
        conta: AccountInput | None = None,
        relatorio: str | None = None,
        grupo: str | None = None,
        columns: list[str] | None = None,
        cadastro: list[str] | None = None,
    ) -> pd.DataFrame:
        """
        Le dados IFDATA Valores com filtros.

        Args:
            start: Periodo inicial (obrigatorio). Formato: '2024-12' ou '202412'.
            end: Periodo final. Se None, retorna apenas start.
            instituicao: CNPJ de 8 digitos. Se None, retorna todas (bulk).
            escopo: Filtro por escopo. Se None, busca em todos.
            conta: Filtro por conta (nome ou codigo).
            relatorio: Filtro por relatorio (case/accent insensitive).
            grupo: Filtro por grupo (case/accent insensitive).
            columns: Colunas a retornar. Se None, retorna todas.
            cadastro: Colunas cadastrais para enriquecer o resultado.

        Raises:
            MissingRequiredParameterError: Se start nao fornecido.
            InvalidDateRangeError: Se start > end.
        """
        self._validate_required_params(start)
        columns = self._validate_columns(columns)
        validate_cadastro_columns(cadastro)

        from ifdata_bcb.core.eras import IFDATA_ERA_BOUNDARY, check_era_boundary

        check_era_boundary(
            self._resolve_date_range(start, end, trimestral=True),
            IFDATA_ERA_BOUNDARY,
            "IFDATA",
        )
        self._logger.debug(f"IFDATA read: instituicao={instituicao}, escopo={escopo}")

        if instituicao is None:
            return self._read_bulk(
                start, end, escopo, conta, relatorio, grupo, columns, cadastro
            )

        escopos = (
            [self._validate_escopo(escopo)]
            if escopo
            else ["individual", "prudencial", "financeiro"]
        )
        results = []

        for esc in escopos:
            df = self._read_single_escopo(
                instituicao, esc, start, end, conta, relatorio, grupo, columns
            )
            if df is not None:
                results.append(df)

        if not results:
            self._diagnose_empty_result(
                source_name="IFDATA",
                has_files=self._ensure_data_exists(),
                had_conta_filter=conta is not None,
            )
            return pd.DataFrame(columns=_EMPTY_COLUMNS)

        df = pd.concat(results, ignore_index=True)
        df = self._apply_canonical_names(df)
        self._logger.debug(f"IFDATA result: {len(df)} rows")
        df = self._finalize_read(df)
        self._check_null_value_instituicoes(df)

        if cadastro is not None:
            df = enrich_with_cadastro(df, cadastro, self._qe, self._resolver)

        df = self._filter_columns(df, columns)
        return df

    def list_contas(
        self,
        termo: str | None = None,
        escopo: EscopoIFDATA | None = None,
        relatorio: str | None = None,
        start: str | None = None,
        end: str | None = None,
        limit: int = 100,
    ) -> pd.DataFrame:
        """
        Lista contas disponiveis.

        Args:
            termo: Filtro por nome (case-insensitive).
            escopo: Filtro por escopo. Se None, busca em todos.
            relatorio: Filtro por relatorio (case-insensitive).
            start: Periodo inicial (filtra contas que existem no periodo).
            end: Periodo final. Se None com start, filtra data unica.
            limit: Maximo de resultados.
        """
        if limit <= 0:
            raise ValueError(f"limit deve ser > 0, recebido: {limit}")
        if not self._ensure_data_exists():
            return pd.DataFrame(columns=_EMPTY_ACCOUNT_COLUMNS)

        path = self._qe.cache_path / self._get_subdir() / self._get_pattern()

        conditions = []
        if termo:
            conditions.append(build_like_condition("NomeColuna", termo))

        if escopo:
            tipo_inst = TIPO_INST_MAP[self._validate_escopo(escopo)]
            conditions.append(f"TipoInstituicao = {tipo_inst}")

        if relatorio:
            conditions.append(build_like_condition("NomeRelatorio", relatorio))

        datas = self._resolve_date_range(start, end, trimestral=True)
        if datas:
            conditions.append(build_int_condition("AnoMes", datas))

        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

        query = f"""
            SELECT DISTINCT Conta as COD_CONTA, NomeColuna as CONTA,
                            NomeRelatorio as RELATORIO, Grupo as GRUPO
            FROM '{path}'
            {where}
            ORDER BY RELATORIO, GRUPO, CONTA, COD_CONTA
            LIMIT {limit}
        """
        return self._qe.sql(query)

    def list_instituicoes(
        self,
        start: str | None = None,
        end: str | None = None,
    ) -> pd.DataFrame:
        """Lista entidades analiticas com disponibilidade por escopo."""
        df = self._temporal.resolve_mapeamento(start, end)
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

    def list_mapeamento(
        self,
        start: str | None = None,
        end: str | None = None,
    ) -> pd.DataFrame:
        """Lista chaves operacionais de reporte do IFDATA por entidade e escopo."""
        return self._temporal.resolve_mapeamento(start, end)

    def list_relatorios(
        self,
        start: str | None = None,
        end: str | None = None,
    ) -> list[str]:
        """Lista relatorios disponiveis."""
        if not self._ensure_data_exists():
            return []

        path = self._qe.cache_path / self._get_subdir() / self._get_pattern()
        where = self._temporal._ifdata_date_where(start, end)

        query = f"""
            SELECT DISTINCT NomeRelatorio as RELATORIO
            FROM '{path}'
            {where}
            ORDER BY RELATORIO
        """

        df = self._qe.sql(query)
        return df["RELATORIO"].tolist() if not df.empty else []
