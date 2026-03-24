"""Explorer para dados IFDATA Valores (trimestrais)."""

from __future__ import annotations

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
    build_in_clause,
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
from ifdata_bcb.providers.ifdata.valores.collector import IFDATAValoresCollector
from ifdata_bcb.providers.ifdata.valores.temporal import TemporalGroup, TemporalResolver
from ifdata_bcb.utils.text import format_entity_labels, stem_ptbr


EscopoIFDATA = Literal["individual", "prudencial", "financeiro"]

_ACCOUNT_COLUMNS = ["COD_CONTA", "CONTA", "RELATORIO", "GRUPO"]


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

    _LIST_COLUMNS: dict[str, str] = {
        "DATA": "AnoMes",
        "ESCOPO": (
            "CASE TipoInstituicao "
            "WHEN 1 THEN 'prudencial' "
            "WHEN 2 THEN 'financeiro' "
            "WHEN 3 THEN 'individual' END"
        ),
        "RELATORIO": "NomeRelatorio",
        "GRUPO": "Grupo",
    }

    _BLOCKED_COLUMNS: dict[str, str] = {
        "CONTA": "Use list_contas(termo='...') para buscar contas.",
        "COD_CONTA": "Use list_contas(termo='...') para buscar contas.",
        "COD_INST": "Use cadastro.search(fonte='ifdata') para listar instituicoes.",
        "VALOR": "VALOR e uma metrica continua, nao listavel.",
    }

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
        entity_str = format_entity_labels(cnpjs, nomes)

        period_range = (
            f"{min(periodos)}-{max(periodos)}"
            if len(periodos) > 1
            else str(periodos[0])
        )
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
        """Condicoes SQL para filtros de conta, relatorio e grupo."""
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

    def _collect_frames(
        self,
        escopos: list[str],
        periodos: list[int],
        instituicao: InstitutionInput | None,
        conta: AccountInput | None,
        relatorio: str | None,
        grupo: str | None,
        columns: list[str] | None,
    ) -> list[pd.DataFrame]:
        """Coleta frames por escopo. Resolve temporal se instituicao fornecida."""
        extra_conditions = self._build_common_conditions(conta, relatorio, grupo)
        storage_columns = self._storage_columns_for_query(columns, required=["CodInst"])
        frames: list[pd.DataFrame] = []

        cnpjs: list[str] | None = None
        if instituicao is not None:
            if isinstance(instituicao, str):
                instituicao = [instituicao]
            cnpjs = [self._resolve_entidade(i) for i in instituicao]

        for esc in escopos:
            tipo_inst = TIPO_INST_MAP[esc]

            if cnpjs is not None:
                groups, unavailable = self._temporal.resolve(cnpjs, esc, periodos)
                if unavailable:
                    self._warn_escopo_unavailable(unavailable, esc, periodos)

                self._collect_resolved_groups(
                    groups,
                    esc,
                    tipo_inst,
                    cnpjs,
                    extra_conditions,
                    storage_columns,
                    frames,
                )
            else:
                conditions = [
                    build_int_condition("TipoInstituicao", [tipo_inst]),
                    build_int_condition("AnoMes", periodos),
                ] + extra_conditions

                df = self._read_glob(
                    pattern=self._get_pattern(),
                    subdir=self._get_subdir(),
                    columns=storage_columns,
                    where=join_conditions(conditions),
                )
                if df.empty:
                    continue
                df = df.copy()
                df["ESCOPO"] = esc
                if esc == "individual":
                    df["CNPJ_8"] = df["CodInst"]
                else:
                    # CodInst numerico = inst. independente (CNPJ direto)
                    # CodInst nao-numerico = conglomerado (precisa lookup)
                    is_numeric = df["CodInst"].str.match(r"^\d+$", na=False)
                    df.loc[is_numeric, "CNPJ_8"] = df.loc[is_numeric, "CodInst"]

                    congl_codes = df.loc[~is_numeric, "CodInst"].unique().tolist()
                    if congl_codes:
                        cnpj_map = self._resolve_bulk_conglomerate_cnpjs(
                            congl_codes, esc
                        )
                        df.loc[~is_numeric, "CNPJ_8"] = df.loc[
                            ~is_numeric, "CodInst"
                        ].map(cnpj_map)
                frames.append(df)

        return frames

    def _resolve_bulk_conglomerate_cnpjs(
        self,
        cod_insts: list[str],
        escopo: str,
    ) -> dict[str, str]:
        """Resolve codigos de conglomerado para CNPJ lider via cadastro.

        Para bulk prudencial/financeiro, CodInst e um codigo de conglomerado
        (ex: C0080329). Este metodo faz o mapeamento reverso para o CNPJ
        da instituicao lider, permitindo resolucao de nomes.
        """
        if not cod_insts:
            return {}

        cod_col = (
            "CodConglomeradoPrudencial"
            if escopo == "prudencial"
            else "CodConglomeradoFinanceiro"
        )
        cadastro_path = self._resolver._source_path("cadastro")
        cod_str = build_in_clause(cod_insts)

        sql = f"""
        SELECT COD_INST, CNPJ_LIDER_8
        FROM (
            SELECT {cod_col} AS COD_INST, CNPJ_LIDER_8,
                   ROW_NUMBER() OVER (
                       PARTITION BY {cod_col} ORDER BY Data DESC
                   ) AS rn
            FROM read_parquet('{cadastro_path}', union_by_name=true)
            WHERE {cod_col} IN ({cod_str})
              AND CNPJ_LIDER_8 IS NOT NULL
              AND {self._resolver.real_entity_condition()}
        )
        WHERE rn = 1
        """

        try:
            df = self._qe.sql(sql)
            return dict(
                zip(
                    df["COD_INST"].astype(str).values,
                    df["CNPJ_LIDER_8"].astype(str).values,
                )
            )
        except Exception as e:
            self._logger.warning(f"Bulk conglomerate CNPJ resolution failed: {e}")
            return {}

    def _collect_resolved_groups(
        self,
        groups: list[TemporalGroup],
        escopo: str,
        tipo_inst: int,
        cnpjs: list[str],
        extra_conditions: list[str],
        storage_columns: list[str] | None,
        frames: list[pd.DataFrame],
    ) -> None:
        """Consolida TemporalGroups com mesmos periodos em queries unicas."""
        if not groups:
            return

        if escopo == "individual":
            # Individual: todos os CNPJs na mesma query (ja era assim)
            group = groups[0]
            conditions = [
                build_string_condition("CodInst", cnpjs),
                build_int_condition("TipoInstituicao", [tipo_inst]),
                build_int_condition("AnoMes", group.periodos),
            ] + extra_conditions

            df = self._read_glob(
                pattern=self._get_pattern(),
                subdir=self._get_subdir(),
                columns=storage_columns,
                where=join_conditions(conditions),
            )
            if not df.empty:
                df = df.copy()
                df["ESCOPO"] = escopo
                df = TemporalResolver.add_cnpj_mapping(df, group.cnpj_map)
                frames.append(df)
            return

        # Conglomerado: agrupar groups por set de periodos para
        # consolidar em uma unica query por batch.
        batches: dict[tuple[int, ...], list] = {}
        for group in groups:
            key = tuple(group.periodos)
            batches.setdefault(key, []).append(group)

        for periodos_key, batch in batches.items():
            all_codes = [g.cod_inst for g in batch]
            merged_cnpj_map: dict[str, list[str]] = {}
            for g in batch:
                merged_cnpj_map.update(g.cnpj_map)

            conditions = [
                build_string_condition("CodInst", all_codes),
                build_int_condition("TipoInstituicao", [tipo_inst]),
                build_int_condition("AnoMes", list(periodos_key)),
            ] + extra_conditions

            df = self._read_glob(
                pattern=self._get_pattern(),
                subdir=self._get_subdir(),
                columns=storage_columns,
                where=join_conditions(conditions),
            )
            if df.empty:
                continue
            df = df.copy()
            df["ESCOPO"] = escopo
            df = TemporalResolver.add_cnpj_mapping(df, merged_cnpj_map)
            frames.append(df)

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

        from ifdata_bcb.core.eras import check_ifdata_era

        check_ifdata_era(
            self._resolve_date_range(start, end, trimestral=True),
            relatorio=relatorio,
            escopo=escopo,
        )
        self._logger.debug(f"IFDATA read: instituicao={instituicao}, escopo={escopo}")

        escopos = (
            [self._validate_escopo(escopo)]
            if escopo
            else ["individual", "prudencial", "financeiro"]
        )
        periodos = self._resolve_date_range(start, end, trimestral=True)
        if not periodos:
            return pd.DataFrame(columns=self._COLUMN_ORDER)

        frames = self._collect_frames(
            escopos, periodos, instituicao, conta, relatorio, grupo, columns
        )

        if not frames:
            self._diagnose_empty_result(
                source_name="IFDATA",
                has_files=self._ensure_data_exists(),
                had_conta_filter=conta is not None,
                had_institution_filter=instituicao is not None,
            )
            return pd.DataFrame(columns=self._COLUMN_ORDER)

        df = pd.concat(frames, ignore_index=True)
        df = self._apply_canonical_names(df)
        df = self._finalize_read(df)
        self._check_null_value_instituicoes(df)

        if cadastro is not None:
            from ifdata_bcb.domain.exceptions import PartialDataWarning

            has_cnpj = "CNPJ_8" in df.columns and df["CNPJ_8"].notna().any()
            if has_cnpj:
                df = enrich_with_cadastro(df, cadastro, self._qe, self._resolver)
            elif instituicao is None:
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

    def list(
        self,
        columns: list[str],
        *,
        start: str | None = None,
        end: str | None = None,
        escopo: EscopoIFDATA | None = None,
        relatorio: str | None = None,
        grupo: str | None = None,
        limit: int = 100,
    ) -> pd.DataFrame:
        """Lista valores distintos para as colunas solicitadas.

        Args:
            columns: Colunas a listar (DATA, ESCOPO, RELATORIO, GRUPO).
            start: Periodo inicial (opcional).
            end: Periodo final (opcional).
            escopo: Filtro por escopo.
            relatorio: Filtro por relatorio (case/accent insensitive).
            grupo: Filtro por grupo (case/accent insensitive).
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
            relatorio=relatorio,
            grupo=grupo,
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

        # Escopo -> TipoInstituicao
        escopo = filters.get("escopo")
        if escopo is not None:
            tipo_inst = TIPO_INST_MAP[self._validate_escopo(str(escopo))]
            conditions.append(f"TipoInstituicao = {tipo_inst}")

        # Relatorio
        relatorio = filters.get("relatorio")
        if relatorio is not None:
            conditions.append(
                build_string_condition(
                    "NomeRelatorio",
                    [str(relatorio)],
                    case_insensitive=True,
                    accent_insensitive=True,
                )
            )

        # Grupo
        grupo = filters.get("grupo")
        if grupo is not None:
            conditions.append(
                build_string_condition(
                    "Grupo",
                    [str(grupo)],
                    case_insensitive=True,
                    accent_insensitive=True,
                )
            )

        return conditions

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
            return pd.DataFrame(columns=_ACCOUNT_COLUMNS)

        path = self._qe.cache_path / self._get_subdir() / self._get_pattern()

        conditions = []
        if termo:
            conditions.append(build_like_condition("NomeColuna", stem_ptbr(termo)))

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

    def mapeamento(
        self,
        start: str | None = None,
        end: str | None = None,
    ) -> pd.DataFrame:
        """Tabela de mapeamento COD_INST <-> CNPJ_8 por escopo."""
        return self._temporal.resolve_mapeamento(start, end)
