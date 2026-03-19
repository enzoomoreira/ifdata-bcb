"""Classe base abstrata para Explorers de dados do BCB."""

from abc import ABC, abstractmethod

import pandas as pd

from ifdata_bcb.core.entity_lookup import EntityLookup
from ifdata_bcb.domain.exceptions import (
    EmptyFilterWarning,
    InvalidDateRangeError,
    InvalidScopeError,
    MissingRequiredParameterError,
    NullValuesWarning,
    PartialDataWarning,
)
from ifdata_bcb.domain.types import AccountInput, DateInput, InstitutionInput
from ifdata_bcb.domain.validation import (
    AccountList,
    InstitutionList,
    NormalizedDates,
    ValidatedCnpj8,
)
from ifdata_bcb.infra.log import emit_user_warning, get_logger
from ifdata_bcb.infra.query import QueryEngine
from ifdata_bcb.infra.sql import (
    build_int_condition,
    build_string_condition,
)
from ifdata_bcb.infra.storage import list_parquet_files


class BaseExplorer(ABC):
    """
    Classe base abstrata para Explorers de dados do BCB.

    Um Explorer combina:
    - Coleta de dados (via Collector)
    - Queries de dados (via QueryEngine com DuckDB)
    - Resolucao de entidades (via EntityLookup)

    Subclasses devem implementar:
    - _get_subdir(): Subdiretorio dos dados (fonte unica)
    - _get_file_prefix(): Prefixo dos arquivos Parquet (fonte unica)

    Multi-source (mesmo schema, multiplas fontes):
    - Override _get_sources() para retornar dict de fontes
    - Exemplo: COSIF com escopos 'individual' e 'prudencial'
    - Metodos list_periodos(), has_data(), describe() suportam parametro source

    Metodos read() e collect() tem assinaturas especificas por provider,
    portanto nao sao declarados na base.

    Configuracao por class attributes:
    - _COLUMN_MAP: Mapeamento de colunas storage -> apresentacao
    - _DERIVED_COLUMNS: Colunas adicionadas pos-query por Python
    - _PASSTHROUGH_COLUMNS: Colunas nativas do parquet sem rename (passam direto)
    - _DROP_COLUMNS: Colunas a remover antes do mapeamento
    - _COLUMN_ORDER: Ordem desejada das colunas no output
    - _VALID_ESCOPOS: Lista de escopos validos para _validate_escopo
    """

    _COLUMN_MAP: dict[str, str] = {}
    _DERIVED_COLUMNS: set[str] = set()
    _PASSTHROUGH_COLUMNS: set[str] = set()
    _DROP_COLUMNS: list[str] = []
    _COLUMN_ORDER: list[str] = []
    _VALID_ESCOPOS: list[str] = []
    _DATE_COLUMN: str | None = None

    def __init__(
        self,
        query_engine: QueryEngine | None = None,
        entity_lookup: EntityLookup | None = None,
    ):
        self._qe = query_engine or QueryEngine()
        self._resolver = entity_lookup or EntityLookup(query_engine=self._qe)
        self._logger = get_logger(__name__)

    @property
    def resolver(self) -> EntityLookup:
        return self._resolver

    @property
    def _reverse_column_map(self) -> dict[str, str]:
        return {v: k for k, v in self._COLUMN_MAP.items()}

    def _storage_col(self, presentation_col: str) -> str:
        """Traduz nome de apresentacao para storage. Retorna original se nao mapeado."""
        return self._reverse_column_map.get(presentation_col, presentation_col)

    def _apply_column_mapping(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self._COLUMN_MAP:
            return df
        rename_map = {k: v for k, v in self._COLUMN_MAP.items() if k in df.columns}
        return df.rename(columns=rename_map) if rename_map else df

    def _read_glob(
        self,
        pattern: str,
        subdir: str,
        columns: list[str] | None = None,
        where: str | None = None,
    ) -> pd.DataFrame:
        """Le parquets via DuckDB com dedup, datetime e exclude automaticos."""
        date_alias = "DATA"
        if self._DATE_COLUMN and self._DATE_COLUMN in self._COLUMN_MAP:
            date_alias = self._COLUMN_MAP[self._DATE_COLUMN]

        return self._qe.read_glob(
            pattern=pattern,
            subdir=subdir,
            columns=columns,
            where=where,
            distinct=True,
            date_column=self._DATE_COLUMN,
            date_alias=date_alias,
            exclude_columns=self._DROP_COLUMNS if not columns else None,
        )

    @abstractmethod
    def _get_subdir(self) -> str: ...

    @abstractmethod
    def _get_file_prefix(self) -> str: ...

    def _get_pattern(self) -> str:
        """Pattern glob para arquivos parquet. Override para multi-source."""
        return f"{self._get_file_prefix()}_*.parquet"

    def _get_sources(self) -> dict[str, dict[str, str]]:
        """
        Retorna fontes de dados do explorer.

        Override para multiplas fontes (mesmo schema).
        Default: fonte unica derivada de _get_subdir/_get_file_prefix.

        Retorna dict no formato:
            {"nome_fonte": {"subdir": "...", "prefix": "..."}}
        """
        return {
            "default": {
                "subdir": self._get_subdir(),
                "prefix": self._get_file_prefix(),
            }
        }

    def _ensure_data_exists(
        self,
        pattern: str | None = None,
        subdir: str | None = None,
    ) -> bool:
        """Retorna True se existem arquivos parquet para o pattern."""
        pattern = pattern or self._get_pattern()
        subdir = subdir or self._get_subdir()
        return self._qe.has_glob(pattern, subdir)

    @staticmethod
    def _align_to_quarter_end(yyyymm: int) -> int:
        """Alinha YYYYMM para o fim do trimestre correspondente (03, 06, 09, 12)."""
        from ifdata_bcb.utils.date import align_to_quarter_end

        return align_to_quarter_end(yyyymm)

    def _normalize_datas(self, datas: DateInput) -> list[int]:
        """Aceita int, str, ou lista. Formatos: 202412, '202412', '2024-12'."""
        result = NormalizedDates(values=datas).values
        self._logger.debug(f"Dates: {datas} -> {result}")
        return result

    def _normalize_contas(self, contas: AccountInput | None) -> list[str] | None:
        if contas is None:
            return None
        return AccountList(values=contas).values

    def _normalize_instituicoes(
        self, instituicoes: InstitutionInput | None
    ) -> list[str] | None:
        if instituicoes is None:
            return None
        return InstitutionList(values=instituicoes).values

    def _resolve_date_range(
        self,
        start: str | None,
        end: str | None,
        trimestral: bool = False,
    ) -> list[int] | None:
        """
        start sozinho: data unica. start + end: range. None: todos periodos.

        Excecoes:
            InvalidDateRangeError: Se start > end.
        """
        if start is None:
            return None
        start_normalized = self._normalize_datas(start)[0]
        if end is None:
            if trimestral:
                return [self._align_to_quarter_end(start_normalized)]
            return [start_normalized]
        end_normalized = self._normalize_datas(end)[0]
        if start_normalized > end_normalized:
            raise InvalidDateRangeError(start, end)
        from ifdata_bcb.utils.date import (
            generate_month_range,
            generate_quarter_range,
        )

        if trimestral:
            return generate_quarter_range(start, end)
        return generate_month_range(start, end)

    def _resolve_entidade(self, identificador: str) -> str:
        """
        Valida CNPJ de 8 digitos.

        Excecoes:
            InvalidIdentifierError: Se nao for CNPJ de 8 digitos.
        """
        validated = ValidatedCnpj8(value=identificador).value
        self._logger.debug(f"Entity validated: {validated}")
        return validated

    def _validate_required_params(
        self,
        start: str | None,
    ) -> None:
        if start is None:
            raise MissingRequiredParameterError("start")

    def _validate_escopo(self, escopo: str) -> str:
        """Valida e normaliza nome de escopo."""
        escopo_lower = escopo.lower()
        if self._VALID_ESCOPOS and escopo_lower not in self._VALID_ESCOPOS:
            raise InvalidScopeError("escopo", escopo, self._VALID_ESCOPOS)
        return escopo_lower

    def _translate_columns(self, columns: list[str] | None) -> list[str] | None:
        """Traduz nomes de apresentacao para storage. Aceita ambos."""
        if columns is None:
            return None
        return [self._storage_col(c) for c in columns]

    def _storage_columns_for_query(
        self,
        columns: list[str] | None,
        required: list[str] | None = None,
    ) -> list[str] | None:
        """Traduz colunas para storage, filtrando derivadas e garantindo required.

        Usado nos explorers antes de chamar read_glob(). Derivadas (adicionadas
        pos-query por Python) sao removidas; colunas em ``required`` sao
        incluidas mesmo que o usuario nao tenha pedido.
        """
        if columns is None:
            return None
        non_derived = [c for c in columns if c not in self._DERIVED_COLUMNS]
        storage = self._translate_columns(non_derived) if non_derived else []
        if required:
            for col in required:
                if col not in storage:
                    storage.append(col)
        return storage if storage else None

    def _validate_columns(self, columns: list[str] | None) -> list[str] | None:
        """Valida nomes de colunas contra o conjunto conhecido. Chamado cedo no read().

        Retorna columns normalizado: lista vazia e convertida para None
        (com warning) para que o restante do pipeline trate como 'sem filtro'.
        """
        if columns is None:
            return None
        if not columns:
            emit_user_warning(
                EmptyFilterWarning(
                    "columns=[] passado como filtro vazio. "
                    "Use columns=None para retornar todas as colunas.",
                    parameter="columns",
                ),
                stacklevel=3,
            )
            return None
        all_known = (
            set(self._COLUMN_MAP.keys())
            | set(self._COLUMN_MAP.values())
            | self._DERIVED_COLUMNS
            | self._PASSTHROUGH_COLUMNS
        )
        unknown = set(columns) - all_known
        if unknown:
            raise InvalidScopeError(
                "columns",
                str(sorted(unknown)),
                sorted(all_known),
            )
        return columns

    def _filter_columns(
        self,
        df: pd.DataFrame,
        columns: list[str] | None,
    ) -> pd.DataFrame:
        """Filtra DataFrame para conter apenas as colunas solicitadas."""
        if columns is None or df.empty:
            return df

        final_cols = []
        for col in columns:
            if col in df.columns:
                final_cols.append(col)
            elif col in self._COLUMN_MAP and self._COLUMN_MAP[col] in df.columns:
                final_cols.append(self._COLUMN_MAP[col])
            elif col in self._reverse_column_map:
                storage = self._reverse_column_map[col]
                if storage in df.columns:
                    final_cols.append(storage)

        return df[final_cols] if final_cols else df

    def _build_date_condition(
        self,
        start: str | None,
        end: str | None,
        trimestral: bool = False,
    ) -> str | None:
        """Constroi condicao WHERE para range de datas. Usa nome de storage."""
        datas = self._resolve_date_range(start, end, trimestral=trimestral)
        if not datas:
            return None
        data_col = self._storage_col("DATA")
        return build_int_condition(data_col, datas)

    def _build_cnpj_condition(
        self,
        instituicoes: InstitutionInput | None,
        column: str = "CNPJ_8",
    ) -> str | None:
        """Constroi condicao WHERE para CNPJs."""
        cnpjs = self._normalize_instituicoes(instituicoes)
        if not cnpjs:
            return None
        return build_string_condition(column, cnpjs)

    def _finalize_read(self, df: pd.DataFrame) -> pd.DataFrame:
        """Post-DuckDB: rename -> sort -> reorder.

        Dedup e datetime conversion sao feitos pelo DuckDB via _read_glob.
        Drop de colunas internas e feito via EXCLUDE no SQL.
        """
        # 1. Drop colunas internas (fallback para colunas que passaram pelo SQL)
        drop_cols = [c for c in self._DROP_COLUMNS if c in df.columns]
        if drop_cols:
            df = df.drop(columns=drop_cols)

        # 2. Mapeamento de colunas (rename storage -> canonico)
        df = self._apply_column_mapping(df)

        if df.empty:
            return df

        # 3. Sort por DATA (pandas e 40x mais rapido que DuckDB ORDER BY)
        if "DATA" in df.columns:
            df = df.sort_values("DATA", ascending=True).reset_index(drop=True)

        # 4. Reordenar colunas (se _COLUMN_ORDER definido)
        if self._COLUMN_ORDER:
            existing = [c for c in self._COLUMN_ORDER if c in df.columns]
            remaining = [c for c in df.columns if c not in existing]
            df = df[existing + remaining]

        return df

    def _check_null_value_instituicoes(self, df: pd.DataFrame) -> None:
        """Emite warning para instituicoes com todos os VALOR NULL."""
        if df.empty or "VALOR" not in df.columns or "CNPJ_8" not in df.columns:
            return

        has_value = set(df.loc[df["VALOR"].notna(), "CNPJ_8"].unique())
        all_null_cnpjs = sorted(
            str(c) for c in df["CNPJ_8"].unique() if c not in has_value
        )
        if not all_null_cnpjs:
            return

        nomes = self._resolver.get_canonical_names_for_cnpjs(all_null_cnpjs)
        entities = []
        for cnpj in all_null_cnpjs:
            nome = nomes.get(cnpj, "")
            label = f"{cnpj} ({nome})" if nome else cnpj
            entities.append(label)

        if len(entities) <= 5:
            entity_str = ", ".join(entities)
        else:
            entity_str = f"{len(entities)} entidades"
        emit_user_warning(
            NullValuesWarning(
                f"Dados com VALOR inteiramente NULL para {entity_str}. "
                f"O BCB registrou a entidade mas nao forneceu valores financeiros.",
                entities=all_null_cnpjs,
            ),
            stacklevel=4,
        )

    def _apply_canonical_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aplica nomes canonicos do cadastro a coluna INSTITUICAO.

        So atua quando INSTITUICAO nao existe no DataFrame (ex: IFDATA bulk
        individual onde CNPJ_8 vem de CodInst e nao ha nome no parquet).
        Quando INSTITUICAO ja existe (ex: COSIF tem NOME_INSTITUICAO), pula
        o lookup pois o parquet ja tem os nomes corretos.
        """
        if df.empty or "CNPJ_8" not in df.columns:
            return df

        if "INSTITUICAO" in df.columns:
            return df

        cnpjs = df["CNPJ_8"].dropna().astype(str).unique().tolist()
        if not cnpjs:
            return df

        nomes = self._resolver.get_canonical_names_for_cnpjs(cnpjs)
        df["INSTITUICAO"] = df["CNPJ_8"].astype(str).map(nomes)

        return df

    def _get_latest_periodo(self, source: str | None = None) -> int | None:
        """Retorna o periodo mais recente disponivel, ou None."""
        periods = self.list_periodos(source)
        return periods[-1] if periods else None

    def _list_periodos_for_source(self, subdir: str, prefix: str) -> list[int]:
        """Lista periodos de uma fonte especifica."""
        files = list_parquet_files(subdir, base_path=self._qe.cache_path)
        periods = []
        for f in files:
            if f.startswith(prefix + "_"):
                try:
                    period_str = f.replace(prefix + "_", "")
                    periods.append(int(period_str))
                except ValueError:
                    continue
        return periods

    def list_periodos(self, source: str | None = None) -> list[int]:
        """
        Lista periodos disponiveis.

        Args:
            source: Nome da fonte (para multi-source). Se None, retorna uniao de todas.
        """
        sources = self._get_sources()

        if source:
            cfg = sources[source]
            return sorted(self._list_periodos_for_source(cfg["subdir"], cfg["prefix"]))

        all_periods: set[int] = set()
        for cfg in sources.values():
            all_periods.update(
                self._list_periodos_for_source(cfg["subdir"], cfg["prefix"])
            )
        return sorted(all_periods)

    def has_data(self, source: str | None = None) -> bool:
        """Verifica se ha dados disponiveis."""
        return len(self.list_periodos(source)) > 0

    def describe(self, source: str | None = None) -> dict:
        """
        Retorna info do explorer.

        Args:
            source: Nome da fonte (para multi-source). Se None, descreve todas.
        """
        sources = self._get_sources()

        if source:
            cfg = sources[source]
            periods = self.list_periodos(source)
            return {
                "source": source,
                "subdir": cfg["subdir"],
                "prefix": cfg["prefix"],
                "periods": periods,
                "period_count": len(periods),
                "has_data": len(periods) > 0,
                "first_period": periods[0] if periods else None,
                "last_period": periods[-1] if periods else None,
            }

        all_periods = self.list_periodos()
        by_source: dict[str, dict] = {}
        result: dict = {
            "sources": list(sources.keys()),
            "periods": all_periods,
            "period_count": len(all_periods),
            "has_data": len(all_periods) > 0,
            "first_period": all_periods[0] if all_periods else None,
            "last_period": all_periods[-1] if all_periods else None,
            "by_source": by_source,
        }

        for name, cfg in sources.items():
            periods = self.list_periodos(name)
            by_source[name] = {
                "subdir": cfg["subdir"],
                "prefix": cfg["prefix"],
                "period_count": len(periods),
                "has_data": len(periods) > 0,
            }

        return result

    def _diagnose_empty_result(
        self,
        source_name: str,
        has_files: bool,
        had_conta_filter: bool,
        had_institution_filter: bool = True,
    ) -> None:
        """Cascata de diagnostico quando read() retorna vazio."""
        if not has_files:
            emit_user_warning(
                PartialDataWarning(
                    f"Nenhum arquivo {source_name} encontrado no cache. "
                    f"Execute {source_name.lower()}.collect() para baixar os dados.",
                    reason="no_files",
                ),
                stacklevel=3,
            )
            return

        if had_conta_filter:
            emit_user_warning(
                PartialDataWarning(
                    f"Filtro de conta nao encontrou resultados em {source_name}. "
                    f"Verifique se o codigo/nome da conta corresponde ao periodo "
                    f"solicitado (codigos mudam entre eras do BCB).",
                    reason="conta_not_found",
                ),
                stacklevel=3,
            )
            return

        if had_institution_filter:
            msg = (
                f"Nenhum dado {source_name} encontrado para os parametros "
                f"solicitados. Verifique se os dados foram coletados e se "
                f"os filtros (periodo, instituicao) estao corretos."
            )
        else:
            msg = (
                f"Nenhum dado {source_name} encontrado para os filtros "
                f"solicitados (periodo, escopo, conta, etc)."
            )

        emit_user_warning(
            PartialDataWarning(msg, reason="empty_result"),
            stacklevel=3,
        )
