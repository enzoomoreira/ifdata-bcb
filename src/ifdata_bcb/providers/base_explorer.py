"""Classe base abstrata para Explorers de dados do BCB."""

import inspect
from abc import ABC, abstractmethod
from typing import ClassVar

import pandas as pd

from ifdata_bcb.core.entity import EntityLookup
from ifdata_bcb.core.eras import EraDiagnostic, diagnose_eras, emit_era_warnings
from ifdata_bcb.domain.exceptions import (
    EmptyFilterWarning,
    InvalidColumnError,
    InvalidDateRangeError,
    InvalidScopeError,
    MissingRequiredParameterError,
    NullValuesWarning,
    PartialDataWarning,
    TruncatedResultWarning,
)
from ifdata_bcb.domain.types import (
    AccountInput,
    DateInput,
    DateScalar,
    EscopoInfo,
    ExplorerInfo,
    InstitutionInput,
)
from ifdata_bcb.domain.validation import (
    normalize_accounts,
    normalize_dates,
    normalize_institutions,
    validate_cnpj8,
)
from ifdata_bcb.infra.log import emit_user_warning, get_logger
from ifdata_bcb.infra.query import QueryEngine
from ifdata_bcb.infra.sql import (
    SqlCondition,
    build_int_condition,
    build_string_condition,
    join_conditions,
    merge_params,
)
from ifdata_bcb.infra.storage import list_parquet_files
from ifdata_bcb.utils.text import format_entity_labels


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
    - list_periodos(), has_data() e describe() aceitam escopo; explorers com
      escopos implementam _periodos_por_escopo() para responder por ele

    Metodos read() e collect() tem assinaturas especificas por provider,
    portanto nao sao declarados na base.

    Configuracao por class attributes:
    - _COLUMN_MAP: Mapeamento de colunas storage -> apresentacao
    - _DERIVED_COLUMNS: Colunas adicionadas pos-query por Python
    - _DROP_COLUMNS: Colunas a remover antes do mapeamento
    - _COLUMN_ORDER: Ordem desejada das colunas no output
    - _VALID_ESCOPOS: Lista de escopos validos para _validate_escopo
    - _ERA_BOUNDARY: Periodo da mudanca de plano contabil (None desliga a checagem)
    - _ERA_GROUP_COLUMN: Coluna que agrupa contas para a analise de era
    """

    _COLUMN_MAP: ClassVar[dict[str, str]] = {}
    _DERIVED_COLUMNS: ClassVar[set[str]] = set()
    _DROP_COLUMNS: ClassVar[list[str]] = []
    _COLUMN_ORDER: ClassVar[list[str]] = []
    _VALID_ESCOPOS: ClassVar[list[str]] = []
    _DATE_COLUMN: str | None = None
    _ERA_BOUNDARY: int | None = None
    _ERA_GROUP_COLUMN: str | None = None
    _ERA_SOURCE_NAME: str = ""
    _TRIMESTRAL: bool = False

    # list_values() infrastructure -- overridden by subclasses
    _LIST_COLUMNS: ClassVar[dict[str, str]] = {}
    _BLOCKED_COLUMNS: ClassVar[dict[str, str]] = {}

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
        date_alias = "data"
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
        return normalize_dates(datas)

    def _normalize_contas(self, contas: AccountInput | None) -> list[str] | None:
        if contas is None:
            return None
        return normalize_accounts(contas)

    def _normalize_instituicoes(
        self, instituicoes: InstitutionInput | None
    ) -> list[str] | None:
        if instituicoes is None:
            return None
        return normalize_institutions(instituicoes)

    def _resolve_date_range(
        self,
        start: DateScalar | None,
        end: DateScalar | None,
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
            # str() aqui e nao no tipo de InvalidDateRangeError: domain.exceptions
            # nao importa nada, e e isso que mantem o lazy loading dos explorers.
            raise InvalidDateRangeError(str(start), str(end))
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
        return validate_cnpj8(identificador)

    def _validate_required_params(
        self,
        start: DateScalar | None,
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
        storage: list[str] = []
        if non_derived:
            # _translate_columns so devolve None quando recebe None
            storage = self._translate_columns(non_derived) or []
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
        )
        unknown = sorted(set(columns) - all_known)
        if unknown:
            # list_values() ja usava InvalidColumnError para o mesmo erro; read()
            # usava InvalidScopeError e produzia "Escopo '['FOO']' invalido".
            extras = ""
            if len(unknown) > 1:
                outras = ", ".join(repr(c) for c in unknown[1:])
                extras = f"Tambem invalidas: {outras}."
            raise InvalidColumnError(unknown[0], sorted(all_known), extras)
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
        start: DateScalar | None,
        end: DateScalar | None,
        trimestral: bool = False,
    ) -> SqlCondition | None:
        """Constroi condicao WHERE para range de datas. Usa nome de storage."""
        datas = self._resolve_date_range(start, end, trimestral=trimestral)
        if not datas:
            return None
        data_col = self._storage_col("data")
        return build_int_condition(data_col, datas)

    def _build_cnpj_condition(
        self,
        instituicoes: InstitutionInput | None,
        column: str = "CNPJ_8",
    ) -> SqlCondition | None:
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

        # 3. Sort por data (pandas e 40x mais rapido que DuckDB ORDER BY)
        if "data" in df.columns:
            df = df.sort_values("data", ascending=True).reset_index(drop=True)

        # 4. Reordenar colunas (se _COLUMN_ORDER definido)
        if self._COLUMN_ORDER:
            existing = [c for c in self._COLUMN_ORDER if c in df.columns]
            remaining = [c for c in df.columns if c not in existing]
            df = df[existing + remaining]

        return df

    def _era_required_columns(self, periodos: list[int] | None) -> list[str]:
        """Colunas de dimensao a forcar na query para viabilizar a analise de era.

        `columns=` projeta na propria query, entao um read(columns=['data','valor'])
        -- justamente quem esta montando serie temporal -- nao teria como detectar
        a renumeracao. Sao lidas apenas quando o range cruza o boundary; no caso
        comum o custo e zero.
        """
        if self._ERA_BOUNDARY is None or not periodos:
            return []
        cruza = any(p < self._ERA_BOUNDARY for p in periodos) and any(
            p >= self._ERA_BOUNDARY for p in periodos
        )
        if not cruza:
            return []
        cols = [self._storage_col("cod_conta")]
        if self._ERA_GROUP_COLUMN:
            cols.append(self._storage_col(self._ERA_GROUP_COLUMN))
        return cols

    def _check_eras(
        self,
        df: pd.DataFrame,
        periodos: list[int] | None,
        *,
        escopo: str | None = None,
    ) -> EraDiagnostic | None:
        """Analisa continuidade dos dados atraves do boundary de era e avisa.

        Roda apos _finalize_read e antes de _filter_columns -- precisa das
        colunas de dimensao, que o filtro de columns pode remover.
        """
        if self._ERA_BOUNDARY is None:
            return None

        diag = diagnose_eras(
            df,
            boundary=self._ERA_BOUNDARY,
            source=self._ERA_SOURCE_NAME,
            periodos_solicitados=periodos,
            group_col=self._ERA_GROUP_COLUMN,
            escopo=escopo,
        )
        emit_era_warnings(diag, stacklevel=4)
        return diag

    def check_era(
        self,
        start: DateScalar,
        end: DateScalar | None = None,
        *,
        escopo: str | None = None,
    ) -> EraDiagnostic:
        """Diagnostico de continuidade entre eras, sem trazer os valores.

        Le apenas as colunas de dimensao (data, codigo de conta e o grupo da
        fonte). Util para decidir como montar a query antes de puxar os dados,
        ou para recuperar o diagnostico quando o DataFrame ja perdeu `attrs`
        no caminho. O ganho de tempo sobre um read() completo e modesto -- a
        leitura do parquet domina o custo.

        Emite os mesmos warnings de read(): o retorno estruturado e um canal
        adicional, nao um substituto silencioso.

        Args:
            start: Periodo inicial. Formato: '2024-12' ou '202412'.
            end: Periodo final. Se None, apenas start.
            escopo: Filtro de escopo, necessario para detectar migracao.

        Raises:
            NotImplementedError: Se o explorer nao declara boundary de era.
        """
        if self._ERA_BOUNDARY is None:
            raise NotImplementedError(
                f"{type(self).__name__} nao tem transicao de era conhecida."
            )

        cols = ["data", "cod_conta"]
        if self._ERA_GROUP_COLUMN:
            cols.append(self._ERA_GROUP_COLUMN)

        df = self.read(start, end, escopo=escopo, columns=cols)  # type: ignore[attr-defined]
        diag = df.attrs.get("era")
        if diag is not None:
            return diag

        # read() vazio nao passa pela analise -- devolve a estrutura assim mesmo
        return diagnose_eras(
            df,
            boundary=self._ERA_BOUNDARY,
            source=self._ERA_SOURCE_NAME,
            periodos_solicitados=self._resolve_date_range(
                start, end, trimestral=self._TRIMESTRAL
            ),
            group_col=self._ERA_GROUP_COLUMN,
            escopo=escopo,
        )

    def _check_null_value_instituicoes(self, df: pd.DataFrame) -> None:
        """Emite warning para instituicoes com todos os valores NULL."""
        if df.empty or "valor" not in df.columns or "cnpj_8" not in df.columns:
            return

        has_value = set(df.loc[df["valor"].notna(), "cnpj_8"].unique())
        all_null_cnpjs = sorted(
            str(c) for c in df["cnpj_8"].unique() if c not in has_value
        )
        if not all_null_cnpjs:
            return

        nomes = self._resolver.get_canonical_names_for_cnpjs(all_null_cnpjs)
        entity_str = format_entity_labels(all_null_cnpjs, nomes)
        emit_user_warning(
            NullValuesWarning(
                f"Dados com valor inteiramente NULL para {entity_str}. "
                f"O BCB registrou a entidade mas nao forneceu valores financeiros.",
                entities=all_null_cnpjs,
            ),
            stacklevel=4,
        )

    def _apply_canonical_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aplica nomes canonicos do cadastro a coluna instituicao.

        So atua quando instituicao nao existe no DataFrame (ex: IFDATA bulk
        individual onde cnpj_8 vem de CodInst e nao ha nome no parquet).
        Quando instituicao ja existe (ex: COSIF tem NOME_INSTITUICAO), pula
        o lookup pois o parquet ja tem os nomes corretos.
        """
        if df.empty or "cnpj_8" not in df.columns:
            return df

        if "instituicao" in df.columns:
            return df

        cnpjs = df["cnpj_8"].dropna().astype(str).unique().tolist()
        if not cnpjs:
            return df

        nomes = self._resolver.get_canonical_names_for_cnpjs(cnpjs)
        df["instituicao"] = df["cnpj_8"].astype(str).map(nomes)

        return df

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

    def _require_escopo(self, escopo: str) -> str:
        """Valida escopo em list_periodos()/has_data()/describe().

        _validate_escopo aceita qualquer valor quando _VALID_ESCOPOS e vazio;
        aqui isso e erro: um explorer sem escopos nao tem o que filtrar.
        """
        if not self._VALID_ESCOPOS:
            raise InvalidScopeError(
                "escopo",
                escopo,
                [],
                hint=f"{type(self).__name__} nao tem escopos; chame sem escopo.",
            )
        return self._validate_escopo(escopo)

    def _periodos_por_escopo(self) -> dict[str, list[int]]:
        """Mapa escopo -> periodos disponiveis. Override onde ha escopos."""
        return {}

    def list_periodos(self, escopo: str | None = None) -> list[int]:
        """
        Lista periodos disponiveis.

        Args:
            escopo: Filtra pelos periodos com dados desse escopo. Se None,
                retorna a uniao de todos.

        Raises:
            InvalidScopeError: Se escopo nao for valido para o explorer.
        """
        if escopo is not None:
            validado = self._require_escopo(escopo)
            return sorted(self._periodos_por_escopo().get(validado, []))

        all_periods: set[int] = set()
        for cfg in self._get_sources().values():
            all_periods.update(
                self._list_periodos_for_source(cfg["subdir"], cfg["prefix"])
            )
        return sorted(all_periods)

    def has_data(self, escopo: str | None = None) -> bool:
        """Verifica se ha dados disponiveis."""
        return len(self.list_periodos(escopo)) > 0

    def _read_signature_info(self) -> tuple[list[str], list[str]]:
        """Filtros aceitos por read() e colunas validas em cadastro=.

        Lido da assinatura em vez de declarado a mao: uma lista paralela
        envelheceria em silencio no primeiro parametro novo, e describe() e
        justamente onde um agente vai confiar para montar a chamada seguinte.
        """
        read = getattr(type(self), "read", None)
        if read is None:
            return [], []

        keyword_only = [
            name
            for name, p in inspect.signature(read).parameters.items()
            if p.kind is inspect.Parameter.KEYWORD_ONLY
        ]
        # columns e cadastro nao filtram linha; saem em chaves proprias.
        filtros = sorted(n for n in keyword_only if n not in ("columns", "cadastro"))

        cadastro_columns: list[str] = []
        if "cadastro" in keyword_only:
            from ifdata_bcb.providers.enrichment import VALID_CADASTRO_COLUMNS

            cadastro_columns = sorted(VALID_CADASTRO_COLUMNS)
        return filtros, cadastro_columns

    def _describe_capabilities(self) -> ExplorerInfo:
        """Parte de describe() que nao depende de quais periodos ha em disco."""
        filtros, cadastro_columns = self._read_signature_info()
        return {
            "escopos": list(self._VALID_ESCOPOS),
            "columns": sorted(self._LIST_COLUMNS.keys()),
            "read_columns": list(self._COLUMN_ORDER),
            "filtros": filtros,
            "cadastro_columns": cadastro_columns,
        }

    def describe(self, escopo: str | None = None) -> ExplorerInfo:
        """
        Retorna o que o explorer aceita e o que ha coletado.

        Alem dos periodos em disco, descreve a superficie de chamada: escopos
        validos, colunas listaveis por list_values(), colunas devolvidas por read(),
        filtros aceitos e colunas validas em cadastro=. E o suficiente para
        montar uma chamada a read() sem ler a documentacao.

        Args:
            escopo: Restringe os periodos a um escopo. Se None, descreve todos
                (com resumo por escopo em by_escopo, quando o explorer tem escopos).

        Raises:
            InvalidScopeError: Se escopo nao for valido para o explorer.
        """
        result: ExplorerInfo = self._describe_capabilities()

        if escopo is not None:
            validado = self._require_escopo(escopo)
            periods = sorted(self._periodos_por_escopo().get(validado, []))
            result["escopo"] = validado
        else:
            periods = self.list_periodos()
            if self._VALID_ESCOPOS:
                por_escopo = self._periodos_por_escopo()
                by_escopo: dict[str, EscopoInfo] = {}
                for esc in self._VALID_ESCOPOS:
                    esc_periods = por_escopo.get(esc, [])
                    by_escopo[esc] = {
                        "period_count": len(esc_periods),
                        "has_data": len(esc_periods) > 0,
                    }
                result["by_escopo"] = by_escopo

        result["periods"] = periods
        result["period_count"] = len(periods)
        result["has_data"] = len(periods) > 0
        result["first_period"] = periods[0] if periods else None
        result["last_period"] = periods[-1] if periods else None
        return result

    # ------------------------------------------------------------------
    # list_values() generic infrastructure
    # ------------------------------------------------------------------

    def _validate_list_columns(self, columns: list[str]) -> None:
        """Valida colunas para list_values(). Levanta erro ou warning conforme o caso."""
        if not columns:
            raise ValueError("columns deve conter pelo menos uma coluna.")

        blocked_found: list[str] = []
        for col in columns:
            col_lower = col.lower()
            if col_lower in self._BLOCKED_COLUMNS:
                blocked_found.append(col_lower)

        if blocked_found:
            for col_name in blocked_found:
                emit_user_warning(
                    UserWarning(self._BLOCKED_COLUMNS[col_name]),
                    stacklevel=4,
                )
            return

        valid_names = sorted(self._LIST_COLUMNS.keys())
        extras = (
            "Para contas: use list_contas(). Para instituicoes: use cadastro.search()."
        )
        for col in columns:
            col_lower = col.lower()
            if col_lower not in self._LIST_COLUMNS:
                raise InvalidColumnError(col, valid_names, extras)

    def _has_blocked_columns(self, columns: list[str]) -> bool:
        """Retorna True se alguma coluna esta bloqueada."""
        return any(col.lower() in self._BLOCKED_COLUMNS for col in columns)

    def _base_list(
        self,
        columns: list[str],
        *,
        start: DateScalar | None = None,
        end: DateScalar | None = None,
        limit: int = 100,
        **filters: object,
    ) -> pd.DataFrame:
        """Implementacao base de list_values(). Chamado pelas subclasses."""
        if limit <= 0:
            raise ValueError(f"limit deve ser > 0, recebido: {limit}")

        self._validate_list_columns(columns)

        # Se coluna bloqueada, retornar DataFrame vazio com colunas solicitadas
        if self._has_blocked_columns(columns):
            return pd.DataFrame(columns=[c.lower() for c in columns])

        # Montar SELECT com expressoes SQL dos _LIST_COLUMNS
        select_parts: list[str] = []
        canonical_names: list[str] = []
        for col in columns:
            col_lower = col.lower()
            canonical_names.append(col_lower)
            expr = self._LIST_COLUMNS[col_lower]
            if col_lower == "data":
                select_parts.append(QueryEngine._date_sql_expr(expr, "data"))
            else:
                select_parts.append(f'{expr} AS "{col_lower}"')

        select_clause = ", ".join(select_parts)
        from_expr = self._get_list_source(
            columns=canonical_names,
            start=start,
            end=end,
            **filters,
        )

        if from_expr is None:
            return pd.DataFrame(columns=canonical_names)

        conditions = self._build_list_conditions(start=start, end=end, **filters)
        where = join_conditions(conditions)
        where_clause = f"WHERE {where}" if where else ""

        order_cols = ", ".join(str(i + 1) for i in range(len(select_parts)))

        query = (
            f"SELECT DISTINCT {select_clause} "
            f"FROM {from_expr} "
            f"{where_clause} "
            f"ORDER BY {order_cols} "
            f"LIMIT {limit}"
        )

        df = self._qe.sql(query, params=merge_params(where))

        if df.empty:
            return pd.DataFrame(columns=canonical_names)

        # Truncation warning
        if len(df) == limit:
            extra_hints: list[str] = []
            if "municipio" in canonical_names:
                extra_hints.append("Filtre com uf='...' para reduzir.")
            if "data" in canonical_names:
                extra_hints.append("Use start=/end= para filtrar periodo.")
            msg = f"Resultado truncado em {limit}. Aumente limit= ou adicione filtros."
            if extra_hints:
                msg += " " + " ".join(extra_hints)
            emit_user_warning(
                TruncatedResultWarning(msg, limit=limit),
                stacklevel=3,
            )

        return df

    def _get_list_source(
        self,
        columns: list[str],
        start: DateScalar | None = None,
        end: DateScalar | None = None,
        **filters: object,
    ) -> str | None:
        """FROM SQL para list_values(). Override em subclasses multi-source."""
        return self._get_list_path()

    def _get_list_path(self) -> str | None:
        """FROM SQL para list_values(). Default: glob do provider."""
        if not self._ensure_data_exists():
            return None
        path = self._qe.cache_path / self._get_subdir() / self._get_pattern()
        return f"read_parquet('{path}', union_by_name=true)"

    def _build_list_conditions(
        self,
        start: DateScalar | None = None,
        end: DateScalar | None = None,
        **filters: object,
    ) -> list[str | None]:
        """WHERE clauses para list_values(). Override em subclasses."""
        return []

    def _diagnose_empty_result(
        self,
        source_name: str,
        has_files: bool,
        had_conta_filter: bool,
        had_institution_filter: bool = True,
        outros_filtros: str = "periodo, escopo, conta, etc",
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
                f"solicitados ({outros_filtros})."
            )

        emit_user_warning(
            PartialDataWarning(msg, reason="empty_result"),
            stacklevel=3,
        )
