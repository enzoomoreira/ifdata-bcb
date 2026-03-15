from abc import ABC, abstractmethod
from collections.abc import Sequence

import pandas as pd

from ifdata_bcb.core.entity_lookup import EntityLookup
from ifdata_bcb.domain.exceptions import (
    InvalidDateRangeError,
    InvalidScopeError,
    MissingRequiredParameterError,
)
from ifdata_bcb.domain.types import DateInput, AccountInput, InstitutionInput
from ifdata_bcb.domain.validation import (
    AccountList,
    InstitutionList,
    NormalizedDates,
    ValidatedCnpj8,
)
from ifdata_bcb.infra.log import get_logger
from ifdata_bcb.infra.query import QueryEngine
from ifdata_bcb.infra.storage import list_parquet_files
from ifdata_bcb.utils.text import normalize_accents


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
    - Metodos list_periods(), has_data(), describe() suportam parametro source

    Metodos read() e collect() tem assinaturas especificas por provider,
    portanto nao sao declarados na base.

    Mapeamento de colunas (storage -> apresentacao):
    - Subclasses podem definir _COLUMN_MAP para mapear nomes de storage
      para nomes de apresentacao
    - Exemplo: _COLUMN_MAP = {"DATA_BASE": "DATA", "NOME_INSTITUICAO": "INSTITUICAO"}
    """

    # Mapeamento de colunas: nome_storage -> nome_apresentacao
    # Subclasses devem sobrescrever com seu mapeamento especifico
    _COLUMN_MAP: dict[str, str] = {}

    # Colunas cadastrais validas para enriquecimento via parametro cadastro=
    # Derivado de CadastroExplorer._COLUMN_MAP (excluindo DATA, CNPJ_8, INSTITUICAO)
    _VALID_CADASTRO_COLUMNS = {
        "SEGMENTO",
        "COD_CONGL_PRUD",
        "COD_CONGL_FIN",
        "SITUACAO",
        "ATIVIDADE",
        "TCB",
        "TD",
        "TC",
        "UF",
        "MUNICIPIO",
        "SR",
        "DATA_INICIO_ATIVIDADE",
    }

    def __init__(
        self,
        query_engine: QueryEngine | None = None,
        entity_lookup: EntityLookup | None = None,
    ):
        self._qe = query_engine or QueryEngine()
        self._resolver = entity_lookup or EntityLookup(query_engine=self._qe)
        self._logger = get_logger(__name__)

    @property
    def query_engine(self) -> QueryEngine:
        return self._qe

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

    @abstractmethod
    def _get_subdir(self) -> str: ...

    @abstractmethod
    def _get_file_prefix(self) -> str: ...

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

    @staticmethod
    def _align_to_quarter_end(yyyymm: int) -> int:
        """Alinha YYYYMM para o fim do trimestre correspondente (03, 06, 09, 12)."""
        year, month = divmod(yyyymm, 100)
        quarter_month = ((month - 1) // 3 + 1) * 3
        return year * 100 + quarter_month

    def _normalize_dates(self, datas: DateInput) -> list[int]:
        """Aceita int, str, ou lista. Formatos: 202412, '202412', '2024-12'."""
        result = NormalizedDates(values=datas).values
        self._logger.debug(f"Dates: {datas} -> {result}")
        return result

    def _normalize_accounts(self, contas: AccountInput | None) -> list[str] | None:
        if contas is None:
            return None
        return AccountList(values=contas).values

    def _normalize_institutions(
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
        # Nenhum filtro de data
        if start is None:
            return None

        # Normalizar start para formato YYYYMM
        start_normalized = self._normalize_dates(start)[0]

        # Data unica (apenas start)
        if end is None:
            if trimestral:
                return [self._align_to_quarter_end(start_normalized)]
            return [start_normalized]

        # Normalizar end e validar range
        end_normalized = self._normalize_dates(end)[0]
        if start_normalized > end_normalized:
            raise InvalidDateRangeError(start, end)

        # Range de datas (start + end)
        from ifdata_bcb.utils.date import (
            generate_month_range,
            generate_quarter_range,
        )

        if trimestral:
            return generate_quarter_range(start, end)
        return generate_month_range(start, end)

    def _resolve_entity(self, identificador: str) -> str:
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
        instituicao: InstitutionInput | None,
        start: str | None,
    ) -> None:
        if instituicao is None:
            raise MissingRequiredParameterError("instituicao")
        if start is None:
            raise MissingRequiredParameterError("start")

    def _validate_cadastro_columns(self, cadastro: list[str] | None) -> None:
        """Valida nomes de colunas cadastrais antes de executar qualquer query."""
        if cadastro is None:
            return
        invalid = set(cadastro) - self._VALID_CADASTRO_COLUMNS
        if invalid:
            raise InvalidScopeError(
                "cadastro",
                str(sorted(invalid)),
                sorted(self._VALID_CADASTRO_COLUMNS),
            )

    def _build_string_condition(
        self,
        column: str,
        values: list[str],
        case_insensitive: bool = False,
        accent_insensitive: bool = False,
    ) -> str:
        """Constroi condicao para valores string com escape de aspas."""
        escaped = [v.strip().replace("'", "''") for v in values]
        col_expr = column

        if accent_insensitive:
            col_expr = f"strip_accents({col_expr})"
            escaped = [normalize_accents(v) for v in escaped]

        if case_insensitive:
            col_expr = f"UPPER({col_expr})"
            escaped = [v.upper() for v in escaped]

        if len(escaped) == 1:
            return f"{col_expr} = '{escaped[0]}'"
        values_str = ", ".join(f"'{v}'" for v in escaped)
        return f"{col_expr} IN ({values_str})"

    def _build_account_condition(
        self,
        name_col: str,
        code_col: str,
        values: list[str],
    ) -> str:
        """Match por nome (accent/case insensitive) OU por codigo (cast to varchar)."""
        name_cond = self._build_string_condition(
            name_col,
            values,
            case_insensitive=True,
            accent_insensitive=True,
        )
        code_cond = self._build_string_condition(
            f"CAST({code_col} AS VARCHAR)",
            values,
            case_insensitive=True,
        )
        return f"({name_cond} OR {code_cond})"

    def _translate_columns(self, columns: list[str] | None) -> list[str] | None:
        """Traduz nomes de apresentacao para storage. Aceita ambos."""
        if columns is None:
            return None
        return [self._storage_col(c) for c in columns]

    def _build_int_condition(self, column: str, values: list[int]) -> str:
        """Constroi condicao para valores inteiros (datas, tipos, etc)."""
        if len(values) == 1:
            return f"{column} = {values[0]}"
        values_str = ", ".join(str(v) for v in values)
        return f"{column} IN ({values_str})"

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
        return self._build_int_condition(data_col, datas)

    def _build_cnpj_condition(
        self,
        instituicoes: InstitutionInput | None,
        column: str = "CNPJ_8",
    ) -> str | None:
        """Constroi condicao WHERE para CNPJs."""
        cnpjs = self._normalize_institutions(instituicoes)
        if not cnpjs:
            return None
        return self._build_string_condition(column, cnpjs)

    def _join_conditions(self, conditions: Sequence[str | None]) -> str | None:
        """Junta condicoes com AND, ignorando None."""
        valid = [c for c in conditions if c]
        return " AND ".join(valid) if valid else None

    def _finalize_read(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aplica mapeamento de colunas, converte DATA para datetime e ordena."""
        # Mapeamento de colunas funciona mesmo em DataFrames vazios
        df = self._apply_column_mapping(df)

        if df.empty:
            return df

        df = df.copy()
        df = df.drop_duplicates()

        if "DATA" in df.columns:
            df["DATA"] = pd.to_datetime(
                df["DATA"].astype(str), format="%Y%m"
            ) + pd.offsets.MonthEnd(0)
            df = df.sort_values("DATA", ascending=True).reset_index(drop=True)

        return df

    def _get_latest_period(self, source: str | None = None) -> int | None:
        """Retorna o periodo mais recente disponivel, ou None."""
        periods = self.list_periods(source)
        return periods[-1] if periods else None

    def _list_periods_for_source(self, subdir: str, prefix: str) -> list[int]:
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

    def list_periods(self, source: str | None = None) -> list[int]:
        """
        Lista periodos disponiveis.

        Args:
            source: Nome da fonte (para multi-source). Se None, retorna uniao de todas.
        """
        sources = self._get_sources()

        if source:
            cfg = sources[source]
            return sorted(self._list_periods_for_source(cfg["subdir"], cfg["prefix"]))

        all_periods: set[int] = set()
        for cfg in sources.values():
            all_periods.update(
                self._list_periods_for_source(cfg["subdir"], cfg["prefix"])
            )
        return sorted(all_periods)

    def has_data(self, source: str | None = None) -> bool:
        """Verifica se ha dados disponiveis."""
        return len(self.list_periods(source)) > 0

    def describe(self, source: str | None = None) -> dict:
        """
        Retorna info do explorer.

        Args:
            source: Nome da fonte (para multi-source). Se None, descreve todas.
        """
        sources = self._get_sources()

        if source:
            cfg = sources[source]
            periods = self.list_periods(source)
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

        # Multi-source: retorna info agregada + detalhes por fonte
        all_periods = self.list_periods()
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
            periods = self.list_periods(name)
            by_source[name] = {
                "subdir": cfg["subdir"],
                "prefix": cfg["prefix"],
                "period_count": len(periods),
                "has_data": len(periods) > 0,
            }

        return result

    def _enrich_with_cadastro(
        self,
        df: pd.DataFrame,
        cadastro_columns: list[str],
    ) -> pd.DataFrame:
        """Enriquece DataFrame financeiro com colunas cadastrais.

        Usa merge temporal backward-looking: cada linha financeira recebe
        os atributos cadastrais do trimestre mais recente <= sua data.
        """
        if df.empty:
            return df

        # Lazy-load CadastroExplorer (import local para evitar circular)
        if not hasattr(self, "_cadastro_explorer"):
            from ifdata_bcb.providers.ifdata.cadastro_explorer import CadastroExplorer

            self._cadastro_explorer = CadastroExplorer(
                query_engine=self._qe, entity_lookup=self._resolver
            )

        cnpjs = df["CNPJ_8"].unique().tolist()
        min_date = df["DATA"].min()
        max_date = df["DATA"].max()

        # Buscar cadastro com 1 trimestre de margem anterior
        start_str = (min_date - pd.DateOffset(months=3)).strftime("%Y-%m")
        end_str = max_date.strftime("%Y-%m")

        df_cad = self._cadastro_explorer.read(
            instituicao=cnpjs,
            start=start_str,
            end=end_str,
        )

        if df_cad.empty:
            for col in cadastro_columns:
                df[col] = pd.NA
            return df

        cad_cols = ["CNPJ_8", "DATA"] + cadastro_columns
        df_cad = df_cad[[c for c in cad_cols if c in df_cad.columns]]

        # Caso data unica: merge simples por CNPJ_8
        if df["DATA"].nunique() == 1:
            df_cad_latest = df_cad.sort_values("DATA").drop_duplicates(
                subset=["CNPJ_8"], keep="last"
            )
            merge_cols = [c for c in cadastro_columns if c in df_cad_latest.columns]
            return df.merge(
                df_cad_latest[["CNPJ_8"] + merge_cols],
                on="CNPJ_8",
                how="left",
            )

        # Time-series: merge_asof para alinhamento temporal
        # merge_asof exige sort pela coluna on= (DATA) como chave primaria
        df_sorted = df.sort_values("DATA")
        df_cad_sorted = df_cad.sort_values("DATA")

        merge_cols = [c for c in cadastro_columns if c in df_cad_sorted.columns]
        result = pd.merge_asof(
            df_sorted,
            df_cad_sorted[["CNPJ_8", "DATA"] + merge_cols],
            on="DATA",
            by="CNPJ_8",
            direction="backward",
        )

        return result.sort_values("DATA").reset_index(drop=True)
