"""
Classes base para Explorers de dados do BCB.

Explorers fornecem interface unificada para coleta e consulta de dados,
combinando collectors (ETL) com queries (DuckDB).
"""

import re
from abc import ABC, abstractmethod
from typing import Optional, Union

import pandas as pd

from ifdata_bcb.services.entity_resolver import EntityResolver
from ifdata_bcb.domain.exceptions import (
    InvalidIdentifierError,
    InvalidDateRangeError,
    MissingRequiredParameterError,
)
from ifdata_bcb.infra.log import get_logger
from ifdata_bcb.infra.query import QueryEngine
from ifdata_bcb.utils.date_utils import yyyymm_to_datetime


# Type aliases para flexibilidade de parametros
DateInput = Union[int, str, list[int], list[str]]
AccountInput = Union[str, list[str]]
InstitutionInput = Union[str, list[str]]


class BaseExplorer(ABC):
    """
    Classe base abstrata para Explorers de dados do BCB.

    Um Explorer combina:
    - Coleta de dados (via Collector)
    - Queries de dados (via QueryEngine com DuckDB)
    - Resolucao de entidades (via EntityResolver)

    Subclasses devem implementar:
    - _get_subdir(): Subdiretorio dos dados
    - _get_file_prefix(): Prefixo dos arquivos Parquet
    - _get_collector(): Retorna o collector adequado
    - read(): Metodo de consulta principal
    - collect(): Metodo de coleta (pode delegar para collector)

    Mapeamento de colunas (storage -> apresentacao):
    - Subclasses podem definir _COLUMN_MAP para mapear nomes de storage
      para nomes de apresentacao
    - Exemplo: _COLUMN_MAP = {"DATA_BASE": "DATA", "NOME_INSTITUICAO": "INSTITUICAO"}

    Exemplo de uso:
        explorer = COSIFExplorer()
        explorer.collect('2024-01', '2024-12')
        df = explorer.read(instituicoes='60872504', contas=['TOTAL GERAL DO ATIVO'], start='2024-12')
    """

    # Mapeamento de colunas: nome_storage -> nome_apresentacao
    # Subclasses devem sobrescrever com seu mapeamento especifico
    _COLUMN_MAP: dict[str, str] = {}

    def __init__(
        self,
        query_engine: Optional[QueryEngine] = None,
        entity_resolver: Optional[EntityResolver] = None,
    ):
        """
        Inicializa o Explorer.

        Args:
            query_engine: QueryEngine customizado. Se None, cria um novo.
            entity_resolver: EntityResolver customizado. Se None, cria um novo.
        """
        self._qe = query_engine or QueryEngine()
        self._resolver = entity_resolver or EntityResolver(query_engine=self._qe)
        self._logger = get_logger(__name__)

    @property
    def query_engine(self) -> QueryEngine:
        """Acesso ao QueryEngine."""
        return self._qe

    @property
    def resolver(self) -> EntityResolver:
        """Acesso ao EntityResolver."""
        return self._resolver

    @property
    def _reverse_column_map(self) -> dict[str, str]:
        """
        Mapa reverso para traducao de nomes de apresentacao para storage.

        Usado para traduzir parametros de filtro (WHERE clauses) que usam
        nomes de apresentacao para os nomes reais de storage.

        Returns:
            Dicionario {nome_apresentacao: nome_storage}
        """
        return {v: k for k, v in self._COLUMN_MAP.items()}

    def _storage_col(self, presentation_col: str) -> str:
        """
        Traduz nome de apresentacao para nome de storage.

        Args:
            presentation_col: Nome da coluna na API publica (apresentacao).

        Returns:
            Nome da coluna no storage (Parquet). Se nao houver mapeamento,
            retorna o nome original.
        """
        return self._reverse_column_map.get(presentation_col, presentation_col)

    def _apply_column_mapping(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aplica renomeacao de colunas de storage para apresentacao.

        Args:
            df: DataFrame com colunas de storage.

        Returns:
            DataFrame com colunas renomeadas para apresentacao.
        """
        if df.empty or not self._COLUMN_MAP:
            return df
        rename_map = {k: v for k, v in self._COLUMN_MAP.items() if k in df.columns}
        return df.rename(columns=rename_map) if rename_map else df

    @abstractmethod
    def _get_subdir(self) -> str:
        """Retorna o subdiretorio dos dados."""
        ...

    @abstractmethod
    def _get_file_prefix(self) -> str:
        """Retorna o prefixo dos arquivos Parquet."""
        ...

    def _normalize_dates(self, datas: DateInput) -> list[int]:
        """
        Normaliza datas para lista de inteiros YYYYMM.

        Args:
            datas: Pode ser int, str, ou lista de int/str.
                  Formatos aceitos: 202412, '202412', '2024-12', etc.

        Returns:
            Lista de inteiros no formato YYYYMM.
        """
        original = datas
        if not isinstance(datas, list):
            datas = [datas]

        result = []
        for d in datas:
            if isinstance(d, int):
                result.append(d)
            elif isinstance(d, str):
                # Remove separadores e converte para int
                clean = d.replace("-", "").replace("/", "")[:6]
                result.append(int(clean))
            else:
                raise ValueError(f"Formato de data invalido: {d}")

        self._logger.debug(f"Dates: {original} -> {result}")
        return result

    def _normalize_accounts(
        self, contas: Optional[AccountInput]
    ) -> Optional[list[str]]:
        """
        Normaliza contas para lista de strings.

        Args:
            contas: Pode ser str ou lista de str. None retorna None.

        Returns:
            Lista de strings com nomes das contas, ou None.
        """
        if contas is None:
            return None

        if isinstance(contas, str):
            return [contas]

        return list(contas)

    def _normalize_institutions(
        self, instituicoes: Optional["InstitutionInput"]
    ) -> Optional[list[str]]:
        """
        Normaliza instituicoes para lista de CNPJs de 8 digitos.

        Args:
            instituicoes: CNPJ ou lista de CNPJs.

        Returns:
            Lista de CNPJs validados ou None.
        """
        if instituicoes is None:
            return None

        if isinstance(instituicoes, str):
            return [self._resolve_entity(instituicoes)]

        return [self._resolve_entity(i) for i in instituicoes]

    def _resolve_date_range(
        self,
        start: Optional[str],
        end: Optional[str],
        trimestral: bool = False,
    ) -> Optional[list[int]]:
        """
        Resolve range de datas para lista de periodos YYYYMM.

        Comportamento unificado:
        - start sozinho: data unica (ex: start='2024-03' -> [202403])
        - start + end: range de datas (ex: start='2024-01', end='2024-12' -> [202401, ..., 202412])
        - nenhum: retorna None (todos periodos)

        Args:
            start: Data inicial ou unica (YYYY-MM ou YYYYMM).
            end: Data final (YYYY-MM ou YYYYMM). Se None, start e tratado como data unica.
            trimestral: Se True, apenas trimestres (03, 06, 09, 12).

        Returns:
            Lista de inteiros YYYYMM ou None.

        Raises:
            InvalidDateRangeError: Se start > end.
        """
        # Nenhum filtro de data
        if start is None:
            return None

        # Normalizar start para formato YYYYMM
        start_normalized = self._normalize_dates(start)[0]

        # Data unica (apenas start)
        if end is None:
            return [start_normalized]

        # Normalizar end e validar range
        end_normalized = self._normalize_dates(end)[0]
        if start_normalized > end_normalized:
            raise InvalidDateRangeError(start, end)

        # Range de datas (start + end)
        from ifdata_bcb.utils.date_utils import (
            generate_month_range,
            generate_quarter_range,
        )

        if trimestral:
            return generate_quarter_range(start, end)
        return generate_month_range(start, end)

    def _resolve_entity(self, identificador: str) -> str:
        """
        Valida e retorna CNPJ_8.

        O sistema exige CNPJ de 8 digitos para evitar ambiguidades.
        Use bcb.search() para encontrar o CNPJ correto.

        Args:
            identificador: CNPJ de 8 digitos.

        Returns:
            CNPJ de 8 digitos validado.

        Raises:
            InvalidIdentifierError: Se nao for CNPJ de 8 digitos.
        """
        identificador = identificador.strip()

        if not re.fullmatch(r"\d{8}", identificador):
            self._logger.warning(f"Invalid identifier: {identificador}")
            raise InvalidIdentifierError(
                identificador=identificador,
                suggestion=f"Use bcb.search('{identificador}') para encontrar o CNPJ.",
            )

        self._logger.debug(f"Entity validated: {identificador}")
        return identificador

    def _validate_required_params(
        self,
        instituicao: Optional[InstitutionInput],
        start: Optional[str],
    ) -> None:
        """
        Valida que parametros obrigatorios foram fornecidos.

        Args:
            instituicao: Parametro instituicao.
            start: Parametro start.

        Raises:
            MissingRequiredParameterError: Se parametro obrigatorio ausente.
        """
        if instituicao is None:
            raise MissingRequiredParameterError(
                "instituicao",
                "Especifique pelo menos uma instituicao (CNPJ de 8 digitos).",
            )
        if start is None:
            raise MissingRequiredParameterError(
                "start",
                "Especifique pelo menos uma data (formato YYYY-MM).",
            )

    def _build_string_condition(
        self,
        column: str,
        values: list[str],
        case_insensitive: bool = False,
    ) -> str:
        """
        Constroi condicao SQL segura para filtro de strings.

        Escapa aspas simples para prevenir SQL injection.

        Args:
            column: Nome da coluna.
            values: Lista de valores a filtrar.
            case_insensitive: Se True, usa UPPER() para comparacao.

        Returns:
            Clausula SQL (ex: "CONTA IN ('valor1', 'valor2')")
        """
        # Normaliza valores: strip e escape de aspas simples
        escaped = [v.strip().replace("'", "''") for v in values]

        if case_insensitive:
            col_expr = f"UPPER({column})"
            escaped = [v.upper() for v in escaped]
        else:
            col_expr = column

        if len(escaped) == 1:
            return f"{col_expr} = '{escaped[0]}'"
        else:
            values_str = ", ".join(f"'{v}'" for v in escaped)
            return f"{col_expr} IN ({values_str})"

    def _finalize_read(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aplica transformacoes finais apos o read().

        Ordem das operacoes:
        1. Aplica mapeamento de colunas (storage -> apresentacao)
        2. Converte coluna DATA de int YYYYMM para datetime

        Args:
            df: DataFrame retornado pela query.

        Returns:
            DataFrame com transformacoes aplicadas.
        """
        if df.empty:
            return df

        df = df.copy()  # Evitar SettingWithCopyWarning

        # 1. Aplicar mapeamento de colunas (storage -> apresentacao)
        df = self._apply_column_mapping(df)

        # 2. Converter DATA para datetime (usa nome de apresentacao)
        if "DATA" in df.columns:
            df["DATA"] = df["DATA"].apply(yyyymm_to_datetime)

        return df

    def list_periods(self) -> list[int]:
        """
        Lista periodos disponiveis nos dados.

        Returns:
            Lista de periodos (YYYYMM) disponiveis.
        """
        files = self._qe.list_files(self._get_subdir())
        prefix = self._get_file_prefix()

        # Extrair periodo do nome do arquivo
        # Formato: {prefix}_{YYYYMM}.parquet -> retorna YYYYMM
        periods = []
        for f in files:
            if f.startswith(prefix + "_"):
                try:
                    period_str = f.replace(prefix + "_", "")
                    periods.append(int(period_str))
                except ValueError:
                    continue

        return sorted(periods)

    def has_data(self) -> bool:
        """Verifica se existem dados disponiveis."""
        return len(self.list_periods()) > 0

    def describe(self) -> dict:
        """
        Retorna informacoes sobre os dados disponiveis.

        Returns:
            Dicionario com:
            - subdir: Subdiretorio dos dados
            - prefix: Prefixo dos arquivos
            - periods: Lista de periodos disponiveis
            - has_data: Se existem dados
        """
        periods = self.list_periods()
        return {
            "subdir": self._get_subdir(),
            "prefix": self._get_file_prefix(),
            "periods": periods,
            "period_count": len(periods),
            "has_data": len(periods) > 0,
            "first_period": periods[0] if periods else None,
            "last_period": periods[-1] if periods else None,
        }

    @abstractmethod
    def collect(
        self,
        start: str,
        end: str,
        force: bool = False,
    ) -> dict[int, bool]:
        """
        Coleta dados do BCB.

        Args:
            start: Data inicial (formato YYYY-MM).
            end: Data final (formato YYYY-MM).
            force: Se True, recoleta mesmo se dados ja existem.

        Returns:
            Dicionario {periodo: sucesso} indicando resultado de cada periodo.
        """
        ...

    @abstractmethod
    def read(
        self,
        instituicao: Optional["InstitutionInput"] = None,
        conta: Optional[AccountInput] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
        **kwargs,
    ) -> pd.DataFrame:
        """
        Le dados com filtros opcionais.

        Args:
            instituicao: CNPJ(s) de 8 digitos da(s) instituicao(oes).
            conta: Nome(s) da(s) conta(s) a filtrar.
            start: Data inicial ou unica (YYYY-MM). Se end=None, filtra apenas este periodo.
            end: Data final (YYYY-MM). Se fornecido com start, gera range de datas.
            **kwargs: Argumentos adicionais especificos do explorer.

        Returns:
            DataFrame com os dados filtrados.
        """
        ...
