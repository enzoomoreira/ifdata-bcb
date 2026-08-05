import contextlib
from pathlib import Path

import duckdb
import pandas as pd

from ifdata_bcb.domain.exceptions import DataProcessingError
from ifdata_bcb.infra.config import get_settings
from ifdata_bcb.infra.log import get_logger


class QueryEngine:
    """Motor de consultas DuckDB sobre arquivos Parquet."""

    def __init__(
        self,
        base_path: Path | None = None,
        progress_bar: bool = False,
    ):
        self._cache_path = Path(base_path) if base_path else get_settings().cache_path
        self._conn = duckdb.connect()
        self._conn.execute(f"SET enable_progress_bar = {str(progress_bar).lower()}")
        self._logger = get_logger(__name__)

    @property
    def cache_path(self) -> Path:
        return self._cache_path

    def has_glob(self, pattern: str, subdir: str) -> bool:
        """Indica se existe ao menos um arquivo para o glob informado."""
        dir_path = self._cache_path / subdir
        return dir_path.exists() and any(dir_path.glob(pattern))

    @staticmethod
    def _date_sql_expr(col: str, alias: str) -> str:
        """Expressao SQL para converter YYYYMM int em DATE (ultimo dia do mes)."""
        return (
            f"LAST_DAY(MAKE_DATE(CAST({col}/100 AS INT), "
            f"CAST({col}%100 AS INT), 1)) AS {alias}"
        )

    def read_glob(
        self,
        pattern: str,
        subdir: str,
        columns: list[str] | None = None,
        where: str | None = None,
        distinct: bool = False,
        date_column: str | None = None,
        date_alias: str = "DATA",
        exclude_columns: list[str] | None = None,
    ) -> pd.DataFrame:
        """
        Le multiplos arquivos Parquet como dataset unico.

        Args:
            pattern: Glob pattern dos arquivos parquet.
            subdir: Subdiretorio dentro do cache.
            columns: Colunas a selecionar (None = todas).
            where: Clausula WHERE (sem a keyword WHERE).
            distinct: Se True, adiciona DISTINCT ao SELECT.
            date_column: Coluna YYYYMM int a converter pra datetime via DuckDB.
            date_alias: Nome da coluna datetime no output (default "DATA").
            exclude_columns: Colunas a excluir via EXCLUDE (so quando columns=None).

        Retorna DataFrame vazio se nenhum arquivo corresponder ao pattern.
        """
        dir_path = self._cache_path / subdir
        if not dir_path.exists():
            return pd.DataFrame()

        matching_files = list(dir_path.glob(pattern))
        if not matching_files:
            return pd.DataFrame()

        full_pattern = str(self._cache_path / subdir / pattern)

        if columns:
            parts = []
            for col in columns:
                if date_column and col == date_column:
                    parts.append(self._date_sql_expr(col, date_alias))
                else:
                    parts.append(col)
            select_clause = ", ".join(parts)
        else:
            exclude = set(exclude_columns or [])
            if date_column:
                exclude.add(date_column)

            if exclude:
                select_clause = f"* EXCLUDE({', '.join(sorted(exclude))})"
            else:
                select_clause = "*"

            if date_column:
                select_clause += f", {self._date_sql_expr(date_column, date_alias)}"

        distinct_kw = "DISTINCT " if distinct else ""
        query = (
            f"SELECT {distinct_kw}{select_clause} "
            f"FROM read_parquet('{full_pattern}', union_by_name=true)"
        )

        if where:
            query += f" WHERE {where}"

        try:
            return self._conn.sql(query).df()
        except duckdb.Error as e:
            # Devolver DataFrame vazio aqui mascarava parquet corrompido, coluna
            # inexistente e erro de sintaxe montado internamente -- o usuario
            # recebia "sem dados" no lugar da causa. Schema heterogeneo entre
            # periodos ja e resolvido por union_by_name, entao chegar aqui
            # significa problema real.
            self._logger.error(f"Glob query failed: {subdir}/{pattern} - {e}")
            raise DataProcessingError(
                f"{subdir}/{pattern}",
                f"Falha ao ler os dados: {e}. Verifique se os parquets do periodo "
                f"estao integros (recolete com force=True se necessario).",
            ) from e

    def sql(self, query: str) -> pd.DataFrame:
        """Executa SQL com substituicao de {cache} pelo path do cache."""
        query = query.replace("{cache}", str(self._cache_path))
        return self._conn.sql(query).df()

    def close(self) -> None:
        """Fecha a conexao DuckDB."""
        self._conn.close()

    def __enter__(self) -> "QueryEngine":
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def sql_with_df(self, query: str, **tables: pd.DataFrame) -> pd.DataFrame:
        """Executa SQL com DataFrames registrados como tabelas virtuais.

        Permite JOINs, ASOF JOINs etc entre DataFrames em memoria e/ou
        parquets via read_parquet() na mesma query.
        """
        try:
            for name, df in tables.items():
                self._conn.register(name, df)

            return self._conn.sql(query).df()
        finally:
            for name in tables:
                # Limpeza best-effort: um unregister que falha nao pode
                # mascarar a excecao original da query.
                with contextlib.suppress(duckdb.Error):
                    self._conn.unregister(name)
