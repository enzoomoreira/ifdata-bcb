from pathlib import Path
from typing import Optional

import duckdb
import pandas as pd

from ifdata_bcb.infra.config import get_cache_path
from ifdata_bcb.infra.log import get_logger


class QueryEngine:
    """Motor de consultas DuckDB sobre arquivos Parquet."""

    def __init__(
        self,
        base_path: Optional[Path] = None,
        progress_bar: bool = False,
    ):
        self._cache_path = Path(base_path) if base_path else get_cache_path()
        self._conn = duckdb.connect()
        self._conn.execute(f"SET enable_progress_bar = {str(progress_bar).lower()}")
        self._logger = get_logger(__name__)

    @property
    def cache_path(self) -> Path:
        return self._cache_path

    def read_glob(
        self,
        pattern: str,
        subdir: str,
        columns: Optional[list[str]] = None,
        where: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Le multiplos arquivos Parquet como dataset unico.

        Retorna DataFrame vazio se nenhum arquivo corresponder ao pattern.
        """
        dir_path = self._cache_path / subdir
        if not dir_path.exists():
            return pd.DataFrame()

        matching_files = list(dir_path.glob(pattern))
        if not matching_files:
            return pd.DataFrame()

        full_pattern = str(self._cache_path / subdir / pattern)
        select_clause = ", ".join(columns) if columns else "*"
        query = f"SELECT {select_clause} FROM '{full_pattern}'"

        if where:
            query += f" WHERE {where}"

        self._logger.debug(f"Glob: {subdir}/{pattern} ({len(matching_files)} files)")

        try:
            return self._conn.sql(query).df()
        except Exception as e:
            self._logger.error(f"Glob failed: {subdir}/{pattern} - {e}")
            return pd.DataFrame()

    def sql(self, query: str) -> pd.DataFrame:
        """Executa SQL com substituicao de {cache} pelo path do cache."""
        query = query.replace("{cache}", str(self._cache_path))

        query_preview = query.strip().replace("\n", " ")[:80]
        self._logger.debug(f"SQL: {query_preview}...")

        return self._conn.sql(query).df()
