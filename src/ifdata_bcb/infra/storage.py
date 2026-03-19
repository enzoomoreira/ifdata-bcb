from pathlib import Path

import duckdb
import pandas as pd

from ifdata_bcb.infra.config import get_settings
from ifdata_bcb.infra.paths import ensure_dir
from ifdata_bcb.infra.log import get_logger
from ifdata_bcb.utils.period import extract_periods_from_files


def _resolve_base_path(base_path: Path | None) -> Path:
    return base_path or get_settings().cache_path


def list_parquet_files(
    subdir: str,
    pattern: str = "*.parquet",
    base_path: Path | None = None,
) -> list[str]:
    dir_path = _resolve_base_path(base_path) / subdir
    if not dir_path.exists():
        return []
    return [f.stem for f in dir_path.glob(pattern)]


def parquet_exists(
    filename: str,
    subdir: str,
    base_path: Path | None = None,
) -> bool:
    filepath = _resolve_base_path(base_path) / subdir / f"{filename}.parquet"
    return filepath.exists()


def get_parquet_path(
    filename: str,
    subdir: str,
    base_path: Path | None = None,
) -> Path:
    return _resolve_base_path(base_path) / subdir / f"{filename}.parquet"


_metadata_conn: duckdb.DuckDBPyConnection | None = None


def _get_metadata_conn() -> duckdb.DuckDBPyConnection:
    """Reutiliza uma unica conexao DuckDB para consultas de metadata."""
    global _metadata_conn
    if _metadata_conn is None:
        _metadata_conn = duckdb.connect()
    return _metadata_conn


def get_parquet_metadata(
    filename: str,
    subdir: str,
    base_path: Path | None = None,
) -> dict | None:
    """Retorna {arquivo, subdir, registros, colunas, status} ou None se nao existir."""
    filepath = _resolve_base_path(base_path) / subdir / f"{filename}.parquet"

    if not filepath.exists():
        return None

    try:
        conn = _get_metadata_conn()
        schema = conn.sql(f"DESCRIBE SELECT * FROM '{filepath}' LIMIT 0").df()
        n_cols = len(schema)

        count_sql = f"SELECT COUNT(*) as total FROM '{filepath}'"
        count_result = conn.sql(count_sql).fetchone()
        n_rows = count_result[0] if count_result else 0

        return {
            "arquivo": filename,
            "subdir": subdir,
            "registros": n_rows,
            "colunas": n_cols,
            "status": "OK",
        }
    except Exception as e:
        return {
            "arquivo": filename,
            "subdir": subdir,
            "registros": 0,
            "colunas": 0,
            "status": f"Erro: {str(e)[:50]}",
        }


class DataManager:
    """Gerenciador de persistencia em Parquet."""

    def __init__(self, base_path: Path | None = None):
        self.cache_path = _resolve_base_path(Path(base_path) if base_path else None)
        self._logger = get_logger(__name__)
        self._conn = duckdb.connect()

    def save(
        self,
        df: pd.DataFrame,
        filename: str,
        subdir: str,
        compression: str = "snappy",
    ) -> Path:
        """Salva DataFrame para Parquet via PyArrow."""
        output_dir = ensure_dir(self.cache_path / subdir)
        filepath = output_dir / f"{filename}.parquet"

        df.to_parquet(filepath, engine="pyarrow", compression=compression, index=False)

        self._logger.info(f"Saved: {subdir}/{filename}.parquet ({len(df):,} rows)")
        return filepath

    def save_from_query(
        self,
        query: str,
        filename: str,
        subdir: str,
        compression: str = "snappy",
    ) -> Path:
        """Salva resultado de query DuckDB direto para Parquet (sem Pandas)."""
        output_dir = ensure_dir(self.cache_path / subdir)
        filepath = output_dir / f"{filename}.parquet"

        self._conn.sql(query).to_parquet(str(filepath), compression=compression)

        count = self._conn.sql(f"SELECT COUNT(*) FROM '{filepath}'").fetchone()[0]
        self._logger.info(f"Saved: {subdir}/{filename}.parquet ({count:,} rows)")
        return filepath

    def list_files(self, subdir: str, pattern: str = "*.parquet") -> list[str]:
        return list_parquet_files(subdir, pattern, self.cache_path)

    def get_metadata(self, filename: str, subdir: str) -> dict | None:
        return get_parquet_metadata(filename, subdir, self.cache_path)

    def get_periodos_disponiveis(
        self,
        prefix: str,
        subdir: str,
    ) -> list[tuple[int, int]]:
        files = self.list_files(subdir, f"{prefix}_*.parquet")
        return extract_periods_from_files(files, prefix)
