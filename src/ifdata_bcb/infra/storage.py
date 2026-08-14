import os
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Literal

import duckdb
import pandas as pd

from ifdata_bcb.infra.config import get_settings
from ifdata_bcb.infra.log import get_logger
from ifdata_bcb.infra.paths import ensure_dir
from ifdata_bcb.utils.period import extract_periods_from_files

_TMP_SUFFIX = ".tmp"

ParquetCompression = Literal["snappy", "gzip", "brotli", "lz4", "zstd"]


def _resolve_base_path(base_path: Path | None) -> Path:
    return base_path or get_settings().cache_path


@contextmanager
def _atomic_write(filepath: Path) -> Iterator[Path]:
    """
    Escreve em arquivo temporario e move para o destino final ao concluir.

    Interrupcao no meio da escrita nao pode deixar um .parquet truncado com o
    nome definitivo: a deteccao de "periodo ja coletado" e feita por nome de
    arquivo, entao o periodo corrompido nunca seria recoletado.
    `os.replace` e atomico dentro do mesmo filesystem, incluindo Windows.

    O nome do temporario carrega pid e thread id: com um nome deterministico,
    dois collect() do mesmo periodo (threads ou processos na mesma cache)
    colidiam no proprio .tmp -- WinError 32 no replace/unlink. Com nomes
    unicos, a disputa fica so no os.replace do destino, que e last-writer-wins.
    O sufixo .tmp e preservado para o cleanup de orfaos continuar enxergando.
    """
    tmp_path = filepath.with_name(
        f"{filepath.name}.{os.getpid()}-{threading.get_ident()}{_TMP_SUFFIX}"
    )
    try:
        yield tmp_path
        _replace_with_retry(tmp_path, filepath)
    except BaseException:
        tmp_path.unlink(missing_ok=True)
        raise


def _replace_with_retry(tmp_path: Path, filepath: Path) -> None:
    """os.replace com retry curto para a corrida de rename do Windows.

    Dois os.replace simultaneos no mesmo destino falham transitoriamente com
    WinError 5/32 enquanto o rename do concorrente segura o destino. Em POSIX
    o rename nunca falha por isso e o loop roda uma unica vez.
    """
    for tentativa in range(5):
        try:
            os.replace(tmp_path, filepath)
            return
        except PermissionError:
            if tentativa == 4:
                raise
            time.sleep(0.01 * (tentativa + 1))


def cleanup_partial_writes(subdir: str, base_path: Path | None = None) -> int:
    """Remove .tmp orfaos de escritas interrompidas. Retorna quantos removeu."""
    dir_path = _resolve_base_path(base_path) / subdir
    if not dir_path.exists():
        return 0
    removed = 0
    for tmp_file in dir_path.glob(f"*{_TMP_SUFFIX}"):
        try:
            tmp_file.unlink()
            removed += 1
        except OSError:
            # Outro processo pode estar escrevendo neste arquivo agora
            pass
    return removed


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
# Uma handle DuckDB nao pode ser usada por varias threads ao mesmo tempo, e a
# coleta consulta metadata a partir do pool de workers.
_metadata_lock = threading.Lock()


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
        with _metadata_lock:
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
        compression: ParquetCompression = "snappy",
    ) -> Path:
        """Salva DataFrame para Parquet via PyArrow."""
        output_dir = ensure_dir(self.cache_path / subdir)
        filepath = output_dir / f"{filename}.parquet"

        with _atomic_write(filepath) as tmp_path:
            df.to_parquet(
                tmp_path, engine="pyarrow", compression=compression, index=False
            )

        self._logger.info(f"Saved: {subdir}/{filename}.parquet ({len(df):,} rows)")
        return filepath

    def save_from_query(
        self,
        query: str,
        filename: str,
        subdir: str,
        compression: ParquetCompression = "snappy",
    ) -> Path:
        """Salva resultado de query DuckDB direto para Parquet (sem Pandas)."""
        output_dir = ensure_dir(self.cache_path / subdir)
        filepath = output_dir / f"{filename}.parquet"

        with _atomic_write(filepath) as tmp_path:
            self._conn.sql(query).to_parquet(str(tmp_path), compression=compression)

        row = self._conn.sql(f"SELECT COUNT(*) FROM '{filepath}'").fetchone()
        assert row is not None  # COUNT(*) sempre devolve exatamente uma linha
        self._logger.info(f"Saved: {subdir}/{filename}.parquet ({row[0]:,} rows)")
        return filepath

    def cleanup_partial_writes(self, subdir: str) -> int:
        """Remove .tmp orfaos de escritas interrompidas. Retorna quantos removeu."""
        return cleanup_partial_writes(subdir, self.cache_path)

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

    def close(self) -> None:
        """Fecha a conexao DuckDB."""
        self._conn.close()

    def __enter__(self) -> "DataManager":
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()
