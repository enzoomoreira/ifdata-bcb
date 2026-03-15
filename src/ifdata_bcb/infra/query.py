"""
Motor de queries DuckDB para arquivos Parquet.

Este modulo fornece uma interface unificada para leitura e consulta
de dados em formato Parquet usando DuckDB, com suporte a:
- Leitura de arquivos unicos ou via glob pattern
- Pushdown de filtros (WHERE) e projecao (SELECT columns)
- SQL arbitrario com substituicao de variaveis de path
"""

from pathlib import Path
from typing import Optional

import duckdb
import pandas as pd

from ifdata_bcb.infra.config import get_cache_path
from ifdata_bcb.infra.log import get_logger


class QueryEngine:
    """
    Motor de consultas DuckDB sobre arquivos Parquet.

    Delega a complexidade de otimizacao (predicate pushdown, column pruning)
    para o DuckDB, mantendo uma interface Pythonica simples.

    Exemplo:
        qe = QueryEngine()

        # Leitura simples
        df = qe.read("ifdata_val_202412", "ifdata/valores")

        # Leitura com filtros
        df = qe.read(
            "ifdata_val_202412",
            "ifdata/valores",
            columns=["COD_INST", "VALOR"],
            where="COD_INST = '60872504'"
        )

        # Leitura de multiplos arquivos via glob
        df = qe.read_glob("ifdata_val_*.parquet", "ifdata/valores")

        # SQL arbitrario
        df = qe.sql("SELECT * FROM '{cache}/ifdata/valores/*.parquet' LIMIT 10")
    """

    def __init__(
        self,
        base_path: Optional[Path] = None,
        progress_bar: bool = False,
    ):
        """
        Inicializa o motor de queries.

        Args:
            base_path: Caminho base para diretorio de cache.
                      Se None, usa get_cache_path().
            progress_bar: Se True, exibe barra de progresso do DuckDB.
        """
        self.cache_path = Path(base_path) if base_path else get_cache_path()
        self._conn = duckdb.connect()
        self._conn.execute(f"SET enable_progress_bar = {str(progress_bar).lower()}")
        self._logger = get_logger(__name__)

    def _build_query(
        self,
        source: str,
        columns: Optional[list[str]] = None,
        where: Optional[str] = None,
    ) -> str:
        """
        Constroi a query SQL.

        Args:
            source: Caminho do arquivo ou glob pattern.
            columns: Lista de colunas para SELECT.
            where: Clausula WHERE.

        Returns:
            Query SQL formatada.
        """
        select_clause = ", ".join(columns) if columns else "*"
        query = f"SELECT {select_clause} FROM '{source}'"

        if where:
            query += f" WHERE {where}"

        return query

    def read(
        self,
        filename: str,
        subdir: str,
        columns: Optional[list[str]] = None,
        where: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Le um arquivo Parquet.

        Args:
            filename: Nome do arquivo (sem extensao .parquet).
            subdir: Subdiretorio dentro de raw/bcb/.
            columns: Lista de colunas para carregar (None = todas).
            where: Filtro SQL (ex: "CNPJ_8 = '60872504'").

        Returns:
            DataFrame com os dados. Retorna DataFrame vazio se arquivo
            nao existir.

        Exemplo:
            df = qe.read(
                "ifdata_val_202412",
                "ifdata/valores",
                columns=["COD_INST", "VALOR"],
                where="COD_INST = '60872504'"
            )
        """
        filepath = self.cache_path / subdir / f"{filename}.parquet"

        if not filepath.exists():
            return pd.DataFrame()

        self._logger.debug(f"Query: {subdir}/{filename} (where={where})")
        query = self._build_query(str(filepath), columns, where)

        try:
            return self._conn.sql(query).df()
        except Exception as e:
            self._logger.error(f"Query failed: {subdir}/{filename} - {e}")
            return pd.DataFrame()

    def read_glob(
        self,
        pattern: str,
        subdir: str,
        columns: Optional[list[str]] = None,
        where: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Le multiplos arquivos Parquet usando glob pattern.

        DuckDB trata multiplos arquivos como um unico dataset, aplicando
        pushdown de filtros e projecao de forma otimizada.

        Args:
            pattern: Glob pattern (ex: "ifdata_val_*.parquet").
            subdir: Subdiretorio dentro de raw/bcb/.
            columns: Lista de colunas para carregar (None = todas).
            where: Filtro SQL.

        Returns:
            DataFrame com dados concatenados. Retorna DataFrame vazio se
            nenhum arquivo corresponder ao pattern.

        Exemplo:
            df = qe.read_glob(
                "ifdata_val_2024*.parquet",
                "ifdata/valores",
                columns=["COD_INST", "DATA", "VALOR"],
                where="COD_INST IN ('60872504', '00000000')"
            )
        """
        full_pattern = str(self.cache_path / subdir / pattern)

        # Verificar se existem arquivos correspondentes
        dir_path = self.cache_path / subdir
        if not dir_path.exists():
            return pd.DataFrame()

        matching_files = list(dir_path.glob(pattern))
        if not matching_files:
            return pd.DataFrame()

        self._logger.debug(f"Glob: {subdir}/{pattern} ({len(matching_files)} files)")
        query = self._build_query(full_pattern, columns, where)

        try:
            return self._conn.sql(query).df()
        except Exception as e:
            self._logger.error(f"Glob failed: {subdir}/{pattern} - {e}")
            return pd.DataFrame()

    def sql(self, query: str) -> pd.DataFrame:
        """
        Executa SQL arbitrario com substituicao de variaveis de path.

        Variaveis disponiveis:
            {cache} - Caminho para diretorio de cache (py-bacen/cache/)
            {raw}   - Alias para {cache} (compatibilidade)

        Args:
            query: Query SQL com placeholders opcionais.

        Returns:
            DataFrame com resultado da query.

        Exemplo:
            df = qe.sql('''
                SELECT CNPJ_8, SUM(VALOR) as total
                FROM '{cache}/ifdata/valores/*.parquet'
                WHERE DATA = 202412
                GROUP BY CNPJ_8
                ORDER BY total DESC
                LIMIT 10
            ''')
        """
        # Substituir variaveis
        cache_str = str(self.cache_path)
        query = query.replace("{cache}", cache_str)
        query = query.replace("{raw}", cache_str)  # Alias para compatibilidade

        # Truncar query longa para log
        query_preview = query.strip().replace("\n", " ")[:80]
        self._logger.debug(f"SQL: {query_preview}...")

        return self._conn.sql(query).df()

    def describe(self, filename: str, subdir: str) -> pd.DataFrame:
        """
        Retorna schema do arquivo (colunas e tipos).

        Args:
            filename: Nome do arquivo (sem extensao).
            subdir: Subdiretorio.

        Returns:
            DataFrame com colunas: column_name, column_type, null, key, default, extra
        """
        filepath = self.cache_path / subdir / f"{filename}.parquet"

        if not filepath.exists():
            return pd.DataFrame()

        return self._conn.sql(f"DESCRIBE SELECT * FROM '{filepath}'").df()

    def get_metadata(self, filename: str, subdir: str) -> Optional[dict]:
        """
        Retorna metadados basicos do arquivo.

        Args:
            filename: Nome do arquivo (sem extensao).
            subdir: Subdiretorio.

        Returns:
            Dict com metadados ou None se arquivo nao existe:
            - arquivo: Nome do arquivo
            - subdir: Subdiretorio
            - registros: Numero total de linhas
            - colunas: Numero de colunas
            - status: 'OK' ou mensagem de erro
        """
        filepath = self.cache_path / subdir / f"{filename}.parquet"

        if not filepath.exists():
            return None

        try:
            # Obter schema
            schema = self._conn.sql(f"DESCRIBE SELECT * FROM '{filepath}' LIMIT 0").df()
            n_cols = len(schema)

            # Contar registros
            count_result = self._conn.sql(
                f"SELECT COUNT(*) as total FROM '{filepath}'"
            ).fetchone()
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

    def list_files(self, subdir: str, pattern: str = "*.parquet") -> list[str]:
        """
        Lista arquivos em um subdiretorio.

        Args:
            subdir: Subdiretorio dentro de raw/bcb/.
            pattern: Glob pattern para filtrar arquivos.

        Returns:
            Lista de nomes de arquivos (sem extensao).
        """
        dir_path = self.cache_path / subdir
        if not dir_path.exists():
            return []

        return [f.stem for f in dir_path.glob(pattern)]

    def file_exists(self, filename: str, subdir: str) -> bool:
        """
        Verifica se um arquivo existe.

        Args:
            filename: Nome do arquivo (sem extensao).
            subdir: Subdiretorio.

        Returns:
            True se arquivo existe, False caso contrario.
        """
        filepath = self.cache_path / subdir / f"{filename}.parquet"
        return filepath.exists()
