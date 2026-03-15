"""
Gerenciador de persistencia de dados em formato Parquet.

Responsavel por operacoes de escrita e gerenciamento de arquivos Parquet,
complementando o QueryEngine que foca em leitura.
"""

import re
from pathlib import Path
from typing import Optional

import pandas as pd

from ifdata_bcb.infra.config import get_cache_path
from ifdata_bcb.infra.log import get_logger
from ifdata_bcb.infra.query import QueryEngine


class DataManager:
    """
    Gerenciador de persistencia em Parquet.

    Responsabilidades:
    - Salvar DataFrames como Parquet
    - Listar e gerenciar arquivos
    - Consultar periodos disponiveis

    Para leitura e queries SQL, use QueryEngine.

    Exemplo:
        dm = DataManager()

        # Salvar DataFrame
        dm.save(df, "ifdata_val_202412", "ifdata/valores")

        # Listar arquivos
        files = dm.list_files("ifdata/valores")

        # Verificar ultimo periodo
        last = dm.get_last_period("ifdata_val", "ifdata/valores")
    """

    def __init__(self, base_path: Optional[Path] = None):
        """
        Inicializa o gerenciador de dados.

        Args:
            base_path: Caminho base para diretorio de cache.
                      Se None, usa get_cache_path().
        """
        self.cache_path = Path(base_path) if base_path else get_cache_path()
        self._qe = QueryEngine(self.cache_path)
        self._logger = get_logger(__name__)

    def save(
        self,
        df: pd.DataFrame,
        filename: str,
        subdir: str,
        compression: str = "snappy",
    ) -> Path:
        """
        Salva DataFrame em arquivo Parquet.

        Args:
            df: DataFrame para salvar.
            filename: Nome do arquivo (sem extensao).
            subdir: Subdiretorio dentro de raw/bcb/.
            compression: Algoritmo de compressao ('snappy', 'gzip', 'zstd').

        Returns:
            Path do arquivo salvo.

        Exemplo:
            path = dm.save(df, "ifdata_val_202412", "ifdata/valores")
        """
        output_dir = self.cache_path / subdir
        output_dir.mkdir(parents=True, exist_ok=True)

        filepath = output_dir / f"{filename}.parquet"

        df.to_parquet(
            filepath,
            engine="pyarrow",
            compression=compression,
            index=False,
        )

        self._logger.info(f"Saved: {subdir}/{filename}.parquet ({len(df):,} rows)")
        return filepath

    def read(
        self,
        filename: str,
        subdir: str,
        columns: Optional[list[str]] = None,
        where: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Le arquivo Parquet via QueryEngine.

        Args:
            filename: Nome do arquivo (sem extensao).
            subdir: Subdiretorio.
            columns: Colunas para carregar (None = todas).
            where: Filtro SQL.

        Returns:
            DataFrame com os dados.
        """
        return self._qe.read(filename, subdir, columns, where)

    def list_files(self, subdir: str, pattern: str = "*.parquet") -> list[str]:
        """
        Lista arquivos em um subdiretorio.

        Args:
            subdir: Subdiretorio dentro de raw/bcb/.
            pattern: Glob pattern para filtrar.

        Returns:
            Lista de nomes de arquivos (sem extensao).
        """
        return self._qe.list_files(subdir, pattern)

    def file_exists(self, filename: str, subdir: str) -> bool:
        """
        Verifica se um arquivo existe.

        Args:
            filename: Nome do arquivo (sem extensao).
            subdir: Subdiretorio.

        Returns:
            True se existe, False caso contrario.
        """
        return self._qe.file_exists(filename, subdir)

    def get_last_period(
        self,
        prefix: str,
        subdir: str,
    ) -> Optional[tuple[int, int]]:
        """
        Retorna o ultimo periodo (ano, mes) disponivel.

        Busca arquivos com padrao {prefix}_YYYYMM.parquet e retorna
        o periodo mais recente.

        Args:
            prefix: Prefixo do arquivo (ex: "ifdata_val", "cosif_ind").
            subdir: Subdiretorio.

        Returns:
            Tupla (ano, mes) do ultimo periodo ou None se nao houver arquivos.

        Exemplo:
            last = dm.get_last_period("ifdata_val", "ifdata/valores")
            # Retorna: (2024, 12) ou None
        """
        files = self.list_files(subdir, f"{prefix}_*.parquet")

        if not files:
            self._logger.debug(f"No periods for {prefix} in {subdir}")
            return None

        # Extrair periodos dos nomes de arquivo
        # Padrao esperado: {prefix}_{YYYYMM} ou {prefix}_{YYYY-MM}
        periods = []
        for f in files:
            # Tentar extrair YYYYMM
            match = re.search(rf"{prefix}_(\d{{6}})", f)
            if match:
                period_str = match.group(1)
                year = int(period_str[:4])
                month = int(period_str[4:6])
                periods.append((year, month))
                continue

            # Tentar extrair YYYY-MM
            match = re.search(rf"{prefix}_(\d{{4}})-(\d{{2}})", f)
            if match:
                year = int(match.group(1))
                month = int(match.group(2))
                periods.append((year, month))

        if not periods:
            self._logger.debug(f"No valid periods parsed for {prefix} in {subdir}")
            return None

        result = max(periods)
        self._logger.debug(f"Last period: {prefix} = {result[0]}-{result[1]:02d}")
        return result

    def get_available_periods(
        self,
        prefix: str,
        subdir: str,
    ) -> list[tuple[int, int]]:
        """
        Retorna todos os periodos disponiveis.

        Args:
            prefix: Prefixo do arquivo.
            subdir: Subdiretorio.

        Returns:
            Lista de tuplas (ano, mes) ordenadas.
        """
        files = self.list_files(subdir, f"{prefix}_*.parquet")

        periods = []
        for f in files:
            # Tentar YYYYMM
            match = re.search(rf"{prefix}_(\d{{6}})", f)
            if match:
                period_str = match.group(1)
                year = int(period_str[:4])
                month = int(period_str[4:6])
                periods.append((year, month))
                continue

            # Tentar YYYY-MM
            match = re.search(rf"{prefix}_(\d{{4}})-(\d{{2}})", f)
            if match:
                year = int(match.group(1))
                month = int(match.group(2))
                periods.append((year, month))

        return sorted(set(periods))

    def delete(self, filename: str, subdir: str) -> bool:
        """
        Remove um arquivo.

        Args:
            filename: Nome do arquivo (sem extensao).
            subdir: Subdiretorio.

        Returns:
            True se removeu, False se nao existia.
        """
        filepath = self.cache_path / subdir / f"{filename}.parquet"

        if filepath.exists():
            filepath.unlink()
            self._logger.info(f"Deleted: {subdir}/{filename}.parquet")
            return True

        self._logger.debug(f"Not found: {subdir}/{filename}.parquet")
        return False

    def get_file_path(self, filename: str, subdir: str) -> Path:
        """
        Retorna o caminho completo do arquivo.

        Args:
            filename: Nome do arquivo (sem extensao).
            subdir: Subdiretorio.

        Returns:
            Path completo do arquivo.
        """
        return self.cache_path / subdir / f"{filename}.parquet"

    def ensure_subdir(self, subdir: str) -> Path:
        """
        Garante que o subdiretorio existe.

        Args:
            subdir: Subdiretorio.

        Returns:
            Path do subdiretorio.
        """
        path = self.cache_path / subdir
        path.mkdir(parents=True, exist_ok=True)
        return path
