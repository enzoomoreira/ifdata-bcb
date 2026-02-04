import threading
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional

import duckdb
import pandas as pd

from ifdata_bcb.infra.log import get_logger
from ifdata_bcb.infra.resilience import staggered_delay
from ifdata_bcb.infra.storage import DataManager
from ifdata_bcb.domain.exceptions import PeriodUnavailableError
from ifdata_bcb.providers.collector_models import CollectStatus
from ifdata_bcb.ui.display import get_display
from ifdata_bcb.utils.date import generate_month_range, generate_quarter_range
from ifdata_bcb.utils.text import normalize_text


class BaseCollector(ABC):
    """
    Classe base para collectors de dados do BCB.

    Subclasses devem implementar:
    - _download_period(): Download de um periodo especifico
    - _process_to_parquet(): Processamento CSV -> Parquet
    - _get_file_prefix(): Prefixo do arquivo (ex: "cosif_ind")
    - _get_subdir(): Subdiretorio (ex: "cosif/individual")
    """

    _PERIOD_TYPE: str = "monthly"
    _MAX_WORKERS: int = 4

    def __init__(self, data_manager: Optional[DataManager] = None):
        self.dm = data_manager or DataManager()
        self.logger = get_logger(self.__class__.__name__)
        self.display = get_display()
        self._collect_total = 0
        self._collect_lock = threading.Lock()
        self._duckdb_conn = duckdb.connect()  # Conexao para cursors thread-local

    def _get_cursor(self) -> duckdb.DuckDBPyConnection:
        """Retorna cursor thread-local para operacoes DuckDB."""
        return self._duckdb_conn.cursor()

    # =========================================================================
    # Metodos abstratos (subclasses devem implementar)
    # =========================================================================

    @abstractmethod
    def _get_file_prefix(self) -> str:
        """Prefixo do arquivo (ex: 'cosif_ind', 'ifdata_val')."""
        pass

    @abstractmethod
    def _get_subdir(self) -> str:
        """Subdiretorio para os arquivos (ex: 'cosif/individual')."""
        pass

    @abstractmethod
    def _download_period(self, period: int) -> Optional[Path]:
        """Baixa dados de um periodo (YYYYMM). Retorna path ou None se falhar."""
        pass

    @abstractmethod
    def _process_to_parquet(
        self, csv_path: Path, period: int
    ) -> Optional[pd.DataFrame]:
        """Processa CSV e retorna DataFrame normalizado, ou None se falhar."""
        pass

    def _start(self, title: str, num_items: int, verbose: bool = True) -> None:
        self._collect_total = 0
        self.display.banner(title, indicator_count=num_items, verbose=verbose)
        self.logger.info(f"Coleta iniciada: {num_items} periodos")

    def _end(
        self,
        verbose: bool = True,
        periodos: Optional[int] = None,
        falhas: Optional[int] = None,
        indisponiveis: Optional[int] = None,
    ) -> None:
        total = self._collect_total if self._collect_total > 0 else None
        self.display.end_banner(
            total=total,
            periodos=periodos,
            falhas=falhas,
            indisponiveis=indisponiveis,
            verbose=verbose,
        )
        if total:
            self.logger.info(f"Coleta concluida: {total:,} registros")
        elif indisponiveis and indisponiveis > 0:
            self.logger.info(
                f"Coleta concluida: {indisponiveis} periodo(s) indisponivel(is)"
            )
        else:
            self.logger.info("Coleta concluida")

    def _fetch_start(
        self, name: str, since: Optional[str] = None, verbose: bool = True
    ) -> None:
        self.display.fetch_start(name, since, verbose=verbose)
        self.logger.debug(f"Fetch start: {name}, since={since}")

    def _fetch_result(self, name: str, count: int, verbose: bool = True) -> None:
        self.display.fetch_result(count, verbose=verbose)
        self._collect_total += count
        if count:
            self.logger.info(f"Fetch OK: {name}, {count:,} registros")
        else:
            self.logger.debug(f"Fetch vazio: {name}")

    def _info(self, message: str, verbose: bool = True) -> None:
        self.display.print_info(message, verbose=verbose)
        self.logger.info(message)

    def _warning(self, message: str, verbose: bool = True) -> None:
        self.display.print_warning(message, verbose=verbose)
        self.logger.info(f"[warning] {message}")

    # =========================================================================
    # Geracao de periodos
    # =========================================================================

    def _generate_periods(self, start: str, end: str) -> list[int]:
        if self._PERIOD_TYPE == "quarterly":
            return generate_quarter_range(start, end)
        return generate_month_range(start, end)

    def _get_missing_periods(self, start: str, end: str) -> list[int]:
        all_periods = self._generate_periods(start, end)
        existing = self.dm.get_available_periods(
            self._get_file_prefix(), self._get_subdir()
        )
        existing_ints = {y * 100 + m for y, m in existing}
        return [p for p in all_periods if p not in existing_ints]

    # =========================================================================
    # Normalizacao de dados
    # =========================================================================

    def _normalize_text_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove newlines e espacos multiplos de colunas de texto dos CSVs do BCB."""
        text_columns = [
            "CONTA",
            "INSTITUICAO",
            "RELATORIO",
            "GRUPO",
            "NOME_CONGL",
        ]

        for col in text_columns:
            if col in df.columns:
                df[col] = df[col].apply(normalize_text)

        return df

    # =========================================================================
    # Coleta principal
    # =========================================================================

    def _process_single_period(
        self, period: int, worker_index: int = 0
    ) -> tuple[int, CollectStatus, Optional[str]]:
        """
        Processa um periodo: download + processamento + salvamento.

        Thread-safe. Usa staggered delay para evitar rate limiting.
        Retorna (registros, status, erro_msg).
        """
        try:
            # Staggered delay para evitar rate limiting
            staggered_delay(worker_index)

            # Download
            csv_path = self._download_period(period)
            if csv_path is None:
                return (
                    0,
                    CollectStatus.FAILED,
                    f"Falha no download do periodo {period}",
                )

            # Processar
            df = self._process_to_parquet(csv_path, period)
            if df is None or df.empty:
                # Periodo sem dados disponiveis (ex: trimestre ainda nao publicado)
                self.logger.debug(f"Periodo {period} indisponivel no BCB")
                return (0, CollectStatus.UNAVAILABLE, None)

            # Normalizar campos de texto antes de salvar
            df = self._normalize_text_fields(df)

            # Salvar
            filename = f"{self._get_file_prefix()}_{period}"
            self.dm.save(df, filename, self._get_subdir())

            return (len(df), CollectStatus.SUCCESS, None)

        except PeriodUnavailableError:
            # Periodo nao disponivel no BCB (404 ou resposta vazia)
            self.logger.debug(f"Periodo {period} indisponivel no BCB")
            return (0, CollectStatus.UNAVAILABLE, None)

        except Exception as e:
            # Log tecnico em arquivo; mensagem visual sera mostrada pelo collect()
            self.logger.debug(f"Erro no periodo {period}: {e}")
            return (0, CollectStatus.FAILED, str(e))

    def collect(
        self,
        start: str,
        end: str,
        force: bool = False,
        verbose: bool = True,
        progress_desc: Optional[str] = None,
        _show_banners: bool = True,
    ) -> tuple[int, int, int, int]:
        """
        Coleta dados do BCB com progresso Rich e logging dual.

        Usa coleta paralela. Retorna (registros, ok, falhas, indisponiveis).
        """
        if force:
            periods = self._generate_periods(start, end)
        else:
            periods = self._get_missing_periods(start, end)

        if not periods:
            if _show_banners:
                self._info(f"{self._get_file_prefix()}: Dados ja atualizados", verbose)
            return (0, 0, 0, 0)

        # Banner de inicio (se habilitado)
        if _show_banners:
            self._start(
                title=f"Coletando {self._get_file_prefix().upper()}",
                num_items=len(periods),
                verbose=verbose,
            )

        falhas = 0
        indisponiveis = 0
        periodos_falhos: list[int] = []
        self._collect_total = 0

        # Coleta paralela com ThreadPoolExecutor
        desc = progress_desc or "Periodos"

        with ThreadPoolExecutor(max_workers=self._MAX_WORKERS) as executor:
            # Submeter todos os periodos com indice para staggered delay
            # Usa modulo para que apenas os primeiros workers de cada batch tenham delay
            futures = {
                executor.submit(
                    self._process_single_period, p, i % self._MAX_WORKERS
                ): p
                for i, p in enumerate(periods)
            }

            # Processar conforme completam (com barra de progresso)
            for future in self.display.progress(
                as_completed(futures), total=len(periods), desc=desc, verbose=verbose
            ):
                period = futures[future]
                registros, status, erro_msg = future.result()

                if status == CollectStatus.FAILED:
                    periodos_falhos.append(period)
                    falhas += 1
                elif status == CollectStatus.UNAVAILABLE:
                    indisponiveis += 1
                elif registros > 0:
                    # Thread-safe increment
                    with self._collect_lock:
                        self._collect_total += registros

        # Mostrar periodos que falharam apos a barra de progresso
        if periodos_falhos and verbose:
            periodos_str = ", ".join(str(p) for p in sorted(periodos_falhos))
            self._warning(f"Periodos com falha: {periodos_str}", verbose)

        # Banner de conclusao (se habilitado)
        periodos_ok = len(periods) - falhas - indisponiveis
        if _show_banners:
            self._end(
                verbose=verbose,
                periodos=periodos_ok,
                falhas=falhas,
                indisponiveis=indisponiveis,
            )

        return (self._collect_total, periodos_ok, falhas, indisponiveis)

    # =========================================================================
    # Status e periodos disponiveis
    # =========================================================================

    def get_status(self) -> pd.DataFrame:
        files = self.dm.list_files(self._get_subdir())
        prefix = self._get_file_prefix()

        # Filtrar apenas arquivos deste collector
        files = [f for f in files if f.startswith(prefix)]

        if not files:
            return pd.DataFrame(columns=["arquivo", "periodo", "registros", "status"])

        status_data = []

        for f in sorted(files):
            meta = self.dm.get_metadata(f, self._get_subdir())
            if meta:
                # Extrair periodo do nome
                period_str = f.replace(f"{prefix}_", "")
                status_data.append(
                    {
                        "arquivo": f,
                        "periodo": period_str,
                        "registros": meta["registros"],
                        "status": meta["status"],
                    }
                )

        return pd.DataFrame(status_data)

    def available_periods(self) -> list[tuple[int, int]]:
        return self.dm.get_available_periods(
            self._get_file_prefix(), self._get_subdir()
        )
