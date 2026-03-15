"""
Classe base para collectors de dados do BCB.

Fornece implementacao comum para download, processamento e persistencia,
reduzindo duplicacao entre os collectors especificos.

Integra Display (visual) + Logger (arquivo) para dual output.
Suporta coleta paralela entre periodos para melhor performance.
"""

import threading
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from enum import Enum, auto
from pathlib import Path
from typing import Optional

import pandas as pd

from ifdata_bcb.infra.log import get_logger
from ifdata_bcb.infra.resilience import staggered_delay
from ifdata_bcb.infra.storage import DataManager
from ifdata_bcb.ui.display import get_display
from ifdata_bcb.utils.date_utils import generate_month_range, generate_quarter_range
from ifdata_bcb.utils.text_utils import normalize_text


class CollectStatus(Enum):
    """Status de coleta de um periodo."""

    SUCCESS = auto()  # Coletado com dados
    UNAVAILABLE = auto()  # Periodo sem dados disponiveis no BCB
    FAILED = auto()  # Erro no download ou processamento


class PeriodUnavailableError(Exception):
    """
    Excecao para indicar que um periodo nao esta disponivel no BCB.

    Usada quando o servidor retorna 404 ou resposta vazia, indicando
    que os dados daquele periodo ainda nao foram publicados.
    """

    def __init__(self, period: int, message: str = ""):
        self.period = period
        self.message = message or f"Periodo {period} indisponivel no BCB"
        super().__init__(self.message)


class BaseCollector(ABC):
    """
    Classe base para collectors de dados do BCB.

    Subclasses devem implementar:
    - _download_period(): Download de um periodo especifico
    - _process_to_parquet(): Processamento CSV -> Parquet
    - _get_file_prefix(): Prefixo do arquivo (ex: "cosif_ind")
    - _get_subdir(): Subdiretorio (ex: "cosif/individual")

    Atributos de classe que podem ser sobrescritos:
    - _PERIOD_TYPE: 'monthly' ou 'quarterly'

    Integracao dual:
    - Display: Output visual para o usuario (banners, progresso, cores)
    - Logger: Logs tecnicos em arquivo para debugging
    """

    _PERIOD_TYPE: str = "monthly"  # 'monthly' ou 'quarterly'
    _MAX_WORKERS: int = 4  # Workers para coleta paralela

    def __init__(self, data_manager: Optional[DataManager] = None):
        """
        Inicializa o collector com Display + Logger dual.

        Args:
            data_manager: DataManager customizado. Se None, cria um novo.
        """
        self.dm = data_manager or DataManager()
        self.logger = get_logger(self.__class__.__name__)
        self.display = get_display()
        self._collect_total = 0  # Acumulador de registros coletados
        self._collect_lock = threading.Lock()  # Lock para acesso thread-safe

    # =========================================================================
    # Metodos abstratos (subclasses devem implementar)
    # =========================================================================

    @abstractmethod
    def _get_file_prefix(self) -> str:
        """
        Retorna o prefixo do arquivo.

        Returns:
            Prefixo (ex: "cosif_ind", "ifdata_val").
        """
        pass

    @abstractmethod
    def _get_subdir(self) -> str:
        """
        Retorna o subdiretorio para os arquivos.

        Returns:
            Subdiretorio (ex: "cosif/individual").
        """
        pass

    @abstractmethod
    def _download_period(self, period: int) -> Optional[Path]:
        """
        Baixa dados de um periodo especifico.

        Args:
            period: Periodo no formato YYYYMM.

        Returns:
            Path do arquivo baixado ou None se falhar.
        """
        pass

    @abstractmethod
    def _process_to_parquet(
        self, csv_path: Path, period: int
    ) -> Optional[pd.DataFrame]:
        """
        Processa CSV e retorna DataFrame normalizado.

        Args:
            csv_path: Caminho do arquivo CSV.
            period: Periodo no formato YYYYMM.

        Returns:
            DataFrame processado ou None se falhar.
        """
        pass

    # =========================================================================
    # Metodos de dual output (Display + Logger)
    # =========================================================================

    def _start(self, title: str, num_items: int, verbose: bool = True) -> None:
        """
        Banner de inicio (console + arquivo).

        Args:
            title: Titulo do banner.
            num_items: Numero de periodos a coletar.
            verbose: Se True, exibe no console.
        """
        self._collect_total = 0
        self.display.set_verbose(verbose)
        self.display.banner(title, indicator_count=num_items)
        self.logger.info(f"Coleta iniciada: {num_items} periodos")

    def _end(
        self,
        verbose: bool = True,
        periodos: Optional[int] = None,
        falhas: Optional[int] = None,
        indisponiveis: Optional[int] = None,
    ) -> None:
        """
        Banner de conclusao com estatisticas (console + arquivo).

        Args:
            verbose: Se True, exibe no console.
            periodos: Numero de periodos coletados com sucesso.
            falhas: Numero de periodos que falharam.
            indisponiveis: Numero de periodos sem dados no BCB.
        """
        total = self._collect_total if self._collect_total > 0 else None
        self.display.set_verbose(verbose)
        self.display.end_banner(
            total=total,
            periodos=periodos,
            falhas=falhas,
            indisponiveis=indisponiveis,
        )
        if total:
            self.logger.info(f"Coleta concluida: {total:,} registros")
        elif indisponiveis and indisponiveis > 0:
            self.logger.info(f"Coleta concluida: {indisponiveis} periodo(s) indisponivel(is)")
        else:
            self.logger.info("Coleta concluida")

    def _fetch_start(
        self, name: str, since: Optional[str] = None, verbose: bool = True
    ) -> None:
        """
        Exibe inicio de fetch (console + arquivo).

        Args:
            name: Nome do periodo/indicador.
            since: Data de inicio (se incremental).
            verbose: Se True, exibe no console.
        """
        self.display.set_verbose(verbose)
        self.display.fetch_start(name, since)
        self.logger.debug(f"Fetch start: {name}, since={since}")

    def _fetch_result(self, name: str, count: int, verbose: bool = True) -> None:
        """
        Exibe resultado de fetch (console + arquivo).

        Args:
            name: Nome do periodo/indicador.
            count: Numero de registros obtidos.
            verbose: Se True, exibe no console.
        """
        self.display.set_verbose(verbose)
        self.display.fetch_result(count)
        self._collect_total += count
        if count:
            self.logger.info(f"Fetch OK: {name}, {count:,} registros")
        else:
            # Fetch vazio e esperado em alguns periodos (ex: trimestres futuros)
            self.logger.debug(f"Fetch vazio: {name}")

    def _info(self, message: str, verbose: bool = True) -> None:
        """
        Mensagem informativa (console + arquivo).

        Args:
            message: Mensagem.
            verbose: Se True, exibe no console.
        """
        self.display.set_verbose(verbose)
        self.display.print_info(message)
        self.logger.info(message)

    def _warning(self, message: str, verbose: bool = True) -> None:
        """
        Warning visual para usuario + log tecnico em arquivo.

        O Display mostra mensagem formatada (amarelo) no console.
        O Logger registra em arquivo como INFO (nao duplica no console,
        ja que console e WARNING+).

        Args:
            message: Mensagem de aviso.
            verbose: Se True, exibe no console via Display.
        """
        self.display.set_verbose(verbose)
        self.display.print_warning(message)
        self.logger.info(f"[warning] {message}")

    # =========================================================================
    # Geracao de periodos
    # =========================================================================

    def _generate_periods(self, start: str, end: str) -> list[int]:
        """
        Gera lista de periodos entre start e end.

        Args:
            start: Data inicial (YYYY-MM-DD, YYYY-MM ou YYYYMM).
            end: Data final (YYYY-MM-DD, YYYY-MM ou YYYYMM).

        Returns:
            Lista de periodos no formato YYYYMM.
        """
        if self._PERIOD_TYPE == "quarterly":
            return generate_quarter_range(start, end)
        return generate_month_range(start, end)

    def _get_missing_periods(self, start: str, end: str) -> list[int]:
        """
        Retorna periodos que faltam coletar.

        Args:
            start: Data inicial.
            end: Data final.

        Returns:
            Lista de periodos faltantes.
        """
        all_periods = self._generate_periods(start, end)
        existing = self.dm.get_available_periods(
            self._get_file_prefix(), self._get_subdir()
        )

        # Converter tuplas (ano, mes) para int YYYYMM
        existing_ints = {y * 100 + m for y, m in existing}

        return [p for p in all_periods if p not in existing_ints]

    # =========================================================================
    # Normalizacao de dados
    # =========================================================================

    def _normalize_text_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normaliza campos de texto no DataFrame.

        Remove newlines e espacos multiplos de colunas de texto conhecidas.
        Isso garante que os nomes de contas e instituicoes sejam consistentes
        e nao contenham caracteres problematicos dos CSVs do BCB.

        Args:
            df: DataFrame a normalizar.

        Returns:
            DataFrame com campos de texto normalizados.
        """
        # Colunas que podem conter newlines/espacos problematicos nos CSVs do BCB
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
        Processa um unico periodo (download + processamento + salvamento).

        Metodo thread-safe para uso com ThreadPoolExecutor.
        Inclui staggered delay para evitar sobrecarga em APIs publicas.

        Args:
            period: Periodo no formato YYYYMM.
            worker_index: Indice do worker para staggered delay.

        Returns:
            Tupla (registros, status, erro_msg).
            - registros: Numero de registros processados (0 se falhou/indisponivel).
            - status: CollectStatus indicando resultado.
            - erro_msg: Mensagem de erro se FAILED, None caso contrario.
        """
        try:
            # Staggered delay para evitar rate limiting
            staggered_delay(worker_index)

            # Download
            csv_path = self._download_period(period)
            if csv_path is None:
                return (0, CollectStatus.FAILED, f"Falha no download do periodo {period}")

            # Processar
            df = self._process_to_parquet(csv_path, period)
            if df is None or df.empty:
                # Periodo sem dados disponiveis no BCB (ex: trimestre ainda nao publicado)
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

        Usa coleta paralela entre periodos para melhor performance.
        Estatisticas sao exibidas no banner de conclusao (se _show_banners=True).

        Args:
            start: Data inicial (YYYY-MM-DD, YYYY-MM ou YYYYMM).
            end: Data final (YYYY-MM-DD, YYYY-MM ou YYYYMM).
            force: Se True, recoleta mesmo que ja exista.
            verbose: Se True, mostra banners e barra de progresso.
            progress_desc: Descricao customizada para barra de progresso.
            _show_banners: Se True, exibe banners de inicio/fim (uso interno).

        Returns:
            Tupla (registros, periodos_ok, falhas, indisponiveis).

        Exemplo:
            collector = COSIFCollector('individual')
            collector.collect('2024-01', '2024-12')
        """
        # Determinar periodos a coletar
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
        self.display.set_verbose(verbose)
        desc = progress_desc or "Periodos"

        with ThreadPoolExecutor(max_workers=self._MAX_WORKERS) as executor:
            # Submeter todos os periodos com indice para staggered delay
            # Usa modulo para que apenas os primeiros workers de cada batch tenham delay
            futures = {
                executor.submit(self._process_single_period, p, i % self._MAX_WORKERS): p
                for i, p in enumerate(periods)
            }

            # Processar conforme completam (com barra de progresso)
            for future in self.display.progress(
                as_completed(futures), total=len(periods), desc=desc
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
        """
        Retorna status dos dados coletados.

        Returns:
            DataFrame com metadados dos arquivos.
        """
        files = self.dm.list_files(self._get_subdir())
        prefix = self._get_file_prefix()

        # Filtrar apenas arquivos deste collector
        files = [f for f in files if f.startswith(prefix)]

        if not files:
            return pd.DataFrame(columns=["arquivo", "periodo", "registros", "status"])

        status_data = []
        qe = self.dm._qe

        for f in sorted(files):
            meta = qe.get_metadata(f, self._get_subdir())
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
        """
        Retorna periodos disponiveis.

        Returns:
            Lista de tuplas (ano, mes).
        """
        return self.dm.get_available_periods(
            self._get_file_prefix(), self._get_subdir()
        )
