"""
Sistema de display para output visual ao usuario.

Usa Rich para formatacao e cores no terminal.
Separa feedback visual (console) de logging tecnico (arquivo).
Para logging tecnico, use get_logger() de infra.log.

Sem emojis - usa marcadores de texto para compatibilidade com terminais Windows.
"""

import sys
import threading
from typing import Iterable, Iterator, Optional, TextIO, TypeVar

from rich.console import Console
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

T = TypeVar("T")

_display_instance: Optional["Display"] = None
_display_lock = threading.Lock()


def get_display(verbose: bool = True) -> "Display":
    """
    Retorna instancia unica de Display (Singleton thread-safe).

    Usa double-checked locking para performance:
    - Primeiro check sem lock (rapido, 99% dos casos)
    - Segundo check com lock (apenas na criacao)

    Args:
        verbose: Se True, exibe mensagens. Atualizado em cada chamada.

    Returns:
        Instancia singleton de Display.
    """
    global _display_instance

    # Primeiro check sem lock (fast path)
    if _display_instance is None:
        with _display_lock:
            # Segundo check com lock (evita race condition)
            if _display_instance is None:
                _display_instance = Display(verbose=verbose)

    # Atualiza verbose (operacao atomica em Python, safe sem lock)
    _display_instance.set_verbose(verbose)
    return _display_instance


class _ProgressBar(Iterator[T]):
    """
    Wrapper em volta do Rich Progress compativel com for-loops.

    Substitui tqdm com interface similar mas usando Rich.
    Detecta automaticamente ambiente Jupyter para ajustar comportamento.
    """

    def __init__(
        self,
        iterable: Iterable[T],
        display: "Display",
        total: Optional[int] = None,
        desc: Optional[str] = None,
        leave: bool = False,
    ):
        self._display = display
        self._iterable = iterable
        self._total = total
        self._desc = desc or "Processando"
        self._leave = leave

        # Em Jupyter: nao usar transient (causa duplicacao) e refresh mais lento
        is_jupyter = display._is_jupyter

        # Criar progress bar do Rich
        # Padding extra no final evita glitch visual no Windows onde caracteres
        # da linha anterior ficam "grudados" quando o texto fica mais curto
        self._progress = Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TaskProgressColumn(),
            TimeElapsedColumn(),
            TextColumn("/"),
            TimeRemainingColumn(elapsed_when_finished=True),
            TextColumn("   "),
            console=display._console,
            disable=not display.verbose,
            transient=not leave and not is_jupyter,
            refresh_per_second=4 if is_jupyter else 10,
        )

        # Iniciar o progress e a task
        self._progress.start()
        self._task_id = self._progress.add_task(self._desc, total=total)

        # Obter iterador
        self._iter = iter(iterable)

        # Incrementa contador de barras ativas (thread-safe)
        with self._display._bars_lock:
            self._display._active_bars += 1

    def __iter__(self) -> Iterator[T]:
        return self

    def __next__(self) -> T:
        try:
            item = next(self._iter)
            self._progress.advance(self._task_id)
            return item
        except StopIteration:
            self.close()
            raise

    def close(self) -> None:
        """Fecha a barra e decrementa contador."""
        if self._progress is not None:
            self._progress.stop()
            # Decrementa contador de barras ativas (thread-safe)
            with self._display._bars_lock:
                self._display._active_bars = max(0, self._display._active_bars - 1)
            self._progress = None

    def __enter__(self) -> "_ProgressBar[T]":
        return self

    def __exit__(self, *args) -> None:
        self.close()


class Display:
    """
    Gerencia output visual para o usuario usando Rich.

    Responsabilidades:
    - Banners e separadores
    - Mensagens de progresso e status
    - Barras de progresso
    - Formatacao consistente
    - Cores para destaque (warning/error/banners)

    Thread-safety:
    - Contador de barras protegido por lock

    Para logging tecnico (arquivo), use get_logger() de infra.log.

    Args:
        verbose: Se True, exibe mensagens. Se False, silencia tudo.
        stream: Stream de saida (default: sys.stdout).
        colors: Se True, usa cores quando suportado.

    Exemplo:
        display = Display(verbose=True)
        display.banner("COSIF - Coleta", indicator_count=10)
        display.fetch_start("202401")
        display.fetch_result(5234)
        display.end_banner(total=5234)

        # Com barra de progresso
        for item in display.progress(items, total=100, desc="Processando"):
            process(item)
    """

    def __init__(
        self,
        verbose: bool = True,
        stream: Optional[TextIO] = None,
        colors: bool = True,
    ):
        self.verbose = verbose
        self.stream = stream or sys.stdout

        # Detecta Jupyter primeiro (antes de criar Console final)
        detect_console = Console()
        self._is_jupyter = detect_console.is_jupyter

        # Em Jupyter, Rich usa IPython.display.display() para cada print, criando outputs
        # separados (bug conhecida: github.com/Textualize/rich/issues/3483).
        # Solucao: force_jupyter=False faz Rich usar ANSI codes (suportados por notebooks modernos)
        self._console = Console(
            file=self.stream,
            no_color=not colors,
            force_jupyter=False if self._is_jupyter else None,
        )

        # Thread-safety para barras de progresso
        self._active_bars = 0
        self._bars_lock = threading.Lock()

    def set_verbose(self, verbose: bool) -> None:
        """
        Altera estado verbose em runtime.

        Args:
            verbose: Se True, exibe mensagens. Se False, silencia.
        """
        self.verbose = verbose

    def _print(self, message: str = "", style: Optional[str] = None) -> None:
        """
        Print interno com controle de verbose.

        Args:
            message: Mensagem a imprimir.
            style: Estilo Rich (ex: "green", "bold red").
        """
        if not self.verbose:
            return

        self._console.print(message, style=style)

    # =========================================================================
    # Progress Bar
    # =========================================================================

    def progress(
        self,
        iterable: Iterable[T],
        total: Optional[int] = None,
        desc: Optional[str] = None,
        leave: bool = False,
    ) -> _ProgressBar[T]:
        """
        Retorna iterador com barra de progresso.

        Args:
            iterable: Iteravel a percorrer.
            total: Numero total de itens (obrigatorio se iterable nao tem __len__).
            desc: Descricao exibida a esquerda da barra.
            leave: Se True, mantem barra apos conclusao.

        Returns:
            Iterador que exibe progresso e pode ser usado em for-loops.

        Exemplo:
            for item in display.progress(items, total=100, desc="Baixando"):
                download(item)

            # Ou com context manager para garantir close():
            with display.progress(futures, total=10) as pbar:
                for future in pbar:
                    result = future.result()
        """
        return _ProgressBar(
            iterable=iterable,
            display=self,
            total=total,
            desc=desc,
            leave=leave,
        )

    # =========================================================================
    # Banners
    # =========================================================================

    def banner(
        self,
        title: str,
        subtitle: Optional[str] = None,
        first_run: Optional[bool] = None,
        indicator_count: Optional[int] = None,
    ) -> None:
        """
        Exibe banner de inicio de coleta.

        Args:
            title: Titulo principal (ex: "COSIF - Coleta Individual").
            subtitle: Subtitulo opcional.
            first_run: Se True, mostra "PRIMEIRA EXECUCAO". Se False, "ATUALIZACAO".
                       Se None, nao mostra status de execucao.
            indicator_count: Numero de periodos/indicadores a coletar.
        """
        if not self.verbose:
            return

        # Construir conteudo do banner
        content_lines = []

        if first_run is not None:
            if first_run:
                content_lines.append(
                    "[bold]PRIMEIRA EXECUCAO[/bold] - Download de Historico Completo"
                )
            else:
                content_lines.append("[bold]ATUALIZACAO INCREMENTAL[/bold]")
            content_lines.append("")

        content_lines.append(f"[bold]{title}[/bold]")

        if subtitle:
            content_lines.append(subtitle)

        if indicator_count is not None:
            content_lines.append("")
            content_lines.append(f"Periodos a coletar: {indicator_count}")

        content = "\n".join(content_lines)

        self._console.print(Panel(content, border_style="green"))
        self._console.print()

    def end_banner(
        self,
        total: Optional[int] = None,
        periodos: Optional[int] = None,
        falhas: Optional[int] = None,
        indisponiveis: Optional[int] = None,
    ) -> None:
        """
        Exibe banner de conclusao com estatisticas.

        Cor do banner:
        - Verde: tudo OK (periodos > 0 e sem falhas)
        - Amarelo: parcial (alguns indisponiveis ou falhas, mas alguns OK)
        - Vermelho: tudo falhou (nenhum periodo coletado)

        Args:
            total: Total de registros coletados.
            periodos: Numero de periodos coletados com sucesso.
            falhas: Numero de periodos que falharam.
            indisponiveis: Numero de periodos sem dados disponiveis no BCB.
        """
        if not self.verbose:
            return

        lines = ["[bold]Coleta concluida![/bold]"]

        # Construir linha de estatisticas
        stats = []
        if periodos is not None:
            stats.append(f"Periodos: {periodos}")
        if total is not None:
            stats.append(f"Registros: {total:,}")
        if indisponiveis is not None and indisponiveis > 0:
            stats.append(f"[dim]Indisponiveis: {indisponiveis}[/dim]")
        if falhas is not None and falhas > 0:
            stats.append(f"[yellow]Falhas: {falhas}[/yellow]")

        if stats:
            lines.append(" | ".join(stats))

        # Determinar cor do banner baseado no resultado
        periodos_ok = periodos or 0
        num_falhas = falhas or 0
        num_indisponiveis = indisponiveis or 0

        if num_falhas > 0 and periodos_ok == 0:
            # Tudo falhou (erros reais, nao indisponiveis)
            border_style = "red"
        elif num_falhas > 0 or num_indisponiveis > 0:
            # Parcial: alguns OK, alguns com problema
            border_style = "yellow"
        else:
            # Tudo OK
            border_style = "green"

        content = "\n".join(lines)
        self._console.print(Panel(content, border_style=border_style))

    def separator(self) -> None:
        """Exibe linha separadora."""
        self._print("-" * 70, style="dim")

    # =========================================================================
    # Status de Fetch
    # =========================================================================

    def fetch_start(self, name: str, since: Optional[str] = None) -> None:
        """
        Exibe inicio de fetch de periodo/indicador.

        Args:
            name: Nome do periodo (ex: "202401").
            since: Data de inicio (se incremental).
        """
        if not self.verbose:
            return

        if since:
            self._console.print(f"  [cyan]>[/cyan] Buscando {name} desde {since}...")
        else:
            self._console.print(f"  [cyan]>[/cyan] Buscando {name}...")

    def fetch_result(self, count: int) -> None:
        """
        Exibe resultado de fetch.

        Args:
            count: Numero de registros obtidos.
        """
        if not self.verbose:
            return

        if count:
            self._console.print(f"    [green]{count:,} registros[/green]")
        else:
            self._console.print("    [yellow]Sem dados disponiveis[/yellow]")
        self._console.print()

    # =========================================================================
    # Arquivos
    # =========================================================================

    def saved(self, path: str) -> None:
        """
        Exibe arquivo salvo.

        Args:
            path: Caminho relativo do arquivo.
        """
        self._print(f"[green]Salvo:[/green] {path}")

    def appended(self, path: str) -> None:
        """
        Exibe arquivo atualizado (append).

        Args:
            path: Caminho relativo do arquivo.
        """
        self._print(f"[green]Append:[/green] {path}")

    # =========================================================================
    # Warnings e Errors
    # =========================================================================

    def print_warning(self, message: str) -> None:
        """
        Exibe warning para usuario (em amarelo).

        Args:
            message: Mensagem de aviso.
        """
        if not self.verbose:
            return
        self._console.print(f"  [yellow][!][/yellow] {message}")

    def print_error(self, message: str) -> None:
        """
        Exibe erro para usuario (em vermelho).

        Args:
            message: Mensagem de erro.
        """
        # Erros sempre sao exibidos, mesmo com verbose=False
        self._console.print(f"[red][X][/red] {message}")

    def print_success(self, message: str) -> None:
        """
        Exibe mensagem de sucesso (em verde).

        Args:
            message: Mensagem de sucesso.
        """
        self._print(f"[green][OK][/green] {message}")

    def print_info(self, message: str) -> None:
        """
        Exibe mensagem informativa.

        Args:
            message: Mensagem informativa.
        """
        self._print(f"[blue][i][/blue] {message}")

    def __repr__(self) -> str:
        return f"Display(verbose={self.verbose})"
