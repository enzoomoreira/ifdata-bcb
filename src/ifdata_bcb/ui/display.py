"""
Display visual para o usuario usando Rich.

Para logging tecnico (arquivo), use get_logger() de infra.log.
"""

import sys
import threading
from collections.abc import Iterable, Iterator
from typing import TextIO, TypeVar

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
)

T = TypeVar("T")

_display_instance: "Display | None" = None
_display_lock = threading.Lock()


def get_display() -> "Display":
    """Singleton thread-safe."""
    global _display_instance

    if _display_instance is None:
        with _display_lock:
            if _display_instance is None:
                _display_instance = Display()

    return _display_instance


class _ProgressBar(Iterator[T]):
    def __init__(
        self,
        iterable: Iterable[T],
        display: "Display",
        total: int | None = None,
        desc: str | None = None,
        leave: bool = False,
        verbose: bool = True,
    ):
        self._display = display
        self._iterable = iterable
        self._total = total
        self._desc = desc or "Processando"
        self._leave = leave

        is_jupyter = display._is_jupyter

        self._progress = Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TaskProgressColumn(),
            TimeElapsedColumn(),
            console=display._console,
            disable=not verbose,
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
    """Gerencia output visual para o usuario usando Rich."""

    def __init__(self, stream: TextIO | None = None, colors: bool = True):
        self.stream = stream or sys.stdout

        detect_console = Console()
        self._is_jupyter = detect_console.is_jupyter

        # force_jupyter=False faz Rich usar ANSI codes (suportados por notebooks modernos)
        # evita bug de outputs separados em Jupyter (github.com/Textualize/rich/issues/3483)
        self._console = Console(
            file=self.stream,
            no_color=not colors,
            force_jupyter=False if self._is_jupyter else None,
        )

        # Thread-safety para barras de progresso
        self._active_bars = 0
        self._bars_lock = threading.Lock()

    def progress(
        self,
        iterable: Iterable[T],
        total: int | None = None,
        desc: str | None = None,
        leave: bool = False,
        verbose: bool = True,
    ) -> _ProgressBar[T]:
        return _ProgressBar(
            iterable=iterable,
            display=self,
            total=total,
            desc=desc,
            leave=leave,
            verbose=verbose,
        )

    def banner(
        self,
        title: str,
        subtitle: str | None = None,
        first_run: bool | None = None,
        indicator_count: int | None = None,
        verbose: bool = True,
    ) -> None:
        """first_run: True="PRIMEIRA EXECUCAO", False="ATUALIZACAO", None=nao mostra."""
        if not verbose:
            return

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
        total: int | None = None,
        periodos: int | None = None,
        falhas: int | None = None,
        indisponiveis: int | None = None,
        verbose: bool = True,
    ) -> None:
        """Cor: verde=OK, amarelo=parcial, vermelho=tudo falhou."""
        if not verbose:
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

    def separator(self, verbose: bool = True) -> None:
        if verbose:
            self._console.print("-" * 70, style="dim")

    def fetch_start(
        self, name: str, since: str | None = None, verbose: bool = True
    ) -> None:
        if not verbose:
            return

        if since:
            self._console.print(f"  [cyan]>[/cyan] Buscando {name} desde {since}...")
        else:
            self._console.print(f"  [cyan]>[/cyan] Buscando {name}...")

    def fetch_result(self, count: int, verbose: bool = True) -> None:
        if not verbose:
            return

        if count:
            self._console.print(f"    [green]{count:,} registros[/green]")
        else:
            self._console.print("    [yellow]Sem dados disponiveis[/yellow]")
        self._console.print()

    def print_warning(self, message: str, verbose: bool = True) -> None:
        if verbose:
            self._console.print(f"  [yellow][!][/yellow] {message}")

    def print_error(self, message: str) -> None:
        self._console.print(f"[red][X][/red] {message}")

    def print_success(self, message: str, verbose: bool = True) -> None:
        if verbose:
            self._console.print(f"[green][OK][/green] {message}")

    def print_info(self, message: str, verbose: bool = True) -> None:
        if verbose:
            self._console.print(f"[blue][i][/blue] {message}")

    def __repr__(self) -> str:
        return "Display()"
