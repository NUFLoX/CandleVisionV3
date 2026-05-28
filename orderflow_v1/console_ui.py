from __future__ import annotations

from datetime import datetime
from threading import Lock

from rich.box import HEAVY, ROUNDED
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from .models import Signal


class ConsoleUI:
    def __init__(self) -> None:
        self.console = Console(soft_wrap=True)
        self._lock = Lock()

    def print_banner(self, version: str, quote: str, testnet: bool, signals_only: bool) -> None:
        mode = "SIGNALS ONLY" if signals_only else "TRADE READY"
        network = "TESTNET" if testnet else "MAINNET"
        title = Text(" CandleVision OrderFlow ", style="bold bright_cyan")
        body = Table.grid(padding=(0, 2))
        body.add_row("Version", f"[bold white]{version}[/bold white]")
        body.add_row("Network", f"[bold magenta]{network}[/bold magenta]")
        body.add_row("Mode", f"[bold green]{mode}[/bold green]")
        body.add_row("Quote", f"[bold yellow]{quote}[/bold yellow]")
        panel = Panel(
            body,
            title=title,
            border_style="bright_blue",
            box=HEAVY,
            padding=(1, 2),
        )
        with self._lock:
            self.console.print()
            self.console.print(panel)
            self.console.print()

    def print_signal(self, signal: Signal) -> None:
        side_style = "bold green" if signal.side == "Buy" else "bold red"
        source_style = "cyan" if signal.source == "orderflow" else "magenta"
        kind_style = "bright_cyan" if signal.source == "orderflow" else "bright_magenta"

        header = Table.grid(expand=True)
        header.add_column(justify="left")
        header.add_column(justify="right")
        header.add_row(
            f"[{kind_style}]{signal.kind}[/{kind_style}]",
            f"[dim]{datetime.now().strftime('%H:%M:%S')}[/dim]",
        )
        header.add_row(
            f"[bold white]{signal.symbol}[/bold white]  [{side_style}]{signal.side}[/{side_style}]",
            f"[{source_style}]{signal.source.upper()}[/{source_style}]  [bold white]score={signal.score:.1f}[/bold white]",
        )

        levels = Table(box=ROUNDED, expand=True, show_header=True)
        levels.add_column("Entry", justify="right")
        levels.add_column("SL", justify="right")
        levels.add_column("TP1", justify="right")
        levels.add_column("TP2", justify="right")
        levels.add_row(
            f"{signal.entry:.8f}",
            f"{signal.stop_loss:.8f}",
            f"{signal.take_profit_1:.8f}",
            f"{signal.take_profit_2:.8f}",
        )

        reasons = Text(", ".join(signal.reasons[:6]) or "-", style="white")
        meta_text = ", ".join(f"{k}={v}" for k, v in list(signal.meta.items())[:6]) or "-"

        body = Table.grid(padding=(0, 1))
        body.add_row(header)
        body.add_row(levels)
        body.add_row(Text("Reasons: ", style="bold white") + reasons)
        body.add_row(Text("Meta: ", style="bold white") + Text(meta_text, style="dim"))

        border = "green" if signal.side == "Buy" else "red"
        with self._lock:
            self.console.print(
                Panel(
                    body,
                    title="[bold]Signal[/bold]",
                    border_style=border,
                    box=HEAVY,
                    padding=(0, 1),
                )
            )
            self.console.print()

    def print_notice(self, text: str, style: str = "bright_blue") -> None:
        with self._lock:
            self.console.print(f"[{style}]{text}[/{style}]")
