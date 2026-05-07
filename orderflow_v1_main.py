# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio
import logging
import traceback

from rich.logging import RichHandler

from orderflow_v1.config import Settings
from orderflow_v1.console_ui import ConsoleUI
from orderflow_v1.runner import OrderFlowRunner

APP_VERSION = "V1.3.1 WS"


def setup_logging() -> None:
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    file_formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.handlers.clear()

    app_file = logging.FileHandler("orderflow_v1.log", encoding="utf-8")
    app_file.setFormatter(file_formatter)

    console = RichHandler(rich_tracebacks=True, markup=True, show_path=False)
    console.setFormatter(formatter)

    macro_file = logging.FileHandler("macro_signals.log", encoding="utf-8")
    macro_file.setFormatter(file_formatter)

    realtime_file = logging.FileHandler("orderflow_signals.log", encoding="utf-8")
    realtime_file.setFormatter(file_formatter)

    root_logger.addHandler(app_file)
    root_logger.addHandler(console)

    for logger_name, file_handler in (
        ("OrderFlow.Signal.Macro", macro_file),
        ("OrderFlow.Signal.Realtime", realtime_file),
    ):
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.INFO)
        logger.handlers.clear()
        logger.propagate = False
        logger.addHandler(file_handler)


async def main() -> None:
    setup_logging()
    settings = Settings()
    ui = ConsoleUI()
    ui.print_banner(
        version=APP_VERSION,
        quote=settings.quote_coin,
        testnet=settings.bybit_testnet,
        signals_only=settings.signals_only,
    )
    logging.getLogger("OrderFlow").info(
        "Config loaded | testnet=%s | signals_only=%s | quote=%s",
        settings.bybit_testnet,
        settings.signals_only,
        settings.quote_coin,
    )
    runner = OrderFlowRunner(settings, ui=ui, version=APP_VERSION)
    await runner.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStopped by user.", flush=True)
    except Exception as exc:
        print(f"Fatal error: {exc}", flush=True)
        traceback.print_exc()
        raise
