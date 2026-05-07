
# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio
import logging
import traceback

from rich.logging import RichHandler

from orderflow_accum.config import Settings
from orderflow_accum.console_ui import ConsoleUI
from orderflow_accum.runner import AccumulationRunner

APP_VERSION = "ACCUM V1.4.2 DIAG"


def setup_logging() -> None:
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.handlers.clear()

    app_file = logging.FileHandler("accumulation_v1.log", encoding="utf-8")
    app_file.setFormatter(formatter)

    console = RichHandler(rich_tracebacks=True, markup=True, show_path=False)
    console.setFormatter(formatter)

    macro_file = logging.FileHandler("accum_macro.log", encoding="utf-8")
    macro_file.setFormatter(formatter)

    realtime_file = logging.FileHandler("accum_orderflow.log", encoding="utf-8")
    realtime_file.setFormatter(formatter)

    root_logger.addHandler(app_file)
    root_logger.addHandler(console)

    for logger_name, file_handler in (
        ("Accum.Signal.Macro", macro_file),
        ("Accum.Signal.Realtime", realtime_file),
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
        signal_mode=settings.signal_mode,
    )
    logging.getLogger("Accum").info(
        "Config loaded | testnet=%s | signals_only=%s | quote=%s | signal_mode=%s",
        settings.bybit_testnet,
        settings.signals_only,
        settings.quote_coin,
        settings.signal_mode,
    )
    runner = AccumulationRunner(settings, ui=ui, version=APP_VERSION)
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
