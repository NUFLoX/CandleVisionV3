from __future__ import annotations

import csv
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .models import Signal


class SignalCsvLogger:
    def __init__(self, path: str = "accumulation_signals.csv"):
        self.path = Path(path)
        self.columns = [
            "timestamp_utc","source","kind","symbol","side","score","entry","stop_loss","take_profit_1","take_profit_2","reasons","meta",
        ]

    def append(self, signal: Signal) -> None:
        file_exists = self.path.exists()
        with self.path.open("a", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=self.columns)
            if not file_exists:
                writer.writeheader()
            writer.writerow({
                "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                "source": signal.source,
                "kind": signal.kind,
                "symbol": signal.symbol,
                "side": signal.side,
                "score": signal.score,
                "entry": round(signal.entry, 10),
                "stop_loss": round(signal.stop_loss, 10),
                "take_profit_1": round(signal.take_profit_1, 10),
                "take_profit_2": round(signal.take_profit_2, 10),
                "reasons": " | ".join(signal.reasons),
                "meta": " | ".join(f"{key}={value}" for key, value in signal.meta.items()),
            })


class RejectionCsvLogger:
    def __init__(self, path: str = "rejection_reasons.csv"):
        self.path = Path(path)
        self.columns = [
            "timestamp_utc","engine","symbol","reason","score","metrics"
        ]

    def append(self, engine: str, symbol: str, reason: str, score: float | None = None, metrics: dict[str, Any] | None = None) -> None:
        file_exists = self.path.exists()
        with self.path.open("a", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=self.columns)
            if not file_exists:
                writer.writeheader()
            writer.writerow({
                "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                "engine": engine,
                "symbol": symbol,
                "reason": reason,
                "score": "" if score is None else round(score, 4),
                "metrics": " | ".join(f"{key}={value}" for key, value in (metrics or {}).items()),
            })
