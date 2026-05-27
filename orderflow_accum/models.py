
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

SignalKind = Literal[
    "BASE_BUILDUP_LONG",
    "ACCUMULATION_LONG_EARLY",
    "ACCUMULATION_LONG_READY",
    "ACCUMULATION_WATCH",
    "ABSORPTION_ZONE",
    "PRE_IMPULSE_ZONE",
    "BREAKOUT_PRESSURE",
    "SHORT_WATCH",
    "DISTRIBUTION_ZONE",
    "PRE_DUMP_ZONE",
    "CONFIRMED_BREAKDOWN",
  
]
SignalSource = Literal["macro", "orderflow"]


@dataclass(slots=True)
class Signal:
    symbol: str
    side: Literal["Buy", "Sell"]
    kind: SignalKind
    source: SignalSource
    score: float
    entry: float
    stop_loss: float
    take_profit_1: float
    take_profit_2: float
    reasons: list[str] = field(default_factory=list)
    meta: dict[str, object] = field(default_factory=dict)

    def dedupe_key(self) -> str:
        return f"{self.symbol}:{self.kind}:{self.side}"
