from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

SignalKind = Literal["ABSORPTION_LONG", "ABSORPTION_SHORT", "BREAKOUT_LONG", "BREAKOUT_SHORT", "MACRO_ACCUMULATION"]
SignalSource = Literal["orderflow", "macro"]

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
    meta: dict = field(default_factory=dict)

    def dedupe_key(self) -> str:
        return f"{self.symbol}:{self.kind}:{self.side}"
