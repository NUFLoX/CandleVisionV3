from __future__ import annotations

from copy import deepcopy

from .schemas import MarketState, PressureStrip

_BTC_DANGER_VALUES = {"DANGER", "BTC_DUMP_RISK", "BTC_BEARISH", "BEAR"}
_TOTAL3_RISK_ON_VALUES = {"STRONG", "NORMAL"}


def normalize_altcoin_market_state(
    market_state: MarketState, pressure_strips: list[PressureStrip] | None = None
) -> MarketState:
    """Apply dashboard-only altcoin mode recovery rules.

    BTC danger is the only condition that forces the dashboard into RISK-OFF.
    Once BTC is no longer dangerous, weak TOTAL3 should not keep altcoin mode
    stuck in RISK-OFF; the dashboard can still show SELECTIVE while allowing
    alt-signal observation during testnet recovery.
    """

    normalized = deepcopy(market_state)
    strips = pressure_strips or []

    if _btc_is_dangerous(normalized):
        normalized.altcoin_mode = "RISK-OFF"
        normalized.can_emit_alt_signals = False
        return normalized

    if _total3_is_risk_on(normalized, strips):
        normalized.altcoin_mode = "RISK-ON"
        normalized.can_emit_alt_signals = True
        return normalized

    normalized.altcoin_mode = "SELECTIVE"
    normalized.can_emit_alt_signals = True
    return normalized


def _btc_is_dangerous(market_state: MarketState) -> bool:
    btc_filter = _state_value(market_state.btc_filter)
    market_regime = _state_value(market_state.market_regime)
    return btc_filter in _BTC_DANGER_VALUES or market_regime in _BTC_DANGER_VALUES


def _total3_is_risk_on(market_state: MarketState, pressure_strips: list[PressureStrip]) -> bool:
    if _state_value(market_state.total3_strength) in _TOTAL3_RISK_ON_VALUES:
        return True
    total3 = next((strip.value for strip in pressure_strips if strip.key == "total3"), None)
    return total3 is not None and total3 >= 45


def _state_value(value: object) -> str:
    return str(value or "").strip().upper().replace("-", "_")
