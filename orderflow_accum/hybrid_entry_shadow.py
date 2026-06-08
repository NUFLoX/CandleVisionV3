from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from .trade_executor import BTC_BEARISH, BTC_DUMP_RISK, BUY, SELL, OrderflowSnapshot, TradeSetup

SCENARIO_PULLBACK = "pullback_shadow"
SCENARIO_MOMENTUM = "momentum_0_5r_shadow"
STATUS_OBSERVING = "OBSERVING"
STATUS_ENTERED = "ENTERED"
STATUS_MISSED = "MISSED"


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


@dataclass(frozen=True)
class HybridShadowObservation:
    signal_key: str
    symbol: str
    timeframe: str
    side: str
    signal_kind: str
    scanner_entry: float
    scanner_sl: float
    original_risk: float
    btc_regime: str | None
    market_regime: str | None
    created_at: str


class HybridEntryShadowEngine:
    """Shadow-only pullback/momentum entry simulator.

    The engine never returns trade-executor decisions and never calls exchange clients.
    It only creates/updates rows that can be compared with the real executor outcome.
    """

    def __init__(
        self,
        *,
        min_volume_impulse: float = 1.2,
        max_spread_bps: float = 15.0,
        ask_wall_entry_limit: float = 0.65,
        support_buffer_multiplier: float = 0.997,
    ) -> None:
        self.min_volume_impulse = float(min_volume_impulse)
        self.max_spread_bps = float(max_spread_bps)
        self.ask_wall_entry_limit = float(ask_wall_entry_limit)
        self.support_buffer_multiplier = float(support_buffer_multiplier)

    def observe(self, *, store: Any, signal_key: str, setup: TradeSetup, snapshot: OrderflowSnapshot) -> None:
        observation = self._observation(signal_key, setup)
        if observation is None:
            return

        for scenario in (SCENARIO_PULLBACK, SCENARIO_MOMENTUM):
            store.ensure_hybrid_entry_shadow_schema()
            row = store.get_hybrid_entry_shadow(signal_key, scenario)
            if row is None:
                row = store.upsert_hybrid_entry_shadow(
                    signal_key=observation.signal_key,
                    symbol=observation.symbol,
                    timeframe=observation.timeframe,
                    side=observation.side,
                    signal_kind=observation.signal_kind,
                    scanner_entry=observation.scanner_entry,
                    scanner_sl=observation.scanner_sl,
                    original_risk=observation.original_risk,
                    scenario=scenario,
                    status=STATUS_OBSERVING,
                    shadow_sl=observation.scanner_sl,
                    features_json={
                        "btc_regime": observation.btc_regime,
                        "market_regime": observation.market_regime,
                        "created_from_candidate": True,
                    },
                    created_at=observation.created_at,
                    reason="hybrid_shadow_observation_started",
                )
            self._update_scenario(store=store, row=row, observation=observation, setup=setup, snapshot=snapshot, scenario=scenario)

    def _observation(self, signal_key: str, setup: TradeSetup) -> HybridShadowObservation | None:
        entry = float(setup.entry_hint or 0.0)
        stop = float(setup.stop_loss or 0.0)
        if entry <= 0 or stop <= 0:
            return None
        if setup.side == BUY:
            risk = entry - stop
        elif setup.side == SELL:
            risk = stop - entry
        else:
            return None
        if risk <= 0:
            return None
        return HybridShadowObservation(
            signal_key=signal_key,
            symbol=str(setup.symbol),
            timeframe=str(setup.timeframe),
            side=str(setup.side),
            signal_kind=str(setup.signal_kind or ""),
            scanner_entry=entry,
            scanner_sl=stop,
            original_risk=float(risk),
            btc_regime=setup.btc_regime,
            market_regime=setup.market_regime,
            created_at=setup.created_at or utc_now_iso(),
        )

    def _update_scenario(
        self,
        *,
        store: Any,
        row: dict[str, Any],
        observation: HybridShadowObservation,
        setup: TradeSetup,
        snapshot: OrderflowSnapshot,
        scenario: str,
    ) -> None:
        status = str(row.get("status") or STATUS_OBSERVING)
        features = self._base_features(observation, snapshot)
        if scenario == SCENARIO_MOMENTUM:
            features.update(self._momentum_features(observation, snapshot))
            if status == STATUS_OBSERVING:
                self._maybe_enter_momentum(store, row, observation, snapshot, features)
            else:
                self._refresh_entered_metrics(store, row, observation, snapshot, features)
            return

        features.update(self._pullback_features(observation, snapshot))
        if status == STATUS_OBSERVING:
            self._maybe_enter_pullback(store, row, observation, snapshot, features)
        else:
            self._refresh_entered_metrics(store, row, observation, snapshot, features)

    def _base_features(self, observation: HybridShadowObservation, snapshot: OrderflowSnapshot) -> dict[str, Any]:
        current_r = self._current_r(observation.side, observation.scanner_entry, float(snapshot.price), observation.original_risk)
        return {
            "scanner_current_r": current_r,
            "btc_regime": observation.btc_regime,
            "market_regime": observation.market_regime,
            "price": float(snapshot.price),
            "buy_flow": float(snapshot.buy_flow),
            "sell_flow": float(snapshot.sell_flow),
            "volume_impulse": float(snapshot.volume_impulse),
            "ask_wall_strength": float(snapshot.ask_wall_strength),
            "spread_bps": float(snapshot.spread_bps),
            "support": snapshot.support,
            "ema20": snapshot.ema20,
            "vwap": snapshot.vwap,
        }

    def _momentum_features(self, observation: HybridShadowObservation, snapshot: OrderflowSnapshot) -> dict[str, Any]:
        current_r = self._current_r(observation.side, observation.scanner_entry, float(snapshot.price), observation.original_risk)
        risk_regime_ok = observation.btc_regime not in {BTC_BEARISH, BTC_DUMP_RISK}
        orderflow_ok = snapshot.buy_flow > snapshot.sell_flow if observation.side == BUY else snapshot.sell_flow > snapshot.buy_flow
        volume_ok = snapshot.volume_impulse >= self.min_volume_impulse
        wall_ok = snapshot.ask_wall_strength <= self.ask_wall_entry_limit if observation.side == BUY else snapshot.bid_wall_strength <= self.ask_wall_entry_limit
        spread_ok = snapshot.spread_bps <= self.max_spread_bps
        return {
            "momentum_triggered_at_r": current_r if current_r >= 0.5 else None,
            "missed_momentum_too_late": current_r >= 1.0,
            "btc_regime_ok": risk_regime_ok,
            "orderflow_confirms": orderflow_ok,
            "volume_impulse_confirms": volume_ok,
            "ask_wall_not_heavy": wall_ok,
            "spread_acceptable": spread_ok,
            "would_miss_impulse": current_r >= 0.5 and not (risk_regime_ok and orderflow_ok and volume_ok and wall_ok and spread_ok),
        }

    def _pullback_features(self, observation: HybridShadowObservation, snapshot: OrderflowSnapshot) -> dict[str, Any]:
        price = float(snapshot.price)
        if observation.side == BUY:
            pullback_depth_r = max((observation.scanner_entry - price) / observation.original_risk, 0.0)
            near_reference = self._near_any(price, observation.original_risk, [observation.scanner_entry, snapshot.vwap, snapshot.ema20, snapshot.support])
            pressure_weakened = snapshot.sell_flow <= max(snapshot.buy_flow * 1.05, snapshot.buy_flow + 1e-12)
            buy_recovering = snapshot.buy_flow >= snapshot.sell_flow * 0.95
            support_holds = snapshot.support is None or price >= float(snapshot.support) * self.support_buffer_multiplier
        else:
            pullback_depth_r = max((price - observation.scanner_entry) / observation.original_risk, 0.0)
            near_reference = self._near_any(price, observation.original_risk, [observation.scanner_entry, snapshot.vwap, snapshot.ema20, snapshot.resistance])
            pressure_weakened = snapshot.buy_flow <= max(snapshot.sell_flow * 1.05, snapshot.sell_flow + 1e-12)
            buy_recovering = snapshot.sell_flow >= snapshot.buy_flow * 0.95
            support_holds = snapshot.resistance is None or price <= float(snapshot.resistance) / self.support_buffer_multiplier
        return {
            "pullback_depth_r": pullback_depth_r,
            "pullback_near_reference": near_reference,
            "sell_pressure_weakens": pressure_weakened if observation.side == BUY else None,
            "buy_flow_recovers": buy_recovering if observation.side == BUY else None,
            "support_holds": support_holds,
            "would_avoid_stop": pullback_depth_r > 0 and support_holds,
        }

    def _maybe_enter_momentum(self, store: Any, row: dict[str, Any], observation: HybridShadowObservation, snapshot: OrderflowSnapshot, features: dict[str, Any]) -> None:
        current_r = float(features["scanner_current_r"])
        if current_r >= 1.0:
            store.upsert_hybrid_entry_shadow(**self._row_update(row, observation, STATUS_MISSED, None, snapshot, "missed_momentum_too_late", features))
            return
        if current_r >= 0.5 and all(
            bool(features[key])
            for key in ("btc_regime_ok", "orderflow_confirms", "volume_impulse_confirms", "ask_wall_not_heavy", "spread_acceptable")
        ):
            store.upsert_hybrid_entry_shadow(**self._row_update(row, observation, STATUS_ENTERED, float(snapshot.price), snapshot, "momentum_0_5r_orderflow_confirmed", features))
            return
        store.upsert_hybrid_entry_shadow(**self._row_update(row, observation, STATUS_OBSERVING, None, snapshot, "momentum_waiting_for_0_5r_confirmation", features))

    def _maybe_enter_pullback(self, store: Any, row: dict[str, Any], observation: HybridShadowObservation, snapshot: OrderflowSnapshot, features: dict[str, Any]) -> None:
        if all(bool(features[key]) for key in ("pullback_near_reference", "support_holds")) and (
            (observation.side == BUY and bool(features.get("buy_flow_recovers")) and bool(features.get("sell_pressure_weakens")))
            or observation.side == SELL
        ):
            store.upsert_hybrid_entry_shadow(**self._row_update(row, observation, STATUS_ENTERED, float(snapshot.price), snapshot, "pullback_retest_held_orderflow_recovered", features))
            return
        store.upsert_hybrid_entry_shadow(**self._row_update(row, observation, STATUS_OBSERVING, None, snapshot, "pullback_waiting_for_retest_hold", features))

    def _refresh_entered_metrics(self, store: Any, row: dict[str, Any], observation: HybridShadowObservation, snapshot: OrderflowSnapshot, features: dict[str, Any]) -> None:
        if str(row.get("status")) != STATUS_ENTERED:
            store.upsert_hybrid_entry_shadow(**self._row_update(row, observation, str(row.get("status") or STATUS_OBSERVING), None, snapshot, str(row.get("reason") or "shadow_observing"), features))
            return
        entry = float(row.get("shadow_entry_price") or observation.scanner_entry)
        store.upsert_hybrid_entry_shadow(**self._row_update(row, observation, STATUS_ENTERED, entry, snapshot, str(row.get("reason") or "shadow_entry_active"), features))

    def _row_update(
        self,
        row: dict[str, Any],
        observation: HybridShadowObservation,
        status: str,
        entry_price: float | None,
        snapshot: OrderflowSnapshot,
        reason: str,
        features: dict[str, Any],
    ) -> dict[str, Any]:
        previous_entry = row.get("shadow_entry_price")
        shadow_entry = entry_price if entry_price is not None else (float(previous_entry) if previous_entry not in (None, "") else None)
        shadow_sl = float(row.get("shadow_sl") or observation.scanner_sl)
        max_gain_r = float(row.get("max_gain_r") or 0.0)
        max_drawdown_r = float(row.get("max_drawdown_r") or 0.0)
        exit_r = row.get("exit_r")
        if shadow_entry is not None:
            risk = abs(float(shadow_entry) - shadow_sl)
            if risk > 0:
                current_r = self._current_r(observation.side, float(shadow_entry), float(snapshot.price), risk)
                max_gain_r = max(max_gain_r, current_r, 0.0)
                max_drawdown_r = max(max_drawdown_r, -current_r, 0.0)
                exit_r = current_r
        return {
            "signal_key": observation.signal_key,
            "symbol": observation.symbol,
            "timeframe": observation.timeframe,
            "side": observation.side,
            "signal_kind": observation.signal_kind,
            "scanner_entry": observation.scanner_entry,
            "scanner_sl": observation.scanner_sl,
            "original_risk": observation.original_risk,
            "scenario": str(row.get("scenario")),
            "status": status,
            "shadow_entry_price": shadow_entry,
            "shadow_sl": shadow_sl,
            "shadow_entry_time": row.get("shadow_entry_time") or (utc_now_iso() if status == STATUS_ENTERED and shadow_entry is not None else None),
            "max_gain_r": max_gain_r,
            "max_drawdown_r": max_drawdown_r,
            "exit_r": exit_r,
            "reason": reason,
            "features_json": features,
            "created_at": row.get("created_at") or observation.created_at,
        }

    @staticmethod
    def _current_r(side: str, entry: float, price: float, risk: float) -> float:
        if risk <= 0:
            return 0.0
        if side == SELL:
            return (entry - price) / risk
        return (price - entry) / risk

    @staticmethod
    def _near_any(price: float, risk: float, refs: list[float | None]) -> bool:
        tolerance = max(risk * 0.25, abs(price) * 0.001)
        return any(ref is not None and abs(price - float(ref)) <= tolerance for ref in refs)
