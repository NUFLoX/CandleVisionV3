from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from .deferred_entry import DeferredEntrySnapshot
from .trade_executor import OrderflowSnapshot


@dataclass(frozen=True)
class DeferredEntrySnapshotBuild:
    """A safe deferred-entry snapshot assembled from caller-provided data.

    This adapter performs no network, persistence, executor, or order actions.
    The caller must supply a structure payload derived from closed H1 candles.
    """

    snapshot: DeferredEntrySnapshot | None
    reason: str
    used_live_orderflow: bool
    used_closed_h1_structure: bool


def _positive_float(value: Any) -> float | None:
    if value is None:
        return None

    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None

    return parsed if parsed > 0 else None


def _non_negative_float(value: Any, default: float = 0.0) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default

    return max(parsed, 0.0)


def _record_metadata(record: Mapping[str, Any]) -> dict[str, Any]:
    metadata = record.get("metadata_json")

    if isinstance(metadata, dict):
        return dict(metadata)

    return {}


def _record_initial_snapshot(
    record: Mapping[str, Any],
) -> dict[str, Any]:
    metadata = _record_metadata(record)
    initial_snapshot = metadata.get("initial_snapshot")

    if isinstance(initial_snapshot, dict):
        return dict(initial_snapshot)

    return {}


def _first_positive(*values: Any) -> float | None:
    for value in values:
        parsed = _positive_float(value)

        if parsed is not None:
            return parsed

    return None


def build_deferred_entry_snapshot(
    record: Mapping[str, Any],
    *,
    orderflow_snapshot: OrderflowSnapshot | None,
    closed_h1_structure: Mapping[str, Any] | None = None,
) -> DeferredEntrySnapshotBuild:
    """Build one safe snapshot for deferred lifecycle evaluation.

    Price and live orderflow come from the websocket-derived snapshot.
    Closed-H1 structure has priority for support/EMA/VWAP references.
    Without a usable live snapshot, the adapter may use a closed-H1 price
    only to allow conservative lifecycle transitions such as expiry or stop
    invalidation; it cannot produce a reclaim-ready flow confirmation.
    """

    structure = dict(closed_h1_structure or {})
    initial_snapshot = _record_initial_snapshot(record)

    live_price = _positive_float(
        getattr(orderflow_snapshot, "price", None)
    )
    closed_price = _first_positive(
        structure.get("price"),
        structure.get("candle_close"),
    )

    price = live_price or closed_price

    if price is None:
        return DeferredEntrySnapshotBuild(
            snapshot=None,
            reason="deferred_entry_snapshot_missing_price",
            used_live_orderflow=False,
            used_closed_h1_structure=bool(structure),
        )

    used_live_orderflow = (
        orderflow_snapshot is not None
        and live_price is not None
    )

    if used_live_orderflow:
        buy_flow = _non_negative_float(
            getattr(orderflow_snapshot, "buy_flow", None)
        )
        sell_flow = _non_negative_float(
            getattr(orderflow_snapshot, "sell_flow", None)
        )
        volume_impulse = _non_negative_float(
            getattr(orderflow_snapshot, "volume_impulse", None)
        )
        ask_wall_strength = _non_negative_float(
            getattr(orderflow_snapshot, "ask_wall_strength", None),
            default=1.0,
        )
    else:
        # A candle-only fallback may track expiry/invalidation, but must never
        # manufacture flow confirmation or a reclaim-ready state.
        buy_flow = 0.0
        sell_flow = 0.0
        volume_impulse = 0.0
        ask_wall_strength = 1.0

    support = _first_positive(
        structure.get("support"),
        getattr(orderflow_snapshot, "support", None),
        record.get("origin_support"),
        initial_snapshot.get("support"),
    )
    ema20 = _first_positive(
        structure.get("ema20"),
        getattr(orderflow_snapshot, "ema20", None),
        record.get("origin_ema20"),
        initial_snapshot.get("ema20"),
    )
    vwap = _first_positive(
        structure.get("vwap"),
        getattr(orderflow_snapshot, "vwap", None),
        record.get("origin_vwap"),
        initial_snapshot.get("vwap"),
    )
    candle_close = _first_positive(
        structure.get("candle_close"),
        structure.get("price"),
        getattr(orderflow_snapshot, "candle_close", None),
        price,
    )

    snapshot = DeferredEntrySnapshot(
        price=price,
        buy_flow=buy_flow,
        sell_flow=sell_flow,
        volume_impulse=volume_impulse,
        ask_wall_strength=ask_wall_strength,
        support=support,
        ema20=ema20,
        vwap=vwap,
        candle_close=candle_close,
    )

    return DeferredEntrySnapshotBuild(
        snapshot=snapshot,
        reason=(
            "deferred_entry_snapshot_live_orderflow"
            if used_live_orderflow
            else "deferred_entry_snapshot_closed_h1_price_fallback"
        ),
        used_live_orderflow=used_live_orderflow,
        used_closed_h1_structure=bool(structure),
    )
