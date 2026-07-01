from __future__ import annotations

import json
from typing import Any

from .research_membership import (
    enroll_research_signal,
    has_research_signal_membership,
    is_research_entry_action,
)
from .research_runs import ResearchRunLedger


def _text(value: object | None) -> str | None:
    if value is None:
        return None

    normalized = str(value).strip()
    return normalized or None


def _row_value(row: object, key: str) -> object | None:
    try:
        return row[key]
    except (KeyError, IndexError, TypeError):
        return None


def _json_object(value: object) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)

    if value in (None, ""):
        return {}

    try:
        parsed = json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}

    return dict(parsed) if isinstance(parsed, dict) else {}


def _signal_meta(signal: object) -> dict[str, Any]:
    meta = getattr(signal, "meta", {})
    return dict(meta) if isinstance(meta, dict) else {}


def copy_executor_observation(
    ledger: ResearchRunLedger,
    *,
    signal_key: str,
    signal: object,
    row: object,
    diagnostics_json: object,
) -> None:
    if not ledger.run_id:
        return

    source_created_at = _row_value(row, "created_at")

    if not ledger.accepts_source_created_at(source_created_at):
        return

    action = _text(_row_value(row, "action"))

    if is_research_entry_action(action):
        enrolled = enroll_research_signal(
            ledger,
            signal_key=signal_key,
            entered_at=source_created_at,
        )

        if not enrolled:
            return
    elif not has_research_signal_membership(
        ledger,
        signal_key=signal_key,
    ):
        return

    diagnostics = _json_object(diagnostics_json)
    meta = _signal_meta(signal)

    ledger.record_observation(
        signal_key=signal_key,
        symbol=str(getattr(signal, "symbol", "") or ""),
        timeframe=(
            _text(diagnostics.get("executor_timeframe"))
            or _text(meta.get("tf"))
            or "1"
        ),
        side=(
            _text(_row_value(row, "side"))
            or _text(getattr(signal, "side", None))
        ),
        state=_text(_row_value(row, "state")),
        action=action,
        reason=_text(_row_value(row, "reason")),
        entry_price=_row_value(row, "entry_price"),
        current_sl=_row_value(row, "current_sl"),
        exit_price=_row_value(row, "exit_price"),
        exit_reason=_text(_row_value(row, "exit_reason")),
        max_gain_r=_row_value(row, "max_gain_r"),
        max_drawdown_r=_row_value(row, "max_drawdown_r"),
        bars_in_trade=_row_value(row, "bars_in_trade"),
        signal_kind=(
            _text(diagnostics.get("signal_kind"))
            or _text(getattr(signal, "kind", None))
        ),
        btc_regime=(
            _text(diagnostics.get("btc_regime"))
            or _text(meta.get("btc_regime"))
        ),
        market_regime=(
            _text(diagnostics.get("market_regime"))
            or _text(meta.get("market_regime"))
        ),
        diagnostics=diagnostics,
    )


def copy_executor_trade(
    ledger: ResearchRunLedger,
    *,
    trade: dict[str, Any],
    signal: object,
    diagnostics_json: object,
) -> None:
    if not ledger.run_id:
        return

    signal_key = _text(trade.get("signal_key"))

    if not signal_key:
        return

    if not ledger.accepts_source_created_at(
        trade.get("created_at")
    ):
        return

    if not has_research_signal_membership(
        ledger,
        signal_key=signal_key,
    ):
        return

    diagnostics = _json_object(diagnostics_json)
    meta = _signal_meta(signal)

    ledger.record_trade(
        trade,
        signal_kind=(
            _text(diagnostics.get("signal_kind"))
            or _text(getattr(signal, "kind", None))
        ),
        btc_regime=(
            _text(diagnostics.get("btc_regime"))
            or _text(meta.get("btc_regime"))
        ),
        market_regime=(
            _text(diagnostics.get("market_regime"))
            or _text(meta.get("market_regime"))
        ),
    )
