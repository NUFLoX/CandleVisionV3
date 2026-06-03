from __future__ import annotations

import sqlite3
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dashboard.ingest_client import signal_to_dashboard_payload
from dashboard.schemas import Signal, SignalStrength, SignalType
from dashboard.signal_outcomes import calculate_signal_outcome
from dashboard.taxonomy import build_signal_taxonomy
from orderflow_accum.signal_store import SignalStore
from orderflow_accum.trade_executor import (
    BUY,
    ENTERED,
    ENTER_LONG,
    OrderflowSnapshot,
    SmartTradeExecutor,
    TradeSetup,
)


@dataclass(slots=True)
class FakeSignal:
    symbol: str = "BTCUSDT"
    side: str = "Buy"
    kind: str = "BREAKOUT_PRESSURE"
    source: str = "orderflow"
    score: float = 8.4
    entry: float = 100.0
    stop_loss: float = 95.0
    take_profit_1: float = 110.0
    take_profit_2: float = 115.0
    reasons: list[str] = field(default_factory=lambda: ["buy_flow_dominance"])
    meta: dict[str, object] = field(default_factory=lambda: {"tf": "15"})


@dataclass(slots=True)
class FakeSignalWithTaxonomy(FakeSignal):
    signal_kind: str = "BREAKOUT_PRESSURE"
    signal_family: str = "long_accumulation"
    signal_focus_group: str = "confirmed_pressure"
    signal_source: str = "orderflow"
    signal_timeframe: str = "15m"


def _stored_signal_core(db_path: Path) -> dict[str, object]:
    with sqlite3.connect(str(db_path)) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT signal_key, symbol, market, timeframe, source, kind, side,
                   score_first, score_last, score_max, status, outcome,
                   entry, stop_loss, take_profit_1, take_profit_2
            FROM signals
            """
        ).fetchone()
    assert row is not None
    return dict(row)


def test_dashboard_taxonomy_payload_adds_labels_without_changing_existing_signal_fields() -> None:
    signal = FakeSignal()
    payload = signal_to_dashboard_payload(signal)

    assert payload["symbol"] == signal.symbol
    assert payload["timeframe"] == "15m"
    assert payload["score"] == signal.score
    assert payload["strength"] == "Strong"
    assert payload["signal_type"] == "Confirmed"
    assert payload["entry"] == signal.entry
    assert payload["stop_loss"] == signal.stop_loss
    assert payload["take_profit_1"] == signal.take_profit_1
    assert payload["take_profit_2"] == signal.take_profit_2
    assert payload["status"] == "ACTIVE"

    assert payload["signal_kind"] == "BREAKOUT_PRESSURE"
    assert payload["signal_family"] == "long_accumulation"
    assert payload["signal_focus_group"] == "confirmed_pressure"
    assert payload["signal_source"] == "orderflow"
    assert payload["signal_timeframe"] == "15m"


def test_taxonomy_fields_do_not_change_signal_store_status_score_or_outcome(tmp_path: Path) -> None:
    plain_db = tmp_path / "plain.db"
    tagged_db = tmp_path / "tagged.db"

    plain_result = SignalStore(db_path=str(plain_db)).upsert_signal(FakeSignal(), market="linear")
    tagged_result = SignalStore(db_path=str(tagged_db)).upsert_signal(FakeSignalWithTaxonomy(), market="linear")

    assert plain_result.to_status == tagged_result.to_status == "BREAKOUT_PRESSURE"
    assert plain_result.should_notify == tagged_result.should_notify is True
    assert plain_result.score_jump == tagged_result.score_jump is False
    assert plain_result.status_changed == tagged_result.status_changed is False

    assert _stored_signal_core(plain_db) == _stored_signal_core(tagged_db)


def test_taxonomy_fields_do_not_change_signal_outcome() -> None:
    created_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    base_payload = {
        "id": "sig-1",
        "symbol": "BTCUSDT",
        "exchange": "Bybit",
        "timeframe": "1m",
        "score": 8.4,
        "strength": SignalStrength.strong,
        "signal_type": SignalType.confirmed,
        "entry": 100.0,
        "stop_loss": 95.0,
        "take_profit_1": 110.0,
        "reason": "unit test",
        "created_at": created_at,
    }
    tagged_payload = {
        **base_payload,
        "signal_kind": "BREAKOUT_PRESSURE",
        "signal_family": "long_accumulation",
        "signal_focus_group": "confirmed_pressure",
        "signal_source": "orderflow",
        "signal_timeframe": "1m",
    }
    candles = [{"start": 1_767_225_600_000, "open": 100.0, "high": 111.0, "low": 99.0, "close": 108.0}]

    plain = calculate_signal_outcome(Signal(**base_payload), candles)
    tagged = calculate_signal_outcome(Signal(**tagged_payload), candles)

    assert plain.outcome == tagged.outcome == "tp"
    assert plain.r_multiple == tagged.r_multiple == 2.0
    assert plain.direction == tagged.direction == "long"
    assert plain.bars_checked == tagged.bars_checked == 1


def test_taxonomy_derivation_does_not_change_executor_decision() -> None:
    setup = TradeSetup(
        symbol="BTCUSDT",
        side=BUY,
        entry_hint=100.0,
        stop_loss=95.0,
        score=8.4,
        timeframe="15m",
        btc_regime="BTC_NEUTRAL",
        reasons=["buy_flow_dominance"],
    )
    snapshot = OrderflowSnapshot(
        price=100.0,
        spread_bps=8.0,
        buy_flow=140.0,
        sell_flow=90.0,
        bid_wall_strength=0.30,
        ask_wall_strength=0.20,
        volume_impulse=1.5,
        support=99.0,
        resistance=101.0,
        ema20=99.5,
        vwap=99.4,
    )
    executor = SmartTradeExecutor()

    before = executor.evaluate_entry(setup, snapshot)
    taxonomy = build_signal_taxonomy(kind="BREAKOUT_PRESSURE", source="orderflow", timeframe=setup.timeframe)
    after = executor.evaluate_entry(setup, snapshot)

    assert taxonomy.signal_family == "long_accumulation"
    assert before == after
    assert after.action == ENTER_LONG
    assert after.reason == "entry_allowed_long"
    assert after.next_state == ENTERED
