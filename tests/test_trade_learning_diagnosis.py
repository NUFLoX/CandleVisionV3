from __future__ import annotations

from pathlib import Path

from orderflow_accum.models import Signal
from orderflow_accum.signal_store import SignalStore
from orderflow_accum.trade_learning import TradeLearningEngine


def make_signal(**overrides) -> Signal:
    data = {
        "symbol": "ETHUSDT",
        "side": "Buy",
        "kind": "CONFIRMED_LONG",
        "source": "orderflow",
        "score": 9.0,
        "entry": 100.0,
        "stop_loss": 95.0,
        "take_profit_1": 105.0,
        "take_profit_2": 110.0,
        "reasons": ["long_promotion_rules_met"],
        "meta": {"tf": "5", "market": "linear", "btc_regime": "BTC_NEUTRAL"},
    }
    data.update(overrides)
    return Signal(**data)


def test_tp_outcome_records_lifecycle_event_and_diagnosis(tmp_path: Path) -> None:
    store = SignalStore(db_path=str(tmp_path / "signals.db"))
    engine = TradeLearningEngine(store)
    signal = make_signal()
    key = "ETHUSDT|linear|5|CONFIRMED_LONG|Buy"

    engine.record_outcome(
        signal,
        key,
        "TP2",
        {"max_gain_pct": 10.0, "time_to_tp1_minutes": 5, "time_to_tp2_minutes": 15},
    )

    events = store.get_trade_lifecycle_events(key)
    assert [event["event_type"] for event in events] == ["OUTCOME_TP"]
    diagnosis = store.get_trade_diagnosis(key)
    assert diagnosis is not None
    assert diagnosis["outcome"] == "TP2"
    assert diagnosis["r_result"] == 2.0
    assert diagnosis["success_factors"]["score"] == 9.0
    assert diagnosis["success_factors"]["reasons"] == ["long_promotion_rules_met"]
    assert diagnosis["success_factors"]["time_to_tp2_minutes"] == 15.0
    store.close()


def test_sl_and_expired_outcomes_record_mapped_events(tmp_path: Path) -> None:
    store = SignalStore(db_path=str(tmp_path / "signals.db"))
    engine = TradeLearningEngine(store)
    signal = make_signal()

    engine.record_outcome(
        signal,
        "sl-key",
        "SL",
        {"max_drawdown_pct": -5.0, "time_to_sl_minutes": 10},
    )
    engine.record_outcome(signal, "expired-key", "EXPIRED", {})

    assert store.get_trade_lifecycle_events("sl-key")[0]["event_type"] == "OUTCOME_SL"
    sl_diagnosis = store.get_trade_diagnosis("sl-key")
    assert sl_diagnosis is not None
    assert sl_diagnosis["r_result"] == -1.0
    assert sl_diagnosis["failure_factors"]["max_drawdown_pct"] == -5.0
    assert "Review entry timing" in sl_diagnosis["recommendation"]

    assert store.get_trade_lifecycle_events("expired-key")[0]["event_type"] == "OUTCOME_EXPIRED"
    expired_diagnosis = store.get_trade_diagnosis("expired-key")
    assert expired_diagnosis is not None
    assert expired_diagnosis["r_result"] == 0.0
    assert "timeout window" in expired_diagnosis["recommendation"]
    store.close()


def test_ambiguous_outcome_records_mapped_event(tmp_path: Path) -> None:
    store = SignalStore(db_path=str(tmp_path / "signals.db"))
    engine = TradeLearningEngine(store)
    key = "ambiguous-key"

    engine.record_outcome(make_signal(), key, "AMBIGUOUS", {})

    assert store.get_trade_lifecycle_events(key)[0]["event_type"] == "OUTCOME_AMBIGUOUS"
    diagnosis = store.get_trade_diagnosis(key)
    assert diagnosis is not None
    assert "ambiguous outcome" in diagnosis["diagnosis"]
    store.close()


def test_duplicate_final_event_is_not_inserted_twice_but_diagnosis_updates(tmp_path: Path) -> None:
    store = SignalStore(db_path=str(tmp_path / "signals.db"))
    engine = TradeLearningEngine(store)
    signal = make_signal()
    key = "dup-key"

    engine.record_outcome(signal, key, "TP1", {"max_gain_pct": 5.0})
    engine.record_outcome(signal, key, "TP1", {"max_gain_pct": 6.0})

    events = store.get_trade_lifecycle_events(key)
    assert [event["event_type"] for event in events] == ["OUTCOME_TP"]
    diagnosis = store.get_trade_diagnosis(key)
    assert diagnosis is not None
    assert diagnosis["max_gain_pct"] == 6.0
    store.close()


def test_get_trade_diagnosis_parses_json_and_non_json_safe_fields_do_not_crash(tmp_path: Path) -> None:
    store = SignalStore(db_path=str(tmp_path / "signals.db"))
    engine = TradeLearningEngine(store)
    key = "json-key"

    engine.record_outcome(
        make_signal(),
        key,
        "TP1",
        {"reasons": {"not": "a-list"}, "custom": {"bad": object()}},
    )

    diagnosis = store.get_trade_diagnosis(key)
    assert diagnosis is not None
    assert isinstance(diagnosis["success_factors"], dict)
    assert diagnosis["success_factors"]["reasons"] == ["long_promotion_rules_met"]
    store.close()


def test_pending_outcome_does_not_record_final_diagnosis(tmp_path: Path) -> None:
    store = SignalStore(db_path=str(tmp_path / "signals.db"))
    engine = TradeLearningEngine(store)
    key = "pending-key"

    engine.record_outcome(make_signal(), key, "PENDING", {})

    assert store.get_trade_lifecycle_events(key) == []
    assert store.get_trade_diagnosis(key) is None
    store.close()
