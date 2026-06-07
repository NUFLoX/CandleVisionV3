from __future__ import annotations

import json
from decimal import Decimal

from orderflow_accum.bybit_testnet_executor import (
    BYBIT_TESTNET_BASE_URL,
    ENTRY_BLOCKED_INSUFFICIENT_TESTNET_BALANCE,
    ENTRY_BLOCKED_TESTNET_MAINNET_GUARD,
    ENTRY_BLOCKED_TESTNET_POSITION_LIMIT,
    ENTRY_BLOCKED_TESTNET_QTY_TOO_SMALL,
    TestnetOrderConfig,
    BybitTestnetOrderExecutor,
    round_down_to_step,
)
from orderflow_accum.signal_store import SignalStore


class FakeBybitClient:
    def __init__(self, *, balance: str = "1000", qty_step: str = "0.001", min_qty: str = "0.001", fail: bool = False):
        self.balance = balance
        self.qty_step = qty_step
        self.min_qty = min_qty
        self.fail = fail
        self.create_order_calls: list[dict] = []
        self.wallet_calls = 0
        self.instrument_calls = 0

    def wallet_balance(self):
        self.wallet_calls += 1
        return {"result": {"list": [{"coin": [{"coin": "USDT", "availableToWithdraw": self.balance}]}]}}

    def instruments_info(self, symbol: str, category: str = "linear"):
        self.instrument_calls += 1
        return {
            "result": {
                "list": [
                    {
                        "symbol": symbol,
                        "lotSizeFilter": {"qtyStep": self.qty_step, "minOrderQty": self.min_qty, "maxOrderQty": "100000"},
                        "priceFilter": {"tickSize": "0.01"},
                    }
                ]
            }
        }

    def create_order(self, payload: dict):
        if self.fail:
            raise RuntimeError("temporary bybit error")
        self.create_order_calls.append(payload)
        return {"retCode": 0, "result": {"orderId": f"order-{len(self.create_order_calls)}"}}


def config(**overrides) -> TestnetOrderConfig:
    data = {
        "bybit_testnet": True,
        "api_key": "key",
        "api_secret": "secret",
        "trading_enabled": True,
        "signals_only": False,
        "kill_switch": False,
        "allow_mainnet": False,
        "order_notional_usdt": Decimal("100"),
        "min_free_balance_usdt": Decimal("20"),
        "max_open_positions": 3,
        "daily_max_orders": 30,
        "order_type": "Market",
        "base_url": BYBIT_TESTNET_BASE_URL,
        "category": "linear",
    }
    data.update(overrides)
    return TestnetOrderConfig(**data)


def make_executor(tmp_path, *, cfg: TestnetOrderConfig | None = None, client: FakeBybitClient | None = None):
    store = SignalStore(db_path=str(tmp_path / "signals.db"))
    executor = BybitTestnetOrderExecutor(store, config=cfg or config(), client=client or FakeBybitClient())
    return executor, store


def test_mainnet_guard_blocks_when_bybit_testnet_false_or_allow_mainnet_true(tmp_path):
    for cfg in (config(bybit_testnet=False), config(allow_mainnet=True)):
        client = FakeBybitClient()
        executor, store = make_executor(tmp_path, cfg=cfg, client=client)
        result = executor.place_entry_order(signal_key=f"sig-{cfg.bybit_testnet}", trade_key="trade", symbol="ETHUSDT", price=100)
        assert result["reason"] == ENTRY_BLOCKED_TESTNET_MAINNET_GUARD
        assert client.create_order_calls == []
        store.close()


def test_no_order_unless_trading_enabled(tmp_path):
    client = FakeBybitClient()
    executor, store = make_executor(tmp_path, cfg=config(trading_enabled=False), client=client)

    result = executor.place_entry_order(signal_key="sig-disabled", trade_key="trade-disabled", symbol="ETHUSDT", price=100)

    assert result["status"] == "blocked"
    assert client.create_order_calls == []
    store.close()


def test_order_notional_never_exceeds_100_usdt(tmp_path):
    client = FakeBybitClient(qty_step="0.001")
    executor, store = make_executor(tmp_path, cfg=config(order_notional_usdt=Decimal("100")), client=client)

    result = executor.place_entry_order(signal_key="sig-notional", trade_key="trade-notional", symbol="ETHUSDT", price=33)

    assert result["ok"] is True
    assert result["notional_usdt"] == Decimal("100")
    assert Decimal(client.create_order_calls[0]["qty"]) * Decimal("33") <= Decimal("100")
    store.close()


def test_insufficient_balance_blocks_entry(tmp_path):
    client = FakeBybitClient(balance="19")
    executor, store = make_executor(tmp_path, client=client)

    result = executor.place_entry_order(signal_key="sig-balance", trade_key="trade-balance", symbol="ETHUSDT", price=100)

    assert result["reason"] == ENTRY_BLOCKED_INSUFFICIENT_TESTNET_BALANCE
    assert client.create_order_calls == []
    store.close()


def test_position_limit_blocks_entry(tmp_path):
    executor, store = make_executor(tmp_path, cfg=config(max_open_positions=1))
    store.insert_testnet_order(
        {
            "signal_key": "existing",
            "trade_key": "existing-trade",
            "symbol": "BTCUSDT",
            "category": "linear",
            "side": "Buy",
            "order_type": "Market",
            "qty": "0.01",
            "notional_usdt": "100",
            "price": "10000",
            "status": "placed",
            "reason": "testnet_entry_order_placed",
        }
    )

    result = executor.place_entry_order(signal_key="sig-limit", trade_key="trade-limit", symbol="ETHUSDT", price=100)

    assert result["reason"] == ENTRY_BLOCKED_TESTNET_POSITION_LIMIT
    store.close()


def test_qty_rounds_down_to_instrument_step(tmp_path):
    client = FakeBybitClient(qty_step="0.1")
    executor, store = make_executor(tmp_path, client=client)

    result = executor.place_entry_order(signal_key="sig-step", trade_key="trade-step", symbol="ETHUSDT", price=33)

    assert result["qty"] == Decimal("3.0")
    assert client.create_order_calls[0]["qty"] == "3"
    assert round_down_to_step(Decimal("3.09"), Decimal("0.1")) == Decimal("3.0")
    store.close()


def test_qty_below_min_order_qty_blocks_entry(tmp_path):
    client = FakeBybitClient(qty_step="0.001", min_qty="2")
    executor, store = make_executor(tmp_path, client=client)

    result = executor.place_entry_order(signal_key="sig-small", trade_key="trade-small", symbol="ETHUSDT", price=100)

    assert result["reason"] == ENTRY_BLOCKED_TESTNET_QTY_TOO_SMALL
    assert client.create_order_calls == []
    store.close()


def test_duplicate_signal_key_does_not_place_duplicate_orders(tmp_path):
    client = FakeBybitClient()
    executor, store = make_executor(tmp_path, client=client)

    first = executor.place_entry_order(signal_key="sig-dupe", trade_key="trade-dupe", symbol="ETHUSDT", price=100)
    second = executor.place_entry_order(signal_key="sig-dupe", trade_key="trade-dupe", symbol="ETHUSDT", price=100)

    assert first["ok"] is True
    assert second["status"] == "blocked"
    assert len(client.create_order_calls) == 1
    store.close()


def test_bybit_api_error_does_not_crash_scanner(tmp_path):
    client = FakeBybitClient(fail=True)
    executor, store = make_executor(tmp_path, client=client)

    result = executor.place_entry_order(signal_key="sig-error", trade_key="trade-error", symbol="ETHUSDT", price=100)

    assert result["status"] == "failed"
    assert result["reason"] == "entry_failed_testnet_api"
    store.close()


def test_exit_creates_reduce_only_close_order_for_existing_testnet_position(tmp_path):
    client = FakeBybitClient()
    executor, store = make_executor(tmp_path, client=client)
    entry = executor.place_entry_order(signal_key="sig-exit", trade_key="trade-exit", symbol="ETHUSDT", price=100)

    result = executor.place_exit_order(signal_key="sig-exit", trade_key="trade-exit", symbol="ETHUSDT", price=105)

    assert entry["ok"] is True
    assert result["ok"] is True
    assert client.create_order_calls[-1]["side"] == "Sell"
    assert client.create_order_calls[-1]["reduceOnly"] is True
    assert client.create_order_calls[-1]["qty"] == "1"
    orders = store.list_testnet_orders(limit=10)
    assert any(row["side"] == "Sell" and row["status"] == "placed" for row in orders)
    store.close()


def test_testnet_orders_table_stores_request_and_response_json(tmp_path):
    client = FakeBybitClient()
    executor, store = make_executor(tmp_path, client=client)

    executor.place_entry_order(signal_key="sig-json", trade_key="trade-json", symbol="ETHUSDT", price=100)

    row = store.list_testnet_orders(limit=1)[0]
    assert row["order_id"] == "order-1"
    assert json.loads(row["request_json"])["category"] == "linear"
    assert json.loads(row["response_json"])["result"]["orderId"] == "order-1"
    store.close()
