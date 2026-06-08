from __future__ import annotations

from orderflow_accum.config import Settings
from orderflow_accum.bybit_testnet_executor import TestnetOrderConfig


def test_market_data_testnet_false_uses_production_public_endpoints(monkeypatch):
    monkeypatch.setenv("BYBIT_TESTNET", "true")
    monkeypatch.setenv("BYBIT_MARKET_DATA_TESTNET", "false")

    settings = Settings()

    assert settings.bybit_testnet is True
    assert settings.bybit_market_data_testnet is False
    assert settings.rest_base_url == "https://api.bybit.com"
    assert settings.ws_public_url == "wss://stream.bybit.com/v5/public/linear"


def test_market_data_testnet_true_uses_testnet_market_endpoints(monkeypatch):
    monkeypatch.setenv("BYBIT_TESTNET", "false")
    monkeypatch.setenv("BYBIT_MARKET_DATA_TESTNET", "true")

    settings = Settings()

    assert settings.bybit_testnet is False
    assert settings.bybit_market_data_testnet is True
    assert settings.rest_base_url == "https://api-testnet.bybit.com"
    assert settings.ws_public_url == "wss://stream-testnet.bybit.com/v5/public/linear"


def test_testnet_trade_mode_keeps_testnet_order_executor_with_mainnet_market_data(monkeypatch):
    monkeypatch.setenv("TRADE_EXECUTOR_MODE", "testnet")
    monkeypatch.setenv("BYBIT_TESTNET", "true")
    monkeypatch.setenv("BYBIT_MARKET_DATA_TESTNET", "false")
    monkeypatch.setenv("BYBIT_API_KEY", "testnet-key")
    monkeypatch.setenv("BYBIT_API_SECRET", "testnet-secret")
    monkeypatch.setenv("TRADING_ENABLED", "true")
    monkeypatch.setenv("SIGNALS_ONLY", "false")
    monkeypatch.setenv("TESTNET_ALLOW_MAINNET", "false")

    settings = Settings()
    order_config = TestnetOrderConfig.from_env()

    assert settings.rest_base_url == "https://api.bybit.com"
    assert settings.ws_public_url == "wss://stream.bybit.com/v5/public/linear"
    assert settings.trade_executor_mode == "testnet"
    assert order_config.can_trade is True
    assert order_config.bybit_testnet is True
    assert order_config.allow_mainnet is False
    assert order_config.base_url == "https://api-testnet.bybit.com"
