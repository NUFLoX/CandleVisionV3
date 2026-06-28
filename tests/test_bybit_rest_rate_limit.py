from __future__ import annotations

import asyncio
from contextlib import suppress

import pytest

import orderflow_accum.bybit_rest as bybit_rest
from orderflow_accum.bybit_rest import (
    AsyncRequestPacer,
    BybitRestClient,
)


class FakeResponse:
    def __init__(self, payload: dict):
        self.payload = payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    def raise_for_status(self) -> None:
        return None

    async def json(self) -> dict:
        return self.payload


class FakeSession:
    def __init__(self, payloads: list[dict]):
        self.payloads = list(payloads)
        self.calls = 0

    def get(self, url: str, params: dict):
        del url, params
        self.calls += 1
        return FakeResponse(
            self.payloads.pop(0)
        )


def test_retries_bybit_10006_then_returns_result():
    async def scenario():
        client = BybitRestClient(
            "https://example.invalid",
            retries=0,
            min_interval_seconds=0.0,
            rate_limit_retries=1,
            rate_limit_backoff_seconds=0.0,
        )

        session = FakeSession(
            [
                {
                    "retCode": 10006,
                    "retMsg": (
                        "Too many visits. "
                        "Exceeded the API Rate Limit."
                    ),
                    "result": {},
                },
                {
                    "retCode": 0,
                    "retMsg": "OK",
                    "result": {"list": ["ok"]},
                },
            ]
        )

        client._session = session

        result = await client._get(
            "/v5/market/kline",
            {"symbol": "BTCUSDT"},
        )

        return result, session.calls

    result, calls = asyncio.run(scenario())

    assert result == {"list": ["ok"]}
    assert calls == 2


def test_non_rate_limit_error_is_not_silently_retried():
    async def scenario():
        client = BybitRestClient(
            "https://example.invalid",
            retries=0,
            min_interval_seconds=0.0,
            rate_limit_retries=3,
            rate_limit_backoff_seconds=0.0,
        )

        session = FakeSession(
            [
                {
                    "retCode": 10001,
                    "retMsg": "Parameter error",
                    "result": {},
                },
            ]
        )

        client._session = session

        with pytest.raises(
            RuntimeError,
            match="Bybit API error",
        ):
            await client._get(
                "/v5/market/kline",
                {"symbol": "BTCUSDT"},
            )

        return session.calls

    assert asyncio.run(scenario()) == 1


def test_pacer_releases_lock_while_waiting(monkeypatch):
    async def scenario():
        pacer = AsyncRequestPacer(
            min_interval_seconds=60.0
        )

        await pacer.wait_turn()

        sleep_started = asyncio.Event()
        never_release_sleep = asyncio.Event()

        async def blocked_sleep(delay_seconds: float):
            del delay_seconds
            sleep_started.set()
            await never_release_sleep.wait()

        monkeypatch.setattr(
            bybit_rest.asyncio,
            "sleep",
            blocked_sleep,
        )

        waiter = asyncio.create_task(
            pacer.wait_turn()
        )

        await asyncio.wait_for(
            sleep_started.wait(),
            timeout=0.5,
        )

        try:
            await asyncio.wait_for(
                pacer.defer(1.0),
                timeout=0.1,
            )
        finally:
            waiter.cancel()

            with suppress(asyncio.CancelledError):
                await waiter

    asyncio.run(scenario())
