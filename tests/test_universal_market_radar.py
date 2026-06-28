from __future__ import annotations

import time

import pandas as pd

from tools.universal_market_radar import (
    HOUR_MS,
    build_bybit_spot_base_index,
    calculate_h1_metrics,
    calculate_radar_score,
    cap_bucket,
    late_impulse_reasons,
)


def make_closed_h1_frame(hours: int = 30) -> pd.DataFrame:
    current_h1_open = (int(time.time() * 1000) // HOUR_MS) * HOUR_MS
    rows = []

    for index in range(hours):
        close = 100.0 + index * 0.10

        rows.append(
            {
                "start": current_h1_open - ((hours - index) * HOUR_MS),
                "open": close - 0.05,
                "high": close + 0.50,
                "low": close - 0.50,
                "close": close,
                "volume": 1000.0,
                "turnover": 10_000.0,
            }
        )

    return pd.DataFrame(rows)


def test_cap_bucket_requires_verified_positive_market_cap():
    assert cap_bucket(10_000_000.0, "CAP_VERIFIED") == "CAP_LT_20M"
    assert cap_bucket(20_000_000.0, "CAP_VERIFIED") == "CAP_20M_100M"
    assert cap_bucket(3_000_000_000.0, "CAP_VERIFIED") == "CAP_GT_3B"

    assert cap_bucket(10_000_000.0, "CAP_MISSING") == "CAP_MISSING"
    assert cap_bucket(0.0, "CAP_VERIFIED") == "CAP_MISSING"
    assert cap_bucket(10_000_000.0, "CAP_AMBIGUOUS") == "CAP_AMBIGUOUS"


def test_bybit_spot_index_uses_only_usdt_pairs_and_keeps_ambiguity():
    markets = [
        {
            "pair": "ABC/USDT",
            "base_currency_id": "abc-alpha",
        },
        {
            "pair": "ABC/USDT",
            "base_currency_id": "abc-beta",
        },
        {
            "pair": "BTC/USDT",
            "base_currency_id": "btc-bitcoin",
        },
        {
            "pair": "ABC/USDC",
            "base_currency_id": "abc-ignored",
        },
        {
            "pair": "",
            "base_currency_id": "ignored",
        },
    ]

    index = build_bybit_spot_base_index(markets)

    assert index["ABC"] == ["abc-alpha", "abc-beta"]
    assert index["BTC"] == ["btc-bitcoin"]
    assert "ABC/USDC" not in index


def test_h1_metrics_use_closed_candles_and_return_expected_windows():
    metrics = calculate_h1_metrics(make_closed_h1_frame())

    assert metrics["h1_status"] == "ok"
    assert metrics["h1_error"] == ""
    assert metrics["h1_closed_bars"] == 30
    assert metrics["turnover1h"] == 10_000.0
    assert metrics["turnover4h"] == 40_000.0
    assert metrics["base_range_24h_pct"] > 0
    assert metrics["range_6h_pct"] > 0
    assert metrics["volume_stability"] == 1.0


def test_short_h1_history_is_not_marked_ready():
    frame = make_closed_h1_frame(hours=6)

    metrics = calculate_h1_metrics(frame)

    assert metrics["h1_status"] == "insufficient_history"
    assert metrics["h1_closed_bars"] == 6
    assert metrics["h1_error"] == "not_enough_closed_h1_bars"


def test_late_impulse_flags_only_upward_overextension():
    reasons = late_impulse_reasons(
        price24h_pct=30.0,
        return_1h_pct=6.0,
        return_4h_pct=11.0,
        return_6h_pct=13.0,
        max_24h_pct=25.0,
        max_1h_pct=5.0,
        max_4h_pct=10.0,
        max_6h_pct=12.0,
    )

    assert reasons == [
        "late_24h_move",
        "late_1h_move",
        "late_4h_move",
        "late_6h_move",
    ]

    assert late_impulse_reasons(
        price24h_pct=-35.0,
        return_1h_pct=-8.0,
        return_4h_pct=-12.0,
        return_6h_pct=-18.0,
        max_24h_pct=25.0,
        max_1h_pct=5.0,
        max_4h_pct=10.0,
        max_6h_pct=12.0,
    ) == []


def test_unverified_market_cap_never_receives_radar_score():
    assert calculate_radar_score(
        cap_status="CAP_MISSING",
        bucket="CAP_MISSING",
        liquidity_to_cap=1.0,
        base_range_24h_pct=2.0,
        max_base_range_pct=14.0,
        volume_stability=1.0,
        is_late=False,
    ) == 0.0

    assert calculate_radar_score(
        cap_status="CAP_VERIFIED",
        bucket="CAP_LT_20M",
        liquidity_to_cap=0.20,
        base_range_24h_pct=4.0,
        max_base_range_pct=14.0,
        volume_stability=0.80,
        is_late=False,
    ) > 0.0
