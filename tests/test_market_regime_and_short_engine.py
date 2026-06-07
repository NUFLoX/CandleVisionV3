import pandas as pd
from orderflow_accum.indicators import add_indicators
from orderflow_accum.market_regime import MarketRegimeAnalyzer
from orderflow_accum.short_engine import DistributionShortEngine
from orderflow_accum.config import Settings



def test_raw_falling_btc_frame_is_enriched_before_regime_analysis():
    raw = pd.DataFrame(
        {
            "open": [100, 98, 96, 94, 92, 90, 88, 86],
            "high": [101, 99, 97, 95, 93, 91, 89, 87],
            "low": [97, 95, 93, 91, 89, 87, 85, 83],
            "close": [98, 96, 94, 92, 90, 88, 86, 84],
            "volume": [10, 11, 12, 13, 14, 15, 16, 17],
            "turnover": [1000, 1050, 1100, 1150, 1200, 1250, 1300, 1350],
        }
    )

    enriched = add_indicators(raw)
    regime = MarketRegimeAnalyzer().analyze_btc({"60": enriched})

    assert {"ema_20", "ema_50", "close_pos", "atr_14", "volume_ratio"}.issubset(enriched.columns)
    assert regime.btc_regime in {"BTC_BEARISH", "BTC_DUMP_RISK"}
    assert regime.btc_regime != "BTC_NEUTRAL"

def test_btc_bearish_enables_short_engine():
    df = pd.DataFrame({"close":[100,99,98,97,96],"ema_20":[101,100,99,98,97],"ema_50":[102,101,100,99,98],"close_pos":[0.4,0.4,0.4,0.4,0.4]})
    regime = MarketRegimeAnalyzer().analyze_btc({"60":df})
    assert regime.shorts_enabled is True


def test_distribution_short_signal_side_sell_smoke():
    s = Settings()
    eng = DistributionShortEngine(s)
    class State:
        trades=[]
    out = eng.analyze("X", pd.DataFrame(), State(), None)
    assert out == []
