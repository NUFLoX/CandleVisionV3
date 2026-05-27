import pandas as pd
from orderflow_accum.market_regime import MarketRegimeAnalyzer
from orderflow_accum.short_engine import DistributionShortEngine
from orderflow_accum.config import Settings


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
