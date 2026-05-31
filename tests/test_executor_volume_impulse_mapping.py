from __future__ import annotations

from tests.test_executor_snapshot_diagnostics import make_runner, make_signal, make_snapshot


def test_executor_snapshot_prefers_explicit_meta_volume_impulse(tmp_path):
    runner = make_runner(tmp_path)
    signal = make_signal(
        meta={
            "tf": "5",
            "market": "linear",
            "volume_impulse": 1.62,
            "executor_snapshot": make_snapshot(volume_impulse=1.1),
        }
    )

    snapshot, _weak = runner._paper_executor_snapshot(signal)

    assert snapshot.volume_impulse == 1.62
    assert signal.meta["_paper_volume_impulse_diagnostics"]["volume_impulse_source"] == "meta.volume_impulse"


def test_executor_snapshot_maps_volume_ratio_alias(tmp_path):
    runner = make_runner(tmp_path)
    signal = make_signal(meta={"tf": "5", "market": "linear", "volume_ratio": 1.27})

    snapshot, _weak = runner._paper_executor_snapshot(signal)

    assert snapshot.volume_impulse == 1.27
    assert signal.meta["_paper_volume_impulse_diagnostics"]["volume_impulse_source"] == "meta.volume_ratio"


def test_executor_snapshot_marks_missing_volume_impulse_default(tmp_path):
    runner = make_runner(tmp_path)
    signal = make_signal(meta={"tf": "5", "market": "linear"}, reasons=["long_promotion_rules_met"])

    snapshot, _weak = runner._paper_executor_snapshot(signal)

    assert snapshot.volume_impulse == 1.0
    assert signal.meta["_paper_volume_impulse_diagnostics"]["volume_impulse_source"] == "missing_default"
    assert signal.meta["_paper_volume_impulse_diagnostics"]["volume_impulse_missing"] is True
