# Roadmap Closure Checklist

This document tracks closure evidence for the final stage of the pre-impulse/signal-store rollout.

## Completed

- Trading guard enforced in execution paths (`trading_enabled`) and covered by tests.
- Multi-TF realtime scanning and TF-aware dedupe/store behavior covered by tests.
- `signals.db` persistence with `signals` + `signal_events` lifecycle logging.
- Outcome tracking with `TP1/TP2/SL/AMBIGUOUS/PENDING/EXPIRED` support.
- Dashboard endpoints for active setups and setup performance.
- Setup performance backend/frontend now includes reason, score bucket, timeframe, kind, source slices.

## Closure evidence

- Tests:
  - `tests/test_trading_guard_paths.py`
  - `tests/test_bybit_trading_guard.py`
  - `tests/test_realtime_intervals_usage.py`
  - `tests/test_signal_store_key_dimensions.py`
  - `tests/test_signal_store_events.py`
  - `tests/test_outcome_tracker_logic.py`
  - `tests/test_outcome_tracker_parsing.py`
  - `tests/test_dashboard_active_setups.py`
  - `tests/test_dashboard_setup_performance.py`
- Runtime checker:
  - `tools/final_readiness_check.py --db data/signals.db`

## Operational next step (manual)

- Run 24–48h live signals-only observation window and compare quality metrics against baseline:
  - win-rate
  - TP/SL distribution
  - reason/timeframe effectiveness
  - duplicate signal suppression ratio


## Documentation

- `docs/OPERATIONS.md` (runbook, safe/live mode, sidecar outcome tracker)
- `docs/MIGRATIONS.md` (schema versioning and migration policy)

