# Operations Guide

## Modes

### Safe default (recommended)
- `TRADING_ENABLED=false`
- `SIGNALS_ONLY=true`

This keeps all execution paths non-trading while scanner/store/dashboard/outcome flows remain active.

### Live trading (explicit opt-in)
Trading is enabled only when **all** are true:
- `TRADING_ENABLED=true`
- `SIGNALS_ONLY=false`
- `BYBIT_API_KEY` is set
- `BYBIT_API_SECRET` is set

## Accumulation runner startup

### Linux/macOS
```bash
./run_accumulation_v1.sh
```

Supported env knobs in launcher:
- `DASHBOARD_API_URL` (default: `http://127.0.0.1:8000`)
- `DASHBOARD_INGEST_TOKEN`
- `SIGNALS_ONLY` (default: `true`)
- `RUN_OUTCOME_TRACKER` (`true|false`, default: `false`)
- `OUTCOME_TRACKER_INTERVAL_MINUTES` (default: `10`)

### Windows
```bat
run_accumulation_v1.bat
```

## Outcome tracker

### As sidecar service (Linux launcher)
Set:
- `RUN_OUTCOME_TRACKER=true`

### Manual run
```bash
python tools/outcome_tracker.py --db data/signals.db --loop --interval-minutes 10
```

Or on Windows:
```bat
run_outcome_tracker.bat
```

## Health checks

### Compile check
```bash
python -m compileall -q .
```

### Test suite
```bash
pytest -q
```

### Signals DB readiness
```bash
python tools/final_readiness_check.py --db data/signals.db
```

## Recommended rollout checklist
1. Run in safe mode (`TRADING_ENABLED=false`, `SIGNALS_ONLY=true`) for 24–48h.
2. Verify dashboard receives heartbeats/signals.
3. Verify `signal_events` growth (`new_setup/status_changed/score_jump/repeat`).
4. Review setup performance slices by reason/TF/score bucket.
5. Only after that consider live mode with explicit `TRADING_ENABLED=true`.
