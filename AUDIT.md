# CandleVisionV3 repository audit

Date: 2026-05-11

## Deep audit summary

1. **Runtime artifacts were under-protected.**
   The repository ignored some local files, but not the active dashboard state file, pytest/mypy/ruff caches, `.venv/`, or the new signal-outcome SQLite runtime files. This could leak local state or create noisy commits.

2. **A stray tracked file was present.**
   The file `how HEAD --name-only` was tracked even though it is a command typo artifact and not part of the application.

3. **Dashboard schemas were too strict for ingest/runtime drift.**
   Several dashboard payload models required every field, which made ingest brittle when upstream scanner payloads were partial or when persisted JSON was missing fields.

4. **Ingest endpoints lacked request authentication.**
   `/api/ingest/*` accepted writes without a shared secret. This was acceptable only on a trusted localhost network and unsafe if the dashboard is exposed.

5. **Signal performance was not measured.**
   Signals were persisted, but the backend did not replay candles after a signal to determine TP/SL/expiry outcomes, R-multiples, or aggregate performance.

6. **Bybit kline clients needed historical windows.**
   Kline helpers accepted `limit` only. Outcome analysis needs deterministic `start`/`end` windows based on signal creation time.

7. **Frontend lacked performance feedback.**
   Signal cards displayed live signal metadata but no historical outcome badge, summary cards, refresh workflow, or breakdown tables.

## Changes made in this branch

- Hardened `.gitignore` for virtualenvs, caches, dashboard JSON state, SQLite databases, and outcome/runtime artifacts.
- Removed the stray tracked `how HEAD --name-only` file.
- Added dashboard environment variables to `.env.example`, including ingest token, signal stats database, lookahead bars, and Bybit public base URL.
- Added safe defaults to dashboard schemas so partial live payloads and persisted state are less likely to crash validation.
- Added optional `DASHBOARD_INGEST_TOKEN` Bearer authentication for all `/api/ingest/*` endpoints and taught the ingest client to send the token when configured.
- Added `SignalOutcome` and `SignalStatsSummary` schemas.
- Added `dashboard/signal_outcomes.py` with TP/SL/ambiguous/expired classification, R calculation, aggregate stats, SQLite persistence, and Bybit-backed refresh.
- Extended Bybit kline fetchers to accept `start` and `end` timestamps.
- Added API endpoints:
  - `GET /api/signal-outcomes`
  - `GET /api/signal-stats`
  - `POST /api/signal-outcomes/refresh`
- Added unit tests for long TP before SL, long SL before TP, short TP before SL, ambiguous same-candle outcomes, expiry, R calculation, and aggregate stats.
- Added a frontend Signal Performance panel with `loadStats()`, a Refresh stats button, summary cards, breakdown tables, recent outcome rows, and outcome badges on signal cards.

## Operational notes

- `DASHBOARD_INGEST_TOKEN` is optional for local development. When it is set, ingest requests must include `Authorization: Bearer <token>`.
- Signal outcome refresh fetches public Bybit linear klines. It should be run where outbound access to Bybit is allowed.
- The signal stats SQLite database is runtime state and intentionally ignored by git.

## Remaining recommendations

- Move dashboard state and signal stats from local JSON/SQLite to PostgreSQL/Redis for multi-process production deployments.
- Add pagination and filters to `/api/signal-outcomes` before the outcome table grows large.
- Consider storing raw candle windows or refresh metadata for auditability of historical outcome decisions.
- Decide a product policy for ambiguous same-candle outcomes; this branch uses a conservative neutral `0R` classification.
- Require `DASHBOARD_INGEST_TOKEN` in deployment manifests and avoid exposing write endpoints without HTTPS.

## 2026-05-11 live-trading safety follow-up

Implemented without changing the signal discovery/scoring path:

- `SIGNALS_ONLY` is now a hard Executor guard: when enabled, the Executor can still report a valid signal but will not place an exchange order or write an active trade.
- WebSocket sniper no longer calls `Executor.process_signal_async()` directly; it queues a signal candidate for the existing orchestrator/executor queue path.
- Tape Reader now uses the shared `BYBIT_WS_PUBLIC_URL` setting instead of a hardcoded testnet WebSocket URL.
- Exchange execution now creates `orderLinkId`, checks local/exchange duplicates, fetches instrument rules, normalizes qty/price to Bybit filters, enforces min qty/min notional, and verifies order/position status before persisting a trade as `pending_order` or `open`.
- Startup now reconciles locally active trades with Bybit positions/open orders when live execution is enabled.
- `/api/signal-outcomes/refresh` now uses the same bearer-token dependency as ingest when `DASHBOARD_INGEST_TOKEN` is configured.

Still recommended for separate follow-up PRs:

- Shared process-wide Bybit REST session, rate limiter, and circuit breaker.
- Full dashboard auth/CORS policy for non-local deployments.
- Moving tracked historical CSV/PNG artifacts out of the repository history/index.
- Legacy cleanup for unused scanner/executor modules.
- P2 signal-quality telemetry (A/B/C labels, rr_fallback stats, rejection stats, MFE/MAE/time-to-*), initially in observe-only mode.

## 2026-05-25 full file-by-file audit pass

- Scope: reviewed repository file inventory end-to-end (127 files total), including source code, configs, scripts, tests, docs, CSV data, and PNG artifacts.
- Automated checks run across all Python modules: AST parse, `bare except` detection, and risky `eval/exec` scan.
- Result: no Python syntax errors; two `bare except` usages found (see findings).

### Findings (current)
1. `api/ws_stream.py:74` uses `except:`; this can suppress unexpected runtime failures and complicate incident debugging.
2. `main.py:132` uses `except:` in the order-placement path; failures may be swallowed without type-specific handling.

### File inventory by extension
- `.bat`: 2
- `.csv`: 2
- `.example`: 2
- `.html`: 1
- `.md`: 2
- `.png`: 21
- `.py`: 92
- `.sh`: 2
- `.txt`: 2
- `<noext>`: 1

### File inventory by top-level area
- `.`: 20
- `accum_charts`: 21
- `agents`: 10
- `api`: 7
- `brain`: 4
- `config`: 3
- `core`: 11
- `dashboard`: 10
- `monitor`: 3
- `orderflow_accum`: 13
- `orderflow_v1`: 12
- `scoring`: 2
- `scout`: 6
- `tests`: 4
- `watchlist`: 1

### Complete reviewed file list
- `.env.example`
- `.gitignore`
- `AUDIT.md`
- `README.md`
- `VERSION.txt`
- `accum_charts/ADAUSDT_00cfb0c331.png`
- `accum_charts/BTCUSDT_14e0bb4140.png`
- `accum_charts/BTCUSDT_63ef35299b.png`
- `accum_charts/BTCUSDT_cbabf8d760.png`
- `accum_charts/DOGEUSDT_4e9d541fbb.png`
- `accum_charts/DOGEUSDT_8ce2a19a2c.png`
- `accum_charts/ENAUSDT_2a3ad37153.png`
- `accum_charts/ETHUSDT_6d064fbff7.png`
- `accum_charts/ETHUSDT_aae1a5956c.png`
- `accum_charts/ICPUSDT_600d6ce419.png`
- `accum_charts/NEARUSDT_cfa0db85de.png`
- `accum_charts/SOLUSDT_0a18cf9659.png`
- `accum_charts/SOLUSDT_1977bac98d.png`
- `accum_charts/SOLUSDT_a1a779fe40.png`
- `accum_charts/SUIUSDT_71bf0039c4.png`
- `accum_charts/SUIUSDT_de1726f57e.png`
- `accum_charts/XRPUSDT_459b9c7742.png`
- `accum_charts/XRPUSDT_528f23f7b1.png`
- `accum_charts/XRPUSDT_67d8f7246f.png`
- `accum_charts/XRPUSDT_a0e5732316.png`
- `accum_charts/XRPUSDT_ff25eed044.png`
- `accumulation_signals.csv`
- `agents/__init__.py`
- `agents/brain.py`
- `agents/macro.py`
- `agents/notifier.py`
- `agents/sentinel_btc.py`
- `agents/sentinel_onchain.py`
- `agents/sentinel_sentiment.py`
- `agents/sentinel_tape.py`
- `agents/sentinel_telegram.py`
- `agents/sonar.py`
- `analyzer.py`
- `api/__init__.py`
- `api/bybit_client.py`
- `api/charting.py`
- `api/exchange_gateway.py`
- `api/market.py`
- `api/telegram.py`
- `api/ws_stream.py`
- `backtester.py`
- `brain/__init__.py`
- `brain/db.py`
- `brain/models.py`
- `brain/queue.py`
- `check_prices.py`
- `config/.env.example`
- `config/__init__.py`
- `config/settings.py`
- `core/__init__.py`
- `core/database.py`
- `core/execution/safe_executor.py`
- `core/executor.py`
- `core/indicators.py`
- `core/orchestrator.py`
- `core/order_manager.py`
- `core/risk_manager.py`
- `core/scout.py`
- `core/sentinel.py`
- `core/triup.py`
- `dashboard/__init__.py`
- `dashboard/health.py`
- `dashboard/ingest_client.py`
- `dashboard/live_data.py`
- `dashboard/persistence.py`
- `dashboard/schemas.py`
- `dashboard/server.py`
- `dashboard/signal_outcomes.py`
- `dashboard/static/index.html`
- `dashboard/store.py`
- `main.py`
- `monitor/__init__.py`
- `monitor/logger.py`
- `monitor/stats.py`
- `orderflow_accum/__init__.py`
- `orderflow_accum/bookflow.py`
- `orderflow_accum/bybit_rest.py`
- `orderflow_accum/chart_render.py`
- `orderflow_accum/config.py`
- `orderflow_accum/console_ui.py`
- `orderflow_accum/engines.py`
- `orderflow_accum/indicators.py`
- `orderflow_accum/models.py`
- `orderflow_accum/runner.py`
- `orderflow_accum/signal_logger.py`
- `orderflow_accum/telegram_notify.py`
- `orderflow_accum/ws_clients.py`
- `orderflow_accum_main.py`
- `orderflow_v1/__init__.py`
- `orderflow_v1/bookflow.py`
- `orderflow_v1/bybit_rest.py`
- `orderflow_v1/config.py`
- `orderflow_v1/console_ui.py`
- `orderflow_v1/engines.py`
- `orderflow_v1/indicators.py`
- `orderflow_v1/models.py`
- `orderflow_v1/runner.py`
- `orderflow_v1/signal_logger.py`
- `orderflow_v1/telegram_notify.py`
- `orderflow_v1/ws_clients.py`
- `orderflow_v1_main.py`
- `rejection_reasons.csv`
- `requirements.txt`
- `run_accumulation_v1.bat`
- `run_accumulation_v1.sh`
- `run_orderflow_v1.bat`
- `run_orderflow_v1.sh`
- `scoring/__init__.py`
- `scoring/scorer.py`
- `scout/__init__.py`
- `scout/scanner.py`
- `scout/strategies/__init__.py`
- `scout/strategies/classic.py`
- `scout/strategies/pump.py`
- `scout/strategies/squeeze.py`
- `test_api.py`
- `test_bybit_correct.py`
- `tests/test_dashboard_live_data.py`
- `tests/test_dashboard_server_import.py`
- `tests/test_execution_safety.py`
- `tests/test_signal_outcomes.py`
- `watchlist/watchlist_manager.py`

## 2026-06-03 signal taxonomy metadata audit

Scope: audited signal discovery, signal persistence, dashboard ingest, setup-performance grouping, signal outcome calculation, and `SmartTradeExecutor` entry decision paths for the requested taxonomy-only PR.

Constraint honored: this change does **not** modify signal generation rules, scanner conditions, score formulas, thresholds, filters, confirmed promotion rules, watchlist selection, `SmartTradeExecutor` decisions, TP/SL logic, or orderflow calculations.

Changes in this PR are metadata/dashboard grouping only:

- Added dashboard-only taxonomy labels: `signal_kind`, `signal_family`, `signal_focus_group`, `signal_source`, and `signal_timeframe`.
- Derived taxonomy labels from already-existing signal `kind`, `source`, and timeframe metadata; no scanner or executor code consumes these labels.
- Added setup-performance dashboard groupings by taxonomy family and focus group, while keeping existing kind/source/timeframe score and outcome groupings unchanged.
- Added invariance tests proving taxonomy labels do not change stored signal status, score fields, outcome calculation, or `SmartTradeExecutor` decisions.

Audit result: for the same scanner/database input, signal keys, signal status transitions, scores, outcomes, and executor entry decisions remain based on the pre-existing fields and logic. Taxonomy labels are descriptive dashboard metadata only.
