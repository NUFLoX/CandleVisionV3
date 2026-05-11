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
codex/conduct-deep-repository-audit-and-implement-changes-kyu74x

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
=======
main
