# CandleVisionV3 repository audit

Date: 2026-05-08

## Key findings

1. **Dashboard showed synthetic market data by default.**
   `dashboard/store.py` seeded logs, signals, pressure strips, watchlist, trades and coin metrics from `_demo_*` helpers. This made a fresh bot look profitable/active even when no scanner or exchange data had arrived.

2. **Coin analytics mixed unavailable metrics with hard-coded values.**
   The dashboard displayed market cap, CEX netflow and whale activity as if they were live. Bybit public derivatives endpoints do not expose these fields directly, so they must be marked unavailable or sourced from a dedicated provider.

3. **Live bot state and dashboard state were disconnected.**
   The ingest API existed, but the default UI state did not make it clear which records came from the bot and which records were placeholders.

4. **Operational status was optimistic.**
   Status fields defaulted to `online`/`OK` even before checking Bybit, database, Redis, Telegram or executor availability.

## Changes made in this branch

- Removed synthetic dashboard seed data from default startup state.
- Added a live data loader that pulls public Bybit tickers, klines and orderbook data for coin analytics.
- Added CoinGecko global market data for dominance/pressure strips, with a Bybit-derived fallback.
- Added periodic dashboard refresh and a manual `/api/refresh` endpoint.
- Updated the frontend copy and empty states so users can see when signals/watchlist/trades are absent instead of seeing demo entries.

## Remaining recommendations

- Persist ingested events in PostgreSQL/Redis so dashboard state survives process restarts.
- Add explicit health checks for Telegram bot delivery, executor loop, database and Redis rather than relying on config labels.
- Add a dedicated provider for market cap, exchange netflow and whale activity if these metrics are required.
- Add authentication to ingest and dashboard endpoints before exposing this service outside a trusted network.
- Add automated tests with mocked Bybit/CoinGecko responses to prevent regressions in live-data parsing.
