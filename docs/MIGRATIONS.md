# Signals DB Migrations

`orderflow_accum.signal_store.SignalStore` uses lightweight schema migration with:
- `CREATE TABLE IF NOT EXISTS ...`
- additive `ALTER TABLE` for missing columns
- `PRAGMA user_version` for schema version marker

## Current schema version
- `SCHEMA_VERSION = 2`

## What version 2 includes
- `signals` table with outcome/performance columns
- `signal_events` table
- lifecycle event recording (`new_setup`, `status_changed`, `score_jump`, `repeat`)

## Upgrade behavior
On startup, store initialization:
1. Reads `PRAGMA user_version`
2. Ensures required tables/indexes/columns exist
3. Sets `PRAGMA user_version = SCHEMA_VERSION` when lower

## Operational advice
- Keep periodic backups of `data/signals.db`
- On major future schema changes, introduce explicit migration scripts in `tools/` and bump `SCHEMA_VERSION`
- Avoid destructive migrations in runtime path
