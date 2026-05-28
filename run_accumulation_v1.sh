#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

python3 -m venv .venv
source .venv/bin/activate

python -m pip install --upgrade pip
python -m pip install -r requirements.txt

[[ -f .env ]] || cp .env.example .env

export DASHBOARD_API_URL="${DASHBOARD_API_URL:-http://127.0.0.1:8000}"
export DASHBOARD_INGEST_TOKEN="${DASHBOARD_INGEST_TOKEN:-}"
export SIGNALS_ONLY="${SIGNALS_ONLY:-true}"
export RUN_OUTCOME_TRACKER="${RUN_OUTCOME_TRACKER:-false}"
export OUTCOME_TRACKER_INTERVAL_MINUTES="${OUTCOME_TRACKER_INTERVAL_MINUTES:-10}"
export PYTHONPATH="$(pwd)"

echo "DASHBOARD_API_URL=$DASHBOARD_API_URL"
echo "SIGNALS_ONLY=$SIGNALS_ONLY"
echo "RUN_OUTCOME_TRACKER=$RUN_OUTCOME_TRACKER"
if [[ "$RUN_OUTCOME_TRACKER" == "true" ]]; then
  python tools/outcome_tracker.py --db data/signals.db --loop --interval-minutes "$OUTCOME_TRACKER_INTERVAL_MINUTES" &
fi

python orderflow_accum_main.py

