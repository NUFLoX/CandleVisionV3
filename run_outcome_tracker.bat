@echo off
setlocal
cd /d "%~dp0"

if "%OUTCOME_TRACKER_INTERVAL_MINUTES%"=="" set "OUTCOME_TRACKER_INTERVAL_MINUTES=10"
if "%OUTCOME_TRACKER_DB%"=="" set "OUTCOME_TRACKER_DB=data\signals.db"

python tools\outcome_tracker.py --db "%OUTCOME_TRACKER_DB%" --loop --interval-minutes %OUTCOME_TRACKER_INTERVAL_MINUTES%
