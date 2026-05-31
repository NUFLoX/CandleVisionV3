from pathlib import Path


LAUNCHER = Path("run_accumulation_v1.bat")


def _launcher_text() -> str:
    return LAUNCHER.read_text(encoding="utf-8")


def test_run_accumulation_bat_contains_report_defaults() -> None:
    text = _launcher_text()

    assert 'set "RUN_LEARNING_REPORT=false"' in text
    assert 'set "LEARNING_REPORT_INTERVAL_SECONDS=3600"' in text
    assert 'set "LEARNING_REPORT_DB=data\\signals.db"' in text
    assert 'set "LEARNING_REPORT_OUT_DIR=reports_learning"' in text
    assert 'set "LEARNING_REPORT_SINCE_HOURS=24"' in text
    assert 'set "LEARNING_REPORT_MIN_SAMPLE=5"' in text
    assert 'set "RUN_WATCHLIST_REPORT=false"' in text
    assert 'set "RUN_REPORT_SCHEDULER=false"' in text
    assert 'set "REPORT_SCHEDULER_POLL_SECONDS=30"' in text
    assert 'set "SCHEDULE_LEARNING_REPORT=true"' in text
    assert 'set "LEARNING_REPORT_EVERY_MINUTES=60"' in text
    assert 'set "WATCHLIST_REPORT_INTERVAL_SECONDS=3600"' in text
    assert 'set "SCHEDULE_WATCHLIST_REPORT=true"' in text
    assert 'set "WATCHLIST_REPORT_AT=23:30"' in text
    assert 'set "WATCHLIST_REPORT_DB=data\\signals.db"' in text
    assert 'set "WATCHLIST_REPORT_OUT_DIR=reports_watchlist"' in text


def test_run_accumulation_bat_preserves_safe_runtime_defaults() -> None:
    text = _launcher_text()

    assert 'set "RUN_DASHBOARD=true"' in text
    assert 'set "RUN_SCANNER=true"' in text
    assert 'set "RUN_OUTCOME_TRACKER=false"' in text
    assert 'set "RUN_TRADE_EXECUTOR=true"' in text
    assert 'set "TRADE_EXECUTOR_MODE=paper"' in text
    assert 'set "SIGNALS_ONLY=true"' in text
    assert 'set "TRADING_ENABLED=false"' in text


def test_report_scheduler_starts_only_behind_flag() -> None:
    text = _launcher_text()
    guard = 'if /I "%RUN_REPORT_SCHEDULER%"=="true" ('
    start = 'start "CandleVision Report Scheduler" cmd /k python -m tools.report_scheduler'

    assert 'set "RUN_REPORT_SCHEDULER=false"' in text
    assert guard in text
    assert start in text
    assert text.index(guard) < text.index(start) < text.index("REM Optional learning report sidecar")


def test_learning_report_starts_only_behind_flag() -> None:
    text = _launcher_text()
    guard = 'if /I "%RUN_LEARNING_REPORT%"=="true" ('
    start = 'start "CandleVision Learning Report"'
    command = (
        "python -m tools.learning_report --db %LEARNING_REPORT_DB% "
        "--out-dir %LEARNING_REPORT_OUT_DIR% --since-hours %LEARNING_REPORT_SINCE_HOURS% "
        "--min-sample %LEARNING_REPORT_MIN_SAMPLE%"
    )

    assert guard in text
    assert start in text
    assert command in text
    assert text.index(guard) < text.index(start) < text.index("REM Optional watchlist transition study sidecar")


def test_watchlist_report_starts_only_behind_flag() -> None:
    text = _launcher_text()
    guard = 'if /I "%RUN_WATCHLIST_REPORT%"=="true" ('
    start = 'start "CandleVision Watchlist Report"'
    command = (
        "python .\\tools\\watchlist_transition_study.py --db %WATCHLIST_REPORT_DB% "
        "--out-dir %WATCHLIST_REPORT_OUT_DIR%"
    )

    assert guard in text
    assert start in text
    assert command in text
    assert text.index(guard) < text.index(start) < text.index("REM Start scanner")
