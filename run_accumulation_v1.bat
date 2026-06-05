@echo off
setlocal
chcp 65001 >nul

set "PROJECT_DIR=%~dp0"
cd /d "%PROJECT_DIR%"

echo [1/6] Project dir: %CD%

where py >nul 2>nul
if %errorlevel%==0 (
    set "PY_CMD=py -3"
) else (
    where python >nul 2>nul
    if %errorlevel%==0 (
        set "PY_CMD=python"
    ) else (
        echo Python not found. Install Python 3.11+ and add it to PATH.
        pause
        exit /b 1
    )
)

echo [2/6] Using: %PY_CMD%

if not exist ".venv\Scripts\python.exe" (
    echo [3/6] Creating virtual environment...
    %PY_CMD% -m venv .venv
    if errorlevel 1 (
        echo Failed to create virtual environment.
        pause
        exit /b 1
    )
) else (
    echo [3/6] Virtual environment already exists.
)

call ".venv\Scripts\activate.bat"
if errorlevel 1 (
    echo Failed to activate virtual environment.
    pause
    exit /b 1
)

echo [4/6] Upgrading pip...
python -m pip install --upgrade pip
if errorlevel 1 (
    echo Pip upgrade failed.
    pause
    exit /b 1
)

echo [5/6] Installing dependencies...
python -m pip install -r requirements.txt
if errorlevel 1 (
    echo Dependency installation failed.
    pause
    exit /b 1
)

if not exist ".env" (
    echo [6/6] Creating .env from .env.example...
    copy /Y ".env.example" ".env" >nul
) else (
    echo [6/6] .env already exists.
)

REM ==================================================
REM Runtime environment
REM ==================================================

if "%RUN_DASHBOARD%"=="" (
    set "RUN_DASHBOARD=true"
)

if "%RUN_SCANNER%"=="" (
    set "RUN_SCANNER=true"
)

if "%RUN_OUTCOME_TRACKER%"=="" (
    set "RUN_OUTCOME_TRACKER=false"
)

if "%RUN_LEARNING_REPORT%"=="" (
    set "RUN_LEARNING_REPORT=false"
)

if "%RUN_WATCHLIST_REPORT%"=="" (
    set "RUN_WATCHLIST_REPORT=false"
)

if "%RUN_REPORT_SCHEDULER%"=="" (
    set "RUN_REPORT_SCHEDULER=false"
)

if "%RUN_TRADE_EXECUTOR%"=="" (
    set "RUN_TRADE_EXECUTOR=true"
)

if "%TRADE_EXECUTOR_MODE%"=="" (
    set "TRADE_EXECUTOR_MODE=paper"
)

if "%EXECUTOR_MANAGEMENT_POLICY%"=="" (
    set "EXECUTOR_MANAGEMENT_POLICY=trailing_40pct_giveback_after_1r"
)

if "%EXECUTOR_PROTECT_AFTER_1R%"=="" (
    set "EXECUTOR_PROTECT_AFTER_1R=true"
)

if "%EXECUTOR_MIN_PROTECTED_R_AFTER_1R%"=="" (
    set "EXECUTOR_MIN_PROTECTED_R_AFTER_1R=0.25"
)

if "%DASHBOARD_HOST%"=="" (
    set "DASHBOARD_HOST=127.0.0.1"
)

if "%DASHBOARD_PORT%"=="" (
    set "DASHBOARD_PORT=8000"
)

if "%DASHBOARD_API_URL%"=="" (
    set "DASHBOARD_API_URL=http://%DASHBOARD_HOST%:%DASHBOARD_PORT%"
)

if "%DASHBOARD_INGEST_TOKEN%"=="" (
    set "DASHBOARD_INGEST_TOKEN="
)

if "%SIGNALS_ONLY%"=="" (
    set "SIGNALS_ONLY=true"
)

if "%TRADING_ENABLED%"=="" (
    set "TRADING_ENABLED=false"
)

if "%OUTCOME_TRACKER_INTERVAL_MINUTES%"=="" (
    set "OUTCOME_TRACKER_INTERVAL_MINUTES=10"
)

if "%REPORT_SCHEDULER_POLL_SECONDS%"=="" (
    set "REPORT_SCHEDULER_POLL_SECONDS=30"
)

if "%LEARNING_REPORT_INTERVAL_SECONDS%"=="" (
    set "LEARNING_REPORT_INTERVAL_SECONDS=3600"
)

if "%SCHEDULE_LEARNING_REPORT%"=="" (
    set "SCHEDULE_LEARNING_REPORT=true"
)

if "%LEARNING_REPORT_EVERY_MINUTES%"=="" (
    set "LEARNING_REPORT_EVERY_MINUTES=60"
)

if "%LEARNING_REPORT_DB%"=="" (
    set "LEARNING_REPORT_DB=data\signals.db"
)

if "%LEARNING_REPORT_OUT_DIR%"=="" (
    set "LEARNING_REPORT_OUT_DIR=reports_learning"
)

if "%LEARNING_REPORT_SINCE_HOURS%"=="" (
    set "LEARNING_REPORT_SINCE_HOURS=24"
)

if "%LEARNING_REPORT_MIN_SAMPLE%"=="" (
    set "LEARNING_REPORT_MIN_SAMPLE=5"
)

if "%WATCHLIST_REPORT_INTERVAL_SECONDS%"=="" (
    set "WATCHLIST_REPORT_INTERVAL_SECONDS=3600"
)

if "%SCHEDULE_WATCHLIST_REPORT%"=="" (
    set "SCHEDULE_WATCHLIST_REPORT=true"
)

if "%WATCHLIST_REPORT_AT%"=="" (
    set "WATCHLIST_REPORT_AT=23:30"
)

if "%WATCHLIST_REPORT_DB%"=="" (
    set "WATCHLIST_REPORT_DB=data\signals.db"
)

if "%WATCHLIST_REPORT_OUT_DIR%"=="" (
    set "WATCHLIST_REPORT_OUT_DIR=reports_watchlist"
)

set "PYTHONPATH=%CD%"

echo.
echo ==================================================
echo CandleVision Accumulation Launcher
echo ==================================================
echo PROJECT_DIR=%CD%
echo RUN_DASHBOARD=%RUN_DASHBOARD%
echo RUN_SCANNER=%RUN_SCANNER%
echo RUN_OUTCOME_TRACKER=%RUN_OUTCOME_TRACKER%
echo RUN_LEARNING_REPORT=%RUN_LEARNING_REPORT%
echo RUN_WATCHLIST_REPORT=%RUN_WATCHLIST_REPORT%
echo RUN_REPORT_SCHEDULER=%RUN_REPORT_SCHEDULER%
echo RUN_TRADE_EXECUTOR=%RUN_TRADE_EXECUTOR%
echo TRADE_EXECUTOR_MODE=%TRADE_EXECUTOR_MODE%
echo EXECUTOR_MANAGEMENT_POLICY=%EXECUTOR_MANAGEMENT_POLICY%
echo EXECUTOR_PROTECT_AFTER_1R=%EXECUTOR_PROTECT_AFTER_1R%
echo EXECUTOR_MIN_PROTECTED_R_AFTER_1R=%EXECUTOR_MIN_PROTECTED_R_AFTER_1R%
echo DASHBOARD_HOST=%DASHBOARD_HOST%
echo DASHBOARD_PORT=%DASHBOARD_PORT%
echo DASHBOARD_API_URL=%DASHBOARD_API_URL%
echo SIGNALS_ONLY=%SIGNALS_ONLY%
echo TRADING_ENABLED=%TRADING_ENABLED%
echo OUTCOME_TRACKER_INTERVAL_MINUTES=%OUTCOME_TRACKER_INTERVAL_MINUTES%
echo REPORT_SCHEDULER_POLL_SECONDS=%REPORT_SCHEDULER_POLL_SECONDS%
echo LEARNING_REPORT_INTERVAL_SECONDS=%LEARNING_REPORT_INTERVAL_SECONDS%
echo SCHEDULE_LEARNING_REPORT=%SCHEDULE_LEARNING_REPORT%
echo LEARNING_REPORT_EVERY_MINUTES=%LEARNING_REPORT_EVERY_MINUTES%
echo LEARNING_REPORT_DB=%LEARNING_REPORT_DB%
echo LEARNING_REPORT_OUT_DIR=%LEARNING_REPORT_OUT_DIR%
echo LEARNING_REPORT_SINCE_HOURS=%LEARNING_REPORT_SINCE_HOURS%
echo LEARNING_REPORT_MIN_SAMPLE=%LEARNING_REPORT_MIN_SAMPLE%
echo WATCHLIST_REPORT_INTERVAL_SECONDS=%WATCHLIST_REPORT_INTERVAL_SECONDS%
echo SCHEDULE_WATCHLIST_REPORT=%SCHEDULE_WATCHLIST_REPORT%
echo WATCHLIST_REPORT_AT=%WATCHLIST_REPORT_AT%
echo WATCHLIST_REPORT_DB=%WATCHLIST_REPORT_DB%
echo WATCHLIST_REPORT_OUT_DIR=%WATCHLIST_REPORT_OUT_DIR%
echo PYTHONPATH=%PYTHONPATH%
echo ==================================================
echo.

REM ==================================================
REM Start dashboard backend
REM ==================================================

if /I "%RUN_DASHBOARD%"=="true" (
    echo Starting CandleVision Dashboard...
    start "CandleVision Dashboard" cmd /k python -m uvicorn dashboard.server:app --host %DASHBOARD_HOST% --port %DASHBOARD_PORT% --reload
    timeout /t 3 >nul
    echo Dashboard started: %DASHBOARD_API_URL%
    echo.
)

REM ==================================================
REM Optional outcome tracker sidecar
REM ==================================================

if /I "%RUN_OUTCOME_TRACKER%"=="true" (
    echo Starting outcome tracker sidecar...
    start "CandleVision Outcome Tracker" cmd /k python tools\outcome_tracker.py --db data\signals.db --loop --interval-minutes %OUTCOME_TRACKER_INTERVAL_MINUTES%
    timeout /t 1 >nul
    echo Outcome tracker started.
    echo.
)

REM ==================================================
REM Optional scheduled report sidecar
REM ==================================================

if /I "%RUN_REPORT_SCHEDULER%"=="true" (
    echo Starting scheduled report sidecar...
    start "CandleVision Report Scheduler" cmd /k python -m tools.report_scheduler
    timeout /t 1 >nul
    echo Report scheduler started.
    echo.
)

REM ==================================================
REM Optional learning report sidecar
REM ==================================================

if /I "%RUN_LEARNING_REPORT%"=="true" (
    echo Starting learning report sidecar...
    start "CandleVision Learning Report" cmd /k "echo CandleVision Learning Report loop started. & for /l %%G in (0,0,1) do (python -m tools.learning_report --db %LEARNING_REPORT_DB% --out-dir %LEARNING_REPORT_OUT_DIR% --since-hours %LEARNING_REPORT_SINCE_HOURS% --min-sample %LEARNING_REPORT_MIN_SAMPLE% ^& timeout /t %LEARNING_REPORT_INTERVAL_SECONDS% /nobreak ^>nul)"
    timeout /t 1 >nul
    echo Learning report started.
    echo.
)

REM ==================================================
REM Optional watchlist transition study sidecar
REM ==================================================

if /I "%RUN_WATCHLIST_REPORT%"=="true" (
    echo Starting watchlist transition report sidecar...
    start "CandleVision Watchlist Report" cmd /k "echo CandleVision Watchlist Report loop started. & for /l %%G in (0,0,1) do (python .\tools\watchlist_transition_study.py --db %WATCHLIST_REPORT_DB% --out-dir %WATCHLIST_REPORT_OUT_DIR% ^& timeout /t %WATCHLIST_REPORT_INTERVAL_SECONDS% /nobreak ^>nul)"
    timeout /t 1 >nul
    echo Watchlist transition report started.
    echo.
)

REM ==================================================
REM Start scanner
REM ==================================================

if /I "%RUN_SCANNER%"=="true" (
    echo Starting Accumulation V1.4.2 DIAG...
    python orderflow_accum_main.py
) else (
    echo Scanner disabled. Dashboard/outcome tracker started if enabled.
)

echo.
echo Process finished with exit code %errorlevel%.
pause
