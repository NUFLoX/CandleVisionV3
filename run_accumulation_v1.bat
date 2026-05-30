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

if "%RUN_TRADE_EXECUTOR%"=="" (
    set "RUN_TRADE_EXECUTOR=true"
)

if "%TRADE_EXECUTOR_MODE%"=="" (
    set "TRADE_EXECUTOR_MODE=paper"
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

set "PYTHONPATH=%CD%"

echo.
echo ==================================================
echo CandleVision Accumulation Launcher
echo ==================================================
echo PROJECT_DIR=%CD%
echo RUN_DASHBOARD=%RUN_DASHBOARD%
echo RUN_SCANNER=%RUN_SCANNER%
echo RUN_OUTCOME_TRACKER=%RUN_OUTCOME_TRACKER%
echo RUN_TRADE_EXECUTOR=%RUN_TRADE_EXECUTOR%
echo TRADE_EXECUTOR_MODE=%TRADE_EXECUTOR_MODE%
echo DASHBOARD_HOST=%DASHBOARD_HOST%
echo DASHBOARD_PORT=%DASHBOARD_PORT%
echo DASHBOARD_API_URL=%DASHBOARD_API_URL%
echo SIGNALS_ONLY=%SIGNALS_ONLY%
echo TRADING_ENABLED=%TRADING_ENABLED%
echo OUTCOME_TRACKER_INTERVAL_MINUTES=%OUTCOME_TRACKER_INTERVAL_MINUTES%
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
