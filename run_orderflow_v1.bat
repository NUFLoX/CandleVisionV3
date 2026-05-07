@echo off
setlocal EnableExtensions EnableDelayedExpansion
title CandleVision OrderFlow V1

cd /d "%~dp0"

echo [1/6] Project dir: %CD%

set "PY_CMD="
where py >nul 2>nul && set "PY_CMD=py -3"
if not defined PY_CMD (
    where python >nul 2>nul && set "PY_CMD=python"
)

if not defined PY_CMD (
    echo Python was not found in PATH.
    echo Install Python 3.10+ and enable "Add python.exe to PATH".
    pause
    exit /b 1
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
    echo pip upgrade failed.
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

if not exist ".env" if exist ".env.example" (
    copy ".env.example" ".env" >nul
    echo Created .env from .env.example
)

echo [6/6] Starting bot...
echo Logs: orderflow_v1.log
python -u orderflow_v1_main.py

echo.
echo Process finished with exit code %errorlevel%.
pause
