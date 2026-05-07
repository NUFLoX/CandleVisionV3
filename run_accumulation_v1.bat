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
)

echo Starting Accumulation V1.4.2 DIAG...
python orderflow_accum_main.py

echo.
echo Process finished with exit code %errorlevel%.
pause
