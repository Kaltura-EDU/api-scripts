@echo off
setlocal
cd /d "%~dp0"
py -3 -c "import sys; sys.exit(sys.version_info < (3, 10))" >nul 2>&1
if not errorlevel 1 (
    set "KALTURA_PYTHON_COMMAND=py -3"
    goto run_dashboard
)
python -c "import sys; sys.exit(sys.version_info < (3, 10))" >nul 2>&1
if not errorlevel 1 (
    set "KALTURA_PYTHON_COMMAND=python"
    goto run_dashboard
)
python3 -c "import sys; sys.exit(sys.version_info < (3, 10))" >nul 2>&1
if not errorlevel 1 (
    set "KALTURA_PYTHON_COMMAND=python3"
    goto run_dashboard
)
echo Python 3.10 or later is required. Install it from https://www.python.org/downloads/
echo Then reopen this launcher. No additional Python packages are required.
pause
exit /b 1

:run_dashboard
%KALTURA_PYTHON_COMMAND% scripts\app_token_dashboard.py %*
set "KALTURA_DASHBOARD_EXIT=%errorlevel%"
if not "%KALTURA_DASHBOARD_EXIT%"=="0" pause
exit /b %KALTURA_DASHBOARD_EXIT%
