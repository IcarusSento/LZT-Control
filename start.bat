@echo off
setlocal EnableExtensions

set "PYTHONUTF8=1"
set "PYTHONIOENCODING=utf-8"
cd /d "%~dp0" || goto :path_error

if not exist ".venv\Scripts\python.exe" goto :setup
".venv\Scripts\python.exe" -c "import sys" >nul 2>nul
if errorlevel 1 goto :setup
goto :run

:setup
call "%~dp0setup.bat"
if errorlevel 1 exit /b 1

:run
if not defined BUMP_HOST set "BUMP_HOST=127.0.0.1"
if not defined BUMP_PORT set "BUMP_PORT=8787"
if not defined BUMP_OPEN_BROWSER set "BUMP_OPEN_BROWSER=1"

echo [LZT Control] Starting: http://%BUMP_HOST%:%BUMP_PORT%
if not "%BUMP_OPEN_BROWSER%"=="0" (
  start "" /b ".venv\Scripts\pythonw.exe" -m services.browser_launcher "http://%BUMP_HOST%:%BUMP_PORT%"
)
".venv\Scripts\python.exe" -m uvicorn app:app --host "%BUMP_HOST%" --port "%BUMP_PORT%"
set "APP_EXIT_CODE=%ERRORLEVEL%"

if not "%APP_EXIT_CODE%"=="0" (
  echo.
  echo [LZT Control] Server stopped with error code %APP_EXIT_CODE%.
  pause
)
exit /b %APP_EXIT_CODE%

:path_error
echo [LZT Control] Cannot open the project directory.
pause
exit /b 1
