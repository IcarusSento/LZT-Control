@echo off
setlocal EnableExtensions

set "PYTHONUTF8=1"
set "PYTHONIOENCODING=utf-8"
cd /d "%~dp0" || goto :path_error

if not exist ".venv\Scripts\python.exe" goto :create
".venv\Scripts\python.exe" -c "import sys" >nul 2>nul
if not errorlevel 1 goto :install
echo [LZT Control] The copied virtual environment is not portable. Rebuilding it...

:create
echo [LZT Control] Creating the virtual environment...
where py >nul 2>nul
if errorlevel 1 goto :find_direct_python
py -3 -c "import sys; raise SystemExit(0 if sys.version_info[:2] >= (3,11) else 1)" >nul 2>nul
if not errorlevel 1 (
  set "PYTHON_LAUNCHER=py -3"
  goto :create_venv
)
py -3.14 -c "import sys" >nul 2>nul
if not errorlevel 1 (
  set "PYTHON_LAUNCHER=py -3.14"
  goto :create_venv
)
py -3.13 -c "import sys" >nul 2>nul
if not errorlevel 1 (
  set "PYTHON_LAUNCHER=py -3.13"
  goto :create_venv
)
py -3.12 -c "import sys" >nul 2>nul
if not errorlevel 1 (
  set "PYTHON_LAUNCHER=py -3.12"
  goto :create_venv
)
py -3.11 -c "import sys" >nul 2>nul
if not errorlevel 1 (
  set "PYTHON_LAUNCHER=py -3.11"
  goto :create_venv
)
goto :find_direct_python

:find_direct_python
for /f "delims=" %%P in ('dir /b /s /a-d "%LOCALAPPDATA%\Programs\Python\Python3*\python.exe" 2^>nul ^| sort /r') do if not defined PYTHON_LAUNCHER call :try_python "%%P"
if defined PYTHON_LAUNCHER goto :create_venv
for /f "delims=" %%P in ('dir /b /s /a-d "%ProgramFiles%\Python3*\python.exe" 2^>nul ^| sort /r') do if not defined PYTHON_LAUNCHER call :try_python "%%P"
if defined PYTHON_LAUNCHER goto :create_venv

where python >nul 2>nul
if errorlevel 1 goto :python_missing
python -c "import sys; raise SystemExit(0 if sys.version_info[:2] >= (3,11) else 1)" >nul 2>nul
if errorlevel 1 goto :python_version
set "PYTHON_LAUNCHER=python"

:create_venv
%PYTHON_LAUNCHER% -m venv --clear ".venv"
if errorlevel 1 goto :error
goto :install

:install
echo [LZT Control] Installing dependencies...
".venv\Scripts\python.exe" -m pip install --upgrade pip
if errorlevel 1 goto :error
".venv\Scripts\python.exe" -m pip install -r "requirements.txt"
if errorlevel 1 goto :error
echo [LZT Control] Setup complete. Run start.bat.
exit /b 0

:python_missing
echo [LZT Control] Python 3.11 or newer was not found.
echo [LZT Control] Install a supported Python version and try again.
pause
exit /b 1

:python_version
echo [LZT Control] Unsupported Python version.
echo [LZT Control] Install Python 3.11 or newer and try again.
pause
exit /b 1

:path_error
echo [LZT Control] Cannot open the project directory.
pause
exit /b 1

:error
echo [LZT Control] Setup failed. Check Python and the internet connection.
pause
exit /b 1

:try_python
if not exist "%~1" exit /b 0
"%~1" -c "import sys; raise SystemExit(0 if sys.version_info[:2] >= (3,11) else 1)" >nul 2>nul
if errorlevel 1 exit /b 0
set "PYTHON_LAUNCHER="%~1""
exit /b 0
