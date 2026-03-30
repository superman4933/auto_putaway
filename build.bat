@echo off
setlocal
cd /d "%~dp0"

echo [1/3] Upgrading pip...
python -m pip install -U pip
if errorlevel 1 goto :err

echo [2/3] Installing dependencies and PyInstaller...
python -m pip install -r requirements.txt
python -m pip install pyinstaller
if errorlevel 1 goto :err

echo [3/3] Building onedir bundle (AutoPutaway)...
pyinstaller --noconfirm --clean AutoPutaway.spec
if errorlevel 1 goto :err

echo.
echo Done. Run: dist\AutoPutaway\AutoPutaway.exe
echo Distribution folder: %CD%\dist\AutoPutaway
pause
goto :eof

:err
echo.
echo Build failed.
pause
exit /b 1
