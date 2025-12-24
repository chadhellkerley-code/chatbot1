@echo off
setlocal
cd /d "%~dp0"
where pyinstaller >nul 2>nul
if errorlevel 1 pip install pyinstaller
pyinstaller --noconfirm --clean --onefile --name insta_cli app.py
echo.
echo EXE generado en dist\insta_cli.exe
pause
endlocal
