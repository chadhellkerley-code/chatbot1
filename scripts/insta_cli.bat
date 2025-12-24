@echo off
chcp 65001 >nul
setlocal
cd /d "%~dp0"
set INSTACLI_EMOJI=1
python app.py
endlocal
