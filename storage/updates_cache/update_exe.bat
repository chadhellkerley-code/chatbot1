@echo off
setlocal
set SRC="C:\Users\Rosen\OneDrive\Escritorio\GianWorks\chatbot\storage\updates_cache\insta_cli_universal.exe"
set DST="C:\Users\Rosen\OneDrive\Escritorio\GianWorks\chatbot\insta_cli_universal.exe"
set BAK="C:\Users\Rosen\OneDrive\Escritorio\GianWorks\chatbot\storage\backups\insta_cli_universal.1768921309.bak"
:loop
timeout /t 1 >nul
if exist %DST% (
  move /Y %DST% %BAK% >nul 2>&1
)
move /Y %SRC% %DST% >nul 2>&1
if exist %SRC% goto loop
endlocal