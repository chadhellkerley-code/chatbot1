@echo off
setlocal
cd /d "%~dp0"
where pyinstaller >nul 2>nul
if errorlevel 1 pip install pyinstaller
set "COLLECT_BASE=--collect-all openpyxl"
if not defined PYINSTALLER_EXCLUDE_HEAVY set PYINSTALLER_EXCLUDE_HEAVY=1
set "EXCLUDES="
if /I not "%PYINSTALLER_EXCLUDE_HEAVY%"=="0" (
  set "EXCLUDES=--exclude-module torch --exclude-module transformers --exclude-module tensorflow --exclude-module tf_keras --exclude-module keras --exclude-module mediapipe --exclude-module deepface --exclude-module retinaface --exclude-module clip --exclude-module kivy --exclude-module kivymd --exclude-module pandas --exclude-module scipy --exclude-module matplotlib --exclude-module cv2 --exclude-module h5py"
)
pyinstaller --noconfirm --clean --onefile --name insta_cli ^
  %COLLECT_BASE% %EXCLUDES% ^
  app.py
echo.
echo EXE generado en dist\insta_cli.exe
pause
endlocal
