@echo off
setlocal
cd /d "%~dp0"
set "ROOT=%cd%\.."
where pyinstaller >nul 2>nul
if errorlevel 1 pip install pyinstaller
set "COLLECT_BASE=--collect-all openpyxl --collect-all PySide6 --collect-all playwright"
if not defined PYINSTALLER_EXCLUDE_HEAVY set PYINSTALLER_EXCLUDE_HEAVY=1
set "EXCLUDES="
if /I not "%PYINSTALLER_EXCLUDE_HEAVY%"=="0" (
  set "EXCLUDES=--exclude-module torch --exclude-module transformers --exclude-module tensorflow --exclude-module tf_keras --exclude-module keras --exclude-module mediapipe --exclude-module deepface --exclude-module retinaface --exclude-module clip --exclude-module kivy --exclude-module kivymd --exclude-module pandas --exclude-module scipy --exclude-module matplotlib --exclude-module cv2 --exclude-module h5py"
)
set "HIDDEN=--hidden-import core.accounts --hidden-import automation.actions.content_publisher --hidden-import automation.actions.interactions --hidden-import automation.actions.interactions_adapters --hidden-import app --hidden-import backend_license_client --hidden-import config --hidden-import gui.gui_app --hidden-import core.ig --hidden-import core.leads --hidden-import licensekit --hidden-import gui.main_window --hidden-import media_norm --hidden-import proxy_manager --hidden-import core.responder --hidden-import runtime.runtime --hidden-import sdk_sanitize --hidden-import core.session_store --hidden-import state_view --hidden-import src.analytics.stats_engine --hidden-import src.image_attribute_filter --hidden-import src.image_prompt_parser --hidden-import src.image_rule_evaluator --hidden-import src.vision.face_detector_scrfd --hidden-import src.vision.fairface_analyzer --hidden-import src.vision.gender_age_analyzer --hidden-import core.storage --hidden-import core.totp_store --hidden-import ui --hidden-import update_system --hidden-import utils --hidden-import automation.whatsapp --hidden-import jaraco --hidden-import jaraco.text --hidden-import jaraco.classes --hidden-import jaraco.functools --hidden-import pkg_resources --hidden-import setuptools --hidden-import onnxruntime"
set "OUT_NAME=insta_owner_gui"
cd /d "%ROOT%"
pyinstaller --noconfirm --clean --onedir --windowed --name %OUT_NAME% ^
  --paths "%ROOT%" ^
  --distpath "%ROOT%\dist" ^
  --workpath "%ROOT%\build\pyinstaller_owner" ^
  --specpath "%ROOT%\build\pyinstaller_owner" ^
  --add-data "%ROOT%\styles.qss;." ^
  %COLLECT_BASE% %HIDDEN% %EXCLUDES% ^
  "launchers\owner_gui_launcher.py"
if errorlevel 1 goto :end

set "PW_SOURCE="
for /d %%D in ("%ROOT%\playwright_browsers\chromium-*") do if not defined PW_SOURCE set "PW_SOURCE=%ROOT%\playwright_browsers"
for /d %%D in ("%ROOT%\runtime\playwright\chromium-*") do if not defined PW_SOURCE set "PW_SOURCE=%ROOT%\runtime\playwright"
for /d %%D in ("%ROOT%\runtime\browsers\chromium-*") do if not defined PW_SOURCE set "PW_SOURCE=%ROOT%\runtime\browsers"
for /d %%D in ("%ROOT%\ms-playwright\chromium-*") do if not defined PW_SOURCE set "PW_SOURCE=%ROOT%\ms-playwright"
if defined PW_SOURCE (
  if exist "%ROOT%\dist\%OUT_NAME%\playwright_browsers" rmdir /s /q "%ROOT%\dist\%OUT_NAME%\playwright_browsers"
  mkdir "%ROOT%\dist\%OUT_NAME%\playwright_browsers" >nul 2>nul
  for /d %%D in ("%PW_SOURCE%\chromium-*") do (
    xcopy /e /i /y "%%~fD" "%ROOT%\dist\%OUT_NAME%\playwright_browsers\%%~nxD" >nul
  )
  for /d %%D in ("%PW_SOURCE%\chromium_headless_shell-*") do (
    xcopy /e /i /y "%%~fD" "%ROOT%\dist\%OUT_NAME%\playwright_browsers\%%~nxD" >nul
  )
  for /d %%D in ("%PW_SOURCE%\ffmpeg-*") do (
    xcopy /e /i /y "%%~fD" "%ROOT%\dist\%OUT_NAME%\playwright_browsers\%%~nxD" >nul
  )
  for /d %%D in ("%PW_SOURCE%\winldd-*") do (
    xcopy /e /i /y "%%~fD" "%ROOT%\dist\%OUT_NAME%\playwright_browsers\%%~nxD" >nul
  )
) else (
  echo [WARN] No se encontro ningun Chromium de Playwright en playwright_browsers, runtime\playwright, runtime\browsers ni ms-playwright.
)

echo.
echo EXE owner generado en dist\%OUT_NAME%\%OUT_NAME%.exe
pause
:end
endlocal
