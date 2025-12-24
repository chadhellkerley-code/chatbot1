@echo off
REM Script para iniciar el backend de licencias

echo ========================================
echo   BACKEND DE LICENCIAS - FastAPI
echo ========================================
echo.

REM Verificar que estamos en el directorio correcto
if not exist "main.py" (
    echo [ERROR] No se encuentra main.py
    echo Por favor ejecuta este script desde el directorio backend/
    pause
    exit /b 1
)

REM Verificar que existe .env
if not exist ".env" (
    echo [WARN] No se encuentra .env, copiando desde .env.example
    copy .env.example .env
    echo [OK] Archivo .env creado
    echo.
)

echo [INFO] Iniciando servidor FastAPI...
echo [INFO] URL: http://localhost:8000
echo [INFO] Docs: http://localhost:8000/docs
echo.
echo Presiona Ctrl+C para detener el servidor
echo.

python -m uvicorn main:app --reload --host 0.0.0.0 --port 8000
