@echo off
REM Script para probar el flujo completo de licencias
REM Inicia el backend y ejecuta las pruebas

echo ========================================
echo   PRUEBAS DEL SISTEMA DE LICENCIAS
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
)

echo [INFO] Verificando si el backend ya esta corriendo...
curl -s http://localhost:8000/health >nul 2>&1
if %errorlevel% equ 0 (
    echo [OK] Backend ya esta corriendo
    goto :run_tests
)

echo [INFO] Iniciando backend en segundo plano...
start /B python -m uvicorn main:app --host 0.0.0.0 --port 8000

REM Esperar a que el backend inicie
echo [INFO] Esperando a que el backend inicie...
timeout /t 3 /nobreak >nul

REM Verificar que el backend inicio correctamente
:check_backend
curl -s http://localhost:8000/health >nul 2>&1
if %errorlevel% neq 0 (
    echo [WARN] Backend aun no responde, esperando...
    timeout /t 2 /nobreak >nul
    goto :check_backend
)

echo [OK] Backend iniciado correctamente

:run_tests
echo.
echo ========================================
echo   EJECUTANDO PRUEBAS
echo ========================================
echo.

python test_license_flow.py

echo.
echo ========================================
echo   PRUEBAS COMPLETADAS
echo ========================================
echo.

pause
