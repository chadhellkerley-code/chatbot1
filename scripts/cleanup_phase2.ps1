# Script de Limpieza Agresiva - Fase 2
# Mueve TODOS los archivos que no son core a ubicaciones apropiadas

Write-Host "================================================================================" -ForegroundColor Cyan
Write-Host "LIMPIEZA AGRESIVA - FASE 2" -ForegroundColor Cyan
Write-Host "================================================================================" -ForegroundColor Cyan
Write-Host ""

$ErrorActionPreference = "Continue"

# FASE 1: Mover documentación restante a docs/
Write-Host "[1] Moviendo documentación restante..." -ForegroundColor Yellow

$docsToMove = @(
    "AUDIT.md",
    "GUIA_LICENCIAS_BACKEND.md",
    "INICIO_RAPIDO_LICENCIAS.md",
    "README_MAC.md",
    "README_OPTIN.md",
    "PLAN_REORGANIZACION.md",
    "RESUMEN_REORGANIZACION.md",
    "REORGANIZACION_COMPLETADA.md"
)

foreach ($doc in $docsToMove) {
    if (Test-Path $doc) {
        Move-Item $doc "docs\" -Force
        Write-Host "  [OK] Movido: $doc -> docs/" -ForegroundColor Green
    }
}

# FASE 2: Mover scripts/utilidades a scripts/
Write-Host "`n[2] Moviendo scripts y utilidades..." -ForegroundColor Yellow

$scriptsToMove = @(
    "analyze_project.py",
    "reorganize.ps1",
    "build_exe.bat",
    "insta_cli.bat",
    "run_mac.sh",
    "setup_mac.sh",
    "LaunchApp.command",
    "celery-manager.sh",
    "client_launcher.py"
)

foreach ($script in $scriptsToMove) {
    if (Test-Path $script) {
        Move-Item $script "scripts\" -Force
        Write-Host "  [OK] Movido: $script -> scripts/" -ForegroundColor Green
    }
}

# FASE 3: Mover archivos grandes/temporales a _archive/misc/
Write-Host "`n[3] Archivando archivos grandes y logs..." -ForegroundColor Yellow

$miscToArchive = @(
    "pagina.txt",
    "celery.log",
    "run.py"
)

foreach ($file in $miscToArchive) {
    if (Test-Path $file) {
        Move-Item $file "_archive\misc\" -Force
        Write-Host "  [OK] Archivado: $file" -ForegroundColor Green
    }
}

# FASE 4: Mover directorios de entornos virtuales duplicados
Write-Host "`n[4] Identificando entornos virtuales duplicados..." -ForegroundColor Yellow

$venvs = @(".venv-1", ".venv_win", "venv")
$activeVenv = ".venv"

Write-Host "  [INFO] Entorno virtual activo: $activeVenv" -ForegroundColor Cyan

foreach ($venv in $venvs) {
    if (Test-Path $venv) {
        Write-Host "  [WARN] Encontrado entorno duplicado: $venv" -ForegroundColor Yellow
        Write-Host "  [INFO] Recomendacion: Mover manualmente a C:\Users\PC\Desktop\_old_venvs_chat\" -ForegroundColor Cyan
    }
}

# FASE 5: Crear README principal
Write-Host "`n[5] Creando README principal..." -ForegroundColor Yellow

$readmeContent = @"
# Instagram Automation Tool

Sistema de automatización para Instagram con gestión de cuentas, leads, mensajería y auto-responder con IA.

## 🚀 Inicio Rápido

``````powershell
# 1. Activar entorno virtual
.\.venv\Scripts\Activate.ps1

# 2. Instalar dependencias
pip install -r requirements.txt

# 3. Ejecutar aplicación
python app.py
``````

## 📁 Estructura del Proyecto

- **Core Modules (raíz):** Módulos principales de la aplicación
- **src/:** Código fuente organizado y refactorizado
- **backend/:** Backend FastAPI para sistema de licencias
- **scripts/:** Scripts de utilidad y herramientas
- **docs/:** Documentación completa del proyecto
- **_archive/:** Archivos obsoletos (no usar)

## 📚 Documentación

Ver la carpeta ``docs/`` para documentación completa:

- **docs/ESTRUCTURA.md** - Estructura detallada del proyecto
- **docs/LICENCIAS.md** - Sistema de licencias
- **docs/OPTIN.md** - Modo opt-in
- **docs/MAC_SETUP.md** - Configuración para macOS

## 🔧 Configuración

1. Copiar ``.env.example`` a ``.env``
2. Configurar variables de entorno necesarias
3. Ver ``docs/`` para guías detalladas

## 🎯 Funcionalidades

- ✅ Gestión de múltiples cuentas de Instagram
- ✅ Gestión de leads y contactos
- ✅ Envío de mensajes con rotación de cuentas
- ✅ Auto-responder con OpenAI
- ✅ Sistema de licencias
- ✅ Integración con WhatsApp
- ✅ Modo opt-in con navegador

## 📝 Licencia

Ver ``docs/LICENCIAS.md`` para información sobre el sistema de licencias.

---

**Última actualización:** $(Get-Date -Format "yyyy-MM-dd")
"@

Set-Content "README.md" $readmeContent -Encoding UTF8
Write-Host "  [OK] Creado: README.md" -ForegroundColor Green

# RESUMEN
Write-Host ""
Write-Host "================================================================================" -ForegroundColor Cyan
Write-Host "LIMPIEZA COMPLETADA" -ForegroundColor Green
Write-Host "================================================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Archivos en raiz ahora:" -ForegroundColor Yellow
Write-Host "  - Modulos core (.py)" -ForegroundColor White
Write-Host "  - Archivos de configuracion (.env, requirements.txt, etc.)" -ForegroundColor White
Write-Host "  - README.md principal" -ForegroundColor White
Write-Host ""
Write-Host "Archivos movidos a:" -ForegroundColor Yellow
Write-Host "  - docs/ (documentacion)" -ForegroundColor White
Write-Host "  - scripts/ (scripts y utilidades)" -ForegroundColor White
Write-Host "  - _archive/misc/ (archivos grandes/temporales)" -ForegroundColor White
Write-Host ""
Write-Host "Proximo paso: git add -A && git commit -m 'Limpieza fase 2: mover documentacion y scripts restantes'" -ForegroundColor Cyan
Write-Host ""
