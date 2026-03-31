# Estructura del Proyecto

**Ãšltima actualizaciÃ³n:** 2025-12-24

## ðŸ“ Directorio RaÃ­z

### Archivos Principales
- **app.py** - Punto de entrada principal de la aplicaciÃ³n
- **run.py** - Launcher alternativo

### MÃ³dulos Core
- **accounts.py** - GestiÃ³n de cuentas de Instagram
- **leads.py** - GestiÃ³n de leads y contactos
- **ig.py** - Funcionalidad principal de Instagram
- **responder.py** - Auto-responder con OpenAI
- **whatsapp.py** - Funcionalidad de WhatsApp
- **state_view.py** - Vista de estado de conversaciones

### MÃ³dulos de Soporte
- **config.py** - ConfiguraciÃ³n centralizada
- **storage.py** - Sistema de almacenamiento
- **ui.py** - Interfaz de usuario (CLI)
- **utils.py** - Utilidades generales
- **paths.py** - GestiÃ³n de rutas
- **runtime.py** - Runtime y ejecuciÃ³n
- **session_store.py** - Almacenamiento de sesiones
- **totp_store.py** - GestiÃ³n de 2FA/TOTP
- **proxy_manager.py** - GestiÃ³n de proxies
- **media_norm.py** - NormalizaciÃ³n de medios
- **client_factory.py** - Factory de clientes

### Sistema de Licencias
- **licensekit.py** - Cliente principal de licencias
- **backend_license_client.py** - Cliente HTTP para backend

## ðŸ“‚ Directorios Principales

### src/
CÃ³digo fuente organizado y refactorizado:
- **instagram_adapter.py** - Adaptador principal de Instagram (29 KB)
- **playwright_service.py** - Servicio de Playwright refactorizado
- **auth/** - AutenticaciÃ³n y onboarding
- **actions/** - Acciones de Instagram
- **jobs/** - Jobs y tareas
- **opt_in/** - MÃ³dulo opt-in
- **tasks/** - Sistema de tareas
- **transport/** - Capa de transporte

### backend/
Backend FastAPI para sistema de licencias:
- **main.py** - Servidor FastAPI
- **test_license_flow.py** - Tests del flujo de licencias
- **start_backend.bat** - Script de inicio (Windows)
- **run_tests.bat** - Script de tests
- **examples/** - Ejemplos de integraciÃ³n

### scripts/
Scripts de utilidad y herramientas:
- **check_playwright.py** - Verificar instalaciÃ³n de Playwright
- **smoke_open_chat.py** - Test de apertura de chat
- **run_test_flow.py** - Test de flujo
- **run_test_jobs.py** - Test de jobs
- **license_backend_menu.py** - MenÃº CLI de licencias
- **manual_login_once.py** - Login manual
- **run_batch_send.py** - EnvÃ­o en lote
- Y otros scripts de automatizaciÃ³n...

### docs/
DocumentaciÃ³n consolidada:
- **MAC_SETUP.md** - ConfiguraciÃ³n para macOS
- **OPTIN.md** - DocumentaciÃ³n del modo opt-in
- **LICENCIAS.md** - GuÃ­a completa del sistema de licencias
- **LICENCIAS_QUICKSTART.md** - Inicio rÃ¡pido de licencias
- **ESTRUCTURA.md** - Este archivo

### optin_browser/
MÃ³dulo de automatizaciÃ³n de navegador:
- Sistema de login humanizado
- EnvÃ­o de DMs
- Respuestas automÃ¡ticas
- GrabaciÃ³n y reproducciÃ³n de flujos

### adapters/
Adaptadores de clientes:
- **base.py** - Clase base
- **instagram_playwright.py** - Adaptador Playwright
- **instagram_stub.py** - Adaptador stub para testing

### adapters/integrations/
Integraciones con servicios externos:
- **adapter.py** - Adaptador base
- **android_sim_adapter.py** - Adaptador Android

### tests/
Tests principales del proyecto

### tests/optin/
Tests especÃ­ficos del mÃ³dulo opt-in

### _archive/
**âš ï¸ NO USAR - Solo para referencia**
Archivos obsoletos y duplicados archivados:
- **backups/** - Archivos de backup antiguos
- **old_adapters/** - Adaptadores obsoletos
- **old_services/** - Servicios obsoletos
- **old_docs/** - DocumentaciÃ³n antigua
- **misc/** - Archivos miscelÃ¡neos

## ðŸ—„ï¸ Directorios de Datos

- **data/** - Datos de la aplicaciÃ³n (JSON)
- **storage/** - Almacenamiento persistente
- **.sessions/** - Sesiones guardadas
- **profiles/** - Perfiles de navegador
- **browser_sessions/** - Sesiones de navegador

## ðŸ”§ ConfiguraciÃ³n

- **.env** - Variables de entorno (NO commitear)
- **.env.example** - Ejemplo de configuraciÃ³n
- **requirements.txt** - Dependencias principales
- **requirements_optin.txt** - Dependencias opt-in
- **.gitignore** - Archivos ignorados por Git

## ðŸ Entornos Virtuales

- **.venv/** - Entorno virtual principal (usar este)

## ðŸ“ Notas Importantes

1. **Imports de instagram_adapter:** Usar rom src.instagram_adapter import (versiÃ³n principal)
2. **Imports de playwright_service:** Usar rom src.playwright_service import (versiÃ³n refactorizada)
3. **Sistema de licencias:** Modular con backend separado
4. **Archivos en _archive/:** Solo para referencia, no usar en producciÃ³n

## ðŸš€ Inicio RÃ¡pido

`powershell
# Activar entorno virtual
.\.venv\Scripts\Activate.ps1

# Instalar dependencias
pip install -r requirements.txt

# Ejecutar aplicaciÃ³n
python app.py
`

## ðŸ“š MÃ¡s InformaciÃ³n

- Ver docs/LICENCIAS.md para el sistema de licencias
- Ver docs/OPTIN.md para el modo opt-in
- Ver docs/MAC_SETUP.md para configuraciÃ³n en macOS
