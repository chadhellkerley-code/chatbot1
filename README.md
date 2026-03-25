# InstaCRM

Snapshot base del proyecto para desarrollo local, sin cuentas, sesiones ni datos personales.

## Inicio rapido

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python run_owner_dev.py
```

`run_owner_dev.py` inicia la version owner para desarrollo y evita el bloqueo de licencia de distribucion. La app crea sus carpetas locales (`storage/`, `runtime/`, `sessions/`, `data/`) al arrancar.

## Configuracion opcional

- `.env.example` incluye variables opcionales como `OPENAI_API_KEY`.
- `app/config.example.json` muestra el formato de configuracion local de Supabase si mas adelante queres conectar licencias o telemetria.
- No se incluyen cuentas, sesiones, logs ni claves reales en este repo.

## Archivos locales que no se deben commitear

- `.env.local`
- `app/config.json`
- `.session_key`
- `storage/`
- `data/`
- `sessions/`
- `runtime/`

## Estructura principal

- `gui/` interfaz de escritorio
- `application/` servicios de aplicacion
- `core/` almacenamiento y flujos legacy
- `src/` runtime y modulos refactorizados
- `backend/` backend FastAPI
- `tests/` pruebas automatizadas

## Documentacion

- `docs/ESTRUCTURA.md`
- `docs/LICENCIAS.md`
- `docs/OPTIN.md`
- `docs/MAC_SETUP.md`
