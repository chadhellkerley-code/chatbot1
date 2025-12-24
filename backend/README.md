# Backend de Licencias (FastAPI + Supabase)

Servicio backend para generar y activar licencias usando Supabase Postgres.

## 🚀 Inicio Rápido (Windows)

### 1. Configuración Inicial

```bash
# Copiar el archivo de ejemplo (si no existe .env)
copy .env.example .env

# Editar .env con tus credenciales de Supabase
notepad .env
```

### 2. Instalar Dependencias

```bash
pip install -r requirements.txt
```

### 3. Aplicar Esquema SQL

1. Ve a tu proyecto en [Supabase](https://supabase.com)
2. Abre el **SQL Editor**
3. Ejecuta el contenido de `../infra/supabase_schema.sql`

### 4. Iniciar el Servidor

**Opción A: Usando el script batch (recomendado)**
```bash
start_backend.bat
```

**Opción B: Comando manual**
```bash
python -m uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

El servidor estará disponible en:
- API: http://localhost:8000
- Health Check: http://localhost:8000/health
- Documentación interactiva: http://localhost:8000/docs

## 🧪 Probar el Sistema

### Opción 1: Script Automático

```bash
run_tests.bat
```

Este script:
1. Inicia el backend automáticamente (si no está corriendo)
2. Ejecuta todas las pruebas
3. Muestra un reporte completo

### Opción 2: Pruebas Manuales

```bash
# Asegurate de que el backend esté corriendo primero
python test_license_flow.py
```

## 📋 Variables de Entorno

Edita el archivo `.env`:

```env
# Credenciales de Supabase (REQUERIDO)
SUPABASE_URL=https://tu-proyecto.supabase.co
SUPABASE_SERVICE_ROLE_KEY=tu-service-role-key

# Token de administrador (REQUERIDO)
ADMIN_TOKEN=tu-token-admin-super-secreto

# Hash secret (opcional, usa SUPABASE_SERVICE_ROLE_KEY por defecto)
LICENSE_HASH_SECRET=otro-secreto-opcional

# Puerto del servidor (opcional, 8000 por defecto)
PORT=8000
```

## 📡 Endpoints

### GET /health

Verifica que el servidor esté funcionando.

**Ejemplo:**
```bash
curl http://localhost:8000/health
```

**Respuesta:**
```json
{"ok": true}
```

---

### POST /admin/licenses

Crea una nueva licencia (requiere autenticación admin).

**Headers:**
- `x-admin-token`: Token de administrador
- `Content-Type`: application/json

**Body:**
```json
{
  "name": "Cliente Demo",
  "days": 60,
  "email": "cliente@example.com"
}
```

**Ejemplo:**
```bash
curl -X POST http://localhost:8000/admin/licenses ^
  -H "Content-Type: application/json" ^
  -H "x-admin-token: %ADMIN_TOKEN%" ^
  -d "{\"name\":\"Cliente Demo\",\"days\":60,\"email\":\"demo@example.com\"}"
```

**Respuesta:**
```json
{
  "license_key": "ABCD1234EFGH5678IJKL",
  "expires_at": "2025-02-22T14:30:00+00:00",
  "customer_id": "uuid-del-cliente"
}
```

**Validaciones:**
- `days` debe ser >= 30
- Si se provee `email`, se reutiliza el cliente existente

---

### POST /activate

Activa una licencia.

**Headers:**
- `Content-Type`: application/json

**Body:**
```json
{
  "license_key": "ABCD1234EFGH5678IJKL",
  "client_fingerprint": "opcional"
}
```

**Ejemplo:**
```bash
curl -X POST http://localhost:8000/activate ^
  -H "Content-Type: application/json" ^
  -d "{\"license_key\":\"ABCD1234EFGH5678IJKL\",\"client_fingerprint\":\"mi-pc\"}"
```

**Respuesta:**
```json
{
  "ok": true,
  "days_left": 59,
  "customer_id": "uuid-del-cliente"
}
```

**Validaciones:**
- Verifica que la licencia exista
- Verifica que esté activa (`is_active = true`)
- Verifica que no haya expirado
- Registra la activación con IP y user-agent

## 🔒 Seguridad

- Las license keys se almacenan **hasheadas** (SHA256) en la base de datos
- El hash usa `LICENSE_HASH_SECRET` como sal (por defecto `SUPABASE_SERVICE_ROLE_KEY`)
- **NUNCA** compartas el `ADMIN_TOKEN` con clientes
- **NUNCA** expongas el `SUPABASE_SERVICE_ROLE_KEY` públicamente
- Row Level Security (RLS) está habilitado en todas las tablas

## 📊 Base de Datos

### Tablas

- **customers**: Información de clientes
  - `id`, `name`, `email`, `created_at`
  
- **licenses**: Licencias emitidas
  - `id`, `customer_id`, `license_key_hash`, `is_active`, `expires_at`, `last_seen_at`, `notes`
  
- **license_activations**: Registro de activaciones
  - `id`, `license_id`, `activated_at`, `client_fingerprint`, `ip`, `user_agent`
  
- **app_config**: Configuración clave-valor
  - `key`, `value`, `updated_at`

### Consultas Útiles

**Ver licencias activas:**
```sql
SELECT 
  c.name,
  c.email,
  l.expires_at,
  l.last_seen_at
FROM licenses l
JOIN customers c ON l.customer_id = c.id
WHERE l.is_active = true
  AND l.expires_at > now()
ORDER BY l.expires_at DESC;
```

**Ver activaciones recientes:**
```sql
SELECT 
  la.activated_at,
  la.client_fingerprint,
  la.ip,
  c.name
FROM license_activations la
JOIN licenses l ON la.license_id = l.id
JOIN customers c ON l.customer_id = c.id
ORDER BY la.activated_at DESC
LIMIT 10;
```

## 🐛 Solución de Problemas

### Error: "SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY are required"

**Solución:** Configura las variables en `.env`

### Error: "ADMIN_TOKEN is required"

**Solución:** Agrega `ADMIN_TOKEN=tu-token` en `.env`

### Error al crear licencia: "PGRST208" o 404

**Solución:** Las tablas no existen. Ejecuta el SQL en `../infra/supabase_schema.sql`

### Puerto 8000 ya en uso

**Solución:** 
- Cambia el puerto en `.env`: `PORT=8001`
- O detén el proceso que usa el puerto 8000

## 📚 Recursos Adicionales

- **Guía Completa**: Ver `../GUIA_LICENCIAS_BACKEND.md`
- **Cliente CLI**: Ver `../backend_license_client.py`
- **Menú Interactivo**: Ver `../license_backend_menu.py`
- **Documentación API**: http://localhost:8000/docs (cuando el servidor esté corriendo)

## 🚀 Despliegue en Producción

Ver la guía completa en `../GUIA_LICENCIAS_BACKEND.md` sección "Despliegue en Producción".

Opciones recomendadas:
- Railway
- Render
- Fly.io
- DigitalOcean App Platform

## 💡 Tips

- Usa `days=365` para licencias anuales
- Monitorea las activaciones regularmente en Supabase
- Considera implementar límites de activaciones por licencia
- Implementa notificaciones cuando una licencia esté por vencer

