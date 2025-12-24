# Guía de Uso: Sistema de Licencias con Backend FastAPI

## 📋 Descripción General

Este sistema de licencias consta de dos partes:
1. **Backend FastAPI**: Servidor que gestiona la creación y activación de licencias usando Supabase
2. **Cliente CLI**: Interfaz de línea de comandos para interactuar con el backend

## 🚀 Configuración Inicial

### 1. Configurar el Backend

#### Variables de Entorno

Crea o edita el archivo `backend/.env`:

```env
# Credenciales de Supabase
SUPABASE_URL=https://tu-proyecto.supabase.co
SUPABASE_SERVICE_ROLE_KEY=tu-service-role-key-aqui

# Token de administrador (genera uno seguro)
ADMIN_TOKEN=tu-token-admin-super-secreto

# Hash secret (opcional, usa SUPABASE_SERVICE_ROLE_KEY por defecto)
LICENSE_HASH_SECRET=otro-secreto-opcional

# Puerto del servidor (opcional, 8000 por defecto)
PORT=8000
```

#### Aplicar el Esquema SQL

1. Ve a tu proyecto en Supabase
2. Abre el **SQL Editor**
3. Ejecuta el contenido de `infra/supabase_schema.sql`

Esto creará las tablas:
- `customers` - Información de clientes
- `licenses` - Licencias emitidas
- `license_activations` - Registro de activaciones
- `app_config` - Configuración de la app

### 2. Configurar el Cliente

Edita el archivo `.env` en la raíz del proyecto:

```env
# URL del backend de licencias
BACKEND_URL=http://localhost:8000

# Token de administrador (mismo que en backend/.env)
ADMIN_TOKEN=tu-token-admin-super-secreto
```

## 🏃 Ejecutar el Backend

### Opción 1: Modo desarrollo (recomendado para testing)

```bash
cd backend
python -m uvicorn main:app --reload --port 8000
```

### Opción 2: Usando el script de Python

```bash
cd backend
python main.py
```

El servidor estará disponible en `http://localhost:8000`

### Verificar que funciona

Abre tu navegador en: `http://localhost:8000/health`

Deberías ver: `{"ok": true}`

## 🧪 Probar el Sistema

### Pruebas Automatizadas

Ejecuta el script de pruebas completo:

```bash
cd backend
python test_license_flow.py
```

Este script probará:
1. ✅ Conexión con el backend
2. ✅ Creación de licencia (admin)
3. ✅ Activación de licencia (cliente)
4. ✅ Rechazo de licencias inválidas
5. ✅ Múltiples activaciones

### Pruebas Manuales con curl

#### 1. Health Check

```bash
curl http://localhost:8000/health
```

#### 2. Crear Licencia (Admin)

```bash
curl -X POST http://localhost:8000/admin/licenses \
  -H "Content-Type: application/json" \
  -H "x-admin-token: tu-token-admin-super-secreto" \
  -d '{
    "name": "Cliente Demo",
    "days": 60,
    "email": "cliente@example.com"
  }'
```

Respuesta:
```json
{
  "license_key": "ABCD1234EFGH5678IJKL",
  "expires_at": "2025-02-22T14:30:00+00:00",
  "customer_id": "uuid-del-cliente"
}
```

#### 3. Activar Licencia (Cliente)

```bash
curl -X POST http://localhost:8000/activate \
  -H "Content-Type: application/json" \
  -d '{
    "license_key": "ABCD1234EFGH5678IJKL",
    "client_fingerprint": "mi-maquina-123"
  }'
```

Respuesta:
```json
{
  "ok": true,
  "days_left": 59,
  "customer_id": "uuid-del-cliente"
}
```

## 💻 Usar desde la CLI

### Opción 1: Menú Interactivo

```bash
python license_backend_menu.py
```

Esto abrirá un menú con las siguientes opciones:
1. Crear nueva licencia (Admin)
2. Activar licencia
3. Verificar conexión con backend
4. Volver al menú principal

### Opción 2: Usar el Cliente Programáticamente

```python
from backend_license_client import LicenseBackendClient

# Inicializar cliente
client = LicenseBackendClient()

# Verificar backend
healthy, error = client.health_check()
if not healthy:
    print(f"Error: {error}")
    exit(1)

# Crear licencia (requiere admin token)
success, data, error = client.create_license(
    name="Cliente VIP",
    days=90,
    email="vip@example.com"
)

if success:
    print(f"License Key: {data['license_key']}")
    license_key = data['license_key']
    
    # Activar licencia
    success, activation, error = client.activate_license(license_key)
    
    if success:
        print(f"Días restantes: {activation['days_left']}")
```

### Opción 3: Funciones de Conveniencia

```python
from backend_license_client import create_license, activate_license

# Crear licencia
success, data, error = create_license(
    name="Cliente ABC",
    days=60,
    email="abc@example.com"
)

# Activar licencia
success, activation, error = activate_license(
    license_key="ABCD1234EFGH5678IJKL"
)
```

## 🔧 Integración con tu CLI Existente

Para integrar el sistema de licencias con tu menú principal, edita `app.py`:

```python
from license_backend_menu import license_management_menu

def menu():
    while True:
        # ... tu código existente ...
        
        print("X) Gestión de Licencias (Backend)")
        
        choice = input("Opción: ").strip()
        
        # ... tus opciones existentes ...
        
        if choice.upper() == "X":
            license_management_menu()
```

## 📊 Flujo de Trabajo Recomendado

### Para Administradores

1. **Iniciar el backend**
   ```bash
   cd backend
   python -m uvicorn main:app --reload --port 8000
   ```

2. **Crear licencia para un cliente**
   - Usar el menú CLI: `python license_backend_menu.py` → Opción 1
   - O usar curl/API directamente

3. **Entregar la license key al cliente**
   - Enviar por email seguro
   - O incluir en un paquete de distribución

### Para Clientes

1. **Recibir la license key** del administrador

2. **Activar la licencia**
   - Usar el menú CLI: `python license_backend_menu.py` → Opción 2
   - O usar la función `activate_license()` en código

3. **Verificar activación**
   - El sistema mostrará los días restantes
   - Se registrará la activación en la base de datos

## 🔒 Seguridad

### Buenas Prácticas

1. **NUNCA** compartas el `ADMIN_TOKEN` con clientes
2. **NUNCA** expongas el `SUPABASE_SERVICE_ROLE_KEY` públicamente
3. Usa HTTPS en producción (no HTTP)
4. Rota el `ADMIN_TOKEN` periódicamente
5. Las license keys se almacenan hasheadas (SHA256) en la BD

### Fingerprinting

El sistema genera automáticamente un "fingerprint" único para cada máquina basado en:
- Nombre del host
- Arquitectura del sistema
- Sistema operativo
- Dirección MAC

Esto permite rastrear activaciones y detectar uso no autorizado.

## 📈 Monitoreo

### Ver Activaciones en Supabase

1. Ve a tu proyecto en Supabase
2. Abre **Table Editor**
3. Selecciona la tabla `license_activations`

Verás:
- Timestamp de activación
- Client fingerprint
- IP del cliente
- User agent

### Consultas Útiles

#### Licencias activas:
```sql
SELECT 
  l.license_key_hash,
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

#### Activaciones recientes:
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

### Error: "Backend no disponible"

**Causa**: El servidor FastAPI no está corriendo

**Solución**:
```bash
cd backend
python -m uvicorn main:app --reload --port 8000
```

### Error: "ADMIN_TOKEN no configurado"

**Causa**: Falta el token en `.env`

**Solución**: Agrega `ADMIN_TOKEN=tu-token` en `backend/.env` y `.env`

### Error: "invalid license"

**Causas posibles**:
1. License key incorrecta
2. Licencia no existe en la BD
3. Licencia expirada

**Solución**: Verifica la license key y su estado en Supabase

### Error: "license expired or inactive"

**Causa**: La licencia venció o fue desactivada

**Solución**: 
- Extender la fecha de expiración en Supabase
- O crear una nueva licencia

## 🚀 Despliegue en Producción

### Backend

Opciones recomendadas:
1. **Railway**: Deploy automático desde GitHub
2. **Render**: Free tier disponible
3. **Fly.io**: Excelente para FastAPI
4. **DigitalOcean App Platform**

### Variables de Entorno en Producción

Asegurate de configurar:
- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`
- `ADMIN_TOKEN`
- `LICENSE_HASH_SECRET` (opcional pero recomendado)

### Cliente

Actualiza `BACKEND_URL` en `.env` con la URL de producción:
```env
BACKEND_URL=https://tu-backend.railway.app
```

## 📚 Referencia de API

### GET /health

Verifica que el servidor esté funcionando.

**Response**: `{"ok": true}`

---

### POST /admin/licenses

Crea una nueva licencia (requiere autenticación admin).

**Headers**:
- `x-admin-token`: Token de administrador
- `Content-Type`: application/json

**Body**:
```json
{
  "name": "Nombre del cliente",
  "days": 60,
  "email": "cliente@example.com"  // opcional
}
```

**Response**:
```json
{
  "license_key": "ABCD1234...",
  "expires_at": "2025-02-22T14:30:00+00:00",
  "customer_id": "uuid"
}
```

**Errores**:
- `403`: Token inválido
- `400`: Validación fallida (ej: days < 30)
- `500`: Error de base de datos

---

### POST /activate

Activa una licencia.

**Headers**:
- `Content-Type`: application/json

**Body**:
```json
{
  "license_key": "ABCD1234...",
  "client_fingerprint": "opcional"  // se genera automáticamente si se omite
}
```

**Response**:
```json
{
  "ok": true,
  "days_left": 59,
  "customer_id": "uuid"
}
```

**Errores**:
- `403`: Licencia inválida, expirada o inactiva
- `500`: Error de base de datos

## 🎯 Próximos Pasos

1. ✅ Probar el flujo completo con `test_license_flow.py`
2. ✅ Integrar el menú de licencias en tu CLI principal
3. ⏳ Desplegar el backend en producción
4. ⏳ Implementar renovación automática de licencias
5. ⏳ Agregar notificaciones por email cuando una licencia está por vencer

## 💡 Tips

- Usa `days=365` para licencias anuales
- Guarda las license keys en un gestor de contraseñas
- Monitorea las activaciones regularmente
- Considera implementar límites de activaciones por licencia
- Implementa un sistema de renovación antes de que expiren

---

**¿Necesitás ayuda?** Revisa los logs del backend y usa el menú de verificación de conexión.
