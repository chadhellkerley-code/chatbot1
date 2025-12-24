# 🎯 INICIO RÁPIDO - Sistema de Licencias

## ✅ Lo que ya tienes funcionando

1. **Backend FastAPI** en `backend/`
   - Endpoints para crear y activar licencias
   - Integración con Supabase
   - Credenciales ya configuradas en `.env.example`

2. **Esquema SQL** en `infra/supabase_schema.sql`
   - Tablas: customers, licenses, license_activations, app_config

3. **Herramientas nuevas creadas:**
   - `backend/test_license_flow.py` - Pruebas automatizadas
   - `backend/start_backend.bat` - Iniciar servidor fácilmente
   - `backend/run_tests.bat` - Ejecutar pruebas automáticamente
   - `backend_license_client.py` - Cliente HTTP para usar desde Python
   - `license_backend_menu.py` - Menú interactivo para CLI
   - `GUIA_LICENCIAS_BACKEND.md` - Documentación completa

## 🚀 Pasos para Empezar (5 minutos)

### 1️⃣ Configurar el Backend

```bash
cd backend

# Si no existe .env, copiarlo desde .env.example
copy .env.example .env

# Instalar dependencias (si no las tenés)
pip install -r requirements.txt
```

### 2️⃣ Aplicar el Esquema SQL en Supabase

1. Ve a https://supabase.com y abre tu proyecto
2. Ve a **SQL Editor**
3. Copia y pega el contenido de `infra/supabase_schema.sql`
4. Ejecuta el SQL (botón "Run")

### 3️⃣ Iniciar el Backend

```bash
# Opción fácil (Windows)
start_backend.bat

# O manualmente
python -m uvicorn main:app --reload --port 8000
```

Deberías ver:
```
INFO:     Uvicorn running on http://0.0.0.0:8000
INFO:     Application startup complete.
```

### 4️⃣ Probar que Funciona

**Opción A: Navegador**
- Abre: http://localhost:8000/health
- Deberías ver: `{"ok": true}`

**Opción B: Script de pruebas**
```bash
# En otra terminal (deja el backend corriendo)
cd backend
python test_license_flow.py
```

## 📝 Ejemplos de Uso

### Desde Python (Programático)

```python
from backend_license_client import LicenseBackendClient

# Crear cliente
client = LicenseBackendClient()

# Verificar backend
healthy, error = client.health_check()
print(f"Backend: {'OK' if healthy else error}")

# Crear licencia (requiere ADMIN_TOKEN en .env)
success, data, error = client.create_license(
    name="Juan Pérez",
    days=90,
    email="juan@example.com"
)

if success:
    print(f"License Key: {data['license_key']}")
    
    # Activar licencia
    success, activation, error = client.activate_license(
        data['license_key']
    )
    
    if success:
        print(f"Días restantes: {activation['days_left']}")
```

### Desde el Menú Interactivo

```bash
python license_backend_menu.py
```

Opciones:
1. Crear nueva licencia (Admin)
2. Activar licencia
3. Verificar conexión con backend
4. Volver al menú principal

### Con curl (Manual)

**Crear licencia:**
```bash
curl -X POST http://localhost:8000/admin/licenses ^
  -H "Content-Type: application/json" ^
  -H "x-admin-token: admin_7f9c3e2a_2026_matidiaz_PRIVATE" ^
  -d "{\"name\":\"Cliente Test\",\"days\":60,\"email\":\"test@example.com\"}"
```

**Activar licencia:**
```bash
curl -X POST http://localhost:8000/activate ^
  -H "Content-Type: application/json" ^
  -d "{\"license_key\":\"TU_LICENSE_KEY_AQUI\"}"
```

## 🔗 Integrar con tu CLI Principal

Edita `app.py` y agrega:

```python
from license_backend_menu import license_management_menu

def menu():
    while True:
        banner()
        print("1) Opción existente 1")
        print("2) Opción existente 2")
        # ... tus opciones actuales ...
        print("9) Gestión de Licencias (Backend)")
        print("0) Salir")
        
        choice = input("Opción: ").strip()
        
        if choice == "9":
            license_management_menu()
        # ... resto de tu código ...
```

## 📊 Monitorear en Supabase

1. Ve a tu proyecto en Supabase
2. Abre **Table Editor**
3. Selecciona las tablas:
   - `customers` - Ver clientes
   - `licenses` - Ver licencias creadas
   - `license_activations` - Ver activaciones

## 🎯 Flujo de Trabajo Típico

### Como Administrador:

1. **Iniciar backend**
   ```bash
   cd backend
   start_backend.bat
   ```

2. **Crear licencia para un cliente**
   ```bash
   python license_backend_menu.py
   # Opción 1: Crear nueva licencia
   ```

3. **Copiar la license key** que se muestra

4. **Enviar al cliente** por email o mensaje seguro

### Como Cliente:

1. **Recibir license key** del administrador

2. **Activar licencia**
   ```bash
   python license_backend_menu.py
   # Opción 2: Activar licencia
   # Pegar la license key
   ```

3. **Verificar activación** - El sistema mostrará días restantes

## 🔧 Archivos Importantes

```
chat/
├── backend/
│   ├── main.py                    # Servidor FastAPI
│   ├── .env                       # Configuración (NO subir a git)
│   ├── .env.example              # Plantilla de configuración
│   ├── requirements.txt          # Dependencias
│   ├── test_license_flow.py      # Pruebas automatizadas
│   ├── start_backend.bat         # Script para iniciar servidor
│   ├── run_tests.bat             # Script para ejecutar pruebas
│   └── README.md                 # Documentación del backend
│
├── infra/
│   └── supabase_schema.sql       # Esquema de base de datos
│
├── backend_license_client.py     # Cliente HTTP para Python
├── license_backend_menu.py       # Menú interactivo CLI
└── GUIA_LICENCIAS_BACKEND.md     # Documentación completa
```

## ❓ Preguntas Frecuentes

**P: ¿Necesito tener el backend corriendo siempre?**
R: Solo cuando quieras crear o activar licencias. Puedes iniciarlo y detenerlo cuando necesites.

**P: ¿Puedo usar esto en producción?**
R: Sí, pero deberías desplegar el backend en un servicio como Railway, Render o Fly.io. Ver `GUIA_LICENCIAS_BACKEND.md` para instrucciones.

**P: ¿Las license keys son seguras?**
R: Sí, se almacenan hasheadas (SHA256) en la base de datos. Nunca se guardan en texto plano.

**P: ¿Puedo cambiar la duración de una licencia?**
R: Sí, puedes actualizar el campo `expires_at` directamente en Supabase.

**P: ¿Cómo revoco una licencia?**
R: Cambia `is_active` a `false` en la tabla `licenses` en Supabase.

## 🆘 Ayuda

Si algo no funciona:

1. **Verifica que el backend esté corriendo**
   - Abre http://localhost:8000/health en tu navegador

2. **Revisa los logs del backend**
   - Mira la terminal donde ejecutaste `start_backend.bat`

3. **Verifica las credenciales**
   - Asegurate de que `.env` tenga las credenciales correctas de Supabase

4. **Consulta la documentación completa**
   - Lee `GUIA_LICENCIAS_BACKEND.md`
   - Lee `backend/README.md`

## 🎉 ¡Listo!

Ya tenés todo configurado. Ahora podés:
- ✅ Crear licencias para tus clientes
- ✅ Activar licencias desde tu CLI
- ✅ Monitorear activaciones en Supabase
- ✅ Integrar con tu aplicación existente

**Próximo paso recomendado:** Ejecuta `backend/run_tests.bat` para verificar que todo funciona correctamente.
