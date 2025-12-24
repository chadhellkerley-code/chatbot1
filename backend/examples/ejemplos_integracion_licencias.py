#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Ejemplo de integración del sistema de licencias con el backend FastAPI.
Este archivo muestra cómo usar el sistema de licencias en tu código.
"""

from backend_license_client import LicenseBackendClient, activate_license, create_license
from datetime import datetime


# ============================================================================
# EJEMPLO 1: Verificar licencia al iniciar la aplicación
# ============================================================================

def check_license_on_startup(license_key: str) -> bool:
    """
    Verifica y activa una licencia al iniciar la aplicación.
    
    Args:
        license_key: License key del usuario
    
    Returns:
        True si la licencia es válida, False en caso contrario
    """
    print("Verificando licencia...")
    
    # Activar licencia
    success, data, error = activate_license(license_key)
    
    if not success:
        print(f"❌ Licencia inválida: {error}")
        return False
    
    days_left = data.get('days_left', 0)
    
    # Advertir si quedan pocos días
    if days_left <= 7:
        print(f"⚠️  ADVERTENCIA: Tu licencia expira en {days_left} días")
    elif days_left <= 30:
        print(f"ℹ️  Tu licencia expira en {days_left} días")
    else:
        print(f"✓ Licencia válida ({days_left} días restantes)")
    
    return True


# ============================================================================
# EJEMPLO 2: Crear licencia para un nuevo cliente (Admin)
# ============================================================================

def create_client_license(client_name: str, client_email: str, duration_days: int = 90):
    """
    Crea una nueva licencia para un cliente.
    
    Args:
        client_name: Nombre del cliente
        client_email: Email del cliente
        duration_days: Días de validez (por defecto 90)
    
    Returns:
        License key si se creó exitosamente, None en caso contrario
    """
    print(f"Creando licencia para {client_name}...")
    
    success, data, error = create_license(
        name=client_name,
        days=duration_days,
        email=client_email
    )
    
    if not success:
        print(f"❌ Error al crear licencia: {error}")
        return None
    
    license_key = data['license_key']
    expires_at = data['expires_at']
    
    print(f"✓ Licencia creada exitosamente")
    print(f"  License Key: {license_key}")
    print(f"  Expira: {expires_at}")
    
    return license_key


# ============================================================================
# EJEMPLO 3: Clase para gestionar licencias en tu aplicación
# ============================================================================

class LicenseManager:
    """Gestor de licencias para tu aplicación."""
    
    def __init__(self, backend_url: str = None):
        """
        Inicializa el gestor de licencias.
        
        Args:
            backend_url: URL del backend (opcional, usa .env por defecto)
        """
        self.client = LicenseBackendClient(backend_url)
        self.license_key = None
        self.customer_id = None
        self.days_left = 0
        self.is_valid = False
    
    def activate(self, license_key: str) -> bool:
        """
        Activa una licencia.
        
        Args:
            license_key: License key a activar
        
        Returns:
            True si la activación fue exitosa
        """
        success, data, error = self.client.activate_license(license_key)
        
        if not success:
            print(f"Error al activar licencia: {error}")
            return False
        
        self.license_key = license_key
        self.customer_id = data.get('customer_id')
        self.days_left = data.get('days_left', 0)
        self.is_valid = data.get('ok', False)
        
        return self.is_valid
    
    def check_expiration(self) -> bool:
        """
        Verifica si la licencia está por expirar.
        
        Returns:
            True si la licencia está por expirar (< 7 días)
        """
        return self.days_left < 7
    
    def get_status_message(self) -> str:
        """
        Obtiene un mensaje descriptivo del estado de la licencia.
        
        Returns:
            Mensaje de estado
        """
        if not self.is_valid:
            return "Licencia inválida o no activada"
        
        if self.days_left == 0:
            return "Licencia expirada"
        elif self.days_left < 7:
            return f"⚠️  Licencia expira en {self.days_left} días"
        elif self.days_left < 30:
            return f"Licencia válida ({self.days_left} días restantes)"
        else:
            return f"Licencia activa ({self.days_left} días restantes)"


# ============================================================================
# EJEMPLO 4: Decorador para proteger funciones con licencia
# ============================================================================

def require_license(license_manager: LicenseManager):
    """
    Decorador que requiere una licencia válida para ejecutar una función.
    
    Args:
        license_manager: Instancia de LicenseManager
    
    Returns:
        Decorador
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            if not license_manager.is_valid:
                print("❌ Esta función requiere una licencia válida")
                return None
            
            if license_manager.days_left == 0:
                print("❌ Tu licencia ha expirado")
                return None
            
            return func(*args, **kwargs)
        
        return wrapper
    return decorator


# ============================================================================
# EJEMPLO 5: Uso completo en una aplicación
# ============================================================================

def main_example():
    """Ejemplo completo de uso del sistema de licencias."""
    
    print("=" * 60)
    print("EJEMPLO DE USO DEL SISTEMA DE LICENCIAS")
    print("=" * 60)
    print()
    
    # 1. Crear gestor de licencias
    license_mgr = LicenseManager()
    
    # 2. Solicitar license key al usuario
    print("Por favor ingresa tu license key:")
    license_key = input("> ").strip()
    
    if not license_key:
        print("❌ License key requerida")
        return
    
    # 3. Activar licencia
    print("\nActivando licencia...")
    if not license_mgr.activate(license_key):
        print("❌ No se pudo activar la licencia")
        return
    
    # 4. Mostrar estado
    print(f"\n✓ {license_mgr.get_status_message()}")
    print(f"  Customer ID: {license_mgr.customer_id}")
    
    # 5. Verificar si está por expirar
    if license_mgr.check_expiration():
        print("\n⚠️  ADVERTENCIA: Tu licencia está por expirar")
        print("   Contacta al administrador para renovarla")
    
    # 6. Usar funciones protegidas
    @require_license(license_mgr)
    def protected_function():
        print("\n✓ Función protegida ejecutada exitosamente")
        return "Resultado de la función"
    
    result = protected_function()
    if result:
        print(f"  Resultado: {result}")


# ============================================================================
# EJEMPLO 6: Integración con tu menú principal
# ============================================================================

def integrate_with_main_menu():
    """
    Ejemplo de cómo integrar el sistema de licencias con tu menú principal.
    """
    
    # Inicializar gestor de licencias
    license_mgr = LicenseManager()
    
    # Verificar licencia al inicio
    stored_license = load_license_from_storage()  # Tu función para cargar licencia guardada
    
    if stored_license:
        if license_mgr.activate(stored_license):
            print(f"✓ Licencia activada: {license_mgr.get_status_message()}")
        else:
            print("⚠️  Licencia inválida, por favor ingresa una nueva")
            # Solicitar nueva licencia
    else:
        print("No se encontró licencia, por favor activa una")
        # Solicitar licencia
    
    # Continuar con tu menú normal
    # ...


# ============================================================================
# FUNCIONES AUXILIARES
# ============================================================================

def load_license_from_storage() -> str:
    """
    Carga la license key almacenada (implementa según tu sistema).
    
    Returns:
        License key o None si no existe
    """
    # Ejemplo: leer de un archivo
    try:
        with open('storage/license.txt', 'r') as f:
            return f.read().strip()
    except:
        return None


def save_license_to_storage(license_key: str) -> bool:
    """
    Guarda la license key (implementa según tu sistema).
    
    Args:
        license_key: License key a guardar
    
    Returns:
        True si se guardó exitosamente
    """
    # Ejemplo: guardar en un archivo
    try:
        import os
        os.makedirs('storage', exist_ok=True)
        with open('storage/license.txt', 'w') as f:
            f.write(license_key)
        return True
    except:
        return False


# ============================================================================
# EJEMPLO 7: Uso en modo batch/script
# ============================================================================

def batch_create_licenses():
    """
    Ejemplo de creación de licencias en batch para múltiples clientes.
    """
    clients = [
        {"name": "Cliente A", "email": "clientea@example.com", "days": 90},
        {"name": "Cliente B", "email": "clienteb@example.com", "days": 180},
        {"name": "Cliente C", "email": "clientec@example.com", "days": 365},
    ]
    
    print("Creando licencias en batch...")
    print()
    
    results = []
    
    for client in clients:
        license_key = create_client_license(
            client_name=client["name"],
            client_email=client["email"],
            duration_days=client["days"]
        )
        
        if license_key:
            results.append({
                "name": client["name"],
                "email": client["email"],
                "license_key": license_key
            })
        
        print()
    
    # Guardar resultados en un archivo
    if results:
        print("Guardando resultados...")
        with open('licenses_created.txt', 'w') as f:
            for r in results:
                f.write(f"{r['name']},{r['email']},{r['license_key']}\n")
        print(f"✓ {len(results)} licencias creadas y guardadas en licenses_created.txt")


# ============================================================================
# PUNTO DE ENTRADA
# ============================================================================

if __name__ == "__main__":
    # Descomentar el ejemplo que quieras probar:
    
    # Ejemplo básico
    # main_example()
    
    # Crear licencias en batch
    # batch_create_licenses()
    
    # Verificar una licencia específica
    # check_license_on_startup("TU_LICENSE_KEY_AQUI")
    
    print("Este archivo contiene ejemplos de uso.")
    print("Edita el código y descomenta el ejemplo que quieras probar.")
