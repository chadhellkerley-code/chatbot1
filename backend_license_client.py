#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Cliente HTTP para interactuar con el backend de licencias FastAPI.
Permite crear y activar licencias desde la CLI.
"""

from __future__ import annotations

import hashlib
import os
import platform
import socket
import uuid
from datetime import datetime
from typing import Dict, Optional, Tuple

import requests
from dotenv import load_dotenv

load_dotenv()


class LicenseBackendClient:
    """Cliente para interactuar con el backend de licencias."""
    
    def __init__(self, backend_url: Optional[str] = None, admin_token: Optional[str] = None):
        """
        Inicializa el cliente.
        
        Args:
            backend_url: URL del backend (por defecto usa BACKEND_URL del .env)
            admin_token: Token de administrador (por defecto usa ADMIN_TOKEN del .env)
        """
        self.backend_url = (backend_url or os.getenv("BACKEND_URL", "http://localhost:8000")).rstrip("/")
        self.admin_token = admin_token or os.getenv("ADMIN_TOKEN")
        self.timeout = 15
    
    def health_check(self) -> Tuple[bool, Optional[str]]:
        """
        Verifica que el backend esté disponible.
        
        Returns:
            (success, error_message)
        """
        try:
            response = requests.get(
                f"{self.backend_url}/health",
                timeout=self.timeout
            )
            
            if response.status_code == 200:
                data = response.json()
                if data.get("ok"):
                    return True, None
                return False, "Backend respondió pero no está OK"
            
            return False, f"Backend respondió con código {response.status_code}"
            
        except requests.RequestException as e:
            return False, f"Error de conexión: {str(e)}"
    
    def create_license(
        self,
        name: str,
        days: int,
        email: Optional[str] = None
    ) -> Tuple[bool, Optional[Dict], Optional[str]]:
        """
        Crea una nueva licencia (requiere permisos de admin).
        
        Args:
            name: Nombre del cliente
            days: Días de validez (mínimo 30)
            email: Email del cliente (opcional)
        
        Returns:
            (success, license_data, error_message)
        """
        if not self.admin_token:
            return False, None, "ADMIN_TOKEN no configurado"
        
        if days < 30:
            return False, None, "La duración mínima es 30 días"
        
        payload = {
            "name": name,
            "days": days
        }
        
        if email:
            payload["email"] = email
        
        try:
            response = requests.post(
                f"{self.backend_url}/admin/licenses",
                json=payload,
                headers={
                    "Content-Type": "application/json",
                    "x-admin-token": self.admin_token
                },
                timeout=self.timeout
            )
            
            if response.status_code == 200:
                data = response.json()
                return True, data, None
            
            error_detail = response.text
            try:
                error_json = response.json()
                error_detail = error_json.get("detail", error_detail)
            except:
                pass
            
            return False, None, f"Error {response.status_code}: {error_detail}"
            
        except requests.RequestException as e:
            return False, None, f"Error de conexión: {str(e)}"
    
    def activate_license(
        self,
        license_key: str,
        client_fingerprint: Optional[str] = None
    ) -> Tuple[bool, Optional[Dict], Optional[str]]:
        """
        Activa una licencia.
        
        Args:
            license_key: Clave de licencia
            client_fingerprint: Identificador del cliente (se genera automáticamente si no se provee)
        
        Returns:
            (success, activation_data, error_message)
        """
        if not license_key or not license_key.strip():
            return False, None, "License key requerida"
        
        # Generar fingerprint si no se provee
        if not client_fingerprint:
            client_fingerprint = self._generate_fingerprint()
        
        payload = {
            "license_key": license_key.strip(),
            "client_fingerprint": client_fingerprint
        }
        
        try:
            response = requests.post(
                f"{self.backend_url}/activate",
                json=payload,
                headers={
                    "Content-Type": "application/json",
                    "User-Agent": self._get_user_agent()
                },
                timeout=self.timeout
            )
            
            if response.status_code == 200:
                data = response.json()
                return True, data, None
            
            error_detail = response.text
            try:
                error_json = response.json()
                error_detail = error_json.get("detail", error_detail)
            except:
                pass
            
            return False, None, f"Error {response.status_code}: {error_detail}"
            
        except requests.RequestException as e:
            return False, None, f"Error de conexión: {str(e)}"
    
    def _generate_fingerprint(self) -> str:
        """
        Genera un fingerprint único para la máquina actual.
        
        Returns:
            Fingerprint único
        """
        components = [
            platform.node(),  # Nombre del host
            platform.machine(),  # Arquitectura
            platform.system(),  # Sistema operativo
            str(uuid.getnode()),  # MAC address
        ]
        
        # Agregar hostname si está disponible
        try:
            components.append(socket.gethostname())
        except:
            pass
        
        # Crear hash
        fingerprint_str = "|".join(components)
        fingerprint_hash = hashlib.sha256(fingerprint_str.encode()).hexdigest()
        
        return f"fp-{fingerprint_hash[:16]}"
    
    def _get_user_agent(self) -> str:
        """
        Genera un User-Agent descriptivo.
        
        Returns:
            User-Agent string
        """
        return f"LicenseClient/1.0 ({platform.system()} {platform.release()}; {platform.machine()})"


# Funciones de conveniencia para uso directo
def create_license(
    name: str,
    days: int,
    email: Optional[str] = None,
    backend_url: Optional[str] = None,
    admin_token: Optional[str] = None
) -> Tuple[bool, Optional[Dict], Optional[str]]:
    """
    Crea una nueva licencia.
    
    Args:
        name: Nombre del cliente
        days: Días de validez
        email: Email del cliente (opcional)
        backend_url: URL del backend (opcional, usa .env por defecto)
        admin_token: Token de admin (opcional, usa .env por defecto)
    
    Returns:
        (success, license_data, error_message)
    """
    client = LicenseBackendClient(backend_url, admin_token)
    return client.create_license(name, days, email)


def activate_license(
    license_key: str,
    client_fingerprint: Optional[str] = None,
    backend_url: Optional[str] = None
) -> Tuple[bool, Optional[Dict], Optional[str]]:
    """
    Activa una licencia.
    
    Args:
        license_key: Clave de licencia
        client_fingerprint: Identificador del cliente (opcional)
        backend_url: URL del backend (opcional, usa .env por defecto)
    
    Returns:
        (success, activation_data, error_message)
    """
    client = LicenseBackendClient(backend_url)
    return client.activate_license(license_key, client_fingerprint)


def check_backend_health(backend_url: Optional[str] = None) -> Tuple[bool, Optional[str]]:
    """
    Verifica que el backend esté disponible.
    
    Args:
        backend_url: URL del backend (opcional, usa .env por defecto)
    
    Returns:
        (is_healthy, error_message)
    """
    client = LicenseBackendClient(backend_url)
    return client.health_check()


# Ejemplo de uso
if __name__ == "__main__":
    import sys
    
    # Verificar salud del backend
    print("Verificando backend...")
    healthy, error = check_backend_health()
    
    if not healthy:
        print(f"❌ Backend no disponible: {error}")
        sys.exit(1)
    
    print("✓ Backend disponible")
    
    # Ejemplo: Crear licencia
    print("\nCreando licencia de prueba...")
    success, data, error = create_license(
        name="Cliente Demo",
        days=60,
        email="demo@example.com"
    )
    
    if success:
        print(f"✓ Licencia creada:")
        print(f"  License Key: {data['license_key']}")
        print(f"  Customer ID: {data['customer_id']}")
        print(f"  Expira: {data['expires_at']}")
        
        # Ejemplo: Activar licencia
        print("\nActivando licencia...")
        success, activation, error = activate_license(data['license_key'])
        
        if success:
            print(f"✓ Licencia activada:")
            print(f"  Días restantes: {activation['days_left']}")
            print(f"  Customer ID: {activation['customer_id']}")
        else:
            print(f"❌ Error al activar: {error}")
    else:
        print(f"❌ Error al crear licencia: {error}")
