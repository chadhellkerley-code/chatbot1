#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Cliente HTTP para interactuar con el backend de licencias FastAPI.
Permite crear y activar licencias desde la CLI.
"""

from __future__ import annotations

import getpass
import hashlib
import os
import platform
import socket
import sys
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

    def _response_detail(self, response: requests.Response) -> str:
        text = (response.text or "").strip()
        if text:
            return text
        try:
            payload = response.json()
        except ValueError:
            return ""
        return str(payload)
    
    def health_check(self) -> Tuple[bool, Optional[str]]:
        """
        Verifica que el backend este disponible.

        Returns:
            (success, error_message)
        """
        try:
            response = requests.get(
                f"{self.backend_url}/health",
                timeout=self.timeout
            )

            if response.status_code == 200:
                try:
                    data = response.json()
                except ValueError:
                    return False, f"Respuesta invalida (no JSON). Status 200: {response.text}"
                is_ok = False
                if isinstance(data, dict):
                    status_value = str(data.get("status", "")).lower()
                    is_ok = bool(data.get("ok")) or status_value == "ok"
                if is_ok:
                    return True, None
                detail = data.get("detail") if isinstance(data, dict) else data
                return False, f"Backend respondio pero no esta OK: {detail}"

            detail = self._response_detail(response)
            return False, f"Backend respondio con codigo {response.status_code}: {detail}"

        except requests.Timeout:
            return False, "Error de conexion: timeout"
        except requests.ConnectionError:
            return False, "Error de conexion: no se pudo conectar"
        except requests.RequestException as e:
            return False, f"Error de conexion: {str(e)}"

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
            days: Dias de validez (minimo 30)
            email: Email del cliente (opcional)

        Returns:
            (success, license_data, error_message)
        """
        if not self.admin_token:
            return False, None, "ADMIN_TOKEN no configurado"

        if days < 30:
            return False, None, "La duracion minima es 30 dias"

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

            if response.status_code in (200, 201):
                try:
                    data = response.json()
                except ValueError:
                    return False, None, (
                        f"Respuesta invalida (no JSON). "
                        f"Status {response.status_code}: {response.text}"
                    )
                return True, data, None

            status = response.status_code
            detail = self._response_detail(response)
            if status in (401, 403):
                return False, None, f"Token invalido (Error {status}): {detail}"
            if status == 422:
                return False, None, f"Payload invalido (Error 422): {detail}"
            if status >= 500:
                return False, None, f"Error del servidor (Error {status}): {detail}"
            return False, None, f"Error {status}: {detail}"

        except requests.Timeout:
            return False, None, "Error de conexion: timeout"
        except requests.ConnectionError:
            return False, None, "Error de conexion: no se pudo conectar"
        except requests.RequestException as e:
            return False, None, f"Error de conexion: {str(e)}"

    def activate_license(
        self,
        license_key: str,
        client_fingerprint: Optional[str] = None,
        machine_id: Optional[str] = None,
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
        
        if client_fingerprint:
            client_fingerprint = client_fingerprint.strip()
        if machine_id:
            machine_id = machine_id.strip()

        # Generar fingerprint si no se provee
        if not client_fingerprint and not machine_id:
            client_fingerprint = self._generate_fingerprint()
        if not client_fingerprint and machine_id:
            client_fingerprint = machine_id
        if not machine_id and client_fingerprint:
            machine_id = client_fingerprint

        payload = {
            "license_key": license_key.strip(),
            "client_fingerprint": client_fingerprint,
            "machine": machine_id,
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
        
        fingerprint = self._hash_components(components)
        if fingerprint:
            return fingerprint

        fallback = [
            platform.node(),
            getpass.getuser(),
            self._windows_volume_serial(),
        ]
        fingerprint = self._hash_components(fallback)
        if fingerprint:
            return fingerprint

        return f"fp-{uuid.uuid4().hex[:16]}"
    
    def _hash_components(self, components) -> Optional[str]:
        clean = [value.strip() for value in components if value and str(value).strip()]
        if not clean:
            return None
        fingerprint_str = "|".join(clean)
        fingerprint_hash = hashlib.sha256(fingerprint_str.encode()).hexdigest()
        return f"fp-{fingerprint_hash[:16]}"

    def _windows_volume_serial(self) -> str:
        if not sys.platform.startswith("win"):
            return ""
        try:
            import ctypes
            from ctypes import wintypes

            drive = os.environ.get("SystemDrive", "C:")
            root = drive.rstrip("\\") + "\\"
            serial = wintypes.DWORD()
            result = ctypes.windll.kernel32.GetVolumeInformationW(
                root, None, 0, ctypes.byref(serial), None, None, None, 0
            )
            if result:
                return f"{serial.value:08X}"
        except Exception:
            return ""
        return ""

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
    machine_id: Optional[str] = None,
    backend_url: Optional[str] = None,
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
    return client.activate_license(license_key, client_fingerprint, machine_id)


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
