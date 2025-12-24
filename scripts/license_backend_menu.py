#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Menú de gestión de licencias integrado con el backend FastAPI.
Se integra con el sistema de menús existente.
"""

from __future__ import annotations

import os
from datetime import datetime
from typing import Optional

from backend_license_client import LicenseBackendClient
from ui import Fore, banner, full_line, style_text
from utils import ask, ask_int, ok, press_enter, warn


def _print_section(title: str, color: str = Fore.CYAN) -> None:
    """Imprime una sección con título."""
    banner()
    print(style_text(title, color=color, bold=True))
    print(full_line(color=color))
    print()


def _print_license_details(data: dict, title: str = "Detalles de la licencia") -> None:
    """Imprime los detalles de una licencia de forma formateada."""
    print(full_line(color=Fore.GREEN))
    print(style_text(title, color=Fore.GREEN, bold=True))
    print(full_line(color=Fore.GREEN))
    
    if "license_key" in data:
        print(f"{style_text('License Key:', color=Fore.CYAN)} {style_text(data['license_key'], color=Fore.YELLOW, bold=True)}")
    
    if "customer_id" in data:
        print(f"{style_text('Customer ID:', color=Fore.CYAN)} {data['customer_id']}")
    
    if "expires_at" in data:
        expires = data['expires_at']
        try:
            # Intentar parsear y formatear la fecha
            if isinstance(expires, str):
                dt = datetime.fromisoformat(expires.replace('Z', '+00:00'))
                expires = dt.strftime('%Y-%m-%d %H:%M:%S UTC')
        except:
            pass
        print(f"{style_text('Expira:', color=Fore.CYAN)} {expires}")
    
    if "days_left" in data:
        days = data['days_left']
        color = Fore.GREEN if days > 30 else Fore.YELLOW if days > 7 else Fore.RED
        print(f"{style_text('Días restantes:', color=Fore.CYAN)} {style_text(str(days), color=color, bold=True)}")
    
    if "ok" in data:
        status = "✓ Activa" if data['ok'] else "✗ Inactiva"
        color = Fore.GREEN if data['ok'] else Fore.RED
        print(f"{style_text('Estado:', color=Fore.CYAN)} {style_text(status, color=color, bold=True)}")
    
    print(full_line(color=Fore.GREEN))


def check_backend_connection(client: LicenseBackendClient) -> bool:
    """
    Verifica la conexión con el backend.
    
    Returns:
        True si el backend está disponible, False en caso contrario
    """
    healthy, error = client.health_check()
    
    if not healthy:
        warn(f"No se pudo conectar al backend: {error}")
        print()
        print(style_text("Asegurate de que:", color=Fore.YELLOW))
        print(f"  1. El backend esté corriendo: {style_text('python -m uvicorn main:app --reload --port 8000', color=Fore.CYAN)}")
        print(f"  2. La URL esté configurada en .env: {style_text('BACKEND_URL=http://localhost:8000', color=Fore.CYAN)}")
        print()
        press_enter()
        return False
    
    return True


def menu_create_license() -> None:
    """Menú para crear una nueva licencia."""
    _print_section("Crear Nueva Licencia")
    
    # Inicializar cliente
    backend_url = os.getenv("BACKEND_URL", "http://localhost:8000")
    admin_token = os.getenv("ADMIN_TOKEN")
    
    if not admin_token:
        warn("ADMIN_TOKEN no configurado en .env")
        print()
        print(style_text("Para crear licencias necesitás configurar ADMIN_TOKEN en tu archivo .env", color=Fore.YELLOW))
        press_enter()
        return
    
    client = LicenseBackendClient(backend_url, admin_token)
    
    # Verificar conexión
    if not check_backend_connection(client):
        return
    
    # Solicitar datos
    print(style_text("Ingresá los datos del cliente:", color=Fore.WHITE))
    print()
    
    name = ask("Nombre del cliente: ").strip()
    if not name:
        warn("El nombre del cliente es requerido")
        press_enter()
        return
    
    email = ask("Email del cliente (opcional, Enter para omitir): ").strip()
    if not email:
        email = None
    
    days = ask_int("Días de validez (mínimo 30): ", min_value=30, default=60)
    
    # Confirmar
    print()
    print(full_line(color=Fore.BLUE))
    print(style_text("Resumen:", color=Fore.BLUE, bold=True))
    print(f"  Cliente: {name}")
    if email:
        print(f"  Email: {email}")
    print(f"  Duración: {days} días")
    print(full_line(color=Fore.BLUE))
    print()
    
    confirm = ask("¿Crear esta licencia? (s/N): ").strip().lower()
    if confirm != 's':
        warn("Operación cancelada")
        press_enter()
        return
    
    # Crear licencia
    print()
    print(style_text("Creando licencia...", color=Fore.CYAN))
    
    success, data, error = client.create_license(name, days, email)
    
    if success:
        print()
        ok("¡Licencia creada exitosamente!")
        print()
        _print_license_details(data, "Nueva Licencia Creada")
        print()
        print(style_text("⚠️  IMPORTANTE: Guardá esta license key de forma segura", color=Fore.YELLOW, bold=True))
        print(style_text("   No se podrá recuperar después", color=Fore.YELLOW))
        print()
    else:
        print()
        warn(f"Error al crear licencia: {error}")
        print()
    
    press_enter()


def menu_activate_license() -> None:
    """Menú para activar una licencia."""
    _print_section("Activar Licencia")
    
    # Inicializar cliente
    backend_url = os.getenv("BACKEND_URL", "http://localhost:8000")
    client = LicenseBackendClient(backend_url)
    
    # Verificar conexión
    if not check_backend_connection(client):
        return
    
    # Solicitar license key
    print(style_text("Ingresá tu código de licencia:", color=Fore.WHITE))
    print()
    
    license_key = ask("License Key: ").strip()
    if not license_key:
        warn("License key requerida")
        press_enter()
        return
    
    # Opcionalmente solicitar fingerprint personalizado
    print()
    use_custom = ask("¿Usar fingerprint personalizado? (s/N): ").strip().lower()
    fingerprint = None
    
    if use_custom == 's':
        fingerprint = ask("Fingerprint: ").strip()
        if not fingerprint:
            fingerprint = None
    
    # Activar licencia
    print()
    print(style_text("Activando licencia...", color=Fore.CYAN))
    
    success, data, error = client.activate_license(license_key, fingerprint)
    
    if success:
        print()
        ok("¡Licencia activada exitosamente!")
        print()
        _print_license_details(data, "Licencia Activada")
        print()
        
        # Mostrar advertencia si quedan pocos días
        days_left = data.get('days_left', 0)
        if days_left <= 7:
            print(style_text(f"⚠️  ADVERTENCIA: Solo quedan {days_left} días de licencia", color=Fore.RED, bold=True))
            print()
    else:
        print()
        warn(f"Error al activar licencia: {error}")
        print()
        
        # Sugerencias según el error
        if "invalid license" in str(error).lower():
            print(style_text("Posibles causas:", color=Fore.YELLOW))
            print("  • La license key es incorrecta")
            print("  • La licencia no existe en el sistema")
            print()
        elif "expired" in str(error).lower():
            print(style_text("La licencia ha expirado o está inactiva", color=Fore.YELLOW))
            print("  • Contactá al administrador para renovarla")
            print()
    
    press_enter()


def menu_check_backend() -> None:
    """Menú para verificar la conexión con el backend."""
    _print_section("Verificar Conexión con Backend")
    
    backend_url = os.getenv("BACKEND_URL", "http://localhost:8000")
    print(f"Backend URL: {style_text(backend_url, color=Fore.CYAN)}")
    print()
    
    client = LicenseBackendClient(backend_url)
    
    print(style_text("Verificando conexión...", color=Fore.CYAN))
    print()
    
    healthy, error = client.health_check()
    
    if healthy:
        ok("✓ Backend disponible y funcionando correctamente")
        print()
        print(full_line(color=Fore.GREEN))
        print(style_text("Estado del Backend", color=Fore.GREEN, bold=True))
        print(full_line(color=Fore.GREEN))
        print(f"  URL: {backend_url}")
        print(f"  Estado: {style_text('ONLINE', color=Fore.GREEN, bold=True)}")
        print(f"  Admin Token: {style_text('Configurado' if os.getenv('ADMIN_TOKEN') else 'NO CONFIGURADO', color=Fore.GREEN if os.getenv('ADMIN_TOKEN') else Fore.YELLOW)}")
        print(full_line(color=Fore.GREEN))
    else:
        warn(f"✗ Backend no disponible: {error}")
        print()
        print(full_line(color=Fore.RED))
        print(style_text("Estado del Backend", color=Fore.RED, bold=True))
        print(full_line(color=Fore.RED))
        print(f"  URL: {backend_url}")
        print(f"  Estado: {style_text('OFFLINE', color=Fore.RED, bold=True)}")
        print(f"  Error: {error}")
        print(full_line(color=Fore.RED))
        print()
        print(style_text("Para iniciar el backend:", color=Fore.YELLOW))
        print(f"  cd backend")
        print(f"  python -m uvicorn main:app --reload --port 8000")
    
    print()
    press_enter()


def license_management_menu() -> None:
    """Menú principal de gestión de licencias."""
    while True:
        _print_section("Gestión de Licencias (Backend)")
        
        print("1) Crear nueva licencia (Admin)")
        print("2) Activar licencia")
        print("3) Verificar conexión con backend")
        print("4) Volver al menú principal")
        print()
        
        choice = ask("Seleccioná una opción: ").strip()
        
        if choice == "1":
            menu_create_license()
        elif choice == "2":
            menu_activate_license()
        elif choice == "3":
            menu_check_backend()
        elif choice == "4":
            break
        else:
            warn("Opción inválida")
            press_enter()


# Para testing directo
if __name__ == "__main__":
    license_management_menu()
