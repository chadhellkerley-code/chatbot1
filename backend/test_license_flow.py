#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script de pruebas para el flujo completo de licencias.
Prueba la creación de licencias (admin) y activación (cliente).
"""

import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

BACKEND_URL = os.getenv("BACKEND_URL", "http://localhost:8000")
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN")

# Colores para terminal
class Colors:
    GREEN = "\033[92m"
    RED = "\033[91m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    CYAN = "\033[96m"
    RESET = "\033[0m"
    BOLD = "\033[1m"


def print_section(title: str, color: str = Colors.CYAN):
    """Imprime una sección con título."""
    print(f"\n{color}{Colors.BOLD}{'=' * 60}{Colors.RESET}")
    print(f"{color}{Colors.BOLD}{title.center(60)}{Colors.RESET}")
    print(f"{color}{Colors.BOLD}{'=' * 60}{Colors.RESET}\n")


def print_success(msg: str):
    """Imprime mensaje de éxito."""
    print(f"{Colors.GREEN}[OK] {msg}{Colors.RESET}")


def print_error(msg: str):
    """Imprime mensaje de error."""
    print(f"{Colors.RED}[ERROR] {msg}{Colors.RESET}")


def print_info(msg: str):
    """Imprime mensaje informativo."""
    print(f"{Colors.BLUE}[INFO] {msg}{Colors.RESET}")


def print_warning(msg: str):
    """Imprime mensaje de advertencia."""
    print(f"{Colors.YELLOW}[WARN] {msg}{Colors.RESET}")


def test_health_check():
    """Prueba el endpoint de health check."""
    print_section("1. Health Check", Colors.CYAN)
    
    try:
        response = requests.get(f"{BACKEND_URL}/health", timeout=5)
        if response.status_code == 200 and response.json().get("ok"):
            print_success(f"Backend respondiendo en {BACKEND_URL}")
            return True
        else:
            print_error(f"Backend respondió con código {response.status_code}")
            return False
    except requests.RequestException as e:
        print_error(f"No se pudo conectar al backend: {e}")
        print_info(f"Asegurate de que el backend esté corriendo en {BACKEND_URL}")
        return False


def test_create_license():
    """Prueba la creación de una licencia."""
    print_section("2. Crear Licencia (Admin)", Colors.CYAN)
    
    if not ADMIN_TOKEN:
        print_error("ADMIN_TOKEN no configurado en .env")
        return None
    
    # Datos de prueba
    test_data = {
        "name": "Cliente de Prueba",
        "days": 60,
        "email": f"test_{datetime.now().timestamp()}@example.com"
    }
    
    print_info(f"Creando licencia para: {test_data['name']}")
    print_info(f"Duración: {test_data['days']} días")
    print_info(f"Email: {test_data['email']}")
    
    try:
        response = requests.post(
            f"{BACKEND_URL}/admin/licenses",
            json=test_data,
            headers={
                "Content-Type": "application/json",
                "x-admin-token": ADMIN_TOKEN
            },
            timeout=10
        )
        
        if response.status_code == 200:
            data = response.json()
            print_success("Licencia creada exitosamente!")
            print(f"\n{Colors.GREEN}{Colors.BOLD}Detalles de la licencia:{Colors.RESET}")
            print(f"  License Key: {Colors.YELLOW}{data['license_key']}{Colors.RESET}")
            print(f"  Customer ID: {data['customer_id']}")
            print(f"  Expira: {data['expires_at']}")
            return data
        else:
            print_error(f"Error al crear licencia: {response.status_code}")
            print_error(f"Respuesta: {response.text}")
            return None
            
    except requests.RequestException as e:
        print_error(f"Error de conexión: {e}")
        return None


def test_activate_license(license_key: str):
    """Prueba la activación de una licencia."""
    print_section("3. Activar Licencia (Cliente)", Colors.CYAN)
    
    print_info(f"Activando licencia: {license_key}")
    
    # Datos de activación
    activation_data = {
        "license_key": license_key,
        "client_fingerprint": f"test-machine-{os.getpid()}"
    }
    
    try:
        response = requests.post(
            f"{BACKEND_URL}/activate",
            json=activation_data,
            headers={
                "Content-Type": "application/json",
                "User-Agent": "TestClient/1.0"
            },
            timeout=10
        )
        
        if response.status_code == 200:
            data = response.json()
            print_success("Licencia activada exitosamente!")
            print(f"\n{Colors.GREEN}{Colors.BOLD}Detalles de activación:{Colors.RESET}")
            print(f"  Estado: {'OK' if data['ok'] else 'FAIL'}")
            print(f"  Días restantes: {Colors.YELLOW}{data['days_left']}{Colors.RESET}")
            print(f"  Customer ID: {data['customer_id']}")
            return True
        else:
            print_error(f"Error al activar licencia: {response.status_code}")
            print_error(f"Respuesta: {response.text}")
            return False
            
    except requests.RequestException as e:
        print_error(f"Error de conexión: {e}")
        return False


def test_activate_invalid_license():
    """Prueba activar una licencia inválida."""
    print_section("4. Probar Licencia Inválida", Colors.CYAN)
    
    invalid_key = "INVALID-LICENSE-KEY-123"
    print_info(f"Intentando activar licencia inválida: {invalid_key}")
    
    try:
        response = requests.post(
            f"{BACKEND_URL}/activate",
            json={"license_key": invalid_key},
            headers={"Content-Type": "application/json"},
            timeout=10
        )
        
        if response.status_code == 403:
            print_success("Validación funcionando correctamente (rechazó licencia inválida)")
            return True
        else:
            print_warning(f"Respuesta inesperada: {response.status_code}")
            return False
            
    except requests.RequestException as e:
        print_error(f"Error de conexión: {e}")
        return False


def test_multiple_activations(license_key: str):
    """Prueba múltiples activaciones de la misma licencia."""
    print_section("5. Múltiples Activaciones", Colors.CYAN)
    
    print_info("Probando 3 activaciones consecutivas de la misma licencia...")
    
    success_count = 0
    for i in range(1, 4):
        print(f"\n{Colors.BLUE}Activación #{i}:{Colors.RESET}")
        
        try:
            response = requests.post(
                f"{BACKEND_URL}/activate",
                json={
                    "license_key": license_key,
                    "client_fingerprint": f"machine-{i}"
                },
                headers={"Content-Type": "application/json"},
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                print_success(f"Activación #{i} exitosa - Días restantes: {data['days_left']}")
                success_count += 1
            else:
                print_error(f"Activación #{i} falló: {response.status_code}")
                
        except requests.RequestException as e:
            print_error(f"Error en activación #{i}: {e}")
    
    if success_count == 3:
        print_success(f"\nTodas las activaciones exitosas ({success_count}/3)")
        return True
    else:
        print_warning(f"\nAlgunas activaciones fallaron ({success_count}/3)")
        return False


def run_all_tests():
    """Ejecuta todas las pruebas."""
    print_section("PRUEBAS DEL FLUJO DE LICENCIAS", Colors.BOLD)
    print_info(f"Backend URL: {BACKEND_URL}")
    print_info(f"Admin Token: {'Configurado' if ADMIN_TOKEN else 'NO CONFIGURADO'}")
    
    results = {
        "health": False,
        "create": False,
        "activate": False,
        "invalid": False,
        "multiple": False
    }
    
    # 1. Health check
    results["health"] = test_health_check()
    if not results["health"]:
        print_error("\n❌ Backend no disponible. Abortando pruebas.")
        return False
    
    # 2. Crear licencia
    license_data = test_create_license()
    results["create"] = license_data is not None
    
    if not license_data:
        print_error("\n❌ No se pudo crear licencia. Abortando pruebas restantes.")
        return False
    
    license_key = license_data["license_key"]
    
    # 3. Activar licencia
    results["activate"] = test_activate_license(license_key)
    
    # 4. Probar licencia inválida
    results["invalid"] = test_activate_invalid_license()
    
    # 5. Múltiples activaciones
    results["multiple"] = test_multiple_activations(license_key)
    
    # Resumen
    print_section("RESUMEN DE PRUEBAS", Colors.BOLD)
    
    total = len(results)
    passed = sum(1 for v in results.values() if v)
    
    for test_name, passed_test in results.items():
        status = f"{Colors.GREEN}✓ PASS{Colors.RESET}" if passed_test else f"{Colors.RED}✗ FAIL{Colors.RESET}"
        print(f"  {test_name.upper().ljust(20)}: {status}")
    
    print(f"\n{Colors.BOLD}Total: {passed}/{total} pruebas pasaron{Colors.RESET}")
    
    if passed == total:
        print_success("\n[SUCCESS] Todas las pruebas pasaron exitosamente!")
        return True
    else:
        print_warning(f"\n[WARN] {total - passed} prueba(s) fallaron")
        return False


if __name__ == "__main__":
    try:
        success = run_all_tests()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print(f"\n\n{Colors.YELLOW}Pruebas interrumpidas por el usuario{Colors.RESET}")
        sys.exit(130)
