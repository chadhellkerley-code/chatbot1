#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script de Prueba Interactivo - Login y Envío con Playwright
Permite probar el login y envío de mensajes de forma directa
"""

import sys
import os
from pathlib import Path

# Configurar encoding para Windows
if os.name == 'nt':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# Agregar raíz del proyecto al path
sys.path.insert(0, str(Path(__file__).parent))

from src.auth.onboarding import login_and_persist, build_proxy
from src.instagram_adapter import human_login, is_logged_in
from playwright.sync_api import sync_playwright
from src.playwright_service import resolve_playwright_executable
import json

def clear_screen():
    """Limpiar pantalla"""
    import os
    os.system('cls' if os.name == 'nt' else 'clear')

def print_header(text):
    """Imprimir encabezado"""
    print("\n" + "=" * 70)
    print(f"  {text}")
    print("=" * 70 + "\n")

def get_account_info():
    """Solicitar información de la cuenta"""
    print_header("INFORMACIÓN DE LA CUENTA")
    
    username = input("Usuario de Instagram (sin @): ").strip()
    if not username:
        print("❌ Usuario requerido")
        return None
    
    password = input("Contraseña: ").strip()
    if not password:
        print("❌ Contraseña requerida")
        return None
    
    # TOTP opcional
    totp_secret = input("TOTP Secret (opcional, Enter para omitir): ").strip()
    
    # Proxy opcional
    use_proxy = input("¿Usar proxy? (s/N): ").strip().lower()
    proxy_config = None
    
    if use_proxy == 's':
        proxy_ip = input("  IP del proxy: ").strip()
        proxy_port = input("  Puerto del proxy: ").strip()
        proxy_user = input("  Usuario del proxy (opcional): ").strip()
        proxy_pass = input("  Contraseña del proxy (opcional): ").strip()
        
        if proxy_ip and proxy_port:
            proxy_config = {
                "ip": proxy_ip,
                "port": proxy_port,
                "username": proxy_user if proxy_user else None,
                "password": proxy_pass if proxy_pass else None,
            }
    
    return {
        "username": username,
        "password": password,
        "totp_secret": totp_secret if totp_secret else None,
        "proxy": build_proxy(proxy_config) if proxy_config else None,
    }

def test_login(account_info, headless=False):
    """Probar login con Playwright"""
    print_header("PROBANDO LOGIN CON PLAYWRIGHT")
    
    print(f"👤 Usuario: {account_info['username']}")
    print(f"🔒 TOTP: {'✅ Configurado' if account_info.get('totp_secret') else '❌ No configurado'}")
    print(f"🌐 Proxy: {'✅ Configurado' if account_info.get('proxy') else '❌ No configurado'}")
    print(f"👁️  Modo: {'Headless' if headless else 'Visible'}")
    print()
    
    # Preparar payload
    payload = {
        "username": account_info["username"],
        "password": account_info["password"],
    }
    
    if account_info.get("totp_secret"):
        # Agregar callback de TOTP
        import pyotp
        totp = pyotp.TOTP(account_info["totp_secret"])
        payload["totp_callback"] = lambda _: totp.now()
    
    if account_info.get("proxy"):
        payload["proxy"] = account_info["proxy"]
    
    print("🚀 Iniciando login...")
    
    try:
        result = login_and_persist(payload, headless=headless)
        
        print("\n" + "=" * 70)
        print("📊 RESULTADO DEL LOGIN")
        print("=" * 70)
        print(f"Estado: {result.get('status')}")
        print(f"Mensaje: {result.get('message')}")
        print(f"Perfil guardado en: {result.get('profile_path')}")
        print("=" * 70 + "\n")
        
        if result.get("status") == "ok":
            print("✅ LOGIN EXITOSO!")
            return True, result.get("profile_path")
        elif result.get("status") == "need_code":
            print("⚠️  Se requiere código de verificación")
            print("   Esto indica que Instagram solicitó verificación adicional")
            return False, None
        else:
            print("❌ LOGIN FALLIDO")
            return False, None
            
    except Exception as e:
        print(f"\n❌ Error durante el login: {e}")
        import traceback
        traceback.print_exc()
        return False, None

def test_send_message(account_info, profile_path):
    """Probar envío de mensaje usando el perfil guardado"""
    print_header("ENVIAR MENSAJE DE PRUEBA")
    
    recipient = input("Usuario destinatario (sin @): ").strip()
    if not recipient:
        print("❌ Destinatario requerido")
        return False
    
    message = input("Mensaje a enviar: ").strip()
    if not message:
        print("❌ Mensaje requerido")
        return False
    
    print(f"\n📤 Enviando mensaje a @{recipient}...")
    print(f"💬 Mensaje: {message}")
    print()
    
    try:
        with sync_playwright() as p:
            # Lanzar navegador con el perfil guardado
            executable = resolve_playwright_executable(headless=False)
            browser = p.chromium.launch_persistent_context(
                user_data_dir=profile_path,
                headless=False,  # Visible para ver el envío
                executable_path=str(executable) if executable else None,
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--disable-dev-shm-usage',
                ]
            )
            
            page = browser.pages[0] if browser.pages else browser.new_page()
            
            # Verificar que sigue logueado
            page.goto("https://www.instagram.com/direct/inbox/", wait_until="domcontentloaded")
            page.wait_for_timeout(3000)
            
            if not is_logged_in(page):
                print("❌ La sesión expiró. Necesitas hacer login nuevamente.")
                browser.close()
                return False
            
            print("✅ Sesión válida, navegando a DMs...")
            
            # Ir a la conversación
            dm_url = f"https://www.instagram.com/direct/t/{recipient}/"
            page.goto(dm_url, wait_until="domcontentloaded")
            page.wait_for_timeout(2000)
            
            # Buscar el campo de mensaje
            message_selectors = [
                "textarea[placeholder*='Message']",
                "textarea[placeholder*='Mensaje']",
                "div[contenteditable='true'][role='textbox']",
                "textarea[aria-label*='Message']",
            ]
            
            message_box = None
            for selector in message_selectors:
                try:
                    message_box = page.wait_for_selector(selector, timeout=5000)
                    if message_box:
                        print(f"✅ Campo de mensaje encontrado: {selector}")
                        break
                except:
                    continue
            
            if not message_box:
                print("❌ No se encontró el campo de mensaje")
                browser.close()
                return False
            
            # Escribir y enviar mensaje
            print("⌨️  Escribiendo mensaje...")
            message_box.fill(message)
            page.wait_for_timeout(1000)
            
            # Buscar botón de enviar
            send_selectors = [
                "button:has-text('Send')",
                "button:has-text('Enviar')",
                "button[type='submit']",
            ]
            
            send_button = None
            for selector in send_selectors:
                try:
                    send_button = page.wait_for_selector(selector, timeout=3000)
                    if send_button:
                        print(f"✅ Botón de enviar encontrado: {selector}")
                        break
                except:
                    continue
            
            if send_button:
                print("📤 Enviando mensaje...")
                send_button.click()
                page.wait_for_timeout(2000)
                print("✅ MENSAJE ENVIADO!")
            else:
                print("⚠️  No se encontró botón de enviar, presiona Enter manualmente")
                page.keyboard.press("Enter")
                page.wait_for_timeout(2000)
                print("✅ Enter presionado")
            
            # Esperar un momento para ver el resultado
            print("\n⏳ Esperando 5 segundos para verificar...")
            page.wait_for_timeout(5000)
            
            browser.close()
            return True
            
    except Exception as e:
        print(f"\n❌ Error al enviar mensaje: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Función principal"""
    clear_screen()
    
    print("""
╔══════════════════════════════════════════════════════════════════╗
║                                                                  ║
║     PRUEBA INTERACTIVA - LOGIN Y ENVÍO CON PLAYWRIGHT           ║
║                                                                  ║
║  Este script te permite probar el login con Playwright          ║
║  y enviar mensajes de prueba de forma directa                   ║
║                                                                  ║
╚══════════════════════════════════════════════════════════════════╝
    """)
    
    # Obtener información de la cuenta
    account_info = get_account_info()
    if not account_info:
        print("\n❌ Información de cuenta incompleta")
        return
    
    # Preguntar modo headless
    print()
    headless_input = input("¿Ejecutar login en modo headless (sin ver navegador)? (s/N): ").strip().lower()
    headless = headless_input == 's'
    
    # Probar login
    success, profile_path = test_login(account_info, headless=headless)
    
    if not success:
        print("\n❌ No se pudo completar el login")
        return
    
    # Preguntar si quiere enviar mensaje de prueba
    print()
    send_test = input("¿Quieres enviar un mensaje de prueba? (S/n): ").strip().lower()
    
    if send_test != 'n':
        test_send_message(account_info, profile_path)
    
    print("\n" + "=" * 70)
    print("  ✅ PRUEBA COMPLETADA")
    print("=" * 70)
    print(f"\n📁 Perfil guardado en: {profile_path}")
    print("   Puedes reutilizar este perfil para futuros logins\n")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Prueba cancelada por el usuario")
    except Exception as e:
        print(f"\n❌ Error inesperado: {e}")
        import traceback
        traceback.print_exc()
