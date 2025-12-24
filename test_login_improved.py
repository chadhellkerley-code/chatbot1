#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script de Prueba Mejorado - Login con Playwright
Versión con mejor detección de login exitoso y debugging
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

from src.instagram_adapter import human_login, is_logged_in
from playwright.sync_api import sync_playwright
import time

def print_header(text):
    """Imprimir encabezado"""
    print("\n" + "=" * 70)
    print(f"  {text}")
    print("=" * 70 + "\n")

def test_login_simple():
    """Prueba de login simple y directa"""
    print_header("PRUEBA DE LOGIN CON PLAYWRIGHT - VERSION MEJORADA")
    
    # Solicitar credenciales
    username = input("Usuario de Instagram (sin @): ").strip()
    if not username:
        print("[X] Usuario requerido")
        return
    
    password = input("Contraseña: ").strip()
    if not password:
        print("[X] Contraseña requerida")
        return
    
    # TOTP opcional
    totp_secret = input("TOTP Secret (opcional, Enter para omitir): ").strip()
    
    # Configurar TOTP callback si se proporciona
    totp_provider = None
    if totp_secret:
        try:
            import pyotp
            totp = pyotp.TOTP(totp_secret)
            totp_provider = lambda _: totp.now()
            print(f"[OK] TOTP configurado. Código actual: {totp.now()}")
        except ImportError:
            print("[!] pyotp no instalado, TOTP no disponible")
        except Exception as e:
            print(f"[!] Error configurando TOTP: {e}")
    
    # Preguntar modo
    headless_input = input("\n¿Ejecutar en modo headless (sin ver navegador)? (s/N): ").strip().lower()
    headless = headless_input == 's'
    
    print(f"\n[*] Iniciando login...")
    print(f"    Usuario: {username}")
    print(f"    TOTP: {'Configurado' if totp_provider else 'No configurado'}")
    print(f"    Modo: {'Headless' if headless else 'Visible'}")
    print()
    
    # Directorio de perfil
    profile_dir = Path("profiles") / username
    profile_dir.mkdir(parents=True, exist_ok=True)
    
    try:
        with sync_playwright() as p:
            # Lanzar navegador
            print("[*] Lanzando navegador...")
            
            browser = p.chromium.launch(
                headless=headless,
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--disable-dev-shm-usage',
                ]
            )
            
            # Crear contexto
            context = browser.new_context(
                viewport={'width': 1280, 'height': 720},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            )
            
            page = context.new_page()
            
            # Ejecutar login
            print("[*] Ejecutando login humano...")
            print()
            
            try:
                result = human_login(
                    page,
                    username,
                    password,
                    totp_secret=totp_secret if totp_secret else None,
                    totp_provider=totp_provider,
                )
                
                print("\n" + "=" * 70)
                print("  RESULTADO DEL LOGIN")
                print("=" * 70)
                
                # Esperar un momento para que la página se estabilice
                print("\n[*] Esperando estabilización de la página...")
                page.wait_for_timeout(3000)
                
                # Verificar estado
                current_url = page.url
                print(f"[*] URL actual: {current_url}")
                
                # Verificar si está logueado
                logged_in = is_logged_in(page)
                print(f"[*] is_logged_in(): {logged_in}")
                
                # Verificaciones adicionales
                print("\n[*] Verificaciones adicionales:")
                
                # 1. Verificar URL
                if "/accounts/login" in current_url:
                    print("    [!] Todavía en página de login")
                elif "/challenge/" in current_url:
                    print("    [!] En página de challenge/verificación")
                elif "instagram.com" in current_url and "/accounts/login" not in current_url:
                    print("    [OK] URL indica login exitoso")
                
                # 2. Verificar elementos de navegación
                nav_selectors = [
                    "a[href='/direct/inbox/']",
                    "nav[role='navigation']",
                    "svg[aria-label='Home']",
                    "svg[aria-label='Inicio']",
                ]
                
                nav_found = False
                for selector in nav_selectors:
                    try:
                        if page.locator(selector).count() > 0:
                            print(f"    [OK] Elemento de navegación encontrado: {selector}")
                            nav_found = True
                            break
                    except:
                        pass
                
                if not nav_found:
                    print("    [!] No se encontraron elementos de navegación")
                
                # 3. Verificar cookies
                cookies = context.cookies()
                session_cookies = [c for c in cookies if 'session' in c.get('name', '').lower()]
                print(f"    [*] Cookies de sesión encontradas: {len(session_cookies)}")
                
                # Decisión final
                print("\n" + "=" * 70)
                if logged_in or nav_found or (len(session_cookies) > 0 and "/accounts/login" not in current_url):
                    print("  [OK] LOGIN EXITOSO!")
                    print("=" * 70)
                    
                    # Guardar storage state
                    storage_path = profile_dir / "storage_state.json"
                    context.storage_state(path=str(storage_path))
                    print(f"\n[OK] Sesión guardada en: {storage_path}")
                    
                    # Navegar a inbox para confirmar
                    print("\n[*] Navegando a inbox para confirmar...")
                    page.goto("https://www.instagram.com/direct/inbox/", wait_until="domcontentloaded")
                    page.wait_for_timeout(3000)
                    
                    final_url = page.url
                    print(f"[*] URL final: {final_url}")
                    
                    if "/direct/inbox" in final_url or "/direct/t/" in final_url:
                        print("[OK] Confirmado: Acceso a inbox exitoso")
                    else:
                        print("[!] No se pudo acceder a inbox, puede requerir verificación")
                    
                    # Preguntar si quiere enviar mensaje de prueba
                    print("\n" + "=" * 70)
                    send_test = input("\n¿Quieres enviar un mensaje de prueba? (S/n): ").strip().lower()
                    
                    if send_test != 'n':
                        test_send_message(page, username)
                    
                else:
                    print("  [X] LOGIN FALLIDO o REQUIERE VERIFICACIÓN")
                    print("=" * 70)
                    print(f"\n[*] URL actual: {current_url}")
                    print("[*] Revisa el navegador para ver qué pasó")
                    
                    if not headless:
                        input("\nPresiona Enter para cerrar el navegador...")
                
            except Exception as e:
                print(f"\n[X] Error durante el login: {e}")
                import traceback
                traceback.print_exc()
                
                if not headless:
                    print("\n[*] El navegador permanecerá abierto para inspección")
                    input("Presiona Enter para cerrar...")
            
            finally:
                browser.close()
                
    except Exception as e:
        print(f"\n[X] Error al iniciar navegador: {e}")
        import traceback
        traceback.print_exc()

def test_send_message(page, username):
    """Enviar mensaje de prueba"""
    print_header("ENVIAR MENSAJE DE PRUEBA")
    
    recipient = input("Usuario destinatario (sin @): ").strip()
    if not recipient:
        print("[X] Destinatario requerido")
        return
    
    message = input("Mensaje a enviar: ").strip()
    if not message:
        print("[X] Mensaje requerido")
        return
    
    print(f"\n[*] Enviando mensaje a @{recipient}...")
    
    try:
        # Ir a la conversación
        dm_url = f"https://www.instagram.com/direct/t/{recipient}/"
        page.goto(dm_url, wait_until="domcontentloaded")
        page.wait_for_timeout(2000)
        
        # Buscar campo de mensaje
        message_selectors = [
            "textarea[placeholder*='Message']",
            "textarea[placeholder*='Mensaje']",
            "div[contenteditable='true'][role='textbox']",
        ]
        
        message_box = None
        for selector in message_selectors:
            try:
                message_box = page.wait_for_selector(selector, timeout=5000)
                if message_box:
                    print(f"[OK] Campo de mensaje encontrado")
                    break
            except:
                continue
        
        if not message_box:
            print("[X] No se encontró el campo de mensaje")
            return
        
        # Escribir mensaje
        print("[*] Escribiendo mensaje...")
        message_box.fill(message)
        page.wait_for_timeout(1000)
        
        # Presionar Enter para enviar
        print("[*] Enviando...")
        page.keyboard.press("Enter")
        page.wait_for_timeout(2000)
        
        print("[OK] MENSAJE ENVIADO!")
        print("\n[*] Esperando 3 segundos para verificar...")
        page.wait_for_timeout(3000)
        
    except Exception as e:
        print(f"\n[X] Error al enviar mensaje: {e}")
        import traceback
        traceback.print_exc()

def main():
    """Función principal"""
    os.system('cls' if os.name == 'nt' else 'clear')
    
    print("""
╔══════════════════════════════════════════════════════════════════╗
║                                                                  ║
║     PRUEBA DE LOGIN CON PLAYWRIGHT - VERSION MEJORADA           ║
║                                                                  ║
║  Versión con mejor detección de login exitoso y debugging       ║
║                                                                  ║
╚══════════════════════════════════════════════════════════════════╝
    """)
    
    test_login_simple()
    
    print("\n" + "=" * 70)
    print("  PRUEBA COMPLETADA")
    print("=" * 70 + "\n")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n[!] Prueba cancelada por el usuario")
    except Exception as e:
        print(f"\n[X] Error inesperado: {e}")
        import traceback
        traceback.print_exc()
