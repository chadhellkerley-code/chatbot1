#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script de prueba rápida - Envío de mensaje en segundo plano
"""

import sys
import os
from pathlib import Path

# Configurar encoding para Windows
if os.name == 'nt':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

sys.path.insert(0, str(Path(__file__).parent))

from adapters.instagram_playwright import InstagramPlaywrightClient

def test_headless_send():
    """Probar envío en segundo plano"""
    print("\n" + "=" * 70)
    print("  PRUEBA DE ENVIO EN SEGUNDO PLANO (HEADLESS)")
    print("=" * 70 + "\n")
    
    username = input("Tu usuario de Instagram (sin @): ").strip()
    recipient = input("Usuario destinatario (sin @): ").strip()
    message = input("Mensaje a enviar: ").strip()
    
    if not username or not recipient or not message:
        print("\n[X] Todos los campos son requeridos")
        return
    
    print(f"\n[*] Enviando mensaje de @{username} a @{recipient}")
    print(f"[*] Modo: HEADLESS (segundo plano)")
    print(f"[*] El navegador NO se mostrará")
    print()
    
    # Crear cliente con la cuenta
    account = {"username": username}
    client = InstagramPlaywrightClient(account=account)
    
    # Marcar como logueado (asumimos que ya hay sesión guardada)
    client._mark_logged_in(username)
    
    try:
        print("[*] Enviando...")
        result = client.send_direct_message(recipient, message)
        
        print("\n" + "=" * 70)
        if result:
            print("  [OK] MENSAJE ENVIADO EXITOSAMENTE!")
        else:
            print("  [X] ERROR AL ENVIAR MENSAJE")
        print("=" * 70 + "\n")
        
    except Exception as e:
        print(f"\n[X] Error: {e}\n")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_headless_send()
