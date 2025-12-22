#!/usr/bin/env python3
"""
Script de testing para el nuevo sistema de jobs.

Prueba:
1. Login con sesión en Redis
2. Envío de mensaje con rate limiting
3. Polling de mensajes
4. Auto-respuesta

Uso:
    python run_test_jobs.py
"""

import time
import sys
from pathlib import Path

# Agregar src al path
sys.path.insert(0, str(Path(__file__).parent))

from src.jobs.login_job import login_account, refresh_session
from src.jobs.send_message_job import send_dm
from src.jobs.read_messages_job import poll_account
from src.state_manager import get_state_manager
from celery.result import AsyncResult


def print_header(text: str):
    """Imprime header bonito."""
    print(f"\n{'='*60}")
    print(f"  {text}")
    print(f"{'='*60}\n")


def wait_for_task(task_id: str, timeout: int = 120) -> dict:
    """Espera a que una tarea termine y devuelve el resultado."""
    result = AsyncResult(task_id)
    print(f"⏳ Esperando tarea {task_id[:8]}...")
    
    start = time.time()
    while not result.ready():
        if time.time() - start > timeout:
            print(f"❌ Timeout esperando tarea")
            return {'error': 'timeout'}
        time.sleep(1)
        print(".", end="", flush=True)
    
    print()
    
    if result.successful():
        return result.result
    else:
        print(f"❌ Tarea falló: {result.info}")
        return {'error': str(result.info)}


def test_login():
    """Test de login con Redis."""
    print_header("TEST 1: Login con Redis")
    
    username = input("Usuario de IG: ").strip()
    password = input("Contraseña: ").strip()
    proxy_url = input("Proxy (Enter para omitir): ").strip()
    
    proxy = {'server': proxy_url} if proxy_url else None
    
    account_payload = {
        'username': username,
        'password': password,
        'proxy': proxy,
    }
    
    print(f"\n📤 Enviando job de login para @{username}...")
    task = login_account.delay(account_payload)
    print(f"✅ Task ID: {task.id}")
    
    result = wait_for_task(task.id)
    
    if result.get('status') == 'ok':
        print(f"✅ Login exitoso!")
        print(f"   Profile: {result.get('profile_path')}")
        
        # Verificar en Redis
        state_mgr = get_state_manager()
        if state_mgr.session_exists(username):
            print(f"✅ Sesión guardada en Redis")
        
        return username, password, proxy
    else:
        print(f"❌ Login falló: {result.get('message')}")
        return None, None, None


def test_send_message(username: str, password: str, proxy: dict):
    """Test de envío de mensaje."""
    print_header("TEST 2: Envío de Mensaje")
    
    target = input("Usuario destino: ").strip()
    message = input("Mensaje: ").strip()
    
    print(f"\n📤 Enviando mensaje de @{username} a @{target}...")
    task = send_dm.delay(
        username=username,
        password=password,
        proxy=proxy,
        target_user=target,
        message_text=message,
        human_delay=True,
    )
    print(f"✅ Task ID: {task.id}")
    
    result = wait_for_task(task.id, timeout=300)
    
    if result.get('success'):
        print(f"✅ Mensaje enviado!")
        print(f"   Contador diario: {result.get('daily_count')}")
    else:
        print(f"❌ Envío falló: {result.get('error')}")


def test_poll_messages(username: str, password: str, proxy: dict):
    """Test de polling de mensajes."""
    print_header("TEST 3: Polling de Mensajes")
    
    print(f"\n📤 Verificando mensajes de @{username}...")
    task = poll_account.delay(
        username=username,
        password=password,
        proxy=proxy,
    )
    print(f"✅ Task ID: {task.id}")
    
    result = wait_for_task(task.id)
    
    if 'error' not in result:
        print(f"✅ Polling completado!")
        print(f"   Mensajes no leídos: {result.get('unread_count', 0)}")
        print(f"   Threads: {result.get('threads', 0)}")
        
        new_messages = result.get('new_messages', [])
        if new_messages:
            print(f"\n📨 Mensajes nuevos:")
            for msg in new_messages[:5]:  # Mostrar máximo 5
                print(f"   - {msg.get('text', '(sin texto)')[:50]}")
    else:
        print(f"❌ Polling falló: {result.get('error')}")


def test_state_manager(username: str):
    """Test del state manager."""
    print_header("TEST 4: State Manager")
    
    state_mgr = get_state_manager()
    
    # Verificar sesión
    print(f"🔍 Verificando estado de @{username}...")
    
    has_session = state_mgr.session_exists(username)
    print(f"   Sesión en Redis: {'✅ Sí' if has_session else '❌ No'}")
    
    # Obtener estado
    state = state_mgr.load_account_state(username)
    if state:
        print(f"   Estado: {state.get('status', 'unknown')}")
        print(f"   Última actividad: {state.get('last_activity', 'N/A')}")
    
    # Contadores
    msg_count = state_mgr.get_daily_counter(username, 'messages_sent')
    print(f"   Mensajes hoy: {msg_count}")
    
    # Rate limit
    is_limited = state_mgr.is_rate_limited(username)
    print(f"   Rate limited: {'⚠️ Sí' if is_limited else '✅ No'}")


def main():
    """Ejecuta todos los tests."""
    print("""
    ╔════════════════════════════════════════════════════════╗
    ║                                                        ║
    ║        🚀 TEST SUITE - Instagram Automation 🚀        ║
    ║                                                        ║
    ║  Sistema de jobs con Redis, rate limiting y stealth   ║
    ║                                                        ║
    ╚════════════════════════════════════════════════════════╝
    """)
    
    # Test 1: Login
    username, password, proxy = test_login()
    if not username:
        print("\n❌ No se pudo hacer login. Abortando tests.")
        return
    
    input("\n⏸️  Presiona Enter para continuar con el test de envío...")
    
    # Test 2: Envío
    test_send_message(username, password, proxy)
    
    input("\n⏸️  Presiona Enter para continuar con el test de polling...")
    
    # Test 3: Polling
    test_poll_messages(username, password, proxy)
    
    input("\n⏸️  Presiona Enter para ver el estado en Redis...")
    
    # Test 4: State Manager
    test_state_manager(username)
    
    print_header("✅ TESTS COMPLETADOS")
    print("""
    Próximos pasos:
    
    1. Iniciar worker de Celery:
       celery -A src.queue_config worker --loglevel=info -Q login,messages,polling,replies
    
    2. Iniciar Celery Beat (para polling automático):
       celery -A src.queue_config beat --loglevel=info
    
    3. Monitorear con Flower:
       celery -A src.queue_config flower
    
    4. Ver logs de Redis:
       redis-cli monitor
    """)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrumpido por el usuario")
    except Exception as e:
        print(f"\n\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
