import time
from src.tasks.auth import login_account_task
from src.tasks.messaging import send_message_task
from celery.result import AsyncResult

def run_test():
    print("🚀 INICIANDO PRUEBA DE FLUJO (CELERY + PLAYWRIGHT) 🚀")
    print("-" * 50)
    
    username = input("Ingresa usuario de IG de origen: ").strip()
    password = input("Ingresa contraseña: ").strip()
    target = input("Ingresa usuario destino para DM: ").strip()
    msg = input("Ingresa mensaje a enviar: ").strip()
    
    proxy_str = input("Proxy (url) [Enter para omitir]: ").strip()
    proxy = {"server": proxy_str} if proxy_str else None
    
    account_payload = {
        "username": username,
        "password": password,
        "proxy": proxy
    }
    
    print(f"\n[1/2] Enviando tarea de LOGIN para {username}...")
    # Enviamos tarea y esperamos (get es bloqueante aquí solo para el test)
    login_task_id = login_account_task.delay(account_payload)
    print(f"Task ID: {login_task_id}")
    
    print("Esperando resultado del worker (esto puede tardar unos segundos)...")
    try:
        login_result = login_task_id.get(timeout=120) # 2 min timeout
        print(f"Resultado Login: {login_result}")
        
        if login_result.get('status') != 'ok':
            print("❌ Login falló. Abortando.")
            return

    except Exception as e:
        print(f"❌ Error esperando login: {e}")
        return

    print(f"\n[2/2] Enviando tarea de MENSAJE a {target}...")
    msg_task_id = send_message_task.delay(username, password, proxy, target, msg)
    print(f"Task ID: {msg_task_id}")
    
    print("Esperando confirmación de envío...")
    try:
        msg_result = msg_task_id.get(timeout=300) # 5 min timeout
        print(f"Resultado Envío: {msg_result}")
        print("\n✅ ¡PRUEBA COMPLETADA CON ÉXITO!")
        
    except Exception as e:
        print(f"❌ Error en envío: {e}")

if __name__ == "__main__":
    run_test()
