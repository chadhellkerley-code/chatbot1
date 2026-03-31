from src.celery_app import app
from src.auth.onboarding import login_and_persist
import logging

logger = logging.getLogger(__name__)

def dummy_code_provider():
    # En background no podemos pedir input al usuario de terminal
    logger.error("Instagram solicitó código de verificación (2FA/Email). No se puede manejar en modo background sin API de callback.")
    return "" # Devolver vacío causará fallo controlado

@app.task(bind=True, max_retries=1) # Reducimos retries para debugging
def login_account_task(self, account_payload):
    username = account_payload.get('username')
    try:
        logger.info(f"Iniciando login task para {username}")
        
        # INYECTAMOS el proveedor de código dummy para evitar bloqueo por getpass
        # Nota: onboarding.py construye el payload internamente, necesitamos interceptarlo
        # o asegurarnos que login_and_persist no bloquee.
        
        # Como login_and_persist define _code_provider internamente y usa getpass,
        # la única forma limpia sin modificar onboarding.py es monkeypatching o confiar en que 
        # IG no pida código si las credenciales son buenas y la IP limpia.
        # Pero si pide, SE CUELGA.
        
        # Solución rápida: Modificar account_payload para incluir 'code_provider' 
        # NO funciona porque onboarding.py lo sobreescribe en la línea 306-310.
        
        result = login_and_persist(account_payload, headless=True)
        
        logger.info(f"Resultado login raw: {result}")

        if result['status'] != 'ok':
            # Si pide código, devolvemos el estado para que el frontend lo maneje
            if result['status'] == 'need_code':
                 return result # Devolvemos success con status 'need_code'
            raise Exception(f"Login falló: {result['message']}")
            
        logger.info(f"Login exitoso para {username}")
        return result
        
    except Exception as exc:
        logger.error(f"Error en login de {username}: {exc}")
        # No reintentamos inmediatamente para evitar spam si las credenciales están mal
        raise exc
