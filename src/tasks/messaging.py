from src.celery_app import app
from src.transport.human_instagram_sender import HumanInstagramSender
import logging
import os
import time
import random

logger = logging.getLogger(__name__)
ALLOW_UNVERIFIED = os.getenv("HUMAN_DM_ALLOW_UNVERIFIED", "0").strip().lower() in {"1", "true", "yes", "y"}

@app.task(bind=True)
def send_message_task(self, username, password, proxy, target_user, message_text):
    """
    Tarea para enviar un DM usando Playwright de forma 'humana'.
    Requiere que el login se haya realizado previamente para tener cookies.
    """
    logger.info(f"Worker: Iniciando tarea de envío para {username} -> {target_user}")
    
    # Simulación de delay humano más robusto
    delay = random.uniform(5, 15)
    logger.info(f"Worker: Esperando {delay:.2f}s antes de enviar...")
    time.sleep(delay)
    
    sender = None
    try:
        # Instanciamos el sender headless
        sender = HumanInstagramSender(headless=True)
        
        # Construimos el objeto account mínimo necesario
        account_payload = {
            "username": username,
            "password": password,
            "proxy": proxy
        }
        
        # Ejecutamos el envío
        logger.info(f"Worker: Navegando para enviar DM...")
        success, detail, payload = sender.send_message_like_human_sync(
            account_payload,
            target_user,
            message_text,
            return_detail=True,
            return_payload=True,
        )

        skip_reason = (payload.get("skip_reason") or detail or "").strip()
        if skip_reason == "NO_DM_BUTTON":
            logger.info("skip | no_dm | Perfil sin botón de mensaje / no permite DM")
            return {"success": False, "skipped": True, "reason": "NO_DM_BUTTON"}

        is_unverified = (
            payload.get("sent_unverified")
            or (payload.get("reason_code") or "").strip().upper() == "SENT_UNVERIFIED"
            or (detail or "").strip().lower() == "sent_unverified"
        )
        if is_unverified and ALLOW_UNVERIFIED:
            logger.warning(
                "warn | sent_unverified | Se intentó enviar y no se pudo verificar en DOM; no cuenta como error"
            )
            return {"success": True, "sent_unverified": True}

        if not success:
            raise Exception("HumanInstagramSender devolvió False")
        if is_unverified:
            raise Exception(detail or "sent_unverified")
            
        logger.info(f"Worker: Mensaje enviado exitosamente a {target_user}")
        return {"success": True, "sender": username, "target": target_user}
        
    except Exception as e:
        logger.error(f"Worker: Fallo al enviar a {target_user}: {e}")
        # Reintentamos en 2 minutos si falla
        raise self.retry(exc=e, countdown=120, max_retries=3)
    finally:
        # Nota: HumanInstagramSender maneja su propio ciclo de vida de navegador internamente
        pass
