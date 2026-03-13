from __future__ import annotations

import logging
import os
import random
import time

from core.account_limits import can_send_message_for_account
from core.accounts import get_account
from core.storage import log_sent
from src.celery_app import app
from src.transport.human_instagram_sender import HumanInstagramSender

logger = logging.getLogger(__name__)
ALLOW_UNVERIFIED = os.getenv("HUMAN_DM_ALLOW_UNVERIFIED", "0").strip().lower() in {"1", "true", "yes", "y"}
MAX_MESSAGES_PER_DAY = int(os.getenv("MAX_MESSAGES_PER_DAY", "100"))


@app.task(bind=True)
def send_message_task(self, username, password, proxy, target_user, message_text):
    """Ruta legacy de envio directo, ahora alineada con el cupo por cuenta."""
    logger.info("Worker: Iniciando tarea de envio para %s -> %s", username, target_user)

    account_record = get_account(username) or {"username": username}
    can_send, sent_today, effective_limit = can_send_message_for_account(
        account=account_record,
        username=username,
        default=MAX_MESSAGES_PER_DAY,
    )
    if not can_send:
        log_sent(
            username,
            target_user,
            False,
            "account_quota_reached",
            skip=True,
            skip_reason="ACCOUNT_QUOTA_REACHED",
        )
        return {
            "success": False,
            "skipped": True,
            "reason": "ACCOUNT_QUOTA_REACHED",
            "daily_count": int(sent_today),
            "daily_limit": int(effective_limit or 0),
        }

    delay = random.uniform(5, 15)
    logger.info("Worker: Esperando %.2fs antes de enviar...", delay)
    time.sleep(delay)

    try:
        sender = HumanInstagramSender(headless=True)
        account_payload = {
            "username": username,
            "password": password,
            "proxy": proxy,
        }
        logger.info("Worker: Navegando para enviar DM...")
        success, detail, payload = sender.send_message_like_human_sync(
            account_payload,
            target_user,
            message_text,
            return_detail=True,
            return_payload=True,
        )

        skip_reason = (payload.get("skip_reason") or detail or "").strip().upper()
        if skip_reason in {"SKIPPED_NO_DM", "NO_DM_BUTTON"}:
            log_sent(
                username,
                target_user,
                False,
                skip_reason or "SKIPPED_NO_DM",
                skip=True,
                skip_reason="SKIPPED_NO_DM",
            )
            logger.info("skip | no_dm | Perfil sin boton de mensaje / no permite DM")
            return {"success": False, "skipped": True, "reason": "SKIPPED_NO_DM"}

        is_unverified = (
            payload.get("sent_unverified")
            or (payload.get("reason_code") or "").strip().upper() == "SENT_UNVERIFIED"
            or (detail or "").strip().lower() == "sent_unverified"
        )
        if is_unverified and ALLOW_UNVERIFIED:
            log_sent(
                username,
                target_user,
                True,
                "sent_unverified",
                verified=False,
                sent_unverified=True,
            )
            logger.warning(
                "warn | sent_unverified | Se intento enviar y no se pudo verificar en DOM; no cuenta como error"
            )
            return {"success": True, "sent_unverified": True}

        if not success:
            error_msg = detail or "HumanInstagramSender devolvio False"
            log_sent(username, target_user, False, error_msg)
            if (payload.get("reason_code") or "").strip().upper() == "ACCOUNT_QUOTA_REACHED":
                return {"success": False, "skipped": True, "reason": "ACCOUNT_QUOTA_REACHED"}
            raise Exception(error_msg)
        if is_unverified:
            log_sent(username, target_user, False, detail or "sent_unverified")
            raise Exception(detail or "sent_unverified")

        log_sent(username, target_user, True, detail or "sent_verified", verified=True)
        logger.info("Worker: Mensaje enviado exitosamente a %s", target_user)
        return {"success": True, "sender": username, "target": target_user}

    except Exception as exc:
        logger.error("Worker: Fallo al enviar a %s: %s", target_user, exc)
        raise self.retry(exc=exc, countdown=120, max_retries=3)
