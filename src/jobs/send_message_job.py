"""Job de envíos de mensajes con delays humanos y rate limiting."""

from __future__ import annotations

import logging
import os
import random
import time
from typing import Any, Dict, Optional

from celery import Task
from celery.exceptions import Retry

from src.queue_config import app
from src.state_manager import get_state_manager
from src.transport.human_instagram_sender import HumanInstagramSender

logger = logging.getLogger(__name__)

# Límites diarios por cuenta (configurables vía env)
MAX_MESSAGES_PER_DAY = int(os.getenv("MAX_MESSAGES_PER_DAY", "100"))
MAX_MESSAGES_PER_HOUR = int(os.getenv("MAX_MESSAGES_PER_HOUR", "20"))


class SendMessageTask(Task):
    """Tarea de envío con rate limiting y delays humanos."""

    autoretry_for = (Exception,)
    retry_kwargs = {"max_retries": 5, "countdown": 60}
    retry_backoff = True
    retry_backoff_max = 1800  # 30 minutos máx
    retry_jitter = True

    def before_start(self, task_id, args, kwargs):
        """Antes de iniciar, verificar rate limits y contadores."""
        username = kwargs.get("username") or (args[0] if args else None)
        if not username:
            return

        state_mgr = get_state_manager()

        # Verificar si está rate limited
        if state_mgr.is_rate_limited(username):
            logger.warning("Account @%s is rate limited, retrying later", username)
            raise Retry(countdown=300)  # Reintentar en 5 minutos

        # Verificar límite diario
        daily_count = state_mgr.get_daily_counter(username, "messages_sent")
        if daily_count >= MAX_MESSAGES_PER_DAY:
            logger.error(
                "Daily limit reached for @%s (%s/%s)",
                username,
                daily_count,
                MAX_MESSAGES_PER_DAY,
            )
            raise Exception(
                f"Daily message limit reached: {daily_count}/{MAX_MESSAGES_PER_DAY}"
            )


@app.task(
    bind=True,
    base=SendMessageTask,
    name="src.jobs.send_message_job.send_dm",
    queue="messages",
    priority=7,
    time_limit=300,
    soft_time_limit=270,
    rate_limit="10/m",  # Máx 10 mensajes por minuto por worker
)
def send_dm(
    self,
    username: str,
    password: str,
    proxy: Optional[Dict],
    target_user: str,
    message_text: str,
    human_delay: bool = True,
) -> Dict[str, Any]:
    """
    Envía DM con comportamiento humano usando Playwright.
    """
    state_mgr = get_state_manager()

    try:
        logger.info("[SEND JOB] @%s -> @%s", username, target_user)

        # Delay humano previo
        if human_delay:
            delay = random.uniform(5, 15)
            logger.debug("Human delay: %.2fs", delay)
            time.sleep(delay)

        account_payload = {
            "username": username,
            "password": password,
            "proxy": proxy,
        }

        sender = HumanInstagramSender(headless=True)
        success, detail = sender.send_message_like_human(
            account_payload,
            target_user,
            message_text,
            return_detail=True,
        )

        if success:
            count = state_mgr.increment_daily_counter(username, "messages_sent")
            logger.info(
                "Message sent successfully. Daily count: %s/%s",
                count,
                MAX_MESSAGES_PER_DAY,
            )
            state_mgr.save_account_state(
                username,
                {
                    "status": "active",
                    "last_message_sent": time.time(),
                },
            )
            return {
                "success": True,
                "sender": username,
                "target": target_user,
                "daily_count": count,
            }

        # Manejo de error
        error_msg = detail or "Unknown error"
        logger.warning("Message failed: %s", error_msg)

        if "rate" in error_msg.lower() or "limit" in error_msg.lower():
            state_mgr.set_rate_limit(username, 3600)  # 1 hora
            logger.error("Rate limit detected for @%s, pausing for 1h", username)
        elif "challenge" in error_msg.lower() or "checkpoint" in error_msg.lower():
            state_mgr.save_account_state(
                username,
                {
                    "status": "challenge",
                    "last_error": error_msg,
                },
            )
            logger.error("Challenge detected for @%s", username)

        raise self.retry(exc=Exception(error_msg))

    except Exception as exc:
        logger.error("[SEND JOB] Error @%s -> @%s: %s", username, target_user, exc)
        state_mgr.save_account_state(
            username,
            {
                "status": "error",
                "last_error": str(exc),
            },
        )
        raise self.retry(exc=exc)


@app.task(
    name="src.jobs.send_message_job.send_bulk",
    queue="messages",
    priority=6,
)
def send_bulk(
    username: str,
    password: str,
    proxy: Optional[Dict],
    targets: list[str],
    message_text: str,
) -> Dict[str, Any]:
    """
    Envía mensaje a múltiples usuarios con delays entre cada uno.
    """
    results = []
    sent = 0
    failed = 0

    for target in targets:
        try:
            if results:  # No delay en el primero
                delay = random.uniform(30, 90)
                logger.info("Waiting %.1fs before next message...", delay)
                time.sleep(delay)

            task = send_dm.apply_async(
                kwargs={
                    "username": username,
                    "password": password,
                    "proxy": proxy,
                    "target_user": target,
                    "message_text": message_text,
                    "human_delay": True,
                },
                countdown=random.randint(5, 20),  # Delay adicional aleatorio
            )

            results.append(
                {
                    "target": target,
                    "task_id": task.id,
                    "status": "queued",
                }
            )
            sent += 1

        except Exception as exc:
            logger.error("Failed to queue message to @%s: %s", target, exc)
            results.append(
                {
                    "target": target,
                    "status": "failed",
                    "error": str(exc),
                }
            )
            failed += 1

    return {
        "total": len(targets),
        "sent": sent,
        "failed": failed,
        "results": results,
    }
