"""Jobs de envio de mensajes con validacion de cupo por cuenta."""

from __future__ import annotations

import logging
import os
import random
import time
from typing import Any, Dict, Optional

from celery import Task
from celery.exceptions import Retry

from core.account_limits import can_send_message_for_account
from core.accounts import get_account
from core.storage import log_sent
from src.queue_config import app
from src.state_manager import get_state_manager
from src.transport.human_instagram_sender import HumanInstagramSender

logger = logging.getLogger(__name__)

MAX_MESSAGES_PER_DAY = int(os.getenv("MAX_MESSAGES_PER_DAY", "100"))
MAX_MESSAGES_PER_HOUR = int(os.getenv("MAX_MESSAGES_PER_HOUR", "20"))


class SendMessageTask(Task):
    """Tarea de envio con rate limiting y delays humanos."""

    autoretry_for = (Exception,)
    retry_kwargs = {"max_retries": 5, "countdown": 60}
    retry_backoff = True
    retry_backoff_max = 1800
    retry_jitter = True

    def before_start(self, task_id, args, kwargs):
        """Antes de iniciar, verificar rate limits."""
        del task_id
        username = kwargs.get("username") or (args[0] if args else None)
        if not username:
            return

        state_mgr = get_state_manager()
        if state_mgr.is_rate_limited(username):
            logger.warning("Account @%s is rate limited, retrying later", username)
            raise Retry(countdown=300)


@app.task(
    bind=True,
    base=SendMessageTask,
    name="src.jobs.send_message_job.send_dm",
    queue="messages",
    priority=7,
    time_limit=300,
    soft_time_limit=270,
    rate_limit="10/m",
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
    """Envia un DM usando Playwright y respeta el cupo configurado en la cuenta."""
    state_mgr = get_state_manager()

    try:
        logger.info("[SEND JOB] @%s -> @%s", username, target_user)

        account_record = get_account(username) or {"username": username}
        can_send, sent_today, effective_limit = can_send_message_for_account(
            account=account_record,
            username=username,
            default=MAX_MESSAGES_PER_DAY,
        )
        if not can_send:
            logger.warning(
                "Account quota reached for @%s (%s/%s)",
                username,
                sent_today,
                effective_limit,
            )
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
                "sender": username,
                "target": target_user,
                "daily_count": int(sent_today),
                "daily_limit": int(effective_limit or 0),
            }

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
        success, detail, payload = sender.send_message_like_human_sync(
            account_payload,
            target_user,
            message_text,
            return_detail=True,
            return_payload=True,
        )

        is_unverified = (
            payload.get("sent_unverified")
            or (payload.get("reason_code") or "").strip().upper() == "SENT_UNVERIFIED"
            or (detail or "").strip().lower() == "sent_unverified"
        )
        if success:
            count = state_mgr.increment_daily_counter(username, "messages_sent")
            logger.info(
                "Message sent successfully. Daily count: %s/%s",
                count,
                effective_limit or MAX_MESSAGES_PER_DAY,
            )
            log_sent(
                username,
                target_user,
                True,
                detail or ("sent_unverified" if is_unverified else "sent_verified"),
                verified=not is_unverified,
                sent_unverified=bool(is_unverified),
            )
            if is_unverified:
                logger.warning(
                    "warn | sent_unverified | Se intento enviar y no se pudo verificar en DOM; no cuenta como error"
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
                "sent_unverified": bool(is_unverified),
            }

        skip_reason = (payload.get("skip_reason") or detail or "").strip().upper()
        if skip_reason in {"SKIPPED_NO_DM", "NO_DM_BUTTON"}:
            logger.info("skip | no_dm | Perfil sin boton de mensaje / no permite DM")
            log_sent(
                username,
                target_user,
                False,
                skip_reason or "SKIPPED_NO_DM",
                skip=True,
                skip_reason="SKIPPED_NO_DM",
            )
            return {
                "success": False,
                "skipped": True,
                "reason": "SKIPPED_NO_DM",
                "sender": username,
                "target": target_user,
            }

        error_msg = detail or "Unknown error"
        logger.warning("Message failed: %s", error_msg)
        log_sent(username, target_user, False, error_msg)

        if "rate" in error_msg.lower() or "limit" in error_msg.lower():
            state_mgr.set_rate_limit(username, 3600)
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
    """Envia mensajes a multiples usuarios con delays entre cada uno."""
    results = []
    sent = 0
    failed = 0

    for target in targets:
        try:
            if results:
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
                countdown=random.randint(5, 20),
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
