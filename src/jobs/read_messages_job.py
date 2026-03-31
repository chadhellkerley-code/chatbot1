"""Inbox polling jobs backed by Playwright session storage."""

from __future__ import annotations

import logging
import random
import time
from typing import Any, Dict, Optional

from core.proxy_preflight import preflight_accounts_for_proxy_runtime
from core.proxy_registry import ProxyResolutionError
from src.proxy_payload import proxy_fields_from_proxy
from src.inbox.endpoint_reader import read_thread_from_storage, sync_account_threads_from_storage
from src.queue_config import app
from src.state_manager import get_state_manager


logger = logging.getLogger(__name__)


def _account_payload(username: str, proxy: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    payload: Dict[str, Any] = {"username": username}
    payload.update(proxy_fields_from_proxy(proxy or {}))
    return payload


@app.task(
    name="src.jobs.read_messages_job.poll_account",
    queue="polling",
    priority=5,
    time_limit=120,
)
def poll_account(username: str, password: str, proxy: Optional[Dict] = None) -> Dict[str, Any]:
    """Read unread inbox messages using controlled endpoint access."""

    del password
    state_mgr = get_state_manager()
    try:
        logger.info("[POLL JOB] Checking messages for @%s", username)
        if state_mgr.is_rate_limited(username):
            logger.debug("Skipping @%s - rate limited", username)
            return {"username": username, "skipped": True, "reason": "rate_limited"}

        account = _account_payload(username, proxy)
        rows = sync_account_threads_from_storage(
            account,
            thread_limit=20,
            message_limit=10,
            max_pages=1,
            timeout_seconds=10.0,
        )
        unread_threads = [row for row in rows if int(row.get("unread_count") or 0) > 0]

        new_messages: list[dict[str, Any]] = []
        seen_message_ids: set[str] = set()
        for row in unread_threads:
            thread_id = str(row.get("thread_id") or "").strip()
            if not thread_id:
                continue
            thread_payload = read_thread_from_storage(
                account,
                thread_id=thread_id,
                thread_href=str(row.get("thread_href") or ""),
                message_limit=10,
                timeout_seconds=10.0,
            )
            for message in thread_payload.get("messages") or []:
                if str(message.get("direction") or "").strip().lower() != "inbound":
                    continue
                message_id = str(message.get("message_id") or "").strip()
                if message_id and message_id in seen_message_ids:
                    continue
                if message_id:
                    seen_message_ids.add(message_id)
                new_messages.append(
                    {
                        "thread_id": thread_id,
                        "message_id": message_id,
                        "from_user": str(row.get("recipient_username") or "").strip(),
                        "text": str(message.get("text") or ""),
                        "timestamp": message.get("timestamp"),
                    }
                )

        logger.info(
            "[POLL JOB] @%s: %s new messages in %s threads",
            username,
            len(new_messages),
            len(unread_threads),
        )
        state_mgr.save_account_state(
            username,
            {
                "status": "active",
                "last_poll": time.time(),
                "unread_count": len(new_messages),
            },
        )
        return {
            "username": username,
            "unread_count": len(new_messages),
            "threads": len(unread_threads),
            "new_messages": new_messages,
        }
    except Exception as exc:
        logger.error("[POLL JOB] Error for @%s: %s", username, exc)
        if "rate" in str(exc).lower() or "limit" in str(exc).lower():
            state_mgr.set_rate_limit(username, 1800)
        return {
            "username": username,
            "error": str(exc),
            "unread_count": 0,
        }


@app.task(
    name="src.jobs.read_messages_job.poll_all_accounts",
    queue="polling",
    priority=4,
)
def poll_all_accounts() -> Dict[str, Any]:
    """Queue polling tasks for active accounts."""

    from core.accounts import is_account_enabled_for_operation, list_all

    logger.info("[POLL ALL] Starting periodic polling")
    active_accounts = [
        acc
        for acc in list_all()
        if acc.get("status") != "disabled" and is_account_enabled_for_operation(acc)
    ]
    logger.info("[POLL ALL] Polling %s accounts", len(active_accounts))

    preflight = preflight_accounts_for_proxy_runtime(active_accounts)
    ready_accounts = [dict(item) for item in (preflight.get("ready_accounts") or []) if isinstance(item, dict)]
    blocked_accounts = [dict(item) for item in (preflight.get("blocked_accounts") or []) if isinstance(item, dict)]

    results = []
    skipped_accounts: list[dict[str, str]] = []
    for blocked in blocked_accounts:
        username = str(blocked.get("username") or "").strip().lstrip("@")
        status = str(blocked.get("status") or "").strip() or "blocked"
        message = str(blocked.get("message") or "Proxy bloqueado.").strip() or "Proxy bloqueado."
        logger.warning("[POLL ALL] Skipping @%s due to proxy preflight: %s", username, message)
        skipped_accounts.append({
            "username": username,
            "status": status,
            "message": message,
        })

    for account in ready_accounts:
        username = account.get("username")
        password = account.get("password")
        try:
            from src.proxy_payload import proxy_from_account

            proxy = proxy_from_account(account)
        except ProxyResolutionError as exc:
            logger.warning("[POLL ALL] Skipping @%s because proxy resolution failed: %s", username, exc)
            skipped_accounts.append(
                {
                    "username": str(username or "").strip().lstrip("@"),
                    "status": "proxy_unresolved",
                    "message": str(exc) or "Proxy no resuelto.",
                }
            )
            continue
        except Exception:
            proxy = account.get("proxy")
        if not username or not password:
            continue

        delay = random.randint(0, 30)
        result = poll_account.apply_async(
            kwargs={
                "username": username,
                "password": password,
                "proxy": proxy,
            },
            countdown=delay,
        )
        results.append(
            {
                "username": username,
                "task_id": result.id,
                "delay": delay,
            }
        )

    return {
        "total_accounts": len(active_accounts),
        "ready_accounts": len(ready_accounts),
        "blocked_accounts": len(blocked_accounts),
        "blocked_status_counts": dict(preflight.get("blocked_status_counts") or {}),
        "tasks_queued": len(results),
        "skipped_accounts": skipped_accounts,
        "results": results,
    }
