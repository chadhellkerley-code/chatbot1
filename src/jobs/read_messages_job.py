"""Job de lectura de mensajes (polling) con lógica inteligente."""

import logging
import time
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from src.queue_config import app
from src.state_manager import get_state_manager
from celery import Task

logger = logging.getLogger(__name__)


@app.task(
    name='src.jobs.read_messages_job.poll_account',
    queue='polling',
    priority=5,
    time_limit=120,
)
def poll_account(username: str, password: str, proxy: Optional[Dict] = None) -> Dict[str, Any]:
    """
    Lee mensajes nuevos de una cuenta.
    
    Returns:
        {
            'username': str,
            'unread_count': int,
            'threads': list,
            'new_messages': list,
        }
    """
    state_mgr = get_state_manager()
    
    try:
        logger.info(f"[POLL JOB] Checking messages for @{username}")
        
        # Verificar si está rate limited
        if state_mgr.is_rate_limited(username):
            logger.debug(f"Skipping @{username} - rate limited")
            return {'username': username, 'skipped': True, 'reason': 'rate_limited'}
        
        # Aquí iría la lógica real de polling con instagrapi o playwright
        # Por ahora, estructura base:
        
        from client_factory import get_instagram_client
        
        # Obtener cliente (reutiliza sesión si existe)
        client = get_instagram_client(username, password, proxy)
        
        # Obtener threads con mensajes no leídos
        threads = client.direct_threads(amount=20)
        unread_threads = [t for t in threads if getattr(t, 'unread_count', 0) > 0]
        
        new_messages = []
        for thread in unread_threads:
            thread_id = getattr(thread, 'id', None)
            if not thread_id:
                continue
            
            # Obtener mensajes del thread
            messages = client.direct_messages(thread_id, amount=10)
            
            for msg in messages:
                # Solo mensajes entrantes no leídos
                if msg.user_id != client.user_id:
                    new_messages.append({
                        'thread_id': thread_id,
                        'message_id': getattr(msg, 'id', None),
                        'from_user': getattr(msg, 'user_id', None),
                        'text': getattr(msg, 'text', ''),
                        'timestamp': getattr(msg, 'timestamp', None),
                    })
        
        logger.info(f"[POLL JOB] @{username}: {len(new_messages)} new messages in {len(unread_threads)} threads")
        
        # Actualizar estado
        state_mgr.save_account_state(username, {
            'status': 'active',
            'last_poll': time.time(),
            'unread_count': len(new_messages),
        })
        
        return {
            'username': username,
            'unread_count': len(new_messages),
            'threads': len(unread_threads),
            'new_messages': new_messages,
        }
        
    except Exception as exc:
        logger.error(f"[POLL JOB] Error for @{username}: {exc}")
        
        # Si es rate limit, marcar
        if 'rate' in str(exc).lower() or 'limit' in str(exc).lower():
            state_mgr.set_rate_limit(username, 1800)  # 30 minutos
        
        return {
            'username': username,
            'error': str(exc),
            'unread_count': 0,
        }


@app.task(
    name='src.jobs.read_messages_job.poll_all_accounts',
    queue='polling',
    priority=4,
)
def poll_all_accounts() -> Dict[str, Any]:
    """
    Polling inteligente de todas las cuentas activas.
    Ejecutado periódicamente por Celery Beat.
    """
    from accounts import list_all
    
    logger.info("[POLL ALL] Starting periodic polling")
    
    # Obtener todas las cuentas activas
    all_accounts = list_all()
    active_accounts = [
        acc for acc in all_accounts
        if acc.get('status') != 'disabled'
    ]
    
    logger.info(f"[POLL ALL] Polling {len(active_accounts)} accounts")
    
    results = []
    for account in active_accounts:
        username = account.get('username')
        password = account.get('password')
        try:
            from src.proxy_payload import proxy_from_account
            proxy = proxy_from_account(account)
        except Exception:
            proxy = account.get('proxy')
        
        if not username or not password:
            continue
        
        # Encolar tarea de polling para cada cuenta
        # Con delay aleatorio para distribuir carga
        import random
        delay = random.randint(0, 30)
        
        result = poll_account.apply_async(
            kwargs={
                'username': username,
                'password': password,
                'proxy': proxy,
            },
            countdown=delay,
        )
        
        results.append({
            'username': username,
            'task_id': result.id,
            'delay': delay,
        })
    
    return {
        'total_accounts': len(active_accounts),
        'tasks_queued': len(results),
        'results': results,
    }
